"""
归集逐笔成交数据采集模块
从Binance永续合约WebSocket接收归集逐笔成交数据（Aggregate Trades）

归集逐笔（@aggTrade）与逐笔成交（@trade）的区别：
- 逐笔成交：每一笔成交都会推送一条消息，数据量大
- 归集逐笔：将同一价格的多笔成交合并为一条消息，数据量小，更适合聚合计算
- 归集逐笔的qty和quoteQty是合并后的总数量和总成交额
"""
import json
import time
import asyncio
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from typing import Dict, List, Set, Optional, Callable
from collections import defaultdict, deque
from datetime import datetime, timezone
import pandas as pd

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol
from ..common.network_utils import log_network_error, ConnectionTimeoutError

logger = get_logger('data_collector')


class TradeCollector:
    """归集逐笔成交数据采集器"""
    
    def __init__(self, symbols: List[str], on_trade_callback: Optional[Callable] = None):
        """
        初始化采集器
        
        Args:
            symbols: 交易对列表（大写，如 ['BTCUSDT', 'ETHUSDT']）
            on_trade_callback: 归集逐笔成交数据回调函数 callback(symbol: str, trade: dict)
        """
        self.symbols = [format_symbol(s) for s in symbols]
        self.symbols_lower = [s.lower() for s in self.symbols]
        self.on_trade_callback = on_trade_callback
        
        # WebSocket配置（根据 execution.mode 自动选择正确的地址，数据层支持testnet回退到live）
        self.ws_base = config.get_binance_ws_base_for_data_layer()
        self.reconnect_delay = 5
        # Binance要求30秒内必须有活动，我们使用20秒ping间隔确保连接稳定
        # 使用websockets库的自动ping机制，更可靠
        # 增加ping间隔和超时时间，减少频繁断开
        self.ping_interval = 20  # 每20秒自动发送ping（Binance建议10-30秒）
        self.ping_timeout = 10  # ping超时时间增加到10秒，避免网络延迟导致的误判
        self.open_timeout = 30  # 连接超时时间
        
        # 统计信息
        self.stats = {
            'trades_received': defaultdict(int),
            'start_time': time.time(),
            'last_message_time': defaultdict(float),
            'reconnect_count': 0,
        }
        
        self.running = False
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.ws_task: Optional[asyncio.Task] = None
        self.ws_connections: List[asyncio.Task] = []  # 多个WebSocket连接任务
        
        # WebSocket URL长度限制（Binance建议每个连接最多200个stream）
        # 为了安全，我们使用更小的批次（100个symbols/连接）
        self.max_symbols_per_connection = 100
    
    def _split_symbols_into_batches(self) -> List[List[str]]:
        """
        将交易对列表拆分为多个批次，避免URL过长
        
        Returns:
            交易对批次列表
        """
        batches = []
        for i in range(0, len(self.symbols_lower), self.max_symbols_per_connection):
            batch = self.symbols_lower[i:i + self.max_symbols_per_connection]
            batches.append(batch)
        return batches
    
    def _get_ws_url(self, symbols_batch: List[str]) -> str:
        """
        构建WebSocket URL（针对一个批次）
        
        Args:
            symbols_batch: 交易对批次（小写）
        
        Returns:
            WebSocket URL
        """
        # Binance永续合约使用组合stream
        # 使用归集逐笔（@aggTrade）而不是逐笔成交（@trade）
        # 归集逐笔将同一价格的多笔成交合并，减少数据量，更适合聚合计算
        streams = [f"{symbol.lower()}@aggTrade" for symbol in symbols_batch]
        stream_str = "/".join(streams)
        url = f"{self.ws_base}/stream?streams={stream_str}"
        return url
    
    def _normalize_timestamp_us(self, ts_ms: int) -> int:
        """将毫秒时间戳转换为微秒"""
        return ts_ms * 1000
    
    async def _process_trade(self, symbol: str, data: dict):
        """
        处理归集逐笔成交数据（Aggregate Trades）
        
        归集逐笔（@aggTrade）与逐笔成交（@trade）的数据结构相同，但归集逐笔将
        同一价格的多笔成交合并为一条消息，减少数据量，更适合聚合计算。
        
        Args:
            symbol: 交易对（大写）
            data: 原始成交数据（来自@aggTrade或@trade stream）
        """
        try:
            # 提取关键字段
            # 归集逐笔和逐笔成交的字段名相同
            trade_id = data.get('a') or data.get('t')  # aggregate trade ID (a) 或 trade ID (t)
            price = float(data.get('p', 0))  # price
            qty = float(data.get('q', 0))  # quantity (归集逐笔中这是合并后的总数量)
            quote_qty = float(data.get('Q', 0))  # quote quantity (归集逐笔中这是合并后的总成交额)
            if quote_qty == 0:
                quote_qty = price * qty
            
            timestamp_ms = data.get('T', 0)  # trade time
            timestamp_us = self._normalize_timestamp_us(timestamp_ms)
            is_buyer_maker = data.get('m', False)  # is buyer maker (归集逐笔中，这是第一笔成交的方向)
            
            # 归集逐笔特有字段（如果存在）
            first_trade_id = data.get('f', None)  # first trade ID in the aggregation
            last_trade_id = data.get('l', None)  # last trade ID in the aggregation
            
            # 构建标准化交易记录
            # 归集逐笔和逐笔成交使用相同的记录格式，便于后续处理
            trade_record = {
                'symbol': format_symbol(symbol),
                'tradeId': trade_id,
                'price': price,
                'qty': qty,  # 归集逐笔中，这是合并后的总数量
                'quoteQty': quote_qty,  # 归集逐笔中，这是合并后的总成交额
                'isBuyerMaker': is_buyer_maker,
                'ts': pd.Timestamp(timestamp_ms, unit='ms', tz='UTC'),
                'ts_ms': timestamp_ms,
                'ts_us': timestamp_us,
                # 归集逐笔特有字段（如果存在）
                'firstTradeId': first_trade_id,
                'lastTradeId': last_trade_id,
                'isAggregated': first_trade_id is not None and last_trade_id is not None,  # 标记是否为归集逐笔
            }
            
            # 更新统计
            self.stats['trades_received'][symbol] += 1
            self.stats['last_message_time'][symbol] = time.time()
            
            # 调用回调函数（传递给K线聚合器）
            if self.on_trade_callback:
                try:
                    await self.on_trade_callback(symbol, trade_record)
                except Exception as e:
                    logger.error(f"Error in trade callback for {symbol}: {e}", exc_info=True)
            
            # 日志（每1000条记录一次）
            if self.stats['trades_received'][symbol] % 1000 == 0:
                logger.debug(
                    f"{symbol}: received {self.stats['trades_received'][symbol]} trades, "
                    f"latest price={price}, qty={qty}"
                )
                
        except Exception as e:
            logger.error(f"Process trade data failed for {symbol}: {e}", exc_info=True)
            logger.error(f"Problem data: {json.dumps(data)[:500]}")
    
    async def _process_message(self, msg: dict):
        """处理WebSocket消息"""
        try:
            stream = msg.get('stream', '')
            data = msg.get('data', {})
            
            if not stream or not data:
                return
            
            # 解析stream名称: btcusdt@aggTrade 或 btcusdt@trade
            parts = stream.split('@')
            if len(parts) < 2:
                return
            
            symbol_lower = parts[0]
            event_type = parts[1]
            
            # 处理归集逐笔（aggTrade）和逐笔成交（trade）事件
            # 优先使用归集逐笔（@aggTrade），这是需求中要求的
            if event_type == 'aggTrade' or event_type == 'trade':
                symbol = format_symbol(symbol_lower)
                await self._process_trade(symbol, data)
            else:
                logger.debug(f"Unhandled event type: {event_type}, stream: {stream}")
                
        except Exception as e:
            logger.error(f"Message processing failed: {e}", exc_info=True)
            logger.error(f"Problem message: {json.dumps(msg)[:500]}")
    
    async def _websocket_handler_single(self, symbols_batch: List[str], batch_index: int):
        """
        单个WebSocket连接处理循环（处理一个批次的交易对）
        
        Args:
            symbols_batch: 交易对批次（小写）
            batch_index: 批次索引
        """
        ws_url = self._get_ws_url(symbols_batch)
        batch_symbols_upper = [s.upper() for s in symbols_batch]
        batch_reconnect_count = 0  # 每个批次独立的重连计数
        
        while self.running:
            try:
                logger.info(
                    f"Connecting to WebSocket batch {batch_index + 1} "
                    f"({len(symbols_batch)} symbols): {ws_url[:100]}..."
                )
                
                async with websockets.connect(
                    ws_url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    close_timeout=10,
                    open_timeout=self.open_timeout
                ) as websocket:
                    logger.info(
                        f"WebSocket batch {batch_index + 1} connected successfully "
                        f"({len(symbols_batch)} symbols)"
                    )
                    
                    # 连接成功后重置该批次的重连计数
                    batch_reconnect_count = 0
                    
                    # 使用websockets库的自动ping机制（ping_interval=10秒）
                    # 不需要手动ping任务，避免冲突
                    # recv()超时设置为略大于ping_interval，确保能及时收到消息或触发超时
                    while self.running:
                        try:
                            # 接收消息，超时时间略大于ping_interval
                            # 这样即使没有消息，也能在ping_interval内触发超时，让自动ping工作
                            # 超时时间设置为ping_interval的1.5倍，确保有足够时间接收消息
                            msg_str = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=self.ping_interval * 1.5  # 30秒超时，确保在20秒ping间隔内能处理
                            )
                            
                            msg = json.loads(msg_str)
                            await self._process_message(msg)
                            
                        except asyncio.TimeoutError:
                            # 超时是正常的，websockets库的自动ping会保持连接
                            # 继续接收消息
                            continue
                                
                        except ConnectionClosed as e:
                            log_network_error(
                                f"WebSocket batch {batch_index + 1} 接收消息",
                                e,
                                context={
                                    "symbols": batch_symbols_upper[:3],
                                    "batch_index": batch_index
                                }
                            )
                            logger.info(f"WebSocket batch {batch_index + 1} 连接被服务器关闭，将重连...")
                            break
                        except Exception as e:
                            # 记录错误，但继续尝试重连而不是直接退出
                            log_network_error(
                                f"WebSocket batch {batch_index + 1} 接收消息",
                                e,
                                context={
                                    "symbols": batch_symbols_upper[:3],
                                    "batch_index": batch_index
                                }
                            )
                            # 对于底层传输错误（如 resume_reading），需要重新连接
                            # 记录详细错误信息以便调试
                            if isinstance(e, AttributeError) and 'resume_reading' in str(e):
                                logger.warning(
                                    f"WebSocket batch {batch_index + 1} 底层传输错误，"
                                    f"连接可能已关闭，将重连..."
                                )
                            else:
                                logger.warning(
                                    f"WebSocket batch {batch_index + 1} 接收消息时出错，"
                                    f"将重连... 错误: {type(e).__name__}: {e}"
                                )
                            break
                            
            except WebSocketException as e:
                if self.running:
                    batch_reconnect_count += 1
                    self.stats['reconnect_count'] += 1
                    # 指数退避：重连延迟随重连次数增加，但不超过30秒
                    reconnect_delay = min(self.reconnect_delay * (1.5 ** min(batch_reconnect_count - 1, 3)), 30)
                    # 错开重连时间：每个批次延迟不同，避免同时重连
                    jitter = batch_index * 0.5  # 每个批次错开0.5秒
                    total_delay = reconnect_delay + jitter
                    
                    log_network_error(
                        f"WebSocket batch {batch_index + 1} 连接",
                        e,
                        context={
                            "symbols": batch_symbols_upper[:3],
                            "reconnect_count": batch_reconnect_count,
                            "url": ws_url[:200],
                            "batch_index": batch_index
                        }
                    )
                    logger.info(
                        f"WebSocket batch {batch_index + 1} 网络连接失败，"
                        f"{total_delay:.1f}秒后重连... "
                        f"(批次重连次数: {batch_reconnect_count})"
                    )
                    await asyncio.sleep(total_delay)
                else:
                    break
            except (ConnectionTimeoutError, asyncio.TimeoutError) as e:
                if self.running:
                    batch_reconnect_count += 1
                    self.stats['reconnect_count'] += 1
                    # 指数退避：重连延迟随重连次数增加，但不超过30秒
                    reconnect_delay = min(self.reconnect_delay * (1.5 ** min(batch_reconnect_count - 1, 3)), 30)
                    # 错开重连时间：每个批次延迟不同，避免同时重连
                    jitter = batch_index * 0.5  # 每个批次错开0.5秒
                    total_delay = reconnect_delay + jitter
                    
                    log_network_error(
                        f"WebSocket batch {batch_index + 1} 连接超时",
                        e,
                        context={
                            "symbols": batch_symbols_upper[:3],
                            "reconnect_count": batch_reconnect_count,
                            "url": ws_url[:200],
                            "batch_index": batch_index
                        }
                    )
                    logger.info(
                        f"WebSocket batch {batch_index + 1} 连接超时，"
                        f"{total_delay:.1f}秒后重连... "
                        f"(批次重连次数: {batch_reconnect_count})"
                    )
                    await asyncio.sleep(total_delay)
                else:
                    break
            except Exception as e:
                if self.running:
                    import socket
                    
                    batch_reconnect_count += 1
                    self.stats['reconnect_count'] += 1
                    
                    # DNS解析失败通常是网络暂时性问题，使用较短的重连延迟
                    if isinstance(e, (socket.gaierror, OSError)) and "getaddrinfo" in str(e):
                        # DNS错误：使用较短延迟，因为可能是暂时性网络问题
                        reconnect_delay = min(self.reconnect_delay * (1.2 ** min(batch_reconnect_count - 1, 2)), 10)
                    else:
                        # 其他错误：使用指数退避
                        reconnect_delay = min(self.reconnect_delay * (1.5 ** min(batch_reconnect_count - 1, 3)), 30)
                    
                    # 错开重连时间：每个批次延迟不同，避免同时重连
                    jitter = batch_index * 0.5  # 每个批次错开0.5秒
                    total_delay = reconnect_delay + jitter
                    
                    log_network_error(
                        f"WebSocket batch {batch_index + 1} 处理",
                        e,
                        context={
                            "symbols": batch_symbols_upper[:3],
                            "reconnect_count": batch_reconnect_count,
                            "url": ws_url[:200],
                            "batch_index": batch_index
                        }
                    )
                    
                    # DNS错误使用更友好的日志消息
                    if isinstance(e, (socket.gaierror, OSError)) and "getaddrinfo" in str(e):
                        logger.info(
                            f"WebSocket batch {batch_index + 1} DNS解析失败（可能是网络暂时性问题），"
                            f"{total_delay:.1f}秒后重连... "
                            f"(批次重连次数: {batch_reconnect_count})"
                        )
                    else:
                        logger.info(
                            f"WebSocket batch {batch_index + 1} 意外错误，"
                            f"{total_delay:.1f}秒后重连... "
                            f"(批次重连次数: {batch_reconnect_count})"
                        )
                    await asyncio.sleep(total_delay)
                else:
                    break
    
    async def _websocket_handler(self):
        """
        WebSocket处理循环（管理多个连接）
        将交易对拆分为多个批次，每个批次使用独立的WebSocket连接
        """
        # 将交易对拆分为多个批次
        batches = self._split_symbols_into_batches()
        
        if not batches:
            logger.warning("No symbols to subscribe, skipping WebSocket connection")
            return
        
        logger.info(
            f"Subscribing to {len(self.symbols)} symbols using {len(batches)} WebSocket connection(s) "
            f"({self.max_symbols_per_connection} symbols per connection)"
        )
        
        # 为每个批次创建独立的WebSocket连接
        self.ws_connections = []
        for i, batch in enumerate(batches):
            task = asyncio.create_task(self._websocket_handler_single(batch, i))
            self.ws_connections.append(task)
            # 稍微错开连接时间，避免同时连接
            await asyncio.sleep(0.5)
        
        # 等待所有连接任务完成
        # 使用 return_exceptions=True 确保即使某个任务失败，也不会导致整个方法退出
        try:
            results = await asyncio.gather(*self.ws_connections, return_exceptions=True)
            # 检查是否有任务因为异常而退出
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        f"WebSocket batch {i + 1} 任务异常退出: {type(result).__name__}: {result}",
                        exc_info=result if isinstance(result, BaseException) else None
                    )
                    # 如果进程仍在运行，重新启动该批次的任务
                    if self.running and i < len(batches):
                        logger.info(f"重新启动 WebSocket batch {i + 1}...")
                        task = asyncio.create_task(self._websocket_handler_single(batches[i], i))
                        self.ws_connections[i] = task
        except Exception as e:
            logger.error(f"Error in WebSocket connections: {e}", exc_info=True)
        finally:
            self.websocket = None
    
    async def update_symbols(self, new_symbols: List[str]):
        """
        更新监听的交易对列表
        注意：需要重新连接WebSocket才能生效
        """
        old_symbols = set(self.symbols)
        new_symbols_set = {format_symbol(s) for s in new_symbols}
        
        added = new_symbols_set - old_symbols
        removed = old_symbols - new_symbols_set
        
        if added or removed:
            self.symbols = list(new_symbols_set)
            self.symbols_lower = [s.lower() for s in self.symbols]
            logger.info(f"Symbols updated: added {len(added)}, removed {len(removed)}")
            
            # 优化：清理不再使用的symbol的统计信息，防止内存累积
            if removed:
                for symbol in removed:
                    self.stats['trades_received'].pop(symbol, None)
                    self.stats['last_message_time'].pop(symbol, None)
                logger.debug(f"Cleaned up stats for {len(removed)} removed symbols")
            
            # 如果正在运行，需要重新连接
            if self.running:
                logger.info("Restarting WebSocket connection with new symbols...")
                # 停止所有现有的WebSocket连接
                if self.ws_connections:
                    for task in self.ws_connections:
                        if not task.done():
                            task.cancel()
                    # 等待所有任务完成或取消
                    try:
                        await asyncio.gather(*self.ws_connections, return_exceptions=True)
                    except Exception as e:
                        logger.warning(f"Error stopping WebSocket connections: {e}")
                    self.ws_connections = []
                
                # 取消主任务
                if self.ws_task and not self.ws_task.done():
                    self.ws_task.cancel()
                    try:
                        await self.ws_task
                    except asyncio.CancelledError:
                        pass
                
                # 重新启动websocket handler（使用新的symbols）
                self.ws_task = asyncio.create_task(self._websocket_handler())
    
    async def start(self):
        """启动采集器"""
        if self.running:
            logger.warning("Collector is already running")
            return
        
        if not self.symbols:
            logger.error("No symbols to collect")
            return
        
        self.running = True
        self.stats['start_time'] = time.time()
        logger.info(f"Starting trade collector for {len(self.symbols)} symbols")
        
        self.ws_task = asyncio.create_task(self._websocket_handler())
    
    async def stop(self):
        """停止采集器"""
        logger.info("Stopping trade collector...")
        self.running = False
        
        # 关闭所有WebSocket连接
        if self.ws_connections:
            logger.info(f"Cancelling {len(self.ws_connections)} WebSocket connection tasks...")
            for task in self.ws_connections:
                if not task.done():
                    task.cancel()
            # 等待所有任务完成或取消
            try:
                await asyncio.gather(*self.ws_connections, return_exceptions=True)
            except Exception as e:
                logger.warning(f"Error stopping WebSocket connections: {e}")
            self.ws_connections = []
        
        if self.ws_task and not self.ws_task.done():
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning(f"Error stopping WebSocket task: {e}")
        
        if self.websocket:
            try:
                # 检查websocket是否有closed属性
                if hasattr(self.websocket, 'closed') and not self.websocket.closed:
                    await asyncio.wait_for(self.websocket.close(), timeout=5.0)
                elif not hasattr(self.websocket, 'closed'):
                    # 旧版本websockets可能没有closed属性
                    await asyncio.wait_for(self.websocket.close(), timeout=5.0)
            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"关闭WebSocket连接时出错: {e}")
        
        # 打印统计信息
        runtime = time.time() - self.stats['start_time']
        total_trades = sum(self.stats['trades_received'].values())
        logger.info(f"Trade collector stopped. Runtime: {runtime/60:.1f} minutes")
        logger.info(f"Total trades received: {total_trades}")
        for symbol in self.symbols:
            count = self.stats['trades_received'][symbol]
            if count > 0:
                logger.info(f"  {symbol}: {count} trades")
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        runtime = time.time() - self.stats['start_time']
        total_trades = sum(self.stats['trades_received'].values())
        
        return {
            'running': self.running,
            'symbols_count': len(self.symbols),
            'runtime_seconds': runtime,
            'total_trades_received': total_trades,
            'reconnect_count': self.stats['reconnect_count'],
            'trades_by_symbol': dict(self.stats['trades_received']),
            'last_message_time_by_symbol': {
                symbol: self.stats['last_message_time'][symbol] 
                for symbol in self.symbols 
                if symbol in self.stats['last_message_time']
            }
        }
