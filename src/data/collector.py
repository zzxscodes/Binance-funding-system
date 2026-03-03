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
import random
import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from typing import Dict, List, Set, Optional, Callable
from collections import defaultdict, deque
from datetime import datetime, timezone
import pandas as pd

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol
from ..common.network_utils import safe_http_request, log_network_error, ConnectionTimeoutError

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
        self.api_base = config.get_binance_api_base_for_data_layer()
        self.reconnect_delay = 5
        # Binance要求30秒内必须有活动，使用可配置ping参数以适配不同网络质量
        self.ping_interval = int(config.get("data.collector_ping_interval_seconds", 20))
        self.ping_timeout = int(config.get("data.collector_ping_timeout_seconds", 20))
        self.open_timeout = int(config.get("data.collector_open_timeout_seconds", 30))
        self.ping_interval_jitter_seconds = float(
            config.get("data.collector_ping_interval_jitter_seconds", 2.5)
        )
        self.batch_connect_stagger_seconds = float(
            config.get("data.collector_batch_connect_stagger_seconds", 0.6)
        )
        
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
        self.callback_queue: asyncio.Queue = asyncio.Queue(
            maxsize=int(config.get("data.collector_callback_queue_maxsize", 20000))
        )
        self.callback_queues: List[asyncio.Queue] = []
        self.callback_workers: List[asyncio.Task] = []
        self.callback_worker_count = int(config.get("data.collector_callback_workers", 16))
        self.recover_on_reconnect = bool(config.get("data.collector_recover_on_reconnect", True))
        self.recover_max_pages = int(config.get("data.collector_recover_max_pages", 3))
        self.recover_page_limit = int(config.get("data.collector_recover_page_limit", 1000))
        self.recover_max_trades_per_reconnect = int(
            config.get("data.collector_recover_max_trades_per_reconnect", 2000)
        )
        self.recover_active_window_seconds = int(
            config.get("data.collector_recover_active_window_seconds", 120)
        )
        self.recover_max_symbols_per_reconnect = int(
            config.get("data.collector_recover_max_symbols_per_reconnect", 12)
        )
        self.recover_symbol_request_delay_ms = int(
            config.get("data.collector_recover_symbol_request_delay_ms", 80)
        )
        self.recover_max_requests_per_minute = int(
            config.get("data.collector_recover_max_requests_per_minute", 20)
        )
        self._recover_requests_remaining = self.recover_max_requests_per_minute
        self._recover_requests_window_start = time.time()
        self.recover_suspended_until = 0.0
        self._recover_lock = asyncio.Lock()
        self.last_agg_trade_id: Dict[str, int] = {}
        self.gap_recover_enabled = bool(config.get("data.collector_gap_recover_enabled", True))
        self.gap_recover_max_trades = int(config.get("data.collector_gap_recover_max_trades", 2000))
        self.gap_recover_page_limit = int(config.get("data.collector_gap_recover_page_limit", 500))
        self.gap_recover_cooldown_seconds = float(
            config.get("data.collector_gap_recover_cooldown_seconds", 5.0)
        )
        self.gap_recover_inflight: Set[str] = set()
        self.gap_recover_last_ts: Dict[str, float] = {}
        self.gap_recover_global_budget_per_minute = int(
            config.get("data.collector_gap_recover_global_budget_per_minute", 3000)
        )
        self._gap_recover_budget_remaining = self.gap_recover_global_budget_per_minute
        self._gap_recover_budget_window_start = time.time()
        self.recent_agg_id_cache_size = int(
            config.get("data.collector_recent_agg_id_cache_size", 500)
        )
        self.recent_agg_ids: Dict[str, Set[int]] = defaultdict(set)
        self.recent_agg_id_order: Dict[str, deque] = defaultdict(deque)
        
        # WebSocket URL长度限制（Binance建议每个连接最多200个stream）
        # 为了安全，我们使用更小的批次，减轻单连接流量压力
        self.max_symbols_per_connection = int(config.get("data.collector_max_symbols_per_connection", 50))
    
    def _split_symbols_into_batches(self) -> List[List[str]]:
        """
        将交易对列表拆分为多个批次，避免URL过长
        
        Returns:
            交易对批次列表
        """
        if not self.symbols_lower:
            return []

        # 采用轮询分配而不是顺序切片，避免高活跃symbol集中在同一连接造成过载
        batch_count = max(
            1, (len(self.symbols_lower) + self.max_symbols_per_connection - 1) // self.max_symbols_per_connection
        )
        batches: List[List[str]] = [[] for _ in range(batch_count)]
        for idx, symbol in enumerate(self.symbols_lower):
            batches[idx % batch_count].append(symbol)
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
    
    async def _process_trade(self, symbol: str, data: dict, from_recovery: bool = False):
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
            try:
                trade_id_int = int(trade_id) if trade_id is not None else None
            except Exception:
                trade_id_int = None
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
            if trade_id_int is not None:
                prev_id = self.last_agg_trade_id.get(symbol)
                # 实时流中出现小于等于已处理ID的消息，多为重连重放/乱序回放，直接丢弃。
                # 恢复流(from_recovery=True)允许补旧ID，不在这里拦截。
                if (not from_recovery) and prev_id is not None and trade_id_int <= prev_id:
                    return

                # 内存级去重：避免重连补缺/网络重放导致同一aggTrade重复进入聚合器
                id_set = self.recent_agg_ids[symbol]
                if trade_id_int in id_set:
                    return
                id_set.add(trade_id_int)
                id_queue = self.recent_agg_id_order[symbol]
                id_queue.append(trade_id_int)
                while len(id_queue) > self.recent_agg_id_cache_size:
                    old_id = id_queue.popleft()
                    id_set.discard(old_id)

                # 若检测到ID跳跃，说明该symbol在实时链路出现缺口，触发受控补缺
                if (
                    (not from_recovery)
                    and self.gap_recover_enabled
                    and prev_id is not None
                    and trade_id_int > prev_id + 1
                ):
                    now_ts = time.time()
                    can_recover = (
                        symbol not in self.gap_recover_inflight
                        and now_ts - self.gap_recover_last_ts.get(symbol, 0.0)
                        >= self.gap_recover_cooldown_seconds
                    )
                    if can_recover:
                        from_id = prev_id + 1
                        to_id = trade_id_int - 1
                        self.gap_recover_inflight.add(symbol)
                        self.gap_recover_last_ts[symbol] = now_ts
                        asyncio.create_task(
                            self._recover_symbol_gap(symbol, from_id, to_id)
                        )
                if prev_id is None or trade_id_int > prev_id:
                    self.last_agg_trade_id[symbol] = trade_id_int
            
            # 调用回调函数（传递给K线聚合器）
            if self.on_trade_callback:
                try:
                    if self.callback_queues:
                        idx = hash(symbol) % len(self.callback_queues)
                        await self.callback_queues[idx].put((symbol, trade_record))
                    else:
                        await self.callback_queue.put((symbol, trade_record))
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

    async def _schedule_gap_recover_continuation(
        self, symbol: str, from_id: int, to_id: int, delay_seconds: float
    ):
        """延迟继续单symbol缺口补齐，避免预算耗尽时形成任务风暴。"""
        try:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            if not self.running:
                self.gap_recover_inflight.discard(symbol)
                return
            await self._recover_symbol_gap(symbol, from_id, to_id)
        except asyncio.CancelledError:
            self.gap_recover_inflight.discard(symbol)
        except Exception as e:
            logger.warning(
                f"Failed to continue gap recover for {symbol} "
                f"(fromId={from_id}, toId={to_id}): {e}"
            )
            self.gap_recover_inflight.discard(symbol)

    async def _recover_symbol_gap(self, symbol: str, from_id: int, to_id: int):
        """针对单symbol检测到的aggTrade ID缺口做受控补缺。"""
        recovered = 0
        rescheduled = False
        remaining_from: Optional[int] = None
        retry_delay_seconds = 0.0
        try:
            if time.time() < self.recover_suspended_until:
                return
            async with self._recover_lock:
                # 全局预算按分钟重置，避免高并发symbol同时补缺触发请求洪峰
                now_ts = time.time()
                elapsed = now_ts - self._gap_recover_budget_window_start
                if elapsed >= 60:
                    self._gap_recover_budget_window_start = now_ts
                    self._gap_recover_budget_remaining = self.gap_recover_global_budget_per_minute
                    elapsed = 0.0
                if self._gap_recover_budget_remaining <= 0:
                    # 预算耗尽时保留未完成区间，等待下一分钟窗口继续补缺，避免“半截恢复后永远丢失”
                    remaining_from = from_id
                    retry_delay_seconds = max(1.0, 60.0 - elapsed + 0.2)
                else:
                    headers = {"User-Agent": "data-collector-gap-recover/1.0"}
                    async with aiohttp.ClientSession(headers=headers) as session:
                        next_id = from_id
                        while (
                            next_id <= to_id
                            and recovered < self.gap_recover_max_trades
                            and self._gap_recover_budget_remaining > 0
                        ):
                            params = {
                                "symbol": symbol,
                                "fromId": next_id,
                                "limit": self.gap_recover_page_limit,
                            }
                            try:
                                rows = await safe_http_request(
                                    session,
                                    "GET",
                                    f"{self.api_base}/fapi/v1/aggTrades",
                                    params=params,
                                    max_retries=0,
                                    timeout=10.0,
                                    return_json=True,
                                    use_rate_limit=True,
                                )
                            except Exception:
                                self.recover_suspended_until = time.time() + 600
                                break
                            if not rows:
                                break
                            reached_to_id = False
                            processed_full_page = True
                            for r in rows:
                                try:
                                    agg_id = int(r.get("a"))
                                except Exception:
                                    continue
                                if agg_id < from_id:
                                    continue
                                if agg_id > to_id:
                                    next_id = to_id + 1
                                    reached_to_id = True
                                    break
                                await self._process_trade(symbol, r, from_recovery=True)
                                recovered += 1
                                self._gap_recover_budget_remaining -= 1
                                next_id = agg_id + 1
                                if (
                                    recovered >= self.gap_recover_max_trades
                                    or self._gap_recover_budget_remaining <= 0
                                ):
                                    # 这里是页内提前中断，next_id 已指向“下一条未处理ID”。
                                    # 不能在后面覆盖为 rows[-1]+1，否则会跳过当前页剩余未处理ID。
                                    processed_full_page = False
                                    break
                            if reached_to_id:
                                break
                            if processed_full_page:
                                # 当前页被完整处理，next_id 直接推进到本页最后ID之后
                                if rows:
                                    try:
                                        next_id = int(rows[-1].get("a", next_id)) + 1
                                    except Exception:
                                        next_id += self.gap_recover_page_limit
                            if len(rows) < self.gap_recover_page_limit:
                                break

                        # 若触及单次上限或预算上限且区间尚未覆盖，继续分段补缺
                        if next_id <= to_id and (
                            recovered >= self.gap_recover_max_trades
                            or self._gap_recover_budget_remaining <= 0
                        ):
                            remaining_from = next_id
                            if self._gap_recover_budget_remaining <= 0:
                                elapsed = time.time() - self._gap_recover_budget_window_start
                                retry_delay_seconds = max(1.0, 60.0 - elapsed + 0.2)
                            else:
                                retry_delay_seconds = max(
                                    0.2, min(self.gap_recover_cooldown_seconds, 5.0)
                                )

            if recovered > 0:
                logger.info(
                    f"Recovered {recovered} gap aggTrades for {symbol} "
                    f"(fromId={from_id}, toId={to_id})"
                )
            if remaining_from is not None and self.running:
                rescheduled = True
                asyncio.create_task(
                    self._schedule_gap_recover_continuation(
                        symbol, remaining_from, to_id, retry_delay_seconds
                    )
                )
        finally:
            if not rescheduled:
                self.gap_recover_inflight.discard(symbol)
    
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
                ping_jitter = random.uniform(
                    -self.ping_interval_jitter_seconds, self.ping_interval_jitter_seconds
                )
                effective_ping_interval = max(5.0, float(self.ping_interval) + ping_jitter)
                effective_ping_timeout = max(
                    float(self.ping_timeout), effective_ping_interval + 5.0
                )
                logger.info(
                    f"Connecting to WebSocket batch {batch_index + 1} "
                    f"({len(symbols_batch)} symbols, ping={effective_ping_interval:.1f}/"
                    f"{effective_ping_timeout:.1f}s): {ws_url[:100]}..."
                )
                
                async with websockets.connect(
                    ws_url,
                    ping_interval=effective_ping_interval,
                    ping_timeout=effective_ping_timeout,
                    close_timeout=10,
                    open_timeout=self.open_timeout
                ) as websocket:
                    logger.info(
                        f"WebSocket batch {batch_index + 1} connected successfully "
                        f"({len(symbols_batch)} symbols)"
                    )
                    if self.recover_on_reconnect:
                        await self._recover_batch_missing_agg_trades(batch_symbols_upper, batch_index)
                    
                    # 连接成功后重置该批次的重连计数
                    batch_reconnect_count = 0
                    
                    # 使用websockets库的自动ping机制，不对recv做wait_for取消，
                    # 避免底层传输对象在取消后进入不一致状态（会触发resume_reading错误）
                    while self.running:
                        try:
                            msg_str = await websocket.recv()
                            
                            msg = json.loads(msg_str)
                            await self._process_message(msg)
                                
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
                    
                    # ---- 防连接风暴：ConnectionClosed/内循环异常 break 后，
                    # ---- 按 batch_index 错开重连，避免所有 batch 同时涌向服务器 ----
                    if self.running:
                        stagger = 1.0 + batch_index * 0.3 + random.uniform(0, 1.5)
                        logger.info(
                            f"WebSocket batch {batch_index + 1} 等待 {stagger:.1f}s "
                            f"后重连（防止连接风暴）"
                        )
                        await asyncio.sleep(stagger)
                            
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
            await asyncio.sleep(max(0.1, self.batch_connect_stagger_seconds))
        
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

    async def _recover_batch_missing_agg_trades(self, symbols_upper: List[str], batch_index: int):
        """重连后按fromId补齐断线期间丢失的aggTrades（修聚合链路根因）。"""
        if time.time() < self.recover_suspended_until:
            return
        if not symbols_upper:
            return
        now_ts = time.time()
        active_symbols = []
        for symbol in symbols_upper:
            last_msg_time = float(self.stats["last_message_time"].get(symbol, 0.0) or 0.0)
            if now_ts - last_msg_time <= self.recover_active_window_seconds:
                active_symbols.append(symbol)
        if not active_symbols:
            return
        active_symbols.sort(
            key=lambda s: float(self.stats["last_message_time"].get(s, 0.0) or 0.0),
            reverse=True,
        )
        recover_symbols = active_symbols[: self.recover_max_symbols_per_reconnect]

        headers = {"User-Agent": "data-collector-recover/1.0"}
        async with self._recover_lock:
            now_ts = time.time()
            if now_ts - self._recover_requests_window_start >= 60:
                self._recover_requests_window_start = now_ts
                self._recover_requests_remaining = self.recover_max_requests_per_minute
            if self._recover_requests_remaining <= 0:
                return
            async with aiohttp.ClientSession(headers=headers) as session:
                recovered_total = 0
                for symbol in recover_symbols:
                    last_id = self.last_agg_trade_id.get(symbol)
                    if last_id is None:
                        continue

                    from_id = last_id + 1
                    pages = 0
                    while pages < self.recover_max_pages:
                        if recovered_total >= self.recover_max_trades_per_reconnect:
                            break
                        if self._recover_requests_remaining <= 0:
                            break
                        params = {
                            "symbol": symbol,
                            "fromId": from_id,
                            "limit": self.recover_page_limit,
                        }
                        try:
                            rows = await safe_http_request(
                                session,
                                "GET",
                                f"{self.api_base}/fapi/v1/aggTrades",
                                params=params,
                                max_retries=0,
                                timeout=10.0,
                                return_json=True,
                                use_rate_limit=True,
                            )
                            self._recover_requests_remaining -= 1
                        except Exception:
                            # 命中高风险错误时暂停恢复，避免放大请求压力
                            self.recover_suspended_until = time.time() + 600
                            break

                        if not rows:
                            break

                        for r in rows:
                            await self._process_trade(symbol, r)
                            try:
                                from_id = int(r.get("a", from_id)) + 1
                            except Exception:
                                pass

                        recovered_total += len(rows)
                        pages += 1
                        if len(rows) < self.recover_page_limit:
                            break
                    if self.recover_symbol_request_delay_ms > 0:
                        await asyncio.sleep(self.recover_symbol_request_delay_ms / 1000.0)

                if recovered_total > 0:
                    logger.info(
                        f"WebSocket batch {batch_index + 1} recovered {recovered_total} missing aggTrades "
                        f"after reconnect from {len(recover_symbols)} active symbols"
                    )

    async def _callback_worker(self, worker_idx: int, queue: asyncio.Queue):
        """异步回调worker：解耦WebSocket接收与下游处理。"""
        while self.running or (not queue.empty()):
            try:
                symbol, trade_record = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue

            try:
                await self.on_trade_callback(symbol, trade_record)
            except Exception as e:
                logger.error(f"Callback worker {worker_idx} failed for {symbol}: {e}", exc_info=True)
            finally:
                queue.task_done()
    
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

        self.callback_workers = []
        worker_count = max(1, self.callback_worker_count)
        self.callback_queues = [
            asyncio.Queue(maxsize=max(1000, int(self.callback_queue.maxsize / worker_count)))
            for _ in range(worker_count)
        ]
        for i in range(worker_count):
            self.callback_workers.append(asyncio.create_task(self._callback_worker(i, self.callback_queues[i])))
        
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

        for q in self.callback_queues:
            try:
                await asyncio.wait_for(q.join(), timeout=8.0)
            except Exception:
                pass
        for task in self.callback_workers:
            if not task.done():
                task.cancel()
        if self.callback_workers:
            await asyncio.gather(*self.callback_workers, return_exceptions=True)
        self.callback_workers = []
        self.callback_queues = []
        
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
