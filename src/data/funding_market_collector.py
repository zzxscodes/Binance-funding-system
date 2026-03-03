"""
资金费率/标记价格/溢价指数 WebSocket 实时采集模块

通过订阅 Binance 的真实数据流（@indexPrice 和 @markPrice），
用组合流 WebSocket 连接接收所有交易对的真实数据：
  - 标记价格 (mark price) - 来自 @markPrice 流
  - 指数价格 (index price) - 来自 @indexPrice 流（真实数据，非评估值）
  - 资金费率 (funding rate) - 通过 WebSocket 检测结算时间，然后通过 REST API
                               获取真实的已结算资金费率（而非 WebSocket 的预测值）
  - 下次结算时间 (next funding time) - 来自 @markPrice 流

与 aggTrade 采集器采用相同的 WebSocket 架构，保证同等稳定性。
资金费率获取策略：
  - WebSocket 实时监控结算时间变化（每 8 小时一次）
  - 检测到结算时，立即通过 REST API /fapi/v1/fundingRate 获取真实的已结算费率
  - 如果 API 获取失败，回退到 WebSocket 的预测值（作为备选方案）

注意：
  - 使用真实的 @indexPrice 流而非 !markPrice@arr 中的评估数据，
    确保溢价指数计算基于真实的指数价格
  - 资金费率使用 REST API 获取真实的已结算值，而非 WebSocket 的预测值
"""
import json
import time
import asyncio
import random
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from typing import Dict, List, Set, Optional, Callable
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import pandas as pd
import aiohttp

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol
from ..common.network_utils import log_network_error, ConnectionTimeoutError, safe_http_request

logger = get_logger('funding_market_collector')


class FundingMarketCollector:
    """
    资金费率/标记价格/溢价指数实时采集器

    通过 WebSocket 订阅 @indexPrice 和 @markPrice 流，实时接收所有 USDT-M 永续合约的
    真实标记价格、真实指数价格数据。

    资金费率获取策略：
    - WebSocket 实时监控结算时间变化（通过 @markPrice 流的 T 字段）
    - 检测到结算时，通过 REST API 获取真实的已结算资金费率
    - 确保获取的是真实的已结算费率，而非 WebSocket 的预测值

    与 aggTrade 采集器使用相同的 WebSocket 推送架构（服务器主动推数据），
    资金费率仅在结算时调用 REST API，频率很低（每 8 小时一次），不会触发限流。
    """

    def __init__(
        self,
        symbols: List[str],
        on_funding_rate_callback: Optional[Callable] = None,
        on_premium_index_callback: Optional[Callable] = None,
    ):
        """
        Args:
            symbols: 需要监控的交易对列表（大写，如 ['BTCUSDT', 'ETHUSDT']）
            on_funding_rate_callback: 资金费率结算回调
                callback(symbol, {fundingRate, fundingTime, markPrice})
            on_premium_index_callback: 溢价指数 5 分钟 K 线完成回调
                callback(symbol, kline_dict)
        """
        self.symbols_set: Set[str] = {format_symbol(s) for s in symbols}
        self.on_funding_rate_callback = on_funding_rate_callback
        self.on_premium_index_callback = on_premium_index_callback

        # WebSocket 配置（与 TradeCollector 保持一致的重连策略）
        self.ws_base = config.get_binance_ws_base_for_data_layer()
        self.reconnect_delay = 5
        self.ping_interval = int(config.get("data.mark_price_ping_interval_seconds", 20))
        self.ping_timeout = int(config.get("data.mark_price_ping_timeout_seconds", 30))
        self.open_timeout = int(config.get("data.mark_price_open_timeout_seconds", 30))

        # REST API 配置（用于获取真实的已结算资金费率）
        self.api_base = config.get_binance_api_base_for_data_layer()
        self._http_session: Optional[aiohttp.ClientSession] = None

        # ---- 资金费率追踪 ----
        # {symbol: {"rate": float, "next_time": int(ms), "mark_price": float}}
        self._funding_state: Dict[str, Dict] = {}

        # ---- 标记价格和指数价格缓存 ----
        # {symbol: {"mark_price": float, "index_price": float, "last_update": int(ms)}}
        self._price_cache: Dict[str, Dict] = {}

        # ---- 溢价指数 K 线聚合 ----
        self._interval_ms = 5 * 60 * 1000  # 5 分钟
        # {symbol: {window_start_ms: {"open","high","low","close","ticks"}}}
        self._premium_windows: Dict[str, Dict[int, Dict]] = defaultdict(dict)

        # ---- 统计 ----
        self.stats = {
            'messages_received': 0,
            'funding_rate_changes': 0,
            'real_funding_rates_fetched': 0,  # 成功获取真实已结算资金费率的次数
            'funding_rate_api_failures': 0,   # API 获取失败次数（回退到预测值）
            'premium_klines_generated': 0,
            'start_time': time.time(),
            'reconnect_count': 0,
        }

        self.running = False

    # ------------------------------------------------------------------
    # 公共方法
    # ------------------------------------------------------------------

    def update_symbols(self, symbols: List[str]):
        """
        更新监控的交易对
        
        注意：更新 symbols 后需要重新连接 WebSocket 以订阅新的流。
        调用此方法后，外部需要调用 stop() 和 start() 来重新连接。
        """
        self.symbols_set = {format_symbol(s) for s in symbols}
        logger.info(
            f"Funding market collector symbols updated: {len(self.symbols_set)} symbols. "
            f"Note: WebSocket will reconnect on next iteration to subscribe new streams."
        )

    async def start(self):
        """启动采集器"""
        self.running = True
        logger.info(
            f"Starting funding market collector for {len(self.symbols_set)} symbols "
            f"via WebSocket @indexPrice + @markPrice streams (real data)"
        )
        await self._websocket_handler()

    async def stop(self):
        """停止采集器"""
        self.running = False
        if self._http_session:
            await self._http_session.close()
            self._http_session = None
        logger.info("Funding market collector stopped")

    def get_current_funding_rates(self) -> Dict[str, Dict]:
        """获取当前所有 symbol 的资金费率快照"""
        return dict(self._funding_state)

    def get_stats(self) -> Dict:
        return {
            **self.stats,
            'symbols_tracked': len(self._funding_state),
            'premium_windows_pending': sum(
                len(w) for w in self._premium_windows.values()
            ),
        }

    # ------------------------------------------------------------------
    # WebSocket 主循环（与 TradeCollector._websocket_handler_single 对齐）
    # ------------------------------------------------------------------

    async def _websocket_handler(self):
        """
        使用组合流订阅各个 symbol 的 @indexPrice 和 @markPrice 流
        格式: /stream?streams=btcusdt@indexPrice/btcusdt@markPrice/ethusdt@indexPrice/ethusdt@markPrice...
        """
        update_speed = config.get("data.mark_price_update_speed", "3s")
        speed_suffix = "@1s" if update_speed == "1s" else ""
        
        # 构建组合流 URL
        streams = []
        for symbol in sorted(self.symbols_set):
            symbol_lower = symbol.lower()
            streams.append(f"{symbol_lower}@indexPrice{speed_suffix}")
            streams.append(f"{symbol_lower}@markPrice{speed_suffix}")
        
        stream_str = "/".join(streams)
        ws_url = f"{self.ws_base}/stream?streams={stream_str}"
        reconnect_count = 0

        # Binance 建议每个连接最多 200 个 streams
        max_streams_per_connection = 200
        if len(streams) > max_streams_per_connection:
            logger.warning(
                f"Stream count ({len(streams)}) exceeds Binance recommended limit "
                f"({max_streams_per_connection}). Consider reducing symbol count or "
                f"implementing batch connections."
            )

        logger.info(
            f"Connecting to real data streams for {len(self.symbols_set)} symbols "
            f"(@indexPrice + @markPrice, {len(streams)} streams total)"
        )

        while self.running:
            try:
                ping_jitter = random.uniform(-2.0, 2.0)
                eff_ping = max(5.0, float(self.ping_interval) + ping_jitter)
                eff_timeout = max(float(self.ping_timeout), eff_ping + 5.0)

                logger.info(
                    f"Connecting to real data streams: {len(streams)} streams "
                    f"(ping={eff_ping:.1f}/{eff_timeout:.1f}s)"
                )

                async with websockets.connect(
                    ws_url,
                    ping_interval=eff_ping,
                    ping_timeout=eff_timeout,
                    close_timeout=10,
                    open_timeout=self.open_timeout,
                ) as websocket:
                    logger.info(f"Real data WebSocket connected ({len(streams)} streams)")
                    reconnect_count = 0

                    while self.running:
                        try:
                            msg_str = await websocket.recv()
                            data = json.loads(msg_str)
                            await self._process_stream_message(data)
                        except ConnectionClosed as e:
                            log_network_error("Real data WebSocket recv", e)
                            logger.info("Real data WebSocket closed by server, reconnecting...")
                            break
                        except Exception as e:
                            log_network_error("Real data message processing", e)
                            break

                    # 防连接风暴：断连后随机延迟再重连，避免与 aggTrade batch 同时涌入
                    if self.running:
                        stagger = random.uniform(2.0, 5.0)
                        logger.info(f"Real data waiting {stagger:.1f}s before reconnect (anti-storm)")
                        await asyncio.sleep(stagger)

            except WebSocketException as e:
                if not self.running:
                    break
                reconnect_count += 1
                self.stats['reconnect_count'] += 1
                delay = min(self.reconnect_delay * (1.5 ** min(reconnect_count - 1, 3)), 30)
                log_network_error("Real data WebSocket connect", e)
                logger.info(f"Real data reconnecting in {delay:.1f}s (#{reconnect_count})")
                await asyncio.sleep(delay)

            except (ConnectionTimeoutError, asyncio.TimeoutError) as e:
                if not self.running:
                    break
                reconnect_count += 1
                self.stats['reconnect_count'] += 1
                delay = min(self.reconnect_delay * (1.5 ** min(reconnect_count - 1, 3)), 30)
                log_network_error("Real data WebSocket timeout", e)
                logger.info(f"Real data timeout, reconnecting in {delay:.1f}s")
                await asyncio.sleep(delay)

            except Exception as e:
                if not self.running:
                    break
                import socket
                reconnect_count += 1
                self.stats['reconnect_count'] += 1
                if isinstance(e, (socket.gaierror, OSError)) and "getaddrinfo" in str(e):
                    delay = min(self.reconnect_delay * (1.2 ** min(reconnect_count - 1, 2)), 10)
                    logger.info(f"Real data DNS error, reconnecting in {delay:.1f}s")
                else:
                    delay = min(self.reconnect_delay * (1.5 ** min(reconnect_count - 1, 3)), 30)
                    log_network_error("Real data WebSocket error", e)
                    logger.info(f"Real data error, reconnecting in {delay:.1f}s")
                await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # 消息处理
    # ------------------------------------------------------------------

    async def _process_stream_message(self, msg):
        """
        处理组合流消息
        消息格式: {"stream": "btcusdt@indexPrice", "data": {...}}
        或: {"stream": "btcusdt@markPrice", "data": {...}}
        """
        if not isinstance(msg, dict):
            return

        stream_name = msg.get('stream', '')
        data = msg.get('data', {})

        if not stream_name or not data:
            return

        self.stats['messages_received'] += 1

        # 解析 stream 名称，提取 symbol 和类型
        # 格式: btcusdt@indexPrice 或 btcusdt@markPrice
        parts = stream_name.split('@')
        if len(parts) < 2:
            return

        symbol_raw = parts[0].upper()
        stream_type = parts[1]

        if symbol_raw not in self.symbols_set:
            return

        symbol = format_symbol(symbol_raw)

        try:
            if stream_type.startswith('indexPrice'):
                # 处理指数价格更新（真实数据）
                await self._process_index_price_update(symbol, data)
            elif stream_type.startswith('markPrice'):
                # 处理标记价格更新（包含资金费率）
                await self._process_mark_price_update(symbol, data)
        except (ValueError, TypeError, KeyError) as e:
            logger.debug(f"Skip invalid {stream_type} message for {symbol}: {e}")

    async def _process_index_price_update(self, symbol: str, data: Dict):
        """处理 @indexPrice 流消息（真实指数价格）"""
        event_time_ms = int(data.get('E', 0))
        index_price = float(data.get('p', 0))  # 指数价格

        if index_price <= 0:
            return

        # 更新指数价格缓存
        if symbol not in self._price_cache:
            self._price_cache[symbol] = {}
        self._price_cache[symbol]['index_price'] = index_price
        self._price_cache[symbol]['last_update'] = event_time_ms

        # 如果有标记价格，计算溢价指数
        cache = self._price_cache.get(symbol, {})
        mark_price = cache.get('mark_price')
        if mark_price and mark_price > 0:
            premium_index = (mark_price - index_price) / index_price
            await self._handle_premium_index(symbol, premium_index, event_time_ms)

    async def _process_mark_price_update(self, symbol: str, data: Dict):
        """处理 @markPrice 流消息（标记价格和资金费率）"""
        event_time_ms = int(data.get('E', 0))
        mark_price = float(data.get('p', 0))  # 标记价格
        funding_rate = float(data.get('r', 0))  # 资金费率
        next_funding_time_ms = int(data.get('T', 0))  # 下次结算时间

        if mark_price <= 0:
            return

        # 更新标记价格缓存
        if symbol not in self._price_cache:
            self._price_cache[symbol] = {}
        self._price_cache[symbol]['mark_price'] = mark_price
        self._price_cache[symbol]['last_update'] = event_time_ms

        # 1) 资金费率变化检测
        await self._handle_funding_rate(
            symbol, funding_rate, next_funding_time_ms,
            mark_price, event_time_ms,
        )

        # 2) 如果有指数价格，计算溢价指数
        cache = self._price_cache.get(symbol, {})
        index_price = cache.get('index_price')
        if index_price and index_price > 0:
            premium_index = (mark_price - index_price) / index_price
            await self._handle_premium_index(symbol, premium_index, event_time_ms)

    # ------------------------------------------------------------------
    # 资金费率
    # ------------------------------------------------------------------

    async def _get_settled_funding_rate_from_api(
        self, symbol: str, funding_time_ms: int
    ) -> Optional[Dict]:
        """
        通过 REST API 获取真实的已结算资金费率
        
        Args:
            symbol: 交易对
            funding_time_ms: 资金费率结算时间（毫秒时间戳）
        
        Returns:
            包含真实资金费率信息的字典，如果获取失败返回 None
            {
                'fundingRate': float,
                'fundingTime': int (ms),
                'markPrice': float
            }
        """
        try:
            if not self._http_session:
                self._http_session = aiohttp.ClientSession()

            # 获取结算时间前后 1 小时的数据，确保能获取到刚结算的记录
            start_time_ms = funding_time_ms - 3600 * 1000  # 结算前 1 小时
            end_time_ms = funding_time_ms + 3600 * 1000    # 结算后 1 小时

            url = f"{self.api_base}/fapi/v1/fundingRate"
            params = {
                'symbol': symbol,
                'startTime': start_time_ms,
                'endTime': end_time_ms,
                'limit': 10  # 获取最多 10 条记录
            }

            data = await safe_http_request(
                self._http_session,
                'GET',
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            )

            if data and isinstance(data, list):
                # 找到最接近结算时间的记录
                best_match = None
                min_diff = float('inf')
                
                for record in data:
                    record_time_ms = int(record.get('fundingTime', 0))
                    if record_time_ms == 0:
                        continue
                    
                    # 计算时间差（取绝对值）
                    time_diff = abs(record_time_ms - funding_time_ms)
                    
                    # 如果时间差在 1 小时内，且更接近结算时间，则使用这条记录
                    if time_diff < 3600 * 1000 and time_diff < min_diff:
                        min_diff = time_diff
                        best_match = {
                            'fundingRate': float(record.get('fundingRate', 0)),
                            'fundingTime': record_time_ms,
                            'markPrice': float(record.get('markPrice', 0)),
                        }

                if best_match:
                    logger.debug(
                        f"Fetched real settled funding rate for {symbol} "
                        f"at {funding_time_ms} (time_diff={min_diff/1000:.1f}s)"
                    )
                    return best_match
                else:
                    logger.warning(
                        f"No settled funding rate found for {symbol} "
                        f"around {funding_time_ms} in API response"
                    )

        except Exception as e:
            log_network_error(f"Fetch settled funding rate for {symbol}", e)
            logger.debug(
                f"Failed to fetch real settled funding rate for {symbol} "
                f"from API, will use WebSocket predicted value: {e}"
            )

        return None

    async def _handle_funding_rate(
        self, symbol: str, rate: float, next_time_ms: int,
        mark_price: float, event_time_ms: int,
    ):
        """
        当 next_funding_time (T) 变化时，表示新的 8 小时结算刚发生。
        此时通过 REST API 获取真实的已结算资金费率，而不是使用 WebSocket 的预测值。
        """
        prev = self._funding_state.get(symbol)

        if prev is None:
            self._funding_state[symbol] = {
                "rate": rate,
                "next_time": next_time_ms,
                "mark_price": mark_price,
            }
            return

        prev_next_time = prev["next_time"]
        prev_rate = prev["rate"]

        # T 发生跳变 → 上一轮费率已结算
        if next_time_ms != prev_next_time and prev_next_time > 0:
            self.stats['funding_rate_changes'] += 1

            if self.on_funding_rate_callback:
                try:
                    # 优先通过 REST API 获取真实的已结算资金费率
                    settled_data = await self._get_settled_funding_rate_from_api(
                        symbol, prev_next_time
                    )

                    if settled_data:
                        # 使用真实的已结算资金费率
                        self.stats['real_funding_rates_fetched'] += 1
                        callback_data = {
                            'fundingRate': settled_data['fundingRate'],
                            'fundingTime': settled_data['fundingTime'],
                            'markPrice': settled_data['markPrice'],
                        }
                        logger.info(
                            f"Using real settled funding rate for {symbol}: "
                            f"{settled_data['fundingRate']:.6f} "
                            f"(time={settled_data['fundingTime']})"
                        )
                    else:
                        # 如果 API 获取失败，回退到 WebSocket 的预测值（作为备选方案）
                        self.stats['funding_rate_api_failures'] += 1
                        callback_data = {
                            'fundingRate': prev_rate,
                            'fundingTime': prev_next_time,
                            'markPrice': mark_price,
                        }
                        logger.warning(
                            f"Using WebSocket predicted funding rate for {symbol}: "
                            f"{prev_rate:.6f} (real settled rate unavailable)"
                        )

                    await self.on_funding_rate_callback(symbol, callback_data)
                except Exception as e:
                    logger.error(
                        f"Funding rate callback error for {symbol}: {e}",
                        exc_info=True,
                    )

        # 更新缓存
        self._funding_state[symbol] = {
            "rate": rate,
            "next_time": next_time_ms,
            "mark_price": mark_price,
        }

    # ------------------------------------------------------------------
    # 溢价指数 5 分钟 K 线
    # ------------------------------------------------------------------

    def _get_window_start(self, ts_ms: int) -> int:
        return ts_ms - (ts_ms % self._interval_ms)

    async def _handle_premium_index(
        self, symbol: str, premium_index: float, event_time_ms: int,
    ):
        window_start_ms = self._get_window_start(event_time_ms)
        windows = self._premium_windows[symbol]

        if window_start_ms not in windows:
            # 新窗口出现 → flush 旧窗口
            await self._flush_completed_premium_windows(symbol, window_start_ms)
            windows[window_start_ms] = {
                "open": premium_index,
                "high": premium_index,
                "low": premium_index,
                "close": premium_index,
                "ticks": 1,
            }
        else:
            w = windows[window_start_ms]
            w["high"] = max(w["high"], premium_index)
            w["low"] = min(w["low"], premium_index)
            w["close"] = premium_index
            w["ticks"] += 1

    async def _flush_completed_premium_windows(
        self, symbol: str, current_window_start_ms: int,
    ):
        """flush 当前窗口之前的所有已完成窗口"""
        windows = self._premium_windows.get(symbol, {})
        completed = [ws for ws in windows if ws < current_window_start_ms]

        for ws in sorted(completed):
            w = windows.pop(ws)
            self.stats['premium_klines_generated'] += 1

            if self.on_premium_index_callback:
                try:
                    open_time = datetime.fromtimestamp(ws / 1000, tz=timezone.utc)
                    close_time = open_time + timedelta(minutes=5) - timedelta(milliseconds=1)

                    kline = {
                        'symbol': format_symbol(symbol),
                        'open_time': pd.Timestamp(open_time),
                        'open': w["open"],
                        'high': w["high"],
                        'low': w["low"],
                        'close': w["close"],
                        'volume': 0.0,
                        'close_time': pd.Timestamp(close_time),
                        'quote_volume': 0.0,
                        'trade_count': w["ticks"],
                        'taker_buy_base_volume': 0.0,
                        'taker_buy_quote_volume': 0.0,
                        'time_lable': int(
                            (open_time.hour * 60 + open_time.minute) / 5
                        ) + 1,
                    }
                    await self.on_premium_index_callback(symbol, kline)
                except Exception as e:
                    logger.error(
                        f"Premium index callback error for {symbol}: {e}",
                        exc_info=True,
                    )

    async def flush_all_premium_windows(self):
        """进程退出时，flush 所有 pending 窗口"""
        for symbol in list(self._premium_windows.keys()):
            windows = self._premium_windows.get(symbol, {})
            for ws in sorted(windows.keys()):
                w = windows[ws]
                if self.on_premium_index_callback:
                    try:
                        open_time = datetime.fromtimestamp(ws / 1000, tz=timezone.utc)
                        close_time = open_time + timedelta(minutes=5) - timedelta(milliseconds=1)
                        kline = {
                            'symbol': format_symbol(symbol),
                            'open_time': pd.Timestamp(open_time),
                            'open': w["open"],
                            'high': w["high"],
                            'low': w["low"],
                            'close': w["close"],
                            'volume': 0.0,
                            'close_time': pd.Timestamp(close_time),
                            'quote_volume': 0.0,
                            'trade_count': w["ticks"],
                            'taker_buy_base_volume': 0.0,
                            'taker_buy_quote_volume': 0.0,
                            'time_lable': int(
                                (open_time.hour * 60 + open_time.minute) / 5
                            ) + 1,
                        }
                        await self.on_premium_index_callback(symbol, kline)
                    except Exception as e:
                        logger.error(
                            f"Premium index flush error for {symbol}: {e}",
                            exc_info=True,
                        )
            self._premium_windows[symbol] = {}
