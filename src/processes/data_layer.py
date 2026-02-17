"""
数据层进程（Process 2）
持续运行，接收逐笔成交数据，聚合生成K线，提供API服务
"""
import asyncio
import signal
import sys
import time
from pathlib import Path
from typing import Set, Optional, Dict, List
from datetime import datetime, timedelta, timezone

import pandas as pd

from ..common.config import config
from ..common.logger import get_logger
from ..common.ipc import IPCClient, MessageType
from ..common.utils import beijing_now
from ..monitoring.performance import get_performance_monitor
from ..data.universe_manager import get_universe_manager
from ..data.collector import TradeCollector
from ..data.mock_collector import MockTradeCollector
from ..data.mock_universe_manager import MockUniverseManager
from ..data.kline_aggregator import KlineAggregator
from ..data.storage import get_data_storage
from ..data.api import get_data_api
from ..data.funding_rate_collector import FundingRateCollector
from ..data.premium_index_collector import get_premium_index_collector

logger = get_logger('data_layer')


class DataLayerProcess:
    """数据层进程"""
    
    def __init__(self):
        # 检查执行模式
        execution_mode = config.get('execution.mode', 'mock')
        
        # Mock模式使用模拟组件
        self.is_mock_mode = (execution_mode == 'mock')
        
        if self.is_mock_mode:
            logger.info("Data layer running in MOCK mode (using mock data, no WebSocket connection)")
            self.universe_manager = MockUniverseManager()
        else:
            self.universe_manager = get_universe_manager()
        
        self.storage = get_data_storage()
        self.kline_aggregator: Optional[KlineAggregator] = None
        self.collector: Optional = None  # 可能是TradeCollector或MockTradeCollector
        self.funding_rate_collector: Optional[FundingRateCollector] = None
        self.premium_index_collector = None  # 溢价指数收集器
        self.data_api = None
        self.ipc_client: Optional[IPCClient] = None
        
        self.running = False
        self.save_interval = config.get('data.save_interval', 60)  # 秒
        self.cleanup_interval = config.get('data.cleanup_interval', 3600)  # 秒
        self.last_save_time = asyncio.get_event_loop().time()
        self.last_cleanup_time = asyncio.get_event_loop().time()
        
        # 任务集合
        self.tasks: Set[asyncio.Task] = set()
        
        # 用于批量保存trades数据的缓冲区
        # 格式: {symbol: [trade1, trade2, ...]}
        self.trades_buffer: Dict[str, List[dict]] = {}
    
    def _add_task_with_error_handler(self, task: asyncio.Task, task_name: str):
        """添加任务并设置异常处理回调"""
        def task_done_callback(t: asyncio.Task):
            try:
                exception = t.exception()
                if exception:
                    logger.error(f"Task '{task_name}' failed with exception: {exception}", exc_info=exception)
                    # 如果关键任务失败，记录错误但不终止进程
                    if task_name in ['collector', 'kline_aggregator']:
                        logger.critical(
                            f"Critical task '{task_name}' failed, process may be unstable. "
                            f"Exception: {exception}. This may cause data collection to stop."
                        )
                        # 记录进程状态以便调试
                        logger.critical(f"Process running state: {self.running}, Active tasks count: {len(self.tasks)}")
                        # 对于关键任务失败，不设置running=False，让进程继续运行以便诊断
            except asyncio.CancelledError:
                # 任务被取消是正常的，不需要记录
                pass
            except Exception as e:
                logger.error(f"Error in task done callback for '{task_name}': {e}", exc_info=True)
            finally:
                # 确保任务从集合中移除（如果已完成）
                try:
                    self.tasks.discard(t)
                except Exception:
                    pass
        
        task.add_done_callback(task_done_callback)
        self.tasks.add(task)
        self.trades_buffer_max_size = config.get('data.trades_buffer_max_size', 1000)  # 每个symbol最多缓存条数
        self.trades_buffer_total_max_size = config.get('data.trades_buffer_total_max_size', 100000)  # 所有symbol的总缓冲区大小限制
        
        # 资金费率采集配置
        self.funding_rate_collect_interval = config.get('data.funding_rate_collect_interval', 28800)  # 8小时
        self.last_funding_rate_collect_time = 0
        self.funding_rate_collect_days = config.get('data.funding_rate_collect_days', 30)  # 采集最近N天的历史数据
        
        # 溢价指数K线采集配置
        self.premium_index_collect_interval = config.get('data.premium_index_collect_interval', 300)  # 5分钟
        self.last_premium_index_collect_time = 0
        self.premium_index_collect_days = config.get('data.premium_index_collect_days', 7)  # 采集最近N天的历史数据

        # data_complete 去重：同一个5分钟窗口（prev_window_end_5min）只通知一次
        # 说明：prev_window_end_5min 是"上一窗口的结束时间 = 当前窗口起始时间"，用它作为窗口唯一键
        self._last_notified_data_complete_5min_end: Optional[int] = None
        # 失败重试节流：同一个窗口内，最多每10秒尝试通知一次（避免刷屏，同时不丢窗口）
        self._last_attempted_data_complete_5min_end: Optional[int] = None
        self._last_attempted_data_complete_monotonic: float = 0.0
        # 数据完整性检查节流：每5秒最多检查一次，避免频繁检查
        self._last_completeness_check_time: float = 0.0
        self._completeness_check_interval: float = 5.0  # 5秒
        
        # 性能监控
        self.performance_monitor = get_performance_monitor()
    
    async def _on_trade_received(self, symbol: str, trade: dict):
        """逐笔成交数据回调：传递给K线聚合器，并缓存用于批量保存"""
        # 传递给K线聚合器
        if self.kline_aggregator:
            await self.kline_aggregator.add_trade(symbol, trade)
        
        # 缓存trades数据用于批量保存
        if symbol not in self.trades_buffer:
            self.trades_buffer[symbol] = []
        
        self.trades_buffer[symbol].append(trade)
        
        # 检查总缓冲区大小，防止内存溢出（更激进的清理策略，目标40%系统内存）
        total_buffer_size = sum(len(buf) for buf in self.trades_buffer.values())
        
        # 如果单个symbol的缓冲区达到阈值，立即保存
        if len(self.trades_buffer[symbol]) >= self.trades_buffer_max_size:
            await self._save_trades_batch(symbol)
        # 如果总缓冲区接近限制（30%），提前清理
        elif total_buffer_size >= int(self.trades_buffer_total_max_size * 0.3):
            # 保存最大的5个缓冲区，防止接近限制
            sorted_symbols = sorted(
                self.trades_buffer.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            for sym, _ in sorted_symbols[:5]:
                if len(self.trades_buffer[sym]) >= int(self.trades_buffer_max_size * 0.2):
                    await self._save_trades_batch(sym)
        # 如果总缓冲区超过限制（50%），强制保存更多
        elif total_buffer_size >= int(self.trades_buffer_total_max_size * 0.5):
            # 强制保存所有缓冲区中最大的几个symbol
            logger.warning(f"Trades buffer total size ({total_buffer_size}) exceeds 50% limit, forcing save...")
            # 按缓冲区大小排序，优先保存最大的
            sorted_symbols = sorted(
                self.trades_buffer.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            # 保存前15个最大的缓冲区（增加保存数量，更及时清理）
            for sym, _ in sorted_symbols[:15]:
                if self.trades_buffer[sym]:
                    await self._save_trades_batch(sym)
        # 如果总缓冲区超过限制，强制保存所有
        elif total_buffer_size >= self.trades_buffer_total_max_size:
            # 强制保存所有缓冲区
            logger.error(f"Trades buffer total size ({total_buffer_size}) exceeds limit, forcing save all...")
            for sym in list(self.trades_buffer.keys()):
                if self.trades_buffer[sym]:
                    await self._save_trades_batch(sym)
    
    async def _on_kline_generated(self, symbol: str, kline):
        """K线生成回调：保存K线数据"""
        with self.performance_monitor.measure('data_layer', 'kline_save', {'symbol': symbol}):
            try:
                # 转换为DataFrame
                if isinstance(kline, dict):
                    kline_df = pd.DataFrame([kline])
                else:
                    kline_df = pd.DataFrame([kline.to_dict()])
                
                # 保存到存储
                self.storage.save_klines(symbol, kline_df)
                
                # 检查是否需要通知策略进程（所有合约的5分钟K线都更新完成）
                # 使用时间节流：每5秒最多检查一次，避免频繁检查
                current_time = time.monotonic()
                if current_time - self._last_completeness_check_time >= self._completeness_check_interval:
                    self._last_completeness_check_time = current_time
                    await self._check_and_notify_data_complete()
                
            except Exception as e:
                logger.error(f"Error in kline callback for {symbol}: {e}", exc_info=True)
    
    async def _check_and_notify_data_complete(self):
        """
        检查所有合约的bar和tran_stats是否都更新完成（包括5min,1h,4h,8h,12h,24h）
        如果是，通知事件协调进程（Process 1）
        
        逻辑：
        1. 检查5分钟bar和tran_stats是否完成（基础数据）
        2. 由于其他周期（1h,4h,8h,12h,24h）是从5分钟数据聚合的，如果5分钟数据完整，其他周期也可以聚合
        3. 检查所有交易对是否都有上一窗口（当前窗口的前一个5分钟窗口）的完整K线
        """
        with self.performance_monitor.measure('data_layer', 'data_completeness_check'):
            try:
                # 获取当前Universe
                universe = self.universe_manager.current_universe
                if not universe:
                    return
                
                # 需要检查的周期列表
                intervals_to_check = ['5min', '1h', '4h', '8h', '12h', '24h']
                interval_minutes_map = {
                    '5min': 5,
                    '1h': 60,
                    '4h': 240,
                    '8h': 480,
                    '12h': 720,
                    '24h': 1440,
                }
                
                # 计算当前时间和各周期的上一窗口结束时间
                current_time = datetime.now(timezone.utc)
                current_timestamp = current_time.timestamp()
                
                # 对于每个周期，计算上一窗口的结束时间
                prev_window_ends = {}
                for interval in intervals_to_check:
                    interval_minutes = interval_minutes_map[interval]
                    interval_seconds = interval_minutes * 60
                    window_start_s = int(current_timestamp // interval_seconds) * interval_seconds
                    window_start_ms = window_start_s * 1000
                    prev_window_ends[interval] = window_start_ms
                
                # all_complete: 所有有数据的交易对是否都完成了上一窗口（所有周期）
                # symbols_with_data: 有K线数据的交易对数量
                # symbols_complete: 完成上一窗口的交易对数量
                all_complete = True
                symbols_with_data = 0
                symbols_complete = 0
                incomplete_symbols = []
                
                for symbol in universe:
                    # 检查5分钟数据（基础数据）
                    latest_kline = self.kline_aggregator.get_latest_kline(symbol)
                    if latest_kline is None:
                        # 没有K线数据（可能是交易量极少，在窗口内没有交易）
                        incomplete_symbols.append(f"{symbol}(no_kline)")
                        continue
                    
                    symbols_with_data += 1
                    
                    # 检查最新K线的结束时间
                    # latest_kline是dict（Polars row转换为named dict）
                    if isinstance(latest_kline, dict):
                        close_time = latest_kline.get('close_time')
                    else:
                        close_time = latest_kline.get('close_time') if hasattr(latest_kline, 'get') else None
                    
                    if close_time is None:
                        all_complete = False
                        incomplete_symbols.append(f"{symbol}(no_close_time)")
                        continue
                    
                    # 转换为毫秒时间戳
                    try:
                        if isinstance(close_time, pd.Timestamp):
                            latest_close_ms = close_time.value // 1_000_000
                        elif isinstance(close_time, datetime):
                            latest_close_ms = int(close_time.timestamp() * 1000)
                        else:
                            # 尝试直接转换
                            latest_close_ms = int(pd.Timestamp(close_time).value // 1_000_000)
                    except Exception as e:
                        all_complete = False
                        incomplete_symbols.append(f"{symbol}(time_convert_error:{e})")
                        continue
                    
                    # 检查5分钟数据是否完成（基础检查）
                    #
                    # prev_window_end_5min 实际上是"当前5分钟窗口的开始时间"(window_start_ms)。
                    # 若最新K线的 close_time 早于该窗口开始，则说明上一窗口还没产出/补齐。
                    prev_window_end_5min = prev_window_ends["5min"]
                    if latest_close_ms < prev_window_end_5min:
                        all_complete = False
                        incomplete_symbols.append(
                            f"{symbol}(5min_not_complete:latest={latest_close_ms}, expected>={prev_window_end_5min})"
                        )
                        continue
                    
                    # 检查tran_stats数据是否完整（bar和tran_stats使用相同的数据源，如果bar完整，tran_stats也应该完整）
                    # 由于bar和tran_stats是在同一个聚合过程中生成的，如果bar完整，tran_stats也应该完整
                    # 这里主要检查bar数据，tran_stats数据会随着bar数据一起完成
                    
                    symbols_complete += 1
                
                # 计算数据完整度阈值：至少95%的交易对有数据且完整
                # 这样可以处理交易量极少的交易对（可能没有交易就没有K线）
                completeness_threshold = config.get('data.completeness_threshold', 0.95)  # 默认95%阈值
                min_symbols_with_data = int(len(universe) * completeness_threshold)
                
                # 检查条件：
                # 1. 有数据的交易对数量达到阈值（至少95%）
                # 2. 所有有数据的交易对都完成了上一窗口（all_complete为True，包括5min,1h,4h,8h,12h,24h）
                # 注意：由于其他周期（1h,4h,8h,12h,24h）是从5分钟数据聚合的，如果5分钟数据完整，其他周期也可以聚合
                # 所以主要检查5分钟数据是否完整即可
                if symbols_with_data >= min_symbols_with_data and all_complete:
                    # ===== data_complete 去重：同一个5min窗口只通知一次 =====
                    prev_window_end_5min = prev_window_ends.get('5min')
                    if prev_window_end_5min is not None and self._last_notified_data_complete_5min_end == prev_window_end_5min:
                        # 已经通知过该窗口，避免刷屏触发策略
                        return

                    # ===== 同窗口失败重试节流：避免IPC短暂不可用时丢触发 =====
                    retry_throttle = config.get('data.data_complete_retry_throttle', 10.0)
                    if prev_window_end_5min is not None and self._last_attempted_data_complete_5min_end == prev_window_end_5min:
                        if time.monotonic() - self._last_attempted_data_complete_monotonic < retry_throttle:
                            return

                    # 通知事件协调进程（只通知有数据的交易对）
                    symbols_with_complete_data = [
                        symbol for symbol in universe
                        if symbol not in [s.split('(')[0] for s in incomplete_symbols]
                    ]
                    
                    if self.ipc_client:
                        # 发送通知也做轻量重试，避免临时网络/IPC抖动导致刷屏
                        if prev_window_end_5min is not None:
                            self._last_attempted_data_complete_5min_end = prev_window_end_5min
                            self._last_attempted_data_complete_monotonic = time.monotonic()

                        last_err: Optional[Exception] = None
                        retry_attempts = config.get('data.data_complete_retry_attempts', 3)
                        retry_delays = config.get('data.data_complete_retry_delays', [0.05, 0.2, 0.5])
                        for attempt in range(retry_attempts):
                            try:
                                await self.ipc_client.send_data_complete(
                                    current_time,
                                    symbols_with_complete_data
                                )
                                # 成功后记录本窗口已通知，确保只通知一次
                                if prev_window_end_5min is not None:
                                    self._last_notified_data_complete_5min_end = prev_window_end_5min
                                logger.info(
                                    f"Notified data complete (5min,1h,4h,8h,12h,24h) for {len(symbols_with_complete_data)}/{len(universe)} symbols "
                                    f"({symbols_with_data} have data, {len(incomplete_symbols)} incomplete) at {current_time}"
                                )
                                break
                            except Exception as e:
                                last_err = e
                                if attempt < retry_attempts - 1:
                                    delay = retry_delays[attempt] if attempt < len(retry_delays) else retry_delays[-1]
                                    await asyncio.sleep(delay)
                        else:
                            # 失败不标记 notified：后续仍会按"10秒节流"重试，避免丢窗口触发
                            logger.error(f"Failed to notify data complete after retries: {last_err}", exc_info=True)
                else:
                    # 记录调试信息（定期记录，帮助诊断问题）
                    if incomplete_symbols:
                        # 使用时间戳和窗口ID来节流日志，避免刷屏
                        prev_window_end_5min = prev_window_ends.get('5min', 0)
                        
                        # 初始化节流状态
                        if not hasattr(self, '_last_log_window'):
                            self._last_log_window = None
                            self._last_log_time = 0
                        
                        current_time_monotonic = time.monotonic()
                        
                        # 节流策略：
                        # 1. 同一窗口只记录一次
                        # 2. 不同窗口至少间隔5秒才记录
                        # 3. 如果incomplete_symbols数量变化（增加或减少），立即记录
                        should_log = False
                        
                        if self._last_log_window != prev_window_end_5min:
                            # 窗口变化，检查时间间隔
                            if current_time_monotonic - self._last_log_time >= 5.0:
                                should_log = True
                        elif current_time_monotonic - self._last_log_time >= 30.0:
                            # 同一窗口，但超过30秒，记录一次（避免长时间无日志）
                            should_log = True
                        
                        if should_log:
                            window_start_5min = prev_window_end_5min
                            logger.info(
                                f"Data completeness check: {symbols_with_data}/{len(universe)} symbols have data. "
                                f"Incomplete: {len(incomplete_symbols)} symbols. "
                                f"Current window: {window_start_5min}, Prev window end: {prev_window_end_5min}. "
                                f"Sample incomplete: {incomplete_symbols[:5] if len(incomplete_symbols) > 5 else incomplete_symbols}"
                            )
                            self._last_log_window = prev_window_end_5min
                            self._last_log_time = current_time_monotonic
                        
            except Exception as e:
                logger.error(f"Error checking data completeness: {e}", exc_info=True)
    
    async def _save_trades_batch(self, symbol: str):
        """批量保存trades数据"""
        try:
            if symbol not in self.trades_buffer or not self.trades_buffer[symbol]:
                return
            
            # 获取缓冲区中的数据
            trades_list = self.trades_buffer[symbol]
            if not trades_list:
                return
            
            # 转换为DataFrame
            trades_df = pd.DataFrame(trades_list)
            
            # 保存到存储
            self.storage.save_trades(symbol, trades_df)
            
            # 清空缓冲区
            self.trades_buffer[symbol] = []
            
            logger.debug(f"Saved {len(trades_list)} trades for {symbol}")
            
        except Exception as e:
            logger.error(f"Failed to save trades batch for {symbol}: {e}", exc_info=True)
    
    async def _periodic_save(self):
        """定期保存数据"""
        while self.running:
            try:
                await asyncio.sleep(self.save_interval)
                
                if not self.running:
                    break
                
                # 刷新所有待处理的K线
                if self.kline_aggregator:
                    await self.kline_aggregator.flush_pending()
                    
                    # 检查并生成无交易的窗口K线
                    universe = self.universe_manager.current_universe
                    if universe:
                        await self.kline_aggregator.check_and_generate_empty_windows(list(universe))
                
                # 定期保存聚合好的K线
                if self.kline_aggregator:
                    all_klines = self.kline_aggregator.get_all_klines()
                    for symbol, df in all_klines.items():
                        if not df.empty:
                            self.storage.save_klines(symbol, df)
                    
                    logger.debug(f"Periodic save completed, {len(all_klines)} symbols")
                
                # 定期保存trades数据（保存所有缓冲区中的数据）
                for symbol in list(self.trades_buffer.keys()):
                    if self.trades_buffer[symbol]:
                        await self._save_trades_batch(symbol)
                
                # 清理空的缓冲区条目，释放内存
                empty_symbols = [
                    sym for sym, buf in self.trades_buffer.items() if not buf
                ]
                for sym in empty_symbols:
                    del self.trades_buffer[sym]
                
                self.last_save_time = asyncio.get_event_loop().time()
                
                # 定期清理旧数据
                current_time = asyncio.get_event_loop().time()
                if current_time - self.last_cleanup_time >= self.cleanup_interval:
                    max_days = config.get('data.max_history_days', 30)
                    self.storage.cleanup_old_data(days=max_days)
                    self.last_cleanup_time = current_time
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic save: {e}", exc_info=True)
    
    async def _update_symbols(self):
        """定期更新Universe并更新采集器的交易对列表"""
        while self.running:
            try:
                # 等待Universe管理器更新
                check_interval = config.get('data.universe_check_interval', 300)  # 默认5分钟
                await asyncio.sleep(check_interval)
                
                if not self.running:
                    break
                
                # 获取最新的Universe
                new_universe = self.universe_manager.current_universe
                if new_universe and self.collector:
                    new_symbols = list(new_universe)
                    old_symbols = set(self.collector.symbols)
                    
                    if set(new_symbols) != old_symbols:
                        logger.info(f"Universe updated: {len(old_symbols)} -> {len(new_symbols)} symbols")
                        await self.collector.update_symbols(new_symbols)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating symbols: {e}", exc_info=True)
    
    async def start(self):
        """启动数据层进程"""
        if self.running:
            logger.warning("Data layer process is already running")
            return
        
        logger.info("Starting data layer process...")
        self.running = True
        
        # 0. 检查并设置数据层端点（支持 testnet 回退到 live）
        execution_mode = config.get('execution.mode', 'mock')
        if execution_mode == 'testnet':
            try:
                # 异步检查端点并设置回退
                api_base = await config.get_binance_api_base_for_data_layer_async()
                ws_base = await config.get_binance_ws_base_for_data_layer_async()
                logger.info(f"Data layer endpoints: API={api_base}, WS={ws_base}")
            except Exception as e:
                logger.warning(f"Failed to check endpoint connectivity, using default: {e}")
        
        # 1. 初始化Universe管理器并加载Universe
        try:
            universe = self.universe_manager.load_universe()
            if not universe:
                if self.is_mock_mode:
                    logger.info("No universe found, using mock universe...")
                else:
                    logger.warning("No universe found, will fetch from Binance...")
                await self.universe_manager.update_universe()
                universe = self.universe_manager.current_universe
            
            logger.info(f"Loaded universe: {len(universe)} symbols")
        except Exception as e:
            logger.error(f"Failed to load universe: {e}", exc_info=True)
            return
        
        # 2. 启动Universe管理器的定时更新（dry-run模式下MockUniverseManager不需要定时更新）
        if not self.is_mock_mode:
            universe_task = asyncio.create_task(self.universe_manager.start())
            self._add_task_with_error_handler(universe_task, "universe_manager")
        else:
            # dry-run模式下，MockUniverseManager的start()只是设置标志
            await self.universe_manager.start()
        
        # 3. 初始化K线聚合器
        self.kline_aggregator = KlineAggregator(
            interval_minutes=5,
            on_kline_callback=self._on_kline_generated
        )
        
        # 4. 初始化数据API
        self.data_api = get_data_api(self.kline_aggregator)
        
        # 5. 初始化IPC客户端（连接到事件协调进程）
        try:
            self.ipc_client = IPCClient()
            await self.ipc_client.connect()
            logger.info("Connected to event coordinator via IPC")
        except Exception as e:
            logger.warning(f"Failed to connect to event coordinator: {e}")
            logger.warning("Will continue without IPC notification")
        
        # 6. 初始化逐笔成交采集器（dry-run模式下使用模拟采集器）
        symbols = list(universe) if universe else []
        if symbols:
            if self.is_mock_mode:
                # 使用模拟采集器，不连接WebSocket
                self.collector = MockTradeCollector(
                    symbols=symbols,
                    on_trade_callback=self._on_trade_received
                )
                logger.info(f"Started MOCK trade collector for {len(symbols)} symbols (no WebSocket connection)")
            else:
                # 使用真实采集器，连接WebSocket
                self.collector = TradeCollector(
                    symbols=symbols,
                    on_trade_callback=self._on_trade_received
                )
                logger.info(f"Started trade collector for {len(symbols)} symbols (connecting to WebSocket)")
            
            collector_task = asyncio.create_task(self.collector.start())
            self._add_task_with_error_handler(collector_task, "collector")
        else:
            logger.error("No symbols to collect")
            return
        
        # 7. 启动定期保存任务
        save_task = asyncio.create_task(self._periodic_save())
        self._add_task_with_error_handler(save_task, "periodic_save")
        
        # 8. 启动符号更新任务
        symbol_update_task = asyncio.create_task(self._update_symbols())
        self._add_task_with_error_handler(symbol_update_task, "update_symbols")
        
        # 9. 初始化资金费率采集器（非mock模式）
        if not self.is_mock_mode:
            # 使用异步方法获取正确的API端点（支持testnet回退到live）
            api_base = await config.get_binance_api_base_for_data_layer_async()
            self.funding_rate_collector = FundingRateCollector(api_base=api_base)
            logger.info(f"Funding rate collector initialized with API: {api_base}")
            # 启动资金费率采集任务
            funding_rate_task = asyncio.create_task(self._periodic_collect_funding_rates())
            self._add_task_with_error_handler(funding_rate_task, "funding_rate_collector")
            # 立即执行一次初始采集
            asyncio.create_task(self._collect_funding_rates_initial())
        
        # 10. 初始化溢价指数K线采集器（非mock模式）
        if not self.is_mock_mode:
            self.premium_index_collector = get_premium_index_collector()
            # 启动溢价指数K线采集任务
            premium_index_task = asyncio.create_task(self._periodic_collect_premium_index())
            self._add_task_with_error_handler(premium_index_task, "premium_index_collector")
            # 立即执行一次初始采集
            asyncio.create_task(self._collect_premium_index_initial())
        
        # 11. 启动数据完整性检查和自动补全任务（每1小时检查一次）
        if not self.is_mock_mode:
            data_integrity_task = asyncio.create_task(self._check_and_complete_data())
            self._add_task_with_error_handler(data_integrity_task, "data_integrity_check")
        
        # 12. 启动定期内存清理任务（每30分钟清理一次）
        memory_cleanup_task = asyncio.create_task(self._periodic_memory_cleanup())
        self._add_task_with_error_handler(memory_cleanup_task, "memory_cleanup")
        
        logger.info("Data layer process started successfully")
    
    async def stop(self):
        """停止数据层进程"""
        logger.info("Stopping data layer process...")
        self.running = False
        
        # 停止所有任务
        tasks_to_cancel = list(self.tasks)  # 创建副本，避免在迭代时修改集合
        for task in tasks_to_cancel:
            try:
                task.cancel()
            except Exception as e:
                logger.error(f"Error cancelling task during stop: {e}", exc_info=True)
        
        if tasks_to_cancel:
            try:
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error gathering tasks during stop: {e}", exc_info=True)
        
        # 停止采集器
        if self.collector:
            await self.collector.stop()
        
        # 停止Universe管理器
        if self.universe_manager:
            await self.universe_manager.stop()
        
        # 保存所有待处理的K线
        if self.kline_aggregator:
            await self.kline_aggregator.flush_pending()
            
            # 保存所有K线数据
            all_klines = self.kline_aggregator.get_all_klines()
            for symbol, df in all_klines.items():
                if not df.empty:
                    self.storage.save_klines(symbol, df)
        
        # 保存所有待处理的trades数据
        for symbol in list(self.trades_buffer.keys()):
            if self.trades_buffer[symbol]:
                await self._save_trades_batch(symbol)
        
        # 断开IPC连接
        if self.ipc_client:
            await self.ipc_client.disconnect()
        
        logger.info("Data layer process stopped")
    
    async def _collect_funding_rates_initial(self):
        """初始采集资金费率数据（采集最近N天的历史数据）"""
        try:
            if not self.funding_rate_collector:
                return
            
            universe = self.universe_manager.current_universe
            if not universe:
                logger.warning("No universe available for initial funding rate collection")
                return
            
            logger.info(f"Starting initial funding rate collection for {len(universe)} symbols...")
            
            # 计算时间范围（最近N天）
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=self.funding_rate_collect_days)
            
            # 检查已存在的数据，只采集缺失的部分
            symbols = list(universe)
            symbols_to_collect = []
            existing_count = 0
            
            for symbol in symbols:
                # 检查该交易对是否已有数据（检查最近3天的数据，确保有最新数据）
                # 使用最近3天而不是整个时间范围，确保总是采集最新数据
                recent_end_time = datetime.now(timezone.utc)
                recent_start_time = recent_end_time - timedelta(days=3)
                existing_data = self.storage.load_funding_rates(symbol, recent_start_time, recent_end_time)
                if existing_data.empty or len(existing_data) < 3:  # 最近3天至少需要3条数据（每天1条，3天3条）
                    symbols_to_collect.append(symbol)
                    logger.debug(f"{symbol} needs funding rate collection: recent_data={len(existing_data)}")
                else:
                    existing_count += 1
                    logger.debug(f"{symbol} has recent funding rate data: {len(existing_data)} records")
            
            if existing_count > 0:
                logger.info(f"Found existing funding rate data for {existing_count} symbols, skipping collection")
            
            if not symbols_to_collect:
                logger.info("All symbols already have funding rate data, skipping initial collection")
                self.last_funding_rate_collect_time = asyncio.get_event_loop().time()
                return
            
            # 批量采集（降低并发数，避免API限流）
            max_concurrent = config.get('data.history_collect_max_concurrent', 1)  # 降低并发数，避免限流
            logger.info(f"Starting bulk funding rate collection for {len(symbols_to_collect)} symbols (missing data) with max_concurrent={max_concurrent}")
            
            funding_rates_map = await self.funding_rate_collector.fetch_funding_rates_bulk(
                symbols=symbols_to_collect,
                start_time=start_time,
                end_time=end_time,
                max_concurrent=max_concurrent
            )
            
            # 保存到存储
            saved_count = 0
            empty_count = 0
            error_count = 0
            total_records = 0
            
            for symbol, df in funding_rates_map.items():
                if not df.empty:
                    try:
                        self.storage.save_funding_rates(symbol, df)
                        saved_count += 1
                        total_records += len(df)
                        logger.debug(f"Saved {len(df)} funding rates for {symbol}")
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Failed to save funding rates for {symbol}: {e}", exc_info=True)
                else:
                    empty_count += 1
                    logger.warning(f"No funding rate data fetched for {symbol}")
            
            logger.info(
                f"Initial funding rate collection completed: "
                f"saved={saved_count}/{len(symbols_to_collect)}, existing={existing_count}, empty={empty_count}, errors={error_count}, "
                f"total_records={total_records}"
            )
            self.last_funding_rate_collect_time = asyncio.get_event_loop().time()
            
        except Exception as e:
            logger.error(f"Error in initial funding rate collection: {e}", exc_info=True)
    
    async def _periodic_collect_funding_rates(self):
        """定期采集资金费率数据（每8小时一次）"""
        while self.running:
            try:
                # 等待8小时
                await asyncio.sleep(self.funding_rate_collect_interval)
                
                if not self.running:
                    break
                
                if not self.funding_rate_collector:
                    continue
                
                universe = self.universe_manager.current_universe
                if not universe:
                    continue
                
                logger.info(f"Starting periodic funding rate collection for {len(universe)} symbols...")
                
                # 采集最近24小时的数据（确保覆盖最新的资金费率）
                end_time = datetime.now(timezone.utc)
                start_time = end_time - timedelta(days=1)
                
                symbols = list(universe)
                max_concurrent = config.get('data.history_collect_max_concurrent', 2)  # 降低并发数，避免限流
                funding_rates_map = await self.funding_rate_collector.fetch_funding_rates_bulk(
                    symbols=symbols,
                    start_time=start_time,
                    end_time=end_time,
                    max_concurrent=max_concurrent
                )
                
                # 保存到存储
                saved_count = 0
                empty_count = 0
                error_count = 0
                total_records = 0
                
                for symbol, df in funding_rates_map.items():
                    if not df.empty:
                        try:
                            self.storage.save_funding_rates(symbol, df)
                            saved_count += 1
                            total_records += len(df)
                        except Exception as e:
                            error_count += 1
                            logger.error(f"Failed to save funding rates for {symbol}: {e}", exc_info=True)
                    else:
                        empty_count += 1
                
                logger.info(
                    f"Periodic funding rate collection completed: "
                    f"saved={saved_count}/{len(symbols)}, empty={empty_count}, errors={error_count}, "
                    f"total_records={total_records}"
                )
                
                logger.info(f"Periodic funding rate collection completed: {saved_count}/{len(symbols)} symbols updated")
                self.last_funding_rate_collect_time = asyncio.get_event_loop().time()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic funding rate collection: {e}", exc_info=True)
                retry_delay = config.get('data.funding_rate_collect_interval', 28800)  # 出错后等待配置的间隔再重试
                await asyncio.sleep(min(retry_delay, 3600))  # 最多等待1小时
    
    async def _collect_premium_index_initial(self):
        """初始采集溢价指数K线数据（采集最近N天的历史数据）"""
        try:
            if not self.premium_index_collector:
                return
            
            universe = self.universe_manager.current_universe
            if not universe:
                logger.warning("No universe available for initial premium index collection")
                return
            
            logger.info(f"Starting initial premium index collection for {len(universe)} symbols...")
            
            # 计算时间范围（最近N天）
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=self.premium_index_collect_days)
            
            # 检查已存在的数据，只采集缺失的部分
            symbols = list(universe)
            symbols_to_collect = []
            existing_count = 0
            
            for symbol in symbols:
                # 检查该交易对是否已有数据（检查最近3天的数据，确保有最新数据）
                recent_end_time = datetime.now(timezone.utc)
                recent_start_time = recent_end_time - timedelta(days=3)
                existing_data = self.storage.load_premium_index_klines(symbol, recent_start_time, recent_end_time)
                # 5分钟K线，3天应该有约864条（3*24*12），但考虑到可能不完整，至少需要100条
                if existing_data.empty or len(existing_data) < 100:
                    symbols_to_collect.append(symbol)
                    logger.debug(f"{symbol} needs premium index collection: recent_data={len(existing_data)}")
                else:
                    existing_count += 1
                    logger.debug(f"{symbol} has recent premium index data: {len(existing_data)} records")
            
            if existing_count > 0:
                logger.info(f"Found existing premium index data for {existing_count} symbols, skipping collection")
            
            if not symbols_to_collect:
                logger.info("All symbols already have premium index data, skipping initial collection")
                self.last_premium_index_collect_time = asyncio.get_event_loop().time()
                return
            
            # 批量采集（降低并发数，避免API限流）
            max_concurrent = config.get('data.history_collect_max_concurrent', 5)
            logger.info(f"Starting bulk premium index collection for {len(symbols_to_collect)} symbols (missing data) with max_concurrent={max_concurrent}")
            
            premium_index_map = await self.premium_index_collector.fetch_premium_index_klines_bulk(
                symbols=symbols_to_collect,
                start_time=start_time,
                end_time=end_time,
                interval='5m',
                max_concurrent=max_concurrent
            )
            
            # 保存到存储
            saved_count = 0
            error_count = 0
            for symbol, df in premium_index_map.items():
                if not df.empty:
                    try:
                        self.storage.save_premium_index_klines(symbol, df)
                        saved_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Failed to save premium index klines for {symbol}: {e}", exc_info=True)
            
            logger.info(f"Initial premium index collection completed: saved={saved_count}/{len(symbols_to_collect)}, existing={existing_count}, errors={error_count}")
            self.last_premium_index_collect_time = asyncio.get_event_loop().time()
            
        except Exception as e:
            logger.error(f"Error in initial premium index collection: {e}", exc_info=True)
    
    async def _periodic_collect_premium_index(self):
        """定期采集溢价指数K线数据（每5分钟一次）"""
        while self.running:
            try:
                # 等待5分钟
                await asyncio.sleep(self.premium_index_collect_interval)
                
                if not self.running:
                    break
                
                if not self.premium_index_collector:
                    continue
                
                universe = self.universe_manager.current_universe
                if not universe:
                    continue
                
                logger.info(f"Starting periodic premium index collection for {len(universe)} symbols...")
                
                # 采集最近24小时的数据（确保覆盖最新的溢价指数K线）
                end_time = datetime.now(timezone.utc)
                start_time = end_time - timedelta(days=1)
                
                symbols = list(universe)
                max_concurrent = config.get('data.history_collect_max_concurrent', 5)
                premium_index_map = await self.premium_index_collector.fetch_premium_index_klines_bulk(
                    symbols=symbols,
                    start_time=start_time,
                    end_time=end_time,
                    interval='5m',
                    max_concurrent=max_concurrent
                )
                
                # 保存到存储
                saved_count = 0
                for symbol, df in premium_index_map.items():
                    if not df.empty:
                        try:
                            self.storage.save_premium_index_klines(symbol, df)
                            saved_count += 1
                        except Exception as e:
                            logger.error(f"Failed to save premium index klines for {symbol}: {e}", exc_info=True)
                
                logger.info(f"Periodic premium index collection completed: {saved_count}/{len(symbols)} symbols updated")
                self.last_premium_index_collect_time = asyncio.get_event_loop().time()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic premium index collection: {e}", exc_info=True)
                retry_delay = config.get('data.premium_index_collect_interval', 300)  # 出错后等待配置的间隔再重试
                await asyncio.sleep(retry_delay)
    
    async def _check_and_complete_data(self):
        """定期检查数据完整性并自动补全缺失数据（每1小时检查一次）"""
        # 等待初始采集完成后再开始检查
        await asyncio.sleep(3600)  # 等待1小时
        
        while self.running:
            try:
                # 等待1小时
                await asyncio.sleep(3600)  # 1小时 = 3600秒
                
                if not self.running:
                    break
                
                logger.info("Starting data integrity check and auto-completion...")
                
                universe = self.universe_manager.current_universe
                if not universe:
                    logger.warning("No universe available for data integrity check")
                    continue
                
                symbols = list(universe)
                end_time = datetime.now(timezone.utc)
                
                # 1. 检查资金费率数据完整性
                if self.funding_rate_collector:
                    missing_funding_rates = []
                    recent_start_time = end_time - timedelta(days=3)
                    
                    for symbol in symbols:
                        existing_data = self.storage.load_funding_rates(symbol, recent_start_time, end_time)
                        if existing_data.empty or len(existing_data) < 3:
                            missing_funding_rates.append(symbol)
                    
                    if missing_funding_rates:
                        logger.warning(f"Found {len(missing_funding_rates)} symbols with missing funding rate data, auto-completing...")
                        # 采集最近7天的数据以确保完整性
                        start_time = end_time - timedelta(days=7)
                        max_concurrent = config.get('data.history_collect_max_concurrent', 2)
                        funding_rates_map = await self.funding_rate_collector.fetch_funding_rates_bulk(
                            symbols=missing_funding_rates,
                            start_time=start_time,
                            end_time=end_time,
                            max_concurrent=max_concurrent
                        )
                        
                        saved_count = 0
                        for symbol, df in funding_rates_map.items():
                            if not df.empty:
                                try:
                                    self.storage.save_funding_rates(symbol, df)
                                    saved_count += 1
                                except Exception as e:
                                    logger.error(f"Failed to save funding rates for {symbol} during auto-completion: {e}")
                        
                        logger.info(f"Auto-completed funding rate data for {saved_count}/{len(missing_funding_rates)} symbols")
                
                # 2. 检查溢价指数K线数据完整性
                if self.premium_index_collector:
                    missing_premium_index = []
                    recent_start_time = end_time - timedelta(days=3)
                    
                    for symbol in symbols:
                        existing_data = self.storage.load_premium_index_klines(symbol, recent_start_time, end_time)
                        if existing_data.empty or len(existing_data) < 100:
                            missing_premium_index.append(symbol)
                    
                    if missing_premium_index:
                        logger.warning(f"Found {len(missing_premium_index)} symbols with missing premium index data, auto-completing...")
                        # 采集最近7天的数据以确保完整性
                        start_time = end_time - timedelta(days=7)
                        max_concurrent = config.get('data.history_collect_max_concurrent', 5)
                        premium_index_map = await self.premium_index_collector.fetch_premium_index_klines_bulk(
                            symbols=missing_premium_index,
                            start_time=start_time,
                            end_time=end_time,
                            interval='5m',
                            max_concurrent=max_concurrent
                        )
                        
                        saved_count = 0
                        for symbol, df in premium_index_map.items():
                            if not df.empty:
                                try:
                                    self.storage.save_premium_index_klines(symbol, df)
                                    saved_count += 1
                                except Exception as e:
                                    logger.error(f"Failed to save premium index klines for {symbol} during auto-completion: {e}")
                        
                        logger.info(f"Auto-completed premium index data for {saved_count}/{len(missing_premium_index)} symbols")
                
                logger.info("Data integrity check completed")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in data integrity check: {e}", exc_info=True)
                # 出错后等待30分钟再重试
                await asyncio.sleep(1800)
    
    async def _periodic_memory_cleanup(self):
        """定期内存清理任务：清理不再使用的数据，释放内存（目标2-2.5GB）"""
        cleanup_interval = config.get('data.memory_cleanup_interval', 600)  # 10分钟（更频繁的清理）
        
        while self.running:
            try:
                await asyncio.sleep(cleanup_interval)
                
                if not self.running:
                    break
                
                logger.debug("Starting periodic memory cleanup...")
                
                # 1. 清理K线聚合器中的旧数据
                if self.kline_aggregator:
                    # 清理不再活跃的symbol的pending_trades
                    universe = self.universe_manager.current_universe
                    if universe:
                        active_symbols = set(universe)
                        # 清理不在universe中的symbol的pending_trades
                        inactive_symbols = set(self.kline_aggregator.pending_trades.keys()) - active_symbols
                        for symbol in inactive_symbols:
                            if symbol in self.kline_aggregator.pending_trades:
                                del self.kline_aggregator.pending_trades[symbol]
                        if inactive_symbols:
                            logger.debug(f"Cleaned up pending_trades for {len(inactive_symbols)} inactive symbols")
                        
                        # 清理不在universe中的symbol的klines（但保留最近的数据以防需要）
                        inactive_klines = set(self.kline_aggregator.klines.keys()) - active_symbols
                        for symbol in inactive_klines:
                            if symbol in self.kline_aggregator.klines:
                                del self.kline_aggregator.klines[symbol]
                        if inactive_klines:
                            logger.debug(f"Cleaned up klines for {len(inactive_klines)} inactive symbols")
                
                # 2. 清理trades_buffer中不再活跃的symbol
                if universe:
                    active_symbols = set(universe)
                    inactive_trades_buffer = set(self.trades_buffer.keys()) - active_symbols
                    for symbol in inactive_trades_buffer:
                        if symbol in self.trades_buffer:
                            # 先保存再删除
                            if self.trades_buffer[symbol]:
                                await self._save_trades_batch(symbol)
                            del self.trades_buffer[symbol]
                    if inactive_trades_buffer:
                        logger.debug(f"Cleaned up trades_buffer for {len(inactive_trades_buffer)} inactive symbols")
                
                # 3. 清理数据API的内存缓存（如果存在）
                if self.data_api:
                    # 触发缓存清理（如果缓存超过限制）
                    self.data_api._cleanup_cache_if_needed()
                
                # 4. 强制垃圾回收（Python的gc）
                import gc
                collected = gc.collect()
                if collected > 0:
                    logger.debug(f"Garbage collection freed {collected} objects")
                
                # 5. 清理空的trades_buffer条目（减少字典大小）
                empty_buffers = [sym for sym, buf in self.trades_buffer.items() if not buf]
                for sym in empty_buffers:
                    del self.trades_buffer[sym]
                if empty_buffers:
                    logger.debug(f"Cleaned up {len(empty_buffers)} empty trades_buffer entries")
                
                logger.debug("Periodic memory cleanup completed")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic memory cleanup: {e}", exc_info=True)
                # 出错后等待较短时间再重试
                await asyncio.sleep(300)  # 5分钟后重试
    
    def print_stats(self):
        """打印统计信息"""
        if self.collector:
            collector_stats = self.collector.get_stats()
            logger.info(f"Collector stats: {collector_stats}")
        
        if self.kline_aggregator:
            aggregator_stats = self.kline_aggregator.get_stats()
            logger.info(f"Aggregator stats: {aggregator_stats}")


async def main():
    """主函数"""
    process = DataLayerProcess()
    shutdown_event = asyncio.Event()
    
    # 信号处理 - 使用事件而不是直接创建任务
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        shutdown_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await process.start()
        
        # 保持运行，同时监听关闭事件
        loop_iteration = 0
        while process.running:
            loop_iteration += 1
            try:
                # 使用wait_for来同时等待sleep和shutdown事件
                # 检查事件循环是否已关闭
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError as e:
                    logger.critical(f"Event loop is closed or not running: {e}")
                    break
                
                done, pending = await asyncio.wait(
                    [
                        asyncio.create_task(asyncio.sleep(1)),
                        asyncio.create_task(shutdown_event.wait())
                    ],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # 处理已完成的任务（检查是否有异常）
                for task in done:
                    try:
                        # 获取任务结果，如果有异常会在这里抛出
                        task.result()
                    except asyncio.CancelledError:
                        # 任务被取消是正常的
                        pass
                    except Exception as e:
                        # 记录任务中的异常，但不终止主循环
                        logger.error(f"Error in main loop task: {e}", exc_info=True)
                
                # 取消未完成的任务
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as e:
                        # 记录取消任务时的异常
                        logger.error(f"Error cancelling task in main loop: {e}", exc_info=True)
                
                # 如果收到关闭信号，退出循环
                if shutdown_event.is_set():
                    logger.info("Shutdown signal received, stopping...")
                    break
                
                # 定期打印统计（每5分钟）
                if hasattr(process, 'last_stats_time'):
                    if asyncio.get_event_loop().time() - process.last_stats_time > 300:
                        process.print_stats()
                        process.last_stats_time = asyncio.get_event_loop().time()
                else:
                    process.last_stats_time = asyncio.get_event_loop().time()
                    
            except asyncio.CancelledError:
                logger.info("Main loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in main loop (iteration {loop_iteration}): {e}", exc_info=True)
                # 出错后等待1秒再继续，避免快速循环
                await asyncio.sleep(1)
        
        # 如果循环退出，记录原因和详细信息
        if not process.running:
            logger.info("Main loop exited because process.running is False")
        else:
            logger.warning("Main loop exited but process.running is still True - this should not happen")
            # 记录更多调试信息
            logger.warning(f"Active tasks count: {len(process.tasks)}")
            logger.warning(f"Shutdown event is set: {shutdown_event.is_set()}")
            # 检查关键组件状态
            if process.collector:
                collector_running = getattr(process.collector, 'running', None)
                logger.warning(f"Collector running state: {collector_running}")
            if process.kline_aggregator:
                logger.warning("Kline aggregator exists")
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Data layer process error: {e}", exc_info=True)
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        try:
            await process.stop()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)


if __name__ == "__main__":
    import pandas as pd
    try:
        # 使用asyncio.run，但添加全局异常处理
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        # 捕获asyncio.run可能抛出的未处理异常
        logger.critical(f"Unhandled exception in asyncio.run: {e}", exc_info=True)
        import traceback
        logger.critical(f"Full traceback: {traceback.format_exc()}")
        raise  # 重新抛出以便系统可以检测到进程异常退出
