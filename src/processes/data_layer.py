"""
数据层进程（Process 2）
持续运行，接收逐笔成交数据，聚合生成K线，提供API服务
"""
import asyncio
import signal
import sys
import time
import os
from pathlib import Path
from typing import Set, Optional, Dict, List
from datetime import datetime, timedelta, timezone

import pandas as pd
try:
    import psutil
except ImportError:
    psutil = None

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
        # 使用time.time()而不是asyncio.get_event_loop().time()，因为初始化时可能还没有事件循环
        self.last_save_time = time.time()
        self.last_cleanup_time = time.time()
        
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
        # 内存清理状态：用于避免force_rebuild进入“每轮都触发”的正反馈
        self._cleanup_counter = 0
        self._last_memory = 0.0
        self._high_growth_streak = 0
        self._force_rebuild_cooldown = 0
    
    async def _on_trade_received(self, symbol: str, trade: dict):
        """逐笔成交数据回调：传递给K线聚合器，并缓存用于批量保存"""
        # 传递给K线聚合器
        if self.kline_aggregator:
            await self.kline_aggregator.add_trade(symbol, trade)
        
        # 缓存trades数据用于批量保存
        if symbol not in self.trades_buffer:
            self.trades_buffer[symbol] = []
        
        self.trades_buffer[symbol].append(trade)
        
        # 检查总缓冲区大小，防止内存溢出（更激进的清理策略，提前清理）
        total_buffer_size = sum(len(buf) for buf in self.trades_buffer.values())
        
        # 如果单个symbol的缓冲区达到阈值，立即保存
        if len(self.trades_buffer[symbol]) >= self.trades_buffer_max_size:
            await self._save_trades_batch(symbol)
        # 如果总缓冲区接近限制（10%），提前清理（更激进的阈值）
        elif total_buffer_size >= int(self.trades_buffer_total_max_size * 0.1):
            # 保存最大的5个缓冲区，防止接近限制
            sorted_symbols = sorted(
                self.trades_buffer.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            for sym, _ in sorted_symbols[:5]:
                if len(self.trades_buffer[sym]) >= int(self.trades_buffer_max_size * 0.2):
                    await self._save_trades_batch(sym)
        # 如果总缓冲区超过限制（20%），强制保存更多
        elif total_buffer_size >= int(self.trades_buffer_total_max_size * 0.2):
            # 强制保存所有缓冲区中最大的几个symbol
            logger.warning(f"Trades buffer total size ({total_buffer_size}) exceeds 20% limit, forcing save...")
            # 按缓冲区大小排序，优先保存最大的
            sorted_symbols = sorted(
                self.trades_buffer.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            # 保存前10个最大的缓冲区（更及时清理）
            for sym, _ in sorted_symbols[:10]:
                if self.trades_buffer[sym]:
                    await self._save_trades_batch(sym)
        # 如果总缓冲区超过限制（40%），强制保存更多
        elif total_buffer_size >= int(self.trades_buffer_total_max_size * 0.4):
            # 强制保存所有缓冲区中最大的symbol
            logger.error(f"Trades buffer total size ({total_buffer_size}) exceeds 40% limit, forcing save...")
            sorted_symbols = sorted(
                self.trades_buffer.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            # 保存前20个最大的缓冲区
            for sym, _ in sorted_symbols[:20]:
                if self.trades_buffer[sym]:
                    await self._save_trades_batch(sym)
        # 如果总缓冲区超过限制（70%），强制保存更多
        elif total_buffer_size >= int(self.trades_buffer_total_max_size * 0.7):
            # 强制保存所有缓冲区中最大的symbol
            logger.error(f"Trades buffer total size ({total_buffer_size}) exceeds 70% limit, forcing save...")
            sorted_symbols = sorted(
                self.trades_buffer.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )
            # 保存前30个最大的缓冲区
            for sym, _ in sorted_symbols[:30]:
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
                # 修复内存泄漏：直接使用polars DataFrame，避免pandas转换
                # 减少DataFrame转换次数，降低内存占用
                import polars as pl
                if isinstance(kline, dict):
                    # 直接创建polars DataFrame，避免pandas中间转换
                    df_pl = pl.DataFrame([kline])
                    self.storage.save_klines(symbol, df_pl)
                    # 立即清理引用
                    del df_pl
                else:
                    # 如果是其他类型，转换为dict
                    kline_dict = kline.to_dict() if hasattr(kline, 'to_dict') else dict(kline)
                    df_pl = pl.DataFrame([kline_dict])
                    self.storage.save_klines(symbol, df_pl)
                    # 立即清理引用
                    del df_pl
                    del kline_dict
                
                # 修复内存泄漏：定期触发GC，避免DataFrame累积
                # 每100个K线触发一次GC（降低频率，避免性能影响）
                if not hasattr(self, '_kline_save_count'):
                    self._kline_save_count = 0
                self._kline_save_count += 1
                if self._kline_save_count % 100 == 0:
                    import gc
                    gc.collect()
                
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
            
            # 获取缓冲区中的数据（引用），保存后原地清理，减少对象残留
            trades_list = self.trades_buffer[symbol]
            if not trades_list:
                return
            
            saved_count = len(trades_list)

            # 转换为DataFrame
            trades_df = pd.DataFrame(trades_list)
            
            # 保存到存储
            self.storage.save_trades(symbol, trades_df)
            
            # 原地清空缓冲区并释放临时对象引用
            trades_list.clear()
            self.trades_buffer[symbol] = trades_list
            del trades_df
            
            logger.debug(f"Saved {saved_count} trades for {symbol}")
            
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
                # 优化：直接使用polars DataFrame，避免转换为pandas DataFrame造成内存泄漏
                # 极端优化：分批保存，避免一次性处理所有symbol造成内存峰值
                if self.kline_aggregator:
                    saved_count = 0
                    symbols_to_save = list(self.kline_aggregator.klines.keys())
                    batch_size = 50  # 每批处理50个symbol，避免内存峰值
                    
                    for i in range(0, len(symbols_to_save), batch_size):
                        batch_symbols = symbols_to_save[i:i + batch_size]
                        for symbol in batch_symbols:
                            if symbol not in self.kline_aggregator.klines:
                                continue
                            df_pl = self.kline_aggregator.klines[symbol]
                            if not df_pl.is_empty():
                                try:
                                    # 直接使用polars DataFrame保存，避免转换为pandas
                                    # storage.save_klines内部会处理polars DataFrame
                                    # 优化：只在需要时转换为pandas（storage内部会处理）
                                    self.storage.save_klines(symbol, df_pl)
                                    saved_count += 1
                                except Exception as e:
                                    logger.error(f"Failed to save klines for {symbol}: {e}", exc_info=True)
                        
                        # 每批处理后强制垃圾回收，释放内存
                        import gc
                        gc.collect()
                    
                    logger.debug(f"Periodic save completed, {saved_count} symbols")
                
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
                
                self.last_save_time = time.time()
                
                # 定期清理旧数据
                current_time = time.time()
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
            
            # 保存所有K线数据（直接使用polars DataFrame，避免内存泄漏）
            for symbol, df_pl in self.kline_aggregator.klines.items():
                if not df_pl.is_empty():
                    # 直接使用polars DataFrame，storage内部会处理
                    self.storage.save_klines(symbol, df_pl)
        
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
                self.last_funding_rate_collect_time = time.time()
                return
            
            # 分批采集与保存，避免一次性在内存中持有全部symbols的历史DataFrame
            max_concurrent = config.get('data.history_collect_max_concurrent', 1)
            batch_size = config.get('data.history_collect_batch_size', 25)
            logger.info(
                f"Starting bulk funding rate collection for {len(symbols_to_collect)} symbols "
                f"(missing data) with max_concurrent={max_concurrent}, batch_size={batch_size}"
            )

            saved_count = 0
            empty_count = 0
            error_count = 0
            total_records = 0

            for i in range(0, len(symbols_to_collect), batch_size):
                batch_symbols = symbols_to_collect[i:i + batch_size]
                funding_rates_map = await self.funding_rate_collector.fetch_funding_rates_bulk(
                    symbols=batch_symbols,
                    start_time=start_time,
                    end_time=end_time,
                    max_concurrent=max_concurrent,
                )

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

                # 及时释放批次对象，降低RSS上升斜率
                del funding_rates_map
                import gc
                gc.collect()
            
            logger.info(
                f"Initial funding rate collection completed: "
                f"saved={saved_count}/{len(symbols_to_collect)}, existing={existing_count}, empty={empty_count}, errors={error_count}, "
                f"total_records={total_records}"
            )
            self.last_funding_rate_collect_time = time.time()
            
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
                max_concurrent = config.get('data.history_collect_max_concurrent', 2)
                batch_size = config.get('data.history_collect_batch_size', 25)

                # 分批采集，避免全量map长时间驻留内存
                saved_count = 0
                empty_count = 0
                error_count = 0
                total_records = 0

                for i in range(0, len(symbols), batch_size):
                    batch_symbols = symbols[i:i + batch_size]
                    funding_rates_map = await self.funding_rate_collector.fetch_funding_rates_bulk(
                        symbols=batch_symbols,
                        start_time=start_time,
                        end_time=end_time,
                        max_concurrent=max_concurrent
                    )

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

                    del funding_rates_map
                    import gc
                    gc.collect()
                
                logger.info(
                    f"Periodic funding rate collection completed: "
                    f"saved={saved_count}/{len(symbols)}, empty={empty_count}, errors={error_count}, "
                    f"total_records={total_records}"
                )
                self.last_funding_rate_collect_time = time.time()
                
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
                self.last_premium_index_collect_time = time.time()
                return
            
            # 分批采集（避免全量结果同时驻留内存）
            max_concurrent = config.get('data.history_collect_max_concurrent', 5)
            batch_size = config.get('data.history_collect_batch_size', 25)
            logger.info(
                f"Starting bulk premium index collection for {len(symbols_to_collect)} symbols "
                f"(missing data) with max_concurrent={max_concurrent}, batch_size={batch_size}"
            )

            saved_count = 0
            error_count = 0
            for i in range(0, len(symbols_to_collect), batch_size):
                batch_symbols = symbols_to_collect[i:i + batch_size]
                premium_index_map = await self.premium_index_collector.fetch_premium_index_klines_bulk(
                    symbols=batch_symbols,
                    start_time=start_time,
                    end_time=end_time,
                    interval='5m',
                    max_concurrent=max_concurrent
                )
                for symbol, df in premium_index_map.items():
                    if not df.empty:
                        try:
                            self.storage.save_premium_index_klines(symbol, df)
                            saved_count += 1
                        except Exception as e:
                            error_count += 1
                            logger.error(f"Failed to save premium index klines for {symbol}: {e}", exc_info=True)
                del premium_index_map
                import gc
                gc.collect()
            
            logger.info(f"Initial premium index collection completed: saved={saved_count}/{len(symbols_to_collect)}, existing={existing_count}, errors={error_count}")
            self.last_premium_index_collect_time = time.time()
            
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
                batch_size = config.get('data.history_collect_batch_size', 25)

                # 分批采集与保存，降低内存峰值
                saved_count = 0
                for i in range(0, len(symbols), batch_size):
                    batch_symbols = symbols[i:i + batch_size]
                    premium_index_map = await self.premium_index_collector.fetch_premium_index_klines_bulk(
                        symbols=batch_symbols,
                        start_time=start_time,
                        end_time=end_time,
                        interval='5m',
                        max_concurrent=max_concurrent
                    )

                    for symbol, df in premium_index_map.items():
                        if not df.empty:
                            try:
                                self.storage.save_premium_index_klines(symbol, df)
                                saved_count += 1
                            except Exception as e:
                                logger.error(f"Failed to save premium index klines for {symbol}: {e}", exc_info=True)

                    del premium_index_map
                    import gc
                    gc.collect()
                
                logger.info(f"Periodic premium index collection completed: {saved_count}/{len(symbols)} symbols updated")
                self.last_premium_index_collect_time = time.time()
                
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
                        batch_size = config.get('data.history_collect_batch_size', 25)
                        saved_count = 0
                        for i in range(0, len(missing_funding_rates), batch_size):
                            batch_symbols = missing_funding_rates[i:i + batch_size]
                            funding_rates_map = await self.funding_rate_collector.fetch_funding_rates_bulk(
                                symbols=batch_symbols,
                                start_time=start_time,
                                end_time=end_time,
                                max_concurrent=max_concurrent
                            )
                            for symbol, df in funding_rates_map.items():
                                if not df.empty:
                                    try:
                                        self.storage.save_funding_rates(symbol, df)
                                        saved_count += 1
                                    except Exception as e:
                                        logger.error(f"Failed to save funding rates for {symbol} during auto-completion: {e}")
                            del funding_rates_map
                            import gc
                            gc.collect()
                        
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
                        batch_size = config.get('data.history_collect_batch_size', 25)
                        saved_count = 0
                        for i in range(0, len(missing_premium_index), batch_size):
                            batch_symbols = missing_premium_index[i:i + batch_size]
                            premium_index_map = await self.premium_index_collector.fetch_premium_index_klines_bulk(
                                symbols=batch_symbols,
                                start_time=start_time,
                                end_time=end_time,
                                interval='5m',
                                max_concurrent=max_concurrent
                            )
                            for symbol, df in premium_index_map.items():
                                if not df.empty:
                                    try:
                                        self.storage.save_premium_index_klines(symbol, df)
                                        saved_count += 1
                                    except Exception as e:
                                        logger.error(f"Failed to save premium index klines for {symbol} during auto-completion: {e}")
                            del premium_index_map
                            import gc
                            gc.collect()
                        
                        logger.info(f"Auto-completed premium index data for {saved_count}/{len(missing_premium_index)} symbols")
                
                logger.info("Data integrity check completed")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in data integrity check: {e}", exc_info=True)
                # 出错后等待30分钟再重试
                await asyncio.sleep(1800)
    
    async def _periodic_memory_cleanup(self):
        """定期内存清理任务：清理不再使用的数据，释放内存（目标1.5-2GB）"""
        cleanup_interval = config.get('data.memory_cleanup_interval', 30)  # 30秒（更频繁的清理）
        memory_warning_threshold = 1500  # MB，超过此值发出警告并强制清理
        memory_critical_threshold = 2000  # MB，超过此值强制清理
        
        while self.running:
            try:
                await asyncio.sleep(cleanup_interval)
                
                if not self.running:
                    break
                
                # 记录清理前的内存使用情况
                mem_before = 0
                mem_after = 0
                if psutil:
                    try:
                        process = psutil.Process(os.getpid())
                        mem_before = process.memory_info().rss / 1024 / 1024  # MB
                    except Exception:
                        pass
                
                # 内存告警：如果超过阈值，强制清理
                if mem_before > memory_critical_threshold:
                    logger.error(
                        f"Memory usage ({mem_before:.2f}MB) exceeds critical threshold ({memory_critical_threshold}MB), "
                        f"forcing aggressive cleanup..."
                    )
                elif mem_before > memory_warning_threshold:
                    logger.warning(
                        f"Memory usage ({mem_before:.2f}MB) exceeds warning threshold ({memory_warning_threshold}MB), "
                        f"performing cleanup..."
                    )
                
                logger.debug("Starting periodic memory cleanup...")
                
                # 1. 清理K线聚合器中的旧数据
                if self.kline_aggregator:
                    # 清理不再活跃的symbol的pending_trades
                    universe = self.universe_manager.current_universe
                    if universe:
                        active_symbols = set(universe)
                        # 清理不在universe中的symbol的pending_trades
                        # 优化：使用list()创建副本，避免在迭代时修改字典
                        all_pending_symbols = list(self.kline_aggregator.pending_trades.keys())
                        inactive_symbols = set(all_pending_symbols) - active_symbols
                        inactive_cleaned = 0
                        for symbol in inactive_symbols:
                            if symbol not in self.kline_aggregator.pending_trades:
                                continue
                            # 清理defaultdict中的条目，避免内存泄漏
                            # 修复内存泄漏：确保所有pending_trades被完全清理
                            windows = self.kline_aggregator.pending_trades.pop(symbol, None)
                            if windows:
                                # 清理所有窗口的trades列表
                                for window_start, window_trades in list(windows.items()):
                                    if window_trades:
                                        window_trades.clear()
                                        del window_trades
                                windows.clear()
                                del windows
                                inactive_cleaned += 1
                        
                        # 修复内存泄漏：清理所有空字典条目（即使symbol在universe中）
                        # 这是关键修复：清理pending_trades中的空字典条目
                        empty_symbols = []
                        for symbol in list(self.kline_aggregator.pending_trades.keys()):
                            if symbol not in self.kline_aggregator.pending_trades:
                                continue
                            if not self.kline_aggregator.pending_trades[symbol]:
                                # 空字典，删除整个条目
                                del self.kline_aggregator.pending_trades[symbol]
                                empty_symbols.append(symbol)
                        if empty_symbols:
                            logger.info(
                                f"Cleaned up {len(empty_symbols)} empty pending_trades entries "
                                f"(memory leak fix)"
                            )
                        if inactive_cleaned > 0:
                            logger.info(
                                f"Cleaned up pending_trades for {inactive_cleaned} inactive symbols "
                                f"(removed {len(inactive_symbols)} symbols from pending_trades)"
                            )
                            # 强制垃圾回收，帮助释放内存
                            import gc
                            gc.collect()
                        
                        # 增强：强制清理所有symbol的旧pending窗口（即使symbol在universe中）
                        # 清理超过时间窗口的pending_trades（保留最近2个窗口）
                        current_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                        current_window_start_ms = self.kline_aggregator._get_window_start(current_time_ms)
                        max_pending_windows = config.get('data.kline_aggregator_max_pending_windows', 2)
                        
                        pending_cleaned_count = 0
                        for symbol in list(self.kline_aggregator.pending_trades.keys()):
                            if symbol not in self.kline_aggregator.pending_trades:
                                continue
                            windows = self.kline_aggregator.pending_trades[symbol]
                            if not windows:
                                continue
                            
                            # 找出所有旧的窗口（超过max_pending_windows个）
                            window_starts = sorted(windows.keys())
                            if len(window_starts) > max_pending_windows:
                                # 保留最新的max_pending_windows个窗口，删除最旧的
                                # 注意：window_starts[:-max_pending_windows] 会保留除了最后N个之外的所有窗口（这是要删除的）
                                windows_to_remove = window_starts[:len(window_starts) - max_pending_windows]
                                for window_start in windows_to_remove:
                                    if window_start not in windows:
                                        continue  # 可能已被其他线程删除
                                    # 先尝试聚合，如果失败则直接删除
                                    # 修复内存泄漏：确保窗口和trades列表被完全清理
                                    try:
                                        await self.kline_aggregator._aggregate_window(symbol, window_start)
                                        # 聚合成功，窗口已被_aggregate_window内部的pop移除
                                        pending_cleaned_count += 1
                                    except Exception as e:
                                        # 如果聚合失败，直接删除并清理
                                        if window_start in windows:
                                            window_trades = windows.pop(window_start, None)
                                            if window_trades:
                                                window_trades.clear()
                                                del window_trades
                                        pending_cleaned_count += 1
                                        logger.debug(
                                            f"Failed to aggregate window {window_start} for {symbol}, "
                                            f"deleted directly: {e}"
                                        )
                        
                        if pending_cleaned_count > 0:
                            logger.info(
                                f"Cleaned up {pending_cleaned_count} old pending windows "
                                f"(kept {max_pending_windows} latest windows per symbol)"
                            )

                        # 额外内存保护：限制pending_trades总成交条数，防止总量持续累积
                        # 高波动下，单窗口交易可能很大；这里优先保留每个窗口最新成交。
                        pending_total_soft_limit = config.get(
                            "data.kline_aggregator_pending_trades_total_soft_limit", 200000
                        )
                        pending_trim_per_window = config.get(
                            "data.kline_aggregator_pending_trades_trim_per_window", 1500
                        )
                        total_pending_trades = 0
                        for windows in self.kline_aggregator.pending_trades.values():
                            for trades in windows.values():
                                total_pending_trades += len(trades)

                        if (
                            total_pending_trades > pending_total_soft_limit
                            or mem_before > memory_warning_threshold
                        ):
                            trimmed_trades = 0
                            for symbol in list(self.kline_aggregator.pending_trades.keys()):
                                if symbol not in self.kline_aggregator.pending_trades:
                                    continue
                                windows = self.kline_aggregator.pending_trades[symbol]
                                for window_start in list(windows.keys()):
                                    if window_start not in windows:
                                        continue
                                    trades_list = windows[window_start]
                                    if len(trades_list) > pending_trim_per_window:
                                        remove_count = len(trades_list) - pending_trim_per_window
                                        del trades_list[:remove_count]
                                        trimmed_trades += remove_count
                            if trimmed_trades > 0:
                                logger.warning(
                                    "Pending trades guard triggered: "
                                    f"total_pending_trades={total_pending_trades}, "
                                    f"trimmed={trimmed_trades}, "
                                    f"per_window_limit={pending_trim_per_window}"
                                )
                                import gc
                                gc.collect()
                        
                        # 清理不在universe中的symbol的klines
                        # 修复内存泄漏：确保所有DataFrame被完全释放
                        # 优化：使用list()创建副本，避免在迭代时修改字典
                        all_kline_symbols = list(self.kline_aggregator.klines.keys())
                        inactive_klines = set(all_kline_symbols) - active_symbols
                        for symbol in inactive_klines:
                            if symbol in self.kline_aggregator.klines:
                                df = self.kline_aggregator.klines.pop(symbol, None)
                                if df is not None:
                                    # 确保DataFrame被完全释放
                                    del df
                        if inactive_klines:
                            logger.debug(f"Cleaned up klines for {len(inactive_klines)} inactive symbols")
                            # 强制垃圾回收，帮助释放内存
                            import gc
                            gc.collect()
                        
                        # 重新实现：使用kline_aggregator的强制清理方法，确保真正释放内存
                        max_klines = config.get('data.kline_aggregator_max_klines', 288)
                        
                        # 关键修复：限制force_rebuild触发频率，避免“重建->内存抬升->再次重建”的正反馈
                        memory_growth_rate = (
                            mem_before - self._last_memory if self._last_memory > 0 else 0
                        )
                        self._last_memory = mem_before
                        self._cleanup_counter += 1

                        growth_threshold_mb = 15.0
                        if memory_growth_rate > growth_threshold_mb:
                            self._high_growth_streak += 1
                        else:
                            self._high_growth_streak = 0

                        force_rebuild = False
                        # 周期性重建：降低频率，避免每次cleanup都全量重建
                        if self._cleanup_counter % 12 == 0:
                            force_rebuild = True
                        # 高增长触发：需要连续增长且达到一定内存规模，并且不在冷却期
                        elif (
                            mem_before > 800
                            and self._high_growth_streak >= 3
                            and self._force_rebuild_cooldown <= 0
                        ):
                            force_rebuild = True

                        if force_rebuild:
                            # 冷却窗口：避免连续多次强制重建
                            self._force_rebuild_cooldown = 4
                        elif self._force_rebuild_cooldown > 0:
                            self._force_rebuild_cooldown -= 1
                        
                        # 调用kline_aggregator的强制清理方法
                        cleaned_count, total_trimmed = self.kline_aggregator.force_cleanup_klines(
                            max_klines, force_rebuild_all=force_rebuild
                        )
                        
                        if cleaned_count > 0:
                            if total_trimmed > 0:
                                logger.info(
                                    f"Force cleaned klines: {cleaned_count} symbols processed, "
                                    f"removed {total_trimmed} klines total (max_klines={max_klines}, "
                                    f"force_rebuild={force_rebuild})"
                                )
                            else:
                                if force_rebuild:
                                    logger.info(
                                        f"Force cleaned klines: {cleaned_count} symbols rebuilt to release memory fragments "
                                        f"(max_klines={max_klines}, memory_growth_rate={memory_growth_rate:.2f}MB)"
                                    )
                                else:
                                    logger.debug(
                                        f"Force cleaned klines: {cleaned_count} symbols checked, "
                                        f"no trimming needed (all within limit, max_klines={max_klines})"
                                    )
                            # 强制GC，确保内存被释放
                            import gc
                            gc.collect()
                        
                        # 清理统计信息中不再活跃的symbol
                        # 修复内存泄漏：确保统计信息被及时清理
                        self.kline_aggregator._cleanup_stats()
                        
                        # 强制清理统计信息：即使不在清理间隔内，也确保统计信息不会无限积累
                        # 如果统计信息中的symbol数量远大于活跃symbol数量，强制清理
                        stats_symbols = set()
                        for stat_key in ["trades_processed", "klines_generated", "last_kline_time"]:
                            if stat_key in self.kline_aggregator.stats:
                                stats_dict = self.kline_aggregator.stats[stat_key]
                                if isinstance(stats_dict, dict):
                                    stats_symbols.update(stats_dict.keys())
                        
                        if len(stats_symbols) > len(active_symbols) * 1.5:  # 如果统计信息中的symbol数量超过活跃symbol的1.5倍，强制清理
                            logger.debug(f"Force cleaning stats: stats_symbols={len(stats_symbols)}, active_symbols={len(active_symbols)}")
                            for stat_key in ["trades_processed", "klines_generated", "last_kline_time"]:
                                if stat_key in self.kline_aggregator.stats:
                                    stats_dict = self.kline_aggregator.stats[stat_key]
                                    if isinstance(stats_dict, dict):
                                        inactive_symbols = set(stats_dict.keys()) - active_symbols
                                        for symbol in inactive_symbols:
                                            stats_dict.pop(symbol, None)
                            # 强制垃圾回收
                            import gc
                            gc.collect()
                
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
                    
                    # 增强：强制清理所有symbol的trades_buffer（即使symbol在universe中）
                    # 如果总缓冲区超过阈值，强制保存所有
                    total_buffer_size = sum(len(buf) for buf in self.trades_buffer.values())
                    if total_buffer_size > 0:
                        # 如果总缓冲区超过50%限制，强制保存所有
                        if total_buffer_size >= int(self.trades_buffer_total_max_size * 0.5):
                            logger.warning(f"Trades buffer total size ({total_buffer_size}) exceeds 50% limit, forcing save all during cleanup...")
                            for sym in list(self.trades_buffer.keys()):
                                if self.trades_buffer[sym]:
                                    await self._save_trades_batch(sym)
                
                # 3. 清理数据API的内存缓存（如果存在）
                if self.data_api:
                    # 触发缓存清理（如果缓存超过限制）
                    self.data_api._cleanup_cache_if_needed()
                    
                    # 修复内存泄漏：强制清理缓存访问时间中不再使用的条目
                    with self.data_api._cache_lock:
                        cache_symbols = set(self.data_api._memory_cache.keys())
                        access_time_keys = set(self.data_api._cache_access_time.keys())
                        stale_keys = access_time_keys - cache_symbols
                        if stale_keys:
                            for key in stale_keys:
                                del self.data_api._cache_access_time[key]
                            logger.debug(f"Cleaned up {len(stale_keys)} stale cache access time entries")
                
                # 4. 强制垃圾回收（Python的gc）
                # 修复内存泄漏：更激进的GC策略，确保内存被真正释放
                import gc
                # 如果内存超过阈值，执行多次GC
                gc_rounds = 5 if mem_before > memory_warning_threshold else 2
                total_collected = 0
                for _ in range(gc_rounds):
                    collected = gc.collect()
                    total_collected += collected
                if total_collected > 0:
                    logger.debug(f"Garbage collection freed {total_collected} objects ({gc_rounds} rounds)")
                
                # 如果内存仍然很高，执行更激进的GC（包括清理循环引用）
                if mem_before > memory_warning_threshold:
                    # 清理所有代（generation）的垃圾
                    for generation in range(3):
                        gc.collect(generation)
                
                # 修复内存泄漏：强制清理pending_trades中的空字典（关键修复）
                # 在GC之前清理，确保空字典被完全删除
                if self.kline_aggregator:
                    empty_pending_count = 0
                    for symbol in list(self.kline_aggregator.pending_trades.keys()):
                        if symbol not in self.kline_aggregator.pending_trades:
                            continue
                        if not self.kline_aggregator.pending_trades[symbol]:
                            del self.kline_aggregator.pending_trades[symbol]
                            empty_pending_count += 1
                    if empty_pending_count > 0:
                        logger.info(
                            f"Memory cleanup: removed {empty_pending_count} empty pending_trades entries "
                            f"(critical memory leak fix)"
                        )
                        # 再次GC，确保释放内存
                        gc.collect()
                
                # 如果内存仍然很高，强制清理所有可能的缓存
                if mem_before > memory_critical_threshold and self.kline_aggregator:
                    logger.warning("Memory still high after cleanup, performing aggressive trim...")
                    # 强制trim所有klines到50%限制
                    # 修复：当max_klines=1时，不应该进行aggressive trim（因为trim后就没有数据了）
                    max_klines = config.get('data.kline_aggregator_max_klines', 288)
                    if max_klines > 1:
                        aggressive_limit = max(1, int(max_klines * 0.5))  # 至少保留1条
                        aggressive_cleaned = 0
                        for symbol in list(self.kline_aggregator.klines.keys()):
                            if symbol in self.kline_aggregator.klines:
                                df = self.kline_aggregator.klines[symbol]
                                if not df.is_empty() and len(df) > aggressive_limit:
                                    # 修复内存泄漏：确保旧DataFrame被完全释放
                                    old_df = self.kline_aggregator.klines[symbol]
                                    trimmed_df = old_df.tail(aggressive_limit).clone()
                                    self.kline_aggregator.klines[symbol] = trimmed_df
                                    if old_df is not None and old_df is not trimmed_df:
                                        del old_df
                                    del trimmed_df
                                    aggressive_cleaned += 1
                        if aggressive_cleaned > 0:
                            logger.warning(f"Aggressive trim: cleaned {aggressive_cleaned} symbols to {aggressive_limit} klines")
                        # 再次GC
                        gc.collect()
                    elif max_klines == 1:
                        # 当max_klines=1时，如果某个symbol超过1条，trim到1条
                        aggressive_cleaned = 0
                        for symbol in list(self.kline_aggregator.klines.keys()):
                            if symbol in self.kline_aggregator.klines:
                                df = self.kline_aggregator.klines[symbol]
                                if not df.is_empty() and len(df) > 1:
                                    old_df = self.kline_aggregator.klines[symbol]
                                    trimmed_df = old_df.tail(1).clone()
                                    self.kline_aggregator.klines[symbol] = trimmed_df
                                    if old_df is not None and old_df is not trimmed_df:
                                        del old_df
                                    del trimmed_df
                                    aggressive_cleaned += 1
                        if aggressive_cleaned > 0:
                            logger.warning(f"Aggressive trim: cleaned {aggressive_cleaned} symbols to 1 kline")
                        # 再次GC
                        gc.collect()
                
                # 5. 清理空的trades_buffer条目（减少字典大小）
                empty_buffers = [sym for sym, buf in self.trades_buffer.items() if not buf]
                for sym in empty_buffers:
                    del self.trades_buffer[sym]
                if empty_buffers:
                    logger.debug(f"Cleaned up {len(empty_buffers)} empty trades_buffer entries")
                
                # 6. 清理performance_monitor的旧数据（如果启用）
                # 修复内存泄漏：确保performance_monitor不会无限积累数据
                if self.performance_monitor and self.performance_monitor.enabled:
                    try:
                        # 触发自动清理
                        self.performance_monitor.cleanup_old_rounds()
                    except Exception as e:
                        logger.debug(f"Failed to cleanup performance monitor: {e}")
                
                # 记录清理后的内存使用情况和统计信息
                if psutil:
                    try:
                        process = psutil.Process(os.getpid())
                        mem_after = process.memory_info().rss / 1024 / 1024  # MB
                    except Exception:
                        mem_after = 0
                mem_freed = mem_before - mem_after
                
                # 统计各组件内存使用情况
                stats = {
                    'trades_buffer_size': sum(len(buf) for buf in self.trades_buffer.values()),
                    'trades_buffer_symbols': len(self.trades_buffer),
                }
                if self.kline_aggregator:
                    stats['pending_trades_symbols'] = len(self.kline_aggregator.pending_trades)
                    stats['pending_trades_windows'] = sum(len(windows) for windows in self.kline_aggregator.pending_trades.values())
                    stats['klines_symbols'] = len(self.kline_aggregator.klines)
                    stats['klines_total'] = sum(len(df) for df in self.kline_aggregator.klines.values() if not df.is_empty())
                    # 计算平均每个symbol的klines数量
                    if stats['klines_symbols'] > 0:
                        stats['avg_klines_per_symbol'] = round(stats['klines_total'] / stats['klines_symbols'], 1)
                if self.data_api:
                    with self.data_api._cache_lock:
                        stats['cache_symbols'] = len(self.data_api._memory_cache)
                        stats['cache_total_klines'] = sum(len(df) for df in self.data_api._memory_cache.values() if not df.is_empty())
                
                # 检查清理效果和内存趋势
                max_klines = config.get('data.kline_aggregator_max_klines', 288)
                klines_total = stats.get('klines_total', 0)
                klines_symbols = stats.get('klines_symbols', 0)
                expected_max_klines = klines_symbols * max_klines if klines_symbols > 0 else 0
                
                # 如果klines_total超过预期，发出警告
                if klines_total > expected_max_klines * 1.1:  # 超过10%容差
                    logger.warning(
                        f"Klines total ({klines_total}) exceeds expected limit ({expected_max_klines:.0f}), "
                        f"some symbols may not be trimmed properly. "
                        f"Average klines per symbol: {klines_total/klines_symbols:.1f} (max: {max_klines})"
                    )
                
                # 如果清理效果不明显（释放内存<1MB且内存仍在增长），触发更激进的清理
                if mem_freed < 1.0 and mem_before > 200:  # 内存超过200MB但释放很少
                    logger.warning(
                        f"Memory cleanup had limited effect: freed only {mem_freed:.2f}MB. "
                        f"Current memory: {mem_after:.2f}MB. "
                        f"Triggering aggressive cleanup..."
                    )
                    # 触发更激进的清理：trim所有klines到40%限制
                    if self.kline_aggregator:
                        max_klines = config.get('data.kline_aggregator_max_klines', 288)
                        # 修复：当max_klines=1时，不应该进行aggressive cleanup（因为trim后就没有数据了）
                        if max_klines > 1:
                            aggressive_limit = max(1, int(max_klines * 0.4))  # 更激进的40%
                            aggressive_cleaned = 0
                            for symbol in list(self.kline_aggregator.klines.keys()):
                                if symbol in self.kline_aggregator.klines:
                                    df = self.kline_aggregator.klines[symbol]
                                    if not df.is_empty() and len(df) > aggressive_limit:
                                        old_df = self.kline_aggregator.klines[symbol]
                                        trimmed_df = old_df.tail(aggressive_limit).clone()
                                        self.kline_aggregator.klines[symbol] = trimmed_df
                                        if old_df is not None and old_df is not trimmed_df:
                                            del old_df
                                        del trimmed_df
                                        aggressive_cleaned += 1
                            if aggressive_cleaned > 0:
                                logger.info(f"Aggressive cleanup: trimmed {aggressive_cleaned} symbols to {aggressive_limit} klines")
                                # 再次GC
                                import gc
                                gc.collect()
                        elif max_klines == 1:
                            # 当max_klines=1时，如果某个symbol超过1条，trim到1条
                            aggressive_cleaned = 0
                            for symbol in list(self.kline_aggregator.klines.keys()):
                                if symbol in self.kline_aggregator.klines:
                                    df = self.kline_aggregator.klines[symbol]
                                    if not df.is_empty() and len(df) > 1:
                                        old_df = self.kline_aggregator.klines[symbol]
                                        trimmed_df = old_df.tail(1).clone()
                                        self.kline_aggregator.klines[symbol] = trimmed_df
                                        if old_df is not None and old_df is not trimmed_df:
                                            del old_df
                                        del trimmed_df
                                        aggressive_cleaned += 1
                            if aggressive_cleaned > 0:
                                logger.info(f"Aggressive cleanup: trimmed {aggressive_cleaned} symbols to 1 kline")
                                # 再次GC
                                import gc
                                gc.collect()
                
                # 记录清理详情（INFO级别，便于监控）
                cleanup_details = []
                if stats.get('pending_trades_windows', 0) > klines_symbols * 2:  # 平均每个symbol超过2个窗口
                    cleanup_details.append(f"pending_windows={stats.get('pending_trades_windows', 0)}")
                if stats.get('trades_buffer_size', 0) > 0:
                    cleanup_details.append(f"trades_buffer={stats.get('trades_buffer_size', 0)}")
                
                if cleanup_details:
                    logger.info(f"Memory cleanup details: {', '.join(cleanup_details)}")
                
                logger.info(
                    f"Memory cleanup completed: "
                    f"before={mem_before:.2f}MB, after={mem_after:.2f}MB, freed={mem_freed:.2f}MB. "
                    f"Stats: {stats}"
                )
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic memory cleanup: {e}", exc_info=True)
                # 出错后等待较短时间再重试
                await asyncio.sleep(300)  # 5分钟后重试
    
    def print_stats(self):
        """打印统计信息（轻量摘要，避免构造超大日志对象）"""
        if self.collector:
            try:
                runtime = time.time() - self.collector.stats.get('start_time', time.time())
                trades_received = self.collector.stats.get('trades_received', {})
                total_trades = sum(trades_received.values())
                active_trade_symbols = sum(1 for v in trades_received.values() if v > 0)
                logger.info(
                    "Collector stats summary: "
                    f"running={self.collector.running}, "
                    f"symbols={len(self.collector.symbols)}, "
                    f"runtime={runtime:.1f}s, "
                    f"total_trades={total_trades}, "
                    f"active_symbols={active_trade_symbols}, "
                    f"reconnects={self.collector.stats.get('reconnect_count', 0)}"
                )
            except Exception as e:
                logger.debug(f"Failed to print collector stats summary: {e}")
        
        if self.kline_aggregator:
            try:
                pending_symbols = len(self.kline_aggregator.pending_trades)
                pending_windows = sum(len(w) for w in self.kline_aggregator.pending_trades.values())
                klines_symbols = len(self.kline_aggregator.klines)
                klines_total = sum(
                    len(df) for df in self.kline_aggregator.klines.values() if not df.is_empty()
                )
                trades_processed_total = sum(
                    self.kline_aggregator.stats.get("trades_processed", {}).values()
                )
                klines_generated_total = sum(
                    self.kline_aggregator.stats.get("klines_generated", {}).values()
                )
                logger.info(
                    "Aggregator stats summary: "
                    f"pending_symbols={pending_symbols}, "
                    f"pending_windows={pending_windows}, "
                    f"klines_symbols={klines_symbols}, "
                    f"klines_total={klines_total}, "
                    f"trades_processed_total={trades_processed_total}, "
                    f"klines_generated_total={klines_generated_total}"
                )
            except Exception as e:
                logger.debug(f"Failed to print aggregator stats summary: {e}")


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
                    if time.time() - process.last_stats_time > 300:
                        process.print_stats()
                        process.last_stats_time = time.time()
                else:
                    process.last_stats_time = time.time()
                    
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
