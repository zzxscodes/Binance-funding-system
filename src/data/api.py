"""
数据层API模块
提供API接口供策略进程调用，获取历史K线数据
"""
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Union
from pathlib import Path
from collections import defaultdict
import threading

import pandas as pd
import polars as pl

from ..common.config import config
from ..common.logger import get_logger
from ..monitoring.performance import get_performance_monitor
from .storage import get_data_storage
from .kline_aggregator import KlineAggregator
from ..common.utils import to_system_symbol, to_exchange_symbol

logger = get_logger('data_api')


class DataAPI:
    """数据层API服务"""
    
    def __init__(self, kline_aggregator: Optional[KlineAggregator] = None):
        """
        初始化数据API
        
        Args:
            kline_aggregator: K线聚合器实例（可选，用于获取实时K线）
        """
        self.kline_aggregator = kline_aggregator
        self.storage = get_data_storage()
        
        # 历史数据天数限制（最长30天）
        self.max_history_days = config.get('data.max_history_days', 30)
        self.strategy_history_days = config.get('strategy.history_days', 30)
        
        # 内存滑动窗口缓存：保持最近30天的K线数据（288*30=8640根）
        # 格式: {symbol: pl.DataFrame}，每个DataFrame按open_time排序，最多8640行
        # 使用polars DataFrame减少pandas/polars转换开销
        import polars as pl
        self._memory_cache: Dict[str, pl.DataFrame] = {}
        self._cache_lock = threading.Lock()
        self._cache_max_klines = config.get('data.cache_max_klines', 8640)  # 默认8640根K线
        self._cache_max_symbols = config.get('data.cache_max_symbols', 100)  # 默认最多缓存100个symbol，防止内存无限增长
        self._cache_initialized = False
        # 缓存访问时间戳（用于LRU清理）
        self._cache_access_time: Dict[str, float] = {}
        
        # 性能监控
        self.performance_monitor = get_performance_monitor()
    
    def _ensure_cache_loaded(self, begin_time: datetime, end_time: datetime) -> bool:
        """
        确保缓存已加载数据，如果缓存为空则按需加载
        
        Args:
            begin_time: 开始时间
            end_time: 结束时间
            
        Returns:
            bool: 如果成功加载数据返回True，否则返回False
        """
        # 如果缓存未初始化，尝试初始化
        if not self._cache_initialized:
            self.initialize_memory_cache()
        
        # 获取所有缓存的symbol
        with self._cache_lock:
            cached_symbols = list(self._memory_cache.keys())
        
        # 如果缓存为空，尝试按需加载数据
        # 在mock模式下，数据可能还没有被加载到缓存中，需要从存储直接加载
        if not cached_symbols:
            logger.debug("Memory cache is empty, attempting to load data on-demand from storage")
            # 尝试从universe获取symbol列表
            try:
                universe = self.get_universe()
                if universe:
                    # 计算需要加载的天数（基于时间范围）
                    time_diff = end_time - begin_time
                    days = max(1, int(time_diff.total_seconds() / 86400) + 1)  # 至少1天
                    days = min(days, self.max_history_days)  # 不超过最大限制
                    
                    # 使用get_klines按需加载数据（这会填充缓存）
                    logger.info(f"Loading {len(universe)} symbols from storage for {days} days (on-demand)")
                    self.get_klines(universe, days=days, interval_minutes=5)
                    
                    # 重新获取缓存
                    with self._cache_lock:
                        cached_symbols = list(self._memory_cache.keys())
                    logger.debug(f"Loaded {len(cached_symbols)} symbols into cache")
                    return len(cached_symbols) > 0
            except Exception as e:
                logger.warning(f"Failed to load data on-demand: {e}", exc_info=True)
                return False
        
        return len(cached_symbols) > 0
    
    def get_klines(self, symbols: List[str], days: int, 
                   interval_minutes: int = 5) -> Dict[str, pd.DataFrame]:
        """
        获取多个交易对的历史K线数据
        
        Args:
            symbols: 交易对列表（大写，如 ['BTCUSDT', 'ETHUSDT']）
            days: 历史天数（最长30天）
            interval_minutes: K线周期（分钟），默认5分钟
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含该交易对的历史K线
            如果没有数据，返回空DataFrame
        
        注意：
            - days不能超过max_history_days（默认30天）
            - 返回的DataFrame包含288个5分钟K线/天
            - 如果某个交易对没有足够数据，返回已有数据（可能少于请求的天数）
        """
        try:
            # 验证days参数
            # 说明：plan.md 里 30 天是“常用上限/建议值”，不是硬限制。
            # 这里保留 max_history_days 作为“推荐/默认清理窗口”，但不强制截断用户请求。
            if days > self.max_history_days:
                logger.warning(
                    f"Requested {days} days which is greater than configured recommended window "
                    f"data.max_history_days={self.max_history_days}. "
                    f"Proceeding without truncation (this may be slow and requires sufficient local data)."
                )
            
            if days <= 0:
                logger.error(f"Invalid days parameter: {days}")
                return {}
            
            # 计算时间范围
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=days)
            
            # 降级为debug（策略计算路径，可能频繁调用）
            logger.debug(
                f"Fetching klines for {len(symbols)} symbols, "
                f"{days} days ({start_time.date()} to {end_time.date()})"
            )
            
            # 使用性能监控包裹整个方法
            with self.performance_monitor.measure('data_api', 'get_klines', {'symbols_count': len(symbols), 'days': days}):
                result = {}

                # 1) 批量并发加载本地存储（主要瓶颈）
                with self.performance_monitor.measure('data_api', 'load_klines_bulk', {'symbols_count': len(symbols), 'days': days}):
                    storage_map = self.storage.load_klines_bulk(
                        symbols=[to_exchange_symbol(s) for s in symbols],
                        start_date=start_time,
                        end_date=end_time,
                    )

                # 2) 合并实时聚合器数据（如有）
                for raw_symbol in symbols:
                    try:
                        ex_symbol = to_exchange_symbol(raw_symbol)
                        df_storage = storage_map.get(ex_symbol, pd.DataFrame())

                        if self.kline_aggregator:
                            df_realtime = self.kline_aggregator.get_klines(ex_symbol, start_time, end_time)
                        else:
                            df_realtime = pd.DataFrame()

                        if not df_realtime.empty:
                            if not df_storage.empty:
                                # 优化：使用polars进行合并和去重，减少内存占用
                                # 转换为polars进行高效操作
                                df_storage_pl = pl.from_pandas(df_storage)
                                df_realtime_pl = pl.from_pandas(df_realtime)
                                # 立即释放pandas DataFrame
                                del df_storage
                                del df_realtime
                                
                                # 使用lazy API进行合并、去重、排序和过滤
                                combined_df_pl = (
                                    pl.concat([df_storage_pl, df_realtime_pl])
                                    .lazy()
                                    .unique(subset=['open_time'], keep='last')
                                    .sort('open_time')
                                    .filter(
                                        (pl.col('open_time') >= start_time) &
                                        (pl.col('close_time') <= end_time)
                                    )
                                    .collect()
                                )
                                # 清理中间对象
                                del df_storage_pl
                                del df_realtime_pl
                                
                                # 转换回pandas（保持API兼容性）
                                df = combined_df_pl.to_pandas()
                                del combined_df_pl
                            else:
                                df = df_realtime[
                                    (df_realtime['open_time'] >= start_time) &
                                    (df_realtime['close_time'] <= end_time)
                                ]
                                del df_realtime  # 清理原始引用
                        else:
                            df = df_storage
                            del df_realtime  # 清理空DataFrame

                        # 返回key：系统格式 btc-usdt（策略侧更友好）
                        key = to_system_symbol(ex_symbol)
                        result[key] = df

                        if not df.empty and 'open_time' in df.columns:
                            logger.debug(
                                f"{key}: loaded {len(df)} klines "
                                f"({df['open_time'].min()} to {df['open_time'].max()})"
                            )
                        elif df.empty:
                            # 大规模 universe 下逐币种 warning 会刷屏；这里降级为 debug，仅保留汇总日志
                            if len(symbols) <= 50:
                                logger.warning(f"{key}: no kline data found")
                            else:
                                logger.debug(f"{key}: no kline data found")
                    except Exception as e:
                        logger.error(f"Failed to get klines for {raw_symbol}: {e}", exc_info=True)
                        result[to_system_symbol(raw_symbol)] = pd.DataFrame()
                
                # 统计
                total_klines = sum(len(df) for df in result.values())
                symbols_with_data = sum(1 for df in result.values() if not df.empty)
                # 降级为debug（策略计算路径，可能频繁调用）
                logger.debug(
                    f"Kline fetch completed: {symbols_with_data}/{len(symbols)} symbols have data, "
                    f"total {total_klines} klines"
                )
                
                return result
            
        except Exception as e:
            logger.error(f"Error in get_klines: {e}", exc_info=True)
            raise
    
    def _get_symbol_klines(self, symbol: str, start_time: datetime, 
                           end_time: datetime, interval_minutes: int) -> pd.DataFrame:
        """
        获取单个交易对的K线数据
        优先从存储加载，如果K线聚合器有实时数据，也会合并进来
        """
        # 兼容旧逻辑：symbol 允许传 btc-usdt / BTCUSDT
        symbol = to_exchange_symbol(symbol)
        # 从存储加载历史数据
        df_storage = self.storage.load_klines(symbol, start_time, end_time)
        
        # 如果K线聚合器有实时数据，合并进来
        if self.kline_aggregator:
            df_realtime = self.kline_aggregator.get_klines(symbol, start_time, end_time)
            
            if not df_realtime.empty:
                if not df_storage.empty:
                    # 合并数据
                    combined_df = pd.concat([df_storage, df_realtime], ignore_index=True)
                    # 立即释放原始DataFrame引用
                    del df_storage
                    del df_realtime
                    # 去重（按open_time）
                    combined_df = combined_df.drop_duplicates(subset=['open_time'], keep='last')
                    combined_df = combined_df.sort_values('open_time').reset_index(drop=True)
                    # 时间过滤
                    combined_df = combined_df[
                        (combined_df['open_time'] >= start_time) & 
                        (combined_df['close_time'] <= end_time)
                    ]
                    return combined_df
                else:
                    # 只有实时数据
                    filtered_df = df_realtime[
                        (df_realtime['open_time'] >= start_time) &
                        (df_realtime['close_time'] <= end_time)
                    ]
                    del df_realtime  # 清理原始引用
                    return filtered_df
        
        # 返回存储的数据
        return df_storage
    
    def get_latest_klines(self, symbols: List[str], 
                          interval_minutes: int = 5) -> Dict[str, Optional[Union[pd.Series, Dict]]]:
        """
        获取多个交易对的最新K线
        
        Args:
            symbols: 交易对列表
            interval_minutes: K线周期
        
        Returns:
            Dict[symbol, Series/dict或None]，如果某个交易对没有K线数据，返回None
            注意：现在返回dict（Polars优化后），保持向后兼容
        """
        result = {}
        
        if not self.kline_aggregator:
            logger.warning("Kline aggregator not available, cannot get latest klines")
            return {symbol: None for symbol in symbols}
        
        for symbol in symbols:
            try:
                kline = self.kline_aggregator.get_latest_kline(symbol)
                # 如果返回dict，转换为Series以保持兼容性
                if kline is not None and isinstance(kline, dict):
                    kline = pd.Series(kline)
                result[symbol] = kline
            except Exception as e:
                logger.error(f"Failed to get latest kline for {symbol}: {e}")
                result[symbol] = None
        
        return result
    
    def get_universe(self, date: Optional[datetime] = None, version: str = 'v1') -> List[str]:
        """
        获取Universe（可交易资产列表）
        
        Args:
            date: 日期，如果不指定，返回最新的
            version: 版本号，如 'v1', 'v2' 等，默认为 'v1'
        
        Returns:
            交易对列表
        """
        try:
            # 检查是否为mock模式
            execution_mode = config.get('execution.mode', 'mock')
            if execution_mode == 'mock':
                # 在mock模式下使用MockUniverseManager
                from .mock_universe_manager import MockUniverseManager
                universe_manager = MockUniverseManager()
                symbols = list(universe_manager.load_universe())
                logger.debug(f"Loaded universe from MockUniverseManager: {len(symbols)} symbols")
                return symbols
            else:
                # 在正常模式下使用UniverseManager
                from .universe_manager import get_universe_manager
                universe_manager = get_universe_manager()
                symbols = list(universe_manager.load_universe(date, version))
                logger.debug(f"Loaded universe: {len(symbols)} symbols (version: {version})")
                return symbols
        except Exception as e:
            logger.error(f"Failed to get universe: {e}", exc_info=True)
            return []
    
    def check_data_completeness(self, symbols: List[str], days: int) -> Dict[str, Dict]:
        """
        检查数据完整性
        
        Args:
            symbols: 交易对列表
            days: 检查的天数
        
        Returns:
            Dict[symbol, Dict]，包含数据完整性信息
            例如: {'BTCUSDT': {'total_expected': 8640, 'total_actual': 8630, 'completeness': 0.998}}
        """
        result = {}
        
        # 每天288个5分钟K线
        expected_per_day = 288
        total_expected = expected_per_day * days
        
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=days)
        
        for symbol in symbols:
            try:
                df = self._get_symbol_klines(symbol, start_time, end_time, 5)
                total_actual = len(df)
                completeness = total_actual / total_expected if total_expected > 0 else 0
                
                result[symbol] = {
                    'total_expected': total_expected,
                    'total_actual': total_actual,
                    'completeness': completeness,
                    'missing_count': total_expected - total_actual,
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat(),
                }
            except Exception as e:
                logger.error(f"Failed to check completeness for {symbol}: {e}")
                result[symbol] = {
                    'total_expected': total_expected,
                    'total_actual': 0,
                    'completeness': 0,
                    'missing_count': total_expected,
                    'error': str(e),
                }
        
        return result
    
    def initialize_memory_cache(self, symbols: Optional[List[str]] = None):
        """
        初始化内存缓存：从存储加载最近30天的数据
        
        Args:
            symbols: 交易对列表（可选），如果不提供则从Universe获取
        """
        try:
            if self._cache_initialized:
                logger.debug("Memory cache already initialized")
                return
            
            if symbols is None:
                symbols = self.get_universe()
            
            if not symbols:
                logger.warning("No symbols available for cache initialization")
                return
            
            logger.info(f"Initializing memory cache for {len(symbols)} symbols...")
            
            # 计算时间范围（最近1天，极端优化）
            # 注意：30天缓存无法实现，已减少到1天以确保整套系统内存占用在目标范围内
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=1)
            
            # 批量加载数据
            storage_map = self.storage.load_klines_bulk(
                symbols=[to_exchange_symbol(s) for s in symbols],
                start_date=start_time,
                end_date=end_time,
            )
            
            # 填充内存缓存（限制symbol数量，防止内存爆炸）
            import time
            current_time = time.time()
            
            with self._cache_lock:
                # 限制初始化的symbol数量，避免一次性加载太多
                max_init_symbols = min(len(symbols), self._cache_max_symbols)
                symbols_to_init = symbols[:max_init_symbols]
                
                if len(symbols) > max_init_symbols:
                    logger.warning(
                        f"Limiting cache initialization to {max_init_symbols} symbols "
                        f"(requested {len(symbols)}, max allowed: {self._cache_max_symbols})"
                    )
                
                for raw_symbol in symbols_to_init:
                    try:
                        ex_symbol = to_exchange_symbol(raw_symbol)
                        sys_symbol = to_system_symbol(ex_symbol)
                        
                        df_pd = storage_map.get(ex_symbol, pd.DataFrame())
                        
                        if not df_pd.empty and 'open_time' in df_pd.columns:
                            # 转换为polars DataFrame（内部使用polars）
                            df_pl = pl.from_pandas(df_pd)
                            
                            # 按时间排序
                            df_pl = df_pl.sort('open_time')
                            
                            # 如果超过最大数量，只保留最新的
                            if len(df_pl) > self._cache_max_klines:
                                df_pl = df_pl.tail(self._cache_max_klines)
                            
                            self._memory_cache[sys_symbol] = df_pl
                            self._cache_access_time[sys_symbol] = current_time
                            
                    except Exception as e:
                        logger.error(f"Failed to initialize cache for {raw_symbol}: {e}", exc_info=True)
                        continue
                
                self._cache_initialized = True
            
            cached_count = sum(1 for df in self._memory_cache.values() if not df.is_empty())
            logger.info(f"Memory cache initialized: {cached_count}/{len(symbols_to_init)} symbols have data (limited to {self._cache_max_symbols} max symbols)")
            
        except Exception as e:
            logger.error(f"Failed to initialize memory cache: {e}", exc_info=True)
    
    def set_kline_aggregator_callback(self):
        """
        设置K线聚合器的回调函数，用于自动更新内存缓存
        注意：需要在K线聚合器启动后调用
        """
        if not self.kline_aggregator:
            return
        
        # 保存原始回调
        original_callback = self.kline_aggregator.on_kline_callback
        
        async def cache_update_callback(symbol: str, kline_data: dict):
            """更新内存缓存的回调函数"""
            await self._update_memory_cache(symbol, kline_data)
            # 调用原始回调（如果有）
            if original_callback:
                await original_callback(symbol, kline_data)
        
        # 设置新的回调
        self.kline_aggregator.on_kline_callback = cache_update_callback
        logger.debug("Kline aggregator callback set for memory cache updates")
    
    def _cleanup_cache_if_needed(self):
        """
        清理缓存：如果symbol数量超过限制，删除最久未访问的symbol
        优化：更激进的清理策略，提前清理（在达到30%限制时就开始清理，目标60%-70%系统内存）
        """
        with self._cache_lock:
            # 提前清理：在达到30%限制时就开始清理（更激进的策略）
            cleanup_threshold = int(self._cache_max_symbols * 0.3)
            if len(self._memory_cache) <= cleanup_threshold:
                return
            
            # 按访问时间排序，删除最久未访问的
            import time
            current_time = time.time()
            
            # 清理不在memory_cache中的access_time条目（避免累积）
            cache_symbols = set(self._memory_cache.keys())
            access_time_keys = set(self._cache_access_time.keys())
            stale_keys = access_time_keys - cache_symbols
            for key in stale_keys:
                del self._cache_access_time[key]
            
            # 更新当前访问时间
            for symbol in list(self._memory_cache.keys()):
                if symbol not in self._cache_access_time:
                    self._cache_access_time[symbol] = current_time
            
            # 按访问时间排序，删除最久未访问的
            sorted_symbols = sorted(
                self._cache_access_time.items(),
                key=lambda x: x[1]
            )
            
            # 删除最久未访问的symbol，直到满足限制（保留到20%）
            # 修复内存泄漏：确保所有DataFrame被完全释放
            target_size = int(self._cache_max_symbols * 0.2)
            to_remove = len(self._memory_cache) - target_size
            if to_remove > 0:
                removed_count = 0
                for symbol, _ in sorted_symbols[:to_remove]:
                    if symbol in self._memory_cache:
                        # 确保DataFrame被完全释放
                        df = self._memory_cache.pop(symbol, None)
                        if df is not None:
                            del df
                        removed_count += 1
                    if symbol in self._cache_access_time:
                        del self._cache_access_time[symbol]
                
                if removed_count > 0:
                    logger.debug(
                        f"Cleaned up {removed_count} symbols from memory cache "
                        f"(current: {len(self._memory_cache)}/{self._cache_max_symbols})"
                    )
                    # 强制垃圾回收，帮助释放内存
                    import gc
                    gc.collect()
    
    async def _update_memory_cache(self, symbol: str, kline_data: dict):
        """
        更新内存缓存：添加新的K线，移除最旧的K线（保持滑动窗口）
        
        Args:
            symbol: 交易对（交易所格式，如BTCUSDT）
            kline_data: K线数据字典
        """
        try:
            with self._cache_lock:
                # 转换为polars DataFrame（直接使用polars，减少转换）
                new_kline_df = pl.DataFrame([kline_data])
                
                # 确保时间戳精度为纳秒（统一格式）
                if 'open_time' in new_kline_df.columns:
                    new_kline_df = new_kline_df.with_columns(
                        pl.col('open_time').cast(pl.Datetime('ns', time_zone='UTC'))
                    )
                if 'close_time' in new_kline_df.columns:
                    new_kline_df = new_kline_df.with_columns(
                        pl.col('close_time').cast(pl.Datetime('ns', time_zone='UTC'))
                    )
                
                # 获取系统格式的symbol作为key
                sys_symbol = to_system_symbol(symbol)
                
                # 更新访问时间
                import time
                self._cache_access_time[sys_symbol] = time.time()
                
                # 获取当前缓存（如果不存在则创建空DataFrame）
                cached_df = self._memory_cache.get(sys_symbol, pl.DataFrame())
                
                if cached_df.is_empty():
                    # 如果缓存为空，直接添加
                    self._memory_cache[sys_symbol] = new_kline_df
                else:
                    # 极端优化：更激进的清理策略，在达到50%限制时就开始清理
                    cleanup_threshold = int(self._cache_max_klines * 0.5)
                    if len(cached_df) >= cleanup_threshold:
                        # 保留最新的（max-1）条，为新K线腾出空间
                        cached_df = cached_df.tail(self._cache_max_klines - 1)
                    
                    # 合并新数据（使用polars concat，比pandas快）
                    # 修复内存泄漏：确保所有中间DataFrame被正确释放
                    old_cached_df = cached_df
                    combined_df = pl.concat([cached_df, new_kline_df])
                    # 清理中间对象引用，帮助GC
                    del cached_df
                    del new_kline_df
                    
                    # 如果数据量大，使用lazy API优化去重和排序
                    # 优化：降低阈值，更频繁使用lazy API（目标60%-70%系统内存）
                    combined_len = len(combined_df)
                    if combined_len > 50:
                        old_combined_df = combined_df
                        combined_df = (
                            old_combined_df
                            .lazy()
                            .unique(subset=['open_time'], keep='last')
                            .sort('open_time')
                            .collect()
                        )
                        if old_combined_df is not None and old_combined_df is not combined_df:
                            del old_combined_df
                    elif combined_len > 1:
                        # 去重（按open_time，保留最新的）
                        old_combined_df = combined_df
                        combined_df = old_combined_df.unique(subset=['open_time'], keep='last')
                        combined_df = combined_df.sort('open_time')
                        if old_combined_df is not None and old_combined_df is not combined_df:
                            del old_combined_df
                    
                    # 如果超过最大数量，移除最旧的数据（双重检查，确保不超过限制）
                    if len(combined_df) > self._cache_max_klines:
                        # 保留最新的K线
                        old_combined_df = combined_df
                        combined_df = old_combined_df.tail(self._cache_max_klines)
                        if old_combined_df is not None and old_combined_df is not combined_df:
                            del old_combined_df
                    
                    # 更新缓存，确保旧DataFrame被释放
                    old_cached = self._memory_cache.get(sys_symbol)
                    self._memory_cache[sys_symbol] = combined_df
                    if old_cached is not None and old_cached is not combined_df and old_cached is not old_cached_df:
                        del old_cached
                    if old_cached_df is not None and old_cached_df is not combined_df:
                        del old_cached_df
                
                # 检查并清理缓存
                self._cleanup_cache_if_needed()
                    
        except Exception as e:
            logger.error(f"Failed to update memory cache for {symbol}: {e}", exc_info=True)
    
    def _parse_date_time_label(self, date_time_label: str) -> datetime:
        """
        解析date_time_label格式为datetime
        
        Args:
            date_time_label: 格式 'YYYY-MM-DD-HHH'，例如 '2025-12-22-004'
                            HHH是3位数的time_label（1-288）
        
        Returns:
            datetime对象（UTC时区）
        """
        try:
            parts = date_time_label.split('-')
            if len(parts) != 4:
                raise ValueError(f"Invalid date_time_label format: {date_time_label}, expected 'YYYY-MM-DD-HHH'")
            
            year, month, day, time_label_str = parts
            time_label = int(time_label_str)
            
            if not (1 <= time_label <= 288):
                raise ValueError(f"Invalid time_label: {time_label}, must be 1-288")
            
            # 计算该时间标签对应的具体时间（5分钟间隔）
            # time_label是1-288，转换为0-287用于计算分钟偏移
            base_date = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
            minutes_offset = (time_label - 1) * 5  # 每个time_label代表5分钟，从1开始
            target_time = base_date + timedelta(minutes=minutes_offset)
            
            return target_time
        except Exception as e:
            raise ValueError(f"Failed to parse date_time_label '{date_time_label}': {e}")
    
    def _get_date_time_label_from_datetime(self, dt: datetime) -> str:
        """
        从datetime生成date_time_label格式
        
        Args:
            dt: datetime对象（UTC时区）
        
        Returns:
            date_time_label字符串，格式 'YYYY-MM-DD-HHH'
        """
        # 计算当天的第几个5分钟窗口（1-288）
        day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes_since_midnight = (dt - day_start).total_seconds() / 60
        time_label = int(minutes_since_midnight // 5) + 1
        
        # 确保time_label在有效范围内
        time_label = max(1, min(288, time_label))
        
        return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}-{time_label:03d}"
    
    def get_bar_between(
        self, 
        begin_date_time_label: str, 
        end_date_time_label: str,
        mode: str = '5min'
    ) -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的bar数据
        
        Args:
            begin_date_time_label: 开始时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2025-12-22-004'
            end_date_time_label: 结束时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2026-01-10-013'
            mode: 时间周期，支持 '5min', '1h', '4h', '8h', '12h', '24h'，默认为 '5min'
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含bar表字段
            格式: {'btc-usdt': DataFrame, 'eth-usdt': DataFrame, ...}
        
        Bar表字段包括：
            - 基础字段: symbol, open_time, close_time, open, high, low, close, volume, quote_volume, trade_count
            - bar表专用字段: microsecond_since_trad, span_begin_datetime, span_end_datetime, span_status,
                            last, vwap, dolvol, buydolvol, selldolvol, buyvolume, sellvolume,
                            buytradecount, selltradecount, time_lable
        """
        # 使用性能监控包裹整个方法
        with self.performance_monitor.measure('data_api', 'get_bar_between', {'mode': mode}):
            try:
                begin_time = self._parse_date_time_label(begin_date_time_label)
                end_time = self._parse_date_time_label(end_date_time_label)
                
                if begin_time >= end_time:
                    logger.warning(f"Invalid time range: begin >= end ({begin_date_time_label} >= {end_date_time_label})")
                    return {}
                
                # 确保缓存已加载数据
                if not self._ensure_cache_loaded(begin_time, end_time):
                    logger.warning("Failed to load data into cache, returning empty result")
                    return {}
                
                # 获取所有缓存的symbol
                with self._cache_lock:
                    cached_symbols = list(self._memory_cache.keys())
                
                result = {}
                
                # 如果mode不是5min，需要从5min数据聚合
                if mode != '5min':
                    from .multi_interval_aggregator import get_multi_interval_aggregator
                    aggregator = get_multi_interval_aggregator()
                import time
                current_time = time.time()
                
                for sys_symbol in cached_symbols:
                    try:
                        # 在锁内快速获取数据引用（polars DataFrame是immutable的，可以安全共享）
                        with self._cache_lock:
                            if sys_symbol not in self._memory_cache:
                                continue
                            cached_df_pl = self._memory_cache[sys_symbol]
                            # 更新访问时间
                            self._cache_access_time[sys_symbol] = current_time
                        
                        if cached_df_pl.is_empty():
                            continue
                        
                        # 过滤时间范围（使用polars filter，比pandas快）
                        if 'open_time' in cached_df_pl.columns:
                            # 对于非5min周期，需要加载更早的数据以便聚合
                            if mode != '5min':
                                # 加载更早的数据（至少一个周期）
                                from .multi_interval_aggregator import MultiIntervalAggregator
                                target_minutes = MultiIntervalAggregator.INTERVAL_MAP.get(mode, 5)
                                extended_begin = begin_time - timedelta(minutes=target_minutes)
                                filtered_df_pl = cached_df_pl.filter(
                                    (pl.col('open_time') >= extended_begin) &
                                    (pl.col('open_time') <= end_time)
                                )
                            else:
                                filtered_df_pl = cached_df_pl.filter(
                                    (pl.col('open_time') >= begin_time) &
                                    (pl.col('open_time') <= end_time)
                                )
                            
                            if not filtered_df_pl.is_empty():
                                # 如果不是5min，进行聚合（需要转换为pandas，因为aggregator使用pandas）
                                if mode != '5min':
                                    filtered_df_pd = filtered_df_pl.to_pandas()
                                    filtered_df_pd = aggregator.aggregate_klines(filtered_df_pd, mode)
                                    # 再次过滤时间范围（聚合后）
                                    filtered_df_pd = filtered_df_pd[
                                        (filtered_df_pd['open_time'] >= begin_time) &
                                        (filtered_df_pd['open_time'] <= end_time)
                                    ].copy()
                                    filtered_df_pl = pl.from_pandas(filtered_df_pd)
                                else:
                                    # 5min模式，直接使用polars结果
                                    pass
                            
                            if not filtered_df_pl.is_empty():
                                # 只返回bar表字段
                                bar_fields = [
                                    'symbol', 'open_time', 'close_time', 'open', 'high', 'low', 'close',
                                    'volume', 'quote_volume', 'trade_count',
                                    'microsecond_since_trad', 'span_begin_datetime', 'span_end_datetime',
                                    'span_status', 'last', 'vwap', 'dolvol', 'buydolvol', 'selldolvol',
                                    'buyvolume', 'sellvolume', 'tradecount', 'buytradecount', 'selltradecount', 'time_lable'
                                ]
                                
                                # 只选择存在的字段（使用polars select）
                                available_fields = [f for f in bar_fields if f in filtered_df_pl.columns]
                                # 转换为pandas返回（保持API兼容性）
                                result[sys_symbol] = filtered_df_pl.select(available_fields).to_pandas()
                        
                    except Exception as e:
                        logger.error(f"Failed to get bar data for {sys_symbol}: {e}", exc_info=True)
                        continue
                
                logger.debug(f"get_bar_between (mode={mode}): {len(result)} symbols with data")
                return result
                
            except Exception as e:
                logger.error(f"Error in get_bar_between: {e}", exc_info=True)
                raise
    
    def get_tran_stats_between(
        self, 
        begin_date_time_label: str, 
        end_date_time_label: str,
        mode: str = '5min'
    ) -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的tran_stats数据
        
        Args:
            begin_date_time_label: 开始时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2025-12-22-004'
            end_date_time_label: 结束时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2026-01-10-013'
            mode: 时间周期，支持 '5min', '1h', '4h', '8h', '12h', '24h'，默认为 '5min'
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含tran_stats表字段
            格式: {'btc-usdt': DataFrame, 'eth-usdt': DataFrame, ...}
        
        Tran_stats表字段包括：
            - 基础统计: buy_volume, buy_dolvol, buy_trade_count, sell_volume, sell_dolvol, sell_trade_count, time_lable
            - 按金额分档统计（24个字段）:
              buy_volume1-4, buy_dolvol1-4, buy_trade_count1-4,
              sell_volume1-4, sell_dolvol1-4, sell_trade_count1-4
        """
        # 使用性能监控包裹整个方法
        with self.performance_monitor.measure('data_api', 'get_tran_stats_between', {'mode': mode}):
            try:
                begin_time = self._parse_date_time_label(begin_date_time_label)
                end_time = self._parse_date_time_label(end_date_time_label)
                
                if begin_time >= end_time:
                    logger.warning(f"Invalid time range: begin >= end ({begin_date_time_label} >= {end_date_time_label})")
                    return {}
                
                # 确保缓存已加载数据
                if not self._ensure_cache_loaded(begin_time, end_time):
                    logger.warning("Failed to load data into cache, returning empty result")
                    return {}
                
                # 获取所有缓存的symbol
                with self._cache_lock:
                    cached_symbols = list(self._memory_cache.keys())
                
                result = {}
                
                # 如果mode不是5min，需要从5min数据聚合
                if mode != '5min':
                    from .multi_interval_aggregator import get_multi_interval_aggregator
                    aggregator = get_multi_interval_aggregator()
                import time
                current_time = time.time()
                
                for sys_symbol in cached_symbols:
                    try:
                        # 在锁内快速获取数据引用（polars DataFrame是immutable的，可以安全共享）
                        with self._cache_lock:
                            if sys_symbol not in self._memory_cache:
                                continue
                            cached_df_pl = self._memory_cache[sys_symbol]
                            # 更新访问时间
                            self._cache_access_time[sys_symbol] = current_time
                        
                        if cached_df_pl.is_empty():
                            continue
                        
                        # 过滤时间范围（使用polars filter，比pandas快）
                        if 'open_time' in cached_df_pl.columns:
                            # 对于非5min周期，需要加载更早的数据以便聚合
                            if mode != '5min':
                                # 加载更早的数据（至少一个周期）
                                from .multi_interval_aggregator import MultiIntervalAggregator
                                target_minutes = MultiIntervalAggregator.INTERVAL_MAP.get(mode, 5)
                                extended_begin = begin_time - timedelta(minutes=target_minutes)
                                filtered_df_pl = cached_df_pl.filter(
                                    (pl.col('open_time') >= extended_begin) &
                                    (pl.col('open_time') <= end_time)
                                )
                            else:
                                filtered_df_pl = cached_df_pl.filter(
                                    (pl.col('open_time') >= begin_time) &
                                    (pl.col('open_time') <= end_time)
                                )
                            
                            if not filtered_df_pl.is_empty():
                                # 如果不是5min，进行聚合（需要转换为pandas，因为aggregator使用pandas）
                                if mode != '5min':
                                    filtered_df_pd = filtered_df_pl.to_pandas()
                                    filtered_df_pd = aggregator.aggregate_klines(filtered_df_pd, mode)
                                    # 再次过滤时间范围（聚合后）
                                    filtered_df_pd = filtered_df_pd[
                                        (filtered_df_pd['open_time'] >= begin_time) &
                                        (filtered_df_pd['open_time'] <= end_time)
                                    ].copy()
                                    filtered_df_pl = pl.from_pandas(filtered_df_pd)
                                else:
                                    # 5min模式，直接使用polars结果
                                    pass
                                
                                if not filtered_df_pl.is_empty():
                                    # 只返回tran_stats表字段
                                    tran_stats_fields = [
                                    'symbol', 'open_time', 'time_lable',
                                    'buy_volume', 'buy_dolvol', 'buy_trade_count',
                                    'sell_volume', 'sell_dolvol', 'sell_trade_count',
                                    'buy_volume1', 'buy_volume2', 'buy_volume3', 'buy_volume4',
                                    'buy_dolvol1', 'buy_dolvol2', 'buy_dolvol3', 'buy_dolvol4',
                                    'buy_trade_count1', 'buy_trade_count2', 'buy_trade_count3', 'buy_trade_count4',
                                    'sell_volume1', 'sell_volume2', 'sell_volume3', 'sell_volume4',
                                    'sell_dolvol1', 'sell_dolvol2', 'sell_dolvol3', 'sell_dolvol4',
                                    'sell_trade_count1', 'sell_trade_count2', 'sell_trade_count3', 'sell_trade_count4',
                                    ]
                                    
                                    # 只选择存在的字段（使用polars select）
                                    available_fields = [f for f in tran_stats_fields if f in filtered_df_pl.columns]
                                    # 转换为pandas返回（保持API兼容性）
                                    result[sys_symbol] = filtered_df_pl.select(available_fields).to_pandas()
                        
                    except Exception as e:
                        logger.error(f"Failed to get tran_stats data for {sys_symbol}: {e}", exc_info=True)
                        continue
                
                logger.debug(f"get_tran_stats_between: {len(result)} symbols with data")
                return result
                
            except Exception as e:
                logger.error(f"Error in get_tran_stats_between: {e}", exc_info=True)
                raise
    
    def get_funding_rate_between(self, begin_date_time_label: str, end_date_time_label: str) -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的历史资金费率数据
        
        Args:
            begin_date_time_label: 开始时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2025-12-22-004'
            end_date_time_label: 结束时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2026-01-10-013'
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含资金费率数据
            格式: {'btc-usdt': DataFrame, 'eth-usdt': DataFrame, ...}
        
        资金费率DataFrame字段包括：
            - symbol: 交易对
            - fundingTime: 资金费率时间戳
            - fundingRate: 资金费率
            - markPrice: 标记价格（如果有）
        """
        try:
            begin_time = self._parse_date_time_label(begin_date_time_label)
            end_time = self._parse_date_time_label(end_date_time_label)
            
            if begin_time >= end_time:
                logger.warning(f"Invalid time range: begin >= end ({begin_date_time_label} >= {end_date_time_label})")
                return {}
            
            # 从Universe获取所有symbol
            universe = self.get_universe()
            if not universe:
                logger.warning("No universe available for funding rate query")
                return {}
            
            # 批量加载资金费率数据
            storage_map = self.storage.load_funding_rates_bulk(
                symbols=[to_exchange_symbol(s) for s in universe],
                start_date=begin_time,
                end_date=end_time,
            )
            
            result = {}
            
            for raw_symbol in universe:
                try:
                    ex_symbol = to_exchange_symbol(raw_symbol)
                    sys_symbol = to_system_symbol(ex_symbol)
                    
                    df = storage_map.get(ex_symbol, pd.DataFrame())
                    
                    if not df.empty and 'fundingTime' in df.columns:
                        # 过滤时间范围
                        filtered_df = df[
                            (df['fundingTime'] >= begin_time) &
                            (df['fundingTime'] <= end_time)
                        ].copy()
                        
                        if not filtered_df.empty:
                            result[sys_symbol] = filtered_df
                            logger.debug(
                                f"{sys_symbol}: loaded {len(filtered_df)} funding rates "
                                f"({filtered_df['fundingTime'].min()} to {filtered_df['fundingTime'].max()})"
                            )
                    
                except Exception as e:
                    logger.error(f"Failed to get funding rate data for {raw_symbol}: {e}", exc_info=True)
                    continue
            
            logger.debug(f"get_funding_rate_between: {len(result)} symbols with data")
            return result
            
        except Exception as e:
            logger.error(f"Error in get_funding_rate_between: {e}", exc_info=True)
            raise
    
    def get_premium_index_bar_between(
        self, 
        begin_date_time_label: str, 
        end_date_time_label: str,
        mode: str = '5min'
    ) -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的溢价指数K线数据
        
        Args:
            begin_date_time_label: 开始时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2025-12-22-004'
            end_date_time_label: 结束时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2026-01-10-013'
            mode: 时间周期，目前支持 '5min'，默认为 '5min'
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含溢价指数K线数据
            格式: {'btc-usdt': DataFrame, 'eth-usdt': DataFrame, ...}
        
        溢价指数K线DataFrame字段包括：
            - symbol: 交易对
            - open_time, close_time: 时间戳
            - open, high, low, close: OHLC价格
            - volume, quote_volume: 成交量
            - trade_count: 成交笔数
            - time_lable: 时间标签（1-288）
        """
        try:
            begin_time = self._parse_date_time_label(begin_date_time_label)
            end_time = self._parse_date_time_label(end_date_time_label)
            
            if begin_time >= end_time:
                logger.warning(f"Invalid time range: begin >= end ({begin_date_time_label} >= {end_date_time_label})")
                return {}
            
            # 目前只支持5min，其他周期需要从5min聚合（类似bar数据）
            if mode != '5min':
                logger.warning(f"Premium index klines only support 5min mode currently, requested: {mode}")
                # 可以扩展支持多周期聚合，但暂时只返回5min
                mode = '5min'
            
            # 从Universe获取所有symbol
            universe = self.get_universe()
            if not universe:
                logger.warning("No universe available for premium index kline query")
                return {}
            
            # 批量加载溢价指数K线数据
            storage_map = self.storage.load_premium_index_klines_bulk(
                symbols=[to_exchange_symbol(s) for s in universe],
                start_date=begin_time - timedelta(days=1),  # 多加载1天以确保覆盖
                end_date=end_time + timedelta(days=1),
            )
            
            result = {}
            
            for raw_symbol in universe:
                try:
                    ex_symbol = to_exchange_symbol(raw_symbol)
                    sys_symbol = to_system_symbol(ex_symbol)
                    
                    df = storage_map.get(ex_symbol, pd.DataFrame())
                    
                    if not df.empty and 'open_time' in df.columns:
                        # 过滤时间范围
                        filtered_df = df[
                            (df['open_time'] >= begin_time) &
                            (df['open_time'] <= end_time)
                        ].copy()
                        
                        if not filtered_df.empty:
                            result[sys_symbol] = filtered_df
                            logger.debug(
                                f"{sys_symbol}: loaded {len(filtered_df)} premium index klines "
                                f"({filtered_df['open_time'].min()} to {filtered_df['open_time'].max()})"
                            )
                    
                except Exception as e:
                    logger.error(f"Failed to get premium index kline data for {raw_symbol}: {e}", exc_info=True)
                    continue
            
            logger.debug(f"get_premium_index_bar_between: {len(result)} symbols with data")
            return result
            
        except Exception as e:
            logger.error(f"Error in get_premium_index_bar_between: {e}", exc_info=True)
            raise


# 全局API实例
_data_api: Optional[DataAPI] = None


def get_data_api(kline_aggregator: Optional[KlineAggregator] = None) -> DataAPI:
    """获取数据API实例"""
    global _data_api
    if _data_api is None:
        _data_api = DataAPI(kline_aggregator)
    else:
        # 保持单例：如果传入了新的聚合器，仅更新引用，不替换实例
        if kline_aggregator is not None:
            _data_api.kline_aggregator = kline_aggregator
    return _data_api
