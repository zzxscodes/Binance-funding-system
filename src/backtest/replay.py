"""
历史数据重放引擎
从本地存储中读取历史K线数据，按时间顺序重放
"""
from typing import Dict, List, Optional, Iterator, Tuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import polars as pl
from collections import defaultdict

from ..common.logger import get_logger
from ..common.config import config
from ..data.storage import get_data_storage
from .models import HistoricalKline, KlineSnapshot, ReplayEvent

logger = get_logger('replay_engine')


class DataReplayEngine:
    """数据重放引擎 - 从本地存储读取历史K线数据并按时间顺序重放"""
    
    def __init__(self, 
                 symbols: List[str],
                 start_date: datetime,
                 end_date: datetime,
                 interval: str = "5m"):
        """
        初始化数据重放引擎
        
        Args:
            symbols: 交易对列表 (e.g., ["BTCUSDT", "ETHUSDT"])
            start_date: 重放开始日期
            end_date: 重放结束日期
            interval: K线周期 (e.g., "5m", "1h", "4h", etc.)
        """
        self.symbols = symbols
        self.start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        self.end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=0)
        self.interval = interval
        self.storage = get_data_storage()
        
        # 转换为UTC（如果输入是本地时间）
        if self.start_date.tzinfo is None:
            self.start_date = self.start_date.replace(tzinfo=timezone.utc)
        if self.end_date.tzinfo is None:
            self.end_date = self.end_date.replace(tzinfo=timezone.utc)
        
        # 加载所有历史K线数据
        self.kline_data: Dict[str, HistoricalKline] = {}
        self._load_historical_klines()
        
        # 当前重放状态
        self._current_indices: Dict[str, int] = {symbol: 0 for symbol in symbols}
        self._current_timestamp: Optional[datetime] = None
        self._all_timestamps: List[datetime] = []
        
        logger.info(
            f"Initialized ReplayEngine: {len(symbols)} symbols, "
            f"{self.start_date.date()} to {self.end_date.date()}, interval={interval}"
        )
    
    def _load_historical_klines(self):
        """从存储加载所有历史K线数据"""
        logger.info(f"Loading historical klines for {len(self.symbols)} symbols...")
        
        for symbol in self.symbols:
            try:
                # 从存储读取K线数据
                kline_df = self.storage.load_klines(
                    symbol=symbol,
                    start_date=self.start_date,
                    end_date=self.end_date
                )
                
                if kline_df is None or len(kline_df) == 0:
                    logger.warning(f"No historical kline data found for {symbol}")
                    continue
                
                # 转换为HistoricalKline对象
                historical_kline = HistoricalKline(
                    symbol=symbol,
                    interval=self.interval,
                    data=kline_df.to_pandas() if isinstance(kline_df, pl.DataFrame) else kline_df
                )
                
                self.kline_data[symbol] = historical_kline
                logger.info(f"Loaded {len(historical_kline)} klines for {symbol}")
                
            except Exception as e:
                logger.error(f"Failed to load klines for {symbol}: {e}", exc_info=True)
        
        # 构建所有唯一的时间戳列表（用于同步多个交易对）
        self._build_timestamp_index()
        
        if len(self.kline_data) == 0:
            logger.warning("No historical kline data loaded!")
        else:
            logger.info(f"Successfully loaded klines for {len(self.kline_data)} symbols")
    
    def _build_timestamp_index(self):
        """构建时间戳索引 - 用于同步多个交易对的数据"""
        all_timestamps = set()
        
        for historical_kline in self.kline_data.values():
            if 'open_time' in historical_kline.data.columns:
                timestamps = pd.to_datetime(historical_kline.data['open_time']).unique()
                all_timestamps.update(timestamps)
        
        # 按时间排序
        self._all_timestamps = sorted(list(all_timestamps))
        logger.info(f"Built timestamp index with {len(self._all_timestamps)} unique timestamps")
    
    def get_available_symbols(self) -> List[str]:
        """获取有数据的交易对列表"""
        return list(self.kline_data.keys())
    
    def has_data(self) -> bool:
        """是否有可重放的数据"""
        return len(self.kline_data) > 0
    
    def get_kline_count(self, symbol: str) -> int:
        """获取某个交易对的K线数量"""
        if symbol in self.kline_data:
            return len(self.kline_data[symbol])
        return 0
    
    def get_data_range(self) -> Tuple[datetime, datetime]:
        """获取可用数据的时间范围"""
        if not self._all_timestamps:
            return self.start_date, self.end_date
        return self._all_timestamps[0], self._all_timestamps[-1]
    
    def reset(self):
        """重置重放指针"""
        self._current_indices = {symbol: 0 for symbol in self.symbols}
        self._current_timestamp = None
        logger.info("Replay engine reset")
    
    def replay_iterator(self) -> Iterator[Tuple[datetime, Dict[str, KlineSnapshot]]]:
        """
        迭代重放所有时间步
        
        Yields:
            (timestamp, {symbol: kline_snapshot, ...})
        """
        self.reset()
        
        # 迭代所有时间戳
        for timestamp in self._all_timestamps:
            self._current_timestamp = timestamp
            
            # 为每个交易对获取对应时间戳的K线
            klines = {}
            for symbol in self.symbols:
                if symbol not in self.kline_data:
                    continue
                
                historical_kline = self.kline_data[symbol]
                
                # 找到最接近的K线
                kline_snapshot = self._get_kline_at_timestamp(symbol, timestamp)
                
                if kline_snapshot:
                    klines[symbol] = kline_snapshot
            
            # 只有当有至少一个交易对有数据时才yield
            if klines:
                yield timestamp, klines
    
    def _get_kline_at_timestamp(self, symbol: str, timestamp: datetime) -> Optional[KlineSnapshot]:
        """获取指定时间戳的K线"""
        if symbol not in self.kline_data:
            return None
        
        historical_kline = self.kline_data[symbol]
        df = historical_kline.data
        
        # 查找相同或最接近的时间戳
        if 'open_time' not in df.columns:
            return None
        
        open_times = pd.to_datetime(df['open_time'])
        
        # 精确匹配
        mask = open_times == timestamp
        if mask.any():
            idx = df[mask].index[0]
            return historical_kline.get_at_index(df.index.get_loc(idx))
        
        # 向后查找最接近的K线（确保不会向前查找导致时序错误）
        mask = open_times <= timestamp
        if mask.any():
            idx = mask.rindex(True)
            return historical_kline.get_at_index(idx)
        
        return None
    
    def get_current_snapshot(self) -> Dict[str, KlineSnapshot]:
        """获取当前时间戳的所有交易对的K线快照"""
        if self._current_timestamp is None:
            return {}
        
        klines = {}
        for symbol in self.symbols:
            kline = self._get_kline_at_timestamp(symbol, self._current_timestamp)
            if kline:
                klines[symbol] = kline
        
        return klines


class MultiIntervalReplayEngine:
    """多周期数据重放引擎"""
    
    def __init__(self,
                 symbols: List[str],
                 start_date: datetime,
                 end_date: datetime,
                 intervals: List[str] = None):
        """
        初始化多周期重放引擎
        
        Args:
            symbols: 交易对列表
            start_date: 重放开始日期
            end_date: 重放结束日期
            intervals: K线周期列表 (e.g., ["5m", "1h", "4h"])
        """
        if intervals is None:
            intervals = ["5m"]
        
        self.symbols = symbols
        self.start_date = start_date
        self.end_date = end_date
        self.intervals = intervals
        
        # 为每个周期创建重放引擎
        self.engines: Dict[str, DataReplayEngine] = {}
        for interval in intervals:
            self.engines[interval] = DataReplayEngine(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
                interval=interval
            )
        
        logger.info(
            f"Initialized MultiIntervalReplayEngine: {len(intervals)} intervals, "
            f"{len(symbols)} symbols"
        )
    
    def get_engine(self, interval: str) -> Optional[DataReplayEngine]:
        """获取指定周期的重放引擎"""
        return self.engines.get(interval)
    
    def reset(self):
        """重置所有引擎"""
        for engine in self.engines.values():
            engine.reset()
    
    def get_available_symbols(self) -> List[str]:
        """获取有数据的交易对列表（所有周期的并集）"""
        all_symbols = set()
        for engine in self.engines.values():
            all_symbols.update(engine.get_available_symbols())
        return list(all_symbols)
