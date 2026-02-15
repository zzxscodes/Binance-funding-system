"""
因子挖掘模拟数据生成器

为因子挖掘系统生成模拟的历史数据，用于示例和测试
"""

from __future__ import annotations

from typing import Dict, Optional, List
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np

from ..common.logger import get_logger
from ..data.api import DataAPI
from ..backtest.mock_data import MockKlineGenerator

logger = get_logger('factor_mining.mock_data')


class MockDataAPI(DataAPI):
    """模拟数据API，用于因子挖掘示例和测试"""
    
    def __init__(self, symbols: Optional[List[str]] = None, seed: Optional[int] = None):
        """
        初始化模拟数据API
        
        Args:
            symbols: 交易对列表，如果为None则使用默认列表
            seed: 随机种子，用于复现结果
        """
        # 不调用父类的__init__，避免初始化真实的storage
        self.symbols = symbols or ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT']
        self.seed = seed
        if seed is not None:
            np.random.seed(seed)
        
        # 初始化模拟数据生成器
        self.mock_generator = MockKlineGenerator(seed=seed)
        
        # 生成模拟数据缓存
        self._mock_data_cache: Dict[str, Dict[str, pd.DataFrame]] = {}
        self._cache_initialized = False
        
        logger.info(f"初始化MockDataAPI，symbols={self.symbols}, seed={seed}")
    
    def _initialize_cache(self, start_date: datetime, end_date: datetime, interval: str = '5min', history_days: int = 30):
        """初始化模拟数据缓存"""
        # 扩展日期范围以包含历史数据
        cache_start = start_date - timedelta(days=history_days)
        cache_end = end_date
        
        # 如果缓存已初始化且覆盖所需范围，则不需要重新生成
        if self._cache_initialized:
            # 检查缓存是否覆盖所需范围
            if hasattr(self, '_cache_start') and hasattr(self, '_cache_end'):
                if self._cache_start <= cache_start and self._cache_end >= cache_end:
                    return  # 缓存已覆盖所需范围
        
        logger.info(f"生成模拟数据: {cache_start.date()} - {cache_end.date()}, interval={interval}")
        
        # 解析interval
        interval_minutes = self._parse_interval(interval)
        
        # 为每个symbol生成模拟数据
        initial_prices = {
            'BTCUSDT': 50000.0,
            'ETHUSDT': 3000.0,
            'BNBUSDT': 400.0,
            'SOLUSDT': 100.0,
            'ADAUSDT': 0.5,
        }
        
        bar_data = {}
        tran_stats = {}
        
        for symbol in self.symbols:
            initial_price = initial_prices.get(symbol, 100.0)
            
            # 生成K线数据（扩展到包含历史数据）
            klines_df = self.mock_generator.generate_klines(
                symbol=symbol,
                start_date=cache_start,
                end_date=cache_end,
                initial_price=initial_price,
                volatility=0.02,  # 2%日波动率
                interval_minutes=interval_minutes,
                trend=0.0001,  # 轻微上涨趋势
            )
            
            bar_data[symbol] = klines_df
            
            # 生成tran_stats数据（简化版）
            tran_stats_df = self._generate_mock_tran_stats(klines_df, symbol)
            tran_stats[symbol] = tran_stats_df
        
        self._mock_data_cache['bar'] = bar_data
        self._mock_data_cache['tran_stats'] = tran_stats
        self._cache_start = cache_start
        self._cache_end = cache_end
        self._cache_initialized = True
        
        logger.info(f"模拟数据生成完成: {len(bar_data)} symbols, {len(klines_df)} klines per symbol")
    
    def _parse_interval(self, interval: str) -> int:
        """解析interval字符串为分钟数"""
        interval_map = {
            '5min': 5,
            '1h': 60,
            '4h': 240,
            '8h': 480,
            '12h': 720,
            '24h': 1440,
        }
        return interval_map.get(interval, 5)
    
    def _generate_mock_tran_stats(self, klines_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """生成模拟的tran_stats数据"""
        if klines_df.empty:
            return pd.DataFrame()
        
        tran_stats_list = []
        
        for _, row in klines_df.iterrows():
            # 从K线数据中提取基础信息
            volume = float(row.get('volume', 0)) if pd.notna(row.get('volume', 0)) else 0
            dolvol = float(row.get('dolvol', 0)) if pd.notna(row.get('dolvol', 0)) else volume * float(row.get('close', 0))
            
            # 生成tran_stats字段
            trade_count = max(1, int(volume / 1000)) if volume > 0 else 1
            
            # 分配买卖量（随机分配，但保持合理比例）
            buy_ratio = np.random.uniform(0.4, 0.6)  # 买卖比例在40%-60%之间
            buy_volume = volume * buy_ratio
            sell_volume = volume * (1 - buy_ratio)
            buy_dolvol = dolvol * buy_ratio
            sell_dolvol = dolvol * (1 - buy_ratio)
            
            # 生成tier数据（简化版，所有交易都在tier1）
            tier1_ratio = np.random.uniform(0.3, 0.5)
            
            # 确保open_time是毫秒时间戳
            open_time = row.get('open_time', 0)
            if isinstance(open_time, (pd.Timestamp, datetime)):
                open_time = int(pd.to_datetime(open_time).timestamp() * 1000)
            elif isinstance(open_time, str):
                open_time = int(pd.to_datetime(open_time).timestamp() * 1000)
            elif hasattr(open_time, 'timestamp'):
                open_time = int(open_time.timestamp() * 1000)
            else:
                # 如果已经是数值，确保是毫秒时间戳
                open_time = int(open_time) if pd.notna(open_time) else 0
            
            close_time = row.get('close_time', 0)
            if isinstance(close_time, (pd.Timestamp, datetime)):
                close_time = int(pd.to_datetime(close_time).timestamp() * 1000)
            elif isinstance(close_time, str):
                close_time = int(pd.to_datetime(close_time).timestamp() * 1000)
            elif hasattr(close_time, 'timestamp'):
                close_time = int(close_time.timestamp() * 1000)
            else:
                # 如果已经是数值，确保是毫秒时间戳
                close_time = int(close_time) if pd.notna(close_time) else 0
            
            tran_stat = {
                'open_time': open_time,
                'close_time': close_time,
                'tradecount': trade_count,
                'volume': str(volume),
                'dolvol': str(int(dolvol)),
                'buy_volume': str(buy_volume),
                'sell_volume': str(sell_volume),
                'buy_dolvol': str(int(buy_dolvol)),
                'sell_dolvol': str(int(sell_dolvol)),
                'buy_trade_count': int(trade_count * buy_ratio),
                'sell_trade_count': int(trade_count * (1 - buy_ratio)),
                # Tier 1-4数据（简化版）
                'buy_volume1': str(buy_volume * tier1_ratio),
                'buy_volume2': str(buy_volume * (1 - tier1_ratio) * 0.3),
                'buy_volume3': str(buy_volume * (1 - tier1_ratio) * 0.3),
                'buy_volume4': str(buy_volume * (1 - tier1_ratio) * 0.4),
                'sell_volume1': str(sell_volume * tier1_ratio),
                'sell_volume2': str(sell_volume * (1 - tier1_ratio) * 0.3),
                'sell_volume3': str(sell_volume * (1 - tier1_ratio) * 0.3),
                'sell_volume4': str(sell_volume * (1 - tier1_ratio) * 0.4),
                'buy_dolvol1': str(int(buy_dolvol * tier1_ratio)),
                'buy_dolvol2': str(int(buy_dolvol * (1 - tier1_ratio) * 0.3)),
                'buy_dolvol3': str(int(buy_dolvol * (1 - tier1_ratio) * 0.3)),
                'buy_dolvol4': str(int(buy_dolvol * (1 - tier1_ratio) * 0.4)),
                'sell_dolvol1': str(int(sell_dolvol * tier1_ratio)),
                'sell_dolvol2': str(int(sell_dolvol * (1 - tier1_ratio) * 0.3)),
                'sell_dolvol3': str(int(sell_dolvol * (1 - tier1_ratio) * 0.3)),
                'sell_dolvol4': str(int(sell_dolvol * (1 - tier1_ratio) * 0.4)),
                'buy_trade_count1': int(trade_count * buy_ratio * tier1_ratio),
                'buy_trade_count2': int(trade_count * buy_ratio * (1 - tier1_ratio) * 0.3),
                'buy_trade_count3': int(trade_count * buy_ratio * (1 - tier1_ratio) * 0.3),
                'buy_trade_count4': int(trade_count * buy_ratio * (1 - tier1_ratio) * 0.4),
                'sell_trade_count1': int(trade_count * (1 - buy_ratio) * tier1_ratio),
                'sell_trade_count2': int(trade_count * (1 - buy_ratio) * (1 - tier1_ratio) * 0.3),
                'sell_trade_count3': int(trade_count * (1 - buy_ratio) * (1 - tier1_ratio) * 0.3),
                'sell_trade_count4': int(trade_count * (1 - buy_ratio) * (1 - tier1_ratio) * 0.4),
            }
            
            tran_stats_list.append(tran_stat)
        
        return pd.DataFrame(tran_stats_list)
    
    def _get_date_time_label_from_datetime(self, dt: datetime) -> str:
        """将datetime转换为日期时间标签"""
        return dt.strftime('%Y%m%d')
    
    def get_bar_between(self, begin_label: str, end_label: str, mode: str = '5min') -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的K线数据
        
        Args:
            begin_label: 开始日期标签 (YYYYMMDD)
            end_label: 结束日期标签 (YYYYMMDD)
            mode: 时间间隔
        
        Returns:
            {symbol: DataFrame} 字典
        """
        # 解析日期标签
        start_date = datetime.strptime(begin_label, '%Y%m%d').replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(end_label, '%Y%m%d').replace(tzinfo=timezone.utc)
        
        # 初始化缓存
        self._initialize_cache(start_date, end_date, mode)
        
        # 从缓存中筛选数据
        result = {}
        for symbol, df in self._mock_data_cache['bar'].items():
            if 'open_time' in df.columns:
                start_ms = int(start_date.timestamp() * 1000)
                end_ms = int(end_date.timestamp() * 1000)
                
                # 处理open_time字段类型（可能是datetime或int）
                if df['open_time'].dtype == 'datetime64[ns, UTC]' or 'datetime' in str(df['open_time'].dtype):
                    # 转换为毫秒时间戳
                    df_filtered = df.copy()
                    df_filtered['open_time'] = pd.to_datetime(df_filtered['open_time']).astype('int64') // 1_000_000
                    filtered = df_filtered[(df_filtered['open_time'] >= start_ms) & (df_filtered['open_time'] <= end_ms)]
                else:
                    # 已经是时间戳格式
                    filtered = df[(df['open_time'] >= start_ms) & (df['open_time'] <= end_ms)]
                
                if not filtered.empty:
                    result[symbol] = filtered.copy()
        
        return result
    
    def get_tran_stats_between(self, begin_label: str, end_label: str, mode: str = '5min') -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的tran_stats数据
        
        Args:
            begin_label: 开始日期标签 (YYYYMMDD)
            end_label: 结束日期标签 (YYYYMMDD)
            mode: 时间间隔
        
        Returns:
            {symbol: DataFrame} 字典
        """
        # 解析日期标签
        start_date = datetime.strptime(begin_label, '%Y%m%d').replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(end_label, '%Y%m%d').replace(tzinfo=timezone.utc)
        
        # 初始化缓存
        self._initialize_cache(start_date, end_date, mode)
        
        # 从缓存中筛选数据
        result = {}
        for symbol, df in self._mock_data_cache['tran_stats'].items():
            if 'open_time' in df.columns:
                start_ms = int(start_date.timestamp() * 1000)
                end_ms = int(end_date.timestamp() * 1000)
                
                # 处理open_time字段类型（可能是datetime或int）
                if df['open_time'].dtype == 'datetime64[ns, UTC]' or 'datetime' in str(df['open_time'].dtype):
                    # 转换为毫秒时间戳
                    df_filtered = df.copy()
                    df_filtered['open_time'] = pd.to_datetime(df_filtered['open_time']).astype('int64') // 1_000_000
                    filtered = df_filtered[(df_filtered['open_time'] >= start_ms) & (df_filtered['open_time'] <= end_ms)]
                else:
                    # 已经是时间戳格式
                    filtered = df[(df['open_time'] >= start_ms) & (df['open_time'] <= end_ms)]
                
                if not filtered.empty:
                    result[symbol] = filtered.copy()
        
        return result


def get_mock_data_api(symbols: Optional[List[str]] = None, seed: Optional[int] = 42) -> MockDataAPI:
    """
    获取模拟数据API实例
    
    Args:
        symbols: 交易对列表
        seed: 随机种子
    
    Returns:
        MockDataAPI实例
    """
    return MockDataAPI(symbols=symbols, seed=seed)
