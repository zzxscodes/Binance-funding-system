"""
多周期K线聚合器
从5分钟K线聚合生成1h, 4h, 8h, 12h, 24h等周期的K线
"""
import polars as pl
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import math

from ..common.logger import get_logger
from ..common.utils import format_symbol

logger = get_logger('multi_interval_aggregator')


class MultiIntervalAggregator:
    """多周期K线聚合器"""
    
    # 支持的周期映射（分钟数）
    INTERVAL_MAP = {
        '5min': 5,
        '1h': 60,
        '4h': 240,
        '8h': 480,
        '12h': 720,
        '24h': 1440,
    }
    
    def __init__(self):
        """初始化多周期聚合器"""
        pass
    
    def _get_window_start(self, timestamp: datetime, interval_minutes: int) -> datetime:
        """
        计算指定周期的窗口起始时间
        
        Args:
            timestamp: 时间戳
            interval_minutes: 周期（分钟）
        
        Returns:
            窗口起始时间
        """
        # 计算从当天0点开始的分钟数
        day_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        minutes_since_midnight = (timestamp - day_start).total_seconds() / 60
        
        # 计算窗口索引
        window_index = int(minutes_since_midnight // interval_minutes)
        
        # 计算窗口起始时间
        window_start = day_start + timedelta(minutes=window_index * interval_minutes)
        
        return window_start
    
    def aggregate_klines(
        self, 
        df_5min: pd.DataFrame, 
        target_interval: str
    ) -> pd.DataFrame:
        """
        从5分钟K线聚合生成指定周期的K线
        
        Args:
            df_5min: 5分钟K线DataFrame
            target_interval: 目标周期，如 '1h', '4h', '8h', '12h', '24h'
        
        Returns:
            聚合后的K线DataFrame
        """
        if df_5min.empty:
            return pd.DataFrame()
        
        if target_interval not in self.INTERVAL_MAP:
            raise ValueError(f"Unsupported interval: {target_interval}, supported: {list(self.INTERVAL_MAP.keys())}")
        
        if target_interval == '5min':
            # 已经是5分钟，直接返回
            return df_5min.copy()
        
        target_minutes = self.INTERVAL_MAP[target_interval]
        
        # 优化：如果已经是polars DataFrame，直接使用；否则转换
        if isinstance(df_5min, pl.DataFrame):
            df_pl = df_5min
        else:
            # 转换为Polars DataFrame以提高性能
            df_pl = pl.from_pandas(df_5min)
        
        # 使用polars直接计算窗口起始时间（避免pandas转换）
        # 计算从当天0点开始的分钟数，然后计算窗口索引
        df_pl = df_pl.with_columns([
            # 计算当天的分钟数偏移
            (
                (pl.col('open_time').dt.hour() * 60 + pl.col('open_time').dt.minute()) 
                // target_minutes * target_minutes
            ).alias('minutes_offset')
        ])
        
        # 计算窗口起始时间：当天0点 + minutes_offset
        df_pl = df_pl.with_columns([
            (
                pl.col('open_time').dt.date().cast(pl.Datetime('ns', time_zone='UTC')) +
                pl.duration(minutes=pl.col('minutes_offset'))
            ).alias('target_window_start')
        ])
        
        # 移除临时列
        df_pl = df_pl.drop('minutes_offset')
        
        # 按窗口聚合
        agg_result = df_pl.group_by('target_window_start').agg([
            # OHLC: 第一个open，最高high，最低low，最后一个close
            pl.first('open').alias('open'),
            pl.max('high').alias('high'),
            pl.min('low').alias('low'),
            pl.last('close').alias('close'),
            
            # 成交量：求和
            pl.sum('volume').alias('volume'),
            pl.sum('quote_volume').alias('quote_volume'),
            pl.sum('trade_count').alias('trade_count'),
            
            # 买卖量：求和
            pl.sum('buy_volume').alias('buy_volume'),
            pl.sum('sell_volume').alias('sell_volume'),
            pl.sum('buydolvol').alias('buydolvol'),
            pl.sum('selldolvol').alias('selldolvol'),
            pl.sum('buyvolume').alias('buyvolume'),
            pl.sum('sellvolume').alias('sellvolume'),
            pl.sum('buytradecount').alias('buytradecount'),
            pl.sum('selltradecount').alias('selltradecount'),
            
            # 分档统计：求和
            pl.sum('buy_volume1').alias('buy_volume1'),
            pl.sum('buy_volume2').alias('buy_volume2'),
            pl.sum('buy_volume3').alias('buy_volume3'),
            pl.sum('buy_volume4').alias('buy_volume4'),
            pl.sum('buy_dolvol1').alias('buy_dolvol1'),
            pl.sum('buy_dolvol2').alias('buy_dolvol2'),
            pl.sum('buy_dolvol3').alias('buy_dolvol3'),
            pl.sum('buy_dolvol4').alias('buy_dolvol4'),
            pl.sum('buy_trade_count1').alias('buy_trade_count1'),
            pl.sum('buy_trade_count2').alias('buy_trade_count2'),
            pl.sum('buy_trade_count3').alias('buy_trade_count3'),
            pl.sum('buy_trade_count4').alias('buy_trade_count4'),
            pl.sum('sell_volume1').alias('sell_volume1'),
            pl.sum('sell_volume2').alias('sell_volume2'),
            pl.sum('sell_volume3').alias('sell_volume3'),
            pl.sum('sell_volume4').alias('sell_volume4'),
            pl.sum('sell_dolvol1').alias('sell_dolvol1'),
            pl.sum('sell_dolvol2').alias('sell_dolvol2'),
            pl.sum('sell_dolvol3').alias('sell_dolvol3'),
            pl.sum('sell_dolvol4').alias('sell_dolvol4'),
            pl.sum('sell_trade_count1').alias('sell_trade_count1'),
            pl.sum('sell_trade_count2').alias('sell_trade_count2'),
            pl.sum('sell_trade_count3').alias('sell_trade_count3'),
            pl.sum('sell_trade_count4').alias('sell_trade_count4'),
            
            # 时间字段：第一个open_time，最后一个close_time
            pl.first('open_time').alias('open_time'),
            pl.last('close_time').alias('close_time'),
            
            # 其他字段：保留第一个值
            pl.first('symbol').alias('symbol'),
            pl.first('span_status').alias('span_status'),
        ])
        
        # 计算VWAP（成交量加权平均价）
        # VWAP = sum(price * volume) / sum(volume)
        # 对于聚合K线，使用quote_volume / volume
        agg_result = agg_result.with_columns([
            (pl.col('quote_volume') / pl.col('volume')).alias('vwap')
        ])
        
        # 处理volume为0的情况（VWAP为nan）
        agg_result = agg_result.with_columns([
            pl.when(pl.col('volume') == 0)
            .then(float('nan'))
            .otherwise(pl.col('vwap'))
            .alias('vwap')
        ])
        
        # 计算其他字段
        agg_result = agg_result.with_columns([
            pl.col('close').alias('last'),
            pl.col('quote_volume').alias('dolvol'),
            pl.col('target_window_start').alias('span_begin_datetime'),
            (pl.col('target_window_start') + pl.duration(minutes=target_minutes)).alias('span_end_datetime'),
        ])
        
        # 计算time_label（基于目标周期，使用polars）
        agg_result = agg_result.with_columns([
            (
                (pl.col('target_window_start').dt.hour() * 60 + 
                 pl.col('target_window_start').dt.minute()) // target_minutes + 1
            ).alias('time_lable')
        ])
        
        # 按时间排序
        agg_result = agg_result.sort('open_time')
        
        # 优化：如果输入是polars DataFrame，尝试返回polars DataFrame
        # 但为了保持API兼容性，仍然转换为pandas
        # 注意：这里可以进一步优化，让调用方直接使用polars DataFrame
        result_df = agg_result.to_pandas()
        
        # 清理polars DataFrame引用
        del agg_result
        
        # 确保时间字段格式正确（polars转换后应该已经是datetime，但确保一下）
        if 'open_time' in result_df.columns:
            result_df['open_time'] = pd.to_datetime(result_df['open_time'])
        if 'close_time' in result_df.columns:
            result_df['close_time'] = pd.to_datetime(result_df['close_time'])
        
        return result_df


# 全局实例
_multi_interval_aggregator: Optional[MultiIntervalAggregator] = None


def get_multi_interval_aggregator() -> MultiIntervalAggregator:
    """获取多周期聚合器实例"""
    global _multi_interval_aggregator
    if _multi_interval_aggregator is None:
        _multi_interval_aggregator = MultiIntervalAggregator()
    return _multi_interval_aggregator
