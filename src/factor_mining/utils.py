"""
因子挖掘系统工具函数

提供通用的工具函数，避免硬编码
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

from ..common.config import config
from ..common.logger import get_logger

logger = get_logger('factor_mining.utils')


def get_interval_timedelta(interval: str) -> timedelta:
    """
    根据interval字符串获取对应的timedelta
    
    Args:
        interval: 时间间隔字符串，如 '5min', '1h', '4h', '8h', '12h', '24h'
    
    Returns:
        对应的timedelta对象
    """
    interval_map = {
        '5min': timedelta(minutes=5),
        '1h': timedelta(hours=1),
        '4h': timedelta(hours=4),
        '8h': timedelta(hours=8),
        '12h': timedelta(hours=12),
        '24h': timedelta(hours=24),
    }
    
    if interval not in interval_map:
        logger.warning(f"未知的interval: {interval}, 使用默认值5min")
        return timedelta(minutes=5)
    
    return interval_map[interval]


def get_default_interval() -> str:
    """从配置获取默认时间间隔"""
    return config.get('strategy.factor_mining.evaluation.default_interval', '5min')


def get_default_forward_periods() -> int:
    """从配置获取默认未来收益周期数"""
    return int(config.get('strategy.factor_mining.evaluation.default_forward_periods', 1))


def get_default_history_days() -> int:
    """从配置获取默认历史数据天数"""
    return int(config.get('strategy.factor_mining.evaluation.default_history_days', 30))


def get_default_initial_balance() -> float:
    """从配置获取默认初始余额"""
    return float(config.get('strategy.factor_mining.evaluation.default_initial_balance', 10000.0))


def get_default_lookback_bars() -> int:
    """从配置获取默认回看K线数"""
    return int(config.get('strategy.factor_mining.generation.default_lookback_bars', 1000))
