"""
回测系统工具函数

提供通用的工具函数，避免硬编码
"""

from __future__ import annotations

from typing import Optional

from ..common.config import config
from ..common.logger import get_logger
from .config import BacktestConfigManager

logger = get_logger('backtest.utils')


def get_default_initial_balance() -> float:
    """从配置获取默认初始余额"""
    # 优先从backtest配置读取
    value = BacktestConfigManager.get_config('backtest.default_backtest.initial_balance', None)
    if value is not None:
        return float(value)
    
    # 从因子挖掘配置读取（如果存在）
    value = config.get('strategy.factor_mining.evaluation.default_initial_balance', None)
    if value is not None:
        return float(value)
    
    # 默认值
    return 10000.0


def get_default_interval() -> str:
    """从配置获取默认时间间隔"""
    # 优先从backtest配置读取
    value = BacktestConfigManager.get_config('backtest.replay.interval', None)
    if value is not None:
        return str(value)
    
    # 从策略配置读取
    value = config.get('strategy.calculation_interval', None)
    if value is not None:
        return str(value)
    
    # 从数据配置读取
    value = config.get('data.kline_interval', None)
    if value is not None:
        return str(value)
    
    # 默认值
    return '5m'


def get_default_buffer_size() -> int:
    """从配置获取默认缓冲区大小"""
    return int(BacktestConfigManager.get_config('backtest.replay.buffer_size', 1000))


def get_default_maker_fee() -> float:
    """从配置获取默认挂单手续费"""
    return float(BacktestConfigManager.get_config('backtest.execution.maker_fee', 0.0002))


def get_default_taker_fee() -> float:
    """从配置获取默认吃单手续费"""
    return float(BacktestConfigManager.get_config('backtest.execution.taker_fee', 0.0004))


def get_default_leverage() -> float:
    """从配置获取默认杠杆"""
    return float(BacktestConfigManager.get_config('backtest.execution.leverage', 1.0))


def get_available_balance_ratio() -> float:
    """从配置获取可用余额比例（用于计算available_balance）"""
    # 从配置读取，如果没有则使用默认值0.5（50%）
    return float(config.get('backtest.portfolio.available_balance_ratio', 0.5))


def get_trading_days_per_year() -> int:
    """从配置获取每年交易日数（用于年化计算）"""
    # 标准值：252个交易日/年，但允许配置
    return int(config.get('backtest.metrics.trading_days_per_year', 252))


def get_annualization_factor() -> float:
    """从配置获取年化因子（用于Sharpe比率等指标计算）"""
    trading_days = get_trading_days_per_year()
    return trading_days ** 0.5


def get_progress_log_interval() -> int:
    """从配置获取进度日志输出间隔（步数）"""
    return int(config.get('backtest.execution.progress_log_interval', 1000))


def get_backtest_result_dir() -> Path:
    """从配置获取回测结果输出目录"""
    from pathlib import Path
    # 使用BacktestConfigManager读取配置（支持嵌套路径）
    result_dir = BacktestConfigManager.get_config('backtest.output.result_dir', 'data/backtest_results')
    result_path = Path(result_dir)
    result_path.mkdir(parents=True, exist_ok=True)
    return result_path


def get_save_json() -> bool:
    """从配置获取是否保存JSON格式结果"""
    return bool(BacktestConfigManager.get_config('backtest.output.save_json', True))


def get_save_csv() -> bool:
    """从配置获取是否保存CSV格式结果"""
    return bool(BacktestConfigManager.get_config('backtest.output.save_csv', True))


def get_save_report() -> bool:
    """从配置获取是否保存文本报告"""
    return bool(BacktestConfigManager.get_config('backtest.output.save_report', True))


def get_save_config() -> bool:
    """从配置获取是否保存配置文件"""
    return bool(BacktestConfigManager.get_config('backtest.output.save_config', True))
