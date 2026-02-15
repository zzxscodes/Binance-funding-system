"""
因子挖掘系统

功能：
1. 因子生成：基于模板和参数自动生成因子
2. 因子评估：IC、IR、回测评估
3. 因子筛选：相关性、稳定性筛选
4. 自动集成：自动生成calculator文件，与回测和实盘系统集成
"""

from .generator import FactorGenerator, FactorTemplate
from .evaluator import FactorEvaluator, FactorMetrics
from .selector import FactorSelector
from .backtest_integration import FactorBacktestRunner
from .live_integration import FactorLiveIntegrator
from .cli import FactorMiningCLI
from .result_saver import FactorMiningResultSaver
from .mock_data import MockDataAPI, get_mock_data_api
from .utils import (
    get_interval_timedelta,
    get_default_interval,
    get_default_forward_periods,
    get_default_history_days,
    get_default_initial_balance,
    get_default_lookback_bars,
)

__all__ = [
    'FactorGenerator',
    'FactorTemplate',
    'FactorEvaluator',
    'FactorMetrics',
    'FactorSelector',
    'FactorBacktestRunner',
    'FactorLiveIntegrator',
    'FactorMiningCLI',
    'get_interval_timedelta',
    'get_default_interval',
    'get_default_forward_periods',
    'get_default_history_days',
    'get_default_initial_balance',
    'get_default_lookback_bars',
    'FactorMiningResultSaver',
    'MockDataAPI',
    'get_mock_data_api',
]
