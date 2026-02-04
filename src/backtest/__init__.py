"""
回测模块 - 多因子量化交易回测系统
"""
from .models import (
    BacktestConfig,
    Order, Position, Trade, PortfolioState,
    BacktestResult, KlineSnapshot, HistoricalKline, ReplayEvent,
    OrderSide, OrderStatus, PositionMode,
    create_backtest_result,
)
from .replay import DataReplayEngine, MultiIntervalReplayEngine
from .backtest import (
    FactorBacktestConfig,
    WeightVector,
    TradeRecord,
    BacktestMetrics,
    MultiFactorBacktest,
    SingleCalculatorBacktest,
    AlphaBacktest,
    run_single_calculator_backtest,
    run_alpha_backtest,
    run_backtest,
    compare_calculators,
)
from .metrics import (
    FactorMetrics,
    AlphaMetrics,
    FactorEvaluator,
    AlphaEvaluator,
    evaluate_factor_ic,
    evaluate_factor_group_return,
)

__all__ = [
    'BacktestConfig',
    'Order', 'Position', 'Trade', 'PortfolioState',
    'BacktestResult', 'KlineSnapshot', 'HistoricalKline', 'ReplayEvent',
    'OrderSide', 'OrderStatus', 'PositionMode',
    'create_backtest_result',
    'DataReplayEngine', 'MultiIntervalReplayEngine',
    'FactorBacktestConfig',
    'WeightVector',
    'TradeRecord',
    'BacktestMetrics',
    'MultiFactorBacktest',
    'SingleCalculatorBacktest',
    'AlphaBacktest',
    'run_single_calculator_backtest',
    'run_alpha_backtest',
    'run_backtest',
    'compare_calculators',
    'FactorMetrics',
    'AlphaMetrics',
    'FactorEvaluator',
    'AlphaEvaluator',
    'evaluate_factor_ic',
    'evaluate_factor_group_return',
]
