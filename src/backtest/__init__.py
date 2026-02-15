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
from .executor import (
    BacktestExecutor,
    FactorBacktestExecutor,
    run_backtest as run_backtest_new,
)
from .order_engine import (
    OrderEngine,
    OrderBook,
    Order,
    Trade as OrderEngineTrade,
    OrderType,
    OrderSide as OEOrderSide,
    OrderStatus as OEOrderStatus,
    SymbolInfo,
    TWAPExecutor,
)
from .metrics import (
    FactorMetrics,
    AlphaMetrics,
    FactorEvaluator,
    AlphaEvaluator,
    evaluate_factor_ic,
    evaluate_factor_group_return,
)
from .result_saver import BacktestResultSaver

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
    'BacktestExecutor',
    'FactorBacktestExecutor',
    'OrderEngine',
    'OrderBook',
    'Order',
    'OrderType',
    'OrderSide',
    'OrderStatus',
    'SymbolInfo',
    'TWAPExecutor',
    'FactorMetrics',
    'AlphaMetrics',
    'FactorEvaluator',
    'AlphaEvaluator',
    'evaluate_factor_ic',
    'evaluate_factor_group_return',
    'BacktestResultSaver',
]
