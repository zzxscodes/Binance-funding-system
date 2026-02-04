"""
回测模块 - 历史数据重放和回测系统
提供完整的历史数据重放、策略回测、结果分析等功能
"""

from .models import (
    BacktestConfig, Order, Position, Trade, PortfolioState, 
    BacktestResult, KlineSnapshot, HistoricalKline, ReplayEvent,
    OrderSide, OrderStatus, PositionMode
)
from .replay import DataReplayEngine, MultiIntervalReplayEngine
from .executor import BacktestExecutor
from .api import BacktestAPI, create_backtest_config
from .analysis import BacktestAnalyzer
from .mock_data import MockKlineGenerator, MockDataManager, MockFundingRateGenerator

__all__ = [
    # Models
    'BacktestConfig', 'Order', 'Position', 'Trade', 'PortfolioState',
    'BacktestResult', 'KlineSnapshot', 'HistoricalKline', 'ReplayEvent',
    'OrderSide', 'OrderStatus', 'PositionMode',
    
    # Replay engine
    'DataReplayEngine', 'MultiIntervalReplayEngine',
    
    # Executor
    'BacktestExecutor',
    
    # API
    'BacktestAPI', 'create_backtest_config',
    
    # Analysis
    'BacktestAnalyzer',
    
    # Mock data
    'MockKlineGenerator', 'MockDataManager', 'MockFundingRateGenerator',
]
