"""
回测核心数据模型
定义回测过程中使用的各种数据结构
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
import pandas as pd

from .utils import get_default_initial_balance, get_default_interval, get_default_leverage, get_default_maker_fee, get_default_taker_fee


class OrderSide(Enum):
    """订单方向"""
    LONG = "LONG"
    SHORT = "SHORT"


class OrderStatus(Enum):
    """订单状态"""
    PENDING = "PENDING"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class PositionMode(Enum):
    """持仓模式"""
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"


@dataclass
class BacktestConfig:
    """回测配置"""
    name: str
    start_date: datetime
    end_date: datetime
    initial_balance: float = field(default_factory=get_default_initial_balance)
    symbols: List[str] = field(default_factory=list)
    leverage: float = field(default_factory=get_default_leverage)
    maker_fee: float = field(default_factory=get_default_maker_fee)
    taker_fee: float = field(default_factory=get_default_taker_fee)
    slippage: float = 0.0
    commission_type: str = "fixed"
    funding_rate_apply: bool = True
    capital_allocation: str = "equal_weight"
    long_count: int = 10
    short_count: int = 10
    min_weight: float = 0.02
    max_weight: float = 0.2
    interval: str = field(default_factory=get_default_interval)


@dataclass
class KlineSnapshot:
    """K线快照"""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_asset_volume: float
    
    def __hash__(self):
        return hash((self.symbol, self.timestamp.timestamp()))


@dataclass
class HistoricalKline:
    """历史K线数据"""
    symbol: str
    interval: str
    data: pd.DataFrame
    
    def __len__(self):
        return len(self.data)
    
    def get_at_index(self, idx: int) -> Optional[KlineSnapshot]:
        if idx < 0 or idx >= len(self.data):
            return None
        row = self.data.iloc[idx]
        
        # 根据图片要求，bar表字段使用varchar/int类型，需要转换为float
        # 辅助函数：安全转换为float
        def safe_float(val, default=0.0):
            if pd.isna(val):
                return default
            try:
                return float(val)
            except (ValueError, TypeError):
                return default
        
        return KlineSnapshot(
            symbol=self.symbol,
            timestamp=pd.to_datetime(row['open_time']),
            open=safe_float(row.get('open', 0)),
            high=safe_float(row.get('high', 0)),
            low=safe_float(row.get('low', 0)),
            close=safe_float(row.get('close', 0)),
            volume=safe_float(row.get('volume', 0)),
            quote_asset_volume=safe_float(row.get('quote_asset_volume', 0))
        )


@dataclass
class Order:
    """订单信息"""
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    filled_quantity: float = 0.0
    filled_price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    filled_at: Optional[datetime] = None
    commission: float = 0.0
    comment: str = ""


@dataclass
class Position:
    """持仓信息"""
    symbol: str
    mode: PositionMode
    quantity: float = 0.0
    entry_price: float = 0.0
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    accumulated_commission: float = 0.0
    entry_time: Optional[datetime] = None
    
    def update_price(self, new_price: float):
        self.current_price = new_price
        if self.quantity != 0:
            self.unrealized_pnl = self.quantity * (new_price - self.entry_price)
        else:
            self.unrealized_pnl = 0.0


@dataclass
class PortfolioState:
    """投资组合状态"""
    timestamp: datetime
    total_balance: float
    available_balance: float
    used_margin: float = 0.0
    total_pnl: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    positions: Dict[str, Position] = field(default_factory=dict)
    open_orders: Dict[str, Order] = field(default_factory=dict)
    trades_count: int = 0
    commission_paid: float = 0.0


@dataclass
class Trade:
    """交易记录"""
    trade_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    executed_at: datetime
    commission: float = 0.0
    pnl: float = 0.0
    
    @property
    def trade_value(self) -> float:
        return self.quantity * self.price


@dataclass
class BacktestResult:
    """
    统一回测结果
    
    兼容旧系统和新多因子系统
    """
    config: BacktestConfig
    trades: List[Trade] = field(default_factory=list)
    portfolio_history: List[PortfolioState] = field(default_factory=list)
    
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    execution_time_seconds: float = 0.0
    
    factor_weights: Dict[datetime, Dict[str, float]] = field(default_factory=dict)
    next_returns: Dict[datetime, Dict[str, float]] = field(default_factory=dict)
    
    # 收益指标
    total_return: float = 0.0
    annual_return: float = 0.0
    monthly_returns: List[float] = field(default_factory=list)
    daily_returns: List[float] = field(default_factory=list)
    
    # 风险指标
    volatility: float = 0.0
    downside_volatility: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    
    # 回撤指标
    max_drawdown: float = 0.0
    max_drawdown_duration: int = 0
    avg_drawdown: float = 0.0
    
    # 交易统计
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    avg_trade: float = 0.0
    avg_winning_trade: float = 0.0
    avg_losing_trade: float = 0.0
    best_trade: float = 0.0
    worst_trade: float = 0.0
    avg_profit_per_trade: float = 0.0
    max_profit_per_trade: float = 0.0
    max_loss_per_trade: float = 0.0
    
    # 多空统计
    long_trades: int = 0
    short_trades: int = 0
    long_win_rate: float = 0.0
    short_win_rate: float = 0.0
    long_return: float = 0.0
    short_return: float = 0.0
    
    # 因子评估
    ic_mean: float = 0.0
    ic_std: float = 0.0
    icir: float = 0.0
    rank_ic_mean: float = 0.0
    selection_accuracy: float = 0.0
    long_spread: float = 0.0
    group_return_spread: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            'config': {
                'name': self.config.name,
                'start_date': self.config.start_date.isoformat(),
                'end_date': self.config.end_date.isoformat(),
                'initial_balance': self.config.initial_balance,
                'symbols': self.config.symbols,
                'leverage': self.config.leverage,
            },
            'trades': [
                {
                    'trade_id': t.trade_id,
                    'symbol': t.symbol,
                    'side': t.side.value,
                    'quantity': t.quantity,
                    'price': t.price,
                    'executed_at': t.executed_at.isoformat(),
                    'commission': t.commission,
                    'pnl': t.pnl,
                }
                for t in self.trades
            ],
            'metrics': {
                '收益指标': {
                    '总收益率': f"{self.total_return:.2f}%",
                    '年化收益率': f"{self.annual_return:.2f}%",
                },
                '风险指标': {
                    '夏普比率': f"{self.sharpe_ratio:.2f}",
                    '最大回撤': f"{self.max_drawdown:.2f}%",
                    '索提诺比率': f"{self.sortino_ratio:.2f}",
                },
                '交易统计': {
                    '总交易数': self.total_trades,
                    '胜率': f"{self.win_rate:.2f}%",
                    '盈亏比': f"{self.profit_factor:.2f}",
                },
            },
            '因子评估': {
                'IC均值': f"{self.ic_mean:.4f}",
                'ICIR': f"{self.icir:.4f}",
                'Rank IC': f"{self.rank_ic_mean:.4f}",
                '选币准确率': f"{self.selection_accuracy:.2f}%",
            },
            'execution_time_seconds': self.execution_time_seconds,
        }


@dataclass
class ReplayEvent:
    """数据重放事件"""
    timestamp: datetime
    event_type: str
    symbol: str
    data: Dict


def create_backtest_result(
    config: BacktestConfig,
    portfolio_df: pd.DataFrame,
    trades: List[Trade],
    factor_weights: Optional[Dict[datetime, Dict[str, float]]] = None,
    next_returns: Optional[Dict[datetime, Dict[str, float]]] = None,
) -> BacktestResult:
    """创建回测结果"""
    from .metrics import AlphaEvaluator, FactorEvaluator
    
    result = BacktestResult(
        config=config,
        trades=trades,
        factor_weights=factor_weights or {},
        next_returns=next_returns or {},
    )
    
    if portfolio_df is not None and not portfolio_df.empty:
        result.portfolio_history = []
        for _, row in portfolio_df.iterrows():
            result.portfolio_history.append(PortfolioState(
                timestamp=row.get('timestamp', datetime.now(timezone.utc)),
                total_balance=row.get('total_balance', config.initial_balance),
                available_balance=row.get('available_balance', config.initial_balance),
                total_pnl=row.get('total_pnl', 0),
            ))
        
        portfolio_values = portfolio_df['total_balance'].tolist()
        
        if len(portfolio_values) >= 2:
            alpha_metrics = AlphaEvaluator.evaluate_alpha(
                name=config.name,
                portfolio_values=portfolio_values,
                trades=[{
                    'symbol': t.symbol,
                    'side': t.side.value,
                    'pnl': t.pnl,
                } for t in trades],
                initial_balance=config.initial_balance,
            )
            
            result.total_return = alpha_metrics.total_return
            result.annual_return = alpha_metrics.annual_return
            result.volatility = alpha_metrics.volatility
            result.sharpe_ratio = alpha_metrics.sharpe_ratio
            result.sortino_ratio = alpha_metrics.sortino_ratio
            result.max_drawdown = alpha_metrics.max_drawdown
            result.max_drawdown_duration = alpha_metrics.max_drawdown_duration
            result.win_rate = alpha_metrics.win_rate
            result.profit_factor = alpha_metrics.profit_factor
            result.avg_trade = alpha_metrics.avg_trade
            result.best_trade = alpha_metrics.best_trade
            result.worst_trade = alpha_metrics.worst_trade
            result.long_trades = alpha_metrics.long_trades
            result.short_trades = alpha_metrics.short_trades
            result.long_win_rate = alpha_metrics.long_win_rate
            result.short_win_rate = alpha_metrics.short_win_rate
    
    if factor_weights and next_returns:
        factor_metrics = FactorEvaluator.evaluate_factor(
            name="factor",
            factor_weights=factor_weights,
            next_returns=next_returns,
        )
        result.ic_mean = factor_metrics.ic_mean
        result.ic_std = factor_metrics.ic_std
        result.icir = factor_metrics.icir
        result.rank_ic_mean = factor_metrics.rank_ic_mean
        result.selection_accuracy = factor_metrics.selection_accuracy
        result.long_spread = factor_metrics.long_spread
        result.group_return_spread = factor_metrics.group_return_spread
    
    return result
