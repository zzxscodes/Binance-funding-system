"""
回测核心数据模型
定义回测过程中使用的各种数据结构
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from enum import Enum
import pandas as pd


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
    FLAT = "FLAT"  # 无持仓


@dataclass
class BacktestConfig:
    """回测配置"""
    name: str  # 回测名称
    start_date: datetime  # 开始日期
    end_date: datetime  # 结束日期
    initial_balance: float = 10000.0  # 初始资金
    symbols: List[str] = field(default_factory=list)  # 交易对列表
    leverage: float = 1.0  # 杠杆倍数（1-125）
    maker_fee: float = 0.0002  # 挂单手续费（0.02%）
    taker_fee: float = 0.0004  # 吃单手续费（0.04%）
    slippage: float = 0.0  # 滑点（万分之几）
    commission_type: str = "fixed"  # 手续费计算方式：fixed（固定）或percentage（百分比）
    funding_rate_apply: bool = True  # 是否应用资金费率


@dataclass
class KlineSnapshot:
    """K线快照 - 单个K线数据"""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_asset_volume: float  # 成交额（USDT）
    
    def __hash__(self):
        return hash((self.symbol, self.timestamp.timestamp()))


@dataclass
class HistoricalKline:
    """历史K线数据"""
    symbol: str
    interval: str  # "5m", "1h", "4h", etc.
    data: pd.DataFrame  # 包含 open_time, open, high, low, close, volume 等列
    
    def __len__(self):
        return len(self.data)
    
    def get_at_index(self, idx: int) -> Optional[KlineSnapshot]:
        """获取指定索引的K线"""
        if idx < 0 or idx >= len(self.data):
            return None
        row = self.data.iloc[idx]
        return KlineSnapshot(
            symbol=self.symbol,
            timestamp=pd.to_datetime(row['open_time']),
            open=float(row['open']),
            high=float(row['high']),
            low=float(row['low']),
            close=float(row['close']),
            volume=float(row.get('volume', 0)),
            quote_asset_volume=float(row.get('quote_asset_volume', 0))
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
    mode: PositionMode  # LONG, SHORT, FLAT
    quantity: float = 0.0  # 正数表示多头，负数表示空头
    entry_price: float = 0.0
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    accumulated_commission: float = 0.0
    entry_time: Optional[datetime] = None
    
    def update_price(self, new_price: float):
        """更新当前价格并计算未实现PnL"""
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
    used_margin: float
    total_pnl: float  # 总收益（已实现+未实现）
    realized_pnl: float = 0.0  # 已实现收益
    unrealized_pnl: float = 0.0  # 未实现收益
    positions: Dict[str, Position] = field(default_factory=dict)
    open_orders: Dict[str, Order] = field(default_factory=dict)
    trades_count: int = 0
    commission_paid: float = 0.0


@dataclass
class Trade:
    """交易记录（已成交）"""
    trade_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    executed_at: datetime
    commission: float = 0.0
    pnl: float = 0.0  # 仅适用于平仓交易
    
    @property
    def trade_value(self) -> float:
        """交易总价值（USDT）"""
        return self.quantity * self.price


@dataclass
class BacktestResult:
    """回测结果"""
    config: BacktestConfig
    trades: List[Trade] = field(default_factory=list)
    portfolio_history: List[PortfolioState] = field(default_factory=list)
    
    # 统计指标
    total_return: float = 0.0  # 总收益率 (%)
    annual_return: float = 0.0  # 年化收益率 (%)
    sharpe_ratio: float = 0.0  # 夏普比率
    max_drawdown: float = 0.0  # 最大回撤 (%)
    win_rate: float = 0.0  # 胜率 (%)
    profit_factor: float = 0.0  # 盈亏比
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_profit_per_trade: float = 0.0
    max_profit_per_trade: float = 0.0
    max_loss_per_trade: float = 0.0
    
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    execution_time_seconds: float = 0.0
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
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
                'total_return': self.total_return,
                'annual_return': self.annual_return,
                'sharpe_ratio': self.sharpe_ratio,
                'max_drawdown': self.max_drawdown,
                'win_rate': self.win_rate,
                'profit_factor': self.profit_factor,
                'total_trades': self.total_trades,
                'winning_trades': self.winning_trades,
                'losing_trades': self.losing_trades,
                'avg_profit_per_trade': self.avg_profit_per_trade,
                'max_profit_per_trade': self.max_profit_per_trade,
                'max_loss_per_trade': self.max_loss_per_trade,
            },
            'execution_time_seconds': self.execution_time_seconds,
        }


@dataclass
class ReplayEvent:
    """数据重放事件"""
    timestamp: datetime
    event_type: str  # "kline", "funding_rate", "signal", etc.
    symbol: str
    data: Dict  # 事件具体数据
