"""
回测订单引擎
模拟真实订单簿和撮合逻辑，支持限价单、市价单、部分成交等
"""
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from collections import defaultdict
import uuid
import math
import logging

from ..common.logger import get_logger
from ..common.config import config
from ..common.utils import round_qty

logger = logging.getLogger('order_engine')


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    PENDING = "PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class PositionSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    BOTH = "BOTH"


@dataclass
class Order:
    """订单"""
    order_id: str
    symbol: str
    order_type: OrderType
    side: OrderSide
    quantity: float
    price: Optional[float] = None
    filled_quantity: float = 0.0
    average_price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    filled_at: Optional[datetime] = None
    reduce_only: bool = False
    commission: float = 0.0
    commission_asset: str = "USDT"

    @property
    def remaining_quantity(self) -> float:
        return self.quantity - self.filled_quantity

    @property
    def is_active(self) -> bool:
        return self.status in [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]


@dataclass
class Trade:
    """成交记录"""
    trade_id: str
    symbol: str
    order_id: str
    side: OrderSide
    quantity: float
    price: float
    executed_at: datetime
    commission: float = 0.0
    commission_asset: str = "USDT"
    maker: bool = False


@dataclass
class SymbolInfo:
    """交易对精度信息"""
    symbol: str
    tick_size: float = 0.01
    step_size: float = 0.001
    min_qty: float = 0.001
    min_notional: float = 5.0
    max_orders: int = 200
    leverage: int = 20


class OrderBook:
    """模拟订单簿"""

    def __init__(self, symbol: str, symbol_info: SymbolInfo):
        self.symbol = symbol
        self.symbol_info = symbol_info
        self.bids: List[Tuple[float, float]] = []  # [(price, quantity), ...]
        self.asks: List[Tuple[float, float]] = []   # [(price, quantity), ...]
        self.last_price: float = 0.0
        self.last_trade_price: float = 0.0
        self._trade_count = 0

    def update_market_price(self, open_: float, high: float, low: float, close: float, volume: float):
        """根据K线数据更新市场价格"""
        self.last_price = close

        spread_pct = 0.0005  # 0.05% 买卖价差
        mid_price = (high + low) / 2
        bid_price = mid_price * (1 - spread_pct)
        ask_price = mid_price * (1 + spread_pct)

        bid_price = self._round_price(bid_price)
        ask_price = self._round_price(ask_price)

        base_volume = volume / 1000
        bid_depth = base_volume * (0.3 + 0.2 * abs(close - open_) / open_)
        ask_depth = base_volume * (0.3 + 0.2 * abs(close - open_) / open_)

        self.bids = self._generate_depth(bid_price, bid_depth, is_bid=True)
        self.asks = self._generate_depth(ask_price, ask_depth, is_bid=False)

        self.last_trade_price = close

    def _round_price(self, price: float) -> float:
        tick = self.symbol_info.tick_size
        return math.floor(price / tick) * tick

    def _generate_depth(self, center_price: float, total_volume: float, is_bid: bool) -> List[Tuple[float, float]]:
        levels = []
        remaining = total_volume
        level_count = min(10, max(3, int(total_volume / 10)))

        for i in range(level_count):
            if remaining <= 0:
                break

            if is_bid:
                price = center_price * (1 - 0.0001 * (i + 1))
            else:
                price = center_price * (1 + 0.0001 * (i + 1))

            price = self._round_price(price)
            qty = remaining / level_count * (1 - 0.1 * i)
            qty = round_qty(qty, self.symbol_info.step_size)

            if qty >= self.symbol_info.min_qty:
                levels.append((price, qty))
                remaining -= qty

        return levels

    def get_best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None

    def get_best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None

    def get_spread(self) -> float:
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if bid and ask:
            return ask - bid
        return 0.0

    def get_market_impact_price(self, quantity: float, side: OrderSide) -> float:
        """计算执行指定数量对市场价格的影响"""
        if side == OrderSide.BUY:
            depth = sorted(self.asks, key=lambda x: x[0])
            remaining = quantity
            total_cost = 0
            for price, avail in depth:
                if remaining <= 0:
                    break
                filled = min(remaining, avail)
                total_cost += filled * price
                remaining -= filled
            return total_cost / quantity if quantity > 0 else self.last_price
        else:
            depth = sorted(self.bids, key=lambda x: -x[0])
            remaining = quantity
            total_cost = 0
            for price, avail in depth:
                if remaining <= 0:
                    break
                filled = min(remaining, avail)
                total_cost += filled * price
                remaining -= filled
            return total_cost / quantity if quantity > 0 else self.last_price


class OrderEngine:
    """
    回测订单引擎
    支持：
    - 限价单、市价单
    - 部分成交
    - 滑点模拟
    - 市场冲击模拟
    """

    def __init__(self, initial_balance: float = 100000.0, taker_fee: float = 0.0004, maker_fee: float = 0.0002, leverage: float = 1.0):
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.taker_fee = taker_fee
        self.maker_fee = maker_fee
        self.leverage = leverage  # 杠杆倍数，用于计算保证金要求

        self.orders: Dict[str, Order] = {}
        self.trades: List[Trade] = []
        self.positions: Dict[str, Dict] = defaultdict(lambda: {
            'quantity': 0.0,
            'entry_price': 0.0,
            'realized_pnl': 0.0,
            'commission': 0.0
        })

        self.order_books: Dict[str, OrderBook] = {}
        self.symbol_info: Dict[str, SymbolInfo] = {}

        self._current_time: Optional[datetime] = None

    def register_symbol(self, symbol: str, symbol_info: SymbolInfo):
        """注册交易对信息"""
        self.symbol_info[symbol] = symbol_info
        self.order_books[symbol] = OrderBook(symbol, symbol_info)
        logger.debug(f"Registered symbol {symbol} with info: tick={symbol_info.tick_size}, step={symbol_info.step_size}")

    def set_time(self, timestamp: datetime):
        """设置当前时间"""
        self._current_time = timestamp

    def update_market_data(self, symbol: str, open_: float, high: float, low: float, close: float, volume: float):
        """更新市场数据"""
        if symbol in self.order_books:
            self.order_books[symbol].update_market_price(open_, high, low, close, volume)
            # 推进撮合：检查该 symbol 的挂单是否因价格变化而触发成交
            self._process_pending_limit_orders(symbol)

    def _process_pending_limit_orders(self, symbol: str) -> None:
        """
        处理挂单撮合（回测）：
        - 当 best_ask/best_bid 穿越 limit 价格时，按当前最优价成交剩余数量
        - 当前实现不做复杂的部分成交深度模拟（深度已在 OrderBook 中体现，挂单触发后按最优价一次成交）
        """
        ob = self.order_books.get(symbol)
        if ob is None:
            return

        best_bid = ob.get_best_bid()
        best_ask = ob.get_best_ask()
        if best_bid is None and best_ask is None:
            return

        # 遍历当前所有 LIMIT 活跃单
        for order in list(self.orders.values()):
            if order.symbol != symbol:
                continue
            if order.order_type != OrderType.LIMIT:
                continue
            if order.status not in [OrderStatus.PENDING, OrderStatus.PARTIALLY_FILLED]:
                continue
            if order.price is None:
                continue

            # 买单：价格 >= best_ask 时可成交；卖单：价格 <= best_bid 时可成交
            if order.side == OrderSide.BUY and best_ask is not None and order.price >= best_ask:
                self._execute_limit_order_at_market(order, market_price=best_ask)
            elif order.side == OrderSide.SELL and best_bid is not None and order.price <= best_bid:
                self._execute_limit_order_at_market(order, market_price=best_bid)

    def place_order(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: float,
        price: Optional[float] = None,
        reduce_only: bool = False,
        time_in_force: str = "GTC"
    ) -> Order:
        """
        下单

        Args:
            symbol: 交易对
            side: 买入/卖出
            order_type: 限价单/市价单
            quantity: 数量
            price: 价格（限价单必需）
            reduce_only: 是否只减仓
            time_in_force: 有效期（GTC, IOC, FOK）
        """
        if symbol not in self.symbol_info:
            raise ValueError(f"Symbol {symbol} not registered")

        info = self.symbol_info[symbol]

        quantity = round_qty(quantity, info.step_size)
        if quantity < info.min_qty:
            raise ValueError(f"Quantity {quantity} < min_qty {info.min_qty}")

        if price is not None:
            price = round(price / info.tick_size) * info.tick_size

        order_id = str(uuid.uuid4())[:8]
        order = Order(
            order_id=order_id,
            symbol=symbol,
            order_type=order_type,
            side=side,
            quantity=quantity,
            price=price,
            reduce_only=reduce_only,
            created_at=self._current_time or datetime.now(timezone.utc)
        )

        self.orders[order_id] = order

        if order_type == OrderType.MARKET:
            self._execute_market_order(order)
        else:
            self._add_limit_order(order, time_in_force)

        return order

    def _execute_market_order(self, order: Order):
        """执行市价单"""
        if order.symbol not in self.order_books:
            order.status = OrderStatus.REJECTED
            return

        ob = self.order_books[order.symbol]
        remaining = order.quantity
        total_cost = 0
        filled_qty = 0

        if order.side == OrderSide.BUY:
            depth = sorted(ob.asks, key=lambda x: x[0])
        else:
            depth = sorted(ob.bids, key=lambda x: -x[0])

        for price, available in depth:
            if remaining <= 0:
                break

            filled = min(remaining, available)
            market_impact = 1.0

            slippage = config.get('backtest.slippage', 0.0002)
            if order.side == OrderSide.BUY:
                exec_price = price * (1 + slippage + market_impact * 0.0001)
            else:
                exec_price = price * (1 - slippage - market_impact * 0.0001)

            exec_price = round(exec_price / ob.symbol_info.tick_size) * ob.symbol_info.tick_size

            commission = filled * exec_price * self.taker_fee
            total_cost += filled * exec_price + (commission if order.side == OrderSide.BUY else -commission)
            filled_qty += filled
            remaining -= filled

            trade = Trade(
                trade_id=str(uuid.uuid4())[:8],
                symbol=order.symbol,
                order_id=order.order_id,
                side=order.side,
                quantity=filled,
                price=exec_price,
                executed_at=self._current_time or datetime.now(timezone.utc),
                commission=commission,
                maker=False
            )
            self.trades.append(trade)
            ob.last_trade_price = exec_price

        if filled_qty > 0:
            avg_price = total_cost / filled_qty if order.side == OrderSide.BUY else total_cost / filled_qty
            order.filled_quantity = filled_qty
            order.average_price = avg_price
            order.status = OrderStatus.FILLED if remaining == 0 else OrderStatus.PARTIALLY_FILLED
            order.filled_at = self._current_time

            # 在更新持仓之前，检查是开仓还是平仓
            pos = self.positions[order.symbol]
            existing_qty = pos.get('quantity', 0.0)
            is_opening = (existing_qty == 0) or \
                        (existing_qty > 0 and order.side == OrderSide.BUY) or \
                        (existing_qty < 0 and order.side == OrderSide.SELL)
            
            self._update_position(order, avg_price, filled_qty)
            
            # 在杠杆交易中，扣除的是保证金（margin = notional / leverage），而不是全额
            notional = filled_qty * avg_price
            margin_amount = notional / max(self.leverage, 1.0)
            
            if is_opening:
                # 开仓：扣除保证金
                self.balance -= (margin_amount + commission)
            else:
                # 平仓：释放之前占用的保证金，盈亏已在_update_position中计算
                # 释放的保证金 = 平仓数量对应的保证金
                released_margin = margin_amount
                # 平仓时，盈亏已经反映在realized_pnl中，这里只需要释放保证金
                self.balance += released_margin - commission

            logger.debug(f"Market order filled: {order.symbol} {order.side} {filled_qty} @ {avg_price}")
        else:
            order.status = OrderStatus.REJECTED

    def _add_limit_order(self, order: Order, time_in_force: str):
        """添加限价单到订单簿"""
        if order.price is None:
            order.status = OrderStatus.REJECTED
            return

        ob = self.order_books.get(order.symbol)
        if ob is None:
            order.status = OrderStatus.REJECTED
            return

        best_bid = ob.get_best_bid()
        best_ask = ob.get_best_ask()

        if order.side == OrderSide.BUY and best_ask and order.price >= best_ask:
            self._execute_limit_order_immediately(order, best_ask)
        elif order.side == OrderSide.SELL and best_bid and order.price <= best_bid:
            self._execute_limit_order_immediately(order, best_bid)
        else:
            order.status = OrderStatus.PENDING
            logger.debug(f"Limit order placed: {order.symbol} {order.side} {order.quantity} @ {order.price}")

    def _execute_limit_order_immediately(self, order: Order, market_price: float):
        """限价单立即成交（吃单）"""
        self._execute_limit_order_at_market(order, market_price=market_price)

    def _execute_limit_order_at_market(self, order: Order, market_price: float) -> None:
        """
        将限价单在当前市场最优价成交剩余数量（触发吃单）。
        """
        remaining = order.remaining_quantity
        if remaining <= 0:
            return

        exec_price = market_price
        commission = remaining * exec_price * self.taker_fee

        order.filled_quantity += remaining
        # average price：本引擎暂不模拟多次分段成交，remaining 一次性成交即可
        order.average_price = exec_price
        order.status = OrderStatus.FILLED
        order.filled_at = self._current_time
        order.commission += commission

        trade = Trade(
            trade_id=str(uuid.uuid4())[:8],
            symbol=order.symbol,
            order_id=order.order_id,
            side=order.side,
            quantity=remaining,
            price=exec_price,
            executed_at=self._current_time or datetime.now(timezone.utc),
            commission=commission,
            maker=False
        )
        self.trades.append(trade)

        # 在更新持仓之前，检查是开仓还是平仓
        pos = self.positions[order.symbol]
        existing_qty = pos.get('quantity', 0.0)
        is_opening = (existing_qty == 0) or \
                    (existing_qty > 0 and order.side == OrderSide.BUY) or \
                    (existing_qty < 0 and order.side == OrderSide.SELL)
        
        self._update_position(order, exec_price, remaining)
        
        # 在杠杆交易中，扣除的是保证金（margin = notional / leverage），而不是全额
        notional = remaining * exec_price
        margin_amount = notional / max(self.leverage, 1.0)
        
        if is_opening:
            # 开仓：扣除保证金
            self.balance -= (margin_amount + commission)
        else:
            # 平仓：释放之前占用的保证金，盈亏已在_update_position中计算
            released_margin = margin_amount
            self.balance += released_margin - commission

        logger.debug(f"Limit order filled: {order.symbol} {order.side} {remaining} @ {exec_price}")

    def _update_position(self, order: Order, price: float, filled_qty: float):
        """更新持仓"""
        pos = self.positions[order.symbol]

        if pos['quantity'] == 0:
            pos['quantity'] = filled_qty if order.side == OrderSide.BUY else -filled_qty
            pos['entry_price'] = price
        else:
            existing = pos['quantity']
            if (existing > 0 and order.side == OrderSide.BUY) or (existing < 0 and order.side == OrderSide.SELL):
                total_qty = abs(existing) + filled_qty
                avg_price = (abs(existing) * pos['entry_price'] + filled_qty * price) / total_qty
                pos['quantity'] = existing + (filled_qty if order.side == OrderSide.BUY else -filled_qty)
                pos['entry_price'] = avg_price
            else:
                if abs(filled_qty) >= abs(existing):
                    pos['realized_pnl'] += existing * (price - pos['entry_price']) if existing > 0 else -existing * (pos['entry_price'] - price)
                    pos['quantity'] = -filled_qty + existing if order.side == OrderSide.BUY else filled_qty + existing
                    pos['entry_price'] = price
                else:
                    pos['realized_pnl'] += filled_qty * (price - pos['entry_price']) if existing > 0 else -filled_qty * (pos['entry_price'] - price)
                    pos['quantity'] += filled_qty if order.side == OrderSide.BUY else -filled_qty

        pos['commission'] += order.commission if hasattr(order, 'commission') else 0

    def cancel_order(self, order_id: str) -> bool:
        """取消订单"""
        if order_id in self.orders:
            order = self.orders[order_id]
            if order.is_active:
                order.status = OrderStatus.CANCELLED
                logger.debug(f"Order cancelled: {order_id}")
                return True
        return False

    def cancel_all_orders(self, symbol: Optional[str] = None) -> int:
        """取消所有订单"""
        count = 0
        for order_id, order in self.orders.items():
            if order.is_active and (symbol is None or order.symbol == symbol):
                order.status = OrderStatus.CANCELLED
                count += 1
        return count

    def get_order_status(self, order_id: str) -> Optional[Order]:
        """获取订单状态"""
        return self.orders.get(order_id)

    def get_positions(self) -> Dict[str, Dict]:
        """获取当前持仓"""
        result = {}
        for symbol, pos in self.positions.items():
            if abs(pos['quantity']) > 1e-8:
                result[symbol] = dict(pos)
        return result

    def get_balance(self) -> float:
        """获取可用余额"""
        return self.balance

    def get_total_equity(self, prices: Dict[str, float]) -> float:
        """计算总权益"""
        total = self.balance
        for symbol, pos in self.positions.items():
            if abs(pos['quantity']) > 1e-8 and symbol in prices:
                position_value = abs(pos['quantity']) * prices[symbol]
                if pos['quantity'] > 0:
                    unrealized = pos['quantity'] * (prices[symbol] - pos['entry_price'])
                else:
                    unrealized = -pos['quantity'] * (pos['entry_price'] - prices[symbol])
                total += position_value + pos['realized_pnl'] - pos['commission'] + unrealized
        return total

    def get_trades(self) -> List[Trade]:
        """获取所有成交"""
        return self.trades.copy()

    def reset(self):
        """重置引擎状态"""
        self.balance = self.initial_balance
        self.orders.clear()
        self.trades.clear()
        self.positions.clear()
        for ob in self.order_books.values():
            ob.bids.clear()
            ob.asks.clear()
        logger.debug("Order engine reset")


class TWAPExecutor:
    """TWAP执行器 - 时间加权平均"""

    def __init__(self, order_engine: OrderEngine, default_interval_minutes: int = 5):
        self.engine = order_engine
        self.default_interval = default_interval_minutes

    def execute(
        self,
        symbol: str,
        side: OrderSide,
        total_quantity: float,
        duration_minutes: int = 60,
        interval_minutes: Optional[int] = None,
        price_type: str = "VWAP"
    ) -> List[Order]:
        """
        执行TWAP订单

        Args:
            symbol: 交易对
            side: 买入/卖出
            total_quantity: 总数量
            duration_minutes: 执行时长（分钟）
            interval_minutes: 每次下单间隔
            price_type: 价格类型 (VWAP, TWAP, MARKET)

        Returns:
            生成的订单列表
        """
        if interval_minutes is None:
            interval_minutes = self.default_interval

        num_splits = max(1, int(duration_minutes / interval_minutes))
        quantity_per_order = total_quantity / num_splits

        info = self.engine.symbol_info.get(symbol)
        if info:
            quantity_per_order = round_qty(quantity_per_order, info.step_size)

        orders = []
        for i in range(num_splits):
            if quantity_per_order < (info.min_qty if info else 0.001):
                break

            order = self.engine.place_order(
                symbol=symbol,
                side=side,
                order_type=OrderType.MARKET,
                quantity=quantity_per_order
            )
            orders.append(order)

        logger.info(f"TWAP executed: {symbol} {side} {total_quantity} in {len(orders)} splits")
        return orders


def get_default_symbol_info(symbol: str) -> SymbolInfo:
    """获取默认的交易对信息"""
    defaults = {
        'BTCUSDT': SymbolInfo(symbol='BTCUSDT', tick_size=0.01, step_size=0.001, min_qty=0.001, min_notional=5.0),
        'ETHUSDT': SymbolInfo(symbol='ETHUSDT', tick_size=0.01, step_size=0.001, min_qty=0.001, min_notional=5.0),
    }
    return defaults.get(symbol, SymbolInfo(symbol=symbol))
