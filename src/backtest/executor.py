"""
回测执行引擎 - 重构版
对齐实盘业务逻辑：订单规范化、持仓管理、权重转换
"""
from typing import Dict, List, Optional, Callable, Any, Tuple
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
import uuid
import logging
import time

import pandas as pd
import numpy as np

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol, round_qty
from ..data.storage import get_data_storage
from ..data.api import get_data_api
from .models import (
    BacktestConfig, BacktestResult, PortfolioState, Trade, OrderSide,
    OrderStatus, PositionMode, KlineSnapshot, create_backtest_result
)
from .replay import DataReplayEngine
from .order_engine import (
    OrderEngine, OrderBook, Order, Trade as EngineTrade,
    OrderType, OrderSide as EngineOrderSide, OrderStatus as EngineOrderStatus,
    SymbolInfo, get_default_symbol_info, TWAPExecutor
)

logger = logging.getLogger('backtest_executor')


@dataclass
class BacktestOrder:
    """回测订单"""
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    filled_quantity: float = 0.0
    filled_price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    created_at: Optional[datetime] = None
    commission: float = 0.0


@dataclass
class BacktestPosition:
    """回测持仓"""
    symbol: str
    mode: PositionMode
    quantity: float = 0.0
    entry_price: float = 0.0
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    accumulated_commission: float = 0.0
    entry_time: Optional[datetime] = None


class BacktestExecutor:
    """
    回测执行引擎 - 重构版

    对齐实盘业务逻辑：
    1. 订单规范化（精度、最小金额）
    2. 权重转换为实际数量
    3. 持仓偏差计算
    4. 限价单/市价单撮合
    5. 部分成交模拟
    6. 滑点和市场冲击
    """

    def __init__(self, config: BacktestConfig, replay_engine: DataReplayEngine):
        self.config = config
        self.replay_engine = replay_engine

        self.order_engine = OrderEngine(
            initial_balance=config.initial_balance,
            taker_fee=config.taker_fee,
            maker_fee=config.maker_fee
        )

        self._init_symbols()
        self._register_symbols()

        self.portfolio_history: List[PortfolioState] = []
        self.backtest_trades: List[Trade] = []

        self._current_timestamp: Optional[datetime] = None
        self._current_prices: Dict[str, float] = {}

        self._slippage = config.slippage

        logger.info(f"Initialized BacktestExecutor: {config.name}, balance={config.initial_balance}")

    def _init_symbols(self):
        """初始化交易所信息"""
        self.exchange_info: Dict[str, SymbolInfo] = {}

        for symbol in self.config.symbols:
            info = self._get_symbol_info(symbol)
            self.exchange_info[symbol] = info

    def _get_symbol_info(self, symbol: str) -> SymbolInfo:
        """获取交易对信息（模拟）"""
        info = get_default_symbol_info(symbol)
        info.leverage = int(self.config.leverage)
        return info

    def _register_symbols(self):
        """注册所有交易对到订单引擎"""
        for symbol, info in self.exchange_info.items():
            self.order_engine.register_symbol(symbol, info)

    def run(
        self,
        strategy_func: Callable[[PortfolioState, Dict[str, KlineSnapshot]], Dict[str, float]],
        verbose: bool = True
    ) -> BacktestResult:
        """
        运行回测

        Args:
            strategy_func: 策略函数，接收(portfolio_state, klines)，返回目标持仓权重
            verbose: 是否打印进度

        Returns:
            BacktestResult
        """
        start_time = time.time()
        start_datetime = datetime.now(timezone.utc)

        if verbose:
            logger.info(f"Starting backtest: {self.config.name}")
            logger.info(f"Period: {self.replay_engine.start_date.date()} ~ {self.replay_engine.end_date.date()}")
            logger.info(f"Initial balance: ${self.config.initial_balance:,.0f}")
            logger.info(f"Leverage: {self.config.leverage}x")
            logger.info(f"Slippage: {self._slippage * 100:.2f} bps")

        self.portfolio_history = []
        self.backtest_trades = []

        step_count = 0
        try:
            for timestamp, klines_snapshot in self.replay_engine.replay_iterator():
                self._current_timestamp = timestamp
                self._update_prices(klines_snapshot)

                portfolio_state = self._get_portfolio_state()

                try:
                    target_weights = strategy_func(portfolio_state, klines_snapshot)
                except Exception as e:
                    logger.error(f"Strategy error at {timestamp}: {e}", exc_info=True)
                    target_weights = {}

                if target_weights:
                    self._execute_target_positions(target_weights, portfolio_state)

                self.portfolio_history.append(portfolio_state)

                step_count += 1
                if verbose and step_count % 1000 == 0:
                    logger.info(f"Progress: {step_count} steps, {timestamp}")

        except Exception as e:
            logger.error(f"Backtest error: {e}", exc_info=True)
            raise

        self._close_all_positions()

        end_time = time.time()
        end_datetime = datetime.now(timezone.utc)
        execution_time = end_time - start_time

        if verbose:
            logger.info(f"Backtest completed in {execution_time:.2f}s, {len(self.backtest_trades)} trades")

        result = self._generate_backtest_result(start_datetime, end_datetime, execution_time)
        return result

    def _update_prices(self, klines_snapshot: Dict[str, KlineSnapshot]):
        """更新当前价格"""
        self._current_prices = {}
        for symbol, kline in klines_snapshot.items():
            self._current_prices[symbol] = kline.close

            self.order_engine.update_market_data(
                symbol=symbol,
                open_=kline.open,
                high=kline.high,
                low=kline.low,
                close=kline.close,
                volume=kline.volume
            )

            if symbol in self.order_engine.positions:
                pos = self.order_engine.positions[symbol]
                pos['current_price'] = kline.close

    def _get_portfolio_state(self) -> PortfolioState:
        """获取投资组合状态"""
        prices = self._current_prices.copy()
        total_equity = self.order_engine.get_total_equity(prices)
        available_balance = self.order_engine.get_balance()
        used_margin = 0.0

        positions = {}
        unrealized_pnl = 0

        current_time = self._current_timestamp or datetime.now(timezone.utc)

        for symbol, pos_data in self.order_engine.positions.items():
            qty = pos_data['quantity']
            if abs(qty) > 1e-8:
                entry_price = pos_data.get('entry_price', 0) or 0.0
                current_price = prices.get(symbol, entry_price) or entry_price
                if qty > 0:
                    upnl = qty * (current_price - entry_price)
                else:
                    upnl = -qty * (entry_price - current_price)

                unrealized_pnl += upnl

                position_value = abs(qty) * current_price
                used_margin += position_value / max(self.config.leverage, 1.0)

                positions[symbol] = BacktestPosition(
                    symbol=symbol,
                    mode=PositionMode.LONG if qty > 0 else PositionMode.SHORT,
                    quantity=qty,
                    entry_price=pos_data['entry_price'],
                    current_price=current_price,
                    unrealized_pnl=upnl,
                    realized_pnl=pos_data['realized_pnl'],
                    accumulated_commission=pos_data['commission']
                )

        realized_pnl = sum(t.pnl for t in self.backtest_trades) if self.backtest_trades else 0
        total_pnl = realized_pnl + unrealized_pnl

        return PortfolioState(
            timestamp=current_time,
            total_balance=total_equity,
            available_balance=available_balance,
            used_margin=used_margin,
            total_pnl=total_pnl,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            positions=positions,
            trades_count=len(self.backtest_trades),
            commission_paid=sum(t.commission for t in self.backtest_trades) if self.backtest_trades else 0.0
        )

    def _convert_weights_to_quantities(
        self,
        target_weights: Dict[str, float],
        portfolio_state: PortfolioState
    ) -> Dict[str, float]:
        """将权重转换为实际数量（对齐实盘逻辑）"""
        quantities = {}

        total_weight = sum(abs(w) for w in target_weights.values())
        if total_weight < 1e-8:
            return {}

        normalized_weights = {sym: w / total_weight for sym, w in target_weights.items()}

        available_balance = portfolio_state.available_balance

        for symbol, weight in normalized_weights.items():
            if symbol not in self._current_prices:
                continue

            price = self._current_prices[symbol]
            if price <= 0:
                continue

            target_notional = abs(weight) * available_balance * self.config.leverage
            quantity = target_notional / price

            info = self.exchange_info.get(symbol)
            if info:
                quantity = round_qty(quantity, info.step_size)
                if quantity < info.min_qty:
                    continue

                notional = quantity * price
                if notional < info.min_notional:
                    continue

            if weight < 0:
                quantity = -quantity

            quantities[symbol] = quantity

        return quantities

    def _normalize_orders(
        self,
        orders: List[Dict],
        prices: Dict[str, float]
    ) -> List[Dict]:
        """规范化订单（对齐实盘逻辑）"""
        normalized = []

        for order in orders:
            symbol = order['symbol']
            quantity = order['quantity']
            info = self.exchange_info.get(symbol)

            if not info:
                continue

            quantity = round_qty(quantity, info.step_size)
            if quantity < info.min_qty:
                continue

            price = prices.get(symbol, 0)
            notional = quantity * price
            if notional < info.min_notional:
                continue

            order['normalized_quantity'] = quantity
            normalized.append(order)

        return normalized

    def _calculate_position_diff(
        self,
        target_quantities: Dict[str, float]
    ) -> List[Dict]:
        """计算持仓偏差，生成订单（对齐实盘逻辑）"""
        orders = []

        all_symbols = set(target_quantities.keys()) | set(self.order_engine.positions.keys())

        for symbol in all_symbols:
            target_qty = target_quantities.get(symbol, 0.0)
            pos_data = self.order_engine.positions.get(symbol, {'quantity': 0.0})
            current_qty = pos_data.get('quantity', 0.0)

            diff = target_qty - current_qty

            if abs(diff) < 1e-8:
                continue

            if diff > 0:
                side = EngineOrderSide.BUY
            else:
                side = EngineOrderSide.SELL

            reduce_only = False
            if current_qty > 0 and diff < 0:
                reduce_only = True
            elif current_qty < 0 and diff > 0:
                reduce_only = True

            orders.append({
                'symbol': symbol,
                'side': side,
                'quantity': abs(diff),
                'reduce_only': reduce_only,
                'current_position': current_qty,
                'target_position': target_qty,
            })

        return orders

    def _execute_target_positions(
        self,
        target_weights: Dict[str, float],
        portfolio_state: PortfolioState
    ):
        """执行目标持仓"""
        if not target_weights:
            return

        target_quantities = self._convert_weights_to_quantities(target_weights, portfolio_state)
        if not target_quantities:
            return

        orders = self._calculate_position_diff(target_quantities)
        if not orders:
            return

        normalized_orders = self._normalize_orders(orders, self._current_prices)

        for order in normalized_orders:
            symbol = order['symbol']
            side = order['side']
            quantity = order['normalized_quantity']
            reduce_only = order.get('reduce_only', False)

            self.order_engine.place_order(
                symbol=symbol,
                side=side,
                order_type=OrderType.MARKET,
                quantity=quantity,
                reduce_only=reduce_only
            )

        self._sync_trades()

    def _sync_trades(self):
        """同步订单引擎的交易到回测记录"""
        engine_trades = self.order_engine.get_trades()
        current_time = self._current_timestamp or datetime.now(timezone.utc)

        for engine_trade in engine_trades:
            trade_time = getattr(engine_trade, 'executed_at', None) or current_time

            trade = Trade(
                trade_id=engine_trade.trade_id,
                symbol=engine_trade.symbol,
                side=OrderSide.LONG if engine_trade.side == EngineOrderSide.BUY else OrderSide.SHORT,
                quantity=engine_trade.quantity,
                price=engine_trade.price,
                executed_at=trade_time,
                commission=engine_trade.commission,
                pnl=0.0
            )

            self._update_trade_pnl(trade)
            self.backtest_trades.append(trade)

    def _update_trade_pnl(self, trade: Trade):
        """更新交易盈亏"""
        pos = self.order_engine.positions.get(trade.symbol, {'quantity': 0.0})
        if pos:
            trade.pnl = pos.get('realized_pnl', 0.0)

    def _close_all_positions(self):
        """平仓所有持仓"""
        for symbol in list(self.order_engine.positions.keys()):
            pos_data = self.order_engine.positions.get(symbol)
            if pos_data and abs(pos_data['quantity']) > 1e-8:
                close_price = self._current_prices.get(symbol, pos_data['entry_price'])

                side = EngineOrderSide.SELL if pos_data['quantity'] > 0 else EngineOrderSide.BUY

                self.order_engine.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=OrderType.MARKET,
                    quantity=abs(pos_data['quantity']),
                    reduce_only=True
                )

        self._sync_trades()

    def _generate_backtest_result(
        self,
        start_datetime: datetime,
        end_datetime: datetime,
        execution_time: float
    ) -> BacktestResult:
        """生成回测结果"""
        result = create_backtest_result(
            config=self.config,
            portfolio_df=pd.DataFrame([
                {
                    'timestamp': s.timestamp,
                    'total_balance': s.total_balance,
                    'available_balance': s.available_balance,
                    'total_pnl': s.total_pnl,
                }
                for s in self.portfolio_history
            ]),
            trades=self.backtest_trades,
            factor_weights={},
            next_returns={},
        )

        result.start_time = start_datetime
        result.end_time = end_datetime
        result.execution_time_seconds = execution_time

        return result


class FactorBacktestExecutor:
    """
    因子回测执行器 - 兼容原有API
    """

    def __init__(
        self,
        config,
        replay_engine: Optional[DataReplayEngine] = None
    ):
        from .backtest import FactorBacktestConfig as OldConfig

        if isinstance(config, OldConfig):
            bt_config = BacktestConfig(
                name=config.name,
                start_date=config.start_date,
                end_date=config.end_date,
                initial_balance=config.initial_balance,
                symbols=config.symbols or [],
                leverage=config.leverage,
                interval=config.interval,
                capital_allocation=getattr(config, 'capital_allocation', 'equal_weight'),
                long_count=getattr(config, 'long_count', 10),
                short_count=getattr(config, 'short_count', 10),
            )
        else:
            bt_config = config

        if replay_engine is None and hasattr(bt_config, 'symbols'):
            replay_engine = DataReplayEngine(
                symbols=bt_config.symbols,
                start_date=bt_config.start_date,
                end_date=bt_config.end_date,
                interval=getattr(bt_config, 'interval', '5m')
            )

        if replay_engine is None:
            raise ValueError("replay_engine is required if config doesn't have necessary attributes")

        self.executor = BacktestExecutor(bt_config, replay_engine)

    def run(
        self,
        strategy_func: Callable,
        verbose: bool = True
    ) -> BacktestResult:
        """运行回测"""
        return self.executor.run(strategy_func, verbose=verbose)


def run_backtest(
    calculator,
    start_date: datetime,
    end_date: datetime,
    initial_balance: float = 10000.0,
    symbols: Optional[List[str]] = None,
    capital_allocation: str = "rank_weight",
    long_count: int = 5,
    short_count: int = 5,
    leverage: float = 1.0,
    verbose: bool = True,
) -> BacktestResult:
    """
    运行回测 - 兼容原有API

    Example:
        >>> from src.backtest import run_backtest
        >>> from src.strategy.calculators import MeanBuyDolvol4OverDolvolRankCalculator
        >>>
        >>> calc = MeanBuyDolvol4OverDolvolRankCalculator(lookback_bars=1000)
        >>> result = run_backtest(
        ...     calculator=calc,
        ...     start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ...     end_date=datetime(2024, 3, 31, tzinfo=timezone.utc),
        ... )
    """
    from .backtest import FactorBacktestConfig

    config = FactorBacktestConfig(
        name=f"bt_{getattr(calculator, 'name', 'unknown')}",
        start_date=start_date,
        end_date=end_date,
        initial_balance=initial_balance,
        symbols=symbols,
        capital_allocation=capital_allocation,
        long_count=long_count,
        short_count=short_count,
        leverage=leverage,
    )

    replay_engine = DataReplayEngine(
        symbols=config.symbols or ["BTCUSDT", "ETHUSDT"],
        start_date=config.start_date,
        end_date=config.end_date,
        interval="5m"
    )

    executor = BacktestExecutor(
        config=BacktestConfig(
            name=config.name,
            start_date=config.start_date,
            end_date=config.end_date,
            initial_balance=config.initial_balance,
            symbols=config.symbols or ["BTCUSDT", "ETHUSDT"],
            leverage=config.leverage,
            interval="5m",
        ),
        replay_engine=replay_engine
    )

    def strategy_wrapper(portfolio_state, klines):
        from .backtest import AlphaDataView

        view = AlphaDataView(
            bar_data=klines,
            tran_stats={},
            symbols=set(klines.keys()),
            copy_on_read=False
        )

        raw_weights = calculator.run(view)

        processed_weights = {}
        sorted_items = sorted(raw_weights.items(), key=lambda x: x[1], reverse=True)

        long_n = min(config.long_count, len(sorted_items) // 2)
        short_n = min(config.short_count, len(sorted_items) - long_n)

        for i, (sym, w) in enumerate(sorted_items):
            if i < long_n:
                processed_weights[sym] = config.leverage / long_n
            elif i >= len(sorted_items) - short_n and w < 0:
                processed_weights[sym] = -config.leverage / short_n

        return processed_weights

    return executor.run(strategy_wrapper, verbose=verbose)
