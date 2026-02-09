"""
回测执行引擎 - 重构版
对齐实盘业务逻辑：订单规范化、持仓管理、权重转换
"""
from typing import Dict, List, Optional, Callable, Any, Tuple
from datetime import datetime, timezone, timedelta
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
from ..execution.execution_method_selector import (
    ExecutionMethodSelector,
    AccountSnapshot,
    SymbolFeatures,
    PerformanceSnapshot,
    METHOD_MARKET,
    METHOD_LIMIT,
    METHOD_TWAP,
    METHOD_VWAP,
    METHOD_CANCEL_ALL_ORDERS,
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
            leverage=config.leverage,
            maker_fee=config.maker_fee
        )

        self._init_symbols()
        self._register_symbols()

        self.portfolio_history: List[PortfolioState] = []
        self.backtest_trades: List[Trade] = []

        self._current_timestamp: Optional[datetime] = None
        self._current_prices: Dict[str, float] = {}

        self._slippage = config.slippage

        # Execution method selector (shared with live execution)
        # 使用配置适配器，合并BacktestConfig和全局config
        from .config_adapter import BacktestConfigAdapter
        config_adapter = BacktestConfigAdapter(config)
        self.method_selector = ExecutionMethodSelector(config_adapter)
        
        # 资金费率数据（从存储加载）
        self._funding_rates_cache: Dict[str, pd.DataFrame] = {}
        self._last_funding_rate_apply_time: Dict[str, datetime] = {}
        self._load_funding_rates()

        # Scheduled child orders for TWAP/VWAP (executed across future bars)
        self._scheduled_orders: List[Dict] = []
        self._stop_trading: bool = False

        # Trade sync cursor (avoid duplicate sync)
        self._synced_trade_count: int = 0

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

                # Execute any scheduled child orders (TWAP/VWAP) due at this timestamp
                self._execute_scheduled_orders(timestamp)
                
                # Apply funding rates (every 8 hours)
                self._apply_funding_rates(timestamp)

                portfolio_state = self._get_portfolio_state()

                try:
                    target_weights = strategy_func(portfolio_state, klines_snapshot)
                except Exception as e:
                    logger.error(f"Strategy error at {timestamp}: {e}", exc_info=True)
                    target_weights = {}

                if (not self._stop_trading) and target_weights:
                    self._execute_target_positions(target_weights, portfolio_state)

                self.portfolio_history.append(portfolio_state)

                # Risk control check (backtest/live consistent)
                try:
                    perf = self._get_performance_snapshot()
                    acct = AccountSnapshot(
                        total_wallet_balance=float(portfolio_state.total_balance),
                        available_balance=float(portfolio_state.available_balance),
                    )
                    decision = self.method_selector.select_global_action(acct, perf)
                    if decision and decision.method == METHOD_CANCEL_ALL_ORDERS:
                        # Cancel all active orders and stop further trading
                        self.order_engine.cancel_all_orders(symbol=None)
                        self._scheduled_orders.clear()
                        self._stop_trading = True
                        logger.warning(f"Backtest CANCEL_ALL_ORDERS triggered at {timestamp}: {decision.reason}")
                except Exception:
                    pass

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

            # 应用杠杆：在计算notional时应用杠杆，使可用资金放大
            # 例如：10000 USDT余额，20倍杠杆，可用资金 = 10000 * 20 = 200000 USDT
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

            self._place_order_with_dynamic_method(
                symbol=symbol,
                side=side,
                quantity=quantity,
                reduce_only=reduce_only,
                portfolio_state=portfolio_state
            )

        self._sync_trades()

    def _sync_trades(self):
        """同步订单引擎的交易到回测记录"""
        engine_trades = self.order_engine.get_trades()
        if self._synced_trade_count < 0:
            self._synced_trade_count = 0
        new_trades = engine_trades[self._synced_trade_count:]
        self._synced_trade_count = len(engine_trades)
        current_time = self._current_timestamp or datetime.now(timezone.utc)

        for engine_trade in new_trades:
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

    def _execute_scheduled_orders(self, timestamp: datetime) -> None:
        """执行到期的计划订单（用于TWAP/VWAP跨bar执行）"""
        if not self._scheduled_orders:
            return

        due = [o for o in self._scheduled_orders if o['execute_at'] <= timestamp]
        if not due:
            return

        self._scheduled_orders = [o for o in self._scheduled_orders if o['execute_at'] > timestamp]

        for o in due:
            self.order_engine.place_order(
                symbol=o['symbol'],
                side=o['side'],
                order_type=OrderType.MARKET,
                quantity=o['quantity'],
                reduce_only=o.get('reduce_only', False)
            )
        self._sync_trades()

    def _parse_interval_minutes(self, interval: str) -> int:
        s = str(interval).strip().lower()
        if not s:
            raise ValueError("Empty interval")
        if s.endswith('m'):
            return int(s[:-1])
        if s.endswith('h'):
            return int(s[:-1]) * 60
        raise ValueError(f"Unsupported interval format: {interval}")

    def _get_symbol_features(self, symbol: str, timestamp: datetime) -> SymbolFeatures:
        """
        Backtest features from replay history up to timestamp.
        Percent values are fractions (0.02 == 2%).
        """
        ms_cfg = config.get('execution.method_selection')
        if not ms_cfg or not isinstance(ms_cfg, dict):
            return SymbolFeatures()
        features_cfg = ms_cfg.get('features')
        if not features_cfg or not isinstance(features_cfg, dict):
            # 如果配置缺失，返回空的SymbolFeatures（而不是抛出异常）
            return SymbolFeatures()
        min_bars = features_cfg.get('min_bars')
        lookback_days = features_cfg.get('lookback_days')
        if min_bars is None or lookback_days is None:
            raise ValueError("Missing config keys: execution.method_selection.features.min_bars/lookback_days")

        hist = self.replay_engine.kline_data.get(symbol)
        if hist is None:
            return SymbolFeatures()
        df = hist.data
        if df is None or df.empty or 'open_time' not in df.columns:
            return SymbolFeatures()

        ts = pd.to_datetime(timestamp)
        open_times = pd.to_datetime(df['open_time'])
        mask = open_times <= ts
        df2 = df.loc[mask].tail(int(min_bars) * int(lookback_days))  # coarse cap
        if len(df2) < int(min_bars):
            return SymbolFeatures()

        vol = None
        try:
            if 'close' in df2.columns:
                ret = pd.to_numeric(df2['close'], errors='coerce').pct_change().dropna()
                vol = float(ret.std()) if len(ret) > 0 else None
        except Exception:
            vol = None

        avg_dolvol = None
        for col in ['dolvol', 'quote_volume', 'quoteVolume']:
            if col in df2.columns:
                try:
                    avg_dolvol = float(pd.to_numeric(df2[col], errors='coerce').dropna().mean())
                except Exception:
                    avg_dolvol = None
                break

        vwap_dev = None
        try:
            if 'vwap' in df2.columns and 'close' in df2.columns:
                v = pd.to_numeric(df2['vwap'], errors='coerce')
                c = pd.to_numeric(df2['close'], errors='coerce')
                valid = (c > 0) & v.notna()
                if valid.any():
                    vwap_dev = float(((c[valid] - v[valid]).abs() / c[valid]).mean())
        except Exception:
            vwap_dev = None

        return SymbolFeatures(volatility_pct=vol, avg_dolvol=avg_dolvol, vwap_deviation_pct=vwap_dev)

    def _get_performance_snapshot(self) -> PerformanceSnapshot:
        """
        Compute performance from portfolio_history (fraction format).
        """
        try:
            if not self.portfolio_history:
                return PerformanceSnapshot()
            equity = [float(s.total_balance) for s in self.portfolio_history if s is not None]
            if not equity:
                return PerformanceSnapshot()
            initial = equity[0]
            if initial <= 0:
                return PerformanceSnapshot(data_points=len(equity))
            current = equity[-1]
            total_return = current / initial - 1.0
            cummax = np.maximum.accumulate(np.array(equity))
            dd = (np.array(equity) - cummax).min() / initial
            return PerformanceSnapshot(
                total_return_pct=float(total_return),
                max_drawdown_pct=abs(float(dd)),
                data_points=len(equity),
            )
        except Exception:
            return PerformanceSnapshot()

    def _place_order_with_dynamic_method(
        self,
        symbol: str,
        side: EngineOrderSide,
        quantity: float,
        reduce_only: bool,
        portfolio_state: PortfolioState
    ) -> None:
        """
        Place order using the shared selector.
        """
        # Account snapshot (fraction percent)
        acct = AccountSnapshot(
            total_wallet_balance=float(portfolio_state.total_balance),
            available_balance=float(portfolio_state.available_balance),
        )

        # Current price
        px = float(self._current_prices.get(symbol, 0) or 0)
        notional = px * float(quantity) if px > 0 else None
        notional_pct_equity = None
        if notional is not None and acct.total_wallet_balance and acct.total_wallet_balance > 0:
            notional_pct_equity = float(notional) / float(acct.total_wallet_balance)

        feats = self._get_symbol_features(symbol, self._current_timestamp or datetime.now(timezone.utc))
        impact_pct = None
        if notional is not None and feats.avg_dolvol and feats.avg_dolvol > 0:
            impact_pct = float(notional) / float(feats.avg_dolvol)

        decision = self.method_selector.select_for_order(
            symbol=symbol,
            side='BUY' if side == EngineOrderSide.BUY else 'SELL',
            reduce_only=bool(reduce_only),
            order_notional=notional,
            order_notional_pct_equity=notional_pct_equity,
            liquidity_impact_pct=impact_pct,
            features=feats,
            account=acct,
        )

        method = decision.method.upper()
        if method == METHOD_LIMIT:
            limit_cfg = config.get('execution.method_selection.limit')
            if not limit_cfg or not isinstance(limit_cfg, dict):
                raise ValueError("Missing config section: execution.method_selection.limit")
            offset_pct = limit_cfg.get('price_offset_pct')
            if offset_pct is None:
                raise ValueError("Missing config key: execution.method_selection.limit.price_offset_pct")

            if px <= 0:
                method = METHOD_MARKET
            else:
                raw_price = px * (1.0 - float(offset_pct)) if side == EngineOrderSide.BUY else px * (1.0 + float(offset_pct))
                tick = float(self.exchange_info[symbol].tick_size)
                # BUY 向下，SELL 向上
                import math
                p_steps = raw_price / tick if tick > 0 else raw_price
                limit_price = (math.floor(p_steps) * tick) if side == EngineOrderSide.BUY else (math.ceil(p_steps) * tick)
                self.order_engine.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=OrderType.LIMIT,
                    quantity=quantity,
                    price=limit_price,
                    reduce_only=reduce_only
                )
                return

        if method in [METHOD_TWAP, METHOD_VWAP]:
            self._schedule_child_orders(symbol, side, quantity, reduce_only, method)
            return

        # MARKET
        self.order_engine.place_order(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=quantity,
            reduce_only=reduce_only
        )

    def _schedule_child_orders(self, symbol: str, side: EngineOrderSide, total_qty: float, reduce_only: bool, method: str) -> None:
        """
        Schedule TWAP/VWAP child orders over future bars.
        """
        interval = config.get('data.kline_interval')
        if not interval:
            raise ValueError("Missing config key: data.kline_interval")
        interval_minutes = self._parse_interval_minutes(interval)

        if method == METHOD_VWAP:
            duration_minutes = config.get('execution.vwap.default_duration_minutes')
        else:
            duration_minutes = config.get('execution.twap.default_duration_minutes')
        if duration_minutes is None:
            raise ValueError("Missing config key: execution.twap.default_duration_minutes / execution.vwap.default_duration_minutes")

        num_splits = max(1, int(int(duration_minutes) / int(interval_minutes)))
        if num_splits <= 1:
            self.order_engine.place_order(symbol=symbol, side=side, order_type=OrderType.MARKET, quantity=total_qty, reduce_only=reduce_only)
            return

        # Build quantities
        quantities: List[float] = []
        info = self.exchange_info.get(symbol)
        step = float(info.step_size) if info else 0.001
        min_qty = float(info.min_qty) if info else 0.001

        if method == METHOD_VWAP:
            # VWAP-like distribution based on historical average dolvol by minute slot
            hist = self.replay_engine.kline_data.get(symbol)
            weights = [1.0] * num_splits
            if hist is not None and hist.data is not None and (not hist.data.empty) and 'open_time' in hist.data.columns:
                df = hist.data
                df = df.copy()
                df['open_time'] = pd.to_datetime(df['open_time'])
                # choose volume column
                vol_col = None
                for c in ['dolvol', 'quote_volume', 'quoteVolume', 'volume']:
                    if c in df.columns:
                        vol_col = c
                        break
                if vol_col:
                    df[vol_col] = pd.to_numeric(df[vol_col], errors='coerce')
                    df = df.dropna(subset=[vol_col])
                    if not df.empty:
                        slot = (df['open_time'].dt.minute // interval_minutes) * interval_minutes
                        avg = df.groupby(slot)[vol_col].mean().to_dict()
                        for i in range(num_splits):
                            t = (self._current_timestamp or datetime.now(timezone.utc)) + timedelta(minutes=i * interval_minutes)
                            s = (t.minute // interval_minutes) * interval_minutes
                            weights[i] = float(avg.get(s, 1.0))

            total_w = sum(weights) if sum(weights) > 0 else float(num_splits)
            for w in weights:
                q = total_qty * (float(w) / float(total_w))
                q = round_qty(q, step)
                quantities.append(q)
        else:
            q = round_qty(total_qty / num_splits, step)
            quantities = [q] * num_splits

        # Normalize quantities to sum close to total_qty (rounding may reduce)
        quantities = [q for q in quantities if q >= min_qty]
        if not quantities:
            self.order_engine.place_order(symbol=symbol, side=side, order_type=OrderType.MARKET, quantity=total_qty, reduce_only=reduce_only)
            return

        base_time = self._current_timestamp or datetime.now(timezone.utc)
        for i, q in enumerate(quantities):
            exec_at = base_time + timedelta(minutes=i * interval_minutes)
            self._scheduled_orders.append({
                'execute_at': exec_at,
                'symbol': symbol,
                'side': side,
                'quantity': q,
                'reduce_only': reduce_only,
                'method': method,
            })

    def _update_trade_pnl(self, trade: Trade):
        """更新交易盈亏"""
        pos = self.order_engine.positions.get(trade.symbol, {'quantity': 0.0})
        if pos:
            trade.pnl = pos.get('realized_pnl', 0.0)
    
    def _load_funding_rates(self):
        """加载资金费率数据"""
        try:
            from ..data.storage import get_data_storage
            storage = get_data_storage()
            
            for symbol in self.config.symbols:
                try:
                    funding_rates_df = storage.load_funding_rates(
                        symbol=symbol,
                        start_date=self.replay_engine.start_date,
                        end_date=self.replay_engine.end_date
                    )
                    if not funding_rates_df.empty and 'fundingRate' in funding_rates_df.columns:
                        self._funding_rates_cache[symbol] = funding_rates_df
                        logger.debug(f"Loaded {len(funding_rates_df)} funding rate records for {symbol}")
                except Exception as e:
                    logger.warning(f"Failed to load funding rates for {symbol}: {e}")
        except Exception as e:
            logger.warning(f"Failed to load funding rates: {e}")
    
    def _get_funding_rate(self, symbol: str, timestamp: datetime) -> Optional[float]:
        """获取指定时间点的资金费率"""
        if symbol not in self._funding_rates_cache:
            return None
        
        df = self._funding_rates_cache[symbol]
        if df.empty or 'fundingTime' not in df.columns or 'fundingRate' not in df.columns:
            return None
        
        # 找到时间戳之前最近的一次资金费率
        df['fundingTime'] = pd.to_datetime(df['fundingTime'])
        before_timestamp = df[df['fundingTime'] <= timestamp]
        
        if not before_timestamp.empty:
            latest = before_timestamp.sort_values('fundingTime').iloc[-1]
            return float(latest['fundingRate'])
        
        # 如果没有之前的数据，使用之后最近的一次
        after_timestamp = df[df['fundingTime'] > timestamp]
        if not after_timestamp.empty:
            earliest = after_timestamp.sort_values('fundingTime').iloc[0]
            return float(earliest['fundingRate'])
        
        return None
    
    def _apply_funding_rates(self, timestamp: datetime):
        """应用资金费率（每8小时一次）"""
        # 资金费率每8小时收取一次（00:00, 08:00, 16:00 UTC）
        funding_hours = [0, 8, 16]
        current_hour = timestamp.hour
        
        # 检查是否是资金费率收取时间
        if current_hour not in funding_hours:
            return
        
        # 检查是否已经在这个小时应用过（避免重复应用）
        for symbol in self.order_engine.positions.keys():
            last_apply_time = self._last_funding_rate_apply_time.get(symbol)
            if last_apply_time and last_apply_time.date() == timestamp.date() and last_apply_time.hour == current_hour:
                continue
            
            pos_data = self.order_engine.positions.get(symbol)
            if not pos_data or abs(pos_data['quantity']) < 1e-8:
                continue
            
            # 获取资金费率
            funding_rate = self._get_funding_rate(symbol, timestamp)
            if funding_rate is None:
                continue
            
            # 计算持仓市值
            current_price = self._current_prices.get(symbol, pos_data.get('entry_price', 0))
            if current_price <= 0:
                continue
            
            position_value = abs(pos_data['quantity']) * current_price
            
            # 计算资金费率成本
            # 资金费率：多仓支付费率，空仓收取费率（如果费率为正）
            # 如果费率为正：多仓支付，空仓收取
            # 如果费率为负：多仓收取，空仓支付
            position_qty = pos_data['quantity']
            if position_qty > 0:  # 多仓
                funding_cost = position_value * funding_rate
            else:  # 空仓
                funding_cost = -position_value * funding_rate  # 空仓方向相反
            
            # 从余额中扣除资金费率成本
            self.order_engine.balance -= funding_cost
            
            # 记录应用时间
            self._last_funding_rate_apply_time[symbol] = timestamp
            
            logger.debug(
                f"Applied funding rate for {symbol} at {timestamp}: "
                f"rate={funding_rate:.6f}, cost={funding_cost:.4f} USDT, "
                f"position_value={position_value:.2f} USDT"
            )

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