"""
回测执行引擎
执行回测策略，模拟订单、持仓、账户资金等
"""
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timezone
from collections import defaultdict
import uuid
import logging

from ..common.logger import get_logger
from .models import (
    BacktestConfig, Order, Position, Trade, PortfolioState, OrderSide,
    OrderStatus, PositionMode, KlineSnapshot, BacktestResult
)
from .replay import DataReplayEngine

logger = get_logger('backtest_engine')


class BacktestExecutor:
    """
    回测执行引擎
    驱动整个回测过程：
    1. 迭代历史K线数据
    2. 调用策略生成目标持仓
    3. 执行订单、更新持仓和账户状态
    4. 记录所有交易和账户状态
    """
    
    def __init__(self, config: BacktestConfig, replay_engine: DataReplayEngine):
        """
        初始化回测执行器
        
        Args:
            config: 回测配置
            replay_engine: 数据重放引擎
        """
        self.config = config
        self.replay_engine = replay_engine
        
        # 账户状态
        self.initial_balance = config.initial_balance
        self.total_balance = config.initial_balance
        self.available_balance = config.initial_balance
        self.used_margin = 0.0
        
        # 持仓追踪
        self.positions: Dict[str, Position] = {}
        self.open_orders: Dict[str, Order] = {}
        self.trades: List[Trade] = []
        
        # 历史数据
        self.portfolio_history: List[PortfolioState] = []
        
        # 手续费
        self.total_commission = 0.0
        self.maker_fee = config.maker_fee
        self.taker_fee = config.taker_fee
        
        # 状态追踪
        self._current_timestamp: Optional[datetime] = None
        self._current_prices: Dict[str, float] = {}
        self._unrealized_pnl = 0.0
        
        logger.info(f"Initialized BacktestExecutor: {config.name}, balance={config.initial_balance}")
    
    def run(self, strategy_func: Callable[[PortfolioState, Dict[str, KlineSnapshot]], Dict[str, float]]) -> BacktestResult:
        """
        运行回测
        
        Args:
            strategy_func: 策略函数，接收(portfolio_state, klines)，返回目标持仓权重Dict[symbol, weight]
        
        Returns:
            BacktestResult 回测结果
        """
        import time
        start_time = time.time()
        start_datetime = datetime.now(timezone.utc)
        
        logger.info(f"Starting backtest: {self.config.name}")
        
        try:
            # 迭代重放所有数据
            step_count = 0
            for timestamp, klines_snapshot in self.replay_engine.replay_iterator():
                self._current_timestamp = timestamp
                
                # 1. 更新当前价格
                self._update_prices(klines_snapshot)
                
                # 2. 获取投资组合状态
                portfolio_state = self._get_portfolio_state()
                
                # 3. 调用策略获取目标持仓
                try:
                    target_weights = strategy_func(portfolio_state, klines_snapshot)
                except Exception as e:
                    logger.error(f"Strategy error at {timestamp}: {e}", exc_info=True)
                    target_weights = {}
                
                # 4. 执行目标持仓
                if target_weights:
                    self._execute_target_positions(target_weights, portfolio_state)
                
                # 5. 记录账户状态
                self.portfolio_history.append(portfolio_state)
                
                step_count += 1
                if step_count % 1000 == 0:
                    logger.debug(f"Backtest progress: {step_count} steps, {timestamp}")
            
            # 平仓所有持仓
            self._close_all_positions()
            
            end_time = time.time()
            end_datetime = datetime.now(timezone.utc)
            execution_time = end_time - start_time
            
            logger.info(f"Backtest completed in {execution_time:.2f}s, {len(self.trades)} trades")
            
            # 生成回测结果
            result = self._generate_backtest_result(start_datetime, end_datetime, execution_time)
            
            return result
            
        except Exception as e:
            logger.error(f"Backtest failed: {e}", exc_info=True)
            raise
    
    def _update_prices(self, klines_snapshot: Dict[str, KlineSnapshot]):
        """更新所有交易对的当前价格"""
        for symbol, kline in klines_snapshot.items():
            self._current_prices[symbol] = kline.close
            
            # 更新对应持仓的当前价格和未实现PnL
            if symbol in self.positions:
                self.positions[symbol].update_price(kline.close)
    
    def _get_portfolio_state(self) -> PortfolioState:
        """获取当前投资组合状态"""
        # 计算已实现和未实现PnL
        realized_pnl = sum(t.pnl for t in self.trades)
        unrealized_pnl = sum(p.unrealized_pnl for p in self.positions.values() if p.quantity != 0)
        total_pnl = realized_pnl + unrealized_pnl
        
        # 更新账户余额
        self.total_balance = self.initial_balance + total_pnl - self.total_commission
        self.available_balance = self.total_balance - self.used_margin
        
        # 计算已用保证金（简单实现：假设1倍杠杆下需要的保证金）
        self.used_margin = 0.0
        for position in self.positions.values():
            if position.quantity != 0:
                # 假设使用杠杆，所需保证金 = 仓位价值 / 杠杆倍数
                position_value = abs(position.quantity * position.current_price)
                self.used_margin += position_value / max(self.config.leverage, 1.0)
        
        return PortfolioState(
            timestamp=self._current_timestamp,
            total_balance=self.total_balance,
            available_balance=self.available_balance,
            used_margin=self.used_margin,
            total_pnl=total_pnl,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            positions=dict(self.positions),
            open_orders=dict(self.open_orders),
            trades_count=len(self.trades),
            commission_paid=self.total_commission
        )
    
    def _execute_target_positions(self, target_weights: Dict[str, float], portfolio_state: PortfolioState):
        """
        执行目标持仓
        
        Args:
            target_weights: Dict[symbol, weight] 目标持仓权重
            portfolio_state: 当前投资组合状态
        """
        # 将权重转换为实际数量
        target_quantities = self._weights_to_quantities(target_weights, portfolio_state)
        
        # 对每个交易对，计算需要的订单
        for symbol in list(self.positions.keys()) + list(target_quantities.keys()):
            current_qty = self.positions.get(symbol, Position(symbol=symbol, mode=PositionMode.FLAT)).quantity
            target_qty = target_quantities.get(symbol, 0.0)
            
            # 计算需要成交的数量
            delta_qty = target_qty - current_qty
            
            if abs(delta_qty) > 1e-8:  # 有足够大的变化
                if delta_qty > 0:
                    # 增持（买入）
                    order = self._create_and_execute_order(
                        symbol=symbol,
                        side=OrderSide.LONG,
                        quantity=delta_qty,
                        price=self._current_prices.get(symbol, 0.0)
                    )
                else:
                    # 减持（卖出）
                    order = self._create_and_execute_order(
                        symbol=symbol,
                        side=OrderSide.SHORT,
                        quantity=abs(delta_qty),
                        price=self._current_prices.get(symbol, 0.0)
                    )
    
    def _weights_to_quantities(self, weights: Dict[str, float], portfolio_state: PortfolioState) -> Dict[str, float]:
        """将权重转换为实际持仓数量"""
        quantities = {}
        
        for symbol, weight in weights.items():
            if weight == 0:
                quantities[symbol] = 0.0
                continue
            
            if symbol not in self._current_prices:
                logger.warning(f"No price available for {symbol}, skipping")
                continue
            
            price = self._current_prices[symbol]
            
            # 计算数量：(可用资金 * 权重) / 价格
            amount = portfolio_state.available_balance * abs(weight)
            quantity = amount / price
            
            # 如果权重为负表示反向（这里简化处理，只考虑正向持仓）
            if weight < 0:
                quantity = -quantity
            
            quantities[symbol] = quantity
        
        return quantities
    
    def _create_and_execute_order(self, symbol: str, side: OrderSide, quantity: float, price: float) -> Optional[Trade]:
        """创建并执行订单"""
        try:
            # 检查余额
            order_value = quantity * price
            required_margin = order_value / max(self.config.leverage, 1.0)
            
            if required_margin > self.available_balance:
                logger.warning(
                    f"Insufficient margin for {symbol}: required={required_margin:.2f}, available={self.available_balance:.2f}"
                )
                return None
            
            # 计算手续费
            commission = order_value * self.taker_fee
            
            # 应用滑点
            slippage_amount = order_value * (self.config.slippage / 10000.0)
            actual_price = price * (1 + self.config.slippage / 10000.0) if side == OrderSide.LONG else price * (1 - self.config.slippage / 10000.0)
            
            # 创建Trade记录
            trade = Trade(
                trade_id=str(uuid.uuid4()),
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=actual_price,
                executed_at=self._current_timestamp,
                commission=commission
            )
            
            # 更新持仓
            if symbol not in self.positions:
                self.positions[symbol] = Position(
                    symbol=symbol,
                    mode=PositionMode.LONG if side == OrderSide.LONG else PositionMode.SHORT,
                    entry_time=self._current_timestamp
                )
            
            position = self.positions[symbol]
            
            # 更新持仓数量和平均价格
            old_qty = position.quantity
            new_qty = position.quantity + (quantity if side == OrderSide.LONG else -quantity)
            
            # 计算新的持仓成本
            if new_qty == 0:
                # 平仓
                if old_qty != 0:
                    pnl = (actual_price - position.entry_price) * old_qty
                    trade.pnl = pnl
                position.mode = PositionMode.FLAT
                position.quantity = 0.0
                position.unrealized_pnl = 0.0
            else:
                # 更新持仓
                if old_qty == 0:
                    # 新开仓
                    position.entry_price = actual_price
                    position.quantity = new_qty
                    position.mode = PositionMode.LONG if new_qty > 0 else PositionMode.SHORT
                else:
                    # 加仓或减仓
                    if (old_qty > 0 and new_qty > 0) or (old_qty < 0 and new_qty < 0):
                        # 加仓
                        position.entry_price = (position.entry_price * old_qty + actual_price * (new_qty - old_qty)) / new_qty
                    else:
                        # 反向开仓
                        position.entry_price = actual_price
                    
                    position.quantity = new_qty
                    position.mode = PositionMode.LONG if new_qty > 0 else PositionMode.SHORT
            
            # 更新账户
            self.total_commission += commission
            self.trades.append(trade)
            
            return trade
            
        except Exception as e:
            logger.error(f"Failed to execute order for {symbol}: {e}", exc_info=True)
            return None
    
    def _close_all_positions(self):
        """平仓所有持仓"""
        for symbol in list(self.positions.keys()):
            position = self.positions[symbol]
            if position.quantity != 0:
                # 计算平仓价格（使用最后已知价格）
                close_price = self._current_prices.get(symbol, position.entry_price)
                
                # 创建平仓Trade
                trade = Trade(
                    trade_id=str(uuid.uuid4()),
                    symbol=symbol,
                    side=OrderSide.SHORT if position.quantity > 0 else OrderSide.LONG,
                    quantity=abs(position.quantity),
                    price=close_price,
                    executed_at=self._current_timestamp,
                    commission=0.0  # 已在持仓期间计算
                )
                
                # 计算PnL
                if position.quantity > 0:
                    trade.pnl = position.quantity * (close_price - position.entry_price)
                else:
                    trade.pnl = position.quantity * (position.entry_price - close_price)
                
                self.trades.append(trade)
                position.quantity = 0.0
                position.mode = PositionMode.FLAT
    
    def _generate_backtest_result(self, start_datetime: datetime, end_datetime: datetime, execution_time: float) -> BacktestResult:
        """生成回测结果"""
        result = BacktestResult(
            config=self.config,
            trades=self.trades,
            portfolio_history=self.portfolio_history,
            start_time=start_datetime,
            end_time=end_datetime,
            execution_time_seconds=execution_time
        )
        
        # 计算统计指标
        if len(self.trades) > 0:
            result.total_trades = len(self.trades)
            
            # 计算收益
            final_balance = self.total_balance
            result.total_return = ((final_balance - self.initial_balance) / self.initial_balance) * 100
            
            # 年化收益（简化计算）
            if len(self.portfolio_history) > 0:
                days = (self._current_timestamp - self.portfolio_history[0].timestamp).days
                if days > 0:
                    result.annual_return = result.total_return * (365.0 / days)
            
            # 胜率
            winning_trades = [t for t in self.trades if t.pnl > 0]
            losing_trades = [t for t in self.trades if t.pnl < 0]
            result.winning_trades = len(winning_trades)
            result.losing_trades = len(losing_trades)
            result.win_rate = (len(winning_trades) / len(self.trades)) * 100 if len(self.trades) > 0 else 0
            
            # 平均PnL
            total_pnl = sum(t.pnl for t in self.trades)
            result.avg_profit_per_trade = total_pnl / len(self.trades) if len(self.trades) > 0 else 0
            result.max_profit_per_trade = max((t.pnl for t in self.trades), default=0)
            result.max_loss_per_trade = min((t.pnl for t in self.trades), default=0)
            
            # 盈亏比
            if len(losing_trades) > 0:
                avg_win = sum(t.pnl for t in winning_trades) / len(winning_trades) if winning_trades else 0
                avg_loss = abs(sum(t.pnl for t in losing_trades) / len(losing_trades)) if losing_trades else 1
                result.profit_factor = avg_win / avg_loss if avg_loss > 0 else 0
            
            # 最大回撤
            if len(self.portfolio_history) > 1:
                cumulative_returns = [s.total_pnl for s in self.portfolio_history]
                running_max = 0
                max_dd = 0
                for ret in cumulative_returns:
                    running_max = max(running_max, ret)
                    drawdown = running_max - ret
                    max_dd = max(max_dd, drawdown)
                
                result.max_drawdown = (max_dd / self.initial_balance) * 100 if self.initial_balance > 0 else 0
            
            # 夏普比率（简化计算）
            if len(self.portfolio_history) > 1:
                returns = []
                for i in range(1, len(self.portfolio_history)):
                    ret = (self.portfolio_history[i].total_pnl - self.portfolio_history[i-1].total_pnl) / self.initial_balance
                    returns.append(ret)
                
                if len(returns) > 1:
                    import statistics
                    avg_return = statistics.mean(returns)
                    std_dev = statistics.stdev(returns) if len(returns) > 1 else 1
                    result.sharpe_ratio = (avg_return / std_dev * (252 ** 0.5)) if std_dev > 0 else 0
        
        return result
