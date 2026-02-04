"""
多空多因子选币回测系统

设计目标：
1. 支持对单个Calculator（因子）做回测 - 评估单个因子的选币能力
2. 支持对整个Alpha（因子组合）做回测 - 评估多因子组合的整体表现
3. 完全适配实盘的多空多因子选币交易系统

回测流程：
1. 加载历史数据
2. 在每个时间点：
   a. 运行Calculator(s)获取权重向量
   b. 对权重进行排序和筛选（选币逻辑）
   c. 根据权重分配资金
   d. 执行交易
3. 计算绩效指标
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Callable, Tuple, Set, Any
from dataclasses import dataclass, field
from pathlib import Path
import logging
import json
import time

import pandas as pd
import numpy as np

from ..common.config import config
from ..common.logger import get_logger
from ..data.storage import get_data_storage
from ..data.api import get_data_api
from ..strategy.alpha import AlphaEngine, AlphaResult
from ..strategy.calculator import AlphaCalculatorBase, AlphaDataView
from ..strategy.position_generator import PositionGenerator
from .models import (
    BacktestConfig,
    BacktestResult,
    PortfolioState,
    Trade,
    OrderSide,
    create_backtest_result,
)

logger = logging.getLogger('backtest')


@dataclass
class FactorBacktestConfig:
    """因子回测配置"""
    name: str
    start_date: datetime
    end_date: datetime
    initial_balance: float = 10000.0
    interval: str = "5m"
    leverage: float = 1.0
    
    capital_allocation: str = "equal_weight"
    long_count: int = 10
    short_count: int = 10
    min_weight: float = 0.02
    max_weight: float = 0.2
    
    symbols: Optional[List[str]] = None
    calculator_names: Optional[List[str]] = None


@dataclass
class WeightVector:
    """权重向量"""
    weights: Dict[str, float]  # symbol -> weight
    long_symbols: List[str] = field(default_factory=list)
    short_symbols: List[str] = field(default_factory=list)
    timestamp: Optional[datetime] = None


@dataclass
class TradeRecord:
    """交易记录"""
    timestamp: datetime
    symbol: str
    side: str  # LONG, SHORT, FLAT
    quantity: float
    price: float
    weight_before: float
    weight_after: float
    pnl: float = 0.0
    commission: float = 0.0


@dataclass
class BacktestMetrics:
    """回测指标"""
    total_return: float = 0.0
    annual_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_trade: float = 0.0
    best_trade: float = 0.0
    worst_trade: float = 0.0
    long_trades: int = 0
    short_trades: int = 0
    long_win_rate: float = 0.0
    short_win_rate: float = 0.0
    
    def to_dict(self) -> Dict:
        return {
            "收益指标": {
                "总收益率": f"{self.total_return:.2f}%",
                "年化收益率": f"{self.annual_return:.2f}%",
            },
            "风险指标": {
                "夏普比率": f"{self.sharpe_ratio:.2f}",
                "最大回撤": f"{self.max_drawdown:.2f}%",
            },
            "交易统计": {
                "总交易数": self.total_trades,
                "盈利交易": self.winning_trades,
                "亏损交易": self.losing_trades,
                "胜率": f"{self.win_rate:.2f}%",
                "盈亏比": f"{self.profit_factor:.2f}",
                "多做交易": self.long_trades,
                "做空交易": self.short_trades,
                "多做胜率": f"{self.long_win_rate:.2f}%",
                "做空胜率": f"{self.short_win_rate:.2f}%",
            }
        }


class MultiFactorBacktest:
    """
    多空多因子选币回测引擎
    
    特性：
    - 支持单个Calculator回测
    - 支持整个Alpha回测
    - 支持多空选币逻辑
    - 支持多种资金分配方式
    """
    
    def __init__(self, config: FactorBacktestConfig):
        self.config = config
        self.storage = get_data_storage()
        self.position_generator = PositionGenerator()
        
        self.symbols: List[str] = []
        self.current_weights: Dict[str, float] = {}
        self.portfolio_value: float = config.initial_balance
        self.portfolio_history: List[Dict] = []
        self.trades: List[TradeRecord] = []
        
        self.long_positions: Set[str] = set()
        self.short_positions: Set[str] = set()
        
        self._prepare_symbols()
    
    def _prepare_symbols(self):
        """准备交易对列表"""
        if self.config.symbols:
            self.symbols = self.config.symbols
        else:
            universe_dir = Path(config.get('data.universe_directory', 'data/universe'))
            if universe_dir.exists():
                for date_dir in sorted(universe_dir.iterdir(), reverse=True):
                    universe_file = date_dir / 'v1' / 'universe.csv'
                    if universe_file.exists():
                        df = pd.read_csv(universe_file)
                        self.symbols = df['symbol'].tolist()[:100]
                        break
            
            if not self.symbols:
                self.symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"]
        
        logger.info(f"Using {len(self.symbols)} symbols for backtest")
    
    def run(
        self,
        calculators: Optional[List[AlphaCalculatorBase]] = None,
        verbose: bool = True
    ) -> Tuple[BacktestMetrics, pd.DataFrame]:
        """
        运行回测 - 兼容旧版
        
        Args:
            calculators: 要使用的Calculator列表，None表示使用全部
            verbose: 是否打印进度
            
        Returns:
            (回测指标, 账户历史DataFrame)
        """
        start_time = time.time()
        
        if verbose:
            logger.info(f"Starting backtest: {self.config.name}")
            logger.info(f"Period: {self.config.start_date.date()} ~ {self.config.end_date.date()}")
            logger.info(f"Initial balance: ${self.config.initial_balance:,.0f}")
            logger.info(f"Leverage: {self.config.leverage}x")
        
        self.portfolio_value = self.config.initial_balance
        self.portfolio_history = []
        self.trades = []
        self.current_weights = {}
        self.long_positions = set()
        self.short_positions = set()
        
        data_api = get_data_api()
        
        end_label = data_api._get_date_time_label_from_datetime(self.config.end_date)
        start_label = data_api._get_date_time_label_from_datetime(self.config.start_date)
        
        bar_data = data_api.get_bar_between(start_label, end_label, mode=self.config.interval)
        
        symbols_in_data = set()
        for sym, df in bar_data.items():
            if not df.empty:
                symbols_in_data.add(sym)
        
        valid_symbols = [s for s in self.symbols if s in symbols_in_data]
        if not valid_symbols:
            valid_symbols = list(symbols_in_data)[:50]
        
        if verbose:
            logger.info(f"Valid symbols with data: {len(valid_symbols)}")
        
        timestamps: List[datetime] = []
        for sym, df in bar_data.items():
            if sym in valid_symbols and 'open_time' in df.columns:
                timestamps.extend(df['open_time'].unique().tolist())
        
        timestamps = sorted(list(set(timestamps)))
        
        if verbose:
            logger.info(f"Total timestamps: {len(timestamps)}")
        
        calculators = calculators or []
        factor_weights: Dict[datetime, Dict[str, float]] = {}
        next_returns: Dict[datetime, Dict[str, float]] = {}
        
        for i, timestamp in enumerate(timestamps):
            if i % 100 == 0 and verbose:
                logger.info(f"Progress: {i}/{len(timestamps)} ({i*100/len(timestamps):.1f}%)")
            
            current_bar_data = {}
            for sym in valid_symbols:
                if sym in bar_data and 'open_time' in bar_data[sym].columns:
                    df = bar_data[sym][bar_data[sym]['open_time'] == timestamp]
                    if not df.empty:
                        current_bar_data[sym] = df.iloc[0]
            
            if not current_bar_data:
                continue
            
            view = AlphaDataView(
                bar_data={sym: df[df['open_time'] == timestamp] for sym, df in bar_data.items() if sym in valid_symbols},
                tran_stats={},
                symbols=set(valid_symbols),
                copy_on_read=False
            )
            
            raw_weights = {}
            for calc in calculators:
                calc_weights = calc.run(view)
                for sym, w in calc_weights.items():
                    if sym in valid_symbols:
                        raw_weights[sym] = raw_weights.get(sym, 0.0) + w
            
            factor_weights[timestamp] = raw_weights.copy()
            
            target_weights = self._process_weights(raw_weights)
            
            trades = self._rebalance(target_weights, current_bar_data, timestamp)
            self.trades.extend(trades)
            
            portfolio_state = self._update_portfolio(target_weights, current_bar_data, timestamp)
            self.portfolio_history.append(portfolio_state)
            
            next_ts_idx = i + 1
            if next_ts_idx < len(timestamps):
                next_ts = timestamps[next_ts_idx]
                next_bar_data = {}
                for sym in valid_symbols:
                    if sym in bar_data and 'open_time' in bar_data[sym].columns:
                        df = bar_data[sym][bar_data[sym]['open_time'] == next_ts]
                        if not df.empty:
                            curr = current_bar_data.get(sym, {})
                            next_row = df.iloc[0]
                            if curr:
                                curr_close = curr.get('close', 0)
                                next_close = next_row.get('close', 0)
                                if curr_close > 0:
                                    ret = (next_close - curr_close) / curr_close
                                    next_returns[timestamp] = next_returns.get(timestamp, {})
                                    next_returns[timestamp][sym] = ret
        
        metrics = self._calculate_metrics()
        portfolio_df = pd.DataFrame(self.portfolio_history)
        
        execution_time = time.time() - start_time
        if verbose:
            self._print_results(metrics)
            logger.info(f"Backtest completed in {execution_time:.2f}s")
        
        return metrics, portfolio_df
    
    def run_with_result(
        self,
        calculators: Optional[List[AlphaCalculatorBase]] = None,
        verbose: bool = True
    ) -> BacktestResult:
        """
        运行回测 - 返回统一的BacktestResult
        
        Args:
            calculators: 要使用的Calculator列表
            verbose: 是否打印进度
            
        Returns:
            BacktestResult - 统一的回测结果
        """
        from .models import BacktestConfig as ModelBacktestConfig
        
        config = ModelBacktestConfig(
            name=self.config.name,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            initial_balance=self.config.initial_balance,
            symbols=self.symbols,
            leverage=self.config.leverage,
            capital_allocation=self.config.capital_allocation,
            long_count=self.config.long_count,
            short_count=self.config.short_count,
            interval=self.config.interval,
        )
        
        trades = []
        for i, tr in enumerate(self.trades):
            side = OrderSide.LONG if tr.side == "LONG" else OrderSide.SHORT
            trades.append(Trade(
                trade_id=f"trade_{i}",
                symbol=tr.symbol,
                side=side,
                quantity=tr.quantity,
                price=tr.price,
                executed_at=tr.timestamp,
                commission=tr.commission,
                pnl=tr.pnl,
            ))
        
        result = create_backtest_result(
            config=config,
            portfolio_df=portfolio_df,
            trades=trades,
            factor_weights=getattr(self, 'factor_weights', {}),
            next_returns=getattr(self, 'next_returns', {}),
        )
        
        return result
    
    def _process_weights(self, raw_weights: Dict[str, float]) -> WeightVector:
        """
        处理权重向量：
        1. 排序
        2. 选币（选出做多和做空的币）
        3. 分配资金
        """
        if not raw_weights:
            return WeightVector(weights={})
        
        sorted_items = sorted(raw_weights.items(), key=lambda x: x[1], reverse=True)
        
        long_count = min(self.config.long_count, len(sorted_items) // 2)
        short_count = min(self.config.short_count, len(sorted_items) - long_count)
        
        long_symbols = [s for s, w in sorted_items[:long_count]]
        short_symbols = [s for s, w in sorted_items[-short_count:] if w < 0]
        
        weights = {}
        
        if long_symbols:
            if self.config.capital_allocation == "equal_weight":
                long_weight = self.config.leverage / len(long_symbols)
                for sym in long_symbols:
                    weights[sym] = min(long_weight, self.config.max_weight)
            elif self.config.capital_allocation == "rank_weight":
                total_rank = sum(range(1, len(long_symbols) + 1))
                for i, sym in enumerate(long_symbols):
                    rank_weight = (len(long_symbols) - i) / total_rank
                    w = rank_weight * self.config.leverage
                    weights[sym] = min(w, self.config.max_weight)
        
        if short_symbols:
            short_weight = -self.config.leverage / len(short_symbols)
            for sym in short_symbols:
                weights[sym] = max(short_weight, -self.config.max_weight)
        
        return WeightVector(
            weights=weights,
            long_symbols=long_symbols,
            short_symbols=short_symbols
        )
    
    def _rebalance(
        self,
        target: WeightVector,
        prices: Dict[str, Any],
        timestamp: datetime
    ) -> List[TradeRecord]:
        """执行调仓"""
        trades = []
        
        current_weight_sum = sum(abs(w) for w in self.current_weights.values())
        available_balance = self.portfolio_value * 0.5
        
        for sym, target_w in target.weights.items():
            current_w = self.current_weights.get(sym, 0.0)
            delta_w = target_w - current_w
            
            if abs(delta_w) < self.config.min_weight:
                continue
            
            price = prices.get(sym, {}).get('close', 0)
            if price <= 0:
                continue
            
            if delta_w > 0:
                quantity = (available_balance * delta_w) / price
                if quantity > 0:
                    trades.append(TradeRecord(
                        timestamp=timestamp,
                        symbol=sym,
                        side="LONG",
                        quantity=quantity,
                        price=price,
                        weight_before=current_w,
                        weight_after=target_w
                    ))
            else:
                quantity = (available_balance * abs(delta_w)) / price
                if quantity > 0:
                    trades.append(TradeRecord(
                        timestamp=timestamp,
                        symbol=sym,
                        side="SHORT",
                        quantity=quantity,
                        price=price,
                        weight_before=current_w,
                        weight_after=target_w
                    ))
        
        self.current_weights = target.weights.copy()
        
        return trades
    
    def _update_portfolio(
        self,
        target: WeightVector,
        prices: Dict[str, Any],
        timestamp: datetime
    ) -> Dict:
        """更新组合状态"""
        total_value = self.portfolio_value
        
        unrealized_pnl = 0
        for sym, weight in self.current_weights.items():
            price = prices.get(sym, {}).get('close', 0)
            if price > 0 and weight != 0:
                position_value = total_value * abs(weight)
                unrealized_pnl += 0
        
        self.portfolio_value = total_value + unrealized_pnl
        
        return {
            'timestamp': timestamp,
            'total_balance': self.portfolio_value,
            'available_balance': self.portfolio_value * 0.5,
            'total_pnl': self.portfolio_value - self.config.initial_balance,
            'unrealized_pnl': unrealized_pnl,
            'positions_count': len([s for s, w in self.current_weights.items() if w != 0]),
            'long_count': len(target.long_symbols),
            'short_count': len(target.short_symbols),
        }
    
    def _calculate_metrics(self) -> BacktestMetrics:
        """计算绩效指标"""
        if not self.portfolio_history:
            return BacktestMetrics()
        
        df = pd.DataFrame(self.portfolio_history)
        
        initial_value = self.config.initial_balance
        final_value = df['total_balance'].iloc[-1]
        
        total_return = (final_value - initial_value) / initial_value * 100
        
        daily_returns = df['total_balance'].pct_change().dropna()
        if len(daily_returns) > 1:
            sharpe = daily_returns.mean() / daily_returns.std() * np.sqrt(252) if daily_returns.std() > 0 else 0
        else:
            sharpe = 0
        
        cummax = df['total_balance'].cummax()
        drawdowns = (df['total_balance'] - cummax) / cummax * 100
        max_drawdown = abs(drawdowns.min()) if len(drawdowns) > 0 else 0
        
        winning_trades = [t for t in self.trades if t.pnl > 0]
        losing_trades = [t for t in self.trades if t.pnl < 0]
        
        long_trades = [t for t in self.trades if t.side == "LONG"]
        short_trades = [t for t in self.trades if t.side == "SHORT"]
        
        long_wins = [t for t in long_trades if t.pnl > 0]
        short_wins = [t for t in short_trades if t.pnl > 0]
        
        total_trades = len(self.trades)
        win_rate = len(winning_trades) / total_trades * 100 if total_trades > 0 else 0
        long_win_rate = len(long_wins) / len(long_trades) * 100 if len(long_trades) > 0 else 0
        short_win_rate = len(short_wins) / len(short_trades) * 100 if len(short_trades) > 0 else 0
        
        gross_profit = sum(t.pnl for t in winning_trades) if winning_trades else 0
        gross_loss = abs(sum(t.pnl for t in losing_trades)) if losing_trades else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
        
        avg_trade = np.mean([t.pnl for t in self.trades]) if self.trades else 0
        best_trade = max([t.pnl for t in self.trades]) if self.trades else 0
        worst_trade = min([t.pnl for t in self.trades]) if self.trades else 0
        
        return BacktestMetrics(
            total_return=total_return,
            sharpe_ratio=sharpe,
            max_drawdown=max_drawdown,
            win_rate=win_rate,
            profit_factor=profit_factor,
            total_trades=total_trades,
            winning_trades=len(winning_trades),
            losing_trades=len(losing_trades),
            avg_trade=avg_trade,
            best_trade=best_trade,
            worst_trade=worst_trade,
            long_trades=len(long_trades),
            short_trades=len(short_trades),
            long_win_rate=long_win_rate,
            short_win_rate=short_win_rate,
        )
    
    def _print_results(self, metrics: BacktestMetrics):
        """打印结果"""
        print("\n" + "=" * 60)
        print(f"回测结果: {self.config.name}")
        print("=" * 60)
        
        print(f"\n【收益指标】")
        print(f"  总收益率: {metrics.total_return:.2f}%")
        print(f"  夏普比率: {metrics.sharpe_ratio:.2f}")
        
        print(f"\n【风险指标】")
        print(f"  最大回撤: {metrics.max_drawdown:.2f}%")
        print(f"  盈亏比: {metrics.profit_factor:.2f}")
        
        print(f"\n【交易统计】")
        print(f"  总交易数: {metrics.total_trades}")
        print(f"  胜率: {metrics.win_rate:.2f}%")
        print(f"  多做/做空: {metrics.long_trades}/{metrics.short_trades}")
        print(f"  多做胜率: {metrics.long_win_rate:.2f}%")
        print(f"  做空胜率: {metrics.short_win_rate:.2f}%")
        
        print("=" * 60)


class SingleCalculatorBacktest:
    """单Calculator回测 - 测试单个因子的表现"""
    
    def __init__(self, config: FactorBacktestConfig):
        self.config = config
        self.backtest = MultiFactorBacktest(config)
    
    def run(
        self,
        calculator: AlphaCalculatorBase,
        verbose: bool = True
    ) -> Tuple[BacktestMetrics, pd.DataFrame, Dict]:
        """
        运行单Calculator回测
        
        Returns:
            (指标, 账户历史, 因子分析)
        """
        if verbose:
            logger.info(f"Testing calculator: {calculator.name}")
        
        metrics, portfolio_df = self.backtest.run([calculator], verbose=verbose)
        
        factor_analysis = self._analyze_factor(calculator, portfolio_df)
        
        if verbose:
            print(f"\n因子分析 - {calculator.name}:")
            print(f"  平均权重: {factor_analysis['avg_weight']:.4f}")
            print(f"  权重标准差: {factor_analysis['weight_std']:.4f}")
            print(f"  选对次数: {factor_analysis['correct_selections']}/{factor_analysis['total_selections']}")
            print(f"  选币准确率: {factor_analysis['selection_accuracy']:.2f}%")
        
        return metrics, portfolio_df, factor_analysis
    
    def _analyze_factor(
        self,
        calculator: AlphaCalculatorBase,
        portfolio_df: pd.DataFrame
    ) -> Dict:
        """分析因子的表现"""
        weights_over_time = []
        
        for state in self.backtest.portfolio_history:
            if 'weights' in state:
                weights_over_time.append(state['weights'])
        
        if not weights_over_time:
            return {
                'avg_weight': 0,
                'weight_std': 0,
                'total_selections': 0,
                'correct_selections': 0,
                'selection_accuracy': 0,
            }
        
        all_weights = []
        for w in weights_over_time:
            all_weights.extend(w.values())
        
        avg_weight = np.mean(all_weights) if all_weights else 0
        weight_std = np.std(all_weights) if all_weights else 0
        
        total_selections = len(weights_over_time)
        positive_returns = 0
        for i in range(1, len(weights_over_time)):
            prev_weights = weights_over_time[i-1]
            curr_weights = weights_over_time[i]
            if i < len(portfolio_df):
                ret = (portfolio_df.iloc[i]['total_balance'] - portfolio_df.iloc[i-1]['total_balance']) / portfolio_df.iloc[i-1]['total_balance']
                if ret > 0:
                    positive_returns += 1
        
        return {
            'avg_weight': avg_weight,
            'weight_std': weight_std,
            'total_selections': total_selections,
            'correct_selections': positive_returns,
            'selection_accuracy': positive_returns / total_selections * 100 if total_selections > 0 else 0,
        }


class AlphaBacktest:
    """Alpha回测 - 测试整个因子组合的表现"""
    
    def __init__(self, config: FactorBacktestConfig):
        self.config = config
        self.backtest = MultiFactorBacktest(config)
    
    def run(
        self,
        calculators: Optional[List[AlphaCalculatorBase]] = None,
        verbose: bool = True
    ) -> Tuple[BacktestMetrics, pd.DataFrame]:
        """
        运行Alpha回测
        
        Args:
            calculators: 要使用的Calculator列表，None表示使用Alpha Engine的全部Calculator
        """
        if calculators is None:
            from ..strategy.calculators import load_calculators
            calculators = list(load_calculators())
        
        if verbose:
            logger.info(f"Testing Alpha with {len(calculators)} calculators:")
            for calc in calculators:
                logger.info(f"  - {calc.name}")
        
        return self.backtest.run(calculators, verbose=verbose)


def run_single_calculator_backtest(
    calculator: AlphaCalculatorBase,
    start_date: datetime,
    end_date: datetime,
    initial_balance: float = 10000.0,
    symbols: Optional[List[str]] = None,
    verbose: bool = True,
) -> Dict:
    """
    便捷函数：对单个Calculator进行回测
    
    Example:
        >>> from src.strategy.calculators import MeanBuyDolvol4OverDolvolRankCalculator
        >>> from src.backtest import run_single_calculator_backtest
        >>> 
        >>> calc = MeanBuyDolvol4OverDolvolRankCalculator(lookback_bars=1000)
        >>> result = run_single_calculator_backtest(
        ...     calculator=calc,
        ...     start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ...     end_date=datetime(2024, 3, 31, tzinfo=timezone.utc),
        ... )
    """
    config = FactorBacktestConfig(
        name=f"bt_{calculator.name}",
        start_date=start_date,
        end_date=end_date,
        initial_balance=initial_balance,
        symbols=symbols,
    )
    
    backtest = SingleCalculatorBacktest(config)
    metrics, portfolio_df, factor_analysis = backtest.run(calculator, verbose=verbose)
    
    return {
        'metrics': metrics.to_dict(),
        'portfolio_df': portfolio_df,
        'factor_analysis': factor_analysis,
        'calculator_name': calculator.name,
        # Top-level access for convenience
        'total_return': metrics.total_return,
        'sharpe_ratio': metrics.sharpe_ratio,
        'max_drawdown': metrics.max_drawdown,
        'win_rate': metrics.win_rate,
        'ic_mean': getattr(metrics, 'ic_mean', 0),
        'selection_accuracy': getattr(metrics, 'selection_accuracy', 0),
    }


def run_alpha_backtest(
    start_date: datetime,
    end_date: datetime,
    initial_balance: float = 10000.0,
    calculator_names: Optional[List[str]] = None,
    symbols: Optional[List[str]] = None,
    verbose: bool = True,
) -> Dict:
    """
    便捷函数：对整个Alpha进行回测
    
    Example:
        >>> from src.backtest import run_alpha_backtest
        >>> 
        >>> result = run_alpha_backtest(
        ...     start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ...     end_date=datetime(2024, 3, 31, tzinfo=timezone.utc),
        ...     initial_balance=50000,
        ... )
    """
    from ..strategy.calculators import load_calculators
    
    all_calculators = list(load_calculators())
    
    if calculator_names:
        calculators = [c for c in all_calculators if c.name in calculator_names]
    else:
        calculators = all_calculators
    
    config = FactorBacktestConfig(
        name="bt_alpha",
        start_date=start_date,
        end_date=end_date,
        initial_balance=initial_balance,
        symbols=symbols,
    )
    
    backtest = AlphaBacktest(config)
    metrics, portfolio_df = backtest.run(calculators, verbose=verbose)
    
    return {
        'metrics': metrics.to_dict(),
        'portfolio_df': portfolio_df,
        'calculators': [c.name for c in calculators],
    }


def compare_calculators(
    calculators: List[AlphaCalculatorBase],
    start_date: datetime,
    end_date: datetime,
    initial_balance: float = 10000.0,
    verbose: bool = True,
) -> Dict:
    """
    对比多个Calculator的表现
    
    Returns:
        对比结果字典
    """
    results = {}
    
    for calc in calculators:
        if verbose:
            print(f"\nTesting {calc.name}...")
        
        result = run_single_calculator_backtest(
            calculator=calc,
            start_date=start_date,
            end_date=end_date,
            initial_balance=initial_balance,
            verbose=verbose,
        )
        
        results[calc.name] = result
    
    if verbose:
        print("\n" + "=" * 60)
        print("Calculator 对比结果")
        print("=" * 60)
        
        for name, result in results.items():
            metrics = result['metrics']
            print(f"\n{name}:")
            print(f"  总收益: {metrics['收益指标']['总收益率']}")
            print(f"  夏普: {metrics['风险指标']['夏普比率']}")
            print(f"  最大回撤: {metrics['风险指标']['最大回撤']}")
    
    return results


__all__ = [
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
]


def run_backtest(
    calculator: AlphaCalculatorBase,
    start_date: datetime,
    end_date: datetime,
    initial_balance: float = 10000.0,
    symbols: Optional[List[str]] = None,
    capital_allocation: str = "rank_weight",
    long_count: int = 5,
    short_count: int = 5,
    verbose: bool = True,
) -> BacktestResult:
    """
    运行回测 - 返回统一的BacktestResult
    
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
    config = FactorBacktestConfig(
        name=f"bt_{calculator.name}",
        start_date=start_date,
        end_date=end_date,
        initial_balance=initial_balance,
        symbols=symbols,
        capital_allocation=capital_allocation,
        long_count=long_count,
        short_count=short_count,
    )
    
    backtest = MultiFactorBacktest(config)
    result = backtest.run_with_result([calculator], verbose=verbose)
    
    return result
