"""
多因子回测系统示例

演示如何使用多因子回测系统：
1. 因子评估 - IC, Rank IC, 选币准确率
2. Alpha评估 - 收益, 风险, 回撤
3. 图表生成 - 因子图表 vs Alpha图表
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from src.backtest import (
    FactorEvaluator,
    AlphaEvaluator,
    FactorBacktestConfig,
    MultiFactorBacktest,
    BacktestConfig,
    BacktestResult,
    Trade,
    PortfolioState,
    OrderSide,
)


class DemoFactor:
    """演示因子"""
    name = "demo_factor"
    
    def run(self, view):
        weights = {}
        for symbol in view.iter_symbols():
            bar = view.get_bar(symbol, tail=20)
            if not bar.empty and len(bar) >= 10:
                ret = (bar['close'].iloc[-1] - bar['close'].iloc[-5]) / bar['close'].iloc[-5]
                weights[symbol] = ret * 3
            else:
                weights[symbol] = 0.0
        return weights


def example_factor_evaluation():
    """示例1: 因子评估"""
    print("\n" + "=" * 60)
    print("示例1: 因子评估 (IC, Rank IC, 选币准确率)")
    print("=" * 60)
    
    import numpy as np
    
    factor_data = {}
    returns_data = {}
    
    for i in range(100):
        ts = datetime(2024, 1, 1) + timedelta(days=i)
        ts = ts.replace(tzinfo=timezone.utc)
        
        factor_values = {f"SYM{j}": np.random.randn() * 0.1 for j in range(50)}
        returns = {f"SYM{j}": np.random.randn() * 0.02 + 0.001 * factor_values[f"SYM{j}"] for j in range(50)}
        
        factor_data[ts] = factor_values
        returns_data[ts] = returns
    
    metrics = FactorEvaluator.evaluate_factor(
        name="demo_factor",
        factor_weights=factor_data,
        next_returns=returns_data,
        groups=5,
    )
    
    print(f"\n因子评估结果:")
    print(f"  IC均值: {metrics.ic_mean:.4f}")
    print(f"  IC标准差: {metrics.ic_std:.4f}")
    print(f"  ICIR: {metrics.icir:.4f}")
    print(f"  Rank IC: {metrics.rank_ic_mean:.4f}")
    print(f"  选币准确率: {metrics.selection_accuracy:.2f}%")
    print(f"  多做Spread: {metrics.long_spread:.4f}")
    
    return metrics


def example_alpha_evaluation():
    """示例2: Alpha评估"""
    print("\n" + "=" * 60)
    print("示例2: Alpha评估 (收益, 风险, 回撤)")
    print("=" * 60)
    
    import numpy as np
    
    portfolio_values = [50000.0]
    for i in range(100):
        daily_ret = np.random.randn() * 0.02 + 0.001
        new_value = portfolio_values[-1] * (1 + daily_ret)
        portfolio_values.append(float(new_value))
    
    trades = [
        {'symbol': 'BTC', 'side': 'LONG', 'pnl': 150.0},
        {'symbol': 'ETH', 'side': 'LONG', 'pnl': -80.0},
        {'symbol': 'SOL', 'side': 'SHORT', 'pnl': 220.0},
    ]
    
    metrics = AlphaEvaluator.evaluate_alpha(
        name="demo_alpha",
        portfolio_values=portfolio_values,
        trades=trades,
        initial_balance=50000.0,
    )
    
    print(f"\nAlpha评估结果:")
    print(f"  总收益率: {metrics.total_return:.2f}%")
    print(f"  年化收益率: {metrics.annual_return:.2f}%")
    print(f"  夏普比率: {metrics.sharpe_ratio:.2f}")
    print(f"  索提诺比率: {metrics.sortino_ratio:.2f}")
    print(f"  最大回撤: {metrics.max_drawdown:.2f}%")
    print(f"  胜率: {metrics.win_rate:.2f}%")
    
    return metrics


def example_charts():
    """示例3: 生成图表"""
    print("\n" + "=" * 60)
    print("示例3: 生成图表")
    print("=" * 60)
    
    from src.backtest.chart import generate_factor_charts, generate_alpha_charts, export_csv
    import numpy as np
    from datetime import timedelta
    
    # 使用配置的默认输出目录（data/backtest_results）
    from src.backtest.config import BacktestConfigManager
    output_dir = BacktestConfigManager.get_result_dir()
    
    print("\n3.1 生成因子评估图表...")
    
    factor_metrics = FactorEvaluator.evaluate_factor(
        name="demo_factor",
        factor_weights={
            datetime(2024, 1, 1) + timedelta(days=i): 
            {f"SYM{j}": np.random.randn() * 0.1 for j in range(30)}
            for i in range(50)
        },
        next_returns={
            datetime(2024, 1, 1) + timedelta(days=i):
            {f"SYM{j}": np.random.randn() * 0.02 for j in range(30)}
            for i in range(50)
        },
        groups=5,
    )
    
    factor_charts = generate_factor_charts(factor_metrics, str(output_dir), "factor_demo")
    print("  因子图表:")
    for chart_type, path in factor_charts.items():
        print(f"    ✓ {chart_type}: {path}")
    
    print("\n3.2 生成Alpha评估图表...")
    
    config = BacktestConfig(
        name="sample_backtest",
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2024, 6, 30, tzinfo=timezone.utc),
        initial_balance=50000.0,
        symbols=["BTCUSDT", "ETHUSDT"],
        leverage=1.0,
    )
    
    trades = []
    for i in range(30):
        ts = config.start_date + timedelta(days=i)
        side = OrderSide.LONG if i % 3 != 0 else OrderSide.SHORT
        trades.append(Trade(
            trade_id=f"trade_{i}",
            symbol=f"SYM{i % 10}",
            side=side,
            quantity=10.0,
            price=100.0 + np.random.randn() * 5,
            executed_at=ts,
            commission=5.0,
            pnl=np.random.randn() * 100,
        ))
    
    portfolio_history = []
    balance = 50000.0
    for i in range(180):
        ts = config.start_date + timedelta(days=i)
        balance = balance * (1 + np.random.randn() * 0.02 + 0.001)
        portfolio_history.append(PortfolioState(
            timestamp=ts,
            total_balance=balance,
            available_balance=balance * 0.5,
            total_pnl=balance - 50000,
        ))
    
    backtest_result = BacktestResult(
        config=config,
        trades=trades,
        portfolio_history=portfolio_history,
        start_time=config.start_date,
        end_time=config.end_date,
        execution_time_seconds=2.5,
        total_return=15.5,
        annual_return=32.1,
        sharpe_ratio=1.85,
        sortino_ratio=2.3,
        max_drawdown=8.5,
        win_rate=62.5,
        profit_factor=1.8,
        total_trades=30,
        winning_trades=18,
        losing_trades=12,
        avg_trade=0.5,
        best_trade=5.2,
        worst_trade=-3.8,
        ic_mean=0.042,
        rank_ic_mean=0.038,
        selection_accuracy=72.3,
        long_spread=0.015,
        group_return_spread=0.028,
    )
    
    alpha_charts = generate_alpha_charts(backtest_result, str(output_dir), "alpha_demo")
    print("  Alpha图表:")
    for chart_type, path in alpha_charts.items():
        print(f"    ✓ {chart_type}: {path}")
    
    print("\n3.3 导出CSV数据...")
    portfolio_file, trades_file = export_csv(backtest_result, str(output_dir), "demo")
    print(f"    ✓ 账户历史: {portfolio_file}")
    print(f"    ✓ 交易记录: {trades_file}")
    
    return factor_charts, alpha_charts


def run_all_examples():
    """运行所有示例"""
    print("\n" + "#" * 60)
    print("# 多因子回测系统示例")
    print("#" * 60)
    
    example_factor_evaluation()
    example_alpha_evaluation()
    example_charts()
    
    print("\n" + "#" * 60)
    print("# 所有示例运行完成")
    from src.backtest.config import BacktestConfigManager
    result_dir = BacktestConfigManager.get_result_dir()
    print(f"# 图表保存在: {result_dir}/")
    print("#" * 60 + "\n")


if __name__ == "__main__":
    run_all_examples()
