# 多因子回测系统 (Multi-Factor Backtest System)

## 概述

完整的多因子量化交易回测系统，支持：

- **因子评估** - 评估单个因子(Calculator)的预测能力
- **Alpha回测** - 评估多因子组合的整体表现  
- **因子对比** - 对比多个因子的表现
- **图表生成** - 可视化回测结果

## 文件结构

```
src/backtest/
├── __init__.py              # 模块导出和便捷函数
├── backtest.py              # 核心回测引擎
├── metrics.py               # 评估指标体系
├── chart.py                 # 图表生成
├── models.py                # 数据模型
├── api.py                  # API接口
├── analysis.py             # 结果分析
├── executor.py             # 执行引擎
├── replay.py               # 数据重放
├── mock_data.py            # 模拟数据
└── config.py              # 配置
```

## 快速开始

### 1. 最简单的回测

```python
from datetime import datetime, timezone
from src.backtest import run_backtest, BacktestChartGenerator

# 定义你的因子
class MyFactor:
    name = "my_factor"
    
    def run(self, view):
        weights = {}
        for symbol in view.iter_symbols():
            bar = view.get_bar(symbol, tail=10)
            if not bar.empty and len(bar) >= 2:
                ret = (bar['close'].iloc[-1] - bar['close'].iloc[-2]) / bar['close'].iloc[-2]
                weights[symbol] = ret * 10
        return weights

# 运行回测（自动生成图表）
result = run_backtest(
    calculator=MyFactor(),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 3, 31, tzinfo=timezone.utc),
    initial_balance=50000.0,
)

# 查看结果
print(f"总收益: {result.total_return:.2f}%")
print(f"夏普比率: {result.sharpe_ratio:.2f}")
print(f"最大回撤: {result.max_drawdown:.2f}%")
print(f"IC均值: {result.ic_mean:.4f}")
```

### 2. 带图表的回测

```python
from src.backtest import run_backtest, BacktestChartGenerator

result = run_backtest(
    calculator=MyFactor(),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 3, 31, tzinfo=timezone.utc),
    initial_balance=50000.0,
)

# 生成图表
chart_gen = BacktestChartGenerator("results")
charts = chart_gen.generate_all_charts(result, "my_backtest")

print("生成的图表:")
for chart_type, path in charts.items():
    print(f"  {chart_type}: {path}")
```

### 3. 高级配置

```python
from src.backtest import run_backtest

result = run_backtest(
    calculator=MyFactor(),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 3, 31, tzinfo=timezone.utc),
    initial_balance=50000.0,
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],  # 指定交易对
    capital_allocation="rank_weight",  # 资金分配方式
    long_count=5,   # 做多数量
    short_count=5,   # 做空数量
)
```

## 回测结果 (BacktestResult)

统一的回测结果对象，包含所有指标：

```python
# 收益指标
result.total_return      # 总收益率 (%)
result.annual_return    # 年化收益率 (%)
result.monthly_returns  # 月度收益列表

# 风险指标
result.sharpe_ratio     # 夏普比率
result.sortino_ratio    # 索提诺比率
result.calmar_ratio     # 卡玛比率
result.volatility       # 年化波动率

# 回撤指标
result.max_drawdown     # 最大回撤 (%)
result.max_drawdown_duration  # 最大回撤持续期数

# 交易统计
result.total_trades     # 总交易数
result.winning_trades   # 盈利交易数
result.losing_trades   # 亏损交易数
result.win_rate         # 胜率 (%)
result.profit_factor   # 盈亏比
result.avg_trade       # 平均每笔收益

# 多空统计
result.long_trades      # 做多交易数
result.short_trades    # 做空交易数
result.long_win_rate   # 做多胜率
result.short_win_rate  # 做空胜率

# 因子评估
result.ic_mean         # IC均值
result.rank_ic_mean    # Rank IC
result.icir            # ICIR
result.selection_accuracy  # 选币准确率
result.long_spread     # 多空Spread
```

## 评估指标

### 因子评估指标 (FactorMetrics)

| 指标 | 说明 | 参考 |
|------|------|------|
| IC | 信息系数，>0.03 为有效 | 越高越好 |
| Rank IC | 秩相关系数，更稳健 | >0.03 有效 |
| ICIR | IC均值/IC标准差 | >0.5 为好 |
| 选币准确率 | 分组收益差 | 越高越好 |
| 多空Spread | 做多组-做空组收益 | 正值有效 |
| 换手率 | 权重变化率 | 适中最好 |

### Alpha评估指标 (AlphaMetrics)

| 指标 | 说明 | 参考 |
|------|------|------|
| 总收益率 | 期间总收益 | 越高越好 |
| 年化收益率 | 年化收益 | 正值为好 |
| 夏普比率 | 风险调整收益 | >1 好，>2 优秀 |
| 索提诺比率 | 下行风险调整 | 越高越好 |
| 最大回撤 | 最大跌幅 | 越小越好 |
| 卡玛比率 | 年化收益/最大回撤 | >2 为好 |
| 胜率 | 盈利交易占比 | 越高越好 |
| 盈亏比 | 平均盈利/平均亏损 | >1 为好 |

## 运行示例

```bash
# 运行完整示例
python backtest_example.py
```

## 图表输出

运行回测后会生成以下图表（保存在 `backtest_results/` 目录）：

| 图表 | 说明 |
|------|------|
| `{name}_equity_curve.png` | 净值曲线 |
| `{name}_drawdown.png` | 回撤分析 |
| `{name}_returns.png` | 收益分布 |
| `{name}_trades.png` | 交易分析 |
| `{name}_summary.png` | 综合仪表 |

同时生成 CSV 数据文件：

| 文件 | 说明 |
|------|------|
| `{name}_portfolio.csv` | 账户历史 |
| `{name}_trades.csv` | 交易记录 |

## 模拟数据

如果没有真实数据，可以使用模拟数据：

```python
from src.backtest.mock_data import MockDataManager
from src.backtest.backtest import run_backtest

# 生成模拟数据
MockDataManager.generate_data_for_backtest(
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 3, 31, tzinfo=timezone.utc),
)

# 然后运行回测
result = run_backtest(...)
```

## 完整示例

```python
from datetime import datetime, timezone
from src.backtest import run_backtest, BacktestChartGenerator

# 动量因子
class MomentumFactor:
    name = "momentum"
    
    def run(self, view):
        weights = {}
        for symbol in view.iter_symbols():
            bar = view.get_bar(symbol, tail=20)
            if not bar.empty and len(bar) >= 5:
                ma5 = bar['close'].rolling(5).mean().iloc[-1]
                ma20 = bar['close'].rolling(20).mean().iloc[-1]
                if ma20 > 0:
                    score = (ma5 - ma20) / ma20
                    weights[symbol] = score * 5
        return weights

# 运行回测
result = run_backtest(
    calculator=MomentumFactor(),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 6, 30, tzinfo=timezone.utc),
    initial_balance=100000.0,
    capital_allocation="rank_weight",
    long_count=5,
    short_count=5,
)

# 生成图表
charts = BacktestChartGenerator("results").generate_all_charts(
    result, "momentum_factor"
)

# 打印结果
print("\n" + "="*50)
print(f"总收益: {result.total_return:.2f}%")
print(f"夏普: {result.sharpe_ratio:.2f}")
print(f"最大回撤: {result.max_drawdown:.2f}%")
print(f"胜率: {result.win_rate:.2f}%")
print(f"IC: {result.ic_mean:.4f}")
print("="*50)
```

## API 参考

### run_backtest()

```python
from src.backtest import run_backtest

result = run_backtest(
    calculator: AlphaCalculatorBase,     # 因子计算器
    start_date: datetime,                 # 开始日期
    end_date: datetime,                   # 结束日期
    initial_balance: float = 10000.0,     # 初始资金
    symbols: Optional[List[str]] = None,   # 交易对列表
    capital_allocation: str = "equal_weight",  # 资金分配
    long_count: int = 10,                # 做多数量
    short_count: int = 10,                # 做空数量
    verbose: bool = True,                # 打印进度
) -> BacktestResult
```

### BacktestChartGenerator

```python
from src.backtest import BacktestChartGenerator

gen = BacktestChartGenerator(output_dir="results")

# 生成所有图表
charts = gen.generate_all_charts(result, name="backtest")

# 导出数据
portfolio_file, trades_file = gen.export_data_csv(result, name="backtest")
```

### 便捷函数

```python
from src.backtest import (
    run_backtest,           # 运行回测
    compare_calculators,   # 对比因子
    FactorEvaluator,       # 因子评估
    AlphaEvaluator,        # Alpha评估
)
```
