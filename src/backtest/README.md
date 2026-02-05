# 多因子回测系统 (Multi-Factor Backtest System)

## 概述

完整的多因子量化交易回测系统，支持：

- **因子评估** - 评估单个因子(Calculator)的预测能力 (IC, Rank IC, 选币准确率)
- **Alpha回测** - 评估多因子组合的整体表现 (收益, 风险, 回撤)
- **因子对比** - 对比多个因子的表现
- **图表生成** - 可视化因子评估和Alpha回测结果

## 文件结构

```
src/backtest/
├── __init__.py              # 模块导出
├── backtest.py              # 核心回测引擎
├── metrics.py               # 评估指标体系 (FactorMetrics, AlphaMetrics, 评估器)
├── chart.py                 # 图表生成 (generate_factor_charts, generate_alpha_charts)
├── models.py                # 数据模型
├── api.py                   # API接口
├── analysis.py              # 结果分析
├── executor.py              # 执行引擎
├── replay.py                # 数据重放
├── mock_data.py             # 模拟数据
└── config.py                # 配置
```

## 快速开始

### 1. 因子评估

评估单个因子的预测能力：

```python
from datetime import datetime, timezone, timedelta
from src.backtest import FactorEvaluator
import numpy as np

# 准备因子数据和收益数据
factor_data = {}
returns_data = {}

for i in range(100):
    ts = datetime(2024, 1, 1) + timedelta(days=i)
    factor_values = {f"SYM{j}": np.random.randn() * 0.1 for j in range(50)}
    returns = {f"SYM{j}": np.random.randn() * 0.02 for j in range(50)}
    factor_data[ts] = factor_values
    returns_data[ts] = returns

# 评估因子
metrics = FactorEvaluator.evaluate_factor(
    name="my_factor",
    factor_weights=factor_data,
    next_returns=returns_data,
    groups=5,
)

print(f"IC均值: {metrics.ic_mean:.4f}")
print(f"ICIR: {metrics.icir:.4f}")
print(f"选币准确率: {metrics.selection_accuracy:.2f}%")
```

### 2. Alpha回测

评估策略的整体表现：

```python
from datetime import datetime, timezone
from src.backtest import (
    BacktestConfig, BacktestResult, Trade, PortfolioState,
    OrderSide, AlphaEvaluator
)

# 创建配置
config = BacktestConfig(
    name="my_strategy",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 6, 30, tzinfo=timezone.utc),
    initial_balance=50000.0,
    symbols=["BTCUSDT", "ETHUSDT"],
    leverage=1.0,
)

# 创建回测结果
trades = [...]
portfolio_history = [...]

result = BacktestResult(
    config=config,
    trades=trades,
    portfolio_history=portfolio_history,
    start_time=config.start_date,
    end_time=config.end_date,
    total_return=15.5,
    annual_return=32.1,
    sharpe_ratio=1.85,
    max_drawdown=8.5,
    win_rate=62.5,
    # ... 其他指标
)

# 评估Alpha
alpha_metrics = AlphaEvaluator.evaluate_alpha(
    name="my_strategy",
    portfolio_values=[s.total_balance for s in portfolio_history],
    trades=trades,
    risk_free_rate=0.02,
)

print(f"总收益: {alpha_metrics.total_return:.2f}%")
print(f"夏普比率: {alpha_metrics.sharpe_ratio:.2f}")
print(f"最大回撤: {alpha_metrics.max_drawdown:.2f}%")
```

### 3. 生成图表

```python
from src.backtest.chart import generate_factor_charts, generate_alpha_charts, export_csv

# 生成因子评估图表
factor_charts = generate_factor_charts(
    metrics=metrics,
    output_dir="backtest_results",
    name="my_factor"
)

# 生成Alpha评估图表
alpha_charts = generate_alpha_charts(
    result=result,
    output_dir="backtest_results",
    name="my_strategy"
)

# 导出CSV数据
portfolio_file, trades_file = export_csv(
    result=result,
    output_dir="backtest_results",
    name="my_strategy"
)
```

## 评估指标

### 因子评估指标 (FactorMetrics)

| 指标 | 说明 | 参考 |
|------|------|------|
| ic_mean | IC均值，信息系数 | >0.03 为有效 |
| ic_std | IC标准差 | 越小越稳定 |
| icir | IC均值/IC标准差 | >0.5 为好 |
| rank_ic_mean | Rank IC，秩相关系数 | >0.03 有效 |
| selection_accuracy | 选币准确率 | 越高越好 |
| long_spread | 做多组-做空组收益差 | 正值有效 |
| top_group_return | 最高分组收益 | 越高越好 |
| bottom_group_return | 最低分组收益 | 越低越好 |
| group_return_spread | 分组收益差 | 越大因子越有效 |
| turnover | 换手率 | 适中最好 |

### Alpha评估指标 (AlphaMetrics)

| 指标 | 说明 | 参考 |
|------|------|------|
| total_return | 总收益率 | 越高越好 |
| annual_return | 年化收益率 | 正值为好 |
| sharpe_ratio | 夏普比率 | >1 好，>2 优秀 |
| sortino_ratio | 索提诺比率 | 越高越好 |
| max_drawdown | 最大回撤 | 越小越好 |
| win_rate | 胜率 | 越高越好 |
| profit_factor | 盈亏比 | >1 为好 |
| total_trades | 总交易数 | - |
| winning_trades | 盈利交易数 | - |
| losing_trades | 亏损交易数 | - |

## 图表输出

### 因子评估图表 (generate_factor_charts)

生成 `name_factor_charts.png`，包含4个子图：

1. **IC Time Series** - IC时间序列变化
2. **IC Distribution** - IC分布直方图
3. **Group Returns** - 各分组收益柱状图
4. **Factor Metrics** - 因子指标汇总表

### Alpha评估图表 (generate_alpha_charts)

生成4个独立图表：

| 图表 | 文件 | 说明 |
|------|------|------|
| 净值曲线 | `{name}_equity_curve.png` | 累计收益曲线 |
| 回撤分析 | `{name}_drawdown.png` | 回撤随时间变化 |
| 交易分析 | `{name}_trades.png` | 累计PnL、单笔PnL、PnL分布、按符号PnL |
| 汇总表 | `{name}_summary.png` | 所有指标汇总 |

### CSV导出

| 文件 | 说明 |
|------|------|
| `{name}_portfolio.csv` | 账户历史 (timestamp, total_balance, total_pnl) |
| `{name}_trades.csv` | 交易记录 (symbol, side, quantity, price, pnl) |

## 运行示例

```bash
# 激活环境
cd Binance-funding-system
source quant/bin/activate

# 运行示例
python backtest_example.py

# 查看结果
ls backtest_results/
```

示例输出：

```
因子评估结果:
  IC均值: 0.0173
  IC标准差: 0.1512
  ICIR: 0.1145
  Rank IC: 0.0150

Alpha评估结果:
  总收益率: -29.32%
  年化收益率: -58.29%
  夏普比率: -2.42
  最大回撤: 30.62%

生成的图表:
  equity_curve: backtest_results/alpha_demo_equity_curve.png
  drawdown: backtest_results/alpha_demo_drawdown.png
  trades: backtest_results/alpha_demo_trades.png
  summary: backtest_results/alpha_demo_summary.png
```

## 便捷函数

```python
from src.backtest import (
    # 评估器
    FactorEvaluator,      # 因子评估
    AlphaEvaluator,       # Alpha评估
    
    # 评估指标
    FactorMetrics,        # 因子指标对象
    AlphaMetrics,         # Alpha指标对象
    
    # 图表生成
    generate_factor_charts,   # 生成因子图表
    generate_alpha_charts,    # 生成Alpha图表
    export_csv,               # 导出CSV
    
    # 回测运行
    run_single_calculator_backtest,
    run_alpha_backtest,
    run_backtest,
    compare_calculators,
    
    # 数据模型
    BacktestConfig,
    BacktestResult,
    Trade,
    PortfolioState,
)
```

## 模拟数据

如果没有真实数据，可以使用模拟数据：

```python
from src.backtest.mock_data import MockDataManager

# 生成模拟K线数据
MockDataManager.generate_klines(
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 3, 31, tzinfo=timezone.utc),
)
```

## 核心概念

### 资金分配方式

- **equal_weight** - 等权重，每个选中的币种分配相同资金
- **rank_weight** - 排名权重，排名靠前的币种分配更多资金

### 因子评估 vs Alpha评估

**因子评估**关注因子本身的预测能力：
- 因子值与未来收益的相关性 (IC, Rank IC)
- 分组收益单调性
- 选币准确率

**Alpha评估**关注策略的整体表现：
- 收益率、夏普比率、最大回撤
- 胜率、盈亏比
- 交易统计

## 目录结构建议

```
Binance-funding-system/
├── backtest_example.py      # 使用示例
├── backtest_results/        # 图表输出目录
│   ├── factor_demo_factor_charts.png
│   ├── alpha_demo_equity_curve.png
│   ├── alpha_demo_drawdown.png
│   ├── alpha_demo_trades.png
│   ├── alpha_demo_summary.png
│   ├── demo_portfolio.csv
│   └── demo_trades.csv
└── src/backtest/            # 核心模块
```