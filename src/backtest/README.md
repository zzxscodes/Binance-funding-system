# 回测系统 README

这是 Binance 量化交易系统中的回测模块，提供完整的历史数据重放和策略回测功能。

## 📦 模块内容

- **models.py** - 数据模型定义（BacktestConfig、Trade、Position等）
- **mock_data.py** - 模拟数据生成器，支持生成几何布朗运动模型的K线数据
- **replay.py** - 数据重放引擎，按时间顺序加载和迭代历史K线
- **executor.py** - 回测执行引擎，驱动整个回测过程
- **api.py** - 高级API接口，简化用户调用
- **analysis.py** - 结果分析和报告生成

## 🚀 快速开始

### 最小化示例

```python
from datetime import datetime, timezone
from src.backtest import BacktestAPI, create_backtest_config, MockDataManager, PortfolioState

# Step 1: 生成模拟数据
manager = MockDataManager()
manager.generate_and_save_mock_data(
    symbols=["BTCUSDT"],
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
    initial_prices={"BTCUSDT": 42000},
    seed=42
)

# Step 2: 创建回测配置
config = create_backtest_config(
    name="my_backtest",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
    initial_balance=10000.0,
    symbols=["BTCUSDT"]
)

# Step 3: 定义交易策略
def simple_strategy(portfolio: PortfolioState, klines: dict) -> dict:
    """永远持仓50%"""
    return {"BTCUSDT": 0.5}

# Step 4: 运行回测
result = BacktestAPI.run_backtest(config, simple_strategy)

# Step 5: 查看结果
print(f"总交易数: {result.total_trades}")
print(f"总收益率: {result.total_return:.2f}%")
print(f"夏普比率: {result.sharpe_ratio:.2f}")
```

## 📚 文档

- **[完整使用指南](../BACKTEST_GUIDE.md)** - 详细的功能说明和API参考
- **[快速参考](../BACKTEST_QUICK_REFERENCE.md)** - 常用模板和速查表
- **[示例脚本](../backtest_examples.py)** - 5个完整的使用示例

## 🎯 核心功能

### 1. 模拟数据生成

使用几何布朗运动生成现实的K线数据：

```python
from src.backtest import MockDataManager

manager = MockDataManager()
manager.generate_and_save_mock_data(
    symbols=["BTCUSDT", "ETHUSDT"],
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
    initial_prices={"BTCUSDT": 40000, "ETHUSDT": 2300},
    volatilities={"BTCUSDT": 0.02, "ETHUSDT": 0.025},
    seed=42  # 使用固定种子确保可复现
)
```

### 2. 数据重放

按时间顺序加载和迭代历史K线数据：

```python
from src.backtest import DataReplayEngine

engine = DataReplayEngine(
    symbols=["BTCUSDT"],
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2024, 1, 31, tzinfo=timezone.utc),
    interval="5m"
)

for timestamp, klines_snapshot in engine.replay_iterator():
    for symbol, kline in klines_snapshot.items():
        print(f"{symbol}: {kline.close}")
```

### 3. 策略执行

在模拟环境中执行交易策略，跟踪账户状态：

```python
from src.backtest import BacktestExecutor

executor = BacktestExecutor(config, replay_engine)
result = executor.run(strategy_function)
```

### 4. 结果分析

生成详细的回测报告和统计指标：

```python
from src.backtest import BacktestAnalyzer

# 生成报告
report = BacktestAnalyzer.generate_report(result)
print(report)

# 导出数据
BacktestAnalyzer.export_trades_csv(result, Path("trades.csv"))
BacktestAnalyzer.export_portfolio_history_csv(result, Path("portfolio.csv"))

# 统计指标
stats = BacktestAnalyzer.calculate_statistics(result)
```

## 📊 策略函数签名

策略函数接收两个参数，返回权重字典：

```python
def strategy(
    portfolio: PortfolioState,  # 当前账户状态
    klines: Dict[str, KlineSnapshot]  # 当前K线数据
) -> Dict[str, float]:  # 返回持仓权重
    """
    权重表示占账户总余额的百分比：
    - 0.0: 不持仓
    - 0.5: 用50%的余额持仓
    - 1.0: 用100%的余额持仓
    """
    return {symbol: weight for symbol, weight in ...}
```

## 📈 输出指标说明

| 指标 | 说明 | 范围 |
|-----|------|------|
| `total_return` | 回测期总收益率 | -100% ~ +∞ |
| `annual_return` | 年化收益率 | -100% ~ +∞ |
| `sharpe_ratio` | 夏普比率（风险调整收益） | > 1 为好 |
| `max_drawdown` | 最大回撤 | 0% ~ -100% |
| `win_rate` | 胜率 | 0% ~ 100% |
| `profit_factor` | 盈亏比 | > 1 为好 |

## ⚙️ 配置参数

### BacktestConfig

```python
BacktestConfig(
    name="test",                    # 回测名称
    start_date=datetime(...),       # 开始日期（UTC）
    end_date=datetime(...),         # 结束日期（UTC）
    initial_balance=100000.0,       # 初始资金
    symbols=["BTCUSDT"],            # 交易对列表
    leverage=1.0,                   # 杠杆倍数（1-125）
    maker_fee=0.0002,               # 挂单手续费
    taker_fee=0.0004,               # 吃单手续费
    slippage=0.0,                   # 滑点（万分位）
    funding_rate_apply=True,        # 是否应用资金费率
)
```

## 🔍 调试技巧

1. **检查数据加载**
```python
engine = DataReplayEngine(...)
print(f"Has data: {engine.has_data()}")
print(f"Symbols: {engine.get_available_symbols()}")
```

2. **查看策略执行**
```python
def debug_strategy(portfolio, klines):
    print(f"Balance: {portfolio.total_balance}")
    print(f"Symbols available: {list(klines.keys())}")
    return weights
```

3. **逐步调试**
```python
for i, (ts, klines) in enumerate(engine.replay_iterator()):
    if i > 100:  # 只看前100步
        break
    print(f"Step {i}: {ts}")
```

## 📁 文件结构

```
src/backtest/
├── __init__.py          # 模块导出
├── models.py            # 数据模型
├── mock_data.py         # 模拟数据生成
├── replay.py            # 数据重放
├── executor.py          # 回测执行
├── api.py               # API接口
└── analysis.py          # 结果分析
```