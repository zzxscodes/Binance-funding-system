# Calculator（因子）开发指南

## 概述

Calculator是系统的核心组件，每个Calculator实现一个独立的因子逻辑，系统会自动发现、加载并并发执行所有Calculator，最后将结果求和得到最终的权重向量。

## 目录结构

```
src/strategy/calculators/
├── __init__.py          # 自动加载机制
├── template.py          # 开发模板（复制此文件开始开发）
├── mean_buy_dolvol4_over_dolvol_rank.py  # 示例Calculator
├── researcher1_factor1.py  # 研究员1的因子1
├── researcher2_factor2.py  # 研究员2的因子2
└── ...
```

## 快速开始

### 1. 创建新的Calculator文件

复制 `template.py` 并重命名：

```bash
cp src/strategy/calculators/template.py src/strategy/calculators/your_name_factor_name.py
```

### 2. 实现Calculator类

```python
from ..calculator import AlphaCalculatorBase, AlphaDataView
from typing import Dict

class YourFactorCalculator(AlphaCalculatorBase):
    def __init__(self):
        self.name = "your_name_factor_name"  # 必须唯一
        self.mutates_inputs = False  # 推荐False，性能更好

    def run(self, view: AlphaDataView) -> Dict[str, float]:
        """实现你的因子逻辑"""
        weights = {}
        for sym in view.iter_symbols():
            # 获取数据
            bar_df = view.get_bar(sym, tail=100)  # 最近100根K线
            tran_df = view.get_tran_stats(sym, tail=100)
            
            # 计算因子
            score = your_calculation(bar_df, tran_df)
            weights[sym] = score
        
        return weights

# 创建实例（系统会自动发现）
CALCULATOR_INSTANCE = YourFactorCalculator()
```

### 3. 系统自动加载

系统会在启动时自动发现并加载你的Calculator，无需修改核心代码。

## 数据访问

### AlphaDataView API

```python
# 遍历所有交易对
for sym in view.iter_symbols():
    # 获取K线数据（DataFrame）
    bar_df = view.get_bar(sym, tail=100)  # 最近100根
    # 列包括：open_time, open, high, low, close, volume, dolvol, ...
    
    # 获取交易统计数据（DataFrame）
    tran_df = view.get_tran_stats(sym, tail=100)  # 最近100根
    # 列包括：open_time, buy_dolvol4, sell_dolvol4, ...
```

### 数据格式

- **交易对符号**：系统格式，如 `"btc-usdt"`（小写，用`-`分隔）
- **时间列**：`open_time`（datetime或timestamp）
- **K线数据**：包含 `open`, `high`, `low`, `close`, `volume`, `dolvol` 等
- **交易统计**：包含 `buy_dolvol4`, `sell_dolvol4` 等

## 最佳实践

### 1. 命名规范

- 文件名：`研究员名_因子名.py`（如 `zhang_momentum_factor.py`）
- Calculator名称：`研究员名_因子名_参数`（如 `zhang_momentum_20d`）
- 确保名称唯一，避免冲突

### 2. 性能优化

- **设置 `mutates_inputs=False`**：如果不需要修改输入数据，设为False可提升性能
- **使用 `tail` 参数**：只获取需要的数据量，避免处理过多历史数据
- **避免重复计算**：缓存中间结果

### 3. 错误处理

```python
def run(self, view: AlphaDataView) -> Dict[str, float]:
    weights = {}
    for sym in view.iter_symbols():
        try:
            bar_df = view.get_bar(sym, tail=100)
            if bar_df.empty:
                continue
            # 你的计算逻辑
            weights[sym] = calculate(bar_df)
        except Exception as e:
            # 记录错误但继续处理其他交易对
            logger.warning(f"计算 {sym} 时出错: {e}")
            continue
    return weights
```

### 4. 权重设计

- 权重可以是任意浮点数（正数=做多，负数=做空）
- 系统会自动处理归一化和求和
- 建议：根据因子强度设置合理的权重范围

## 配置Calculator

在 `config/default.yaml` 中配置要加载的Calculators：

```yaml
strategy:
  calculators:
    enabled:
      - "mean_buy_dolvol4_over_dolvol_rank"
      - "zhang_momentum_factor"
      - "li_reversal_factor"
```

如果不配置 `enabled`，系统会自动发现 `calculators` 目录下的所有Calculator。

## 测试Calculator

### 单元测试示例

```python
def test_your_calculator():
    from src.strategy.calculators.your_name_factor_name import CALCULATOR_INSTANCE
    from src.strategy.calculator import AlphaDataView
    import pandas as pd
    
    # 准备测试数据
    bar_data = {"btc-usdt": pd.DataFrame(...)}
    tran_stats = {"btc-usdt": pd.DataFrame(...)}
    view = AlphaDataView(bar_data=bar_data, tran_stats=tran_stats)
    
    # 运行Calculator
    weights = CALCULATOR_INSTANCE.run(view)
    
    # 验证结果
    assert isinstance(weights, dict)
    assert "btc-usdt" in weights
```

## 常见问题

### Q: 如何确保我的Calculator被加载？

A: 检查以下几点：
1. 文件在 `src/strategy/calculators/` 目录下
2. 文件定义了 `CALCULATOR_INSTANCE` 或 `CALCULATOR_CLASS`
3. 如果配置了 `enabled`，确保你的Calculator在列表中
4. 查看日志中的 `calculators_loader` 相关信息

### Q: 多个Calculator会互相影响吗？

A: 不会。每个Calculator：
- 接收独立的数据视图
- 如果 `mutates_inputs=True`，系统会自动复制数据
- 计算结果独立，最后才求和

### Q: 如何调试Calculator？

A:
1. 查看日志：`logs/alpha_calculator_*.log`
2. 使用单元测试验证逻辑
3. 在 `run()` 方法中添加日志输出

### Q: 可以动态启用/禁用Calculator吗？

A: 可以。修改 `config/default.yaml` 中的 `strategy.calculators.enabled` 列表，然后重启系统。

## 示例

参考 `mean_buy_dolvol4_over_dolvol_rank.py` 查看完整实现示例。
