# 因子挖掘系统

因子挖掘系统是一个完整的因子研究、评估、筛选和部署工具，与实盘系统和回测系统完全集成。

## 功能特性

1. **因子生成**
   - 基于模板快速生成因子
   - 支持参数化因子搜索
   - 自动生成calculator代码

2. **因子评估**
   - IC（信息系数）计算
   - IR（信息比率）计算
   - 回测评估（收益、Sharpe比率、最大回撤等）
   - 因子稳定性评估

3. **因子筛选**
   - 相关性筛选（去除高度相关因子）
   - 指标筛选（IC IR、覆盖率等）
   - 综合排名筛选

4. **系统集成**
   - 与回测系统集成（自动回测评估）
   - 与实盘系统集成（自动生成calculator文件）
   - 自动更新配置文件

## 快速开始

### 1. 生成因子

```python
from src.factor_mining import FactorGenerator
from pathlib import Path

generator = FactorGenerator()

# 使用模板生成因子
code = generator.generate_factor(
    'mean_ratio',
    lookback_bars=1000,
    numerator='buy_dolvol4',
    denominator='dolvol'
)

# 生成calculator文件
filepath = generator.generate_calculator_file(
    'mean_ratio',
    Path('src/strategy/calculators'),
    lookback_bars=1000,
    numerator='buy_dolvol4',
    denominator='dolvol'
)
```

### 2. 评估因子

```python
from src.factor_mining import FactorEvaluator
from datetime import datetime, timedelta
from src.strategy.calculators import load_calculators

evaluator = FactorEvaluator()

# 加载因子
calculators = load_calculators()
calculator = calculators[0]  # 选择要评估的因子

# 评估因子
start_date = datetime.now() - timedelta(days=30)
end_date = datetime.now()

metrics = evaluator.evaluate_factor(
    calculator,
    start_date,
    end_date,
    use_backtest=True
)

print(f"IC IR: {metrics.ic_ir}")
print(f"Sharpe Ratio: {metrics.sharpe_ratio}")
print(f"Total Return: {metrics.total_return}")
```

### 3. 筛选因子

```python
from src.factor_mining import FactorSelector, FactorEvaluator
from src.strategy.calculators import load_calculators

evaluator = FactorEvaluator()
selector = FactorSelector(evaluator)

# 加载所有因子
calculators = load_calculators()

# 评估所有因子
metrics_dict = {}
for calc in calculators:
    metrics = evaluator.evaluate_factor(calc, start_date, end_date)
    metrics_dict[calc.name] = metrics

# 筛选最佳因子
best_factors = selector.select_best_factors(
    calculators,
    metrics_dict,
    top_n=10,
    max_correlation=0.8
)
```

### 4. 部署因子到实盘

```python
from src.factor_mining import FactorLiveIntegrator
from pathlib import Path

integrator = FactorLiveIntegrator()

# 部署因子
filepaths = integrator.deploy_factors(
    calculators=best_factors,
    factor_codes=[calc.code for calc in best_factors],  # 需要提供代码
    enable_in_config=True
)
```

## 使用CLI工具

### 生成因子

```bash
python -m src.factor_mining.cli generate mean_ratio \
    --output data/factor_mining \
    --lookback-bars 1000
```

### 参数搜索

```bash
python -m src.factor_mining.cli search mean_ratio \
    --output data/factor_mining \
    --start-date 2026-01-01 \
    --end-date 2026-02-01 \
    --top-n 10
```

### 评估因子

```bash
python -m src.factor_mining.cli evaluate \
    src/strategy/calculators/my_factor.py \
    --start-date 2026-01-01 \
    --end-date 2026-02-01
```

### 部署因子

```bash
python -m src.factor_mining.cli deploy \
    src/strategy/calculators/factor1.py \
    src/strategy/calculators/factor2.py \
    --enable
```

## 因子模板

系统内置了以下因子模板：

1. **mean_ratio**: 均值比率因子
   - 参数：`lookback_bars`, `numerator`, `denominator`
   - 示例：`buy_dolvol4 / dolvol` 的均值

2. **rank**: 排名因子
   - 参数：`lookback_bars`, `field`
   - 示例：基于 `buy_dolvol4` 的排名

## 评估指标

因子评估包括以下指标：

- **IC相关**
  - `ic_mean`: IC均值
  - `ic_std`: IC标准差
  - `ic_ir`: IC信息比率（IC_mean / IC_std）
  - `ic_positive_ratio`: IC为正的比例

- **回测相关**
  - `total_return`: 总收益
  - `annual_return`: 年化收益
  - `sharpe_ratio`: Sharpe比率
  - `max_drawdown`: 最大回撤
  - `win_rate`: 胜率
  - `profit_factor`: 盈亏比

- **稳定性**
  - `ic_stability`: IC稳定性
  - `return_stability`: 收益稳定性
  - `coverage`: 因子覆盖率

## 配置

在 `config/default.yaml` 中配置因子挖掘系统：

```yaml
strategy:
  factor_mining:
    evaluation:
      min_ic_ir: 0.5
      min_coverage: 0.5
      min_sharpe_ratio: 0.5
    selection:
      max_correlation: 0.8
      top_n: 10
      ranking_weights:
        ic_ir: 0.3
        ic_positive_ratio: 0.2
        sharpe_ratio: 0.3
        total_return: 0.2
```

## 工作流程

1. **因子生成**: 使用模板生成因子代码
2. **参数搜索**: 搜索最优参数组合
3. **因子评估**: 评估因子的IC、IR和回测表现
4. **因子筛选**: 根据指标和相关性筛选最佳因子
5. **因子部署**: 自动部署到实盘系统

## 与系统集成

### 回测系统集成

因子挖掘系统自动与回测系统集成，可以：
- 自动运行因子回测
- 获取回测指标
- 比较多个因子的表现

### 实盘系统集成

因子挖掘系统自动与实盘系统集成，可以：
- 自动生成calculator文件
- 更新配置文件
- 启用/禁用因子

## 扩展

### 添加自定义模板

```python
from src.factor_mining import FactorGenerator, FactorTemplate

template = FactorTemplate(
    name='my_template',
    description='我的因子模板',
    code_template='...',  # 因子代码模板
    parameters={'param1': 100},
    parameter_ranges={'param1': (50, 200, 10)}
)

generator = FactorGenerator()
generator.register_template(template)
```

## 注意事项

1. 因子评估需要历史数据，确保数据完整性
2. 参数搜索可能耗时较长，建议使用较小的参数范围
3. 部署因子前请确保通过回测验证
4. 定期检查因子表现，及时淘汰失效因子
