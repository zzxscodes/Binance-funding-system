#!/usr/bin/env python3
"""
因子挖掘系统使用示例（使用模拟数据）

演示如何使用因子挖掘系统进行：
1. 因子生成
2. 因子评估（使用模拟数据）
3. 因子筛选
4. 因子部署

注意：此示例使用模拟数据，适合快速演示和测试
真实使用请参考 factor_mining_example.py
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.factor_mining import (
    FactorGenerator,
    FactorEvaluator,
    FactorSelector,
    FactorLiveIntegrator,
    get_default_interval,
    get_default_lookback_bars,
    get_default_history_days,
    get_default_initial_balance,
)
from src.factor_mining.mock_data import get_mock_data_api
from src.strategy.calculator import AlphaCalculatorBase, AlphaDataView


class MockCalculator(AlphaCalculatorBase):
    """模拟因子计算器，用于示例"""
    
    def __init__(self, name: str = "mock_factor"):
        self.name = name
        self.mutates_inputs = False
    
    def run(self, view: AlphaDataView) -> dict:
        """计算因子值（模拟）"""
        weights = {}
        for symbol in view.iter_symbols():
            bar = view.get_bar(symbol, tail=20)
            if not bar.empty and len(bar) >= 10:
                # 简单的动量因子：过去5期的收益率
                close_prices = bar['close'].astype(float)
                if len(close_prices) >= 5:
                    ret = (close_prices.iloc[-1] - close_prices.iloc[-5]) / close_prices.iloc[-5]
                    weights[symbol] = float(ret * 3)  # 放大3倍
                else:
                    weights[symbol] = 0.0
            else:
                weights[symbol] = 0.0
        return weights


def example_generate_factor():
    """示例：生成因子"""
    print("=" * 60)
    print("示例1: 生成因子")
    print("=" * 60)
    
    generator = FactorGenerator()
    
    # 生成因子代码
    code = generator.generate_factor(
        'mean_ratio',
        lookback_bars=get_default_lookback_bars(),
        numerator='buy_dolvol4',
        denominator='dolvol'
    )
    
    print("生成的因子代码（前500字符）:")
    print(code[:500])
    print("...")
    
    # 生成calculator文件（会自动保存到data/factor_mining目录）
    from src.factor_mining.result_saver import FactorMiningResultSaver
    output_dir = FactorMiningResultSaver.get_output_dir() / "generated"
    
    filepath = generator.generate_calculator_file(
        'mean_ratio',
        output_dir,
        lookback_bars=get_default_lookback_bars(),
        numerator='buy_dolvol4',
        denominator='dolvol'
    )
    
    print(f"\n因子文件已生成: {filepath}")
    print(f"结果保存在: {output_dir}")


def example_evaluate_factor():
    """示例：评估因子（使用模拟数据）"""
    print("\n" + "=" * 60)
    print("示例2: 评估因子（使用模拟数据）")
    print("=" * 60)
    
    # 使用模拟数据创建评估器
    evaluator = FactorEvaluator(use_mock_data=True)
    
    # 创建模拟因子计算器
    calculator = MockCalculator("mock_momentum_factor")
    print(f"评估因子: {calculator.name}")
    
    # 评估时间范围（使用较短的时间范围，加快示例速度）
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=2)  # 只使用2天数据，加快速度
    
    print(f"时间范围: {start_date.date()} - {end_date.date()}")
    print("使用模拟数据，无需真实历史数据")
    print("注意：为了加快示例速度，只使用2天数据")
    
    # 评估因子（不使用回测，因为模拟数据可能不够完整）
    print("开始评估...")
    # 使用更大的时间间隔（1小时）来减少迭代次数
    metrics = evaluator.evaluate_factor(
        calculator,
        start_date,
        end_date,
        use_backtest=False,  # 模拟数据可能不支持完整回测
        interval='1h'  # 使用1小时间隔，减少迭代次数
    )
    
    print("\n评估结果:")
    print(f"  IC均值: {metrics.ic_mean:.4f}")
    print(f"  IC标准差: {metrics.ic_std:.4f}")
    print(f"  IC IR: {metrics.ic_ir:.4f}")
    print(f"  IC正比例: {metrics.ic_positive_ratio:.4f}")
    print(f"  覆盖率: {metrics.coverage:.4f}")
    
    print("\n注意：这是使用模拟数据的评估结果，仅用于演示")
    print("真实使用请使用真实数据，参考 factor_mining_example.py")


def example_search_parameters():
    """示例：参数搜索"""
    print("\n" + "=" * 60)
    print("示例3: 参数搜索")
    print("=" * 60)
    
    generator = FactorGenerator()
    
    # 搜索参数组合
    default_lookback = get_default_lookback_bars()
    param_combinations = generator.search_parameters(
        'mean_ratio',
        param_grid={
            'lookback_bars': [default_lookback // 2, default_lookback, int(default_lookback * 1.5)],
            'numerator': ['buy_dolvol4', 'buy_dolvol3'],
            'denominator': ['dolvol'],
        }
    )
    
    print(f"找到 {len(param_combinations)} 个参数组合:")
    for i, params in enumerate(param_combinations[:5]):  # 只显示前5个
        print(f"  {i+1}. {params}")
    if len(param_combinations) > 5:
        print(f"  ... 还有 {len(param_combinations) - 5} 个组合")


def example_select_factors():
    """示例：筛选因子（使用模拟数据）"""
    print("\n" + "=" * 60)
    print("示例4: 筛选因子（使用模拟数据）")
    print("=" * 60)
    
    # 使用模拟数据创建评估器和选择器
    evaluator = FactorEvaluator(use_mock_data=True)
    selector = FactorSelector(evaluator)
    
    # 创建多个模拟因子
    calculators = [
        MockCalculator("mock_factor_1"),
        MockCalculator("mock_factor_2"),
        MockCalculator("mock_factor_3"),
    ]
    
    print(f"创建了 {len(calculators)} 个模拟因子")
    
    # 评估时间范围（使用较短的时间范围，加快示例速度）
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=2)  # 只使用2天数据，加快速度
    
    # 评估所有因子
    print("评估因子中...")
    print("注意：为了加快示例速度，只使用2天数据，使用1小时间隔")
    metrics_dict = {}
    for calc in calculators:
        try:
            metrics = evaluator.evaluate_factor(
                calc,
                start_date,
                end_date,
                use_backtest=False,
                interval='1h'  # 使用1小时间隔，减少迭代次数
            )
            metrics_dict[calc.name] = metrics
            print(f"  ✓ {calc.name}: IC={metrics.ic_mean:.4f}, ICIR={metrics.ic_ir:.4f}")
        except Exception as e:
            print(f"  ✗ {calc.name}: 评估失败 - {e}")
    
    if len(metrics_dict) >= 2:
        # 筛选因子
        print("\n筛选因子中...")
        # 注意：这里需要AlphaDataView，模拟数据可能不支持完整筛选
        print("筛选完成（示例）")
        print("注意：完整筛选需要真实数据，参考 factor_mining_example.py")
    else:
        print("需要至少2个因子才能进行筛选")


def main():
    """主函数"""
    print("因子挖掘系统使用示例（模拟数据）")
    print("=" * 60)
    print("注意：此示例使用模拟数据，适合快速演示和测试")
    print("真实使用请参考 factor_mining_example.py")
    print("=" * 60)
    
    try:
        # 示例1: 生成因子
        example_generate_factor()
        
        # 示例2: 评估因子（使用模拟数据）
        example_evaluate_factor()
        
        # 示例3: 参数搜索
        example_search_parameters()
        
        # 示例4: 筛选因子（使用模拟数据）
        example_select_factors()
        
        print("\n" + "=" * 60)
        print("示例完成！")
        print("=" * 60)
        print("\n此示例使用模拟数据，结果仅用于演示")
        print("真实使用请参考: factor_mining_example.py")
        print("更多信息请参考: src/factor_mining/README.md")
        
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
