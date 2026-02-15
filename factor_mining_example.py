#!/usr/bin/env python3
"""
因子挖掘系统使用示例（真实数据）

演示如何使用因子挖掘系统进行：
1. 因子生成
2. 因子评估（使用真实数据）
3. 因子筛选
4. 因子部署

注意：此示例使用真实数据，需要系统中有历史数据
快速演示请参考 factor_mining_example_mock.py（使用模拟数据）
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
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
from src.strategy.calculators import load_calculators


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
    """示例：评估因子（使用真实数据）"""
    print("\n" + "=" * 60)
    print("示例2: 评估因子（使用真实数据）")
    print("=" * 60)
    
    # 使用真实数据创建评估器（默认）
    evaluator = FactorEvaluator(use_mock_data=False)
    
    # 加载现有因子
    calculators = load_calculators()
    if not calculators:
        print("没有找到可用的因子，请先生成因子")
        print("提示：可以使用 factor_mining_example_mock.py 查看使用模拟数据的示例")
        return
    
    calculator = calculators[0]
    print(f"评估因子: {calculator.name}")
    
    # 评估时间范围（使用配置默认值）
    end_date = datetime.now()
    start_date = end_date - timedelta(days=get_default_history_days())
    
    print(f"时间范围: {start_date.date()} - {end_date.date()}")
    print("使用真实历史数据")
    
    # 评估因子（不使用回测，因为可能需要较长时间）
    metrics = evaluator.evaluate_factor(
        calculator,
        start_date,
        end_date,
        use_backtest=False  # 设置为False以加快速度
    )
    
    print("\n评估结果:")
    print(f"  IC均值: {metrics.ic_mean:.4f}")
    print(f"  IC标准差: {metrics.ic_std:.4f}")
    print(f"  IC IR: {metrics.ic_ir:.4f}")
    print(f"  IC正比例: {metrics.ic_positive_ratio:.4f}")
    print(f"  覆盖率: {metrics.coverage:.4f}")


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
    """示例：筛选因子"""
    print("\n" + "=" * 60)
    print("示例4: 筛选因子")
    print("=" * 60)
    
    evaluator = FactorEvaluator()
    selector = FactorSelector(evaluator)
    
    # 加载所有因子
    calculators = load_calculators()
    if len(calculators) < 2:
        print("需要至少2个因子才能进行筛选")
        return
    
    print(f"加载了 {len(calculators)} 个因子")
    
    # 评估所有因子（简化版，只评估IC）
    print("评估因子中...")
    # 注意：实际评估可能需要较长时间，这里只是示例
    
    print("筛选完成（示例）")


def main():
    """主函数"""
    print("因子挖掘系统使用示例（真实数据）")
    print("=" * 60)
    print("注意：此示例使用真实数据，需要系统中有历史数据")
    print("快速演示请参考: factor_mining_example_mock.py（使用模拟数据）")
    print("=" * 60)
    
    try:
        # 示例1: 生成因子
        example_generate_factor()
        
        # 示例2: 评估因子（使用真实数据）
        # example_evaluate_factor()  # 需要真实数据，可能较慢
        
        # 示例3: 参数搜索
        example_search_parameters()
        
        # 示例4: 筛选因子
        # example_select_factors()  # 需要评估，可能较慢
        
        print("\n" + "=" * 60)
        print("示例完成！")
        print("=" * 60)
        print("\n此示例使用真实数据，需要系统中有历史数据")
        print("快速演示请参考: factor_mining_example_mock.py")
        print("更多信息请参考: src/factor_mining/README.md")
        
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
