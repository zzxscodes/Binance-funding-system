"""
Calculator开发模板

研究员可以复制此文件，重命名后实现自己的因子逻辑。

开发步骤：
1. 复制此文件，重命名为你的因子名称（如 researcher1_momentum_factor.py）
2. 修改类名和逻辑
3. 设置 name 和 mutates_inputs
4. 实现 run() 方法
5. 在文件末尾创建 CALCULATOR_INSTANCE
6. 系统会自动发现并加载

注意事项：
- name 必须唯一，建议使用格式：因子名_参数
- mutates_inputs=False 时性能更好（推荐）
- mutates_inputs=True 时如果需要修改数据，系统会自动复制
- run() 方法应返回 Dict[系统交易对符号, 权重]
- 权重可以是任意浮点数，系统会自动归一化
"""

from __future__ import annotations

from typing import Dict

from ..calculator import AlphaCalculatorBase, AlphaDataView

# 导出calculator实例（系统会自动发现）
CALCULATOR_INSTANCE = None


class TemplateCalculator(AlphaCalculatorBase):
    """
    模板Calculator - 请修改此类名和逻辑。

    在此处描述你的因子逻辑。
    """

    def __init__(self, name: str = "template_calculator"):
        self.name = name
        self.mutates_inputs = False  # 如果不需要修改输入数据，设为False（推荐）

    def run(self, view: AlphaDataView) -> Dict[str, float]:
        """
        实现你的因子计算逻辑。

        参数:
            view: 数据视图，包含以下方法：
                - view.iter_symbols(): 遍历所有交易对
                - view.get_bar(symbol, tail=N): 获取最近N根K线数据
                - view.get_tran_stats(symbol, tail=N): 获取最近N根交易统计数据

        返回:
            Dict[str, float]: 交易对符号（系统格式，如 "btc-usdt"） -> 权重

        示例:
            weights = {}
            for sym in view.iter_symbols():
                bar_df = view.get_bar(sym, tail=100)  # 最近100根
                tran_df = view.get_tran_stats(sym, tail=100)
                
                # 你的计算逻辑
                # score = ...
                
                weights[sym] = score  # 或根据score计算权重
            return weights
        """
        weights: Dict[str, float] = {}

        # TODO: 实现你的因子逻辑
        # 示例：遍历所有交易对
        for sym in view.iter_symbols():
            # 获取数据
            bar_df = view.get_bar(sym, tail=100)  # 最近100根K线
            tran_df = view.get_tran_stats(sym, tail=100)  # 最近100根交易统计

            # TODO: 实现你的计算逻辑
            # 例如：
            # if bar_df.empty or tran_df.empty:
            #     continue
            # score = calculate_your_factor(bar_df, tran_df)
            # weights[sym] = score

            # 临时示例：返回空权重（请删除）
            pass

        return weights


# 创建calculator实例（系统会自动发现并加载）
# 建议命名格式：研究员名_因子名_参数
CALCULATOR_INSTANCE = TemplateCalculator(name="template_calculator")
