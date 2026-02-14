"""
示例Calculator：买入金额占比排名因子

因子逻辑：
- 使用最近N根5分钟K线
- score(交易对) = mean( tran_stats.buy_dolvol4 / bar.dolvol )
- 按分数排序，前一半做多，后一半做空

研究员可以复制此文件作为开发模板。
"""

from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from ..calculator import AlphaCalculatorBase, AlphaDataView

# 导出calculator实例（推荐方式）
CALCULATOR_INSTANCE = None


class MeanBuyDolvol4OverDolvolRankCalculator5(AlphaCalculatorBase):
    """
    买入金额占比排名因子。

    使用最近N根5分钟K线计算买入金额占比的均值，然后按排名分配权重。
    """

    def __init__(self, lookback_bars: int = 1000, name: Optional[str] = None):
        self.lookback_bars = int(lookback_bars)
        self.name = name or f"mean_buy_dolvol4_over_dolvol_rank_5_{self.lookback_bars}"
        self.mutates_inputs = False  # 不修改输入数据，性能更好

    @staticmethod
    def _score_symbol(bar_df: pd.DataFrame, tran_df: pd.DataFrame) -> Optional[float]:
        """计算单个交易对的分数。"""
        if bar_df.empty or tran_df.empty:
            return None
        if "open_time" not in bar_df.columns or "open_time" not in tran_df.columns:
            return None
        if "dolvol" not in bar_df.columns or "buy_dolvol4" not in tran_df.columns:
            return None

        # 如果已排序则避免排序成本
        if not bar_df["open_time"].is_monotonic_increasing:
            bar_df = bar_df.sort_values("open_time")
        if not tran_df["open_time"].is_monotonic_increasing:
            tran_df = tran_df.sort_values("open_time")

        bar_s = bar_df.set_index("open_time")["dolvol"]
        tran_s = tran_df.set_index("open_time")["buy_dolvol4"]

        idx = bar_s.index.intersection(tran_s.index)
        if len(idx) == 0:
            return None

        bar_v = pd.to_numeric(bar_s.loc[idx], errors="coerce")
        tran_v = pd.to_numeric(tran_s.loc[idx], errors="coerce")

        valid = (bar_v > 0) & bar_v.notna() & tran_v.notna()
        if int(valid.sum()) == 0:
            return None

        ratio = (tran_v[valid] / bar_v[valid]).astype(float)
        if ratio.empty:
            return None
        score = float(ratio.mean())
        if not (score == score):  # NaN
            return None
        return score

    def run(self, view: AlphaDataView) -> Dict[str, float]:
        """
        运行计算器，返回每个交易对的权重。

        参数:
            view: 数据视图，包含bar和tran_stats数据

        返回:
            Dict[str, float]: 交易对符号 -> 权重
        """
        scores: Dict[str, float] = {}

        # 遍历所有交易对
        for sym in view.iter_symbols():
            bar_df = view.get_bar(sym, tail=self.lookback_bars)
            tran_df = view.get_tran_stats(sym, tail=self.lookback_bars)
            sc = self._score_symbol(bar_df, tran_df)
            if sc is None:
                continue
            scores[sym] = sc

        if len(scores) < 2:
            return {}

        # 按分数排序
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        m = len(ranked)
        half = m // 2
        if half == 0:
            return {}

        # 前一半做多，后一半做空
        longs = [s for s, _ in ranked[:half]]
        shorts = [s for s, _ in ranked[half:]]

        w_long = 1.0 / max(1, len(longs))
        w_short = -1.0 / max(1, len(shorts))

        out: Dict[str, float] = {}
        for s in longs:
            out[s] = out.get(s, 0.0) + w_long
        for s in shorts:
            out[s] = out.get(s, 0.0) + w_short
        return out


# 创建默认实例（系统会自动发现）
CALCULATOR_INSTANCE = MeanBuyDolvol4OverDolvolRankCalculator5(lookback_bars=1000, name="mean_buy_dolvol4_over_dolvol_rank_5_1000")
