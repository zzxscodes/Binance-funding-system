"""
Tests for the NEW alpha-calculator architecture.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timezone

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.strategy.calculator import AlphaDataView, MeanBuyDolvol4OverDolvolRankCalculator


def _mk_bar(symbol: str, dolvol: float, n: int = 10) -> pd.DataFrame:
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    times = pd.date_range(start=t0, periods=n, freq="5min")
    return pd.DataFrame(
        {
            "symbol": symbol,
            "open_time": times,
            "dolvol": [dolvol] * n,
        }
    )


def _mk_tran(symbol: str, buy_dolvol4: float, n: int = 10) -> pd.DataFrame:
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    times = pd.date_range(start=t0, periods=n, freq="5min")
    return pd.DataFrame(
        {
            "symbol": symbol,
            "open_time": times,
            "buy_dolvol4": [buy_dolvol4] * n,
        }
    )


def test_mean_buy_dolvol4_over_dolvol_rank_calculator_outputs_long_short_weights():
    # Four symbols, distinct ratios:
    # A: 4/1=4, B: 3, C: 2, D: 1
    bar_data = {
        "a-usdt": _mk_bar("A", dolvol=1.0),
        "b-usdt": _mk_bar("B", dolvol=1.0),
        "c-usdt": _mk_bar("C", dolvol=1.0),
        "d-usdt": _mk_bar("D", dolvol=1.0),
    }
    tran_stats = {
        "a-usdt": _mk_tran("A", buy_dolvol4=4.0),
        "b-usdt": _mk_tran("B", buy_dolvol4=3.0),
        "c-usdt": _mk_tran("C", buy_dolvol4=2.0),
        "d-usdt": _mk_tran("D", buy_dolvol4=1.0),
    }

    view = AlphaDataView(bar_data=bar_data, tran_stats=tran_stats, symbols=None, copy_on_read=False)
    calc = MeanBuyDolvol4OverDolvolRankCalculator(lookback_bars=10, name="calc")
    weights = calc.run(view)

    # Top half (2): a, b -> +0.5 each
    # Bottom half (2): c, d -> -0.5 each
    assert weights["a-usdt"] == 0.5
    assert weights["b-usdt"] == 0.5
    assert weights["c-usdt"] == -0.5
    assert weights["d-usdt"] == -0.5


def test_alpha_data_view_copy_on_read_isolation():
    bar = _mk_bar("A", dolvol=1.0, n=3)
    tran = _mk_tran("A", buy_dolvol4=3.0, n=3)
    bar_data = {"a-usdt": bar}
    tran_stats = {"a-usdt": tran}

    base = AlphaDataView(bar_data=bar_data, tran_stats=tran_stats, symbols=None, copy_on_read=False)
    mut_view = base.with_copy_on_read(True)

    df_mut = mut_view.get_bar("a-usdt")
    df_mut.loc[:, "dolvol"] = 999.0

    # Base view should still see original data (shared dict value unchanged)
    df_base = base.get_bar("a-usdt")
    assert float(df_base["dolvol"].iloc[0]) == 1.0

