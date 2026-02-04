"""
Tests for multi-calculator behavior (NEW architecture).
"""

import pandas as pd
from datetime import datetime, timezone

from src.strategy.calculator import AlphaDataView, MeanBuyDolvol4OverDolvolRankCalculator


def _mk_series(symbol: str, dolvol: float, buy_dolvol4: float, n: int = 20):
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    times = pd.date_range(start=t0, periods=n, freq="5min")
    bar = pd.DataFrame({"symbol": symbol, "open_time": times, "dolvol": [dolvol] * n})
    tran = pd.DataFrame({"symbol": symbol, "open_time": times, "buy_dolvol4": [buy_dolvol4] * n})
    return bar, tran


def test_two_calculators_sum_weights_correctly():
    symbols = ["a-usdt", "b-usdt", "c-usdt", "d-usdt", "e-usdt", "f-usdt"]

    bar_data = {}
    tran_stats = {}
    # ratios: a=6, b=5, c=4, d=3, e=2, f=1
    for i, s in enumerate(symbols):
        ratio = float(len(symbols) - i)
        bar, tran = _mk_series(s, dolvol=1.0, buy_dolvol4=ratio)
        bar_data[s] = bar
        tran_stats[s] = tran

    view = AlphaDataView(bar_data=bar_data, tran_stats=tran_stats, symbols=set(symbols), copy_on_read=False)
    c1 = MeanBuyDolvol4OverDolvolRankCalculator(lookback_bars=20, name="c1")
    c2 = MeanBuyDolvol4OverDolvolRankCalculator(lookback_bars=5, name="c2")

    w1 = c1.run(view)
    w2 = c2.run(view)
    summed = {}
    for d in (w1, w2):
        for k, v in d.items():
            summed[k] = summed.get(k, 0.0) + v

    # For 6 symbols: top3 long, bottom3 short, each calc contributes +/- 1/3
    assert summed["a-usdt"] == 2.0 / 3.0
    assert summed["b-usdt"] == 2.0 / 3.0
    assert summed["c-usdt"] == 2.0 / 3.0
    assert summed["d-usdt"] == -2.0 / 3.0
    assert summed["e-usdt"] == -2.0 / 3.0
    assert summed["f-usdt"] == -2.0 / 3.0
