"""
Tests for alpha calculators with bar/tran_stats shaped like DataAPI output.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.strategy.calculator import AlphaDataView, MeanBuyDolvol4OverDolvolRankCalculator


def test_alpha_calculator_with_bar_and_tran_stats_data():
    symbol = "btc-usdt"
    dates = pd.date_range(start="2025-12-23", periods=100, freq="5min", tz="UTC")

    bar_df = pd.DataFrame(
        {
            "symbol": [symbol] * 100,
            "open_time": dates,
            "dolvol": [100.0] * 100,
        }
    )
    tran_df = pd.DataFrame(
        {
            "symbol": [symbol] * 100,
            "open_time": dates,
            "buy_dolvol4": [10.0] * 100,
        }
    )

    view = AlphaDataView(bar_data={symbol: bar_df}, tran_stats={symbol: tran_df}, symbols={symbol}, copy_on_read=False)
    calc = MeanBuyDolvol4OverDolvolRankCalculator(lookback_bars=50)
    weights = calc.run(view)

    # With only one symbol, calculator returns {} (needs at least 2 symbols to long/short split).
    assert weights == {}


@pytest.mark.network
def test_alpha_calculator_with_real_api_smoke():
    """
    Optional smoke test: try to fetch real snapshot and run alpha calculator.
    Skips if no data.
    """
    from src.strategy.alpha import AlphaEngine
    from src.data.api import DataAPI

    api = DataAPI()
    universe = api.get_universe()
    if not universe:
        pytest.skip("No universe available")

    engine = AlphaEngine(data_api=api, calculators=[MeanBuyDolvol4OverDolvolRankCalculator(lookback_bars=300)])
    result = engine.run(symbols=list(universe)[:20])
    # Not asserting non-empty because environments may have no historical data
    assert isinstance(result.weights, dict)
