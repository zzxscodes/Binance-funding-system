"""
End-to-end integration test (NO network, NO trading).

Goal:
- Exercise the event-driven flow: DATA_COMPLETE -> strategy generates target positions -> coordinator writes execution trigger.
- Ensure no real trading APIs are called (no place_order/cancel_order).

This test uses temp directories and monkeypatching to avoid touching real data/paths.
"""

import json
from pathlib import Path
from datetime import datetime, timezone

import pytest
import pandas as pd

from src.processes.event_coordinator import EventCoordinator
from src.processes.strategy_process import StrategyProcess
from src.common.ipc import MessageType


@pytest.mark.asyncio
async def test_e2e_signal_flow_no_trade(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    # Redirect all runtime paths into tmp_path
    data_dir = tmp_path / "data"
    signals_dir = data_dir / "signals"
    positions_dir = data_dir / "positions"
    signals_dir.mkdir(parents=True, exist_ok=True)
    positions_dir.mkdir(parents=True, exist_ok=True)

    # Patch working directory expectations (modules use relative 'data/...').
    monkeypatch.chdir(tmp_path)

    # Coordinator writes strategy trigger / execution trigger files under data/signals
    coordinator = EventCoordinator()

    # Strategy process: replace alpha_engine and position_generator with deterministic fakes (no disk/network required).
    strategy = StrategyProcess()

    class FakeDataAPI:
        def get_universe(self, date=None):
            return ["BTCUSDT", "ETHUSDT"]

        def get_klines(self, symbols, days, interval_minutes=5):
            # StrategyCalculator may accept empty/None; but we don't want to depend on calc internals here.
            return {s: None for s in symbols}

        def check_data_completeness(self, symbols, days):
            return {s: {"completeness": 1.0} for s in symbols}
        
        def get_bar_between(self, begin_date_time_label, end_date_time_label):
            # Return minimal mock data to allow strategy to proceed
            import pandas as pd
            from datetime import datetime, timezone, timedelta
            # Create minimal DataFrame with required columns
            dates = pd.date_range(end=datetime.now(timezone.utc), periods=10, freq='5min')
            return {
                'btc-usdt': pd.DataFrame({
                    'symbol': 'BTCUSDT',
                    'open_time': dates,
                    'close_time': dates + pd.Timedelta(minutes=5),
                    'open': [50000.0] * 10,
                    'high': [51000.0] * 10,
                    'low': [49000.0] * 10,
                    'close': [50500.0] * 10,
                    'volume': [100.0] * 10,
                    'quote_volume': [5000000.0] * 10,
                    'trade_count': [100] * 10,
                }),
                'eth-usdt': pd.DataFrame({
                    'symbol': 'ETHUSDT',
                    'open_time': dates,
                    'close_time': dates + pd.Timedelta(minutes=5),
                    'open': [3000.0] * 10,
                    'high': [3100.0] * 10,
                    'low': [2900.0] * 10,
                    'close': [3050.0] * 10,
                    'volume': [1000.0] * 10,
                    'quote_volume': [3000000.0] * 10,
                    'trade_count': [200] * 10,
                })
            }
        
        def get_tran_stats_between(self, begin_date_time_label, end_date_time_label):
            # Return empty dict (tran_stats is optional)
            return {}

    class FakePositionGenerator:
        def build_target_positions_from_weights(self, weights, accounts, timestamp):
            # Return DataFrame format as expected by save_target_positions
            rows = [
                {"symbol": "BTCUSDT", "target_position": 0.0, "timestamp": timestamp.isoformat()},
                {"symbol": "ETHUSDT", "target_position": 0.0, "timestamp": timestamp.isoformat()},
            ]
            return {acc: pd.DataFrame(rows) for acc in accounts}

        def save_target_positions(self, target_positions, timestamp):
            # Create one file per account under data/positions and return paths
            out = {}
            for account_id, positions in target_positions.items():
                # Convert DataFrame to dict records if needed
                if isinstance(positions, pd.DataFrame):
                    positions_dict = positions.to_dict('records')
                else:
                    positions_dict = positions
                fp = positions_dir / f"{account_id}_target_positions_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
                fp.write_text(json.dumps({"account_id": account_id, "positions": positions_dict, "timestamp": timestamp.isoformat()}, indent=2), encoding="utf-8")
                out[account_id] = str(fp)
            return out

    class FakeAlphaEngine:
        def run(self, symbols=None, **kwargs):
            # Return empty weights (no trading). Shape matches AlphaResult.
            class _R:
                weights = {}
                per_calculator = {}

            return _R()

    strategy.data_api = FakeDataAPI()
    strategy.position_generator = FakePositionGenerator()
    strategy.alpha_engine = FakeAlphaEngine()

    # Emulate a DATA_COMPLETE message arriving at coordinator
    ts = datetime.now(timezone.utc)
    await coordinator._handle_data_complete(
        {"type": MessageType.DATA_COMPLETE.value, "timestamp": ts.isoformat(), "symbols": ["BTCUSDT", "ETHUSDT"]}
    )

    # Coordinator should have written strategy trigger
    trigger_fp = signals_dir / "strategy_trigger.json"
    assert trigger_fp.exists()

    # Strategy should react to trigger file in waiting mode; run one iteration by calling private wait+execute
    trigger_data = await strategy._wait_for_trigger(timeout=1.0)
    assert trigger_data is not None
    assert trigger_data.get("event") == "data_complete"

    # Execute strategy with provided symbols; it should write target position files
    await strategy._execute_strategy(trigger_data.get("symbols", []))

    # After strategy run, we expect files in data/positions for account1/2/3 per default config
    created = list(positions_dir.glob("*_target_positions_*.json"))
    assert len(created) >= 1

