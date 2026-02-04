"""
Pytest configuration for dual-mode testing (network is required by default):

1) Default mode (no-keys):
   - Network tests (Binance REST/WS) RUN by default.
   - Tests requiring real account API keys are skipped by default.

2) Keys mode (explicitly enabled):
   - Network tests run (same as default).
   - Account-key tests enabled via a flag/env + required key envs.

Flags / env:
  - --run-keys                     enable tests that require real account API keys
  - BINANCE_RUN_KEY_TESTS=1        (alternative) enable key tests

Required key envs (current integration tests assume account1):
  - ACCOUNT1_API_KEY
  - ACCOUNT1_API_SECRET
"""

from __future__ import annotations

import os
import pytest


def _env_truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "y", "on"}

def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-keys",
        action="store_true",
        default=False,
        help="Run tests that require real Binance account API keys",
    )


def pytest_configure(config: pytest.Config) -> None:
    # Document custom markers (helps `pytest --strict-markers` if enabled later).
    config.addinivalue_line("markers", "network: tests that require outbound network access to Binance")
    config.addinivalue_line("markers", "keys: tests that require real Binance account API keys")
    config.addinivalue_line("markers", "testnet: tests that target Binance Futures testnet endpoints (no real trading)")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    run_keys = bool(config.getoption("--run-keys")) or _env_truthy("BINANCE_RUN_KEY_TESTS")

    # Detect whether account1 keys exist (ExecutionProcess('account1') hard-requires these).
    # You can extend this later for account2/account3 when needed.
    has_account1_key = bool(os.getenv("ACCOUNT1_API_KEY") or os.getenv("ACCOUNT1_API_SECRET"))
    has_account1_secret = bool(os.getenv("ACCOUNT1_API_SECRET") or os.getenv("ACCOUNT1_API_KEY"))
    has_account1_keys = has_account1_key and has_account1_secret

    for item in items:
        nodeid = item.nodeid.replace("\\", "/")

        # --- Network-dependent tests (Binance REST/WS) ---
        is_network = (
            "tests/integration/test_binance_connection.py" in nodeid
            or "tests/test_data/test_universe_manager.py" in nodeid
        )
        if is_network:
            item.add_marker(pytest.mark.network)

        # --- Key-dependent tests (real account keys, read-only) ---
        # Policy: key tests must NOT place orders / modify account state (no testnet, no dry-run).
        # Gating is primarily marker-based: any test marked with @pytest.mark.keys is gated here.
        explicit_keys_marker = item.get_closest_marker("keys") is not None

        # Backward-compatible auto-detection for legacy key tests.
        legacy_keys = (
            "tests/integration/test_processes.py::test_execution_process_initialization" in nodeid
            or "tests/integration/test_processes.py::test_execution_process_wait_for_trigger" in nodeid
            or "tests/integration/test_processes.py::test_process_stop_methods" in nodeid
        )

        if explicit_keys_marker or legacy_keys:
            item.add_marker(pytest.mark.keys)
            if not run_keys:
                item.add_marker(
                    pytest.mark.skip(
                        reason="Skipped: requires real account keys (use --run-keys or set BINANCE_RUN_KEY_TESTS=1)"
                    )
                )
            elif not has_account1_keys:
                item.add_marker(pytest.mark.skip(reason="Skipped: missing ACCOUNT1_API_KEY/ACCOUNT1_API_SECRET env vars"))

