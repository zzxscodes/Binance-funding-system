"""
Testnet + dry-run integration tests (NO real trading).

Policy:
- These tests MUST NOT place real orders.
- These tests may call Binance Futures testnet endpoints and the dry-run order test endpoint.
- These tests run by DEFAULT if testnet keys are present (no --run-keys flag needed).

Enable:
- Set BINANCE_TESTNET_API_KEY / BINANCE_TESTNET_API_SECRET
- Tests will run automatically if keys are present, skip if not
"""

import os
import pytest

from src.execution.binance_client import BinanceClient


TESTNET_FAPI_BASE = os.getenv("BINANCE_TESTNET_FAPI_BASE", "https://testnet.binancefuture.com")


@pytest.mark.testnet
@pytest.mark.asyncio
async def test_testnet_readonly_account_and_positions():
    """Read-only: account + position endpoints on futures testnet (default run if testnet keys present)."""
    api_key = os.getenv("BINANCE_TESTNET_API_KEY", "").strip()
    api_secret = os.getenv("BINANCE_TESTNET_API_SECRET", "").strip()
    if not api_key or not api_secret:
        pytest.skip("Missing BINANCE_TESTNET_API_KEY/BINANCE_TESTNET_API_SECRET env vars (testnet keys required)")

    client = BinanceClient(api_key, api_secret, api_base=TESTNET_FAPI_BASE)
    try:
        account = await client.get_account_info()
        assert isinstance(account, dict)

        positions = await client.get_positions()
        assert isinstance(positions, list)
    finally:
        await client.close()


@pytest.mark.testnet
@pytest.mark.asyncio
async def test_testnet_dry_run_order_test_endpoint():
    """Dry-run: use /fapi/v1/order/test on futures testnet (must not create real orders, default run if testnet keys present)."""
    api_key = os.getenv("BINANCE_TESTNET_API_KEY", "").strip()
    api_secret = os.getenv("BINANCE_TESTNET_API_SECRET", "").strip()
    if not api_key or not api_secret:
        pytest.skip("Missing BINANCE_TESTNET_API_KEY/BINANCE_TESTNET_API_SECRET env vars (testnet keys required)")

    client = BinanceClient(api_key, api_secret, api_base=TESTNET_FAPI_BASE, dry_run=True)
    try:
        # Use a quantity that meets the minimum notional value (100 USDT).
        # Assuming BTC price ~50000, we need at least 0.002 BTC (100 USDT).
        # Using 0.003 BTC to be safe (150 USDT).
        # This is a *test* endpoint and should not create a real order.
        # Symbol choice: BTCUSDT is typically available on testnet.
        resp = await client.test_order(symbol="BTCUSDT", side="BUY", order_type="MARKET", quantity=0.003)
        assert isinstance(resp, dict)
    finally:
        await client.close()

