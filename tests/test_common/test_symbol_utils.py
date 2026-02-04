import pytest

from src.common.utils import to_exchange_symbol, to_system_symbol


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("BTCUSDT", "btc-usdt"),
        ("btcusdt", "btc-usdt"),
        ("BTC-USDT", "btc-usdt"),
        ("btc-usdt", "btc-usdt"),
        ("ETHUSDT", "eth-usdt"),
        ("eth/usdt", "eth-usdt"),
        ("eth_usdt", "eth-usdt"),
    ],
)
def test_to_system_symbol(inp: str, expected: str) -> None:
    assert to_system_symbol(inp) == expected


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("btc-usdt", "BTCUSDT"),
        ("BTC-USDT", "BTCUSDT"),
        ("BTCUSDT", "BTCUSDT"),
        ("eth-usdt", "ETHUSDT"),
        ("eth/usdt", "ETHUSDT"),
        ("eth_usdt", "ETHUSDT"),
    ],
)
def test_to_exchange_symbol(inp: str, expected: str) -> None:
    assert to_exchange_symbol(inp) == expected


def test_to_system_symbol_unknown_fallback() -> None:
    # Unknown formats should not raise; they downgrade to lowercase string.
    assert to_system_symbol("NONEXISTENT") == "nonexistent"

