"""
测试无成交K线生成
"""
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from src.data.kline_aggregator import KlineAggregator


@pytest.mark.asyncio
async def test_no_trade_kline_generation():
    """测试无成交时也生成K线（ohlc=上一K线close，vwap=nan）"""
    aggregator = KlineAggregator(interval_minutes=5)
    
    symbol = 'BTCUSDT'
    base_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # 第一个窗口：有交易
    window1_start_ms = int(base_time.timestamp() * 1000)
    window1_start_ms = (window1_start_ms // 300000) * 300000
    
    trade1 = {
        'symbol': symbol,
        'tradeId': 1,
        'price': 50000.0,
        'qty': 0.1,
        'quoteQty': 5000.0,
        'isBuyerMaker': False,
        'ts_ms': window1_start_ms + 1000,
        'ts_us': (window1_start_ms + 1000) * 1000,
        'ts': pd.Timestamp(window1_start_ms + 1000, unit='ms', tz='UTC'),
    }
    
    await aggregator.add_trade(symbol, trade1)
    await aggregator._aggregate_window(symbol, window1_start_ms)
    
    # 获取第一个K线
    klines = aggregator.get_klines(symbol)
    assert not klines.empty, "First window should have kline"
    
    first_kline = klines.iloc[-1]
    first_close = first_kline['close']
    
    # 第二个窗口：无交易
    window2_start_ms = window1_start_ms + 300000  # 下一个5分钟窗口
    
    # 直接调用聚合窗口，但不添加交易
    await aggregator._aggregate_window(symbol, window2_start_ms, trades_override=[])
    
    # 获取K线
    klines = aggregator.get_klines(symbol)
    assert len(klines) >= 2, "Should have at least 2 klines"
    
    second_kline = klines.iloc[-1]
    
    # 验证无交易K线的属性
    assert second_kline['open'] == first_close, "Open should equal previous close"
    assert second_kline['high'] == first_close, "High should equal previous close"
    assert second_kline['low'] == first_close, "Low should equal previous close"
    assert second_kline['close'] == first_close, "Close should equal previous close"
    assert second_kline['volume'] == 0.0, "Volume should be 0"
    assert second_kline['quote_volume'] == 0.0, "Quote volume should be 0"
    assert second_kline['trade_count'] == 0, "Trade count should be 0"
    
    # VWAP应该是NaN（因为volume为0）
    import math
    assert math.isnan(second_kline['vwap']) or pd.isna(second_kline['vwap']), \
        "VWAP should be NaN when volume is 0"
    
    # 所有分档统计应该为0
    assert second_kline.get('buy_volume1', 0) == 0
    assert second_kline.get('buy_dolvol1', 0) == 0
    assert second_kline.get('sell_volume1', 0) == 0
    assert second_kline.get('sell_dolvol1', 0) == 0
    
    print("✓ No trade kline generation test passed:")
    print(f"  First kline close: {first_close}")
    print(f"  Second kline (no trade): OHLC={second_kline['open']}, volume={second_kline['volume']}, vwap={second_kline['vwap']}")


@pytest.mark.asyncio
async def test_no_trade_kline_with_no_previous_kline():
    """测试没有前一个K线时，无交易K线的处理"""
    aggregator = KlineAggregator(interval_minutes=5)
    
    symbol = 'BTCUSDT'
    base_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    window_start_ms = int(base_time.timestamp() * 1000)
    window_start_ms = (window_start_ms // 300000) * 300000
    
    # 直接生成无交易K线（没有前一个K线）
    await aggregator._aggregate_window(symbol, window_start_ms, trades_override=[])
    
    klines = aggregator.get_klines(symbol)
    
    if not klines.empty:
        kline = klines.iloc[-1]
        # 如果没有前一个K线，close应该是0或默认值
        assert kline['volume'] == 0.0
        assert kline['trade_count'] == 0
        assert pd.isna(kline['vwap']) or kline['vwap'] == 0.0
    
    print("✓ No trade kline with no previous kline test passed")


@pytest.mark.asyncio
async def test_generate_no_trade_klines_method():
    """测试generate_no_trade_klines方法"""
    aggregator = KlineAggregator(interval_minutes=5)
    
    symbol = 'BTCUSDT'
    base_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # 先创建一个有交易的K线
    window1_start_ms = int(base_time.timestamp() * 1000)
    window1_start_ms = (window1_start_ms // 300000) * 300000
    
    trade = {
        'symbol': symbol,
        'tradeId': 1,
        'price': 50000.0,
        'qty': 0.1,
        'quoteQty': 5000.0,
        'isBuyerMaker': False,
        'ts_ms': window1_start_ms + 1000,
        'ts_us': (window1_start_ms + 1000) * 1000,
        'ts': pd.Timestamp(window1_start_ms + 1000, unit='ms', tz='UTC'),
    }
    
    await aggregator.add_trade(symbol, trade)
    await aggregator._aggregate_window(symbol, window1_start_ms)
    
    # 调用generate_no_trade_klines
    await aggregator.check_and_generate_empty_windows([symbol])
    
    klines = aggregator.get_klines(symbol)
    
    # 应该至少有一个K线
    assert not klines.empty, "Should have at least one kline"
    
    print("✓ Generate no trade klines method test passed")
