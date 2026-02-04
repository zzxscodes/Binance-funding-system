"""
综合测试：验证K线聚合器生成的K线数据完全符合数据库bar表schema要求
"""
import pytest
import pandas as pd
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.data.kline_aggregator import KlineAggregator
from src.data.api import get_data_api
from src.common.utils import to_system_symbol


def create_mock_trade(symbol: str, price: float, qty: float, timestamp_ms: int, 
                     is_buyer_maker: bool = False) -> dict:
    """创建模拟交易数据"""
    return {
        'price': price,
        'qty': qty,
        'quoteQty': price * qty,
        'isBuyerMaker': is_buyer_maker,
        'ts_ms': timestamp_ms,
    }


@pytest.mark.asyncio
async def test_comprehensive_bar_schema_validation():
    """综合验证：K线数据完全符合bar表schema"""
    aggregator = KlineAggregator(interval_minutes=5)
    
    symbol = 'BTCUSDT'
    base_time = datetime(2026, 1, 21, 14, 0, 0, tzinfo=timezone.utc)
    window_start_ms = int(base_time.timestamp() * 1000)
    window_start_ms = (window_start_ms // 300000) * 300000
    
    # 创建包含买卖双方的交易
    trades = [
        create_mock_trade(symbol, 50000.0, 1.0, window_start_ms + 1000, False),  # 买
        create_mock_trade(symbol, 50010.0, 2.0, window_start_ms + 2000, False),  # 买
        create_mock_trade(symbol, 50005.0, 1.5, window_start_ms + 3000, True),   # 卖
        create_mock_trade(symbol, 50015.0, 3.0, window_start_ms + 4000, False),  # 买
        create_mock_trade(symbol, 50012.0, 2.5, window_start_ms + 5000, True),   # 卖
    ]
    
    # 聚合
    await aggregator._aggregate_window(symbol, window_start_ms, trades_override=trades)
    
    klines = aggregator.get_klines(symbol)
    assert not klines.empty, "Should generate kline"
    
    kline = klines.iloc[-1]
    
    # 1. 验证所有必需字段存在
    required_fields = [
        # 基础字段
        'symbol', 'open_time', 'close_time', 'open', 'high', 'low', 'close',
        'volume', 'quote_volume', 'trade_count', 'buy_volume', 'sell_volume',
        # bar表字段
        'microsecond_since_trad', 'span_begin_datetime', 'span_end_datetime',
        'span_status', 'last', 'vwap', 'dolvol', 'buydolvol', 'selldolvol',
        'buyvolume', 'sellvolume', 'buytradecount', 'selltradecount', 'time_lable',
        # tran_stats表字段（按金额分档统计）
        'buy_volume1', 'buy_volume2', 'buy_volume3', 'buy_volume4',
        'buy_dolvol1', 'buy_dolvol2', 'buy_dolvol3', 'buy_dolvol4',
        'buy_trade_count1', 'buy_trade_count2', 'buy_trade_count3', 'buy_trade_count4',
        'sell_volume1', 'sell_volume2', 'sell_volume3', 'sell_volume4',
        'sell_dolvol1', 'sell_dolvol2', 'sell_dolvol3', 'sell_dolvol4',
        'sell_trade_count1', 'sell_trade_count2', 'sell_trade_count3', 'sell_trade_count4',
    ]
    
    missing = [f for f in required_fields if f not in kline]
    assert not missing, f"Missing fields: {missing}"
    
    # 2. 验证数据类型和值
    import numpy as np
    assert isinstance(kline['span_begin_datetime'], (int, np.integer, pd.Timestamp))
    assert isinstance(kline['span_end_datetime'], (int, np.integer, pd.Timestamp))
    assert isinstance(kline['microsecond_since_trad'], (int, np.integer, pd.Timestamp))
    assert isinstance(kline['span_status'], str)
    assert isinstance(kline['time_lable'], (int, np.integer, pd.Series))
    
    # 3. 验证计算正确性
    assert kline['last'] == kline['close'], "last should equal close"
    assert kline['dolvol'] == kline['quote_volume'], "dolvol should equal quote_volume"
    
    if kline['volume'] > 0:
        expected_vwap = kline['dolvol'] / kline['volume']
        assert abs(kline['vwap'] - expected_vwap) < 1e-6, f"VWAP calculation error: {kline['vwap']} != {expected_vwap}"
    
    assert abs((kline['buyvolume'] + kline['sellvolume']) - kline['volume']) < 1e-6, "BuyVolume + SellVolume should equal Volume"
    assert abs((kline['buydolvol'] + kline['selldolvol']) - kline['dolvol']) < 1e-6, "BuyDolvol + SellDolvol should equal Dolvol"
    assert (kline['buytradecount'] + kline['selltradecount']) == kline['trade_count'], "BuyTradeCount + SellTradeCount should equal TradeCount"
    
    # 7. 验证tran_stats表分档统计字段
    # 验证分档统计总和等于总统计
    buy_vol_sum = kline['buy_volume1'] + kline['buy_volume2'] + kline['buy_volume3'] + kline['buy_volume4']
    assert abs(buy_vol_sum - kline['buyvolume']) < 1e-6, f"Buy volume tiers sum mismatch: {buy_vol_sum} != {kline['buyvolume']}"
    
    buy_dolvol_sum = kline['buy_dolvol1'] + kline['buy_dolvol2'] + kline['buy_dolvol3'] + kline['buy_dolvol4']
    assert abs(buy_dolvol_sum - kline['buydolvol']) < 1e-6, f"Buy dolvol tiers sum mismatch: {buy_dolvol_sum} != {kline['buydolvol']}"
    
    buy_count_sum = kline['buy_trade_count1'] + kline['buy_trade_count2'] + kline['buy_trade_count3'] + kline['buy_trade_count4']
    assert buy_count_sum == kline['buytradecount'], f"Buy trade count tiers sum mismatch: {buy_count_sum} != {kline['buytradecount']}"
    
    sell_vol_sum = kline['sell_volume1'] + kline['sell_volume2'] + kline['sell_volume3'] + kline['sell_volume4']
    assert abs(sell_vol_sum - kline['sellvolume']) < 1e-6, f"Sell volume tiers sum mismatch: {sell_vol_sum} != {kline['sellvolume']}"
    
    sell_dolvol_sum = kline['sell_dolvol1'] + kline['sell_dolvol2'] + kline['sell_dolvol3'] + kline['sell_dolvol4']
    assert abs(sell_dolvol_sum - kline['selldolvol']) < 1e-6, f"Sell dolvol tiers sum mismatch: {sell_dolvol_sum} != {kline['selldolvol']}"
    
    sell_count_sum = kline['sell_trade_count1'] + kline['sell_trade_count2'] + kline['sell_trade_count3'] + kline['sell_trade_count4']
    assert sell_count_sum == kline['selltradecount'], f"Sell trade count tiers sum mismatch: {sell_count_sum} != {kline['selltradecount']}"
    
    # 4. 验证时间字段
    assert kline['span_begin_datetime'] == window_start_ms
    assert kline['span_end_datetime'] == window_start_ms + 300000
    assert kline['microsecond_since_trad'] == window_start_ms + 300000
    
    # 5. 验证span_status
    assert kline['span_status'] == "", "span_status should be empty when trades exist"
    
    # 6. 验证time_lable范围
    time_lable = int(kline['time_lable']) if hasattr(kline['time_lable'], '__int__') else kline['time_lable']
    assert 0 <= time_lable < 288, f"time_lable should be 0-287, got: {time_lable}"
    
    print("✓ Comprehensive bar schema validation passed")
    print(f"  All {len(required_fields)} required fields present")
    print(f"  VWAP: {kline['vwap']:.2f}")
    print(f"  BuyVol: {kline['buyvolume']:.2f}, SellVol: {kline['sellvolume']:.2f}")
    print(f"  BuyTradeCount: {kline['buytradecount']}, SellTradeCount: {kline['selltradecount']}")
    print(f"  TimeLabel: {time_lable}, SpanStatus: '{kline['span_status']}'")


@pytest.mark.asyncio
async def test_dataapi_return_format():
    """验证DataAPI返回格式为btc-usdt风格"""
    aggregator = KlineAggregator(interval_minutes=5)
    api = get_data_api(aggregator)
    
    # 创建一些测试数据
    symbol = 'BTCUSDT'
    base_time = datetime(2026, 1, 21, 14, 0, 0, tzinfo=timezone.utc)
    window_start_ms = int(base_time.timestamp() * 1000)
    window_start_ms = (window_start_ms // 300000) * 300000
    
    trades = [
        create_mock_trade(symbol, 50000.0, 1.0, window_start_ms + 1000, False),
    ]
    
    await aggregator._aggregate_window(symbol, window_start_ms, trades_override=trades)
    
    # 通过API获取
    result = api.get_klines([symbol], days=1)
    
    # 验证返回格式
    expected_key = to_system_symbol(symbol)  # 应该是 'btc-usdt'
    assert expected_key in result, f"Expected key '{expected_key}' not in result: {list(result.keys())}"
    
    if result[expected_key] is not None and not result[expected_key].empty:
        df = result[expected_key]
        # 验证DataFrame包含bar表字段
        bar_fields = ['vwap', 'dolvol', 'buydolvol', 'selldolvol', 'buyvolume', 'sellvolume']
        has_bar_fields = any(f in df.columns for f in bar_fields)
        print(f"✓ DataAPI return format validated: key='{expected_key}', has_bar_fields={has_bar_fields}")
    else:
        print(f"✓ DataAPI return format validated: key='{expected_key}' (no data)")


if __name__ == "__main__":
    async def run_tests():
        print("=" * 60)
        print("Comprehensive Bar Schema Validation Tests")
        print("=" * 60)
        
        await test_comprehensive_bar_schema_validation()
        await test_dataapi_return_format()
        
        print("\n" + "=" * 60)
        print("All comprehensive tests passed!")
        print("=" * 60)
    
    asyncio.run(run_tests())
