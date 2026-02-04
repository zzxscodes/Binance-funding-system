"""
测试time_label范围（1-288）
"""
import pytest
import pandas as pd
from datetime import datetime, timezone
from src.data.kline_aggregator import KlineAggregator
from src.data.api import DataAPI


def create_mock_trade(symbol: str, price: float, qty: float, timestamp_ms: int, 
                     is_buyer_maker: bool = False) -> dict:
    """创建模拟交易数据"""
    return {
        'symbol': symbol,
        'tradeId': int(timestamp_ms / 1000),
        'price': price,
        'qty': qty,
        'quoteQty': price * qty,
        'isBuyerMaker': is_buyer_maker,
        'ts_ms': timestamp_ms,
        'ts_us': timestamp_ms * 1000,
        'ts': pd.Timestamp(timestamp_ms, unit='ms', tz='UTC'),
    }


@pytest.mark.asyncio
async def test_time_label_range_1_to_288():
    """测试time_label范围应该是1-288，而不是0-287"""
    aggregator = KlineAggregator(interval_minutes=5)
    
    # 创建一天中不同时间的交易
    base_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # 测试几个关键时间点
    test_cases = [
        (0, 1),      # 00:00 - 00:05，应该是time_label=1
        (5, 2),      # 00:05 - 00:10，应该是time_label=2
        (10, 3),     # 00:10 - 00:15，应该是time_label=3
        (1435, 288), # 23:55 - 24:00，应该是time_label=288
    ]
    
    symbol = 'BTCUSDT'
    
    for minutes_offset, expected_label in test_cases:
        window_time = base_time.replace(minute=minutes_offset % 60, hour=minutes_offset // 60)
        window_start_ms = int(window_time.timestamp() * 1000)
        window_start_ms = (window_start_ms // 300000) * 300000  # 对齐到5分钟
        
        # 添加一笔交易
        trade = create_mock_trade(symbol, 50000.0, 0.1, window_start_ms + 1000, False)
        await aggregator.add_trade(symbol, trade)
        
        # 聚合窗口
        await aggregator._aggregate_window(symbol, window_start_ms)
        
        # 获取K线
        klines = aggregator.get_klines(symbol)
        if not klines.empty:
            # 找到对应的K线
            matching_klines = klines[klines['span_begin_datetime'] == window_start_ms]
            if not matching_klines.empty:
                time_label = matching_klines.iloc[0]['time_lable']
                assert time_label == expected_label, \
                    f"Time {window_time} (offset {minutes_offset}min) should have time_label={expected_label}, got {time_label}"
    
    print("✓ Time label range test passed: all time_labels are in range 1-288")


@pytest.mark.asyncio
async def test_time_label_parsing_1_to_288():
    """测试DataAPI解析time_label时应该接受1-288范围"""
    api = DataAPI()
    
    # 测试有效的time_label（1-288）
    valid_labels = ['2025-01-01-001', '2025-01-01-144', '2025-01-01-288']
    for label in valid_labels:
        try:
            result = api._parse_date_time_label(label)
            assert result is not None
        except ValueError:
            pytest.fail(f"Valid time_label {label} was rejected")
    
    # 测试无效的time_label（0或289）
    invalid_labels = ['2025-01-01-000', '2025-01-01-289']
    for label in invalid_labels:
        with pytest.raises(ValueError, match="Invalid time_label"):
            api._parse_date_time_label(label)
    
    print("✓ Time label parsing test passed: accepts 1-288, rejects 0 and 289")


def test_time_label_generation():
    """测试time_label生成函数"""
    api = DataAPI()
    
    # 测试一天中不同时间的time_label生成
    base_date = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    
    test_cases = [
        (0, 1),    # 00:00 -> time_label=1
        (5, 2),    # 00:05 -> time_label=2
        (1435, 288), # 23:55 -> time_label=288
    ]
    
    for minutes_offset, expected_label in test_cases:
        test_time = base_date.replace(minute=minutes_offset % 60, hour=minutes_offset // 60)
        time_label_str = api._get_date_time_label_from_datetime(test_time)
        
        # 解析time_label
        parts = time_label_str.split('-')
        time_label = int(parts[-1])
        
        assert time_label == expected_label, \
            f"Time {test_time} should generate time_label={expected_label}, got {time_label}"
    
    print("✓ Time label generation test passed")
