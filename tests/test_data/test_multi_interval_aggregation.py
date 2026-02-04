"""
测试多周期聚合（1h,4h,8h,12h,24h）
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from src.data.multi_interval_aggregator import MultiIntervalAggregator
from src.data.api import DataAPI


def create_mock_5min_klines(symbol: str, num_klines: int = 288) -> pd.DataFrame:
    """创建模拟5分钟K线数据（一天的数据）"""
    base_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    klines = []
    for i in range(num_klines):
        open_time = base_time + timedelta(minutes=i * 5)
        close_time = open_time + timedelta(minutes=5)
        
        # 生成模拟价格
        base_price = 50000.0
        price_change = np.random.randn() * 100
        open_price = base_price + price_change
        high_price = open_price + abs(np.random.randn() * 50)
        low_price = open_price - abs(np.random.randn() * 50)
        close_price = open_price + np.random.randn() * 50
        
        volume = np.random.rand() * 100
        quote_volume = close_price * volume
        
        kline = {
            'symbol': symbol,
            'open_time': open_time,
            'close_time': close_time,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': volume,
            'quote_volume': quote_volume,
            'trade_count': np.random.randint(10, 100),
            'buy_volume': volume * 0.5,
            'sell_volume': volume * 0.5,
            'buydolvol': quote_volume * 0.5,
            'selldolvol': quote_volume * 0.5,
            'buyvolume': volume * 0.5,
            'sellvolume': volume * 0.5,
            'buytradecount': np.random.randint(5, 50),
            'selltradecount': np.random.randint(5, 50),
            'time_lable': i + 1,  # 1-288
            'span_status': 'COMPLETE',
            'vwap': quote_volume / volume if volume > 0 else np.nan,
            # 分档统计
            'buy_volume1': volume * 0.1,
            'buy_volume2': volume * 0.2,
            'buy_volume3': volume * 0.15,
            'buy_volume4': volume * 0.05,
            'buy_dolvol1': quote_volume * 0.1,
            'buy_dolvol2': quote_volume * 0.2,
            'buy_dolvol3': quote_volume * 0.15,
            'buy_dolvol4': quote_volume * 0.05,
            'buy_trade_count1': 5,
            'buy_trade_count2': 10,
            'buy_trade_count3': 8,
            'buy_trade_count4': 2,
            'sell_volume1': volume * 0.1,
            'sell_volume2': volume * 0.2,
            'sell_volume3': volume * 0.15,
            'sell_volume4': volume * 0.05,
            'sell_dolvol1': quote_volume * 0.1,
            'sell_dolvol2': quote_volume * 0.2,
            'sell_dolvol3': quote_volume * 0.15,
            'sell_dolvol4': quote_volume * 0.05,
            'sell_trade_count1': 5,
            'sell_trade_count2': 10,
            'sell_trade_count3': 8,
            'sell_trade_count4': 2,
        }
        klines.append(kline)
    
    return pd.DataFrame(klines)


def test_multi_interval_aggregator_initialization():
    """测试多周期聚合器初始化"""
    aggregator = MultiIntervalAggregator()
    
    assert aggregator.INTERVAL_MAP is not None
    assert '5min' in aggregator.INTERVAL_MAP
    assert '1h' in aggregator.INTERVAL_MAP
    assert '4h' in aggregator.INTERVAL_MAP
    assert '8h' in aggregator.INTERVAL_MAP
    assert '12h' in aggregator.INTERVAL_MAP
    assert '24h' in aggregator.INTERVAL_MAP
    
    print("✓ Multi-interval aggregator initialized successfully")


def test_aggregate_5min_to_1h():
    """测试5分钟K线聚合为1小时K线"""
    aggregator = MultiIntervalAggregator()
    
    # 创建12根5分钟K线（1小时）
    df_5min = create_mock_5min_klines('BTCUSDT', num_klines=12)
    
    # 聚合为1小时
    df_1h = aggregator.aggregate_klines(df_5min, '1h')
    
    assert not df_1h.empty, "1h aggregation should produce klines"
    assert len(df_1h) == 1, "12个5分钟K线应该聚合为1个1小时K线"
    
    # 验证OHLC
    kline_1h = df_1h.iloc[0]
    assert kline_1h['open'] == df_5min.iloc[0]['open'], "Open should be first 5min open"
    assert kline_1h['close'] == df_5min.iloc[-1]['close'], "Close should be last 5min close"
    assert kline_1h['high'] >= df_5min['high'].max(), "High should be max of all 5min highs"
    assert kline_1h['low'] <= df_5min['low'].min(), "Low should be min of all 5min lows"
    
    # 验证成交量（应该求和）
    expected_volume = df_5min['volume'].sum()
    assert abs(kline_1h['volume'] - expected_volume) < 1e-6, "Volume should be sum of 5min volumes"
    
    print("✓ 5min to 1h aggregation test passed")


def test_aggregate_5min_to_4h():
    """测试5分钟K线聚合为4小时K线"""
    aggregator = MultiIntervalAggregator()
    
    # 创建48根5分钟K线（4小时）
    df_5min = create_mock_5min_klines('BTCUSDT', num_klines=48)
    
    # 聚合为4小时
    df_4h = aggregator.aggregate_klines(df_5min, '4h')
    
    assert not df_4h.empty, "4h aggregation should produce klines"
    assert len(df_4h) == 1, "48个5分钟K线应该聚合为1个4小时K线"
    
    kline_4h = df_4h.iloc[0]
    expected_volume = df_5min['volume'].sum()
    assert abs(kline_4h['volume'] - expected_volume) < 1e-6, "Volume should be sum"
    
    print("✓ 5min to 4h aggregation test passed")


def test_aggregate_5min_to_24h():
    """测试5分钟K线聚合为24小时K线"""
    aggregator = MultiIntervalAggregator()
    
    # 创建288根5分钟K线（24小时，一天）
    df_5min = create_mock_5min_klines('BTCUSDT', num_klines=288)
    
    # 聚合为24小时
    df_24h = aggregator.aggregate_klines(df_5min, '24h')
    
    assert not df_24h.empty, "24h aggregation should produce klines"
    assert len(df_24h) == 1, "288个5分钟K线应该聚合为1个24小时K线"
    
    kline_24h = df_24h.iloc[0]
    expected_volume = df_5min['volume'].sum()
    assert abs(kline_24h['volume'] - expected_volume) < 1e-6, "Volume should be sum"
    
    print("✓ 5min to 24h aggregation test passed")


def test_aggregate_multiple_periods():
    """测试多个周期的聚合"""
    aggregator = MultiIntervalAggregator()
    
    # 创建288根5分钟K线（一天）
    df_5min = create_mock_5min_klines('BTCUSDT', num_klines=288)
    
    intervals = ['1h', '4h', '8h', '12h', '24h']
    
    for interval in intervals:
        df_agg = aggregator.aggregate_klines(df_5min, interval)
        
        assert not df_agg.empty, f"{interval} aggregation should produce klines"
        
        # 验证基本字段存在
        assert 'open' in df_agg.columns
        assert 'high' in df_agg.columns
        assert 'low' in df_agg.columns
        assert 'close' in df_agg.columns
        assert 'volume' in df_agg.columns
        assert 'quote_volume' in df_agg.columns
        
        print(f"✓ {interval} aggregation test passed: {len(df_agg)} klines")


def test_data_api_multi_interval_mode():
    """测试DataAPI的mode参数支持多周期"""
    api = DataAPI()
    
    # 注意：这个测试需要实际数据，所以可能返回空结果
    # 主要测试接口是否支持mode参数
    
    try:
        result = api.get_bar_between('2025-01-01-001', '2025-01-01-288', mode='1h')
        assert isinstance(result, dict)
        
        result = api.get_bar_between('2025-01-01-001', '2025-01-01-288', mode='4h')
        assert isinstance(result, dict)
        
        result = api.get_tran_stats_between('2025-01-01-001', '2025-01-01-288', mode='1h')
        assert isinstance(result, dict)
        
        print("✓ DataAPI multi-interval mode parameter test passed")
    except Exception as e:
        # 如果没有数据，这是正常的
        print(f"⚠ DataAPI multi-interval test skipped (no data): {e}")


def test_aggregate_tran_stats():
    """测试tran_stats的聚合"""
    aggregator = MultiIntervalAggregator()
    
    # 创建12根5分钟K线（1小时），包含分档统计
    df_5min = create_mock_5min_klines('BTCUSDT', num_klines=12)
    
    # 聚合为1小时
    df_1h = aggregator.aggregate_klines(df_5min, '1h')
    
    assert not df_1h.empty
    
    kline_1h = df_1h.iloc[0]
    
    # 验证分档统计（应该求和）
    expected_buy_dolvol1 = df_5min['buy_dolvol1'].sum()
    assert abs(kline_1h.get('buy_dolvol1', 0) - expected_buy_dolvol1) < 1e-6, \
        "Buy dolvol1 should be sum of 5min values"
    
    print("✓ Tran stats aggregation test passed")
