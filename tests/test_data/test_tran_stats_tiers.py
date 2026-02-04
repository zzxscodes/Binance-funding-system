"""
测试tran_stats表的按金额分档统计功能
验证分档规则和统计准确性
"""
import pytest
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.data.kline_aggregator import KlineAggregator


def create_mock_trade(symbol: str, price: float, qty: float, timestamp_ms: int, 
                     is_buyer_maker: bool = False) -> dict:
    """创建模拟交易数据"""
    return {
        'price': price,
        'qty': qty,
        'quoteQty': price * qty,  # dolvol
        'isBuyerMaker': is_buyer_maker,
        'ts_ms': timestamp_ms,
    }


@pytest.mark.asyncio
async def test_tran_stats_tier_classification():
    """
    测试按金额分档统计的正确性
    
    分档规则：
    - Tier 1: dolvol <= 40000
    - Tier 2: dolvol > 40000 && <= 200000
    - Tier 3: dolvol > 200000 && <= 1000000
    - Tier 4: dolvol > 1000000
    """
    aggregator = KlineAggregator(interval_minutes=5)
    
    symbol = 'BTCUSDT'
    base_time = datetime(2026, 1, 21, 14, 0, 0, tzinfo=timezone.utc)
    window_start_ms = int(base_time.timestamp() * 1000)
    window_start_ms = (window_start_ms // 300000) * 300000
    
    # 创建不同金额档位的交易
    # 买方交易（isBuyerMaker=False）
    trades = [
        # Tier 1: <= 40000
        create_mock_trade(symbol, 50000.0, 0.5, window_start_ms + 1000, False),   # dolvol = 25000
        create_mock_trade(symbol, 50000.0, 0.3, window_start_ms + 2000, False),   # dolvol = 15000
        # Tier 2: > 40000 && <= 200000
        create_mock_trade(symbol, 50000.0, 1.0, window_start_ms + 3000, False),    # dolvol = 50000
        create_mock_trade(symbol, 50000.0, 2.0, window_start_ms + 4000, False),    # dolvol = 100000
        # Tier 3: > 200000 && <= 1000000
        create_mock_trade(symbol, 50000.0, 5.0, window_start_ms + 5000, False),    # dolvol = 250000
        create_mock_trade(symbol, 50000.0, 10.0, window_start_ms + 6000, False),   # dolvol = 500000
        # Tier 4: > 1000000
        create_mock_trade(symbol, 50000.0, 25.0, window_start_ms + 7000, False),   # dolvol = 1250000
        
        # 卖方交易（isBuyerMaker=True）
        # Tier 1
        create_mock_trade(symbol, 50000.0, 0.2, window_start_ms + 8000, True),     # dolvol = 10000
        # Tier 2
        create_mock_trade(symbol, 50000.0, 1.5, window_start_ms + 9000, True),     # dolvol = 75000
        # Tier 3
        create_mock_trade(symbol, 50000.0, 8.0, window_start_ms + 10000, True),    # dolvol = 400000
        # Tier 4
        create_mock_trade(symbol, 50000.0, 30.0, window_start_ms + 11000, True),   # dolvol = 1500000
    ]
    
    # 聚合
    await aggregator._aggregate_window(symbol, window_start_ms, trades_override=trades)
    
    klines = aggregator.get_klines(symbol)
    assert not klines.empty, "Should generate kline"
    
    kline = klines.iloc[-1]
    
    # 验证买方分档统计
    # Tier 1: 2笔交易，dolvol = 25000 + 15000 = 40000, volume = 0.5 + 0.3 = 0.8
    assert kline['buy_trade_count1'] == 2, f"Expected 2 buy trades in tier 1, got {kline['buy_trade_count1']}"
    assert abs(kline['buy_dolvol1'] - 40000.0) < 1e-6, f"Expected buy_dolvol1=40000, got {kline['buy_dolvol1']}"
    assert abs(kline['buy_volume1'] - 0.8) < 1e-6, f"Expected buy_volume1=0.8, got {kline['buy_volume1']}"
    
    # Tier 2: 2笔交易，dolvol = 50000 + 100000 = 150000, volume = 1.0 + 2.0 = 3.0
    assert kline['buy_trade_count2'] == 2, f"Expected 2 buy trades in tier 2, got {kline['buy_trade_count2']}"
    assert abs(kline['buy_dolvol2'] - 150000.0) < 1e-6, f"Expected buy_dolvol2=150000, got {kline['buy_dolvol2']}"
    assert abs(kline['buy_volume2'] - 3.0) < 1e-6, f"Expected buy_volume2=3.0, got {kline['buy_volume2']}"
    
    # Tier 3: 2笔交易，dolvol = 250000 + 500000 = 750000, volume = 5.0 + 10.0 = 15.0
    assert kline['buy_trade_count3'] == 2, f"Expected 2 buy trades in tier 3, got {kline['buy_trade_count3']}"
    assert abs(kline['buy_dolvol3'] - 750000.0) < 1e-6, f"Expected buy_dolvol3=750000, got {kline['buy_dolvol3']}"
    assert abs(kline['buy_volume3'] - 15.0) < 1e-6, f"Expected buy_volume3=15.0, got {kline['buy_volume3']}"
    
    # Tier 4: 1笔交易，dolvol = 1250000, volume = 25.0
    assert kline['buy_trade_count4'] == 1, f"Expected 1 buy trade in tier 4, got {kline['buy_trade_count4']}"
    assert abs(kline['buy_dolvol4'] - 1250000.0) < 1e-6, f"Expected buy_dolvol4=1250000, got {kline['buy_dolvol4']}"
    assert abs(kline['buy_volume4'] - 25.0) < 1e-6, f"Expected buy_volume4=25.0, got {kline['buy_volume4']}"
    
    # 验证卖方分档统计
    # Tier 1: 1笔交易，dolvol = 10000, volume = 0.2
    assert kline['sell_trade_count1'] == 1, f"Expected 1 sell trade in tier 1, got {kline['sell_trade_count1']}"
    assert abs(kline['sell_dolvol1'] - 10000.0) < 1e-6, f"Expected sell_dolvol1=10000, got {kline['sell_dolvol1']}"
    assert abs(kline['sell_volume1'] - 0.2) < 1e-6, f"Expected sell_volume1=0.2, got {kline['sell_volume1']}"
    
    # Tier 2: 1笔交易，dolvol = 75000, volume = 1.5
    assert kline['sell_trade_count2'] == 1, f"Expected 1 sell trade in tier 2, got {kline['sell_trade_count2']}"
    assert abs(kline['sell_dolvol2'] - 75000.0) < 1e-6, f"Expected sell_dolvol2=75000, got {kline['sell_dolvol2']}"
    assert abs(kline['sell_volume2'] - 1.5) < 1e-6, f"Expected sell_volume2=1.5, got {kline['sell_volume2']}"
    
    # Tier 3: 1笔交易，dolvol = 400000, volume = 8.0
    assert kline['sell_trade_count3'] == 1, f"Expected 1 sell trade in tier 3, got {kline['sell_trade_count3']}"
    assert abs(kline['sell_dolvol3'] - 400000.0) < 1e-6, f"Expected sell_dolvol3=400000, got {kline['sell_dolvol3']}"
    assert abs(kline['sell_volume3'] - 8.0) < 1e-6, f"Expected sell_volume3=8.0, got {kline['sell_volume3']}"
    
    # Tier 4: 1笔交易，dolvol = 1500000, volume = 30.0
    assert kline['sell_trade_count4'] == 1, f"Expected 1 sell trade in tier 4, got {kline['sell_trade_count4']}"
    assert abs(kline['sell_dolvol4'] - 1500000.0) < 1e-6, f"Expected sell_dolvol4=1500000, got {kline['sell_dolvol4']}"
    assert abs(kline['sell_volume4'] - 30.0) < 1e-6, f"Expected sell_volume4=30.0, got {kline['sell_volume4']}"
    
    # 验证总和
    buy_vol_total = kline['buy_volume1'] + kline['buy_volume2'] + kline['buy_volume3'] + kline['buy_volume4']
    assert abs(buy_vol_total - kline['buyvolume']) < 1e-6, f"Buy volume tiers sum mismatch"
    
    buy_dolvol_total = kline['buy_dolvol1'] + kline['buy_dolvol2'] + kline['buy_dolvol3'] + kline['buy_dolvol4']
    assert abs(buy_dolvol_total - kline['buydolvol']) < 1e-6, f"Buy dolvol tiers sum mismatch"
    
    buy_count_total = kline['buy_trade_count1'] + kline['buy_trade_count2'] + kline['buy_trade_count3'] + kline['buy_trade_count4']
    assert buy_count_total == kline['buytradecount'], f"Buy trade count tiers sum mismatch"
    
    print("✓ Tran_stats tier classification test passed")
    print(f"  Buy tiers: {kline['buy_trade_count1']}+{kline['buy_trade_count2']}+{kline['buy_trade_count3']}+{kline['buy_trade_count4']} = {kline['buytradecount']}")
    print(f"  Sell tiers: {kline['sell_trade_count1']}+{kline['sell_trade_count2']}+{kline['sell_trade_count3']}+{kline['sell_trade_count4']} = {kline['selltradecount']}")


@pytest.mark.asyncio
async def test_tran_stats_boundary_values():
    """测试分档边界值（正好在边界上的交易）"""
    aggregator = KlineAggregator(interval_minutes=5)
    
    symbol = 'BTCUSDT'
    base_time = datetime(2026, 1, 21, 14, 0, 0, tzinfo=timezone.utc)
    window_start_ms = int(base_time.timestamp() * 1000)
    window_start_ms = (window_start_ms // 300000) * 300000
    
    # 测试边界值
    trades = [
        # 正好在边界上的交易
        create_mock_trade(symbol, 40000.0, 1.0, window_start_ms + 1000, False),    # dolvol = 40000 (Tier 1)
        create_mock_trade(symbol, 40000.01, 1.0, window_start_ms + 2000, False),  # dolvol = 40000.01 (Tier 2)
        create_mock_trade(symbol, 200000.0, 1.0, window_start_ms + 3000, False),  # dolvol = 200000 (Tier 2)
        create_mock_trade(symbol, 200000.01, 1.0, window_start_ms + 4000, False), # dolvol = 200000.01 (Tier 3)
        create_mock_trade(symbol, 1000000.0, 1.0, window_start_ms + 5000, False), # dolvol = 1000000 (Tier 3)
        create_mock_trade(symbol, 1000000.01, 1.0, window_start_ms + 6000, False), # dolvol = 1000000.01 (Tier 4)
    ]
    
    await aggregator._aggregate_window(symbol, window_start_ms, trades_override=trades)
    
    klines = aggregator.get_klines(symbol)
    kline = klines.iloc[-1]
    
    # 验证边界值分类
    assert kline['buy_trade_count1'] == 1, "40000 should be in tier 1"
    assert kline['buy_trade_count2'] == 2, "40000.01 and 200000 should be in tier 2"
    assert kline['buy_trade_count3'] == 2, "200000.01 and 1000000 should be in tier 3"
    assert kline['buy_trade_count4'] == 1, "1000000.01 should be in tier 4"
    
    print("✓ Tran_stats boundary values test passed")


if __name__ == "__main__":
    async def run_tests():
        print("=" * 60)
        print("Tran_stats Tier Classification Tests")
        print("=" * 60)
        
        await test_tran_stats_tier_classification()
        await test_tran_stats_boundary_values()
        
        print("\n" + "=" * 60)
        print("All tests passed!")
        print("=" * 60)
    
    asyncio.run(run_tests())
