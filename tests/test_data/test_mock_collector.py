"""
测试模拟逐笔成交数据采集器
"""
import pytest
import asyncio
from src.data.mock_collector import MockTradeCollector


@pytest.mark.asyncio
async def test_mock_collector_initialization():
    """测试模拟采集器初始化"""
    collector = MockTradeCollector(symbols=['BTCUSDT', 'ETHUSDT'])
    
    assert collector.symbols == ['BTCUSDT', 'ETHUSDT']
    assert collector.running is False
    assert 'BTCUSDT' in collector.base_prices
    assert 'ETHUSDT' in collector.base_prices
    
    print("✓ MockTradeCollector initialized")


@pytest.mark.asyncio
async def test_mock_collector_generate_trades():
    """测试模拟采集器生成交易"""
    trades_received = []
    
    async def on_trade(symbol, trade):
        trades_received.append((symbol, trade))
    
    collector = MockTradeCollector(
        symbols=['BTCUSDT', 'ETHUSDT'],
        on_trade_callback=on_trade
    )
    
    # 启动采集器
    await collector.start()
    
    # 等待生成一些交易
    await asyncio.sleep(0.5)  # 等待500ms，应该生成几个交易
    
    # 停止采集器
    await collector.stop()
    
    # 验证收到了交易
    assert len(trades_received) > 0
    
    # 验证交易格式
    for symbol, trade in trades_received:
        assert symbol in ['BTCUSDT', 'ETHUSDT']
        assert 'price' in trade
        assert 'qty' in trade
        assert 'quoteQty' in trade
        assert trade['price'] > 0
        assert trade['qty'] > 0
    
    # 验证统计信息
    stats = collector.get_stats()
    assert stats['total_trades'] > 0
    assert 'BTCUSDT' in stats['trades_by_symbol'] or 'ETHUSDT' in stats['trades_by_symbol']
    
    print(f"✓ MockTradeCollector generated {len(trades_received)} trades")


@pytest.mark.asyncio
async def test_mock_collector_update_symbols():
    """测试更新交易对列表"""
    collector = MockTradeCollector(symbols=['BTCUSDT'])
    
    assert len(collector.symbols) == 1
    
    # 更新交易对
    collector.update_symbols(['BTCUSDT', 'ETHUSDT', 'BNBUSDT'])
    
    assert len(collector.symbols) == 3
    assert 'BTCUSDT' in collector.symbols
    assert 'ETHUSDT' in collector.symbols
    assert 'BNBUSDT' in collector.symbols
    
    print("✓ MockTradeCollector update_symbols works")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Mock Trade Collector")
    print("=" * 60)
    
    asyncio.run(test_mock_collector_initialization())
    asyncio.run(test_mock_collector_generate_trades())
    asyncio.run(test_mock_collector_update_symbols())
    
    print("\n" + "=" * 60)
    print("All mock collector tests completed!")
    print("=" * 60)
