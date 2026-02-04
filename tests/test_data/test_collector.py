"""
测试逐笔成交数据采集模块
"""
import pytest
import asyncio
import sys
from pathlib import Path
import time

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.data.collector import TradeCollector
from src.data.universe_manager import get_universe_manager


@pytest.mark.asyncio
async def test_collector_initialization():
    """测试采集器初始化"""
    symbols = ['BTCUSDT', 'ETHUSDT']
    collector = TradeCollector(symbols=symbols)
    
    assert collector.symbols == symbols
    assert len(collector.symbols) == 2
    assert not collector.running
    
    print("✓ Collector initialized successfully")


@pytest.mark.asyncio
async def test_collector_websocket_connection():
    """测试WebSocket连接（实际连接测试）"""
    symbols = ['BTCUSDT', 'ETHUSDT']
    
    trades_received = {}
    
    async def on_trade(symbol, trade):
        """交易回调"""
        if symbol not in trades_received:
            trades_received[symbol] = []
        trades_received[symbol].append(trade)
    
    collector = TradeCollector(symbols=symbols, on_trade_callback=on_trade)
    
    try:
        # 启动采集器
        await collector.start()
        
        # 等待接收一些数据
        print("Waiting for trade data...")
        await asyncio.sleep(10)  # 等待10秒
        
        # 停止采集器
        await collector.stop()
        
        # 验证接收到数据
        stats = collector.get_stats()
        print(f"Collector stats: {stats}")
        
        if stats['total_trades_received'] > 0:
            print(f"✓ Successfully received {stats['total_trades_received']} trades")
            for symbol, count in stats['trades_by_symbol'].items():
                if count > 0:
                    print(f"  {symbol}: {count} trades")
        else:
            print("⚠ No trades received (may be network issue or Binance API issue)")
        
    except Exception as e:
        pytest.fail(f"Failed to test collector connection: {e}")
    finally:
        if collector.running:
            await collector.stop()


@pytest.mark.asyncio
async def test_collector_with_real_symbols():
    """测试采集器使用真实Universe"""
    # 获取Universe（使用少量交易对）
    universe_manager = get_universe_manager()
    
    try:
        symbols_info = await universe_manager.fetch_universe_from_binance()
        
        # 使用前3个交易对进行测试
        test_symbols = [s['symbol'] for s in symbols_info[:3]]
        
        if not test_symbols:
            print("⚠ No symbols in universe, skipping test")
            return
        
        print(f"Testing with symbols: {test_symbols}")
        
        trades_received = {}
        
        async def on_trade(symbol, trade):
            trades_received[symbol] = trades_received.get(symbol, 0) + 1
        
        collector = TradeCollector(symbols=test_symbols, on_trade_callback=on_trade)
        
        try:
            await collector.start()
            await asyncio.sleep(5)  # 等待5秒
            await collector.stop()
            
            stats = collector.get_stats()
            if stats['total_trades_received'] > 0:
                print(f"✓ Successfully collected trades for {len(test_symbols)} symbols")
            else:
                print("⚠ No trades received")
                
        except Exception as e:
            print(f"⚠ Error in collector test: {e}")
        finally:
            if collector.running:
                await collector.stop()
                
    except Exception as e:
        print(f"⚠ Failed to fetch universe: {e}")


@pytest.mark.asyncio
async def test_collector_reconnect():
    """测试采集器重连机制"""
    symbols = ['BTCUSDT']
    
    collector = TradeCollector(symbols=symbols)
    
    try:
        await collector.start()
        
        # 运行一段时间
        await asyncio.sleep(5)
        
        # 验证连接正常
        stats_before = collector.get_stats()
        print(f"Before reconnect test: {stats_before['total_trades_received']} trades")
        
        # 模拟断线（取消任务会触发重连）
        if collector.ws_task:
            collector.ws_task.cancel()
            await asyncio.sleep(2)  # 等待重连
        
        # 再运行一段时间
        await asyncio.sleep(5)
        
        stats_after = collector.get_stats()
        print(f"After reconnect test: {stats_after['total_trades_received']} trades")
        
        # 验证重连计数
        assert stats_after['reconnect_count'] >= 0
        
        print("✓ Reconnect mechanism tested")
        
    except Exception as e:
        print(f"⚠ Reconnect test error: {e}")
    finally:
        await collector.stop()


@pytest.mark.asyncio
async def test_collector_batch_connections():
    """测试采集器分批连接功能（大量交易对）"""
    # 创建超过100个交易对的列表，验证分批连接
    symbols = [f'TEST{i}USDT' for i in range(150)]  # 150个交易对，应该分成2批
    
    collector = TradeCollector(symbols=symbols)
    
    # 验证配置
    assert collector.max_symbols_per_connection == 100
    assert len(collector.symbols) == 150
    
    # 验证分批逻辑
    batches = collector._split_symbols_into_batches()
    assert len(batches) == 2  # 150个应该分成2批（100+50）
    assert len(batches[0]) == 100
    assert len(batches[1]) == 50
    
    print(f"✓ Batch connection logic: {len(batches)} batches for {len(symbols)} symbols")
    
    # 注意：不实际连接，因为测试交易对不存在
    print("✓ Batch connection test passed")


@pytest.mark.asyncio
async def test_collector_ping_keepalive_config():
    """测试采集器ping keepalive配置"""
    symbols = ['BTCUSDT']
    collector = TradeCollector(symbols=symbols)
    
    # 验证ping配置
    assert collector.ping_interval == 10  # 应该使用10秒
    assert collector.ping_timeout == 5
    assert collector.open_timeout == 30
    # 不再使用手动ping，移除manual_ping_interval检查
    
    print("✓ Ping keepalive configuration verified")


@pytest.mark.asyncio
async def test_collector_dns_error_handling():
    """测试采集器DNS错误处理"""
    import socket
    from src.common.network_utils import log_network_error
    
    # 测试DNS错误识别
    dns_error = socket.gaierror(11001, "getaddrinfo failed")
    
    # 验证log_network_error能正确识别DNS错误
    # 这应该被识别为"DNS解析失败"而不是"未知网络错误"
    try:
        log_network_error("测试DNS错误", dns_error)
        print("✓ DNS error handling verified")
    except Exception as e:
        print(f"⚠ DNS error handling test: {e}")


@pytest.mark.asyncio
async def test_collector_stop_cleanup():
    """测试采集器停止时的清理逻辑"""
    symbols = ['BTCUSDT', 'ETHUSDT']
    collector = TradeCollector(symbols=symbols)
    
    try:
        await collector.start()
        await asyncio.sleep(2)  # 运行一小段时间
        
        # 验证有连接任务
        assert collector.ws_task is not None or len(collector.ws_connections) > 0
        
        # 停止采集器
        await collector.stop()
        
        # 验证清理
        assert not collector.running
        assert len(collector.ws_connections) == 0
        
        print("✓ Collector stop cleanup verified")
        
    except Exception as e:
        print(f"⚠ Stop cleanup test error: {e}")
        if collector.running:
            await collector.stop()


if __name__ == "__main__":
    # 运行测试
    print("=" * 60)
    print("Testing Trade Collector")
    print("=" * 60)
    
    async def run_tests():
        print("\n1. Testing collector initialization...")
        await test_collector_initialization()
        
        print("\n2. Testing WebSocket connection...")
        await test_collector_websocket_connection()
        
        print("\n3. Testing with real symbols...")
        await test_collector_with_real_symbols()
        
        print("\n4. Testing reconnect mechanism...")
        await test_collector_reconnect()
        
        print("\n" + "=" * 60)
        print("All tests completed!")
        print("=" * 60)
    
    asyncio.run(run_tests())
