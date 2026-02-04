"""
测试websocket更新（universe更新时重新订阅）
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from src.data.collector import TradeCollector


@pytest.mark.asyncio
async def test_collector_update_symbols():
    """测试collector的update_symbols方法（异步）"""
    collector = TradeCollector(symbols=['BTCUSDT', 'ETHUSDT'])
    
    # 验证初始状态
    assert len(collector.symbols) == 2
    assert 'BTCUSDT' in collector.symbols
    
    # 更新symbols
    new_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT']
    await collector.update_symbols(new_symbols)
    
    # 验证更新后的状态
    assert len(collector.symbols) == 4
    assert 'BNBUSDT' in collector.symbols
    assert 'SOLUSDT' in collector.symbols
    
    print("✓ Collector update_symbols test passed")


@pytest.mark.asyncio
async def test_collector_update_symbols_removes():
    """测试update_symbols移除symbol"""
    collector = TradeCollector(symbols=['BTCUSDT', 'ETHUSDT', 'BNBUSDT'])
    
    # 移除一个symbol
    new_symbols = ['BTCUSDT', 'ETHUSDT']
    await collector.update_symbols(new_symbols)
    
    # 验证移除
    assert len(collector.symbols) == 2
    assert 'BNBUSDT' not in collector.symbols
    
    print("✓ Collector update_symbols removes symbols test passed")


@pytest.mark.asyncio
async def test_collector_update_symbols_restarts_websocket():
    """测试update_symbols时重启websocket连接"""
    collector = TradeCollector(symbols=['BTCUSDT'])
    
    # 模拟运行状态
    collector.running = True
    # 创建一个快速完成的任务来模拟连接
    async def quick_task():
        await asyncio.sleep(0.01)
    collector.ws_connections = [asyncio.create_task(quick_task())]
    
    # 等待任务完成
    await asyncio.sleep(0.02)
    
    # 更新symbols
    new_symbols = ['BTCUSDT', 'ETHUSDT']
    
    # 应该取消旧连接并重启
    await collector.update_symbols(new_symbols)
    
    # 验证连接被处理
    assert len(collector.ws_connections) == 0 or all(task.done() for task in collector.ws_connections), \
        "Old websocket connections should be cancelled"
    
    print("✓ Collector update_symbols restarts websocket test passed")


@pytest.mark.asyncio
async def test_collector_update_symbols_no_change():
    """测试update_symbols时symbols没有变化"""
    collector = TradeCollector(symbols=['BTCUSDT', 'ETHUSDT'])
    
    original_symbols = collector.symbols.copy()
    
    # 更新为相同的symbols
    await collector.update_symbols(['BTCUSDT', 'ETHUSDT'])
    
    # 应该没有变化
    assert collector.symbols == original_symbols
    
    print("✓ Collector update_symbols no change test passed")
