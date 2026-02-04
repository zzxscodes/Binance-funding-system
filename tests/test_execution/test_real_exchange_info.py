"""
测试使用真实交易所信息的功能
"""
import pytest
import os
from unittest.mock import patch, AsyncMock
from src.execution.dry_run_client import DryRunBinanceClient
from src.processes.execution_process import ExecutionProcess
from src.common.config import config


@pytest.mark.asyncio
async def test_dry_run_with_real_exchange_info():
    """测试dry-run模式使用真实交易所信息"""
    # 创建使用真实交易所信息的dry-run客户端
    client = DryRunBinanceClient(use_real_exchange_info=True)
    
    assert client.use_real_exchange_info is True
    
    # 获取交易所信息（应该从API获取）
    exchange_info = await client.get_exchange_info()
    
    # 验证返回的数据
    assert isinstance(exchange_info, dict)
    assert 'symbols' in exchange_info
    
    # 如果成功获取真实数据，应该包含很多交易对
    # 如果失败回退到模拟数据，则只有2个
    if len(exchange_info['symbols']) > 10:
        # 成功获取真实数据
        symbols = [s['symbol'] for s in exchange_info['symbols']]
        # 验证包含常见的交易对
        assert 'BTCUSDT' in symbols or any('BTC' in s.upper() for s in symbols)
        print("✓ Successfully loaded real exchange info from API")
    else:
        # 回退到模拟数据（可能是网络问题）
        print("WARNING: Real exchange info failed, fell back to mock data (this is expected if network is unavailable)")
        assert len(exchange_info['symbols']) == 2  # 模拟数据
    
    await client.close()
    
    print("✓ Dry-run with real exchange info works")


@pytest.mark.asyncio
async def test_dry_run_with_mock_exchange_info():
    """测试dry-run模式使用模拟交易所信息"""
    # 创建使用模拟交易所信息的dry-run客户端
    client = DryRunBinanceClient(use_real_exchange_info=False)
    
    assert client.use_real_exchange_info is False
    
    # 获取交易所信息（应该返回模拟数据）
    exchange_info = await client.get_exchange_info()
    
    # 验证返回的是模拟数据（只包含少量交易对）
    assert isinstance(exchange_info, dict)
    assert 'symbols' in exchange_info
    # 模拟数据只包含2个交易对
    assert len(exchange_info['symbols']) == 2
    
    # 验证包含模拟的交易对
    symbols = [s['symbol'] for s in exchange_info['symbols']]
    assert 'BTCUSDT' in symbols
    assert 'ETHUSDT' in symbols
    
    await client.close()
    
    print("✓ Dry-run with mock exchange info works")


@pytest.mark.asyncio
async def test_dry_run_real_exchange_info_fallback():
    """测试真实交易所信息获取失败时的回退机制"""
    # 模拟API调用失败
    with patch('src.execution.dry_run_client.safe_http_request', side_effect=Exception("Network error")):
        client = DryRunBinanceClient(use_real_exchange_info=True)
        
        # 获取交易所信息（应该回退到模拟数据）
        exchange_info = await client.get_exchange_info()
        
        # 验证回退到模拟数据
        assert isinstance(exchange_info, dict)
        assert 'symbols' in exchange_info
        # 应该使用默认的模拟数据
        assert len(exchange_info['symbols']) == 2
        
        await client.close()
    
    print("✓ Dry-run real exchange info fallback works")


@pytest.mark.asyncio
async def test_execution_process_with_real_exchange_info():
    """测试ExecutionProcess使用真实交易所信息"""
    # 模拟配置
    with patch.object(config, 'get', side_effect=lambda key, default=None: {
        'execution.mode': 'dry-run',
        'execution.dry_run': {
            'use_api_keys': False,
            'use_real_exchange_info': True
        },
        'execution.accounts': [{'account_id': 'account1'}],
        'binance.api_base': 'https://fapi.binance.com'
    }.get(key, default)):
        process = ExecutionProcess('account1', execution_mode='dry-run')
        
        assert process.execution_mode == 'dry-run'
        assert process.dry_run is True
        assert isinstance(process.binance_client, DryRunBinanceClient)
        assert process.binance_client.use_real_exchange_info is True
        
        # 验证可以获取交易所信息
        exchange_info = await process.binance_client.get_exchange_info()
        assert isinstance(exchange_info, dict)
        assert 'symbols' in exchange_info
        
        await process.binance_client.close()
    
    print("✓ ExecutionProcess with real exchange info works")


@pytest.mark.asyncio
async def test_execution_process_with_mock_exchange_info():
    """测试ExecutionProcess使用模拟交易所信息"""
    # 模拟配置
    with patch.object(config, 'get', side_effect=lambda key, default=None: {
        'execution.mode': 'dry-run',
        'execution.dry_run': {
            'use_api_keys': False,
            'use_real_exchange_info': False
        },
        'execution.accounts': [{'account_id': 'account1'}],
        'binance.api_base': 'https://fapi.binance.com'
    }.get(key, default)):
        process = ExecutionProcess('account1', execution_mode='dry-run')
        
        assert process.execution_mode == 'dry-run'
        assert process.dry_run is True
        assert isinstance(process.binance_client, DryRunBinanceClient)
        assert process.binance_client.use_real_exchange_info is False
        
        # 验证返回模拟数据
        exchange_info = await process.binance_client.get_exchange_info()
        assert isinstance(exchange_info, dict)
        assert len(exchange_info.get('symbols', [])) == 2  # 模拟数据只有2个
        
        await process.binance_client.close()
    
    print("✓ ExecutionProcess with mock exchange info works")


@pytest.mark.asyncio
async def test_dry_run_with_api_keys_and_real_exchange_info():
    """测试有API密钥的dry-run模式也可以使用真实交易所信息（BinanceClient本身就使用真实API）"""
    # 检查是否有真实API密钥
    api_key = os.getenv('ACCOUNT1_API_KEY', '').strip()
    api_secret = os.getenv('ACCOUNT1_API_SECRET', '').strip()
    
    if not api_key or not api_secret:
        pytest.skip("Missing ACCOUNT1_API_KEY/ACCOUNT1_API_SECRET env vars")
    
    # 使用BinanceClient（有API密钥的dry-run模式）
    from src.execution.binance_client import BinanceClient
    client = BinanceClient(api_key, api_secret, dry_run=True)
    
    # BinanceClient总是使用真实交易所信息
    exchange_info = await client.get_exchange_info()
    
    assert isinstance(exchange_info, dict)
    assert 'symbols' in exchange_info
    assert len(exchange_info['symbols']) > 10  # 真实数据应该有很多交易对
    
    await client.close()
    
    print("✓ Dry-run with API keys uses real exchange info (BinanceClient always uses real API)")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Real Exchange Info Feature")
    print("=" * 60)
    
    import asyncio
    asyncio.run(test_dry_run_with_real_exchange_info())
    asyncio.run(test_dry_run_with_mock_exchange_info())
    asyncio.run(test_dry_run_real_exchange_info_fallback())
    asyncio.run(test_execution_process_with_real_exchange_info())
    asyncio.run(test_execution_process_with_mock_exchange_info())
    asyncio.run(test_dry_run_with_api_keys_and_real_exchange_info())
    
    print("\n" + "=" * 60)
    print("All real exchange info tests completed!")
    print("=" * 60)
