"""
测试两种dry-run模式：
1. 无API密钥的dry-run（完全离线，使用配置文件中的模拟账户信息）
2. 有API密钥的dry-run（使用真实API密钥，但使用test_order endpoint）
"""
import pytest
import os
from unittest.mock import patch
from src.execution.dry_run_client import DryRunBinanceClient
from src.execution.binance_client import BinanceClient
from src.processes.execution_process import ExecutionProcess
from src.common.config import config


@pytest.mark.asyncio
async def test_dry_run_without_api_keys_uses_config():
    """测试无API密钥的dry-run模式，从配置文件读取模拟账户信息"""
    # 测试DryRunBinanceClient从配置读取账户信息
    client = DryRunBinanceClient(account_id='account1')
    
    assert client.dry_run_mode is True
    assert client.account_id == 'account1'
    
    # 验证账户信息来自配置
    account_info = await client.get_account_info()
    assert isinstance(account_info, dict)
    assert 'totalWalletBalance' in account_info
    
    # 验证可以获取持仓（可能包含初始持仓）
    positions = await client.get_positions()
    assert isinstance(positions, list)
    
    await client.close()
    
    print("✓ Dry-run without API keys uses config mock account info")


@pytest.mark.asyncio
async def test_dry_run_without_api_keys_custom_account():
    """测试无API密钥的dry-run模式，使用自定义账户配置"""
    # 测试不同账户的配置
    client = DryRunBinanceClient(account_id='account2')
    
    account_info = await client.get_account_info()
    # account2在配置中应该有200000.0的余额
    assert account_info['totalWalletBalance'] == 200000.0
    assert account_info['availableBalance'] == 100000.0
    
    await client.close()
    
    print("✓ Dry-run without API keys uses custom account config")


@pytest.mark.asyncio
async def test_dry_run_with_api_keys():
    """测试有API密钥的dry-run模式（使用test_order endpoint）"""
    # 检查是否有真实API密钥（用于测试）
    api_key = os.getenv('ACCOUNT1_API_KEY', '').strip()
    api_secret = os.getenv('ACCOUNT1_API_SECRET', '').strip()
    
    if not api_key or not api_secret:
        pytest.skip("Missing ACCOUNT1_API_KEY/ACCOUNT1_API_SECRET env vars for dry-run with API keys test")
    
    # 使用BinanceClient，但dry_run=True
    client = BinanceClient(api_key, api_secret, dry_run=True)
    
    assert client.dry_run is True
    
    # 验证可以使用test_order endpoint（不会实际下单）
    # 注意：这里只是测试客户端初始化，不实际调用API
    await client.close()
    
    print("✓ Dry-run with API keys uses BinanceClient with dry_run=True")


@pytest.mark.asyncio
async def test_execution_process_dry_run_without_api_keys():
    """测试ExecutionProcess在无API密钥的dry-run模式下工作"""
    # 临时设置配置，确保use_api_keys=false
    original_config = config.get('execution.dry_run', {})
    
    try:
        # 模拟配置
        with patch.object(config, 'get', side_effect=lambda key, default=None: {
            'execution.mode': 'dry-run',
            'execution.dry_run': {'use_api_keys': False, 'mock_accounts': {}},
            'execution.accounts': [{'account_id': 'account1'}]
        }.get(key, default)):
            process = ExecutionProcess('account1', execution_mode='dry-run')
            
            assert process.execution_mode == 'dry-run'
            assert process.dry_run is True
            assert isinstance(process.binance_client, DryRunBinanceClient)
            assert process.binance_client.account_id == 'account1'
            
            # 验证可以获取账户信息
            account_info = await process.binance_client.get_account_info()
            assert isinstance(account_info, dict)
            
            await process.binance_client.close()
    finally:
        pass
    
    print("✓ ExecutionProcess dry-run without API keys works")


@pytest.mark.asyncio
async def test_execution_process_dry_run_with_api_keys():
    """测试ExecutionProcess在有API密钥的dry-run模式下工作"""
    # 检查是否有真实API密钥
    api_key = os.getenv('ACCOUNT1_API_KEY', '').strip()
    api_secret = os.getenv('ACCOUNT1_API_SECRET', '').strip()
    
    if not api_key or not api_secret:
        pytest.skip("Missing ACCOUNT1_API_KEY/ACCOUNT1_API_SECRET env vars")
    
    # 临时设置环境变量和配置
    with patch.dict(os.environ, {
        'ACCOUNT1_API_KEY': api_key,
        'ACCOUNT1_API_SECRET': api_secret
    }), patch.object(config, 'get', side_effect=lambda key, default=None: {
        'execution.mode': 'dry-run',
        'execution.dry_run': {'use_api_keys': True, 'mock_accounts': {}},
        'execution.accounts': [{'account_id': 'account1'}]
    }.get(key, default)):
        process = ExecutionProcess('account1', execution_mode='dry-run')
        
        assert process.execution_mode == 'dry-run'
        assert process.dry_run is True
        assert isinstance(process.binance_client, BinanceClient)
        assert process.binance_client.dry_run is True
        
        await process.binance_client.close()
    
    print("✓ ExecutionProcess dry-run with API keys works")


def test_dry_run_without_api_keys_missing_keys():
    """测试无API密钥的dry-run模式，即使缺少API密钥也能工作"""
    # 临时移除API密钥
    original_key = os.environ.pop('ACCOUNT1_API_KEY', None)
    original_secret = os.environ.pop('ACCOUNT1_API_SECRET', None)
    
    try:
        with patch.object(config, 'get', side_effect=lambda key, default=None: {
            'execution.mode': 'dry-run',
            'execution.dry_run': {'use_api_keys': False, 'mock_accounts': {}},
            'execution.accounts': [{'account_id': 'account1'}]
        }.get(key, default)):
            # 应该能正常初始化，不需要API密钥
            process = ExecutionProcess('account1', execution_mode='dry-run')
            assert process.dry_run is True
            assert isinstance(process.binance_client, DryRunBinanceClient)
    finally:
        # 恢复环境变量
        if original_key:
            os.environ['ACCOUNT1_API_KEY'] = original_key
        if original_secret:
            os.environ['ACCOUNT1_API_SECRET'] = original_secret
    
    print("✓ Dry-run without API keys works even when API keys are missing")


def test_dry_run_with_api_keys_missing_keys():
    """测试有API密钥的dry-run模式，缺少API密钥时应该报错"""
    # 临时移除API密钥
    original_key = os.environ.pop('ACCOUNT1_API_KEY', None)
    original_secret = os.environ.pop('ACCOUNT1_API_SECRET', None)
    
    try:
        with patch.object(config, 'get', side_effect=lambda key, default=None: {
            'execution.mode': 'dry-run',
            'execution.dry_run': {'use_api_keys': True, 'mock_accounts': {}},
            'execution.accounts': [{'account_id': 'account1'}]
        }.get(key, default)):
            # 应该报错，因为需要API密钥
            with pytest.raises(ValueError, match="Dry-run mode with API keys requires"):
                process = ExecutionProcess('account1', execution_mode='dry-run')
    finally:
        # 恢复环境变量
        if original_key:
            os.environ['ACCOUNT1_API_KEY'] = original_key
        if original_secret:
            os.environ['ACCOUNT1_API_SECRET'] = original_secret
    
    print("✓ Dry-run with API keys correctly raises error when keys are missing")


@pytest.mark.asyncio
async def test_dry_run_mock_positions_from_config():
    """测试从配置文件读取初始持仓"""
    # 创建一个带有初始持仓的配置
    mock_config = {
        'account1': {
            'total_wallet_balance': 100000.0,
            'available_balance': 50000.0,
            'initial_positions': [
                {
                    'symbol': 'BTCUSDT',
                    'position_amt': 0.1,
                    'entry_price': 50000.0
                },
                {
                    'symbol': 'ETHUSDT',
                    'position_amt': -0.5,  # 做空
                    'entry_price': 3000.0
                }
            ]
        }
    }
    
    # 模拟配置读取
    with patch.object(config, 'get', side_effect=lambda key, default=None: {
        'execution.dry_run': {'mock_accounts': mock_config}
    }.get(key, default)):
        client = DryRunBinanceClient(account_id='account1')
        
        # 验证初始持仓已加载
        positions = await client.get_positions()
        assert len(positions) == 2
        
        btc_pos = next((p for p in positions if p['symbol'] == 'BTCUSDT'), None)
        assert btc_pos is not None
        assert btc_pos['positionAmt'] == 0.1
        assert btc_pos['entryPrice'] == 50000.0
        
        eth_pos = next((p for p in positions if p['symbol'] == 'ETHUSDT'), None)
        assert eth_pos is not None
        assert eth_pos['positionAmt'] == -0.5
        assert eth_pos['entryPrice'] == 3000.0
        
        await client.close()
    
    print("✓ Dry-run loads initial positions from config")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Dry-Run Modes")
    print("=" * 60)
    
    import asyncio
    asyncio.run(test_dry_run_without_api_keys_uses_config())
    asyncio.run(test_dry_run_without_api_keys_custom_account())
    asyncio.run(test_dry_run_with_api_keys())
    asyncio.run(test_execution_process_dry_run_without_api_keys())
    asyncio.run(test_execution_process_dry_run_with_api_keys())
    test_dry_run_without_api_keys_missing_keys()
    test_dry_run_with_api_keys_missing_keys()
    asyncio.run(test_dry_run_mock_positions_from_config())
    
    print("\n" + "=" * 60)
    print("All dry-run mode tests completed!")
    print("=" * 60)
