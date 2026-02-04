"""
测试新的执行模式（testnet, mock, live）
适配新的配置结构：每种模式有独立的账户配置
"""
import pytest
import os
from unittest.mock import patch, MagicMock
from src.processes.execution_process import ExecutionProcess
from src.execution.dry_run_client import DryRunBinanceClient
from src.execution.binance_client import BinanceClient
from src.common.config import config


@pytest.fixture(autouse=True)
def setup_config():
    """为每个测试设置和清理配置"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {}).copy()
    original_mock_config = config.get('execution.mock', {}).copy()
    original_live_config = config.get('execution.live', {}).copy()
    
    # 设置测试配置
    testnet_config = {
        'api_base': 'https://testnet.binancefuture.com',
        'ws_base': 'wss://stream.binancefuture.com',
        'accounts': [
            {'account_id': 'account1', 'api_key': 'test_testnet_key1', 'api_secret': 'test_testnet_secret1'},
            {'account_id': 'account2', 'api_key': 'test_testnet_key2', 'api_secret': 'test_testnet_secret2'}
        ]
    }
    mock_config = {
        'api_base': 'https://mock.binance.com',
        'ws_base': 'wss://mock.binance.com',
        'accounts': [
            {'account_id': 'account1', 'total_wallet_balance': 100000.0, 'available_balance': 50000.0, 'initial_positions': []},
            {'account_id': 'account2', 'total_wallet_balance': 200000.0, 'available_balance': 100000.0, 'initial_positions': []}
        ]
    }
    live_config = {
        'api_base': 'https://fapi.binance.com',
        'ws_base': 'wss://fstream.binance.com',
        'accounts': [
            {'account_id': 'account1', 'api_key': 'test_live_key1', 'api_secret': 'test_live_secret1'},
            {'account_id': 'account2', 'api_key': 'test_live_key2', 'api_secret': 'test_live_secret2'}
        ]
    }
    
    config.set('execution.testnet', testnet_config)
    config.set('execution.mock', mock_config)
    config.set('execution.live', live_config)
    
    yield
    
    # 恢复原始配置
    config.set('execution.mode', original_mode)
    config.set('execution.testnet', original_testnet_config)
    config.set('execution.mock', original_mock_config)
    config.set('execution.live', original_live_config)


def test_execution_process_testnet_mode():
    """测试testnet模式初始化"""
    config.set('execution.mode', 'testnet')
    
    process = ExecutionProcess('account1', execution_mode='testnet')
    
    assert process.execution_mode == 'testnet'
    assert process.account_id == 'account1'
    assert process.api_key == 'test_testnet_key1'
    assert process.api_secret == 'test_testnet_secret1'
    assert isinstance(process.binance_client, BinanceClient)
    assert process.binance_client.api_base == 'https://testnet.binancefuture.com'
    assert process.dry_run == config.get('execution.dry_run', False)
    
    print("✓ Testnet mode initialization works")


def test_execution_process_testnet_mode_with_dry_run():
    """测试testnet模式启用dry-run"""
    config.set('execution.mode', 'testnet')
    config.set('execution.dry_run', True)
    
    process = ExecutionProcess('account1', execution_mode='testnet')
    
    assert process.dry_run is True
    assert process.binance_client.dry_run is True
    
    config.set('execution.dry_run', False)  # 恢复
    print("✓ Testnet mode with dry-run works")


def test_execution_process_mock_mode():
    """测试mock模式初始化"""
    config.set('execution.mode', 'mock')
    
    process = ExecutionProcess('account1', execution_mode='mock')
    
    assert process.execution_mode == 'mock'
    assert process.account_id == 'account1'
    assert process.api_key is None
    assert process.api_secret is None
    assert isinstance(process.binance_client, DryRunBinanceClient)
    assert process.dry_run is True  # Mock模式总是dry-run
    
    print("✓ Mock mode initialization works")


def test_execution_process_live_mode():
    """测试live模式初始化"""
    config.set('execution.mode', 'live')
    
    process = ExecutionProcess('account1', execution_mode='live')
    
    assert process.execution_mode == 'live'
    assert process.account_id == 'account1'
    assert process.api_key == 'test_live_key1'
    assert process.api_secret == 'test_live_secret1'
    assert isinstance(process.binance_client, BinanceClient)
    assert process.binance_client.api_base == 'https://fapi.binance.com'
    assert process.dry_run == config.get('execution.dry_run', False)
    
    print("✓ Live mode initialization works")


def test_execution_process_live_mode_with_dry_run():
    """测试live模式启用dry-run"""
    config.set('execution.mode', 'live')
    config.set('execution.dry_run', True)
    
    process = ExecutionProcess('account1', execution_mode='live')
    
    assert process.dry_run is True
    assert process.binance_client.dry_run is True
    
    config.set('execution.dry_run', False)  # 恢复
    print("✓ Live mode with dry-run works")


def test_execution_process_account_not_found():
    """测试账户不存在时的错误处理"""
    config.set('execution.mode', 'testnet')
    
    with pytest.raises(ValueError, match="Account.*not found"):
        ExecutionProcess('nonexistent_account', execution_mode='testnet')
    
    print("✓ Account not found error handling works")


def test_execution_process_mode_not_found():
    """测试模式配置不存在时的错误处理"""
    config.set('execution.mode', 'testnet')
    config.set('execution.testnet', {})  # 清空testnet配置
    
    with pytest.raises(ValueError, match="Configuration for mode"):
        ExecutionProcess('account1', execution_mode='testnet')
    
    print("✓ Mode config not found error handling works")


def test_execution_process_env_var_priority():
    """测试环境变量优先级高于配置文件"""
    config.set('execution.mode', 'testnet')
    
    with patch.dict(os.environ, {
        'ACCOUNT1_TESTNET_API_KEY': 'env_testnet_key',
        'ACCOUNT1_TESTNET_API_SECRET': 'env_testnet_secret'
    }):
        process = ExecutionProcess('account1', execution_mode='testnet')
        
        assert process.api_key == 'env_testnet_key'
        assert process.api_secret == 'env_testnet_secret'
    
    print("✓ Environment variable priority works")


def test_execution_process_multiple_accounts():
    """测试多账户配置"""
    config.set('execution.mode', 'testnet')
    
    # 测试account1
    process1 = ExecutionProcess('account1', execution_mode='testnet')
    assert process1.api_key == 'test_testnet_key1'
    
    # 测试account2
    process2 = ExecutionProcess('account2', execution_mode='testnet')
    assert process2.api_key == 'test_testnet_key2'
    
    print("✓ Multiple accounts configuration works")


@pytest.mark.asyncio
async def test_execution_process_mock_mode_account_info():
    """测试mock模式的账户信息"""
    config.set('execution.mode', 'mock')
    
    process = ExecutionProcess('account1', execution_mode='mock')
    
    # Mock模式应该能获取账户信息（模拟）
    account_info = await process.binance_client.get_account_info()
    assert isinstance(account_info, dict)
    assert 'totalWalletBalance' in account_info
    
    await process.binance_client.close()
    print("✓ Mock mode account info works")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing New Execution Modes")
    print("=" * 60)
    
    test_execution_process_testnet_mode()
    test_execution_process_testnet_mode_with_dry_run()
    test_execution_process_mock_mode()
    test_execution_process_live_mode()
    test_execution_process_live_mode_with_dry_run()
    test_execution_process_account_not_found()
    test_execution_process_mode_not_found()
    test_execution_process_env_var_priority()
    test_execution_process_multiple_accounts()
    
    import asyncio
    asyncio.run(test_execution_process_mock_mode_account_info())
    
    print("\n" + "=" * 60)
    print("All new execution mode tests completed!")
    print("=" * 60)
