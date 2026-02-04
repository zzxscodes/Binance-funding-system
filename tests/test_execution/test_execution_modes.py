"""
测试执行模式（dry-run, testnet, live）
"""
import pytest
import os
from unittest.mock import patch, MagicMock
from src.processes.execution_process import ExecutionProcess
from src.execution.dry_run_client import DryRunBinanceClient
from src.execution.binance_client import BinanceClient


@pytest.mark.asyncio
async def test_execution_process_mock_mode():
    """测试mock模式（不需要API密钥）"""
    from src.common.config import config
    
    # 设置mock模式配置
    original_mode = config.get('execution.mode')
    original_mock_config = config.get('execution.mock', {})
    
    try:
        config.set('execution.mode', 'mock')
        mock_config = {
            'api_base': 'https://mock.binance.com',
            'ws_base': 'wss://mock.binance.com',
            'accounts': [
                {'account_id': 'account1', 'total_wallet_balance': 100000.0, 'available_balance': 50000.0, 'initial_positions': []}
            ]
        }
        config.set('execution.mock', mock_config)
        
        process = ExecutionProcess('account1', execution_mode='mock')
        
        assert process.execution_mode == 'mock'
        assert process.dry_run is True
        assert isinstance(process.binance_client, DryRunBinanceClient)
        assert process.order_manager.dry_run is True
        
        # 验证可以获取账户信息（模拟）
        account_info = await process.binance_client.get_account_info()
        assert isinstance(account_info, dict)
        
        await process.binance_client.close()
        
        print("✓ Execution process mock mode works without API keys")
    finally:
        config.set('execution.mode', original_mode)
        config.set('execution.mock', original_mock_config)


@pytest.mark.asyncio
async def test_execution_process_testnet_mode():
    """测试testnet模式（需要testnet API密钥）"""
    from src.common.config import config
    
    # 检查是否有testnet密钥
    testnet_key = os.getenv('BINANCE_TESTNET_API_KEY', '').strip()
    testnet_secret = os.getenv('BINANCE_TESTNET_API_SECRET', '').strip()
    
    if not testnet_key or not testnet_secret:
        pytest.skip("Missing BINANCE_TESTNET_API_KEY/BINANCE_TESTNET_API_SECRET env vars")
    
    # 设置testnet模式配置
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {})
    
    try:
        config.set('execution.mode', 'testnet')
        testnet_config = {
            'api_base': 'https://testnet.binancefuture.com',
            'ws_base': 'wss://stream.binancefuture.com',
            'accounts': [
                {'account_id': 'account1', 'api_key': testnet_key, 'api_secret': testnet_secret}
            ]
        }
        config.set('execution.testnet', testnet_config)
        
        process = ExecutionProcess('account1', execution_mode='testnet')
        
        assert process.execution_mode == 'testnet'
        assert process.dry_run == config.get('execution.dry_run', False)
        assert isinstance(process.binance_client, BinanceClient)
        assert process.binance_client.api_base == 'https://testnet.binancefuture.com'
        assert 'testnet' in process.binance_client.api_base.lower()
        
        await process.binance_client.close()
        
        print("✓ Execution process testnet mode works")
    finally:
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)


def test_execution_process_testnet_mode_missing_keys():
    """测试testnet模式缺少密钥时的错误处理"""
    from src.common.config import config
    
    # 临时移除testnet密钥（环境变量）
    original_key = os.environ.pop('ACCOUNT1_TESTNET_API_KEY', None)
    original_secret = os.environ.pop('ACCOUNT1_TESTNET_API_SECRET', None)
    
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {}).copy()
    
    try:
        config.set('execution.mode', 'testnet')
        # 设置testnet配置，但账户没有密钥
        testnet_config = {
            'api_base': 'https://testnet.binancefuture.com',
            'ws_base': 'wss://stream.binancefuture.com',
            'accounts': [
                {'account_id': 'account1'}  # 没有api_key和api_secret
            ]
        }
        config.set('execution.testnet', testnet_config)
        
        with pytest.raises(ValueError, match="Testnet mode requires"):
            process = ExecutionProcess('account1', execution_mode='testnet')
    finally:
        # 恢复环境变量
        if original_key:
            os.environ['ACCOUNT1_TESTNET_API_KEY'] = original_key
        if original_secret:
            os.environ['ACCOUNT1_TESTNET_API_SECRET'] = original_secret
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)
    
    print("✓ Testnet mode correctly raises error when keys are missing")


def test_execution_process_live_mode_missing_keys():
    """测试live模式缺少密钥时的错误处理"""
    from src.common.config import config
    
    # 临时移除账户密钥（环境变量）
    original_key = os.environ.pop('ACCOUNT1_API_KEY', None)
    original_secret = os.environ.pop('ACCOUNT1_API_SECRET', None)
    
    original_mode = config.get('execution.mode')
    original_live_config = config.get('execution.live', {}).copy()
    
    try:
        config.set('execution.mode', 'live')
        # 设置live配置，但账户没有密钥
        live_config = {
            'api_base': 'https://fapi.binance.com',
            'ws_base': 'wss://fstream.binance.com',
            'accounts': [
                {'account_id': 'account1'}  # 没有api_key和api_secret
            ]
        }
        config.set('execution.live', live_config)
        
        with pytest.raises(ValueError, match="Live mode requires"):
            process = ExecutionProcess('account1', execution_mode='live')
    finally:
        # 恢复环境变量
        if original_key:
            os.environ['ACCOUNT1_API_KEY'] = original_key
        if original_secret:
            os.environ['ACCOUNT1_API_SECRET'] = original_secret
        config.set('execution.mode', original_mode)
        config.set('execution.live', original_live_config)
    
    print("✓ Live mode correctly raises error when keys are missing")


@pytest.mark.asyncio
async def test_execution_process_mock_execute_positions():
    """测试mock模式下执行目标持仓"""
    from src.common.config import config
    
    original_mode = config.get('execution.mode')
    original_mock_config = config.get('execution.mock', {})
    
    try:
        config.set('execution.mode', 'mock')
        mock_config = {
            'api_base': 'https://mock.binance.com',
            'ws_base': 'wss://mock.binance.com',
            'accounts': [
                {'account_id': 'account1', 'total_wallet_balance': 100000.0, 'available_balance': 50000.0, 'initial_positions': []}
            ]
        }
        config.set('execution.mock', mock_config)
        
        process = ExecutionProcess('account1', execution_mode='mock')
        
        # 创建模拟目标持仓
        target_positions = {
            'BTCUSDT': 0.1,  # 做多0.1 BTC
            'ETHUSDT': -0.5,  # 做空0.5 ETH
        }
        
        # 执行目标持仓（mock模式）
        executed_orders = await process.order_manager.execute_target_positions(target_positions)
        
        # 验证订单已执行（模拟）
        assert isinstance(executed_orders, list)
        # 在mock模式下，应该生成订单（虽然是模拟的）
        
        await process.binance_client.close()
        
        print("✓ Mock execute_target_positions() works")
    finally:
        config.set('execution.mode', original_mode)
        config.set('execution.mock', original_mock_config)


@pytest.mark.asyncio
async def test_order_manager_dry_run_mode():
    """测试OrderManager在dry-run模式下的行为"""
    from src.execution.order_manager import OrderManager
    
    # 创建dry-run客户端
    dry_run_client = DryRunBinanceClient()
    order_manager = OrderManager(dry_run_client, dry_run=True)
    
    assert order_manager.dry_run is True
    
    # 测试执行目标持仓
    target_positions = {'BTCUSDT': 0.1}
    executed_orders = await order_manager.execute_target_positions(target_positions)
    
    # 在dry-run模式下，应该能正常执行（模拟）
    assert isinstance(executed_orders, list)
    
    await dry_run_client.close()
    
    print("✓ OrderManager dry-run mode works")


@pytest.mark.asyncio
async def test_binance_client_dry_run_flag():
    """测试BinanceClient的dry_run标志"""
    # 测试dry_run=True时使用test_order endpoint
    client = BinanceClient(
        api_key='test_key',
        api_secret='test_secret',
        dry_run=True
    )
    
    assert client.dry_run is True
    
    await client.close()
    
    print("✓ BinanceClient dry_run flag works")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Execution Modes")
    print("=" * 60)
    
    import asyncio
    asyncio.run(test_execution_process_mock_mode())
    asyncio.run(test_execution_process_testnet_mode())
    test_execution_process_testnet_mode_missing_keys()
    test_execution_process_live_mode_missing_keys()
    asyncio.run(test_execution_process_mock_execute_positions())
    asyncio.run(test_order_manager_dry_run_mode())
    asyncio.run(test_binance_client_dry_run_flag())
    
    print("\n" + "=" * 60)
    print("All execution mode tests completed!")
    print("=" * 60)
