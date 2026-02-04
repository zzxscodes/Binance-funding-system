"""
测试监控进程的功能
"""
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from src.processes.monitoring_process import MonitoringProcess
from src.common.config import config


@pytest.mark.asyncio
async def test_monitoring_process_without_account_clients():
    """测试监控进程在没有账户客户端时的行为"""
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {})
    
    try:
        config.set('execution.mode', 'testnet')
        # 设置testnet配置，但没有账户（或账户没有密钥）
        testnet_config = {
            'api_base': 'https://testnet.binancefuture.com',
            'ws_base': 'wss://stream.binancefuture.com',
            'accounts': []  # 空账户列表
        }
        config.set('execution.testnet', testnet_config)
        
        process = MonitoringProcess()
        
        # 验证没有账户客户端
        assert len(process.account_clients) == 0
        
        # 启动进程（应该能正常启动，即使没有账户客户端）
        await process.start()
        
        # 验证进程已启动
        assert process.running is True
        
        # 运行一小段时间（应该不会执行监控循环，但进程应该保持运行）
        run_task = asyncio.create_task(process.run())
        
        # 等待一小段时间
        await asyncio.sleep(0.5)
        
        # 停止进程
        process.running = False
        await asyncio.sleep(0.1)
        
        # 清理
        await process.stop()
        
        print("[OK] Monitoring process starts correctly without account clients")
    finally:
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)


@pytest.mark.asyncio
async def test_monitoring_process_with_account_clients():
    """测试监控进程在有账户客户端时的行为"""
    import os
    from src.execution.dry_run_client import DryRunBinanceClient
    
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
        
        process = MonitoringProcess()
        
        # 验证有账户客户端
        assert len(process.account_clients) > 0
        assert 'account1' in process.account_clients
        
        # 启动进程
        await process.start()
        assert process.running is True
        
        # 运行一小段时间
        run_task = asyncio.create_task(process.run())
        
        # 等待一小段时间（让监控循环运行一次）
        await asyncio.sleep(1.0)
        
        # 停止进程
        process.running = False
        await asyncio.sleep(0.1)
        
        # 清理
        await process.stop()
        
        # 关闭所有客户端
        for client in process.account_clients.values():
            await client.close()
        
        print("[OK] Monitoring process works correctly with account clients")
    finally:
        config.set('execution.mode', original_mode)
        config.set('execution.mock', original_mock_config)


@pytest.mark.asyncio
async def test_monitoring_process_run_without_clients():
    """测试监控进程的run方法在没有客户端时的行为"""
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {})
    
    try:
        config.set('execution.mode', 'testnet')
        testnet_config = {
            'api_base': 'https://testnet.binancefuture.com',
            'ws_base': 'wss://stream.binancefuture.com',
            'accounts': []  # 空账户列表
        }
        config.set('execution.testnet', testnet_config)
        
        process = MonitoringProcess()
        process.running = True
        
        # 直接测试run方法（应该保持运行但不执行监控）
        run_task = asyncio.create_task(process.run())
        
        # 等待一小段时间
        await asyncio.sleep(0.5)
        
        # 验证进程仍在运行
        assert process.running is True
        
        # 停止进程
        process.running = False
        await asyncio.sleep(0.1)
        
        # 清理
        await process.stop()
        
        print("[OK] Monitoring process run() works correctly without clients")
    finally:
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)
