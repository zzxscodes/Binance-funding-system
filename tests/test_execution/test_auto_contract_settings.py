"""
测试自动合约设置功能
验证在订单执行前自动设置合约配置（对策略透明）
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_auto_ensure_contract_settings_on_first_order():
    """测试首次下单时自动设置合约配置"""
    from src.execution.binance_client import BinanceClient
    from src.common.config import config
    
    # 保存原始配置
    original_margin_type = config.get('execution.contract_settings.margin_type', 'CROSSED')
    original_leverage = config.get('execution.contract_settings.leverage', 20)
    
    try:
        # 设置测试配置
        config.set('execution.contract_settings.margin_type', 'CROSSED')
        config.set('execution.contract_settings.leverage', 20)
        
        # 创建mock客户端
        client = BinanceClient('test_key', 'test_secret', api_base='https://testnet.binancefuture.com')
        
        # Mock方法
        with patch.object(client, 'change_margin_type', new_callable=AsyncMock) as mock_margin, \
             patch.object(client, 'change_leverage', new_callable=AsyncMock) as mock_leverage, \
             patch.object(client, 'verify_contract_settings', new_callable=AsyncMock) as mock_verify, \
             patch.object(client, '_request', new_callable=AsyncMock) as mock_request:
            
            # 设置验证返回成功
            mock_verify.return_value = {
                'margin_type_ok': True,
                'leverage_ok': True,
                'current_margin_type': 'CROSSED',
                'current_leverage': 20
            }
            
            # 设置下单请求返回
            mock_request.return_value = {
                'orderId': 12345,
                'status': 'FILLED',
                'symbol': 'BTCUSDT'
            }
            
            # 首次下单，应该自动设置合约配置
            result = await client.place_order(
                symbol='BTCUSDT',
                side='BUY',
                order_type='MARKET',
                quantity=0.1
            )
            
            # 验证调用了设置方法
            mock_margin.assert_called_once_with('BTCUSDT', 'CROSSED')
            mock_leverage.assert_called_once_with('BTCUSDT', 20)
            mock_verify.assert_called()
            
            # 验证缓存已更新
            assert 'BTCUSDT' in client._contract_settings_cache
            assert client._contract_settings_cache['BTCUSDT']['verified'] is True
            
            print("[OK] Auto contract settings applied on first order")
            
    finally:
        # 恢复原始配置
        config.set('execution.contract_settings.margin_type', original_margin_type)
        config.set('execution.contract_settings.leverage', original_leverage)


@pytest.mark.asyncio
async def test_auto_ensure_contract_settings_skip_if_verified():
    """测试如果配置已验证，则跳过设置"""
    from src.execution.binance_client import BinanceClient
    from src.common.config import config
    
    # 保存原始配置
    original_margin_type = config.get('execution.contract_settings.margin_type', 'CROSSED')
    original_leverage = config.get('execution.contract_settings.leverage', 20)
    
    try:
        # 设置测试配置
        config.set('execution.contract_settings.margin_type', 'CROSSED')
        config.set('execution.contract_settings.leverage', 20)
        
        # 创建mock客户端
        client = BinanceClient('test_key', 'test_secret', api_base='https://testnet.binancefuture.com')
        
        # 预先设置缓存（模拟已设置过）
        client._contract_settings_cache['BTCUSDT'] = {
            'margin_type': 'CROSSED',
            'leverage': 20,
            'verified': True
        }
        
        # Mock方法
        with patch.object(client, 'change_margin_type', new_callable=AsyncMock) as mock_margin, \
             patch.object(client, 'change_leverage', new_callable=AsyncMock) as mock_leverage, \
             patch.object(client, 'verify_contract_settings', new_callable=AsyncMock) as mock_verify, \
             patch.object(client, '_request', new_callable=AsyncMock) as mock_request:
            
            # 设置验证返回成功（配置仍然正确）
            mock_verify.return_value = {
                'margin_type_ok': True,
                'leverage_ok': True,
                'current_margin_type': 'CROSSED',
                'current_leverage': 20
            }
            
            # 设置下单请求返回
            mock_request.return_value = {
                'orderId': 12345,
                'status': 'FILLED',
                'symbol': 'BTCUSDT'
            }
            
            # 下单，应该跳过设置（因为已验证）
            result = await client.place_order(
                symbol='BTCUSDT',
                side='BUY',
                order_type='MARKET',
                quantity=0.1
            )
            
            # 验证没有调用设置方法（因为已验证）
            mock_margin.assert_not_called()
            mock_leverage.assert_not_called()
            # 但应该验证一次（检查配置是否仍然正确）
            assert mock_verify.call_count >= 1
            
            print("[OK] Auto contract settings skipped when already verified")
            
    finally:
        # 恢复原始配置
        config.set('execution.contract_settings.margin_type', original_margin_type)
        config.set('execution.contract_settings.leverage', original_leverage)


@pytest.mark.asyncio
async def test_auto_ensure_contract_settings_transparent_to_strategy():
    """测试合约设置对策略完全透明"""
    from src.execution.binance_client import BinanceClient
    from src.common.config import config
    
    # 保存原始配置
    original_margin_type = config.get('execution.contract_settings.margin_type', 'CROSSED')
    original_leverage = config.get('execution.contract_settings.leverage', 20)
    
    try:
        # 设置测试配置
        config.set('execution.contract_settings.margin_type', 'CROSSED')
        config.set('execution.contract_settings.leverage', 20)
        
        # 创建mock客户端
        client = BinanceClient('test_key', 'test_secret', api_base='https://testnet.binancefuture.com')
        
        # Mock方法
        with patch.object(client, 'change_margin_type', new_callable=AsyncMock) as mock_margin, \
             patch.object(client, 'change_leverage', new_callable=AsyncMock) as mock_leverage, \
             patch.object(client, 'verify_contract_settings', new_callable=AsyncMock) as mock_verify, \
             patch.object(client, '_request', new_callable=AsyncMock) as mock_request:
            
            # 设置验证返回成功
            mock_verify.return_value = {
                'margin_type_ok': True,
                'leverage_ok': True,
                'current_margin_type': 'CROSSED',
                'current_leverage': 20
            }
            
            # 设置下单请求返回
            mock_request.return_value = {
                'orderId': 12345,
                'status': 'FILLED',
                'symbol': 'BTCUSDT'
            }
            
            # 策略代码只需要调用place_order，不需要关心合约设置
            # 系统自动处理合约配置
            result = await client.place_order(
                symbol='BTCUSDT',
                side='BUY',
                order_type='MARKET',
                quantity=0.1
            )
            
            # 验证订单返回正常（策略可以正常使用）
            assert result['orderId'] == 12345
            assert result['status'] == 'FILLED'
            
            # 验证合约设置已自动完成（对策略透明）
            assert 'BTCUSDT' in client._contract_settings_cache
            
            print("[OK] Contract settings are transparent to strategy")
            
    finally:
        # 恢复原始配置
        config.set('execution.contract_settings.margin_type', original_margin_type)
        config.set('execution.contract_settings.leverage', original_leverage)
