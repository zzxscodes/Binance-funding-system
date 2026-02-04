"""
测试合约设置验证功能
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_verify_contract_settings_success():
    """测试验证合约设置成功的情况"""
    from src.execution.binance_client import BinanceClient
    
    # 创建mock客户端
    client = BinanceClient('test_key', 'test_secret', api_base='https://testnet.binancefuture.com')
    
    # Mock get_position_risk方法
    mock_position_risk = [
        {
            'symbol': 'BTCUSDT',
            'marginType': 'CROSSED',
            'leverage': 20,
            'positionAmt': '0'
        }
    ]
    
    with patch.object(client, 'get_position_risk', new_callable=AsyncMock) as mock_get_risk:
        mock_get_risk.return_value = mock_position_risk
        
        result = await client.verify_contract_settings('BTCUSDT', 'CROSSED', 20)
        
        assert result['margin_type_ok'] is True
        assert result['leverage_ok'] is True
        assert result['current_margin_type'] == 'CROSSED'
        assert result['current_leverage'] == 20
        print("[OK] Contract settings verification works correctly for successful case")


@pytest.mark.asyncio
async def test_verify_contract_settings_failure():
    """测试验证合约设置失败的情况"""
    from src.execution.binance_client import BinanceClient
    
    # 创建mock客户端
    client = BinanceClient('test_key', 'test_secret', api_base='https://testnet.binancefuture.com')
    
    # Mock get_position_risk方法 - 返回不符合要求的设置
    mock_position_risk = [
        {
            'symbol': 'BTCUSDT',
            'marginType': 'ISOLATED',  # 不符合要求（应该是CROSSED）
            'leverage': 10,  # 不符合要求（应该是20）
            'positionAmt': '0'
        }
    ]
    
    with patch.object(client, 'get_position_risk', new_callable=AsyncMock) as mock_get_risk:
        mock_get_risk.return_value = mock_position_risk
        
        result = await client.verify_contract_settings('BTCUSDT', 'CROSSED', 20)
        
        assert result['margin_type_ok'] is False
        assert result['leverage_ok'] is False
        assert result['current_margin_type'] == 'ISOLATED'
        assert result['current_leverage'] == 10
        print("[OK] Contract settings verification correctly detects failures")


@pytest.mark.asyncio
async def test_verify_contract_settings_symbol_not_found():
    """测试验证合约设置时交易对不存在的情况"""
    from src.execution.binance_client import BinanceClient
    
    # 创建mock客户端
    client = BinanceClient('test_key', 'test_secret', api_base='https://testnet.binancefuture.com')
    
    # Mock get_position_risk方法 - 返回空列表
    with patch.object(client, 'get_position_risk', new_callable=AsyncMock) as mock_get_risk:
        mock_get_risk.return_value = []
        
        result = await client.verify_contract_settings('INVALIDUSDT', 'CROSSED', 20)
        
        assert result['margin_type_ok'] is False
        assert result['leverage_ok'] is False
        assert 'error' in result
        print("[OK] Contract settings verification handles missing symbols correctly")
