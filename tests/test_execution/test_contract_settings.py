"""
合约设置测试
"""
import pytest
import asyncio
from src.execution.binance_client import BinanceClient
from src.execution.dry_run_client import DryRunBinanceClient
from src.common.config import config


@pytest.mark.asyncio
async def test_contract_settings_config():
    """测试合约设置配置读取"""
    contract_settings = config.get('execution.contract_settings', {})
    
    assert 'margin_type' in contract_settings
    assert 'position_mode' in contract_settings
    assert 'leverage' in contract_settings
    
    # 验证默认值
    assert contract_settings.get('margin_type') == 'CROSSED'
    assert contract_settings.get('position_mode') == 'one_way'
    assert contract_settings.get('leverage') == 20


@pytest.mark.asyncio
async def test_change_position_mode_dry_run():
    """测试设置持仓模式（dry-run模式）"""
    client = DryRunBinanceClient()
    
    # 测试单向持仓（默认）
    result = await client.change_position_mode(dual_side_position=False)
    assert isinstance(result, dict)
    
    # 测试双向持仓
    result = await client.change_position_mode(dual_side_position=True)
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_change_margin_type_dry_run():
    """测试设置保证金模式（dry-run模式）"""
    client = DryRunBinanceClient()
    
    # 测试全仓（默认）
    result = await client.change_margin_type('BTCUSDT', margin_type='CROSSED')
    assert isinstance(result, dict)
    
    # 测试逐仓
    result = await client.change_margin_type('BTCUSDT', margin_type='ISOLATED')
    assert isinstance(result, dict)


@pytest.mark.asyncio
async def test_change_leverage_dry_run():
    """测试设置杠杆倍数（dry-run模式）"""
    client = DryRunBinanceClient()
    
    # 测试20倍杠杆（默认）
    result = await client.change_leverage('BTCUSDT', leverage=20)
    assert isinstance(result, dict)
    assert result.get('leverage') == 20
    
    # 测试其他杠杆倍数
    result = await client.change_leverage('ETHUSDT', leverage=10)
    assert result.get('leverage') == 10
