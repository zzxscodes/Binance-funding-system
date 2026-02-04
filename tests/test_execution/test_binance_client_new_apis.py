"""
BinanceClient新接口测试
"""
import pytest
import asyncio
from src.execution.binance_client import BinanceClient
from src.execution.dry_run_client import DryRunBinanceClient


@pytest.mark.asyncio
async def test_get_open_orders_dry_run():
    """测试查询活跃订单（dry-run模式）"""
    client = DryRunBinanceClient()
    
    orders = await client.get_open_orders()
    
    assert isinstance(orders, list)
    # dry-run模式下应该返回空列表


@pytest.mark.asyncio
async def test_cancel_all_orders_dry_run():
    """测试取消所有订单（dry-run模式）"""
    client = DryRunBinanceClient()
    
    result = await client.cancel_all_orders('BTCUSDT')
    
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_change_position_mode_dry_run():
    """测试设置合约持仓模式（dry-run模式）"""
    client = DryRunBinanceClient()
    
    result = await client.change_position_mode(dual_side_position=True)
    
    assert isinstance(result, dict)
    assert 'code' in result or 'msg' in result


@pytest.mark.asyncio
async def test_change_leverage_dry_run():
    """测试设置合约杠杆倍数（dry-run模式）"""
    client = DryRunBinanceClient()
    
    result = await client.change_leverage('BTCUSDT', leverage=10)
    
    assert isinstance(result, dict)
    assert 'leverage' in result
    assert result['leverage'] == 10


@pytest.mark.asyncio
async def test_change_margin_type_dry_run():
    """测试设置合约保证金模式（dry-run模式）"""
    client = DryRunBinanceClient()
    
    result = await client.change_margin_type('BTCUSDT', margin_type='ISOLATED')
    
    assert isinstance(result, dict)
    assert 'code' in result or 'msg' in result
