"""
系统API测试
"""
import pytest
import asyncio
from src.system_api import SystemAPI, get_system_api
from src.data.universe_manager import get_universe_manager


def test_system_api_initialization():
    """测试系统API初始化"""
    api = SystemAPI()
    assert api.data_api is not None
    assert api.universe_manager is not None


def test_get_system_api_singleton():
    """测试系统API单例模式"""
    api1 = get_system_api()
    api2 = get_system_api()
    
    assert api1 is api2


def test_get_last_universe():
    """测试get_last_universe接口"""
    api = SystemAPI()
    
    universe = api.get_last_universe()
    
    assert isinstance(universe, list)
    # 如果有数据，应该是字符串列表
    if universe:
        assert all(isinstance(s, str) for s in universe)
        # 应该是系统格式（小写+连字符）
        assert all('-' in s for s in universe)


def test_get_all_uid_info():
    """测试get_all_uid_info接口"""
    api = SystemAPI()
    
    info = api.get_all_uid_info()
    
    assert isinstance(info, dict)
    # 如果有数据，应该包含交易对信息
    if info:
        for symbol, symbol_info in info.items():
            assert isinstance(symbol_info, dict)
            assert 'symbol' in symbol_info
            assert 'exchange_symbol' in symbol_info
            assert 'status' in symbol_info


@pytest.mark.asyncio
async def test_get_account_info():
    """测试get_account_info接口"""
    api = SystemAPI()
    
    account_info = await api.get_account_info(exchange='binance', account='account1')
    
    assert isinstance(account_info, dict)
    # 应该包含账户信息字段
    assert 'exchange' in account_info
    assert 'account' in account_info
    # 如果有数据（不是错误），应该包含余额信息
    if 'error' not in account_info:
        assert 'total_wallet_balance' in account_info
        assert 'available_balance' in account_info


@pytest.mark.asyncio
async def test_get_position_info():
    """测试get_position_info接口"""
    api = SystemAPI()
    
    positions = await api.get_position_info(exchange='binance', account='account1')
    
    assert isinstance(positions, list)
    # 如果有持仓，应该包含持仓信息
    if positions:
        for pos in positions:
            assert isinstance(pos, dict)
            assert 'symbol' in pos
            assert 'position_amt' in pos


@pytest.mark.asyncio
async def test_get_active_order():
    """测试get_active_order接口"""
    api = SystemAPI()
    
    order_ids = await api.get_active_order(exchange='binance', account='account1')
    
    assert isinstance(order_ids, list)
    # 所有元素应该是整数（订单ID）
    assert all(isinstance(order_id, int) for order_id in order_ids)


@pytest.mark.asyncio
async def test_cancel_all_orders():
    """测试cancel_all_orders接口"""
    api = SystemAPI()
    
    result = await api.cancel_all_orders(exchange='binance', account='account1')
    
    assert isinstance(result, dict)
    assert 'exchange' in result
    assert 'account' in result
    # 应该包含取消结果
    assert 'cancelled_count' in result or 'total_cancelled' in result or 'error' in result
