"""
测试完全离线的Dry-Run客户端
不需要API密钥，完全模拟所有操作
"""
import pytest
import asyncio
from src.execution.dry_run_client import DryRunBinanceClient


@pytest.mark.asyncio
async def test_dry_run_client_initialization():
    """测试Dry-Run客户端初始化（不需要API密钥）"""
    client = DryRunBinanceClient()
    
    assert client.dry_run_mode is True
    assert client.api_key == "DRY_RUN_KEY"
    assert client.api_secret == "DRY_RUN_SECRET"
    
    print("✓ Dry-run client initialized without API keys")


@pytest.mark.asyncio
async def test_dry_run_get_account_info():
    """测试获取账户信息（模拟）"""
    client = DryRunBinanceClient()
    
    account_info = await client.get_account_info()
    
    assert isinstance(account_info, dict)
    assert 'totalWalletBalance' in account_info
    assert account_info['totalWalletBalance'] == 100000.0
    
    print("✓ Dry-run get_account_info() works")


@pytest.mark.asyncio
async def test_dry_run_get_positions():
    """测试获取持仓（模拟）"""
    client = DryRunBinanceClient()
    
    positions = await client.get_positions()
    
    assert isinstance(positions, list)
    # 初始应该为空
    assert len(positions) == 0
    
    print("✓ Dry-run get_positions() works")


@pytest.mark.asyncio
async def test_dry_run_place_order():
    """测试下单（模拟，不实际下单）"""
    client = DryRunBinanceClient()
    
    # 下单
    result = await client.place_order(
        symbol='BTCUSDT',
        side='BUY',
        order_type='MARKET',
        quantity=0.1
    )
    
    assert isinstance(result, dict)
    assert 'orderId' in result
    assert result['status'] == 'FILLED'  # MARKET订单立即成交
    assert result['symbol'] == 'BTCUSDT'
    assert result['side'] == 'BUY'
    
    # 验证持仓已更新（dry-run模拟）
    positions = await client.get_positions()
    assert len(positions) > 0
    btc_position = next((p for p in positions if p['symbol'] == 'BTCUSDT'), None)
    assert btc_position is not None
    assert btc_position['positionAmt'] > 0  # 做多
    
    print("✓ Dry-run place_order() works and updates mock positions")


@pytest.mark.asyncio
async def test_dry_run_test_order():
    """测试test_order（与place_order相同）"""
    client = DryRunBinanceClient()
    
    result = await client.test_order(
        symbol='ETHUSDT',
        side='SELL',
        order_type='MARKET',
        quantity=1.0
    )
    
    assert isinstance(result, dict)
    assert 'orderId' in result
    assert result['status'] == 'FILLED'
    
    print("✓ Dry-run test_order() works")


@pytest.mark.asyncio
async def test_dry_run_get_exchange_info():
    """测试获取交易所信息（模拟）"""
    client = DryRunBinanceClient()
    
    exchange_info = await client.get_exchange_info()
    
    assert isinstance(exchange_info, dict)
    assert 'symbols' in exchange_info
    assert len(exchange_info['symbols']) > 0
    
    # 验证包含BTCUSDT
    btc_symbol = next((s for s in exchange_info['symbols'] if s['symbol'] == 'BTCUSDT'), None)
    assert btc_symbol is not None
    
    print("✓ Dry-run get_exchange_info() works")


@pytest.mark.asyncio
async def test_dry_run_cancel_order():
    """测试取消订单（模拟）"""
    client = DryRunBinanceClient()
    
    # 先下一个订单
    order_result = await client.place_order(
        symbol='BTCUSDT',
        side='BUY',
        order_type='LIMIT',
        quantity=0.1,
        price=50000.0
    )
    order_id = order_result['orderId']
    
    # 取消订单
    cancel_result = await client.cancel_order('BTCUSDT', order_id)
    
    assert cancel_result['orderId'] == order_id
    assert cancel_result['status'] == 'CANCELED'
    
    print("✓ Dry-run cancel_order() works")


@pytest.mark.asyncio
async def test_dry_run_get_order_status():
    """测试查询订单状态（模拟）"""
    client = DryRunBinanceClient()
    
    # 下一个订单
    order_result = await client.place_order(
        symbol='BTCUSDT',
        side='BUY',
        order_type='MARKET',
        quantity=0.1
    )
    order_id = order_result['orderId']
    
    # 查询订单状态
    status = await client.get_order_status('BTCUSDT', order_id)
    
    assert status['orderId'] == order_id
    assert 'status' in status
    
    print("✓ Dry-run get_order_status() works")


@pytest.mark.asyncio
async def test_dry_run_position_updates():
    """测试持仓更新逻辑（模拟）"""
    client = DryRunBinanceClient()
    
    # 初始持仓为空
    positions = await client.get_positions()
    assert len(positions) == 0
    
    # 做多
    await client.place_order('BTCUSDT', 'BUY', 'MARKET', quantity=0.1, price=50000.0)
    positions = await client.get_positions()
    assert len(positions) == 1
    assert positions[0]['positionAmt'] == 0.1
    
    # 继续做多（持仓增加）
    await client.place_order('BTCUSDT', 'BUY', 'MARKET', quantity=0.1, price=51000.0)
    positions = await client.get_positions()
    assert len(positions) == 1
    assert positions[0]['positionAmt'] == 0.2  # 持仓增加
    
    # 做空（减少持仓）
    await client.place_order('BTCUSDT', 'SELL', 'MARKET', quantity=0.15, price=52000.0)
    positions = await client.get_positions()
    assert len(positions) == 1
    assert abs(positions[0]['positionAmt'] - 0.05) < 1e-6  # 持仓减少（考虑浮点数精度）
    
    # 完全平仓
    await client.place_order('BTCUSDT', 'SELL', 'MARKET', quantity=0.05, price=53000.0)
    positions = await client.get_positions()
    assert len(positions) == 0  # 持仓归零
    
    print("✓ Dry-run position updates work correctly")


@pytest.mark.asyncio
async def test_dry_run_reset_mock_data():
    """测试重置模拟数据"""
    client = DryRunBinanceClient()
    
    # 创建一些持仓
    await client.place_order('BTCUSDT', 'BUY', 'MARKET', quantity=0.1)
    positions = await client.get_positions()
    assert len(positions) > 0
    
    # 重置
    client.reset_mock_data()
    positions = await client.get_positions()
    assert len(positions) == 0
    
    print("✓ Dry-run reset_mock_data() works")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Dry-Run Binance Client")
    print("=" * 60)
    
    asyncio.run(test_dry_run_client_initialization())
    asyncio.run(test_dry_run_get_account_info())
    asyncio.run(test_dry_run_get_positions())
    asyncio.run(test_dry_run_place_order())
    asyncio.run(test_dry_run_test_order())
    asyncio.run(test_dry_run_get_exchange_info())
    asyncio.run(test_dry_run_cancel_order())
    asyncio.run(test_dry_run_get_order_status())
    asyncio.run(test_dry_run_position_updates())
    asyncio.run(test_dry_run_reset_mock_data())
    
    print("\n" + "=" * 60)
    print("All dry-run client tests completed!")
    print("=" * 60)
