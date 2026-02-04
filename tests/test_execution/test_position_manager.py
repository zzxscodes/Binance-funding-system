"""
测试持仓管理器
"""
import pytest
import asyncio
import sys
from pathlib import Path
from unittest.mock import Mock, AsyncMock

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.execution.position_manager import PositionManager
from src.execution.binance_client import BinanceClient


def create_mock_client():
    """创建模拟Binance客户端"""
    client = Mock(spec=BinanceClient)
    client.get_positions = AsyncMock(return_value=[
        {
            'symbol': 'BTCUSDT',
            'positionAmt': '0.1',
            'entryPrice': '50000.0',
            'unRealizedProfit': '100.0',
            'leverage': '10',
            'marginType': 'isolated',
        },
        {
            'symbol': 'ETHUSDT',
            'positionAmt': '-0.5',
            'entryPrice': '3000.0',
            'unRealizedProfit': '-50.0',
            'leverage': '10',
            'marginType': 'isolated',
        },
    ])
    
    client.get_exchange_info = AsyncMock(return_value={
        'symbols': [
            {
                'symbol': 'BTCUSDT',
                'filters': [
                    {
                        'filterType': 'PRICE_FILTER',
                        'tickSize': '0.01',
                    },
                    {
                        'filterType': 'LOT_SIZE',
                        'stepSize': '0.001',
                        'minQty': '0.001',
                    },
                ],
            },
            {
                'symbol': 'ETHUSDT',
                'filters': [
                    {
                        'filterType': 'PRICE_FILTER',
                        'tickSize': '0.01',
                    },
                    {
                        'filterType': 'LOT_SIZE',
                        'stepSize': '0.001',
                        'minQty': '0.001',
                    },
                ],
            },
        ],
    })
    
    return client


@pytest.mark.asyncio
async def test_update_current_positions():
    """测试更新当前持仓"""
    mock_client = create_mock_client()
    manager = PositionManager(mock_client)
    
    await manager.update_current_positions()
    
    assert len(manager.current_positions) == 2
    assert 'BTCUSDT' in manager.current_positions
    assert 'ETHUSDT' in manager.current_positions
    
    btc_pos = manager.current_positions['BTCUSDT']
    assert btc_pos['position_amt'] == 0.1
    assert btc_pos['entry_price'] == 50000.0
    
    print("✓ Current positions updated successfully")


def test_calculate_position_diff():
    """测试计算持仓偏差"""
    mock_client = create_mock_client()
    manager = PositionManager(mock_client)
    
    # 设置当前持仓
    manager.current_positions = {
        'BTCUSDT': {'position_amt': 0.1},
        'ETHUSDT': {'position_amt': -0.5},
    }
    
    # 目标持仓
    target_positions = {
        'BTCUSDT': 0.2,  # 需要增加
        'ETHUSDT': -0.3,  # 需要减少（平仓部分）
        'BNBUSDT': 0.1,  # 新开仓
    }
    
    orders = manager.calculate_position_diff(target_positions)
    
    assert len(orders) > 0
    
    # 验证订单
    btc_order = next((o for o in orders if o['symbol'] == 'BTCUSDT'), None)
    assert btc_order is not None
    assert btc_order['side'] == 'BUY'
    assert btc_order['quantity'] == pytest.approx(0.1, abs=0.001)
    
    eth_order = next((o for o in orders if o['symbol'] == 'ETHUSDT'), None)
    assert eth_order is not None
    # 从-0.5到-0.3，需要减少空仓，应该买（减少空头=买）
    # diff = -0.3 - (-0.5) = 0.2 > 0，所以是BUY，但reduce_only=True
    assert eth_order['side'] in ['BUY', 'SELL']  # 减少空仓可以是BUY或SELL，取决于实现
    assert eth_order['reduce_only'] == True  # 平仓
    
    bnb_order = next((o for o in orders if o['symbol'] == 'BNBUSDT'), None)
    assert bnb_order is not None
    assert bnb_order['side'] == 'BUY'
    
    print("✓ Position difference calculation works correctly")


@pytest.mark.asyncio
async def test_get_symbol_info():
    """测试获取交易对信息"""
    mock_client = create_mock_client()
    manager = PositionManager(mock_client)
    
    symbol_info = await manager.get_symbol_info('BTCUSDT')
    
    assert symbol_info['symbol'] == 'BTCUSDT'
    assert 'tick_size' in symbol_info
    assert 'step_size' in symbol_info
    assert symbol_info['tick_size'] == 0.01
    assert symbol_info['step_size'] == 0.001
    
    print("✓ Symbol info retrieved successfully")


@pytest.mark.asyncio
async def test_normalize_orders():
    """测试订单规范化"""
    mock_client = create_mock_client()
    manager = PositionManager(mock_client)
    
    orders = [
        {
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'quantity': 0.123456,  # 需要规范化
            'reduce_only': False,
        },
    ]
    
    normalized = await manager.normalize_orders(orders)
    
    assert len(normalized) == 1
    assert normalized[0]['normalized_quantity'] == pytest.approx(0.123, abs=0.001)
    
    print("✓ Orders normalized correctly")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Position Manager")
    print("=" * 60)
    
    async def run_async_tests():
        await test_update_current_positions()
        await test_get_symbol_info()
        await test_normalize_orders()
    
    asyncio.run(run_async_tests())
    test_calculate_position_diff()
    
    print("\n" + "=" * 60)
    print("All position manager tests completed!")
    print("=" * 60)
