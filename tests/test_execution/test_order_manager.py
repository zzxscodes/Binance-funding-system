"""
订单管理器测试
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from src.execution.order_manager import OrderManager
from src.execution.binance_client import BinanceClient


@pytest.fixture
def mock_binance_client():
    """创建模拟的Binance客户端"""
    client = MagicMock(spec=BinanceClient)
    client.place_order = AsyncMock(return_value={
        'orderId': 12345,
        'status': 'FILLED',
        'symbol': 'BTCUSDT',
        'side': 'BUY',
        'executedQty': 0.1
    })
    client.get_order_status = AsyncMock(return_value={
        'orderId': 12345,
        'status': 'FILLED',
        'symbol': 'BTCUSDT'
    })
    client.cancel_order = AsyncMock(return_value={'status': 'CANCELED'})
    return client


@pytest.mark.asyncio
async def test_order_manager_initialization(mock_binance_client):
    """测试订单管理器初始化"""
    manager = OrderManager(mock_binance_client)
    
    assert manager.client is mock_binance_client
    assert manager.position_manager is not None
    assert len(manager.pending_orders) == 0
    assert len(manager.completed_orders) == 0


@pytest.mark.asyncio
async def test_execute_target_positions_no_diff(mock_binance_client):
    """测试执行目标持仓（无差异）"""
    manager = OrderManager(mock_binance_client)
    
    # 模拟position_manager返回空订单列表
    manager.position_manager.update_current_positions = AsyncMock()
    manager.position_manager.calculate_position_diff = MagicMock(return_value=[])
    
    result = await manager.execute_target_positions({'BTCUSDT': 0.1})
    
    assert isinstance(result, list)
    assert len(result) == 0


@pytest.mark.asyncio
async def test_execute_target_positions_with_orders(mock_binance_client):
    """测试执行目标持仓（有订单）"""
    manager = OrderManager(mock_binance_client)
    
    # 模拟position_manager返回订单
    manager.position_manager.update_current_positions = AsyncMock()
    manager.position_manager.calculate_position_diff = MagicMock(return_value=[
        {
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'quantity': 0.1,
            'current_position': 0.0,
            'target_position': 0.1
        }
    ])
    manager.position_manager.normalize_orders = AsyncMock(return_value=[
        {
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'normalized_quantity': 0.1,
            'order_type': 'MARKET',
            'reduce_only': False,
            'current_position': 0.0,
            'target_position': 0.1
        }
    ])
    
    result = await manager.execute_target_positions({'BTCUSDT': 0.1})
    
    assert isinstance(result, list)
    assert len(result) > 0
    assert result[0]['order_id'] == 12345
    assert result[0]['symbol'] == 'BTCUSDT'


@pytest.mark.asyncio
async def test_monitor_orders_empty(mock_binance_client):
    """测试监控订单（无待处理订单）"""
    manager = OrderManager(mock_binance_client)
    
    # 没有待处理订单，应该立即返回
    await manager.monitor_orders(timeout=1.0)


@pytest.mark.asyncio
async def test_monitor_orders_with_pending(mock_binance_client):
    """测试监控订单（有待处理订单）"""
    manager = OrderManager(mock_binance_client)
    
    # 添加一个待处理订单
    manager.pending_orders['12345'] = {
        'order_id': 12345,
        'symbol': 'BTCUSDT',
        'status': 'NEW'
    }
    
    # 监控应该检查订单状态
    await manager.monitor_orders(timeout=1.0)
    
    # 验证调用了get_order_status
    mock_binance_client.get_order_status.assert_called()


@pytest.mark.asyncio
async def test_cancel_all_pending_orders(mock_binance_client):
    """测试取消所有待处理订单"""
    manager = OrderManager(mock_binance_client)
    
    # 添加待处理订单
    manager.pending_orders['12345'] = {
        'order_id': 12345,
        'symbol': 'BTCUSDT',
        'status': 'NEW'
    }
    manager.pending_orders['12346'] = {
        'order_id': 12346,
        'symbol': 'ETHUSDT',
        'status': 'NEW'
    }
    
    await manager.cancel_all_pending_orders()
    
    # 验证调用了cancel_order
    assert mock_binance_client.cancel_order.call_count >= 1
    # 订单应该从pending_orders中移除
    assert len(manager.pending_orders) == 0


@pytest.mark.asyncio
async def test_cancel_orders_by_symbol(mock_binance_client):
    """测试按交易对取消订单"""
    manager = OrderManager(mock_binance_client)
    
    manager.pending_orders['12345'] = {
        'order_id': 12345,
        'symbol': 'BTCUSDT',
        'status': 'NEW'
    }
    manager.pending_orders['12346'] = {
        'order_id': 12346,
        'symbol': 'ETHUSDT',
        'status': 'NEW'
    }
    
    await manager.cancel_all_pending_orders(symbol='BTCUSDT')
    
    # 只应该取消BTCUSDT的订单
    assert '12345' not in manager.pending_orders
    # ETHUSDT的订单应该还在
    assert '12346' in manager.pending_orders


def test_get_order_statistics(mock_binance_client):
    """测试获取订单统计信息"""
    manager = OrderManager(mock_binance_client)
    
    manager.pending_orders['12345'] = {'order_id': 12345}
    manager.completed_orders.append({'order_id': 12346})
    
    stats = manager.get_order_statistics()
    
    assert stats['pending_orders_count'] == 1
    assert stats['completed_orders_count'] == 1
    assert '12345' in stats['pending_order_ids']
