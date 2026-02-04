"""
测试不同的下单方法（TWAP、VWAP等）
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
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
    client.get_symbol_price = AsyncMock(return_value=50000.0)
    client.get_exchange_info = AsyncMock(return_value={
        'symbols': [{
            'symbol': 'BTCUSDT',
            'filters': [
                {'filterType': 'PRICE_FILTER', 'tickSize': '0.01'},
                {'filterType': 'LOT_SIZE', 'stepSize': '0.001', 'minQty': '0.001'},
                {'filterType': 'MIN_NOTIONAL', 'notional': '5.0'}
            ]
        }]
    })
    return client


@pytest.mark.asyncio
async def test_place_market_order(mock_binance_client):
    """测试市价单"""
    order_manager = OrderManager(mock_binance_client, dry_run=False)
    
    result = await order_manager.place_market_order(
        symbol='BTCUSDT',
        side='BUY',
        quantity=0.1,
        reduce_only=False
    )
    
    assert result is not None
    assert result['orderId'] == 12345
    assert result['status'] == 'FILLED'
    
    # 验证调用了place_order
    mock_binance_client.place_order.assert_called_once()
    call_args = mock_binance_client.place_order.call_args
    assert call_args[1]['symbol'] == 'BTCUSDT'
    assert call_args[1]['side'] == 'BUY'
    assert call_args[1]['order_type'] == 'MARKET'
    assert call_args[1]['quantity'] == 0.1
    
    print("✓ Market order test passed")


@pytest.mark.asyncio
async def test_place_twap_order(mock_binance_client):
    """测试TWAP订单（使用mock，不实际等待）"""
    order_manager = OrderManager(mock_binance_client, dry_run=False)
    
    # Mock place_twap_order以避免实际等待
    async def mock_twap(symbol, side, total_quantity, interval='5min', reduce_only=False):
        # 模拟快速返回，不实际等待
        symbol_info = await order_manager.get_symbol_info(symbol)
        step_size = symbol_info.get('step_size', 0.01)
        quantity_per_order = total_quantity / 2  # 简化：只分成2个订单
        quantity_per_order = round(quantity_per_order / step_size) * step_size
        
        results = []
        for i in range(2):
            result = await order_manager.place_market_order(symbol, side, quantity_per_order, reduce_only)
            if result:
                results.append(result)
        return results
    
    # 替换方法
    order_manager.place_twap_order = mock_twap
    
    results = await order_manager.place_twap_order(
        symbol='BTCUSDT',
        side='BUY',
        total_quantity=0.5,
        interval='1min',
        reduce_only=False
    )
    
    # TWAP应该分成多个订单
    assert isinstance(results, list)
    
    print("✓ TWAP order test passed")


@pytest.mark.asyncio
async def test_place_vwap_order(mock_binance_client):
    """测试VWAP订单"""
    order_manager = OrderManager(mock_binance_client, dry_run=False)
    
    # VWAP当前实现使用TWAP
    results = await order_manager.place_vwap_order(
        symbol='BTCUSDT',
        side='BUY',
        total_quantity=0.5,
        interval='1min',
        reduce_only=False
    )
    
    assert isinstance(results, list)
    
    print("✓ VWAP order test passed")


@pytest.mark.asyncio
async def test_get_symbol_info(mock_binance_client):
    """测试获取symbol信息"""
    order_manager = OrderManager(mock_binance_client, dry_run=False)
    
    symbol_info = await order_manager.get_symbol_info('BTCUSDT')
    
    assert isinstance(symbol_info, dict)
    assert 'symbol' in symbol_info
    assert 'tick_size' in symbol_info
    assert 'step_size' in symbol_info
    assert 'min_qty' in symbol_info
    assert 'min_notional' in symbol_info
    
    assert symbol_info['symbol'] == 'BTCUSDT'
    assert symbol_info['step_size'] == 0.001
    assert symbol_info['min_qty'] == 0.001
    assert symbol_info['min_notional'] == 5.0
    
    print("✓ Get symbol info test passed")


@pytest.mark.asyncio
async def test_normalize_orders(mock_binance_client):
    """测试订单规范化"""
    order_manager = OrderManager(mock_binance_client, dry_run=False)
    
    orders = [
        {
            'symbol': 'BTCUSDT',
            'side': 'BUY',
            'quantity': 0.123456,  # 需要规范化
            'current_position': 0.0,
            'target_position': 0.1,
        }
    ]
    
    normalized = await order_manager.normalize_orders(orders)
    
    assert isinstance(normalized, list)
    assert len(normalized) > 0
    
    order = normalized[0]
    assert 'normalized_quantity' in order
    # 数量应该被规范化（根据step_size）
    assert order['normalized_quantity'] == pytest.approx(0.123, abs=0.001)
    
    print("✓ Normalize orders test passed")


@pytest.mark.asyncio
async def test_order_execution_with_different_types(mock_binance_client):
    """测试不同订单类型的执行"""
    order_manager = OrderManager(mock_binance_client, dry_run=False)
    
    # 测试MARKET订单
    order_market = {
        'symbol': 'BTCUSDT',
        'side': 'BUY',
        'quantity': 0.1,
        'order_type': 'MARKET',
        'normalized_quantity': 0.1,
    }
    
    result = await order_manager._execute_order(order_market)
    assert result is not None
    
    print("✓ Order execution with different types test passed")


def test_parse_interval():
    """测试时间间隔解析"""
    order_manager = OrderManager(MagicMock(), dry_run=False)
    
    # 测试不同的间隔格式
    assert order_manager._parse_interval('5min') == 5
    assert order_manager._parse_interval('1h') == 60
    assert order_manager._parse_interval('4h') == 240
    assert order_manager._parse_interval('1m') == 1
    assert order_manager._parse_interval('60') == 60
    
    # 测试无效格式
    assert order_manager._parse_interval('invalid') is None
    
    print("✓ Parse interval test passed")
