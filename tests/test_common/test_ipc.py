"""
IPC通信模块测试
"""
import pytest
import asyncio
from datetime import datetime, timezone
from src.common.ipc import IPCServer, IPCClient, MessageType


@pytest.mark.asyncio
async def test_ipc_server_initialization():
    """测试IPC服务器初始化"""
    server = IPCServer(port=8889)
    assert server.port == 8889
    assert server.running is False
    assert len(server.message_handlers) == 0


@pytest.mark.asyncio
async def test_ipc_client_initialization():
    """测试IPC客户端初始化"""
    client = IPCClient(host='127.0.0.1', port=8889)
    assert client.host == '127.0.0.1'
    assert client.port == 8889
    assert client.reader is None
    assert client.writer is None


@pytest.mark.asyncio
async def test_ipc_server_client_communication():
    """测试IPC服务器和客户端通信"""
    server = IPCServer(port=8890)
    
    # 注册消息处理器
    received_messages = []
    
    async def handle_data_complete(message):
        received_messages.append(message)
        return {'status': 'ok', 'message': 'received'}
    
    server.register_handler(MessageType.DATA_COMPLETE, handle_data_complete)
    
    # 启动服务器（在后台任务中）
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)  # 等待服务器启动
    
    try:
        # 客户端连接并发送消息
        client = IPCClient(host='127.0.0.1', port=8890)
        await client.connect()
        
        message = {
            'type': MessageType.DATA_COMPLETE.value,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'symbols': ['BTCUSDT', 'ETHUSDT']
        }
        
        response = await client.send_message(message)
        
        assert response is not None
        assert response.get('status') == 'ok'
        assert len(received_messages) == 1
        assert received_messages[0]['type'] == MessageType.DATA_COMPLETE.value
        
        await client.disconnect()
        
    finally:
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_ipc_message_types():
    """测试各种消息类型"""
    server = IPCServer(port=8891)
    
    handlers_called = {}
    
    async def handle_target_position(message):
        handlers_called['target_position'] = True
        return {'status': 'ok'}
    
    async def handle_order_executed(message):
        handlers_called['order_executed'] = True
        return {'status': 'ok'}
    
    server.register_handler(MessageType.TARGET_POSITION_READY, handle_target_position)
    server.register_handler(MessageType.ORDER_EXECUTED, handle_order_executed)
    
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)
    
    try:
        client = IPCClient(host='127.0.0.1', port=8891)
        await client.connect()
        
        # 测试TARGET_POSITION_READY消息
        message1 = {
            'type': MessageType.TARGET_POSITION_READY.value,
            'account_id': 'account1',
            'file_path': '/path/to/positions.json',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        await client.send_message(message1)
        await asyncio.sleep(0.2)
        
        # 测试ORDER_EXECUTED消息
        message2 = {
            'type': MessageType.ORDER_EXECUTED.value,
            'account_id': 'account1',
            'order_info': {'order_id': 12345},
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        await client.send_message(message2)
        await asyncio.sleep(0.2)
        
        assert handlers_called.get('target_position') is True
        assert handlers_called.get('order_executed') is True
        
        await client.disconnect()
        
    finally:
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_ipc_client_helper_methods():
    """测试IPC客户端辅助方法"""
    server = IPCServer(port=8892)
    
    received_messages = []
    
    async def handle_message(message):
        received_messages.append(message)
        return {'status': 'ok'}
    
    server.register_handler(MessageType.DATA_COMPLETE, handle_message)
    server.register_handler(MessageType.TARGET_POSITION_READY, handle_message)
    server.register_handler(MessageType.ORDER_EXECUTED, handle_message)
    
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)
    
    try:
        client = IPCClient(host='127.0.0.1', port=8892)
        await client.connect()
        
        # 测试send_data_complete
        timestamp = datetime.now(timezone.utc)
        await client.send_data_complete(timestamp, ['BTCUSDT'])
        await asyncio.sleep(0.2)
        
        # 测试send_target_position_ready
        await client.send_target_position_ready('account1', '/path/to/file.json')
        await asyncio.sleep(0.2)
        
        # 测试send_order_executed
        await client.send_order_executed('account1', {'order_id': 12345})
        await asyncio.sleep(0.2)
        
        assert len(received_messages) == 3
        assert received_messages[0]['type'] == MessageType.DATA_COMPLETE.value
        assert received_messages[1]['type'] == MessageType.TARGET_POSITION_READY.value
        assert received_messages[2]['type'] == MessageType.ORDER_EXECUTED.value
        
        await client.disconnect()
        
    finally:
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_ipc_server_stop():
    """测试IPC服务器停止"""
    server = IPCServer(port=8893)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)
    
    assert server.running is True
    
    await server.stop()
    await asyncio.sleep(0.2)
    
    assert server.running is False
    
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass
