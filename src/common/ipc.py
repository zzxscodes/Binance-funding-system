"""
进程间通信模块
支持TCP Socket和Unix Domain Socket
"""
import json
import asyncio
import socket
import platform
from typing import Dict, Any, Optional, Callable
from enum import Enum
from datetime import datetime, timezone

from .config import config
from .logger import get_logger

logger = get_logger('ipc')


class MessageType(Enum):
    """消息类型"""
    DATA_COMPLETE = "data_complete"
    TARGET_POSITION_READY = "target_position_ready"
    ORDER_EXECUTED = "order_executed"
    HEARTBEAT = "heartbeat"
    ERROR = "error"


class IPCServer:
    """IPC服务器"""
    
    def __init__(self, port: Optional[int] = None, socket_path: Optional[str] = None):
        self.port = port or config.get('ipc.tcp_port', 8888)
        self.socket_path = socket_path or config.get('ipc.socket_path', '/tmp/binance_quant.sock')
        self.socket_type = config.get('ipc.socket_type', 'tcp')
        self.message_handlers: Dict[MessageType, Callable] = {}
        self.server: Optional[asyncio.Server] = None
        self.running = False
        
        # Windows不支持Unix Domain Socket，强制使用TCP
        if platform.system() == 'Windows':
            self.socket_type = 'tcp'
    
    def register_handler(self, message_type: MessageType, handler: Callable):
        """注册消息处理器"""
        self.message_handlers[message_type] = handler
    
    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """处理客户端连接"""
        client_addr = writer.get_extra_info('peername')
        logger.info(f"Client connected: {client_addr}")
        
        try:
            while self.running:
                try:
                    # 读取消息长度（4字节）
                    length_data = await asyncio.wait_for(reader.read(4), timeout=30.0)
                    if not length_data:
                        break
                    
                    message_length = int.from_bytes(length_data, byteorder='big')
                    
                    # 读取消息内容
                    message_data = await asyncio.wait_for(
                        reader.read(message_length),
                        timeout=30.0
                    )
                    
                    if not message_data:
                        break
                    
                    # 解析消息
                    message = json.loads(message_data.decode('utf-8'))
                    await self._process_message(message, writer)
                    
                except asyncio.TimeoutError:
                    # 发送心跳
                    await self._send_message(writer, {
                        'type': MessageType.HEARTBEAT.value,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                except Exception as e:
                    logger.error(f"Error handling client {client_addr}: {e}", exc_info=True)
                    break
                    
        except Exception as e:
            logger.error(f"Client {client_addr} connection error: {e}", exc_info=True)
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Client disconnected: {client_addr}")
    
    async def _process_message(self, message: Dict[str, Any], writer: asyncio.StreamWriter):
        """处理收到的消息"""
        try:
            msg_type = MessageType(message.get('type'))
            handler = self.message_handlers.get(msg_type)
            
            if handler:
                response = await handler(message)
                if response:
                    await self._send_message(writer, response)
            else:
                logger.warning(f"No handler for message type: {msg_type}")
                
        except ValueError:
            logger.error(f"Unknown message type: {message.get('type')}")
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            await self._send_message(writer, {
                'type': MessageType.ERROR.value,
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
    
    async def _send_message(self, writer: asyncio.StreamWriter, message: Dict[str, Any]):
        """发送消息到客户端"""
        try:
            message_data = json.dumps(message).encode('utf-8')
            message_length = len(message_data).to_bytes(4, byteorder='big')
            
            writer.write(message_length + message_data)
            await writer.drain()
        except Exception as e:
            logger.error(f"Error sending message: {e}", exc_info=True)
    
    async def start(self):
        """启动服务器"""
        self.running = True
        
        if self.socket_type == 'tcp':
            self.server = await asyncio.start_server(
                self._handle_client,
                config.get('ipc.tcp_host', '127.0.0.1'),
                self.port
            )
            addr = self.server.sockets[0].getsockname()
            logger.info(f"IPC Server started on TCP {addr[0]}:{addr[1]}")
        else:
            # Unix Domain Socket
            if platform.system() != 'Windows':
                # 删除已存在的socket文件
                import os
                if os.path.exists(self.socket_path):
                    os.unlink(self.socket_path)
                
                self.server = await asyncio.start_unix_server(
                    self._handle_client,
                    self.socket_path
                )
                logger.info(f"IPC Server started on Unix Socket: {self.socket_path}")
            else:
                raise RuntimeError("Unix Domain Socket not supported on Windows")
        
        async with self.server:
            await self.server.serve_forever()
    
    async def stop(self):
        """停止服务器"""
        self.running = False
        if self.server:
            self.server.close()
            try:
                await self.server.wait_closed()
            except Exception as e:
                logger.warning(f"Error waiting for server to close: {e}")
            logger.info("IPC Server stopped")


class IPCClient:
    """IPC客户端"""
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None, 
                 socket_path: Optional[str] = None):
        self.host = host or config.get('ipc.tcp_host', '127.0.0.1')
        self.port = port or config.get('ipc.tcp_port', 8888)
        self.socket_path = socket_path or config.get('ipc.socket_path', '/tmp/binance_quant.sock')
        self.socket_type = config.get('ipc.socket_type', 'tcp')
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self._send_lock = asyncio.Lock()  # 保护send_message的并发调用
        
        # Windows不支持Unix Domain Socket
        if platform.system() == 'Windows':
            self.socket_type = 'tcp'
    
    async def connect(self):
        """连接到服务器"""
        try:
            if self.socket_type == 'tcp':
                self.reader, self.writer = await asyncio.open_connection(
                    self.host,
                    self.port
                )
                logger.info(f"Connected to IPC server at {self.host}:{self.port}")
            else:
                if platform.system() != 'Windows':
                    self.reader, self.writer = await asyncio.open_unix_connection(
                        self.socket_path
                    )
                    logger.info(f"Connected to IPC server at {self.socket_path}")
                else:
                    raise RuntimeError("Unix Domain Socket not supported on Windows")
        except Exception as e:
            logger.error(f"Failed to connect to IPC server: {e}")
            raise
    
    async def disconnect(self):
        """断开连接"""
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
            self.reader = None
            self.writer = None
            logger.info("Disconnected from IPC server")
    
    async def send_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """发送消息并等待响应（线程安全）"""
        if not self.writer:
            await self.connect()
        
        # 使用锁保护并发调用
        async with self._send_lock:
            try:
                # 发送消息
                message_data = json.dumps(message).encode('utf-8')
                message_length = len(message_data).to_bytes(4, byteorder='big')
                
                self.writer.write(message_length + message_data)
                await self.writer.drain()
                
                # 等待响应
                length_data = await asyncio.wait_for(self.reader.read(4), timeout=10.0)
                if not length_data:
                    return None
                
                message_length = int.from_bytes(length_data, byteorder='big')
                message_data = await asyncio.wait_for(
                    self.reader.read(message_length),
                    timeout=10.0
                )
                
                if not message_data:
                    return None
                
                response = json.loads(message_data.decode('utf-8'))
                return response
                
            except asyncio.TimeoutError:
                logger.warning("IPC message timeout")
                return None
            except Exception as e:
                logger.error(f"Error sending IPC message: {e}", exc_info=True)
                return None
    
    async def send_data_complete(self, timestamp: datetime, symbols: list[str]):
        """发送数据完成事件"""
        message = {
            'type': MessageType.DATA_COMPLETE.value,
            'timestamp': timestamp.isoformat(),
            'symbols': symbols,
        }
        return await self.send_message(message)
    
    async def send_target_position_ready(self, account_id: str, file_path: str):
        """发送目标持仓准备完成事件"""
        message = {
            'type': MessageType.TARGET_POSITION_READY.value,
            'account_id': account_id,
            'file_path': file_path,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        return await self.send_message(message)
    
    async def send_order_executed(self, account_id: str, order_info: Dict[str, Any]):
        """发送订单执行完成事件"""
        message = {
            'type': MessageType.ORDER_EXECUTED.value,
            'account_id': account_id,
            'order_info': order_info,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        return await self.send_message(message)
