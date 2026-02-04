"""
事件驱动协调进程（Process 1）
协调各进程之间的通信
- 监听数据层的数据完成事件，通知策略进程
- 接收策略进程的目标持仓文件，通知订单执行进程
"""
import asyncio
import signal
import sys
import os
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timezone

from ..common.config import config
from ..common.logger import get_logger
from ..common.ipc import IPCServer, MessageType
from ..common.utils import beijing_now
from ..monitoring.performance import get_performance_monitor

logger = get_logger('event_coordinator')


class EventCoordinator:
    """事件协调器"""
    
    def __init__(self):
        self.ipc_server = IPCServer()
        self.running = False
        
        # 性能监控
        self.performance_monitor = get_performance_monitor()

        # Windows 下 signal 文件删除/重命名可能因占用短暂失败（WinError 5/32），用锁避免并发写同一个文件
        self._signal_write_lock = asyncio.Lock()
        
        # 注册消息处理器
        self.ipc_server.register_handler(MessageType.DATA_COMPLETE, self._handle_data_complete)
        self.ipc_server.register_handler(MessageType.TARGET_POSITION_READY, self._handle_target_position_ready)
        self.ipc_server.register_handler(MessageType.ORDER_EXECUTED, self._handle_order_executed)
        
        # 状态跟踪
        self.last_data_complete_time: Optional[datetime] = None
        self.pending_target_positions: Dict[str, str] = {}  # account_id -> file_path

    async def _write_signal_file(self, signal_file: Path, signal_data: Dict, *, max_retries: int = 10) -> None:
        """
        以尽可能原子的方式写入信号文件，并对 Windows 文件占用/权限问题做重试。
        - 优先使用 temp + os.replace（覆盖写入）
        - 如多次失败，回退为直接覆盖写（避免完全丢通知）
        """
        import json

        signal_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = signal_file.with_suffix(signal_file.suffix + f".{os.getpid()}.tmp")

        async with self._signal_write_lock:
            last_exc: Optional[Exception] = None
            for attempt in range(max_retries):
                try:
                    # 1) 写临时文件
                    with open(tmp_path, 'w', encoding='utf-8') as f:
                        json.dump(signal_data, f, indent=2, ensure_ascii=False)
                        f.flush()
                        os.fsync(f.fileno())

                    # 2) 原子替换（比 unlink+rename 更稳）
                    os.replace(str(tmp_path), str(signal_file))
                    return
                except Exception as e:
                    last_exc = e

                    # 清理临时文件（可能残留）
                    try:
                        if tmp_path.exists():
                            tmp_path.unlink()
                    except Exception:
                        pass

                    winerr = getattr(e, "winerror", None)
                    retriable = isinstance(e, (PermissionError, OSError)) and (winerr in (5, 32, 33, 13, 145) or winerr is None)
                    if not retriable:
                        break

                    # 指数退避（最多 1s）
                    delay = min(0.05 * (2 ** attempt), 1.0)
                    await asyncio.sleep(delay)

            # fallback: 直接写入目标文件（非原子，但尽量不丢通知）
            try:
                with open(signal_file, 'w', encoding='utf-8') as f:
                    json.dump(signal_data, f, indent=2, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())
                return
            except Exception as e:
                raise RuntimeError(f"Failed to write signal file {signal_file}: {last_exc} / fallback: {e}") from e
    
    async def _handle_data_complete(self, message: Dict) -> Optional[Dict]:
        """
        处理数据完成事件
        收到后通知策略进程（Process 3）
        """
        with self.performance_monitor.measure(
            'event_coordinator',
            'message_handling',
            {'message_type': 'DATA_COMPLETE', 'symbols_count': len(message.get('symbols', []))}
        ):
            try:
                timestamp_str = message.get('timestamp')
                symbols = message.get('symbols', [])
                
                if timestamp_str:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                else:
                    timestamp = datetime.now(timezone.utc)
                
                self.last_data_complete_time = timestamp
                
                logger.info(
                    f"Data complete event received: {len(symbols)} symbols at {timestamp}"
                )
                
                # 通知策略进程
                with self.performance_monitor.measure(
                    'event_coordinator',
                    'strategy_notification',
                    {'symbols_count': len(symbols)}
                ):
                    await self._notify_strategy_process(symbols, timestamp)
                
                return {
                    'type': 'ack',
                    'message': 'Data complete event received',
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
            except Exception as e:
                logger.error(f"Error handling data complete event: {e}", exc_info=True)
                return {
                    'type': 'error',
                    'error': str(e)
                }
    
    async def _notify_strategy_process(self, symbols: List[str], timestamp: datetime):
        """
        通知策略进程开始计算
        可以通过文件信号、Socket消息或其他方式
        """
        try:
            # 方式1: 写入信号文件
            signals_dir = config.get('data.signals_directory', 'data/signals')
            signal_file = Path(signals_dir) / 'strategy_trigger.json'
            
            signal_data = {
                'event': 'data_complete',
                'timestamp': timestamp.isoformat(),
                'symbols': symbols,
                'triggered_at': datetime.now(timezone.utc).isoformat()
            }

            await self._write_signal_file(signal_file, signal_data)
            logger.info(f"Strategy process notified via signal file: {signal_file}")
            
            # 方式2: 可以通过环境变量或其他IPC机制通知
            # 这里我们使用文件信号，策略进程会监控这个文件
            
        except Exception as e:
            logger.error(f"Failed to notify strategy process: {e}", exc_info=True)
    
    async def _handle_target_position_ready(self, message: Dict) -> Optional[Dict]:
        """
        处理目标持仓准备完成事件
        收到后通知订单执行进程（Process 4）
        """
        try:
            account_id = message.get('account_id')
            file_path = message.get('file_path')
            timestamp_str = message.get('timestamp')
            
            if not account_id or not file_path:
                logger.error("Invalid target position ready message: missing account_id or file_path")
                return {
                    'type': 'error',
                    'error': 'Missing account_id or file_path'
                }
            
            # 保存待处理的目标持仓
            self.pending_target_positions[account_id] = file_path
            
            logger.info(
                f"Target position ready for account {account_id}: {file_path}"
            )
            
            # 通知订单执行进程
            await self._notify_execution_process(account_id, file_path)
            
            return {
                'type': 'ack',
                'message': f'Target position received for account {account_id}',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.error(f"Error handling target position ready event: {e}", exc_info=True)
            return {
                'type': 'error',
                'error': str(e)
            }
    
    async def _notify_execution_process(self, account_id: str, file_path: str):
        """
        通知订单执行进程
        """
        try:
            # 写入信号文件
            signals_dir = config.get('data.signals_directory', 'data/signals')
            signal_file = Path(signals_dir) / f'execution_trigger_{account_id}.json'
            
            signal_data = {
                'event': 'target_position_ready',
                'account_id': account_id,
                'file_path': file_path,
                'triggered_at': datetime.now(timezone.utc).isoformat()
            }

            await self._write_signal_file(signal_file, signal_data)
            logger.info(
                f"Execution process notified for account {account_id} via signal file: {signal_file}"
            )
            
        except Exception as e:
            logger.error(f"Failed to notify execution process: {e}", exc_info=True)
    
    async def _handle_order_executed(self, message: Dict) -> Optional[Dict]:
        """
        处理订单执行完成事件
        可以转发给监控进程（Process 5）
        """
        try:
            account_id = message.get('account_id')
            order_info = message.get('order_info', {})
            
            logger.info(
                f"Order executed event received for account {account_id}: "
                f"order_id={order_info.get('orderId', 'N/A')}"
            )
            
            # 可以在这里转发给监控进程或记录统计
            
            return {
                'type': 'ack',
                'message': 'Order executed event received',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.error(f"Error handling order executed event: {e}", exc_info=True)
            return {
                'type': 'error',
                'error': str(e)
            }
    
    async def start(self):
        """启动事件协调器"""
        if self.running:
            logger.warning("Event coordinator is already running")
            return
        
        logger.info("Starting event coordinator process...")
        self.running = True
        
        # 创建信号目录
        signals_dir = config.get('data.signals_directory', 'data/signals')
        Path(signals_dir).mkdir(parents=True, exist_ok=True)
        
        # 启动IPC服务器
        try:
            await self.ipc_server.start()
        except Exception as e:
            logger.error(f"Failed to start IPC server: {e}", exc_info=True)
            raise
    
    async def stop(self):
        """停止事件协调器"""
        logger.info("Stopping event coordinator process...")
        self.running = False
        
        if self.ipc_server:
            await self.ipc_server.stop()
        
        logger.info("Event coordinator stopped")
    
    def get_status(self) -> Dict:
        """获取协调器状态"""
        return {
            'running': self.running,
            'last_data_complete_time': (
                self.last_data_complete_time.isoformat() 
                if self.last_data_complete_time else None
            ),
            'pending_target_positions': self.pending_target_positions.copy(),
            'handlers_registered': len(getattr(self.ipc_server, "message_handlers", {}) or {}),
        }


async def main():
    """主函数"""
    coordinator = EventCoordinator()
    
    # 信号处理
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(coordinator.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await coordinator.start()
        
        # 保持运行
        while coordinator.running:
            await asyncio.sleep(1)
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Event coordinator process error: {e}", exc_info=True)
    finally:
        await coordinator.stop()


if __name__ == "__main__":
    asyncio.run(main())
