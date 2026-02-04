"""
策略进程（Process 3）
按需启动，执行策略计算，生成目标持仓文件
"""
import asyncio
import signal
import sys
import json
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict

from ..common.config import config
from ..common.logger import get_logger
from ..common.ipc import IPCClient, MessageType
from ..common.utils import format_symbol
from ..monitoring.performance import get_performance_monitor
from ..data.api import get_data_api
from ..strategy.alpha import get_alpha_engine
from ..strategy.position_generator import get_position_generator

logger = get_logger('strategy_process')


class StrategyProcess:
    """策略进程"""
    
    def __init__(self):
        # 初始化数据API（需要K线聚合器，但策略进程可能只从存储读取）
        self.data_api = get_data_api()
        
        # 初始化Alpha引擎（新架构：alpha -> calculators -> weights）
        self.alpha_engine = get_alpha_engine()
        
        # 初始化目标持仓生成器
        self.position_generator = get_position_generator()
        
        # IPC客户端（用于通知事件协调进程）
        self.ipc_client: Optional[IPCClient] = None
        
        # 状态
        self.running = False
        self.last_calculation_time: Optional[datetime] = None
        self.is_calculating = False  # 标记是否正在执行策略计算
        
        # Universe缓存：避免频繁加载
        self._cached_universe: Optional[list[str]] = None
        self._universe_cache_time: Optional[datetime] = None
        self._universe_cache_ttl = config.get('strategy.process.universe_cache_ttl', 300)  # 默认5分钟缓存
        
        # 信号文件路径
        signals_dir = config.get('data.signals_directory', 'data/signals')
        self.trigger_file = Path(signals_dir) / 'strategy_trigger.json'
        
        # 性能监控
        self.performance_monitor = get_performance_monitor()
    
    async def _connect_ipc(self):
        """连接IPC服务器（事件协调进程）"""
        try:
            self.ipc_client = IPCClient()
            await self.ipc_client.connect()
            logger.info("Connected to event coordinator via IPC")
        except Exception as e:
            logger.warning(f"Failed to connect to event coordinator: {e}")
            logger.warning("Will continue without IPC notification")
    
    async def _wait_for_trigger(self, timeout: Optional[float] = None) -> Optional[Dict]:
        """
        等待策略触发信号
        
        Args:
            timeout: 超时时间（秒），如果不指定则使用配置值
        
        Returns:
            触发信号数据，如果超时返回None
        """
        if timeout is None:
            timeout = config.get('strategy.process.trigger_wait_timeout', 300.0)
        start_time = time.time()
        
        max_retries = config.get('strategy.process.trigger_file_max_retries', 3)
        retry_delay = config.get('strategy.process.trigger_file_retry_delay', 0.5)
        check_interval = config.get('strategy.process.trigger_wait_interval', 1.0)
        
        while (time.time() - start_time) < timeout:
            # 检查信号文件
            if self.trigger_file.exists():
                try:
                    # 使用重试机制处理文件锁定问题
                    for attempt in range(max_retries):
                        try:
                            with open(self.trigger_file, 'r', encoding='utf-8') as f:
                                content = f.read().strip()
                                
                            # 检查文件内容是否为空
                            if not content:
                                logger.debug(f"Trigger file is empty, waiting...")
                                await asyncio.sleep(retry_delay)
                                continue
                            
                            # 解析JSON
                            try:
                                trigger_data = json.loads(content)
                            except json.JSONDecodeError as json_e:
                                logger.warning(
                                    f"Invalid JSON in trigger file (attempt {attempt + 1}/{max_retries}): {json_e}. "
                                    f"Content preview: {content[:100]}"
                                )
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(retry_delay)
                                    continue
                                else:
                                    # 最后一次尝试失败，删除损坏的文件
                                    try:
                                        self.trigger_file.unlink()
                                    except Exception:
                                        pass
                                    return None
                            
                            # 删除信号文件（避免重复处理）
                            try:
                                self.trigger_file.unlink()
                            except Exception as unlink_e:
                                # 如果删除失败（可能被其他进程占用），记录警告但继续
                                logger.warning(f"Failed to delete trigger file: {unlink_e}")
                            
                            logger.info(f"Strategy trigger received: {trigger_data.get('event')}")
                            return trigger_data
                            
                        except (IOError, OSError) as file_e:
                            # 文件锁定或其他IO错误
                            if attempt < max_retries - 1:
                                logger.debug(
                                    f"File access error (attempt {attempt + 1}/{max_retries}): {file_e}, retrying..."
                                )
                                await asyncio.sleep(retry_delay)
                                continue
                            else:
                                logger.warning(f"Failed to read trigger file after {max_retries} attempts: {file_e}")
                                return None
                    
                    return None
                    
                except Exception as e:
                    logger.error(f"Unexpected error reading trigger file: {e}", exc_info=True)
            
            await asyncio.sleep(check_interval)
        
        return None
    
    async def _execute_strategy(self, symbols: list[str]):
        """
        执行策略计算
        
        Args:
            symbols: 交易对列表
        """
        # 检查是否正在计算，如果是则跳过
        if self.is_calculating:
            logger.warning("Strategy calculation is already in progress, skipping this trigger")
            return
        
        # 开始性能监控轮次
        round_id = f"strategy_{int(time.time() * 1000)}"
        self.performance_monitor.start_round(round_id)
        
        try:
            self.is_calculating = True
            logger.info(f"Starting strategy execution for {len(symbols)} symbols")
            start_time = time.time()

            # 1) 获取账户列表（从对应模式的配置中读取）
            with self.performance_monitor.measure('strategy_process', 'get_accounts'):
                execution_mode = config.get('execution.mode', 'mock')
                mode_config = config.get(f'execution.{execution_mode}', {})
                accounts_config = mode_config.get('accounts', [])
                accounts = [acc.get('account_id') for acc in accounts_config if acc.get('account_id')]
                
                if not accounts:
                    logger.error("No accounts configured, aborting strategy calculation")
                    return
            
            # 2) Alpha计算：weights向量（system symbol -> weight）
            logger.info(f"Running alpha for {len(accounts)} accounts (portfolio shared), {len(symbols)} symbols...")
            with self.performance_monitor.measure('strategy_process', 'alpha_calculation', {'symbols_count': len(symbols)}):
                alpha_result = self.alpha_engine.run(symbols=symbols)
            weights = alpha_result.weights

            logger.info(f"Alpha completed: non-zero weights={len(weights)} (from {len(alpha_result.per_calculator)} calculators)")

            # 3) 转换为目标持仓并保存
            with self.performance_monitor.measure('strategy_process', 'position_generation'):
                timestamp = datetime.now(timezone.utc)
                target_positions = self.position_generator.build_target_positions_from_weights(
                    weights=weights,
                    accounts=accounts,
                    timestamp=timestamp,
                )

                file_paths = self.position_generator.save_target_positions(
                    target_positions=target_positions,
                    timestamp=timestamp
                )
            
            # 4) 通知事件协调进程
            with self.performance_monitor.measure('strategy_process', 'ipc_notification'):
                if self.ipc_client:
                    for account_id, file_path in file_paths.items():
                        try:
                            await self.ipc_client.send_target_position_ready(
                                account_id=account_id,
                                file_path=file_path
                            )
                            logger.info(f"Notified target position ready for account {account_id}")
                        except Exception as e:
                            logger.error(
                                f"Failed to notify target position for account {account_id}: {e}"
                            )
            
            # 更新状态
            self.last_calculation_time = timestamp
            elapsed_time = time.time() - start_time
            
            logger.info(
                f"Strategy execution completed successfully in {elapsed_time:.2f} seconds. "
                f"Generated positions for {len(file_paths)} accounts"
            )
            
        except Exception as e:
            logger.error(f"Error in strategy execution: {e}", exc_info=True)
            raise
        finally:
            # 无论成功还是失败，都要重置计算标志
            self.is_calculating = False
            # 结束性能监控轮次并保存
            self.performance_monitor.end_round(save=True)
    
    def _get_universe(self, force_reload: bool = False) -> list[str]:
        """
        获取Universe，带缓存机制
        
        Args:
            force_reload: 是否强制重新加载
            
        Returns:
            Universe列表
        """
        now = datetime.now(timezone.utc)
        
        # 检查缓存是否有效
        if not force_reload and self._cached_universe is not None and self._universe_cache_time:
            elapsed = (now - self._universe_cache_time).total_seconds()
            if elapsed < self._universe_cache_ttl:
                logger.info(f"Using cached universe ({len(self._cached_universe)} symbols, cached {elapsed:.1f}s ago)")
                return self._cached_universe
        
        # 重新加载Universe
        universe = self.data_api.get_universe()
        if universe:
            self._cached_universe = list(universe)
            self._universe_cache_time = now
            logger.info(f"Loaded and cached universe: {len(self._cached_universe)} symbols")
        else:
            logger.warning("Failed to load universe, using cached if available")
            if self._cached_universe is None:
                return []
        
        return self._cached_universe if self._cached_universe else []
    
    async def run_once(self):
        """执行一次策略计算"""
        try:
            # 连接IPC
            await self._connect_ipc()
            
            # 获取Universe
            universe = self._get_universe()
            if not universe:
                logger.error("No universe available, cannot execute strategy")
                return
            
            # 执行策略
            await self._execute_strategy(universe)
            
        except Exception as e:
            logger.error(f"Strategy process error: {e}", exc_info=True)
        finally:
            # 断开IPC连接
            if self.ipc_client:
                await self.ipc_client.disconnect()
    
    async def run_waiting_mode(self):
        """运行在等待模式：监听触发信号"""
        logger.info("Strategy process running in waiting mode...")
        self.running = True
        
        # 连接IPC
        await self._connect_ipc()
        
        try:
            while self.running:
                # 等待触发信号
                timeout = config.get('strategy.process.run_waiting_timeout', 3600.0)
                trigger_data = await self._wait_for_trigger(timeout=timeout)
                
                if not self.running:
                    break
                
                if trigger_data:
                    event = trigger_data.get('event')
                    if event == 'data_complete':
                        # 检查是否正在计算，如果是则跳过
                        if self.is_calculating:
                            logger.debug("Strategy calculation in progress, skipping data_complete trigger")
                            continue
                        
                        symbols = trigger_data.get('symbols', [])
                        if symbols:
                            await self._execute_strategy(symbols)
                        else:
                            # 如果没有symbols，使用Universe（使用缓存版本）
                            universe = self._get_universe()
                            if universe:
                                await self._execute_strategy(universe)
                
                # 等待一段时间后继续监听
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Error in waiting mode: {e}", exc_info=True)
        finally:
            self.running = False
            if self.ipc_client:
                await self.ipc_client.disconnect()
    
    async def stop(self):
        """停止策略进程"""
        logger.info("Stopping strategy process...")
        self.running = False
        
        if self.ipc_client:
            await self.ipc_client.disconnect()


async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Strategy process")
    parser.add_argument(
        '--mode',
        type=str,
        choices=['once', 'wait'],
        default='once',
        help='Run mode: once (execute once and exit) or wait (wait for triggers)'
    )
    
    args = parser.parse_args()
    
    process = StrategyProcess()
    
    # 信号处理
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(process.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if args.mode == 'once':
            # 执行一次后退出
            await process.run_once()
        else:
            # 等待模式
            await process.run_waiting_mode()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Strategy process error: {e}", exc_info=True)
    finally:
        await process.stop()


if __name__ == "__main__":
    asyncio.run(main())
