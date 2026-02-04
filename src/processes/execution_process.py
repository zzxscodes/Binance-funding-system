"""
订单执行进程（Process 4）
持续运行，执行交易订单，支持多账户
"""
import asyncio
import signal
import sys
import json
import time
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime, timezone, timedelta

from ..common.config import config
from ..common.logger import get_logger
from ..common.ipc import IPCClient, MessageType
from ..common.utils import format_symbol
from ..common.account_validator import validate_account_config
from ..execution.binance_client import BinanceClient
from ..execution.dry_run_client import DryRunBinanceClient
from ..execution.order_manager import OrderManager
from ..strategy.position_generator import get_position_generator
from ..monitoring.strategy_report import get_strategy_report_generator
from ..data.api import get_data_api

logger = get_logger('execution_process')


class ExecutionProcess:
    """订单执行进程"""
    
    def __init__(self, account_id: str, execution_mode: Optional[str] = None):
        """
        初始化订单执行进程
        
        Args:
            account_id: 账户ID
            execution_mode: 执行模式 ('testnet', 'mock', 'live')，如果不指定则从配置读取
        """
        self.account_id = account_id
        
        # 获取执行模式
        if execution_mode is None:
            execution_mode = config.get('execution.mode', 'mock')
        
        if execution_mode not in ['testnet', 'mock', 'live']:
            raise ValueError(f"Invalid execution mode: {execution_mode}. Must be one of: testnet, mock, live")
        
        self.execution_mode = execution_mode
        
        # 获取dry-run开关
        self.dry_run_flag = config.get('execution.dry_run', False)
        
        # 获取对应模式的配置
        mode_config = config.get(f'execution.{execution_mode}', {})
        if not mode_config:
            raise ValueError(f"Configuration for mode '{execution_mode}' not found in execution.{execution_mode}")
        
        # 获取该模式下的账户列表
        mode_accounts = mode_config.get('accounts', [])
        account_config = next(
            (acc for acc in mode_accounts if acc.get('account_id') == account_id),
            None
        )
        
        if not account_config:
            raise ValueError(
                f"Account {account_id} not found in {execution_mode} mode configuration. "
                f"Available accounts: {[acc.get('account_id') for acc in mode_accounts]}"
            )
        
        # 验证账户配置是否完整
        is_valid, error_msg = validate_account_config(account_config, execution_mode)
        if not is_valid:
            raise ValueError(
                f"Account {account_id} configuration is incomplete: {error_msg}"
            )
        
        import os
        
        # 根据执行模式初始化客户端
        if execution_mode == 'testnet':
            # Testnet模式：使用testnet端点和API密钥
            api_base = mode_config.get('api_base', 'https://testnet.binancefuture.com')
            
            # 优先级：环境变量 > 配置文件
            api_key_env = f"{account_id.upper()}_TESTNET_API_KEY"
            api_secret_env = f"{account_id.upper()}_TESTNET_API_SECRET"
            
            api_key = os.getenv(api_key_env, account_config.get('api_key', ''))
            api_secret = os.getenv(api_secret_env, account_config.get('api_secret', ''))
            
            if not api_key or not api_secret:
                raise ValueError(
                    f"Testnet mode requires API keys for account {account_id}. "
                    f"Configure via: {api_key_env}/{api_secret_env} env vars or account config"
                )
            
            logger.info(f"Initializing {account_id} in TESTNET mode (dry_run={self.dry_run_flag})")
            self.api_key = api_key
            self.api_secret = api_secret
            # testnet模式根据dry_run_flag决定是否使用dry_run
            self.binance_client = BinanceClient(api_key, api_secret, api_base=api_base, dry_run=self.dry_run_flag)
            self.dry_run = self.dry_run_flag
            
        elif execution_mode == 'mock':
            # Mock模式：使用模拟端点和模拟账户信息
            api_base = mode_config.get('api_base', 'https://mock.binance.com')
            
            # 验证mock模式必需的配置
            total_wallet_balance = account_config.get('total_wallet_balance')
            available_balance = account_config.get('available_balance')
            
            if total_wallet_balance is None or available_balance is None:
                raise ValueError(
                    f"Mock mode requires balance information for account {account_id}. "
                    f"Please configure total_wallet_balance and available_balance."
                )
            
            logger.info(f"Initializing {account_id} in MOCK mode (dry_run={self.dry_run_flag})")
            # Mock模式不需要API密钥
            self.api_key = None
            self.api_secret = None
            
            # 获取模拟账户信息（DryRunBinanceClient会从配置读取）
            self.binance_client = DryRunBinanceClient(
                account_id=account_id,
                api_base=api_base,
                use_real_exchange_info=False  # Mock模式不使用真实交易所信息
            )
            self.dry_run = True  # Mock模式总是dry-run
            
        elif execution_mode == 'live':
            # Live模式：使用实盘端点和真实API密钥
            api_base = mode_config.get('api_base', 'https://fapi.binance.com')
            
            # 优先级：环境变量 > 配置文件
            api_key_env = f"{account_id.upper()}_API_KEY"
            api_secret_env = f"{account_id.upper()}_API_SECRET"
            
            api_key = os.getenv(api_key_env, account_config.get('api_key', ''))
            api_secret = os.getenv(api_secret_env, account_config.get('api_secret', ''))
            
            if not api_key or not api_secret:
                raise ValueError(
                    f"Live mode requires API keys for account {account_id}. "
                    f"Configure via: {api_key_env}/{api_secret_env} env vars or account config"
                )
            
            if not self.dry_run_flag:
                logger.warning(f"Initializing {account_id} in LIVE mode (REAL TRADING ENABLED!)")
            else:
                logger.info(f"Initializing {account_id} in LIVE mode with DRY-RUN enabled (no real orders)")
            
            self.api_key = api_key
            self.api_secret = api_secret
            # live模式根据dry_run_flag决定是否使用dry_run
            self.binance_client = BinanceClient(api_key, api_secret, api_base=api_base, dry_run=self.dry_run_flag)
            self.dry_run = self.dry_run_flag
        
        # 初始化订单管理器
        self.order_manager = OrderManager(self.binance_client, dry_run=self.dry_run)
        
        # 初始化目标持仓加载器
        self.position_generator = get_position_generator()
        
        # 数据API（用于从数据层获取资金费率）
        self.data_api = get_data_api()
        
        # IPC客户端（用于通知事件协调进程）
        self.ipc_client: Optional[IPCClient] = None
        
        # 状态
        self.running = False
        
        # 信号文件路径
        signals_dir = config.get('data.signals_directory', 'data/signals')
        self.trigger_file = Path(signals_dir) / f'execution_trigger_{account_id}.json'
    
    async def _apply_contract_settings(self):
        """
        应用合约设置（保证金模式、持仓模式、杠杆倍数）
        从配置文件读取设置并应用到所有交易对
        """
        try:
            contract_settings = config.get('execution.contract_settings', {})
            margin_type = contract_settings.get('margin_type', 'CROSSED').upper()
            position_mode = contract_settings.get('position_mode', 'one_way')
            leverage = contract_settings.get('leverage', 20)
            
            # 验证配置
            if margin_type not in ['CROSSED', 'ISOLATED']:
                logger.warning(f"Invalid margin_type: {margin_type}, using default CROSSED")
                margin_type = 'CROSSED'
            
            if position_mode not in ['one_way', 'dual_side']:
                logger.warning(f"Invalid position_mode: {position_mode}, using default one_way")
                position_mode = 'one_way'
            
            if not isinstance(leverage, int) or leverage < 1 or leverage > 125:
                logger.warning(f"Invalid leverage: {leverage}, using default 20")
                leverage = 20
            
            logger.info(
                f"Applying contract settings: margin_type={margin_type}, "
                f"position_mode={position_mode}, leverage={leverage}x"
            )
            
            # 1. 设置持仓模式（只需要设置一次，不是按symbol的）
            dual_side_position = (position_mode == 'dual_side')
            try:
                await self.binance_client.change_position_mode(dual_side_position=dual_side_position)
                logger.info(f"Position mode set to: {'dual_side' if dual_side_position else 'one_way'}")
            except Exception as e:
                logger.warning(f"Failed to set position mode: {e}")
            
            # 2. 不再在启动时批量设置所有symbol的合约配置
            # 合约配置将在首次下单时自动设置（通过BinanceClient.place_order中的_ensure_contract_settings）
            # 这样可以：
            # - 避免启动时大量API调用导致超时
            # - 只配置实际需要交易的symbol
            # - 利用配置缓存机制，避免重复设置
            logger.info(
                f"Contract settings will be applied automatically when placing orders for each symbol. "
                f"Target settings: margin_type={margin_type}, leverage={leverage}x"
            )
            
        except Exception as e:
            logger.error(f"Failed to apply contract settings: {e}", exc_info=True)
    
    async def _connect_ipc(self):
        """连接IPC服务器（事件协调进程）"""
        try:
            self.ipc_client = IPCClient()
            await self.ipc_client.connect()
            logger.info(f"Connected to event coordinator via IPC for account {self.account_id}")
        except Exception as e:
            logger.warning(f"Failed to connect to event coordinator: {e}")
            logger.warning("Will continue without IPC notification")
    
    async def _wait_for_trigger(self, timeout: Optional[float] = None) -> Optional[Dict]:
        """
        等待执行触发信号
        
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
                    # 使用重试机制处理文件锁定问题（类似strategy_process）
                    for attempt in range(max_retries):
                        try:
                            with open(self.trigger_file, 'r', encoding='utf-8') as f:
                                content = f.read().strip()
                            
                            # 检查文件内容是否为空
                            if not content:
                                logger.debug(f"Execution trigger file is empty, waiting...")
                                await asyncio.sleep(retry_delay)
                                continue
                            
                            # 解析JSON
                            try:
                                trigger_data = json.loads(content)
                            except json.JSONDecodeError as json_e:
                                logger.warning(
                                    f"Invalid JSON in execution trigger file (attempt {attempt + 1}/{max_retries}): {json_e}. "
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
                            
                            # 验证账户ID
                            if trigger_data.get('account_id') != self.account_id:
                                await asyncio.sleep(check_interval)
                                continue
                            
                            # 删除信号文件（避免重复处理）
                            try:
                                self.trigger_file.unlink()
                            except Exception as unlink_e:
                                # 如果删除失败（可能被其他进程占用），记录警告但继续
                                logger.warning(f"Failed to delete execution trigger file: {unlink_e}")
                            
                            logger.info(
                                f"Execution trigger received for account {self.account_id}: "
                                f"{trigger_data.get('event')}"
                            )
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
                                logger.warning(f"Failed to read execution trigger file after {max_retries} attempts: {file_e}")
                                return None
                    
                    return None
                    
                except Exception as e:
                    logger.error(f"Unexpected error reading execution trigger file: {e}", exc_info=True)
            
            await asyncio.sleep(check_interval)
        
        return None
    
    async def _execute_target_positions(self, file_path: str):
        """
        执行目标持仓
        
        Args:
            file_path: 目标持仓文件路径
        """
        try:
            logger.info(f"Loading target positions from {file_path} for account {self.account_id}")
            
            # 1. 加载目标持仓文件
            target_data = self.position_generator.load_target_positions(file_path)
            
            if not target_data or 'positions' not in target_data:
                logger.error(f"Invalid target positions file: {file_path}")
                return
            
            positions = target_data['positions']
            
            # 2. 转换为目标持仓字典
            target_positions = {}
            for pos in positions:
                symbol = format_symbol(pos.get('symbol', ''))
                target_position = float(pos.get('target_position', 0.0))
                target_positions[symbol] = target_position
            
            logger.info(
                f"Target positions loaded: {len(target_positions)} symbols for account {self.account_id}"
            )
            
            # 3. 执行目标持仓（带错误恢复）
            executed_orders = []
            try:
                executed_orders = await self.order_manager.execute_target_positions(target_positions)
                
                # 4. 监控订单（等待订单完成）
                if executed_orders:
                    monitor_timeout = config.get('execution.order.monitor_timeout', 30.0)
                    await self.order_manager.monitor_orders(timeout=monitor_timeout)
            except Exception as e:
                logger.error(f"Failed to execute target positions: {e}", exc_info=True)
                # 错误恢复：记录错误但不中断进程，等待下次触发
                # 这样可以确保系统在遇到临时错误时能够继续运行
                logger.info("Execution failed, but process will continue and wait for next trigger")
            
            # 5. 记录已完成的订单到策略报告（用于策略磨损分析）
            try:
                report_generator = get_strategy_report_generator()
                completed_orders = list(self.order_manager.completed_orders)  # 复制列表，避免在迭代时被修改
                
                if completed_orders:
                    logger.info(f"Recording {len(completed_orders)} completed orders for strategy wear analysis")
                
                for order in completed_orders:
                    # 检查订单状态（completed_orders中的订单应该都是FILLED的）
                    status = order.get('status', '')
                    if status != 'FILLED':
                        logger.debug(f"Skipping order {order.get('order_id')} with status {status}")
                        continue
                    
                    symbol = order.get('symbol', '')
                    side = order.get('side', '')
                    order_id = order.get('order_id', '')
                    
                    # 获取成交数量
                    quantity = float(order.get('quantity', 0) or order.get('executedQty', 0) or 0)
                    if quantity <= 0:
                        logger.warning(f"Order {order_id} for {symbol} has invalid quantity: {quantity}")
                        continue
                    
                    # 获取成交价格（优先使用avgPrice，如果没有则使用price）
                    price = float(order.get('avgPrice', 0) or order.get('price', 0) or 0)
                    if price <= 0:
                        # 如果订单信息中没有价格，尝试从订单状态查询
                        logger.warning(f"Order {order_id} for {symbol} has no price, attempting to fetch from order status")
                        try:
                            status_result = await self.order_manager.client.get_order_status(symbol, int(order_id))
                            price = float(status_result.get('avgPrice', 0) or status_result.get('price', 0) or 0)
                        except Exception as e:
                            logger.error(f"Failed to fetch price for order {order_id}: {e}")
                            continue
                    
                    if price <= 0:
                        logger.warning(f"Order {order_id} for {symbol} still has no valid price after fetch, skipping")
                        continue
                    
                    # 确定持仓方向：BUY为正数（做多），SELL为负数（做空）
                    position_amt = quantity if side == 'BUY' else -quantity
                    
                    # 获取订单时间
                    order_time = order.get('time', order.get('updateTime', order.get('executed_time')))
                    if order_time:
                        if isinstance(order_time, str):
                            # ISO格式字符串
                            try:
                                timestamp = datetime.fromisoformat(order_time.replace('Z', '+00:00'))
                            except:
                                timestamp = datetime.now(timezone.utc)
                        elif isinstance(order_time, (int, float)):
                            # 时间戳（毫秒或秒）
                            if order_time > 1e10:  # 毫秒
                                timestamp = datetime.fromtimestamp(order_time / 1000, tz=timezone.utc)
                            else:  # 秒
                                timestamp = datetime.fromtimestamp(order_time, tz=timezone.utc)
                        else:
                            timestamp = datetime.now(timezone.utc)
                    else:
                        timestamp = datetime.now(timezone.utc)
                    
                    # 从数据层获取开仓时的资金费率（从已存储的数据中查找）
                    # 注意：这里记录的是开仓时的资金费率，用于策略磨损分析
                    # 策略磨损分析会使用这个费率乘以持仓期间的资金费率周期数来估算成本
                    funding_rate = None
                    try:
                        # 从数据层获取开仓时间附近的资金费率数据
                        # 资金费率每8小时一次，查找开仓时间之前最近的一次资金费率
                        funding_rates_df = self.data_api.storage.load_funding_rates(
                            symbol=symbol,
                            start_date=timestamp - timedelta(days=1),  # 查找开仓前1天的数据
                            end_date=timestamp + timedelta(hours=1)  # 到开仓后1小时
                        )
                        
                        if not funding_rates_df.empty and 'fundingRate' in funding_rates_df.columns:
                            # 找到开仓时间之前最近的一次资金费率
                            before_open = funding_rates_df[funding_rates_df['fundingTime'] <= timestamp]
                            if not before_open.empty:
                                # 取最近的一条
                                latest = before_open.sort_values('fundingTime').iloc[-1]
                                funding_rate = float(latest['fundingRate'])
                                logger.debug(f"Found funding rate for {symbol} at {timestamp}: {funding_rate}")
                            else:
                                # 如果没有开仓前的数据，使用开仓后最近的一次
                                after_open = funding_rates_df[funding_rates_df['fundingTime'] > timestamp]
                                if not after_open.empty:
                                    latest = after_open.sort_values('fundingTime').iloc[0]
                                    funding_rate = float(latest['fundingRate'])
                                    logger.debug(f"Using next funding rate for {symbol} at {timestamp}: {funding_rate}")
                    except Exception as e:
                        logger.warning(f"Failed to get funding rate from data layer for {symbol}: {e}")
                        # 继续记录开仓，即使资金费率获取失败
                    
                    # 记录开仓信息
                    report_generator.record_position_opening(
                        account_id=self.account_id,
                        symbol=symbol,
                        position_amt=position_amt,
                        entry_price=price,
                        timestamp=timestamp,
                        funding_rate=funding_rate,
                        metadata={'order_id': order_id, 'side': side}
                    )
                    logger.info(f"Recorded position opening for {symbol}: {position_amt} @ {price} (funding_rate={funding_rate}, order_id={order_id})")
                
                # 清空已完成的订单列表（避免重复记录）
                self.order_manager.completed_orders.clear()
                
            except Exception as e:
                logger.error(f"Failed to record position openings: {e}", exc_info=True)
            
            # 6. 通知事件协调进程（订单执行完成）
            if self.ipc_client and executed_orders:
                for order in executed_orders:
                    try:
                        await self.ipc_client.send_order_executed(
                            account_id=self.account_id,
                            order_info=order
                        )
                    except Exception as e:
                        logger.error(f"Failed to notify order executed: {e}")
            
            logger.info(
                f"Target positions execution completed for account {self.account_id}: "
                f"{len(executed_orders)} orders executed"
            )
            
        except Exception as e:
            logger.error(
                f"Failed to execute target positions for account {self.account_id}: {e}",
                exc_info=True
            )
    
    async def start(self):
        """启动订单执行进程"""
        if self.running:
            logger.warning(f"Execution process for account {self.account_id} is already running")
            return
        
        logger.info(f"Starting execution process for account {self.account_id}...")
        self.running = True
        
        # 连接IPC
        await self._connect_ipc()
        
        # 应用合约设置（保证金模式、持仓模式、杠杆倍数）
        # 在以下情况下应用：
        # 1. 真实交易模式（live mode）- 必须应用
        # 2. 测试网模式（testnet mode）- 必须应用
        # 3. Dry-run模式但使用API密钥 - 可以应用（用于测试）
        # 检查是否有api_key属性（避免AttributeError）
        try:
            api_key = getattr(self, 'api_key', None)
            api_secret = getattr(self, 'api_secret', None)
            has_api_keys = api_key and api_secret
        except AttributeError:
            has_api_keys = False
        
        if not self.dry_run or (self.dry_run and has_api_keys):
            try:
                await self._apply_contract_settings()
            except Exception as e:
                logger.warning(f"Failed to apply contract settings during startup: {e}")
        
        # 创建信号目录
        signals_dir = config.get('data.signals_directory', 'data/signals')
        Path(signals_dir).mkdir(parents=True, exist_ok=True)
    
    async def run_waiting_mode(self):
        """运行在等待模式：监听触发信号"""
        logger.info(
            f"Execution process for account {self.account_id} running in waiting mode..."
        )
        
        try:
            while self.running:
                # 等待触发信号
                timeout = config.get('strategy.process.run_waiting_timeout', 3600.0)
                trigger_data = await self._wait_for_trigger(timeout=timeout)
                
                if not self.running:
                    break
                
                if trigger_data:
                    event = trigger_data.get('event')
                    if event == 'target_position_ready':
                        file_path = trigger_data.get('file_path')
                        if file_path:
                            await self._execute_target_positions(file_path)
                
                # 等待一段时间后继续监听
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Error in waiting mode for account {self.account_id}: {e}", exc_info=True)
    
    async def stop(self):
        """停止订单执行进程"""
        logger.info(f"Stopping execution process for account {self.account_id}...")
        self.running = False
        
        # 取消所有待处理订单
        try:
            await self.order_manager.cancel_all_pending_orders()
        except Exception as e:
            logger.error(f"Failed to cancel pending orders: {e}")
        
        # 关闭Binance客户端
        if self.binance_client:
            await self.binance_client.close()
        
        # 断开IPC连接
        if self.ipc_client:
            await self.ipc_client.disconnect()
        
        logger.info(f"Execution process for account {self.account_id} stopped")


async def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Execution process")
    parser.add_argument(
        '--account',
        type=str,
        required=True,
        help='Account ID'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['live', 'testnet', 'dry-run'],
        default=None,
        help='Execution mode: live (real trading), testnet (testnet API), dry-run (offline simulation)'
    )
    
    args = parser.parse_args()
    
    process = ExecutionProcess(account_id=args.account, execution_mode=args.mode)
    
    # 信号处理
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(process.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await process.start()
        await process.run_waiting_mode()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Execution process error: {e}", exc_info=True)
    finally:
        await process.stop()


if __name__ == "__main__":
    asyncio.run(main())
