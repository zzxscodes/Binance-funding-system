"""
订单管理器
执行订单，监控订单状态
"""
import asyncio
import time
from typing import Dict, List, Optional
from datetime import datetime, timezone, timedelta

from ..common.logger import get_logger
from ..common.config import config
from ..common.utils import format_symbol, round_qty
from .binance_client import BinanceClient
from .position_manager import PositionManager

logger = get_logger('order_manager')


class OrderManager:
    """订单管理器"""
    
    def __init__(self, binance_client: BinanceClient, dry_run: bool = False):
        """
        初始化订单管理器
        
        Args:
            binance_client: Binance API客户端
            dry_run: 是否使用dry-run模式（不实际下单）
        """
        self.client = binance_client
        self.dry_run = dry_run
        self.position_manager = PositionManager(binance_client)
        
        # 订单状态跟踪
        self.pending_orders: Dict[str, Dict] = {}  # order_id -> order_info
        self.completed_orders: List[Dict] = []
        
        # 并发控制：限制同时执行的订单数量，避免API限流
        # Binance API限制：每分钟最多1200个请求，每个订单需要多个请求
        max_concurrent = config.get('execution.order.max_concurrent', 5)
        self.order_semaphore = asyncio.Semaphore(max_concurrent)
        
        if self.dry_run:
            logger.info("OrderManager initialized in DRY-RUN mode")
    
    async def execute_target_positions(
        self,
        target_positions: Dict[str, float]
    ) -> List[Dict]:
        """
        执行目标持仓
        
        Args:
            target_positions: Dict[symbol, target_position]，目标持仓（权重，需要转换为实际数量）
        
        Returns:
            已执行的订单列表
        """
        try:
            # 1. 更新当前持仓
            await self.position_manager.update_current_positions()
            
            # 2. 将权重转换为实际数量
            # target_position是权重（如0.5表示50%的账户权益），需要转换为实际数量
            target_positions_quantity = await self._convert_weights_to_quantities(target_positions)
            
            # 3. 计算持仓偏差
            orders = self.position_manager.calculate_position_diff(target_positions_quantity)
            
            if not orders:
                logger.info("No orders needed, positions already match targets")
                return []
            
            # 3. 规范化订单
            normalized_orders = await self.normalize_orders(orders)
            
            if not normalized_orders:
                logger.warning("No valid orders after normalization")
                return []
            
            # 4. 执行订单（使用并发控制）
            executed_orders = []
            failed_orders = []  # 记录失败的订单，用于错误恢复
            
            # 创建并发任务，但通过信号量限制并发数
            async def execute_with_semaphore(order: Dict):
                async with self.order_semaphore:
                    try:
                        result = await self._execute_order(order)
                        if result:
                            return result, None
                        else:
                            return None, order
                    except Exception as e:
                        logger.error(f"Failed to execute order for {order['symbol']}: {e}", exc_info=True)
                        return None, order
            
            # 并发执行所有订单（但受信号量限制）
            tasks = [execute_with_semaphore(order) for order in normalized_orders]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 处理结果
            for res in results:
                if isinstance(res, Exception):
                    logger.error(f"Exception in order execution: {res}", exc_info=True)
                    continue
                elif isinstance(res, tuple):
                    result, failed_order = res
                    if result:
                        executed_orders.append(result)
                    elif failed_order:
                        failed_orders.append(failed_order)
            
            # 错误恢复：对失败的订单进行重试（最多重试1次）
            if failed_orders:
                logger.info(f"Retrying {len(failed_orders)} failed orders...")
                retry_tasks = [execute_with_semaphore(order) for order in failed_orders]
                retry_results = await asyncio.gather(*retry_tasks, return_exceptions=True)
                
                for res in retry_results:
                    if isinstance(res, Exception):
                        continue
                    elif isinstance(res, tuple):
                        result, _ = res
                        if result:
                            executed_orders.append(result)
            
            logger.info(f"Executed {len(executed_orders)} orders out of {len(normalized_orders)}")
            return executed_orders
            
        except Exception as e:
            logger.error(f"Failed to execute target positions: {e}", exc_info=True)
            raise
    
    async def _execute_order(self, order: Dict) -> Optional[Dict]:
        """
        执行单个订单
        
        Args:
            order: 订单信息
        
        Returns:
            订单执行结果
        """
        try:
            symbol = order['symbol']
            side = order['side']
            quantity = order['normalized_quantity']
            order_type = order.get('order_type', 'MARKET')
            reduce_only = order.get('reduce_only', False)
            
            # 下单（dry-run模式下会自动使用test_order endpoint或完全离线模拟）
            if self.dry_run and hasattr(self.client, 'dry_run_mode') and self.client.dry_run_mode:
                # 完全离线dry-run模式（使用DryRunBinanceClient）
                result = await self.client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    position_side='BOTH',
                    reduce_only=reduce_only
                )
            else:
                # 正常模式或使用testnet test_order endpoint
                result = await self.client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    position_side='BOTH',
                    reduce_only=reduce_only
                )
            
            order_id = result.get('orderId')
            status = result.get('status')
            
            # 记录订单
            order_info = {
                'order_id': order_id,
                'symbol': symbol,
                'side': side,
                'quantity': quantity,
                'order_type': order_type,
                'status': status,
                'executed_time': datetime.now(timezone.utc).isoformat(),
                'reduce_only': reduce_only,
                'current_position': order.get('current_position'),
                'target_position': order.get('target_position'),
            }
            
            if status == 'FILLED':
                # 完全成交
                self.completed_orders.append(order_info)
                logger.info(
                    f"Order {order_id} for {symbol} filled: {side} {quantity}"
                )
            else:
                # 部分成交或未成交，需要监控
                self.pending_orders[str(order_id)] = order_info
                logger.info(
                    f"Order {order_id} for {symbol} placed: {side} {quantity}, status={status}"
                )
            
            return order_info
            
        except Exception as e:
            error_str = str(e)
            # 检查是否是Binance的最小订单金额错误（-4164）
            if '-4164' in error_str or 'notional must be no smaller' in error_str.lower():
                logger.warning(
                    f"Order for {symbol} rejected due to insufficient notional value. "
                    f"Quantity: {quantity}, Error: {error_str[:200]}"
                )
                # 这不是致命错误，只是订单金额太小，记录警告即可
            elif '-1111' in error_str or 'precision' in error_str.lower():
                # 精度错误：记录详细信息以便调试
                logger.error(
                    f"Precision error for {symbol}: quantity={quantity} (type={type(quantity).__name__}), "
                    f"original_quantity={order.get('quantity')}, normalized_quantity={order.get('normalized_quantity')}, "
                    f"Error: {error_str[:200]}"
                )
            else:
                logger.error(f"Error executing order: {e}", exc_info=True)
            return None
    
    async def monitor_orders(self, timeout: Optional[float] = None):
        """
        监控待处理订单
        
        Args:
            timeout: 超时时间（秒），如果不指定则使用配置值
        """
        if timeout is None:
            timeout = config.get('execution.order.monitor_timeout', 30.0)
        start_time = time.time()
        
        while (time.time() - start_time) < timeout:
            if not self.pending_orders:
                break
            
            # 检查每个待处理订单的状态
            order_ids_to_remove = []
            
            for order_id_str, order_info in self.pending_orders.items():
                try:
                    order_id = int(order_id_str)
                    symbol = order_info['symbol']
                    
                    # 查询订单状态
                    status_result = await self.client.get_order_status(symbol, order_id)
                    status = status_result.get('status')
                    
                    if status == 'FILLED':
                        # 已成交
                        order_info['status'] = 'FILLED'
                        self.completed_orders.append(order_info)
                        order_ids_to_remove.append(order_id_str)
                        logger.info(f"Order {order_id} for {symbol} completed")
                    elif status in ['CANCELED', 'EXPIRED', 'REJECTED']:
                        # 订单失败
                        order_info['status'] = status
                        order_ids_to_remove.append(order_id_str)
                        logger.warning(f"Order {order_id} for {symbol} failed: {status}")
                
                except Exception as e:
                    logger.error(f"Failed to check order {order_id_str} status: {e}")
                    continue
            
            # 移除已完成的订单
            for order_id in order_ids_to_remove:
                self.pending_orders.pop(order_id, None)
            
            if not self.pending_orders:
                break
            
            monitor_interval = config.get('execution.order.monitor_interval', 2.0)
            await asyncio.sleep(monitor_interval)
    
    async def cancel_all_pending_orders(self, symbol: Optional[str] = None):
        """
        取消所有待处理订单
        
        Args:
            symbol: 如果指定，只取消该交易对的订单；否则取消所有
        """
        try:
            order_ids_to_cancel = []
            
            for order_id_str, order_info in self.pending_orders.items():
                if symbol is None or order_info['symbol'] == symbol:
                    order_ids_to_cancel.append((int(order_id_str), order_info))
            
            for order_id, order_info in order_ids_to_cancel:
                try:
                    await self.client.cancel_order(order_info['symbol'], order_id)
                    self.pending_orders.pop(str(order_id), None)
                    logger.info(f"Cancelled order {order_id} for {order_info['symbol']}")
                except Exception as e:
                    logger.error(f"Failed to cancel order {order_id}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to cancel all pending orders: {e}", exc_info=True)
    
    def get_order_statistics(self) -> Dict:
        """获取订单统计信息"""
        return {
            'pending_orders_count': len(self.pending_orders),
            'completed_orders_count': len(self.completed_orders),
            'pending_order_ids': list(self.pending_orders.keys()),
        }
    
    async def get_symbol_info(self, symbol: str) -> Dict:
        """获取交易对的精度等信息"""
        symbol = format_symbol(symbol)
        
        # 使用简单的缓存（可以扩展为更复杂的缓存机制）
        if not hasattr(self, '_symbol_info_cache'):
            self._symbol_info_cache: Dict[str, Dict] = {}
        
        if symbol in self._symbol_info_cache:
            return self._symbol_info_cache[symbol]
        
        try:
            exchange_info = await self.client.get_exchange_info()
            
            for sym_info in exchange_info.get('symbols', []):
                if format_symbol(sym_info.get('symbol', '')) == symbol:
                    # 提取精度信息
                    tick_size = config.get('execution.order.default_tick_size', 0.01)
                    step_size = config.get('execution.order.default_step_size', 0.01)
                    min_qty = config.get('execution.order.default_min_qty', 0.001)
                    min_notional = config.get('execution.order.default_min_notional', 5.0)
                    
                    for filter_item in sym_info.get('filters', []):
                        filter_type = filter_item.get('filterType', '')
                        if filter_type == 'PRICE_FILTER':
                            tick_size = float(filter_item.get('tickSize', '0.01'))
                        elif filter_type == 'LOT_SIZE':
                            step_size = float(filter_item.get('stepSize', '0.01'))
                            min_qty = float(filter_item.get('minQty', '0.001'))
                        elif filter_type == 'MIN_NOTIONAL':
                            min_notional_str = filter_item.get('notional', '5.0')
                            try:
                                min_notional = float(min_notional_str)
                            except (ValueError, TypeError):
                                min_notional = 5.0
                    
                    info = {
                        'symbol': symbol,
                        'tick_size': tick_size,
                        'step_size': step_size,
                        'min_qty': min_qty,
                        'min_notional': min_notional,
                    }
                    
                    self._symbol_info_cache[symbol] = info
                    return info
            
            # 如果没有找到，返回默认值
            default_info = {
                'symbol': symbol,
                'tick_size': config.get('execution.order.default_tick_size', 0.01),
                'step_size': config.get('execution.order.default_step_size', 0.01),
                'min_qty': config.get('execution.order.default_min_qty', 0.001),
                'min_notional': config.get('execution.order.default_min_notional', 5.0),
            }
            self._symbol_info_cache[symbol] = default_info
            return default_info
            
        except Exception as e:
            if hasattr(self.client, 'dry_run_mode') and self.client.dry_run_mode:
                logger.debug(f"Dry-run mode: using default symbol info for {symbol} due to error: {e}")
            else:
                logger.error(f"Failed to get symbol info for {symbol}: {e}")
            
            default_info = {
                'symbol': symbol,
                'tick_size': config.get('execution.order.default_tick_size', 0.01),
                'step_size': config.get('execution.order.default_step_size', 0.01),
                'min_qty': config.get('execution.order.default_min_qty', 0.001),
                'min_notional': config.get('execution.order.default_min_notional', 5.0),
            }
            if not hasattr(self, '_symbol_info_cache'):
                self._symbol_info_cache = {}
            self._symbol_info_cache[symbol] = default_info
            return default_info
    
    async def normalize_orders(self, orders: List[Dict]) -> List[Dict]:
        """
        规范化订单（根据精度调整数量和价格，并验证最小订单金额）
        
        Args:
            orders: 订单列表
        
        Returns:
            规范化后的订单列表
        """
        normalized_orders = []
        
        for order in orders:
            symbol = order['symbol']
            
            try:
                # 获取symbol精度信息
                symbol_info = await self.get_symbol_info(symbol)
                step_size = symbol_info.get('step_size', 0.01)
                min_qty = symbol_info.get('min_qty', 0.001)
                min_notional = symbol_info.get('min_notional', 5.0)
                
                # 调整数量精度（向下取整）
                quantity = round_qty(order['quantity'], step_size)
                
                # 如果向下取整后数量为0，直接跳过（避免创建无效订单）
                if quantity <= 0:
                    logger.debug(
                        f"Order quantity {order['quantity']} for {symbol} rounded to {quantity} (step_size={step_size}), skipping"
                    )
                    continue
                
                # 检查最小数量
                if quantity < min_qty:
                    logger.warning(
                        f"Order quantity {quantity} for {symbol} is below minimum {min_qty}, skipping"
                    )
                    continue
                
                # 获取当前价格以计算订单金额（用于验证最小订单金额）
                try:
                    current_price = await self.client.get_symbol_price(symbol)
                    if current_price and current_price > 0:
                        order_notional = quantity * current_price
                        if order_notional < min_notional:
                            logger.warning(
                                f"Order notional {order_notional:.2f} USDT for {symbol} is below minimum {min_notional} USDT, skipping"
                            )
                            continue
                    else:
                        logger.debug(f"Could not get price for {symbol} to validate notional, will let API validate")
                except Exception as price_e:
                    logger.debug(f"Could not get price for {symbol} to validate notional: {price_e}, will let API validate")
                
                # 添加规范化后的数量
                order['normalized_quantity'] = quantity
                order['order_type'] = order.get('order_type', 'MARKET')  # 默认市价单，但可以指定其他类型
                order['min_notional'] = min_notional
                
                normalized_orders.append(order)
                
            except Exception as e:
                logger.error(f"Failed to normalize order for {symbol}: {e}")
                continue
        
        return normalized_orders
    
    async def _convert_weights_to_quantities(self, target_positions: Dict[str, float]) -> Dict[str, float]:
        """
        将权重转换为实际数量
        
        Args:
            target_positions: Dict[symbol, weight]，目标持仓权重（如0.5表示50%的账户权益）
        
        Returns:
            Dict[symbol, quantity]，转换后的实际数量
        """
        try:
            # 获取账户信息
            account_info = await self.client.get_account_info()
            if not account_info:
                logger.warning("Could not get account info, using target positions as quantities")
                return target_positions
            
            # 获取账户权益（使用totalWalletBalance或totalMarginBalance）
            total_balance = float(account_info.get('totalWalletBalance', 0) or 
                                 account_info.get('totalMarginBalance', 0) or 0)
            
            if total_balance <= 0:
                logger.warning(f"Invalid account balance: {total_balance}, using target positions as quantities")
                return target_positions
            
            # 获取杠杆倍数（从配置或账户信息）
            leverage = config.get('execution.contract_settings.leverage', 20)
            
            # 计算可用于交易的金额（考虑杠杆）
            # 注意：这里使用total_balance * leverage，但实际可用金额可能受保证金要求限制
            # 为了安全，我们使用total_balance而不是total_balance * leverage
            # 因为杠杆已经在下单时通过合约设置应用了
            available_capital = total_balance
            
            # 归一化权重：计算总权重（绝对值之和），然后归一化
            total_weight = sum(abs(w) for w in target_positions.values())
            if total_weight > 1e-8:
                # 归一化权重，使总权重为1.0（100%）
                normalized_weights = {symbol: w / total_weight for symbol, w in target_positions.items()}
            else:
                # 如果总权重为0，使用原始权重
                normalized_weights = target_positions
                logger.warning("Total weight is 0, using original weights")
            
            # 转换权重为数量
            target_positions_quantity = {}
            
            for symbol, weight in normalized_weights.items():
                if abs(weight) < 1e-8:  # 权重为0，跳过
                    continue
                
                # 计算该交易对应该分配的USDT金额（使用归一化后的权重）
                target_notional = abs(weight) * available_capital
                
                # 获取当前价格
                try:
                    current_price = await self.client.get_symbol_price(symbol)
                    if current_price and current_price > 0:
                        # 计算数量
                        quantity = target_notional / current_price
                        # 根据方向设置正负（使用归一化后的权重）
                        quantity = quantity if weight > 0 else -quantity
                        target_positions_quantity[symbol] = quantity
                    else:
                        logger.warning(f"Could not get price for {symbol}, skipping weight conversion")
                        # 如果无法获取价格，使用原始权重（可能是数量而不是权重）
                        target_positions_quantity[symbol] = weight
                except Exception as e:
                    logger.warning(f"Failed to convert weight to quantity for {symbol}: {e}, using original value")
                    target_positions_quantity[symbol] = weight
            
            logger.info(
                f"Converted weights to quantities: {len(target_positions_quantity)} symbols, "
                f"total_balance={total_balance:.2f} USDT, "
                f"total_weight={total_weight:.4f} (normalized to 1.0)"
            )
            
            return target_positions_quantity
            
        except Exception as e:
            logger.error(f"Failed to convert weights to quantities: {e}", exc_info=True)
            # 如果转换失败，返回原始值（可能是数量而不是权重）
            return target_positions
    
    async def place_market_order(self, symbol: str, side: str, quantity: float, reduce_only: bool = False) -> Optional[Dict]:
        """
        下市价单
        
        Args:
            symbol: 交易对
            side: 方向，'BUY' 或 'SELL'
            quantity: 数量
            reduce_only: 是否只减仓
        
        Returns:
            订单结果
        """
        try:
            result = await self.client.place_order(
                symbol=symbol,
                side=side,
                order_type='MARKET',
                quantity=quantity,
                position_side='BOTH',
                reduce_only=reduce_only
            )
            return result
        except Exception as e:
            logger.error(f"Failed to place market order for {symbol}: {e}", exc_info=True)
            return None
    
    async def place_twap_order(
        self, 
        symbol: str, 
        side: str, 
        total_quantity: float,
        interval: str = '5min',
        reduce_only: bool = False,
        duration_minutes: Optional[int] = None,
        max_splits: Optional[int] = None
    ) -> List[Dict]:
        """
        下TWAP订单（时间加权平均价格）
        
        Args:
            symbol: 交易对
            side: 方向，'BUY' 或 'SELL'
            total_quantity: 总数量
            interval: 时间间隔，如 '5min', '1min' 等
            reduce_only: 是否只减仓
        
        Returns:
            订单结果列表
        """
        try:
            # 解析时间间隔
            interval_minutes = self._parse_interval(interval)
            if interval_minutes is None:
                logger.error(f"Invalid interval: {interval}")
                return []
            
            # 确定执行时长和分割次数
            if duration_minutes is None:
                duration_minutes = config.get('execution.twap.default_duration_minutes', 60)
            
            # 计算分割次数
            num_splits = max(1, int(duration_minutes / interval_minutes))
            if max_splits is None:
                max_splits = config.get('execution.twap.max_splits')
            if max_splits is not None:
                num_splits = min(num_splits, max_splits)
            
            # 计算每个时间段的订单数量
            quantity_per_order = total_quantity / num_splits
            
            # 获取symbol信息以规范化数量
            symbol_info = await self.get_symbol_info(symbol)
            step_size = symbol_info.get('step_size', 0.01)
            quantity_per_order = round_qty(quantity_per_order, step_size)
            
            if quantity_per_order < symbol_info.get('min_qty', 0.001):
                logger.warning(f"TWAP order quantity per split {quantity_per_order} is too small for {symbol}")
                # 如果每单数量太小，只下一单
                order_result = await self.place_market_order(symbol, side, total_quantity, reduce_only)
                return [order_result] if order_result else []
            
            orders = []
            start_time = datetime.now(timezone.utc)
            
            for i in range(num_splits):
                # 计算预期执行时间
                expected_time = start_time + timedelta(minutes=i * interval_minutes)
                current_time = datetime.now(timezone.utc)
                
                # 如果还没到执行时间，等待
                if current_time < expected_time:
                    wait_seconds = (expected_time - current_time).total_seconds()
                    if wait_seconds > 0:
                        await asyncio.sleep(wait_seconds)
                
                order_result = await self.place_market_order(symbol, side, quantity_per_order, reduce_only)
                if order_result:
                    orders.append(order_result)
                    logger.debug(
                        f"TWAP order split {i+1}/{num_splits} for {symbol}: {quantity_per_order} {side}, "
                        f"orderId={order_result.get('orderId')}"
                    )
                else:
                    logger.warning(f"TWAP order split {i+1}/{num_splits} for {symbol} failed")
            
            logger.info(
                f"TWAP order completed for {symbol}: {len(orders)}/{num_splits} orders executed, "
                f"total executed: {sum(o.get('executedQty', 0) for o in orders):.6f}"
            )
            return orders
            
        except Exception as e:
            logger.error(f"Failed to place TWAP order for {symbol}: {e}", exc_info=True)
            return []
    
    async def place_vwap_order(
        self, 
        symbol: str, 
        side: str, 
        total_quantity: float,
        interval: str = '5min',
        reduce_only: bool = False,
        duration_minutes: Optional[int] = None,
        lookback_days: Optional[int] = None
    ) -> List[Dict]:
        """
        下VWAP订单（成交量加权平均价格）
        
        VWAP策略：根据历史成交量分布来分配订单，在成交量大的时间段分配更多订单
        
        Args:
            symbol: 交易对
            side: 方向，'BUY' 或 'SELL'
            total_quantity: 总数量
            interval: 每个子订单之间的时间间隔，如 '5min', '1min' 等
            reduce_only: 是否只减仓
            duration_minutes: 总执行时长（分钟），如果不指定，默认使用interval和历史数据计算
            lookback_days: 用于分析成交量分布的历史数据天数（默认5天）
        
        Returns:
            订单结果列表
        """
        try:
            # 解析时间间隔
            interval_minutes = self._parse_interval(interval)
            if interval_minutes is None:
                logger.error(f"Invalid interval: {interval}")
                return []
            
            # 获取历史成交量数据用于VWAP分配
            try:
                from ..data.api import get_data_api
                from datetime import timedelta
                
                data_api = get_data_api()
                
                # 计算时间范围
                if lookback_days is None:
                    lookback_days = config.get('execution.vwap.lookback_days', 5)
                end_time = datetime.now(timezone.utc)
                start_time = end_time - timedelta(days=lookback_days)
                
                # 获取历史K线数据（使用5分钟数据）
                begin_label = data_api._get_date_time_label_from_datetime(start_time)
                end_label = data_api._get_date_time_label_from_datetime(end_time)
                
                bars = data_api.get_bar_between(begin_label, end_label, mode='5min')
                
                # 获取该symbol的数据（需要转换为系统格式：BTCUSDT -> btc-usdt）
                from ..common.utils import to_system_symbol
                symbol_key = to_system_symbol(symbol)
                if symbol_key not in bars or bars[symbol_key].empty:
                    logger.warning(
                        f"VWAP order for {symbol}: no historical volume data available, "
                        f"falling back to TWAP"
                    )
                    return await self.place_twap_order(symbol, side, total_quantity, interval, reduce_only, duration_minutes)
                
                df = bars[symbol_key]
                
                # 计算每个时间段的平均成交量（按小时内的5分钟窗口分组）
                if 'quote_volume' in df.columns:
                    volume_col = 'quote_volume'
                elif 'dolvol' in df.columns:
                    volume_col = 'dolvol'
                else:
                    logger.warning(f"VWAP order for {symbol}: no volume column found, using TWAP")
                    return await self.place_twap_order(symbol, side, total_quantity, interval, reduce_only, duration_minutes)
                
                # 按小时内的分钟数分组（0-55分钟，每5分钟一组）
                df['hour_minute'] = df['open_time'].dt.hour * 60 + (df['open_time'].dt.minute // interval_minutes) * interval_minutes
                
                # 计算每个时间段的平均成交量
                avg_volumes = df.groupby('hour_minute')[volume_col].mean().to_dict()
                
                if not avg_volumes:
                    logger.warning(f"VWAP order for {symbol}: no volume distribution data, using TWAP")
                    return await self.place_twap_order(symbol, side, total_quantity, interval, reduce_only, duration_minutes)
                
                # 确定执行时长和分割次数
                if duration_minutes is None:
                    duration_minutes = config.get('execution.vwap.default_duration_minutes', 60)
                
                max_splits = config.get('execution.vwap.max_splits', 60)
                num_splits = min(max(1, int(duration_minutes / interval_minutes)), max_splits)
                
                # 计算每个时间段在当前执行窗口内的权重
                current_hour = datetime.now(timezone.utc).hour
                current_minute = (datetime.now(timezone.utc).minute // interval_minutes) * interval_minutes
                
                weights = []
                time_slots = []
                total_weight = 0.0
                
                for i in range(num_splits):
                    slot_minute = (current_minute + i * interval_minutes) % 60
                    slot_hour_minute = current_hour * 60 + slot_minute
                    
                    # 如果这个时间段有历史数据，使用历史平均成交量作为权重
                    # 否则使用平均权重
                    if slot_hour_minute in avg_volumes:
                        weight = avg_volumes[slot_hour_minute]
                    else:
                        # 使用所有时间段的平均值
                        weight = sum(avg_volumes.values()) / len(avg_volumes) if avg_volumes else 1.0
                    
                    weights.append(weight)
                    time_slots.append(slot_hour_minute)
                    total_weight += weight
                
                # 如果总权重为0，使用均匀分配
                if total_weight == 0:
                    logger.warning(f"VWAP order for {symbol}: total weight is 0, using TWAP")
                    return await self.place_twap_order(symbol, side, total_quantity, interval, reduce_only, duration_minutes)
                
                # 根据权重分配订单数量
                quantities = []
                for weight in weights:
                    qty = (weight / total_weight) * total_quantity
                    quantities.append(qty)
                
                # 获取symbol信息以规范化数量
                symbol_info = await self.get_symbol_info(symbol)
                step_size = symbol_info.get('step_size', 0.01)
                min_qty = symbol_info.get('min_qty', 0.001)
                
                # 规范化每个子订单的数量
                normalized_quantities = [round_qty(qty, step_size) for qty in quantities]
                
                # 检查并调整最小数量
                adjusted_quantities = []
                for i, qty in enumerate(normalized_quantities):
                    if qty < min_qty:
                        # 如果数量太小，合并到下一个订单或最后一个订单
                        if i < len(normalized_quantities) - 1:
                            normalized_quantities[i + 1] += qty
                            qty = 0.0
                        else:
                            # 最后一个订单，如果太小则合并到前一个
                            if adjusted_quantities:
                                adjusted_quantities[-1] += qty
                                qty = 0.0
                    adjusted_quantities.append(qty)
                
                # 过滤掉0数量的订单
                final_quantities = [(qty, i) for i, qty in enumerate(adjusted_quantities) if qty >= min_qty]
                
                if not final_quantities:
                    logger.error(f"VWAP order for {symbol}: all quantities too small after normalization")
                    return []
                
                logger.info(
                    f"VWAP order for {symbol}: {total_quantity} {side} over {duration_minutes} minutes, "
                    f"{len(final_quantities)} splits based on volume distribution"
                )
                
                orders = []
                start_time = datetime.now(timezone.utc)
                
                for qty, slot_idx in final_quantities:
                    # 计算预期执行时间
                    expected_time = start_time + timedelta(minutes=slot_idx * interval_minutes)
                    current_time = datetime.now(timezone.utc)
                    
                    # 如果还没到执行时间，等待
                    if current_time < expected_time:
                        wait_seconds = (expected_time - current_time).total_seconds()
                        if wait_seconds > 0:
                            await asyncio.sleep(wait_seconds)
                    
                    # 执行子订单
                    order_result = await self.place_market_order(symbol, side, qty, reduce_only)
                    if order_result:
                        orders.append(order_result)
                        logger.debug(
                            f"VWAP order slot {slot_idx+1} for {symbol}: {qty} {side} "
                            f"(weight={weights[slot_idx]:.2f}), orderId={order_result.get('orderId')}"
                        )
                    else:
                        logger.warning(f"VWAP order slot {slot_idx+1} for {symbol} failed")
                
                logger.info(
                    f"VWAP order completed for {symbol}: {len(orders)}/{len(final_quantities)} orders executed, "
                    f"total executed: {sum(o.get('executedQty', 0) for o in orders):.6f}"
                )
                return orders
                
            except ImportError:
                logger.warning(f"VWAP order for {symbol}: cannot import data_api, using TWAP")
                return await self.place_twap_order(symbol, side, total_quantity, interval, reduce_only, duration_minutes)
            except Exception as e:
                logger.warning(f"VWAP order for {symbol}: error getting volume data ({e}), using TWAP")
                return await self.place_twap_order(symbol, side, total_quantity, interval, reduce_only, duration_minutes)
            
        except Exception as e:
            logger.error(f"Failed to place VWAP order for {symbol}: {e}", exc_info=True)
            return []
    
    def _parse_interval(self, interval: str) -> Optional[int]:
        """
        解析时间间隔字符串为分钟数
        
        Args:
            interval: 时间间隔字符串，如 '5min', '1min', '1h' 等
        
        Returns:
            分钟数，如果解析失败返回None
        """
        try:
            interval = interval.lower().strip()
            if interval.endswith('min'):
                return int(interval[:-3])
            elif interval.endswith('h'):
                return int(interval[:-1]) * 60
            elif interval.endswith('m'):
                return int(interval[:-1])
            else:
                # 尝试直接解析为数字（假设是分钟）
                return int(interval)
        except (ValueError, AttributeError):
            return None