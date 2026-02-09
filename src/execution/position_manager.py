"""
持仓管理模块
计算当前持仓与目标持仓的偏差，生成订单
"""
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime

from ..common.logger import get_logger
from ..common.utils import format_symbol, round_qty
from .binance_client import BinanceClient

logger = get_logger('position_manager')


class PositionManager:
    """持仓管理器"""
    
    def __init__(self, binance_client: BinanceClient):
        """
        初始化持仓管理器
        
        Args:
            binance_client: Binance API客户端
        """
        self.client = binance_client
        self.current_positions: Dict[str, Dict] = {}  # symbol -> position info
    
    async def update_current_positions(self):
        """更新当前持仓"""
        try:
            positions = await self.client.get_positions()
            
            self.current_positions = {}
            for pos in positions:
                symbol = format_symbol(pos.get('symbol', ''))
                position_amt = float(pos.get('positionAmt', 0))
                
                if abs(position_amt) > 1e-8:  # 持仓量不为0
                    self.current_positions[symbol] = {
                        'position_amt': position_amt,
                        'entry_price': float(pos.get('entryPrice', 0)),
                        'unrealized_pnl': float(pos.get('unRealizedProfit', 0)),
                        'leverage': int(pos.get('leverage', 1)),
                        'margin_type': pos.get('marginType', ''),
                    }
            
            logger.debug(f"Updated current positions: {len(self.current_positions)} symbols")
            
        except Exception as e:
            # 在dry-run模式下，如果获取持仓失败，使用空持仓（不影响测试）
            if hasattr(self.client, 'dry_run_mode') and self.client.dry_run_mode:
                logger.debug(f"Dry-run mode: using empty positions due to error: {e}")
                self.current_positions = {}
            else:
                logger.error(f"Failed to update current positions: {e}", exc_info=True)
                raise
    
    def calculate_position_diff(
        self,
        target_positions: Dict[str, float]
    ) -> List[Dict]:
        """
        计算当前持仓与目标持仓的偏差
        
        Args:
            target_positions: Dict[symbol, target_position]，目标持仓
        
        Returns:
            订单列表，每个订单包含: symbol, side, quantity, order_type等
        """
        orders = []
        
        try:
            # 获取所有需要调整的交易对
            all_symbols = set(list(target_positions.keys()) + list(self.current_positions.keys()))
            
            for symbol in all_symbols:
                target_pos = target_positions.get(symbol, 0.0)
                current_pos = self.current_positions.get(symbol, {}).get('position_amt', 0.0)
                
                diff = target_pos - current_pos
                
                # 如果偏差很小，跳过（避免频繁小额调整）
                # 注意：测试模式下可能使用非常小的持仓（0.0001），所以阈值降低到0.00001
                if abs(diff) < 0.00001:
                    continue
                
                # 检测反向开仓：当前持仓和目标持仓方向相反
                # 在单向持仓模式下，反向开仓需要先平仓，再开仓（两个订单）
                is_reverse = (current_pos > 0 and target_pos < 0) or (current_pos < 0 and target_pos > 0)
                
                if is_reverse:
                    # 反向开仓：需要先平仓，再开仓（两个订单）
                    # 订单1：平掉当前持仓
                    close_quantity = abs(current_pos)
                    close_side = 'SELL' if current_pos > 0 else 'BUY'
                    
                    orders.append({
                        'symbol': format_symbol(symbol),
                        'side': close_side,
                        'quantity': close_quantity,
                        'reduce_only': True,  # 平仓订单必须是reduce_only
                        'current_position': current_pos,
                        'target_position': 0.0,  # 平仓后目标为0
                        'diff': -current_pos,  # 平仓的差值
                    })
                    
                    # 订单2：开新方向持仓（如果目标持仓不为0）
                    if abs(target_pos) > 1e-8:
                        open_quantity = abs(target_pos)
                        open_side = 'BUY' if target_pos > 0 else 'SELL'
                        
                        orders.append({
                            'symbol': format_symbol(symbol),
                            'side': open_side,
                            'quantity': open_quantity,
                            'reduce_only': False,  # 开仓订单不能是reduce_only
                            'current_position': 0.0,  # 平仓后当前持仓为0
                            'target_position': target_pos,
                            'diff': target_pos,  # 开仓的差值
                        })
                else:
                    # 正常情况：同方向调整（现有逻辑）
                    if diff > 0:
                        side = 'BUY'
                        quantity = abs(diff)
                    else:
                        side = 'SELL'
                        quantity = abs(diff)
                    
                    # 判断是开仓还是平仓
                    reduce_only = False
                    if current_pos > 0 and diff < 0:
                        # 当前多仓，目标减少 -> 平多仓
                        reduce_only = True
                    elif current_pos < 0 and diff > 0:
                        # 当前空仓，目标增加 -> 平空仓
                        reduce_only = True
                    
                    orders.append({
                        'symbol': format_symbol(symbol),
                        'side': side,
                        'quantity': quantity,
                        'reduce_only': reduce_only,
                        'current_position': current_pos,
                        'target_position': target_pos,
                        'diff': diff,
                    })
            
            logger.info(f"Calculated {len(orders)} orders from position differences")
            return orders
            
        except Exception as e:
            logger.error(f"Failed to calculate position diff: {e}", exc_info=True)
            return []
