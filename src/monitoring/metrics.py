"""
监控指标计算模块
计算各种监控指标：多空敞口、持仓偏差、账户资产等
"""
from typing import Dict, List, Optional
from datetime import datetime, timezone
from decimal import Decimal

from ..common.logger import get_logger
from ..common.utils import format_symbol

logger = get_logger('monitoring_metrics')


class MetricsCalculator:
    """指标计算器"""
    
    def calculate_exposure(self, positions: List[Dict]) -> Dict:
        """
        计算多空敞口
        
        Args:
            positions: 持仓列表，每个持仓包含positionAmt, entryPrice等
        
        Returns:
            敞口信息字典
        """
        try:
            total_long_value = Decimal('0')
            total_short_value = Decimal('0')
            total_long_qty = Decimal('0')
            total_short_qty = Decimal('0')
            
            for pos in positions:
                symbol = format_symbol(pos.get('symbol', ''))
                position_amt = Decimal(str(pos.get('positionAmt', 0)))
                entry_price = Decimal(str(pos.get('entryPrice', 0)))
                
                if position_amt > 0:
                    # 多头
                    position_value = position_amt * entry_price
                    total_long_value += position_value
                    total_long_qty += position_amt
                elif position_amt < 0:
                    # 空头
                    position_value = abs(position_amt) * entry_price
                    total_short_value += position_value
                    total_short_qty += abs(position_amt)
            
            # 净敞口 = 多头敞口 - 空头敞口
            net_exposure = float(total_long_value - total_short_value)
            total_exposure = float(total_long_value + total_short_value)
            
            # 敞口比例
            if total_exposure > 0:
                long_ratio = float(total_long_value) / total_exposure
                short_ratio = float(total_short_value) / total_exposure
            else:
                long_ratio = 0.0
                short_ratio = 0.0
            
            return {
                'total_long_value': float(total_long_value),
                'total_short_value': float(total_short_value),
                'total_long_qty': float(total_long_qty),
                'total_short_qty': float(total_short_qty),
                'net_exposure': net_exposure,
                'total_exposure': total_exposure,
                'long_ratio': long_ratio,
                'short_ratio': short_ratio,
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate exposure: {e}", exc_info=True)
            return {
                'total_long_value': 0.0,
                'total_short_value': 0.0,
                'net_exposure': 0.0,
                'total_exposure': 0.0,
                'long_ratio': 0.0,
                'short_ratio': 0.0,
            }
    
    def calculate_position_deviation(
        self,
        current_positions: Dict[str, float],
        target_positions: Dict[str, float]
    ) -> Dict:
        """
        计算持仓偏差
        
        Args:
            current_positions: Dict[symbol, position_amt]，当前持仓
            target_positions: Dict[symbol, position_amt]，目标持仓
        
        Returns:
            偏差信息字典
        """
        try:
            deviations = {}
            total_deviation_value = 0.0
            max_deviation = 0.0
            max_deviation_symbol = None
            
            # 获取所有交易对
            all_symbols = set(list(current_positions.keys()) + list(target_positions.keys()))
            
            total_deviation_pct = 0.0
            symbols_with_target = 0
            max_deviation_pct = 0.0
            max_deviation_pct_symbol = None
            
            for symbol in all_symbols:
                current_pos = current_positions.get(symbol, 0.0)
                target_pos = target_positions.get(symbol, 0.0)
                
                deviation = abs(current_pos - target_pos)
                
                # 计算偏差百分比（相对于目标持仓）
                if abs(target_pos) > 1e-8:
                    deviation_pct = deviation / abs(target_pos)
                    total_deviation_pct += deviation_pct
                    symbols_with_target += 1
                else:
                    # 如果目标持仓为0，但实际持仓不为0，偏差为100%
                    deviation_pct = 1.0 if abs(current_pos) > 1e-8 else 0.0
                    if abs(current_pos) > 1e-8:
                        total_deviation_pct += deviation_pct
                        symbols_with_target += 1
                
                # 记录最大绝对偏差（用于其他用途）
                if deviation > max_deviation:
                    max_deviation = deviation
                    max_deviation_symbol = symbol
                
                # 记录最大百分比偏差（用于告警）
                if deviation_pct > max_deviation_pct:
                    max_deviation_pct = deviation_pct
                    max_deviation_pct_symbol = symbol
                
                total_deviation_value += deviation
                
                deviations[symbol] = {
                    'current': current_pos,
                    'target': target_pos,
                    'deviation': deviation,
                    'deviation_pct': deviation_pct,
                }
            
            # 平均偏差（使用百分比偏差的平均值，而不是绝对偏差）
            avg_deviation = total_deviation_pct / symbols_with_target if symbols_with_target > 0 else 0.0
            
            return {
                'deviations': deviations,
                'total_deviation': total_deviation_value,
                'avg_deviation': avg_deviation,  # 百分比偏差的平均值
                'max_deviation': max_deviation_pct,  # 最大百分比偏差（用于告警）
                'max_deviation_symbol': max_deviation_pct_symbol,  # 最大百分比偏差的交易对
                'max_deviation_abs': max_deviation,  # 最大绝对偏差（保留用于其他用途）
                'max_deviation_abs_symbol': max_deviation_symbol,  # 最大绝对偏差的交易对
                'symbols_count': len(all_symbols),
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate position deviation: {e}", exc_info=True)
            return {
                'deviations': {},
                'total_deviation': 0.0,
                'avg_deviation': 0.0,
                'max_deviation': 0.0,
                'max_deviation_symbol': None,
                'symbols_count': 0,
            }
    
    def calculate_account_metrics(self, account_info: Dict, positions: List[Dict]) -> Dict:
        """
        计算账户指标
        
        Args:
            account_info: 账户信息（从Binance API获取）
            positions: 持仓列表
        
        Returns:
            账户指标字典
        """
        try:
            # 账户余额
            total_wallet_balance = float(account_info.get('totalWalletBalance', 0))
            available_balance = float(account_info.get('availableBalance', 0))
            margin_balance = float(account_info.get('totalMarginBalance', 0))
            
            # 未实现盈亏
            total_unrealized_pnl = float(account_info.get('totalUnrealizedProfit', 0))
            
            # 计算持仓市值（使用当前价格，而不是开仓价格）
            position_value = 0.0
            for pos in positions:
                position_amt = float(pos.get('positionAmt', 0))
                # 优先使用标记价格（markPrice），如果没有则使用开仓价格
                mark_price = float(pos.get('markPrice', 0))
                entry_price = float(pos.get('entryPrice', 0))
                current_price = mark_price if mark_price > 0 else entry_price
                if abs(position_amt) > 1e-8 and current_price > 0:
                    position_value += abs(position_amt) * current_price
            
            # 持仓数量
            open_positions_count = len([p for p in positions if abs(float(p.get('positionAmt', 0))) > 1e-8])
            
            # 计算敞口
            exposure = self.calculate_exposure(positions)
            
            # 账户利用率（持仓市值 / 账户余额）
            # 注意：在杠杆交易中，持仓市值可能超过账户余额，这是正常的
            # 但为了监控风险，我们仍然计算这个比率
            utilization = (position_value / total_wallet_balance * 100) if total_wallet_balance > 0 else 0.0
            
            # 如果账户余额很小（接近0），利用率可能会异常高，需要特殊处理
            if total_wallet_balance < 1.0 and position_value > 0:
                logger.warning(
                    f"Account balance is very small ({total_wallet_balance:.2f}), "
                    f"utilization calculation may be inaccurate: {utilization:.2f}%"
                )
            
            return {
                'total_wallet_balance': total_wallet_balance,
                'available_balance': available_balance,
                'margin_balance': margin_balance,
                'total_unrealized_pnl': total_unrealized_pnl,
                'position_value': position_value,
                'open_positions_count': open_positions_count,
                'utilization': utilization,
                'exposure': exposure,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate account metrics: {e}", exc_info=True)
            return {
                'total_wallet_balance': 0.0,
                'available_balance': 0.0,
                'margin_balance': 0.0,
                'total_unrealized_pnl': 0.0,
                'position_value': 0.0,
                'open_positions_count': 0,
                'utilization': 0.0,
                'exposure': {},
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }


# 全局实例
_metrics_calculator: Optional[MetricsCalculator] = None


def get_metrics_calculator() -> MetricsCalculator:
    """获取指标计算器实例"""
    global _metrics_calculator
    if _metrics_calculator is None:
        _metrics_calculator = MetricsCalculator()
    return _metrics_calculator
