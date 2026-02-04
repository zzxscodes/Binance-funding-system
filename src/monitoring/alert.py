"""
告警处理模块
根据监控指标触发告警
"""
from typing import Dict, List, Optional
from datetime import datetime, timezone

from ..common.config import config
from ..common.logger import get_logger

logger = get_logger('monitoring_alert')


class AlertManager:
    """告警管理器"""
    
    def __init__(self):
        self.alert_thresholds = config.get('monitoring.alert_thresholds', {})
        self.alert_history: List[Dict] = []
        self.max_history = 1000
    
    def check_position_deviation(self, deviation_metrics: Dict, account_id: str) -> List[Dict]:
        """
        检查持仓偏差告警
        
        Args:
            deviation_metrics: 持仓偏差指标
            account_id: 账户ID
        
        Returns:
            告警列表
        """
        alerts = []
        
        try:
            threshold = self.alert_thresholds.get('position_deviation', 0.05)  # 默认5%
            
            avg_deviation = deviation_metrics.get('avg_deviation', 0.0)
            max_deviation = deviation_metrics.get('max_deviation', 0.0)
            max_deviation_symbol = deviation_metrics.get('max_deviation_symbol')
            
            # 检查平均偏差
            if avg_deviation > threshold:
                alert = {
                    'type': 'position_deviation',
                    'level': 'warning',
                    'account_id': account_id,
                    'message': f"Average position deviation {avg_deviation:.2%} exceeds threshold {threshold:.2%}",
                    'metrics': {
                        'avg_deviation': avg_deviation,
                        'threshold': threshold,
                    },
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                }
                alerts.append(alert)
                self._add_alert(alert)
            
            # 检查最大偏差（使用百分比偏差）
            max_deviation_multiplier = self.alert_thresholds.get('max_deviation_multiplier', 2.0)  # 默认2倍
            max_deviation_threshold = threshold * max_deviation_multiplier
            
            # 从deviations中获取最大偏差的百分比
            deviations = deviation_metrics.get('deviations', {})
            max_deviation_pct = 0.0
            if max_deviation_symbol and max_deviation_symbol in deviations:
                max_deviation_pct = deviations[max_deviation_symbol].get('deviation_pct', 0.0)
            
            if max_deviation_symbol and max_deviation_pct > max_deviation_threshold:
                alert = {
                    'type': 'position_deviation',
                    'level': 'critical',
                    'account_id': account_id,
                    'message': f"Max position deviation for {max_deviation_symbol}: {max_deviation_pct:.2%} (threshold: {max_deviation_threshold:.2%})",
                    'metrics': {
                        'symbol': max_deviation_symbol,
                        'deviation': max_deviation_pct,
                        'threshold': max_deviation_threshold,
                    },
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                }
                alerts.append(alert)
                self._add_alert(alert)
            
        except Exception as e:
            logger.error(f"Failed to check position deviation alerts: {e}", exc_info=True)
        
        return alerts
    
    def check_exposure(self, exposure_metrics: Dict, account_id: str) -> List[Dict]:
        """
        检查敞口告警
        
        Args:
            exposure_metrics: 敞口指标
            account_id: 账户ID
        
        Returns:
            告警列表
        """
        alerts = []
        
        try:
            max_exposure = self.alert_thresholds.get('max_exposure', 1000000)  # 默认100万USDT
            
            total_exposure = exposure_metrics.get('total_exposure', 0.0)
            net_exposure = abs(exposure_metrics.get('net_exposure', 0.0))
            
            # 检查总敞口
            if total_exposure > max_exposure:
                alert = {
                    'type': 'exposure',
                    'level': 'warning',
                    'account_id': account_id,
                    'message': f"Total exposure {total_exposure:,.0f} USDT exceeds threshold {max_exposure:,.0f} USDT",
                    'metrics': {
                        'total_exposure': total_exposure,
                        'threshold': max_exposure,
                    },
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                }
                alerts.append(alert)
                self._add_alert(alert)
            
            # 检查净敞口（如果偏向一边太多）
            if total_exposure > 0:
                net_ratio = net_exposure / total_exposure
                # 从配置读取阈值，默认80%
                net_exposure_threshold = self.alert_thresholds.get('net_exposure_ratio', 0.8)
                if net_ratio > net_exposure_threshold:
                    alert = {
                        'type': 'exposure',
                        'level': 'warning',
                        'account_id': account_id,
                        'message': f"Net exposure ratio {net_ratio:.2%} is too high (unbalanced, threshold: {net_exposure_threshold:.2%})",
                        'metrics': {
                            'net_exposure': net_exposure,
                            'total_exposure': total_exposure,
                            'net_ratio': net_ratio,
                            'threshold': net_exposure_threshold,
                        },
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                    }
                    alerts.append(alert)
                    self._add_alert(alert)
            
        except Exception as e:
            logger.error(f"Failed to check exposure alerts: {e}", exc_info=True)
        
        return alerts
    
    def check_account_health(self, account_metrics: Dict, account_id: str) -> List[Dict]:
        """
        检查账户健康度
        
        Args:
            account_metrics: 账户指标
            account_id: 账户ID
        
        Returns:
            告警列表
        """
        alerts = []
        
        try:
            available_balance = account_metrics.get('available_balance', 0.0)
            total_wallet_balance = account_metrics.get('total_wallet_balance', 0.0)
            total_unrealized_pnl = account_metrics.get('total_unrealized_pnl', 0.0)
            utilization = account_metrics.get('utilization', 0.0)
            
            # 检查可用余额过低
            if total_wallet_balance > 0:
                available_ratio = available_balance / total_wallet_balance
                min_available_ratio = self.alert_thresholds.get('min_available_ratio', 0.1)  # 默认10%
                if available_ratio < min_available_ratio:
                    alert = {
                        'type': 'account_health',
                        'level': 'warning',
                        'account_id': account_id,
                        'message': f"Available balance ratio {available_ratio:.2%} is too low (threshold: {min_available_ratio:.2%})",
                        'metrics': {
                            'available_balance': available_balance,
                            'total_wallet_balance': total_wallet_balance,
                            'available_ratio': available_ratio,
                            'threshold': min_available_ratio,
                        },
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                    }
                    alerts.append(alert)
                    self._add_alert(alert)
            
            # 检查未实现亏损过大
            if total_wallet_balance > 0:
                pnl_ratio = total_unrealized_pnl / total_wallet_balance
                max_unrealized_loss_ratio = self.alert_thresholds.get('max_unrealized_loss_ratio', 0.1)  # 默认10%
                if pnl_ratio < -max_unrealized_loss_ratio:
                    alert = {
                        'type': 'account_health',
                        'level': 'critical',
                        'account_id': account_id,
                        'message': f"Unrealized PnL {total_unrealized_pnl:,.0f} USDT ({pnl_ratio:.2%}) exceeds threshold ({-max_unrealized_loss_ratio:.2%})",
                        'metrics': {
                            'total_unrealized_pnl': total_unrealized_pnl,
                            'pnl_ratio': pnl_ratio,
                            'threshold': -max_unrealized_loss_ratio,
                        },
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                    }
                    alerts.append(alert)
                    self._add_alert(alert)
            
            # 检查账户利用率过高
            max_utilization = self.alert_thresholds.get('max_utilization', 90.0)  # 默认90%
            if utilization > max_utilization:
                alert = {
                    'type': 'account_health',
                    'level': 'warning',
                    'account_id': account_id,
                    'message': f"Account utilization {utilization:.2f}% exceeds threshold {max_utilization:.2f}%",
                    'metrics': {
                        'utilization': utilization,
                        'threshold': max_utilization,
                    },
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                }
                alerts.append(alert)
                self._add_alert(alert)
            
        except Exception as e:
            logger.error(f"Failed to check account health: {e}", exc_info=True)
        
        return alerts
    
    def _add_alert(self, alert: Dict):
        """添加告警到历史"""
        self.alert_history.append(alert)
        
        # 限制历史长度
        if len(self.alert_history) > self.max_history:
            self.alert_history = self.alert_history[-self.max_history:]
        
        # 记录日志
        level = alert.get('level', 'info')
        message = alert.get('message', '')
        
        if level == 'critical':
            logger.critical(f"ALERT: {message}")
        elif level == 'warning':
            logger.warning(f"ALERT: {message}")
        else:
            logger.info(f"ALERT: {message}")
    
    def get_recent_alerts(self, limit: int = 10) -> List[Dict]:
        """获取最近的告警"""
        return self.alert_history[-limit:]


# 全局实例
_alert_manager: Optional[AlertManager] = None


def get_alert_manager() -> AlertManager:
    """获取告警管理器实例"""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager
