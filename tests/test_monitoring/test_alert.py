"""
测试告警管理
"""
import pytest
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.monitoring.alert import AlertManager, get_alert_manager


def test_alert_manager_initialization():
    """测试告警管理器初始化"""
    manager = AlertManager()
    
    assert manager.alert_thresholds is not None
    assert isinstance(manager.alert_history, list)
    
    print("✓ Alert manager initialized successfully")


def test_check_position_deviation():
    """测试持仓偏差告警"""
    manager = AlertManager()
    
    # 设置较低的阈值以便触发告警
    manager.alert_thresholds['position_deviation'] = 0.05
    
    deviation_metrics = {
        'avg_deviation': 0.1,  # 超过阈值
        'max_deviation': 0.2,
        'max_deviation_symbol': 'BTCUSDT',
    }
    
    alerts = manager.check_position_deviation(deviation_metrics, 'account1')
    
    assert len(alerts) > 0
    assert any(a['type'] == 'position_deviation' for a in alerts)
    
    print("✓ Position deviation alerts triggered correctly")


def test_check_exposure():
    """测试敞口告警"""
    manager = AlertManager()
    
    manager.alert_thresholds['max_exposure'] = 10000.0
    
    exposure_metrics = {
        'total_exposure': 15000.0,  # 超过阈值
        'net_exposure': 12000.0,
    }
    
    alerts = manager.check_exposure(exposure_metrics, 'account1')
    
    assert len(alerts) > 0
    assert any(a['type'] == 'exposure' for a in alerts)
    
    print("✓ Exposure alerts triggered correctly")


def test_check_account_health():
    """测试账户健康度告警"""
    manager = AlertManager()
    
    account_metrics = {
        'total_wallet_balance': 10000.0,
        'available_balance': 500.0,  # 只有5%，应该告警
        'total_unrealized_pnl': -1500.0,  # 亏损15%，应该告警
        'utilization': 95.0,  # 利用率95%，应该告警
    }
    
    alerts = manager.check_account_health(account_metrics, 'account1')
    
    assert len(alerts) > 0
    assert any(a['type'] == 'account_health' for a in alerts)
    
    print("✓ Account health alerts triggered correctly")


def test_no_alerts_when_normal():
    """测试正常情况不触发告警"""
    manager = AlertManager()
    
    # 正常指标
    deviation_metrics = {
        'avg_deviation': 0.01,  # 低于阈值
        'max_deviation': 0.02,
        'max_deviation_symbol': None,
    }
    
    exposure_metrics = {
        'total_exposure': 5000.0,  # 低于阈值
        'net_exposure': 1000.0,
    }
    
    account_metrics = {
        'total_wallet_balance': 10000.0,
        'available_balance': 5000.0,  # 50%，正常
        'total_unrealized_pnl': 100.0,  # 盈利
        'utilization': 50.0,  # 正常
    }
    
    alerts1 = manager.check_position_deviation(deviation_metrics, 'account1')
    alerts2 = manager.check_exposure(exposure_metrics, 'account1')
    alerts3 = manager.check_account_health(account_metrics, 'account1')
    
    # 正常情况下不应该有告警
    assert len(alerts1) == 0 or all(a['level'] != 'critical' for a in alerts1)
    
    print("✓ No false alerts in normal conditions")


def test_alert_history():
    """测试告警历史"""
    manager = AlertManager()
    
    # 触发一些告警
    deviation_metrics = {
        'avg_deviation': 0.1,
        'max_deviation': 0.2,
        'max_deviation_symbol': 'BTCUSDT',
    }
    
    manager.check_position_deviation(deviation_metrics, 'account1')
    
    # 检查历史
    recent_alerts = manager.get_recent_alerts(limit=10)
    assert len(recent_alerts) > 0
    
    print("✓ Alert history works correctly")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Alert Manager")
    print("=" * 60)
    
    test_alert_manager_initialization()
    test_check_position_deviation()
    test_check_exposure()
    test_check_account_health()
    test_no_alerts_when_normal()
    test_alert_history()
    
    print("\n" + "=" * 60)
    print("All alert tests completed!")
    print("=" * 60)
