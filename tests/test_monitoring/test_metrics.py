"""
测试监控指标计算
"""
import pytest
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.monitoring.metrics import MetricsCalculator, get_metrics_calculator


def test_metrics_calculator_initialization():
    """测试指标计算器初始化"""
    calculator = MetricsCalculator()
    
    assert calculator is not None
    
    print("✓ Metrics calculator initialized successfully")


def test_calculate_exposure():
    """测试计算多空敞口"""
    calculator = MetricsCalculator()
    
    positions = [
        {
            'symbol': 'BTCUSDT',
            'positionAmt': '0.1',  # 多头
            'entryPrice': '50000.0',
        },
        {
            'symbol': 'ETHUSDT',
            'positionAmt': '-0.5',  # 空头
            'entryPrice': '3000.0',
        },
    ]
    
    exposure = calculator.calculate_exposure(positions)
    
    assert exposure['total_long_value'] == pytest.approx(5000.0, abs=0.01)  # 0.1 * 50000
    assert exposure['total_short_value'] == pytest.approx(1500.0, abs=0.01)  # 0.5 * 3000
    assert exposure['net_exposure'] == pytest.approx(3500.0, abs=0.01)  # 5000 - 1500
    assert exposure['total_exposure'] == pytest.approx(6500.0, abs=0.01)  # 5000 + 1500
    
    print("✓ Exposure calculation works correctly")


def test_calculate_position_deviation():
    """测试计算持仓偏差"""
    calculator = MetricsCalculator()
    
    current_positions = {
        'BTCUSDT': 0.1,
        'ETHUSDT': -0.5,
    }
    
    target_positions = {
        'BTCUSDT': 0.2,  # 偏差 0.1
        'ETHUSDT': -0.3,  # 偏差 0.2
        'BNBUSDT': 0.1,  # 当前没有，目标有
    }
    
    deviation = calculator.calculate_position_deviation(current_positions, target_positions)
    
    assert deviation['total_deviation'] > 0
    assert deviation['avg_deviation'] > 0
    assert 'BTCUSDT' in deviation['deviations']
    assert 'ETHUSDT' in deviation['deviations']
    assert 'BNBUSDT' in deviation['deviations']
    
    btc_dev = deviation['deviations']['BTCUSDT']
    assert btc_dev['current'] == 0.1
    assert btc_dev['target'] == 0.2
    assert btc_dev['deviation'] == pytest.approx(0.1, abs=0.001)
    
    print("✓ Position deviation calculation works correctly")


def test_calculate_account_metrics():
    """测试计算账户指标"""
    calculator = MetricsCalculator()
    
    account_info = {
        'totalWalletBalance': '10000.0',
        'availableBalance': '5000.0',
        'totalMarginBalance': '10100.0',
        'totalUnrealizedProfit': '100.0',
    }
    
    positions = [
        {
            'symbol': 'BTCUSDT',
            'positionAmt': '0.1',
            'entryPrice': '50000.0',
            'unRealizedProfit': '100.0',
        },
    ]
    
    metrics = calculator.calculate_account_metrics(account_info, positions)
    
    assert metrics['total_wallet_balance'] == 10000.0
    assert metrics['available_balance'] == 5000.0
    assert metrics['total_unrealized_pnl'] == 100.0
    assert metrics['open_positions_count'] == 1
    assert 'exposure' in metrics
    
    print("✓ Account metrics calculation works correctly")


def test_exposure_with_no_positions():
    """测试无持仓时的敞口计算"""
    calculator = MetricsCalculator()
    
    exposure = calculator.calculate_exposure([])
    
    assert exposure['total_long_value'] == 0.0
    assert exposure['total_short_value'] == 0.0
    assert exposure['net_exposure'] == 0.0
    
    print("✓ Exposure calculation handles empty positions correctly")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Metrics Calculator")
    print("=" * 60)
    
    test_metrics_calculator_initialization()
    test_calculate_exposure()
    test_calculate_position_deviation()
    test_calculate_account_metrics()
    test_exposure_with_no_positions()
    
    print("\n" + "=" * 60)
    print("All metrics tests completed!")
    print("=" * 60)
