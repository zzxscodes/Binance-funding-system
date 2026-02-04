#!/usr/bin/env python3
"""
运行所有测试
"""
import subprocess
import sys
import os
from pathlib import Path

def run_test(test_file):
    """运行单个测试文件"""
    print(f"\n{'='*60}")
    print(f"Running: {test_file}")
    print(f"{'='*60}\n")
    
    result = subprocess.run(
        [sys.executable, test_file],
        cwd=Path(__file__).parent
    )
    
    return result.returncode == 0

def main():
    """主函数"""
    test_files = [
        # 集成测试
        "tests/integration/test_binance_connection.py",
        # 数据层测试
        "tests/test_data/test_universe_manager.py",
        "tests/test_data/test_kline_aggregator.py",
        "tests/test_data/test_collector.py",
        # 策略模块测试
        "tests/test_strategy/test_calculator.py",
        "tests/test_strategy/test_position_generator.py",
        # 执行模块测试
        "tests/test_execution/test_binance_client.py",
        "tests/test_execution/test_position_manager.py",
        # 监控模块测试
        "tests/test_monitoring/test_metrics.py",
        "tests/test_monitoring/test_alert.py",
    ]
    
    print("="*60)
    print("Binance Quant Trading System - Test Suite")
    print("="*60)
    
    passed = 0
    failed = 0
    
    for test_file in test_files:
        test_path = Path(__file__).parent / test_file
        if not test_path.exists():
            print(f"\n⚠ Test file not found: {test_file}")
            continue
        
        if run_test(str(test_path)):
            passed += 1
            print(f"\n[PASS] {test_file}")
        else:
            failed += 1
            print(f"\n[FAIL] {test_file}")
    
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")
    print("="*60)
    
    if failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
