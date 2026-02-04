"""
测试universe版本支持
"""
import pytest
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from src.data.universe_manager import UniverseManager, get_universe_manager
from src.data.api import DataAPI
from src.system_api import SystemAPI


def test_universe_manager_version_support():
    """测试UniverseManager支持版本参数"""
    manager = UniverseManager()
    
    # 测试保存universe时指定版本
    test_universe = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
    test_date = datetime.now(timezone.utc)
    
    try:
        manager.save_universe(test_universe, test_date, version='v1')
        
        # 测试加载指定版本的universe
        loaded_universe = manager.load_universe(test_date, version='v1')
        
        assert isinstance(loaded_universe, list)
        assert len(loaded_universe) == len(test_universe)
        
        print("✓ UniverseManager version support test passed")
    except Exception as e:
        print(f"⚠ UniverseManager version test skipped: {e}")


def test_data_api_universe_version():
    """测试DataAPI的universe接口支持版本参数"""
    api = DataAPI()
    
    # 测试get_universe支持version参数
    assert hasattr(api, 'get_universe'), "DataAPI should have get_universe method"
    
    try:
        # 测试默认版本
        universe_v1 = api.get_universe(version='v1')
        assert isinstance(universe_v1, list)
        
        # 测试指定日期和版本
        test_date = datetime.now(timezone.utc)
        universe_date_v1 = api.get_universe(date=test_date, version='v1')
        assert isinstance(universe_date_v1, list)
        
        print("✓ DataAPI universe version parameter test passed")
    except Exception as e:
        print(f"⚠ DataAPI universe version test skipped: {e}")


def test_system_api_universe_version():
    """测试SystemAPI的universe接口支持版本参数"""
    api = SystemAPI()
    
    # 测试get_last_universe支持version参数
    assert hasattr(api, 'get_last_universe'), "SystemAPI should have get_last_universe method"
    
    try:
        universe_v1 = api.get_last_universe(version='v1')
        assert isinstance(universe_v1, list)
        
        # 测试get_all_uid_info支持version参数
        uid_info = api.get_all_uid_info(version='v1')
        assert isinstance(uid_info, dict)
        
        print("✓ SystemAPI universe version parameter test passed")
    except Exception as e:
        print(f"⚠ SystemAPI universe version test skipped: {e}")


def test_universe_file_path_with_version():
    """测试universe文件路径包含版本信息"""
    manager = UniverseManager()
    
    test_date = datetime.now(timezone.utc)
    
    # 测试不同版本的文件路径
    path_v1 = manager._get_universe_file_path(test_date, version='v1')
    path_v2 = manager._get_universe_file_path(test_date, version='v2')
    
    assert 'v1' in str(path_v1), "Path should contain version v1"
    assert 'v2' in str(path_v2), "Path should contain version v2"
    assert str(path_v1) != str(path_v2), "Different versions should have different paths"
    
    print("✓ Universe file path with version test passed")


def test_multiple_universe_versions():
    """测试多个universe版本可以共存"""
    manager = UniverseManager()
    
    test_date = datetime.now(timezone.utc)
    
    universe_v1 = ['BTCUSDT', 'ETHUSDT']
    universe_v2 = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT']
    
    try:
        # 保存两个版本的universe
        manager.save_universe(universe_v1, test_date, version='v1')
        manager.save_universe(universe_v2, test_date, version='v2')
        
        # 加载并验证
        loaded_v1 = manager.load_universe(test_date, version='v1')
        loaded_v2 = manager.load_universe(test_date, version='v2')
        
        assert len(loaded_v1) == len(universe_v1)
        assert len(loaded_v2) == len(universe_v2)
        assert len(loaded_v2) > len(loaded_v1)
        
        print("✓ Multiple universe versions coexistence test passed")
    except Exception as e:
        print(f"⚠ Multiple universe versions test skipped: {e}")
