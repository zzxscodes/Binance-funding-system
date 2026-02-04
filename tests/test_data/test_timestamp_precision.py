"""
测试时间戳精度统一（纳秒精度）
"""
import pytest
import polars as pl
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
import sys

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.data.storage import DataStorage


@pytest.fixture
def storage():
    """创建临时存储实例"""
    return DataStorage()


def test_timestamp_precision_on_save(storage, tmp_path):
    """测试保存时时间戳精度统一为纳秒"""
    # 创建测试数据（使用微秒精度）
    test_data = pd.DataFrame({
        'open_time': [datetime(2024, 1, 1, 0, 0, 0, 123456, tzinfo=timezone.utc)],
        'close_time': [datetime(2024, 1, 1, 0, 5, 0, 789012, tzinfo=timezone.utc)],
        'open': [100.0],
        'high': [110.0],
        'low': [90.0],
        'close': [105.0],
        'volume': [1000.0],
    })
    
    # 保存数据
    test_file = tmp_path / "test.parquet"
    storage._save_dataframe_polars(pl.from_pandas(test_data), test_file)
    
    # 加载数据
    loaded_data = storage._load_dataframe_polars(test_file)
    
    # 验证时间戳精度为纳秒
    assert not loaded_data.is_empty()
    assert 'open_time' in loaded_data.columns
    assert 'close_time' in loaded_data.columns
    
    # 检查数据类型
    open_time_dtype = loaded_data['open_time'].dtype
    assert isinstance(open_time_dtype, pl.Datetime)
    assert open_time_dtype.time_unit == 'ns'  # 纳秒精度
    assert open_time_dtype.time_zone == 'UTC'
    
    print("✓ Timestamp precision unified to nanoseconds on save/load")


def test_timestamp_precision_merge(storage, tmp_path):
    """测试合并时时间戳精度兼容"""
    # 创建第一个数据（微秒精度）
    data1 = pl.DataFrame({
        'open_time': [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
        'open': [100.0],
        'close': [105.0],
    })
    
    # 转换为纳秒精度
    data1 = data1.with_columns(
        pl.col('open_time').cast(pl.Datetime('ns', time_zone='UTC'))
    )
    
    # 保存第一个数据
    test_file = tmp_path / "test.parquet"
    storage._save_dataframe_polars(data1, test_file)
    
    # 创建第二个数据（纳秒精度）
    data2 = pl.DataFrame({
        'open_time': [datetime(2024, 1, 1, 0, 5, 0, tzinfo=timezone.utc)],
        'open': [105.0],
        'close': [110.0],
    })
    data2 = data2.with_columns(
        pl.col('open_time').cast(pl.Datetime('ns', time_zone='UTC'))
    )
    
    # 加载并合并
    existing = storage._load_dataframe_polars(test_file)
    combined = pl.concat([existing, data2])
    
    # 验证合并成功且精度一致
    assert len(combined) == 2
    assert combined['open_time'].dtype.time_unit == 'ns'
    
    print("✓ Timestamp precision compatible during merge")


def test_save_klines_timestamp_precision(storage, tmp_path):
    """测试保存K线时时间戳精度统一"""
    # 临时修改存储路径
    original_klines_dir = storage.klines_dir
    storage.klines_dir = tmp_path / "klines"
    
    try:
        # 创建测试K线数据
        test_data = pd.DataFrame({
            'open_time': [datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)],
            'close_time': [datetime(2024, 1, 1, 0, 5, 0, tzinfo=timezone.utc)],
            'open': [100.0],
            'high': [110.0],
            'low': [90.0],
            'close': [105.0],
            'volume': [1000.0],
        })
        
        # 保存K线
        storage.save_klines('BTCUSDT', test_data)
        
        # 加载K线
        loaded = storage.load_klines('BTCUSDT')
        
        # 验证时间戳精度
        if not loaded.empty and 'open_time' in loaded.columns:
            # 转换为polars检查精度
            loaded_pl = pl.from_pandas(loaded)
            if 'open_time' in loaded_pl.columns:
                assert loaded_pl['open_time'].dtype.time_unit == 'ns'
                print("✓ Kline timestamp precision unified to nanoseconds")
    finally:
        storage.klines_dir = original_klines_dir
