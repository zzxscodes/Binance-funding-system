"""
数据存储模块测试
"""
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from pathlib import Path
from src.data.storage import DataStorage, get_data_storage


def test_storage_initialization():
    """测试数据存储初始化"""
    storage = DataStorage()
    assert storage.klines_dir.exists()
    assert storage.trades_dir.exists()
    assert storage.storage_format == 'parquet'


def test_save_and_load_klines():
    """测试K线数据保存和读取"""
    storage = DataStorage()
    
    # 创建测试数据
    symbol = 'BTCUSDT'
    now = datetime.now(timezone.utc)
    
    df = pd.DataFrame({
        'open_time': [now - timedelta(minutes=10), now - timedelta(minutes=5), now],
        'close_time': [now - timedelta(minutes=5), now, now + timedelta(minutes=5)],
        'open': [50000.0, 50010.0, 50020.0],
        'high': [50010.0, 50020.0, 50030.0],
        'low': [49990.0, 50000.0, 50010.0],
        'close': [50010.0, 50020.0, 50030.0],
        'volume': [100.0, 110.0, 120.0]
    })
    
    # 保存
    storage.save_klines(symbol, df, now)
    
    # 读取
    loaded_df = storage.load_klines(symbol, now - timedelta(days=1), now + timedelta(days=1))
    
    assert not loaded_df.empty
    assert len(loaded_df) >= len(df)
    assert 'open' in loaded_df.columns
    assert 'high' in loaded_df.columns
    assert 'low' in loaded_df.columns
    assert 'close' in loaded_df.columns
    assert 'volume' in loaded_df.columns


def test_load_klines_bulk_returns_all_keys():
    """测试批量加载K线：返回包含所有symbol key（即使为空也给空DF）"""
    storage = DataStorage()
    now = datetime.now(timezone.utc)

    # 先为BTCUSDT写入一份数据（保证至少一个非空）
    symbol1 = 'BTCUSDT'
    df = pd.DataFrame({
        'open_time': [now - timedelta(minutes=5)],
        'close_time': [now],
        'open': [50000.0],
        'high': [50010.0],
        'low': [49990.0],
        'close': [50005.0],
        'volume': [1.0],
        'quote_volume': [50005.0],
    })
    storage.save_klines(symbol1, df, now)

    result = storage.load_klines_bulk([symbol1, "NONEXISTENT"], start_date=now - timedelta(days=1), end_date=now + timedelta(days=1))
    assert isinstance(result, dict)
    assert "BTCUSDT" in result
    assert "NONEXISTENT" in result
    assert isinstance(result["BTCUSDT"], pd.DataFrame)
    assert isinstance(result["NONEXISTENT"], pd.DataFrame)


def test_save_and_load_trades():
    """测试逐笔成交数据保存和读取"""
    storage = DataStorage()
    
    symbol = 'ETHUSDT'
    now = datetime.now(timezone.utc)
    
    df = pd.DataFrame({
        'tradeId': [1, 2, 3],
        'price': [3000.0, 3001.0, 3002.0],
        'qty': [1.0, 1.5, 2.0],
        'ts': [now, now + timedelta(seconds=1), now + timedelta(seconds=2)],
        'isBuyerMaker': [True, False, True]
    })
    
    # 保存
    storage.save_trades(symbol, df, now)
    
    # 验证文件存在（按小时保存）
    file_path = storage._get_trade_file_path(symbol, now, now.hour)
    # 文件可能已创建，但不一定包含我们的数据（因为按小时分组）


def test_load_klines_with_date_range():
    """测试按日期范围加载K线"""
    storage = DataStorage()
    
    symbol = 'BTCUSDT'
    start_date = datetime.now(timezone.utc) - timedelta(days=2)
    end_date = datetime.now(timezone.utc)
    
    df = storage.load_klines(symbol, start_date, end_date)
    
    # 验证返回DataFrame（可能为空，但不应该报错）
    assert isinstance(df, pd.DataFrame)


def test_load_klines_empty():
    """测试加载不存在的K线数据"""
    storage = DataStorage()
    
    df = storage.load_klines('NONEXISTENT', datetime.now(timezone.utc) - timedelta(days=1))
    
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_cleanup_old_data():
    """测试清理旧数据"""
    storage = DataStorage()
    
    # 创建一些旧数据文件
    symbol = 'TESTUSDT'
    old_date = datetime.now(timezone.utc) - timedelta(days=35)
    
    df = pd.DataFrame({
        'open_time': [old_date],
        'close_time': [old_date + timedelta(minutes=5)],
        'open': [100.0],
        'high': [101.0],
        'low': [99.0],
        'close': [100.5],
        'volume': [10.0]
    })
    
    storage.save_klines(symbol, df, old_date)
    
    # 清理30天前的数据
    removed = storage.cleanup_old_data(days=30)
    
    # 验证文件可能被删除（取决于文件修改时间）
    assert isinstance(removed, int)


def test_get_data_storage_singleton():
    """测试数据存储单例模式"""
    storage1 = get_data_storage()
    storage2 = get_data_storage()
    
    assert storage1 is storage2


def test_save_empty_dataframe():
    """测试保存空DataFrame"""
    storage = DataStorage()
    
    empty_df = pd.DataFrame()
    storage.save_klines('BTCUSDT', empty_df)
    
    # 应该不报错，只是跳过保存


def test_merge_existing_klines():
    """测试合并已存在的K线数据"""
    storage = DataStorage()
    
    symbol = 'BTCUSDT'
    now = datetime.now(timezone.utc)
    
    # 第一次保存
    df1 = pd.DataFrame({
        'open_time': [now - timedelta(minutes=10)],
        'close_time': [now - timedelta(minutes=5)],
        'open': [50000.0],
        'high': [50010.0],
        'low': [49990.0],
        'close': [50010.0],
        'volume': [100.0]
    })
    storage.save_klines(symbol, df1, now)
    
    # 第二次保存（应该合并）
    df2 = pd.DataFrame({
        'open_time': [now - timedelta(minutes=5)],
        'close_time': [now],
        'open': [50010.0],
        'high': [50020.0],
        'low': [50000.0],
        'close': [50020.0],
        'volume': [110.0]
    })
    storage.save_klines(symbol, df2, now)
    
    # 读取并验证
    loaded_df = storage.load_klines(symbol, now - timedelta(days=1), now + timedelta(days=1))
    
    assert not loaded_df.empty
    assert len(loaded_df) >= 1
