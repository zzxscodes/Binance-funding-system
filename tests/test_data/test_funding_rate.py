"""
资金费率数据存储和查询测试
"""
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from src.data.storage import DataStorage, get_data_storage
from src.data.api import DataAPI, get_data_api


def test_storage_funding_rates_initialization():
    """测试资金费率存储初始化"""
    storage = DataStorage()
    assert storage.funding_rates_dir.exists()


def test_save_and_load_funding_rates():
    """测试资金费率数据保存和读取"""
    storage = DataStorage()
    
    # 创建测试数据
    symbol = 'BTCUSDT'
    now = datetime.now(timezone.utc)
    
    df = pd.DataFrame({
        'symbol': [symbol, symbol, symbol],
        'fundingTime': [
            now - timedelta(hours=8),
            now - timedelta(hours=4),
            now
        ],
        'fundingRate': [0.0001, 0.0002, 0.00015],
        'markPrice': [50000.0, 50100.0, 50050.0]
    })
    
    # 保存
    storage.save_funding_rates(symbol, df, now)
    
    # 读取
    loaded_df = storage.load_funding_rates(symbol, now - timedelta(days=1), now + timedelta(days=1))
    
    assert not loaded_df.empty
    assert len(loaded_df) >= len(df)
    assert 'fundingTime' in loaded_df.columns
    assert 'fundingRate' in loaded_df.columns
    assert 'markPrice' in loaded_df.columns


def test_load_funding_rates_bulk():
    """测试批量加载资金费率数据"""
    storage = DataStorage()
    
    symbols = ['BTCUSDT', 'ETHUSDT']
    now = datetime.now(timezone.utc)
    
    # 为每个symbol创建测试数据
    for symbol in symbols:
        df = pd.DataFrame({
            'symbol': [symbol],
            'fundingTime': [now],
            'fundingRate': [0.0001],
            'markPrice': [50000.0 if symbol == 'BTCUSDT' else 3000.0]
        })
        storage.save_funding_rates(symbol, df, now)
    
    # 批量加载
    result = storage.load_funding_rates_bulk(
        symbols=symbols,
        start_date=now - timedelta(days=1),
        end_date=now + timedelta(days=1)
    )
    
    assert isinstance(result, dict)
    assert 'BTCUSDT' in result
    assert 'ETHUSDT' in result
    assert not result['BTCUSDT'].empty
    assert not result['ETHUSDT'].empty


def test_get_funding_rate_between():
    """测试get_funding_rate_between接口"""
    api = DataAPI()
    storage = get_data_storage()
    
    # 创建测试数据
    symbol = 'BTCUSDT'
    now = datetime.now(timezone.utc)
    
    # 创建多个时间点的资金费率数据
    funding_times = []
    for i in range(10):
        funding_times.append(now - timedelta(hours=8*i))
    
    df = pd.DataFrame({
        'symbol': [symbol] * len(funding_times),
        'fundingTime': funding_times,
        'fundingRate': [0.0001 * (i % 3 + 1) for i in range(len(funding_times))],
        'markPrice': [50000.0 + i * 100 for i in range(len(funding_times))]
    })
    
    # 保存数据
    for funding_time in funding_times:
        day_df = df[df['fundingTime'] == funding_time]
        if not day_df.empty:
            storage.save_funding_rates(symbol, day_df, funding_time)
    
    # 生成date_time_label
    begin_time = now - timedelta(hours=72)  # 3天前
    end_time = now
    
    # 计算time_label（每个5分钟窗口）
    begin_label = f"{begin_time.year:04d}-{begin_time.month:02d}-{begin_time.day:02d}-{int((begin_time.hour * 60 + begin_time.minute) / 5):03d}"
    end_label = f"{end_time.year:04d}-{end_time.month:02d}-{end_time.day:02d}-{int((end_time.hour * 60 + end_time.minute) / 5):03d}"
    
    # 查询（注意：如果universe中没有BTCUSDT，可能返回空）
    result = api.get_funding_rate_between(begin_label, end_label)
    
    assert isinstance(result, dict)
    # 如果universe中有BTCUSDT，应该有数据
    if 'btc-usdt' in result:
        assert isinstance(result['btc-usdt'], pd.DataFrame)


def test_get_funding_rate_between_empty():
    """测试get_funding_rate_between接口（无数据情况）"""
    api = DataAPI()
    
    # 使用一个不存在的日期范围
    begin_label = '2020-01-01-000'
    end_label = '2020-01-02-000'
    
    result = api.get_funding_rate_between(begin_label, end_label)
    
    assert isinstance(result, dict)
    # 应该返回空字典或包含空DataFrame的字典
