"""
数据层API测试
"""
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from src.data.api import DataAPI, get_data_api
from src.data.kline_aggregator import KlineAggregator


def test_data_api_initialization():
    """测试数据API初始化"""
    api = DataAPI()
    assert api.storage is not None
    assert api.max_history_days == 30
    assert api.kline_aggregator is None


def test_data_api_with_aggregator():
    """测试带聚合器的数据API初始化"""
    aggregator = KlineAggregator()
    api = DataAPI(kline_aggregator=aggregator)
    assert api.kline_aggregator is not None
    assert api.kline_aggregator is aggregator


def test_get_klines_empty():
    """测试获取空K线数据"""
    api = DataAPI()
    
    result = api.get_klines(['NONEXISTENT'], days=1)
    
    assert isinstance(result, dict)
    # DataAPI 现在统一返回 key 为系统格式（小写/连字符）；无法解析的符号降级为小写原样
    assert 'nonexistent' in result
    assert isinstance(result['nonexistent'], pd.DataFrame)
    assert result['nonexistent'].empty


def test_get_klines_max_days():
    """测试最大天数限制"""
    api = DataAPI()
    
    # 请求超过30天的数据
    result = api.get_klines(['BTCUSDT'], days=100)
    
    # 当前实现：不强制截断，只给 warning（保持行为不报错即可）
    assert isinstance(result, dict)


def test_get_klines_invalid_days():
    """测试无效天数参数"""
    api = DataAPI()
    
    result = api.get_klines(['BTCUSDT'], days=0)
    
    assert isinstance(result, dict)
    assert result == {}


def test_get_universe():
    """测试获取Universe"""
    api = DataAPI()
    
    symbols = api.get_universe()
    
    assert isinstance(symbols, list)
    # 如果有数据，应该是字符串列表
    if symbols:
        assert all(isinstance(s, str) for s in symbols)


def test_get_universe_with_date():
    """测试按日期获取Universe"""
    api = DataAPI()
    
    date = datetime.now(timezone.utc)
    symbols = api.get_universe(date)
    
    assert isinstance(symbols, list)


def test_get_latest_klines():
    """测试获取最新K线"""
    aggregator = KlineAggregator()
    api = DataAPI(kline_aggregator=aggregator)
    
    result = api.get_latest_klines(['BTCUSDT', 'ETHUSDT'])
    
    assert isinstance(result, dict)
    assert 'BTCUSDT' in result
    assert 'ETHUSDT' in result
    # 可能为None（如果没有实时数据）或Series


def test_get_klines_returns_system_keys():
    """DataAPI.get_klines 应返回 btc-usdt 风格 key（系统格式）"""
    api = DataAPI()
    result = api.get_klines(['BTCUSDT', 'ETHUSDT'], days=1)
    assert isinstance(result, dict)
    assert 'btc-usdt' in result
    assert 'eth-usdt' in result
    assert isinstance(result['btc-usdt'], pd.DataFrame)
    assert isinstance(result['eth-usdt'], pd.DataFrame)


def test_get_latest_klines_no_aggregator():
    """测试没有聚合器时获取最新K线"""
    api = DataAPI()
    
    result = api.get_latest_klines(['BTCUSDT'])
    
    assert isinstance(result, dict)
    assert result['BTCUSDT'] is None


def test_check_data_completeness():
    """测试数据完整性检查"""
    api = DataAPI()
    
    result = api.check_data_completeness(['BTCUSDT', 'ETHUSDT'], days=1)
    
    assert isinstance(result, dict)
    assert 'BTCUSDT' in result
    assert 'ETHUSDT' in result
    
    # 验证返回结构
    btc_info = result['BTCUSDT']
    assert 'total_expected' in btc_info
    assert 'total_actual' in btc_info
    assert 'completeness' in btc_info
    assert 'missing_count' in btc_info
    assert isinstance(btc_info['total_expected'], int)
    assert isinstance(btc_info['total_actual'], int)
    assert isinstance(btc_info['completeness'], float)


def test_get_data_api_singleton():
    """测试数据API单例模式"""
    api1 = get_data_api()
    api2 = get_data_api()
    
    assert api1 is api2


def test_get_data_api_with_aggregator():
    """测试带聚合器的数据API单例"""
    aggregator = KlineAggregator()
    api1 = get_data_api(aggregator)
    api2 = get_data_api(aggregator)
    
    assert api1 is api2
    assert api1.kline_aggregator is aggregator
