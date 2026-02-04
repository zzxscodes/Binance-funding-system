"""
测试配置管理模块
重点测试根据 execution.mode 自动选择正确的 API 和 WebSocket 地址
"""
import pytest
from unittest.mock import patch
from src.common.config import config


def test_get_binance_api_base_mock_mode():
    """测试 mock 模式下返回 mock API 地址"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_mock_config = config.get('execution.mock', {})
    
    try:
        # 设置为 mock 模式并配置 mock 地址
        config.set('execution.mode', 'mock')
        mock_config = {
            'api_base': 'https://mock.binance.com',
            'ws_base': 'wss://mock.binance.com'
        }
        config.set('execution.mock', mock_config)
        
        api_base = config.get_binance_api_base()
        assert api_base == 'https://mock.binance.com'
        
        print("✓ Mock mode returns mock API base")
    finally:
        # 恢复原始配置
        config.set('execution.mode', original_mode)
        config.set('execution.mock', original_mock_config)


def test_get_binance_api_base_testnet_mode():
    """测试 testnet 模式下返回 testnet API 地址"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {})
    
    try:
        # 设置为 testnet 模式并配置 testnet 地址
        config.set('execution.mode', 'testnet')
        testnet_config = {
            'api_base': 'https://testnet.binancefuture.com',
            'ws_base': 'wss://stream.binancefuture.com',
            'api_key': 'test_key',
            'api_secret': 'test_secret'
        }
        config.set('execution.testnet', testnet_config)
        
        api_base = config.get_binance_api_base()
        assert api_base == 'https://testnet.binancefuture.com'
        
        print("✓ Testnet mode returns testnet API base")
    finally:
        # 恢复原始配置
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)


def test_get_binance_api_base_live_mode():
    """测试 live 模式下返回默认 API 地址"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_api_base = config.get('binance.api_base')
    
    try:
        # 设置为 live 模式
        config.set('execution.mode', 'live')
        config.set('binance.api_base', 'https://fapi.binance.com')
        
        api_base = config.get_binance_api_base()
        assert api_base == 'https://fapi.binance.com'
        
        print("✓ Live mode returns default API base")
    finally:
        # 恢复原始配置
        config.set('execution.mode', original_mode)
        config.set('binance.api_base', original_api_base)


def test_get_binance_ws_base_mock_mode():
    """测试 mock 模式下返回 mock WebSocket 地址"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_mock_config = config.get('execution.mock', {})
    
    try:
        # 设置为 mock 模式并配置 mock 地址
        config.set('execution.mode', 'mock')
        mock_config = {
            'api_base': 'https://mock.binance.com',
            'ws_base': 'wss://mock.binance.com'
        }
        config.set('execution.mock', mock_config)
        
        ws_base = config.get_binance_ws_base()
        assert ws_base == 'wss://mock.binance.com'
        
        print("✓ Mock mode returns mock WS base")
    finally:
        # 恢复原始配置
        config.set('execution.mode', original_mode)
        config.set('execution.mock', original_mock_config)


def test_get_binance_ws_base_testnet_mode():
    """测试 testnet 模式下返回 testnet WebSocket 地址"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {})
    
    try:
        # 设置为 testnet 模式并配置 testnet 地址
        config.set('execution.mode', 'testnet')
        testnet_config = {
            'api_base': 'https://testnet.binancefuture.com',
            'ws_base': 'wss://stream.binancefuture.com',
            'api_key': 'test_key',
            'api_secret': 'test_secret'
        }
        config.set('execution.testnet', testnet_config)
        
        ws_base = config.get_binance_ws_base()
        assert ws_base == 'wss://stream.binancefuture.com'
        
        print("✓ Testnet mode returns testnet WS base")
    finally:
        # 恢复原始配置
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)


def test_get_binance_ws_base_live_mode():
    """测试 live 模式下返回默认 WebSocket 地址"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_ws_base = config.get('binance.ws_base')
    
    try:
        # 设置为 live 模式
        config.set('execution.mode', 'live')
        config.set('binance.ws_base', 'wss://fstream.binance.com')
        
        ws_base = config.get_binance_ws_base()
        assert ws_base == 'wss://fstream.binance.com'
        
        print("✓ Live mode returns default WS base")
    finally:
        # 恢复原始配置
        config.set('execution.mode', original_mode)
        config.set('binance.ws_base', original_ws_base)


def test_get_binance_api_base_fallback_to_default():
    """测试当 testnet 配置不存在时，回退到默认值"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {})
    original_api_base = config.get('binance.api_base')
    
    try:
        # 设置为 testnet 模式，但不配置 testnet.api_base
        config.set('execution.mode', 'testnet')
        config.set('execution.testnet', {})  # 空配置
        config.set('binance.api_base', 'https://fapi.binance.com')
        
        # 应该回退到默认值
        api_base = config.get_binance_api_base()
        assert api_base == 'https://testnet.binancefuture.com'  # 方法内部的默认值
        
        print("✓ Testnet mode falls back to default when config missing")
    finally:
        # 恢复原始配置
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)
        config.set('binance.api_base', original_api_base)


def test_get_binance_ws_base_fallback_to_default():
    """测试当 testnet 配置不存在时，回退到默认值"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {})
    original_ws_base = config.get('binance.ws_base')
    
    try:
        # 设置为 testnet 模式，但不配置 testnet.ws_base
        config.set('execution.mode', 'testnet')
        config.set('execution.testnet', {})  # 空配置
        config.set('binance.ws_base', 'wss://fstream.binance.com')
        
        # 应该回退到默认值
        ws_base = config.get_binance_ws_base()
        assert ws_base == 'wss://stream.binancefuture.com'  # 方法内部的默认值
        
        print("✓ Testnet mode WS falls back to default when config missing")
    finally:
        # 恢复原始配置
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)
        config.set('binance.ws_base', original_ws_base)


def test_config_mode_switching():
    """测试在不同模式之间切换时，地址选择是否正确"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {})
    original_api_base = config.get('binance.api_base')
    original_ws_base = config.get('binance.ws_base')
    
    try:
        # 配置 testnet
        testnet_config = {
            'api_base': 'https://testnet.binancefuture.com',
            'ws_base': 'wss://stream.binancefuture.com'
        }
        config.set('execution.testnet', testnet_config)
        config.set('binance.api_base', 'https://fapi.binance.com')
        config.set('binance.ws_base', 'wss://fstream.binance.com')
        
        # 配置 mock 和 live
        mock_config = {
            'api_base': 'https://mock.binance.com',
            'ws_base': 'wss://mock.binance.com'
        }
        config.set('execution.mock', mock_config)
        live_config = {
            'api_base': 'https://fapi.binance.com',
            'ws_base': 'wss://fstream.binance.com'
        }
        config.set('execution.live', live_config)
        
        # 测试 mock 模式
        config.set('execution.mode', 'mock')
        assert config.get_binance_api_base() == 'https://mock.binance.com'
        assert config.get_binance_ws_base() == 'wss://mock.binance.com'
        
        # 测试 testnet 模式
        config.set('execution.mode', 'testnet')
        assert config.get_binance_api_base() == 'https://testnet.binancefuture.com'
        assert config.get_binance_ws_base() == 'wss://stream.binancefuture.com'
        
        # 测试 live 模式
        config.set('execution.mode', 'live')
        assert config.get_binance_api_base() == 'https://fapi.binance.com'
        assert config.get_binance_ws_base() == 'wss://fstream.binance.com'
        
        print("✓ Mode switching works correctly")
    finally:
        # 恢复原始配置
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)
        config.set('binance.api_base', original_api_base)
        config.set('binance.ws_base', original_ws_base)
