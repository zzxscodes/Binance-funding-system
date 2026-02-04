"""
测试账户配置验证工具
"""
import pytest
import os
from unittest.mock import patch
from src.common.account_validator import validate_account_config, get_valid_accounts, validate_all_accounts
from src.common.config import config


def test_validate_account_config_testnet_complete():
    """测试testnet模式完整配置"""
    account_config = {
        'account_id': 'account1',
        'api_key': 'test_key',
        'api_secret': 'test_secret'
    }
    
    is_valid, error_msg = validate_account_config(account_config, 'testnet')
    assert is_valid is True
    assert error_msg is None
    
    print("✓ Testnet mode complete config validation works")


def test_validate_account_config_testnet_missing_keys():
    """测试testnet模式缺少密钥"""
    account_config = {
        'account_id': 'account1'
        # 缺少api_key和api_secret
    }
    
    is_valid, error_msg = validate_account_config(account_config, 'testnet')
    assert is_valid is False
    assert error_msg is not None
    assert 'missing' in error_msg.lower() and 'api key' in error_msg.lower()
    
    print("✓ Testnet mode missing keys validation works")


def test_validate_account_config_testnet_placeholder():
    """测试testnet模式占位符值"""
    account_config = {
        'account_id': 'account1',
        'api_key': 'your_account1_testnet_api_key_here',
        'api_secret': 'your_account1_testnet_api_secret_here'
    }
    
    is_valid, error_msg = validate_account_config(account_config, 'testnet')
    assert is_valid is False
    assert error_msg is not None
    assert 'placeholder' in error_msg.lower()
    
    print("✓ Testnet mode placeholder validation works")


def test_validate_account_config_live_complete():
    """测试live模式完整配置"""
    account_config = {
        'account_id': 'account1',
        'api_key': 'test_key',
        'api_secret': 'test_secret'
    }
    
    is_valid, error_msg = validate_account_config(account_config, 'live')
    assert is_valid is True
    assert error_msg is None
    
    print("✓ Live mode complete config validation works")


def test_validate_account_config_live_missing_keys():
    """测试live模式缺少密钥"""
    account_config = {
        'account_id': 'account1'
        # 缺少api_key和api_secret
    }
    
    is_valid, error_msg = validate_account_config(account_config, 'live')
    assert is_valid is False
    assert error_msg is not None
    assert 'missing' in error_msg.lower() and 'api key' in error_msg.lower()
    
    print("✓ Live mode missing keys validation works")


def test_validate_account_config_mock_complete():
    """测试mock模式完整配置"""
    account_config = {
        'account_id': 'account1',
        'total_wallet_balance': 100000.0,
        'available_balance': 50000.0,
        'initial_positions': []
    }
    
    is_valid, error_msg = validate_account_config(account_config, 'mock')
    assert is_valid is True
    assert error_msg is None
    
    print("✓ Mock mode complete config validation works")


def test_validate_account_config_mock_missing_balance():
    """测试mock模式缺少余额信息"""
    account_config = {
        'account_id': 'account1'
        # 缺少total_wallet_balance和available_balance
    }
    
    is_valid, error_msg = validate_account_config(account_config, 'mock')
    assert is_valid is False
    assert error_msg is not None
    assert 'missing balance' in error_msg.lower()
    
    print("✓ Mock mode missing balance validation works")


def test_validate_account_config_mock_invalid_balance():
    """测试mock模式无效余额"""
    account_config = {
        'account_id': 'account1',
        'total_wallet_balance': 50000.0,
        'available_balance': 100000.0  # available > total (无效)
    }
    
    is_valid, error_msg = validate_account_config(account_config, 'mock')
    assert is_valid is False
    assert error_msg is not None
    assert 'invalid balance' in error_msg.lower() or 'available_balance' in error_msg
    
    print("✓ Mock mode invalid balance validation works")


def test_validate_account_config_missing_account_id():
    """测试缺少account_id"""
    account_config = {
        # 缺少account_id
        'api_key': 'test_key',
        'api_secret': 'test_secret'
    }
    
    is_valid, error_msg = validate_account_config(account_config, 'testnet')
    assert is_valid is False
    assert error_msg is not None
    assert 'account_id' in error_msg.lower()
    
    print("✓ Missing account_id validation works")


def test_validate_account_config_env_var_priority():
    """测试环境变量优先级"""
    account_config = {
        'account_id': 'account1'
        # 配置文件中没有api_key和api_secret
    }
    
    with patch.dict(os.environ, {
        'ACCOUNT1_TESTNET_API_KEY': 'env_test_key',
        'ACCOUNT1_TESTNET_API_SECRET': 'env_test_secret'
    }):
        is_valid, error_msg = validate_account_config(account_config, 'testnet')
        assert is_valid is True
        assert error_msg is None
    
    print("✓ Environment variable priority works")


def test_get_valid_accounts():
    """测试获取有效账户列表"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {}).copy()
    
    try:
        config.set('execution.mode', 'testnet')
        testnet_config = {
            'api_base': 'https://testnet.binancefuture.com',
            'ws_base': 'wss://stream.binancefuture.com',
            'accounts': [
                {'account_id': 'account1', 'api_key': 'key1', 'api_secret': 'secret1'},
                {'account_id': 'account2'},  # 不完整
                {'account_id': 'account3', 'api_key': 'key3', 'api_secret': 'secret3'},
            ]
        }
        config.set('execution.testnet', testnet_config)
        
        valid_accounts = get_valid_accounts('testnet')
        
        assert 'account1' in valid_accounts
        assert 'account2' not in valid_accounts  # 配置不完整
        assert 'account3' in valid_accounts
        assert len(valid_accounts) == 2
        
        print("✓ get_valid_accounts works correctly")
    finally:
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)


def test_validate_all_accounts():
    """测试验证所有账户"""
    # 保存原始配置
    original_mode = config.get('execution.mode')
    original_testnet_config = config.get('execution.testnet', {}).copy()
    
    try:
        config.set('execution.mode', 'testnet')
        testnet_config = {
            'api_base': 'https://testnet.binancefuture.com',
            'ws_base': 'wss://stream.binancefuture.com',
            'accounts': [
                {'account_id': 'account1', 'api_key': 'key1', 'api_secret': 'secret1'},
                {'account_id': 'account2'},  # 不完整
            ]
        }
        config.set('execution.testnet', testnet_config)
        
        valid_accounts, invalid_accounts = validate_all_accounts('testnet')
        
        assert 'account1' in valid_accounts
        assert len(valid_accounts) == 1
        assert len(invalid_accounts) == 1
        assert invalid_accounts[0][0] == 'account2'
        assert 'missing' in invalid_accounts[0][1].lower()
        
        print("✓ validate_all_accounts works correctly")
    finally:
        config.set('execution.mode', original_mode)
        config.set('execution.testnet', original_testnet_config)


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Account Validator")
    print("=" * 60)
    
    test_validate_account_config_testnet_complete()
    test_validate_account_config_testnet_missing_keys()
    test_validate_account_config_testnet_placeholder()
    test_validate_account_config_live_complete()
    test_validate_account_config_live_missing_keys()
    test_validate_account_config_mock_complete()
    test_validate_account_config_mock_missing_balance()
    test_validate_account_config_mock_invalid_balance()
    test_validate_account_config_missing_account_id()
    test_validate_account_config_env_var_priority()
    test_get_valid_accounts()
    test_validate_all_accounts()
    
    print("\n" + "=" * 60)
    print("All account validator tests completed!")
    print("=" * 60)
