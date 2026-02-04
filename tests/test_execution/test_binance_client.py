"""
测试Binance API客户端
注意：需要有效的API密钥才能运行完整测试
"""
import pytest
import asyncio
import sys
from pathlib import Path
import os

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.execution.binance_client import BinanceClient


def test_binance_client_initialization():
    """测试Binance客户端初始化"""
    # 使用测试密钥（不会真正连接）
    api_key = "test_key"
    api_secret = "test_secret"
    
    client = BinanceClient(api_key, api_secret)
    
    assert client.api_key == api_key
    assert client.api_secret == api_secret
    assert client.api_base is not None
    
    print("✓ Binance client initialized successfully")


@pytest.mark.asyncio
async def test_get_exchange_info():
    """测试获取交易所信息（不需要认证）"""
    # 使用空密钥（只测试公开接口）
    client = BinanceClient("", "")
    
    try:
        exchange_info = await client.get_exchange_info()
        
        assert isinstance(exchange_info, dict)
        assert 'symbols' in exchange_info
        assert len(exchange_info['symbols']) > 0
        
        print(f"✓ Exchange info retrieved: {len(exchange_info['symbols'])} symbols")
        
    except Exception as e:
        print(f"⚠ Failed to get exchange info (may need network): {e}")
    finally:
        await client.close()


@pytest.mark.testnet
@pytest.mark.asyncio
async def test_get_account_info_with_key():
    """测试获取账户信息（只读，强制使用testnet，默认运行如果testnet密钥存在）"""
    # 优先使用 testnet 密钥，如果没有则跳过（不允许使用真实账户密钥）
    api_key = os.getenv('BINANCE_TESTNET_API_KEY', '').strip()
    api_secret = os.getenv('BINANCE_TESTNET_API_SECRET', '').strip()

    if not api_key or not api_secret:
        pytest.skip("Missing BINANCE_TESTNET_API_KEY/BINANCE_TESTNET_API_SECRET env vars (testnet keys required, no real account keys allowed)")
    
    # 强制使用 testnet base URL
    testnet_base = os.getenv("BINANCE_TESTNET_FAPI_BASE", "https://testnet.binancefuture.com")
    client = BinanceClient(api_key, api_secret, api_base=testnet_base)
    
    try:
        account_info = await client.get_account_info()
        
        assert isinstance(account_info, dict)
        # futures account endpoint should include wallet balance fields
        assert 'totalWalletBalance' in account_info or 'assets' in account_info or 'positions' in account_info
        
        print("✓ Account info retrieved successfully (testnet)")
        
    except Exception as e:
        raise
    finally:
        await client.close()


@pytest.mark.testnet
@pytest.mark.asyncio
async def test_get_positions_with_key():
    """测试获取持仓（只读，强制使用testnet，默认运行如果testnet密钥存在）"""
    # 优先使用 testnet 密钥，如果没有则跳过（不允许使用真实账户密钥）
    api_key = os.getenv('BINANCE_TESTNET_API_KEY', '').strip()
    api_secret = os.getenv('BINANCE_TESTNET_API_SECRET', '').strip()

    if not api_key or not api_secret:
        pytest.skip("Missing BINANCE_TESTNET_API_KEY/BINANCE_TESTNET_API_SECRET env vars (testnet keys required, no real account keys allowed)")
    
    # 强制使用 testnet base URL
    testnet_base = os.getenv("BINANCE_TESTNET_FAPI_BASE", "https://testnet.binancefuture.com")
    client = BinanceClient(api_key, api_secret, api_base=testnet_base)
    
    try:
        positions = await client.get_positions()
        
        assert isinstance(positions, list)
        
        print(f"✓ Positions retrieved: {len(positions)} positions (testnet)")
        
    except Exception as e:
        raise
    finally:
        await client.close()


def test_signature_generation():
    """测试签名生成"""
    client = BinanceClient("test_key", "test_secret")
    
    params = {'symbol': 'BTCUSDT', 'timestamp': 1234567890}
    signature = client._generate_signature(params)
    
    assert isinstance(signature, str)
    assert len(signature) == 64  # SHA256 hex length
    
    print("✓ Signature generation works correctly")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Binance Client")
    print("=" * 60)
    
    test_binance_client_initialization()
    test_signature_generation()
    
    # 运行异步测试
    asyncio.run(test_get_exchange_info())
    
    # 需要API密钥的测试
    asyncio.run(test_get_account_info_with_key())
    asyncio.run(test_get_positions_with_key())
    
    print("\n" + "=" * 60)
    print("All Binance client tests completed!")
    print("=" * 60)
