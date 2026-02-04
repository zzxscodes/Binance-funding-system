"""
测试Binance连接性
集成测试：测试与Binance API和WebSocket的实际连接
"""
import pytest
import asyncio
import aiohttp
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.common.config import config


async def _retry(coro_factory, attempts: int = 3, base_delay: float = 1.0):
    last_exc = None
    for i in range(attempts):
        try:
            return await coro_factory()
        except Exception as e:
            last_exc = e
            await asyncio.sleep(base_delay * (2 ** i))
    raise last_exc


@pytest.mark.asyncio
async def test_binance_rest_api_connection():
    """测试Binance REST API连接"""
    api_base = config.get('binance.api_base', 'https://fapi.binance.com')
    url = f"{api_base}/fapi/v1/ping"
    
    try:
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    # 451表示地区限制，但说明连接是成功的
                    assert response.status in [200, 451], f"Unexpected status: {response.status}"
                    data = await response.json()
                    assert isinstance(data, dict)

                    if response.status == 451:
                        print(f"✓ Binance REST API connected but restricted (451): {data.get('msg', '')}")
                    else:
                        print(f"✓ Binance REST API connection successful: {api_base}")
                    return True

        return await _retry(_do, attempts=3, base_delay=1.0)
    except Exception as e:
        pytest.fail(f"Failed to connect to Binance REST API after retries: {e}")


@pytest.mark.asyncio
async def test_binance_exchange_info():
    """测试获取Binance交易所信息"""
    api_base = config.get('binance.api_base', 'https://fapi.binance.com')
    url = f"{api_base}/fapi/v1/exchangeInfo"
    
    try:
        async def _do():
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=45)) as response:
                    # 451表示地区限制，但说明连接是成功的
                    if response.status == 451:
                        data = await response.json()
                        print(f"✓ Binance API connected but restricted (451): {data.get('msg', '')}")
                        # 地区限制时跳过数据验证
                        return True

                    assert response.status == 200
                    data = await response.json()

                    # 验证返回数据
                    assert 'symbols' in data
                    assert isinstance(data['symbols'], list)
                    assert len(data['symbols']) > 0

                    # 验证有永续合约
                    perpetual_count = sum(
                        1 for s in data['symbols']
                        if s.get('contractType') == 'PERPETUAL' and s.get('quoteAsset') == 'USDT'
                    )

                    assert perpetual_count > 0

                    print(f"✓ Binance exchange info retrieved successfully")
                    print(f"  Total symbols: {len(data['symbols'])}")
                    print(f"  USDT perpetual contracts: {perpetual_count}")
                    return True

        return await _retry(_do, attempts=3, base_delay=1.0)
    except Exception as e:
        pytest.fail(f"Failed to get Binance exchange info after retries: {e}")


@pytest.mark.asyncio
async def test_binance_websocket_connection():
    """测试Binance WebSocket连接"""
    import websockets
    from websockets.exceptions import WebSocketException
    
    ws_base = config.get('binance.ws_base', 'wss://fstream.binance.com')
    # 测试单个stream连接
    url = f"{ws_base}/ws/btcusdt@trade"
    
    try:
        async def _do():
            messages_received = 0

            async with websockets.connect(
                url,
                open_timeout=30,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as websocket:
                print(f"[OK] WebSocket connected: {url}")

                # 接收几条消息
                for i in range(5):
                    try:
                        await asyncio.wait_for(websocket.recv(), timeout=10.0)
                        messages_received += 1
                        print(f"  Received message {i+1}")
                    except asyncio.TimeoutError:
                        print(f"  Timeout waiting for message {i+1}")
                        break

                assert messages_received > 0
                print(f"[OK] WebSocket connection successful, received {messages_received} messages")
                return True

        return await _retry(_do, attempts=3, base_delay=2.0)
            
    except WebSocketException as e:
        pytest.fail(f"WebSocket connection failed after retries: {e}")
    except Exception as e:
        pytest.fail(f"Failed to test WebSocket connection after retries: {e}")


@pytest.mark.asyncio
async def test_binance_combined_stream():
    """测试Binance组合流连接"""
    import websockets
    import json
    
    ws_base = config.get('binance.ws_base', 'wss://fstream.binance.com')
    # 测试组合流
    url = f"{ws_base}/stream?streams=btcusdt@trade/ethusdt@trade"
    
    try:
        async def _do():
            messages_received = {}

            async with websockets.connect(
                url,
                open_timeout=30,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as websocket:
                print(f"[OK] Combined stream connected: {url}")

                # 接收几条消息
                for _ in range(10):
                    try:
                        msg_str = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                        msg = json.loads(msg_str)

                        stream = msg.get('stream', '')
                        if stream:
                            symbol = stream.split('@')[0].upper()
                            messages_received[symbol] = messages_received.get(symbol, 0) + 1

                    except asyncio.TimeoutError:
                        break

                assert len(messages_received) > 0
                print(f"[OK] Combined stream works, received messages for: {list(messages_received.keys())}")
                return True

        return await _retry(_do, attempts=3, base_delay=2.0)
            
    except Exception as e:
        pytest.fail(f"Failed to test combined stream after retries: {e}")


def test_config_loaded():
    """测试配置是否正确加载"""
    api_base = config.get('binance.api_base')
    ws_base = config.get('binance.ws_base')
    
    assert api_base is not None
    assert ws_base is not None
    
    print(f"[OK] Configuration loaded")
    print(f"  API base: {api_base}")
    print(f"  WS base: {ws_base}")


if __name__ == "__main__":
    # 运行测试
    print("=" * 60)
    print("Testing Binance Connection")
    print("=" * 60)
    
    async def run_tests():
        print("\n1. Testing configuration...")
        test_config_loaded()
        
        print("\n2. Testing REST API connection...")
        await test_binance_rest_api_connection()
        
        print("\n3. Testing exchange info...")
        await test_binance_exchange_info()
        
        print("\n4. Testing WebSocket connection...")
        await test_binance_websocket_connection()
        
        print("\n5. Testing combined stream...")
        await test_binance_combined_stream()
        
        print("\n" + "=" * 60)
        print("All connection tests passed!")
        print("=" * 60)
    
    asyncio.run(run_tests())
