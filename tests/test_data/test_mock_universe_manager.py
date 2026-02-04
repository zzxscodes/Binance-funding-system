"""
测试模拟Universe管理器
"""
import pytest
from src.data.mock_universe_manager import MockUniverseManager


def test_mock_universe_manager_initialization():
    """测试模拟Universe管理器初始化"""
    manager = MockUniverseManager()
    
    assert len(manager.mock_symbols) > 0
    assert 'BTCUSDT' in manager.mock_symbols
    assert 'ETHUSDT' in manager.mock_symbols
    
    print("✓ MockUniverseManager initialized")


def test_mock_universe_manager_load_universe():
    """测试加载Universe"""
    manager = MockUniverseManager()
    
    universe = manager.load_universe()
    
    assert len(universe) > 0
    assert 'BTCUSDT' in universe
    assert 'ETHUSDT' in universe
    
    print(f"✓ MockUniverseManager loaded {len(universe)} symbols")


@pytest.mark.asyncio
async def test_mock_universe_manager_update_universe():
    """测试更新Universe"""
    manager = MockUniverseManager()
    
    await manager.update_universe()
    
    assert len(manager.current_universe) > 0
    assert 'BTCUSDT' in manager.current_universe
    
    print("✓ MockUniverseManager update_universe works")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Mock Universe Manager")
    print("=" * 60)
    
    test_mock_universe_manager_initialization()
    test_mock_universe_manager_load_universe()
    import asyncio
    asyncio.run(test_mock_universe_manager_update_universe())
    
    print("\n" + "=" * 60)
    print("All mock universe manager tests completed!")
    print("=" * 60)
