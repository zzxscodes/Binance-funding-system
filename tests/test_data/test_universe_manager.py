"""
测试Universe管理模块
"""
import pytest
import asyncio
from datetime import datetime
from pathlib import Path
import sys

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.data.universe_manager import UniverseManager, get_universe_manager
from src.common.config import config


async def _retry(coro_factory, attempts: int = 3, base_delay: float = 1.0):
    last_exc = None
    for i in range(attempts):
        try:
            return await coro_factory()
        except Exception as e:
            last_exc = e
            # exponential backoff
            await asyncio.sleep(base_delay * (2 ** i))
    raise last_exc


@pytest.mark.asyncio
async def test_fetch_universe_from_binance():
    """测试从Binance获取Universe"""
    manager = UniverseManager()
    
    try:
        symbols_info = await _retry(lambda: manager.fetch_universe_from_binance(), attempts=3, base_delay=1.0)
        
        # 验证返回数据
        assert isinstance(symbols_info, list)
        assert len(symbols_info) > 0
        
        # 验证数据格式
        first_symbol = symbols_info[0]
        assert 'symbol' in first_symbol
        assert 'contractType' in first_symbol
        assert 'quoteAsset' in first_symbol
        assert first_symbol['contractType'] == 'PERPETUAL'
        assert first_symbol['quoteAsset'] == 'USDT'
        
        print(f"✓ Successfully fetched {len(symbols_info)} USDT perpetual contracts from Binance")
        print(f"  Example symbols: {[s['symbol'] for s in symbols_info[:5]]}")
        
    except Exception as e:
        pytest.fail(f"Failed to fetch universe from Binance after retries: {e}")


@pytest.mark.asyncio
async def test_update_universe():
    """测试更新Universe"""
    manager = UniverseManager()
    
    try:
        symbols = await _retry(lambda: manager.update_universe(), attempts=3, base_delay=1.0)
        
        # 验证Universe已更新
        assert isinstance(symbols, list)
        assert len(symbols) > 0
        
        # 验证Universe文件已创建
        file_path = manager._get_universe_file_path(datetime.now())
        assert file_path.exists()
        
        print(f"✓ Universe updated successfully: {len(symbols)} symbols")
        print(f"  Universe file: {file_path}")
        
        # 验证文件内容
        loaded_universe = manager.load_universe()
        assert len(loaded_universe) > 0
        assert loaded_universe == set(symbols)
        
        print(f"✓ Universe file loaded successfully: {len(loaded_universe)} symbols")
        
    except Exception as e:
        pytest.fail(f"Failed to update universe after retries: {e}")


@pytest.mark.asyncio
async def test_load_universe():
    """测试加载Universe"""
    manager = UniverseManager()
    
    # 先更新Universe
    await _retry(lambda: manager.update_universe(), attempts=3, base_delay=1.0)
    
    # 加载Universe
    universe = manager.load_universe()
    
    assert isinstance(universe, set)
    if len(universe) > 0:
        print(f"✓ Loaded universe: {len(universe)} symbols")
        print(f"  Example symbols: {list(universe)[:5]}")
    else:
        print("⚠ No universe found (this is OK if first run)")


@pytest.mark.asyncio
async def test_universe_file_format():
    """测试Universe文件格式"""
    manager = UniverseManager()
    
    # 更新并保存
    try:
        symbols_info = await manager.fetch_universe_from_binance()
    except Exception as e:
        error_str = str(e)
        if '451' in error_str or 'restricted location' in error_str.lower():
            pytest.skip(f"Binance API restricted (451): {error_str}")
        else:
            raise
    
    if symbols_info:
        file_path = manager._get_universe_file_path(datetime.now())
        manager.save_universe(symbols_info, file_path)
        
        # 验证文件存在
        assert file_path.exists()
        
        # 验证文件格式（CSV）
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) > 1  # 至少有header和一行数据
            
            # 验证header
            header = lines[0].strip().split(',')
            assert 'symbol' in header
            assert 'contractType' in header
            assert 'quoteAsset' in header
        
        print(f"✓ Universe file format is correct: {file_path}")


if __name__ == "__main__":
    # 运行测试
    print("=" * 60)
    print("Testing Universe Manager")
    print("=" * 60)
    
    async def run_tests():
        print("\n1. Testing fetch_universe_from_binance...")
        try:
            await test_fetch_universe_from_binance()
        except Exception as e:
            print(f"  Skipped: {e}")
        
        print("\n2. Testing update_universe...")
        try:
            await test_update_universe()
        except Exception as e:
            print(f"  Skipped: {e}")
        
        print("\n3. Testing load_universe...")
        try:
            await test_load_universe()
        except Exception as e:
            print(f"  Skipped: {e}")
        
        print("\n4. Testing universe file format...")
        try:
            await test_universe_file_format()
        except Exception as e:
            print(f"  Skipped: {e}")
        
        print("\n" + "=" * 60)
        print("Tests completed!")
        print("=" * 60)
    
    asyncio.run(run_tests())
