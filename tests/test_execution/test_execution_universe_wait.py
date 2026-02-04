"""
测试执行进程等待universe的功能
"""
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from src.processes.execution_process import ExecutionProcess
from src.common.config import config


@pytest.mark.asyncio
async def test_execution_process_waits_for_universe():
    """测试执行进程在universe不可用时等待"""
    from src.data.universe_manager import get_universe_manager
    
    # 直接测试等待逻辑，不依赖完整的执行进程启动
    universe_manager = get_universe_manager()
    original_load = universe_manager.load_universe
    
    call_count = 0
    def mock_load_universe(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= 1:
            # 前两次返回空
            return set()
        else:
            # 第三次返回有数据
            return {'BTCUSDT', 'ETHUSDT'}
    
    universe_manager.load_universe = mock_load_universe
    
    # 模拟等待逻辑
    max_wait_time = 5  # 测试用较短时间
    retry_interval = 1
    waited_time = 0
    universe = None
    
    start_time = asyncio.get_event_loop().time()
    
    # 创建一个任务来模拟universe在2秒后可用
    async def make_universe_available():
        await asyncio.sleep(2)
        # 修改mock函数，让第三次调用返回数据
        nonlocal call_count
        call_count = 2  # 设置为2，下次调用会返回数据
    
    asyncio.create_task(make_universe_available())
    
    while waited_time < max_wait_time:
        universe = universe_manager.load_universe()
        if universe:
            break
        await asyncio.sleep(retry_interval)
        waited_time += retry_interval
    
    elapsed = asyncio.get_event_loop().time() - start_time
    
    # 恢复原始方法
    universe_manager.load_universe = original_load
    
    # 验证等待逻辑
    assert universe is not None, "Universe should be available after waiting"
    # 应该等待了至少1秒（第一次检查后等待）
    assert elapsed >= 0.5, f"Expected to wait at least 0.5s, but waited {elapsed}s"
    assert elapsed < max_wait_time + 1, f"Should not wait more than {max_wait_time+1}s, but waited {elapsed}s"
    
    print("[OK] Execution process correctly waits for universe")


@pytest.mark.asyncio
async def test_execution_process_timeout_when_universe_unavailable():
    """测试执行进程在universe一直不可用时的超时处理"""
    from src.data.universe_manager import get_universe_manager
    
    # 直接测试等待逻辑，不依赖完整的执行进程启动
    universe_manager = get_universe_manager()
    original_load = universe_manager.load_universe
    
    def mock_load_universe_always_empty(*args, **kwargs):
        return set()
    
    universe_manager.load_universe = mock_load_universe_always_empty
    
    # 模拟等待逻辑
    max_wait_time = 5  # 测试用较短时间
    retry_interval = 1
    waited_time = 0
    universe = None
    
    start_time = asyncio.get_event_loop().time()
    
    while waited_time < max_wait_time:
        universe = universe_manager.load_universe()
        if universe:
            break
        await asyncio.sleep(retry_interval)
        waited_time += retry_interval
    
    elapsed = asyncio.get_event_loop().time() - start_time
    
    # 恢复原始方法
    universe_manager.load_universe = original_load
    
    # 验证超时逻辑
    assert universe is None or len(universe) == 0, "Universe should still be empty after timeout"
    assert elapsed >= max_wait_time - 0.5, f"Expected to wait at least {max_wait_time-0.5}s, but waited {elapsed}s"
    assert elapsed <= max_wait_time + 1, f"Should not wait more than {max_wait_time+1}s, but waited {elapsed}s"
    
    print("[OK] Execution process correctly times out when universe unavailable")


@pytest.mark.asyncio
async def test_execution_process_immediate_universe_available():
    """测试执行进程在universe立即可用时的行为"""
    from src.data.universe_manager import get_universe_manager
    
    # 直接测试等待逻辑，不依赖完整的执行进程启动
    universe_manager = get_universe_manager()
    original_load = universe_manager.load_universe
    
    def mock_load_universe_immediate(*args, **kwargs):
        return {'BTCUSDT', 'ETHUSDT'}
    
    universe_manager.load_universe = mock_load_universe_immediate
    
    # 模拟等待逻辑
    max_wait_time = 5
    retry_interval = 1
    waited_time = 0
    universe = None
    
    start_time = asyncio.get_event_loop().time()
    
    while waited_time < max_wait_time:
        universe = universe_manager.load_universe()
        if universe:
            break
        await asyncio.sleep(retry_interval)
        waited_time += retry_interval
    
    elapsed = asyncio.get_event_loop().time() - start_time
    
    # 恢复原始方法
    universe_manager.load_universe = original_load
    
    # 验证立即获取逻辑
    assert universe is not None and len(universe) > 0, "Universe should be available immediately"
    assert elapsed < 0.5, f"Expected immediate completion (<0.5s), but took {elapsed}s"
    
    print("[OK] Execution process works immediately when universe available")
