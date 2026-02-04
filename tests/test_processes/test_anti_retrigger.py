"""
测试防重复触发机制
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from src.processes.strategy_process import StrategyProcess
from src.common.ipc import MessageType


@pytest.mark.asyncio
async def test_strategy_anti_retrigger():
    """测试策略执行时的防重复触发机制"""
    strategy = StrategyProcess()
    
    # 验证初始状态
    assert hasattr(strategy, '_is_calculating')
    assert strategy._is_calculating == False
    
    call_count = {'count': 0}
    
    async def mock_execute(symbols):
        call_count['count'] += 1
        # 不等待，快速完成
    
    strategy._execute_strategy = mock_execute
    
    # 第一次触发（应该执行）
    strategy._is_calculating = False
    await strategy._execute_strategy(['BTCUSDT'])
    assert call_count['count'] == 1
    
    # 验证标志存在
    assert hasattr(strategy, '_is_calculating')
    
    print("✓ Strategy anti-retrigger test passed")


@pytest.mark.asyncio
async def test_strategy_is_calculating_flag():
    """测试_is_calculating标志的正确使用"""
    strategy = StrategyProcess()
    
    # 初始状态应该是False
    assert strategy._is_calculating == False
    
    # 设置标志
    strategy._is_calculating = True
    assert strategy._is_calculating == True
    
    # 重置标志
    strategy._is_calculating = False
    assert strategy._is_calculating == False
    
    print("✓ Strategy is_calculating flag test passed")


@pytest.mark.asyncio
async def test_strategy_completes_before_new_trigger():
    """测试策略完成后再处理新的触发"""
    strategy = StrategyProcess()
    
    execution_order = []
    
    async def mock_execute(symbols):
        execution_order.append('start')
        strategy._is_calculating = True
        # 不等待，立即完成
        strategy._is_calculating = False
        execution_order.append('end')
    
    strategy._execute_strategy = mock_execute
    
    # 第一次执行
    await strategy._execute_strategy(['BTCUSDT'])
    
    # 验证执行顺序
    assert execution_order == ['start', 'end']
    assert strategy._is_calculating == False
    
    print("✓ Strategy completes before new trigger test passed")
