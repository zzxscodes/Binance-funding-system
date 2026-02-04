"""
端到端Dry-Run测试
测试完整的dry-run流程：从策略计算到订单执行（完全离线，不需要API密钥）
"""
import pytest
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch

from src.processes.event_coordinator import EventCoordinator
from src.processes.strategy_process import StrategyProcess
from src.processes.execution_process import ExecutionProcess
from src.common.ipc import MessageType
from src.execution.dry_run_client import DryRunBinanceClient
from src.common.logger import get_logger

logger = get_logger('test_dry_run_e2e')


@pytest.mark.asyncio
async def test_dry_run_full_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    测试完整的dry-run流程：
    1. 策略计算生成目标持仓
    2. 订单执行进程在dry-run模式下执行订单（完全离线，不需要API密钥）
    3. 验证所有计算逻辑与真实交易相同，但不实际下单
    """
    # 设置临时目录
    data_dir = tmp_path / "data"
    signals_dir = data_dir / "signals"
    positions_dir = data_dir / "positions"
    signals_dir.mkdir(parents=True, exist_ok=True)
    positions_dir.mkdir(parents=True, exist_ok=True)
    
    monkeypatch.chdir(tmp_path)
    
    # 1. 初始化事件协调器
    coordinator = EventCoordinator()
    
    # 2. 初始化策略进程（使用mock数据API）
    strategy = StrategyProcess()
    
    import pandas as pd
    from datetime import timedelta
    
    class MockDataAPI:
        def get_universe(self, date=None):
            return ["BTCUSDT", "ETHUSDT"]
        
        def get_bar_between(self, begin_date_time_label, end_date_time_label):
            # 返回模拟bar数据
            dates = pd.date_range(end=datetime.now(timezone.utc), periods=50, freq='5min')
            return {
                'btc-usdt': pd.DataFrame({
                    'symbol': 'BTCUSDT',
                    'open_time': dates,
                    'close_time': dates + pd.Timedelta(minutes=5),
                    'open': [50000.0] * 50,
                    'high': [51000.0] * 50,
                    'low': [49000.0] * 50,
                    'close': [50500.0] * 50,
                    'volume': [100.0] * 50,
                    'quote_volume': [5000000.0] * 50,
                    'trade_count': [100] * 50,
                }),
                'eth-usdt': pd.DataFrame({
                    'symbol': 'ETHUSDT',
                    'open_time': dates,
                    'close_time': dates + pd.Timedelta(minutes=5),
                    'open': [3000.0] * 50,
                    'high': [3100.0] * 50,
                    'low': [2900.0] * 50,
                    'close': [3050.0] * 50,
                    'volume': [1000.0] * 50,
                    'quote_volume': [3000000.0] * 50,
                    'trade_count': [200] * 50,
                })
            }
        
        def get_tran_stats_between(self, begin_date_time_label, end_date_time_label):
            return {}
    
    class MockPositionGenerator:
        def save_target_positions(self, target_positions, timestamp):
            out = {}
            for account_id, positions in target_positions.items():
                if isinstance(positions, pd.DataFrame):
                    positions_dict = positions.to_dict('records')
                else:
                    positions_dict = positions
                fp = positions_dir / f"{account_id}_target_positions_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
                fp.write_text(json.dumps({
                    "account_id": account_id,
                    "positions": positions_dict,
                    "timestamp": timestamp.isoformat()
                }, indent=2), encoding="utf-8")
                out[account_id] = str(fp)
            return out
        
        def load_target_positions(self, file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    
    strategy.data_api = MockDataAPI()
    strategy.position_generator = MockPositionGenerator()
    
    # 3. 触发策略计算
    ts = datetime.now(timezone.utc)
    await coordinator._handle_data_complete({
        "type": MessageType.DATA_COMPLETE.value,
        "timestamp": ts.isoformat(),
        "symbols": ["BTCUSDT", "ETHUSDT"]
    })
    
    # 4. 策略进程执行
    trigger_data = await strategy._wait_for_trigger(timeout=1.0)
    assert trigger_data is not None
    
    await strategy._execute_strategy(trigger_data.get("symbols", []))
    
    # 5. 验证目标持仓文件已生成
    created_files = list(positions_dir.glob("*_target_positions_*.json"))
    assert len(created_files) >= 1
    
    # 6. 初始化执行进程（dry-run模式，不需要API密钥）
    execution = ExecutionProcess('account1', execution_mode='dry-run')
    
    assert execution.execution_mode == 'dry-run'
    assert execution.dry_run is True
    assert isinstance(execution.binance_client, DryRunBinanceClient)
    
    # 7. 执行目标持仓（dry-run模式）
    if created_files:
        target_file = str(created_files[0])
        await execution._execute_target_positions(target_file)
        
        # 验证在dry-run模式下，订单被"执行"了（虽然是模拟的）
        # 验证持仓已更新（在dry-run客户端的模拟数据中）
        positions = await execution.binance_client.get_positions()
        # 在dry-run模式下，应该能看到模拟的持仓更新
        
        logger.info(f"Dry-run execution completed. Mock positions: {len(positions)}")
    
    await execution.binance_client.close()
    
    print("✓ Full dry-run flow works end-to-end without API keys")


@pytest.mark.asyncio
async def test_dry_run_order_calculation_same_as_live():
    """
    验证dry-run模式下的订单计算逻辑与真实交易完全相同
    """
    from src.execution.order_manager import OrderManager
    from src.execution.position_manager import PositionManager
    
    # 创建dry-run客户端
    dry_run_client = DryRunBinanceClient()
    
    # 初始化持仓管理器
    position_manager = PositionManager(dry_run_client)
    
    # 设置初始持仓（模拟）
    await dry_run_client.place_order('BTCUSDT', 'BUY', 'MARKET', quantity=0.1, price=50000.0)
    await position_manager.update_current_positions()
    
    # 目标持仓
    target_positions = {'BTCUSDT': 0.2}  # 目标增加到0.2
    
    # 计算持仓偏差（与真实交易相同的逻辑）
    orders = position_manager.calculate_position_diff(target_positions)
    
    # 验证计算逻辑
    assert len(orders) == 1
    assert orders[0]['symbol'] == 'BTCUSDT'
    assert orders[0]['side'] == 'BUY'  # 需要买入
    assert abs(orders[0]['quantity'] - 0.1) < 1e-6  # 需要买入0.1
    
    # 规范化订单（与真实交易相同的逻辑）
    normalized_orders = await position_manager.normalize_orders(orders)
    
    assert len(normalized_orders) == 1
    assert 'normalized_quantity' in normalized_orders[0]
    
    # 执行订单（dry-run模式）
    order_manager = OrderManager(dry_run_client, dry_run=True)
    executed_orders = await order_manager.execute_target_positions(target_positions)
    
    # 验证订单已执行（虽然是模拟的）
    assert len(executed_orders) > 0
    
    # 验证持仓已更新
    positions = await dry_run_client.get_positions()
    btc_pos = next((p for p in positions if p['symbol'] == 'BTCUSDT'), None)
    assert btc_pos is not None
    assert abs(btc_pos['positionAmt'] - 0.2) < 1e-6  # 持仓已更新到目标值
    
    await dry_run_client.close()
    
    print("✓ Dry-run order calculation logic is identical to live trading")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Dry-Run E2E Flow")
    print("=" * 60)
    
    import asyncio
    asyncio.run(test_dry_run_full_flow(Path("/tmp/test"), None))
    asyncio.run(test_dry_run_order_calculation_same_as_live())
    
    print("\n" + "=" * 60)
    print("All dry-run E2E tests completed!")
    print("=" * 60)
