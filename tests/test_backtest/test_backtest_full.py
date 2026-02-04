"""
完整的回测系统测试
使用模拟数据测试历史数据重放、回测执行、结果分析等功能
"""
import sys
import asyncio
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict

import pytest

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.backtest.models import (
    BacktestConfig, OrderSide, PositionMode, KlineSnapshot, 
    PortfolioState, Trade, Position
)
from src.backtest.mock_data import MockKlineGenerator, MockDataManager
from src.backtest.replay import DataReplayEngine, MultiIntervalReplayEngine
from src.backtest.executor import BacktestExecutor
from src.backtest.api import BacktestAPI, create_backtest_config
from src.backtest.analysis import BacktestAnalyzer
from src.common.logger import get_logger

logger = get_logger('test_backtest')


class TestMockDataGeneration:
    """测试模拟数据生成"""
    
    def test_mock_kline_generator_creates_valid_data(self):
        """测试模拟K线生成器"""
        generator = MockKlineGenerator(seed=42)
        
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        
        klines = generator.generate_klines(
            symbol="BTCUSDT",
            start_date=start_date,
            end_date=end_date,
            initial_price=42000.0,
            volatility=0.02,
            interval_minutes=5
        )
        
        # 验证数据
        assert len(klines) > 0, "Should generate klines"
        assert 'open_time' in klines.columns, "Should have open_time column"
        assert 'open' in klines.columns, "Should have open column"
        assert 'high' in klines.columns, "Should have high column"
        assert 'low' in klines.columns, "Should have low column"
        assert 'close' in klines.columns, "Should have close column"
        assert 'volume' in klines.columns, "Should have volume column"
        
        # 验证价格逻辑
        for idx, row in klines.iterrows():
            assert row['low'] <= row['open'], "Low should be <= Open"
            assert row['low'] <= row['high'], "Low should be <= High"
            assert row['open'] <= row['high'], "Open should be <= High"
            assert row['close'] <= row['high'], "Close should be <= High"
            assert row['close'] >= row['low'], "Close should be >= Low"
        
        logger.info(f"✓ Generated {len(klines)} valid klines for BTCUSDT")
    
    def test_mock_data_manager_saves_and_loads(self):
        """测试模拟数据管理器的保存和加载"""
        manager = MockDataManager()
        
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 7, tzinfo=timezone.utc)
        symbols = ["BTCUSDT", "ETHUSDT"]
        
        # 生成并保存模拟数据
        manager.generate_and_save_mock_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            initial_prices={"BTCUSDT": 42000, "ETHUSDT": 2500},
            seed=42
        )
        
        logger.info("✓ Mock data generated and saved")


class TestDataReplayEngine:
    """测试数据重放引擎"""
    
    @pytest.fixture
    def setup_mock_data(self):
        """设置模拟数据"""
        manager = MockDataManager()
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 7, tzinfo=timezone.utc)
        symbols = ["BTCUSDT"]
        
        manager.generate_and_save_mock_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            initial_prices={"BTCUSDT": 42000},
            seed=42
        )
        
        return symbols, start_date, end_date
    
    def test_replay_engine_loads_historical_data(self, setup_mock_data):
        """测试重放引擎加载历史数据"""
        symbols, start_date, end_date = setup_mock_data
        
        engine = DataReplayEngine(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            interval="5m"
        )
        
        assert engine.has_data(), "Should have loaded data"
        assert "BTCUSDT" in engine.get_available_symbols(), "Should have BTCUSDT data"
        
        kline_count = engine.get_kline_count("BTCUSDT")
        assert kline_count > 0, "Should have klines for BTCUSDT"
        
        logger.info(f"✓ Loaded {kline_count} klines for BTCUSDT")
    
    def test_replay_iterator_produces_events(self, setup_mock_data):
        """测试重放迭代器生成事件"""
        symbols, start_date, end_date = setup_mock_data
        
        engine = DataReplayEngine(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            interval="5m"
        )
        
        event_count = 0
        for timestamp, klines in engine.replay_iterator():
            event_count += 1
            assert timestamp is not None, "Should have timestamp"
            assert isinstance(klines, dict), "Should return klines dict"
            assert "BTCUSDT" in klines, "Should have BTCUSDT kline"
            
            kline = klines["BTCUSDT"]
            assert isinstance(kline, KlineSnapshot), "Should be KlineSnapshot"
            assert kline.symbol == "BTCUSDT"
            assert kline.close > 0, "Should have valid price"
        
        assert event_count > 0, "Should generate events"
        logger.info(f"✓ Generated {event_count} replay events")
    
    def test_multi_interval_replay_engine(self, setup_mock_data):
        """测试多周期重放引擎"""
        symbols, start_date, end_date = setup_mock_data
        
        engine = MultiIntervalReplayEngine(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            intervals=["5m", "1h"]
        )
        
        assert engine.get_engine("5m") is not None, "Should have 5m engine"
        assert engine.get_engine("1h") is not None, "Should have 1h engine"
        
        available = engine.get_available_symbols()
        assert "BTCUSDT" in available, "Should have BTCUSDT in available symbols"
        
        logger.info(f"✓ Multi-interval engine created with {len(engine.engines)} intervals")


class TestBacktestExecutor:
    """测试回测执行引擎"""
    
    @pytest.fixture
    def simple_config_and_engine(self):
        """创建简单的配置和引擎"""
        manager = MockDataManager()
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 7, tzinfo=timezone.utc)
        symbols = ["BTCUSDT"]
        
        manager.generate_and_save_mock_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            initial_prices={"BTCUSDT": 42000},
            seed=42
        )
        
        config = BacktestConfig(
            name="simple_test",
            start_date=start_date,
            end_date=end_date,
            initial_balance=10000.0,
            symbols=symbols,
            leverage=1.0,
            maker_fee=0.0002,
            taker_fee=0.0004
        )
        
        engine_replay = DataReplayEngine(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            interval="5m"
        )
        
        return config, engine_replay
    
    def test_backtest_executor_runs_simple_strategy(self, simple_config_and_engine):
        """测试回测执行器运行简单策略"""
        config, replay_engine = simple_config_and_engine
        
        executor = BacktestExecutor(config, replay_engine)
        
        # 定义简单策略：永远买入50%
        def simple_strategy(portfolio: PortfolioState, klines: Dict) -> Dict:
            return {symbol: 0.5 for symbol in config.symbols}
        
        result = executor.run(simple_strategy)
        
        assert result is not None, "Should return result"
        assert len(result.trades) > 0, "Should generate trades"
        assert len(result.portfolio_history) > 0, "Should record portfolio history"
        assert result.total_trades > 0, "Should have trades"
        
        logger.info(f"✓ Executed strategy with {result.total_trades} trades")
        logger.info(f"  Final balance: ${result.portfolio_history[-1].total_balance:.2f}")
        logger.info(f"  Total return: {result.total_return:.2f}%")
    
    def test_backtest_executor_with_varying_positions(self, simple_config_and_engine):
        """测试回测执行器处理变化的持仓"""
        config, replay_engine = simple_config_and_engine
        executor = BacktestExecutor(config, replay_engine)
        
        # 定义策略：根据收盘价变化持仓
        def dynamic_strategy(portfolio: PortfolioState, klines: Dict) -> Dict:
            weights = {}
            for symbol, kline in klines.items():
                # 如果收盘价高于开盘价，持仓0.5; 否则0
                weight = 0.5 if kline.close > kline.open else 0.0
                weights[symbol] = weight
            return weights
        
        result = executor.run(dynamic_strategy)
        
        assert result.total_trades > 0, "Should generate trades from price changes"
        logger.info(f"✓ Dynamic strategy generated {result.total_trades} trades")


class TestBacktestAPI:
    """测试回测API"""
    
    @pytest.fixture
    def test_config_with_data(self):
        """创建测试配置和数据"""
        manager = MockDataManager()
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        symbols = ["BTCUSDT", "ETHUSDT"]
        
        manager.generate_and_save_mock_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            initial_prices={"BTCUSDT": 42000, "ETHUSDT": 2500},
            seed=42
        )
        
        config = create_backtest_config(
            name="api_test",
            start_date=start_date,
            end_date=end_date,
            initial_balance=10000.0,
            symbols=symbols,
            leverage=2.0
        )
        
        return config
    
    def test_run_backtest_single_interval(self, test_config_with_data):
        """测试运行单周期回测"""
        config = test_config_with_data
        
        def buy_and_hold_strategy(portfolio: PortfolioState, klines: Dict) -> Dict:
            """买入持有策略"""
            weights = {}
            for symbol in config.symbols:
                weights[symbol] = 1.0 / len(config.symbols)  # 等权持仓
            return weights
        
        result = BacktestAPI.run_backtest(
            config=config,
            strategy_func=buy_and_hold_strategy,
            interval="5m"
        )
        
        assert result.config.name == "api_test"
        assert result.total_trades > 0
        assert len(result.portfolio_history) > 0
        
        logger.info(f"✓ Single interval backtest completed")
        logger.info(f"  Total return: {result.total_return:.2f}%")
    
    def test_run_multi_interval_backtest(self, test_config_with_data):
        """测试运行多周期回测"""
        config = test_config_with_data
        
        def multi_interval_strategy(portfolio: PortfolioState, multi_klines: Dict) -> Dict:
            """多周期策略"""
            weights = {}
            
            # 使用5m周期的价格信息
            klines_5m = multi_klines.get("5m", {})
            for symbol in config.symbols:
                if symbol in klines_5m:
                    weights[symbol] = 0.5
            
            return weights
        
        result = BacktestAPI.run_multi_interval_backtest(
            config=config,
            strategy_func=multi_interval_strategy,
            intervals=["5m", "1h", "4h"]
        )
        
        assert result.total_trades > 0
        logger.info(f"✓ Multi-interval backtest completed")


class TestBacktestAnalyzer:
    """测试回测分析器"""
    
    @pytest.fixture
    def sample_result(self):
        """创建样本回测结果"""
        manager = MockDataManager()
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        symbols = ["BTCUSDT"]
        
        manager.generate_and_save_mock_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            initial_prices={"BTCUSDT": 42000},
            seed=42
        )
        
        config = BacktestConfig(
            name="analysis_test",
            start_date=start_date,
            end_date=end_date,
            initial_balance=10000.0,
            symbols=symbols
        )
        
        replay_engine = DataReplayEngine(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            interval="5m"
        )
        
        executor = BacktestExecutor(config, replay_engine)
        
        def strategy(portfolio: PortfolioState, klines: Dict) -> Dict:
            return {symbol: 0.5 for symbol in config.symbols}
        
        return executor.run(strategy)
    
    def test_analyze_result_generates_statistics(self, sample_result):
        """测试生成统计数据"""
        stats = BacktestAnalyzer.calculate_statistics(sample_result)
        
        assert 'summary' in stats
        assert 'returns' in stats
        assert 'risk_metrics' in stats
        assert 'trade_statistics' in stats
        
        assert stats['summary']['backtest_name'] == 'analysis_test'
        assert stats['trade_statistics']['total_trades'] > 0
        
        logger.info("✓ Generated comprehensive statistics")
    
    def test_generate_report(self, sample_result):
        """测试生成报告"""
        report = BacktestAnalyzer.generate_report(sample_result)
        
        assert report is not None
        assert 'analysis_test' in report
        assert '【基本信息】' in report
        assert '【收益统计】' in report
        assert '【风险指标】' in report
        
        logger.info("✓ Generated backtest report")
        logger.info("\n" + report)


class TestEndToEnd:
    """端到端集成测试"""
    
    def test_full_backtest_workflow(self):
        """测试完整的回测工作流"""
        # 1. 准备数据
        manager = MockDataManager()
        start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        symbols = ["BTCUSDT", "ETHUSDT"]
        
        logger.info("Step 1: Generating mock data...")
        manager.generate_and_save_mock_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            initial_prices={"BTCUSDT": 42000, "ETHUSDT": 2500},
            seed=42
        )
        
        # 2. 创建配置
        logger.info("Step 2: Creating backtest config...")
        config = create_backtest_config(
            name="e2e_test",
            start_date=start_date,
            end_date=end_date,
            initial_balance=10000.0,
            symbols=symbols,
            leverage=2.0
        )
        
        # 3. 定义策略
        logger.info("Step 3: Defining strategy...")
        def momentum_strategy(portfolio: PortfolioState, klines: Dict) -> Dict:
            """动量策略：价格上升则买入，下降则卖出"""
            weights = {}
            for symbol, kline in klines.items():
                momentum = (kline.close - kline.open) / kline.open
                # 使用动量信号确定持仓
                weight = 0.5 if momentum > 0.0001 else 0.0
                weights[symbol] = weight
            return weights
        
        # 4. 运行回测
        logger.info("Step 4: Running backtest...")
        result = BacktestAPI.run_backtest(
            config=config,
            strategy_func=momentum_strategy,
            interval="5m"
        )
        
        # 5. 分析结果
        logger.info("Step 5: Analyzing results...")
        stats = BacktestAnalyzer.calculate_statistics(result)
        report = BacktestAnalyzer.generate_report(result)
        
        # 6. 验证结果
        logger.info("Step 6: Verifying results...")
        assert result.total_trades > 0, "Should have trades"
        assert result.total_return is not None, "Should have return"
        assert len(result.portfolio_history) > 0, "Should have history"
        
        logger.info("\n✅ End-to-end test completed successfully!\n")
        logger.info(report)
        
        return result, stats, report


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v", "-s"])
