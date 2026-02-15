"""
回测API接口
提供统一的回测接口供外部调用
支持多因子回测系统
"""
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timezone
from pathlib import Path
import logging

from ..common.logger import get_logger
from .utils import get_default_initial_balance, get_default_interval
from .models import BacktestConfig, PortfolioState, KlineSnapshot, BacktestResult
from .replay import DataReplayEngine, MultiIntervalReplayEngine
from .executor import BacktestExecutor
from .analysis import BacktestAnalyzer

logger = get_logger('backtest_api')


class BacktestAPI:
    """回测API - 提供高级接口供用户调用"""
    
    @staticmethod
    def run_backtest(
        config: BacktestConfig,
        strategy_func: Callable[[PortfolioState, Dict[str, KlineSnapshot]], Dict[str, float]],
        interval: Optional[str] = None,
    ) -> BacktestResult:
        """
        运行单周期回测
        
        Args:
            config: 回测配置
            strategy_func: 策略函数，接收(portfolio_state, klines)，返回目标持仓权重
            interval: K线周期，如果为None则从配置读取
        
        Returns:
            BacktestResult 回测结果
        """
        if interval is None:
            interval = get_default_interval()
        
        try:
            logger.info(f"Creating replay engine for {len(config.symbols)} symbols")
            replay_engine = DataReplayEngine(
                symbols=config.symbols,
                start_date=config.start_date,
                end_date=config.end_date,
                interval=interval
            )
            
            if not replay_engine.has_data():
                raise ValueError(f"No historical data found for symbols: {config.symbols}")
            
            available_symbols = replay_engine.get_available_symbols()
            logger.info(f"Loaded data for symbols: {available_symbols}")
            
            executor = BacktestExecutor(config, replay_engine)
            
            logger.info(f"Running backtest: {config.name}")
            result = executor.run(strategy_func)
            
            return result
            
        except Exception as e:
            logger.error(f"Backtest failed: {e}", exc_info=True)
            raise
    
    @staticmethod
    def run_multi_interval_backtest(
        config: BacktestConfig,
        strategy_func: Callable[[PortfolioState, Dict[str, Dict[str, KlineSnapshot]]], Dict[str, float]],
        intervals: List[str] = None,
    ) -> BacktestResult:
        """
        运行多周期回测
        """
        if intervals is None:
            intervals = ["5m"]
        
        try:
            logger.info(f"Creating multi-interval replay engine: {intervals}")
            replay_engine = MultiIntervalReplayEngine(
                symbols=config.symbols,
                start_date=config.start_date,
                end_date=config.end_date,
                intervals=intervals
            )
            
            primary_engine = replay_engine.engines[intervals[0]]
            
            if not primary_engine.has_data():
                raise ValueError(f"No historical data found for symbols: {config.symbols}")
            
            executor = BacktestExecutor(config, primary_engine)
            
            def wrapped_strategy(portfolio: PortfolioState, klines: Dict[str, KlineSnapshot]) -> Dict[str, float]:
                multi_interval_klines = {}
                for interval in intervals:
                    engine = replay_engine.get_engine(interval)
                    multi_interval_klines[interval] = engine.get_current_snapshot()
                return strategy_func(portfolio, multi_interval_klines)
            
            logger.info(f"Running multi-interval backtest: {config.name}")
            result = executor.run(wrapped_strategy)
            
            return result
            
        except Exception as e:
            logger.error(f"Multi-interval backtest failed: {e}", exc_info=True)
            raise
    
    @staticmethod
    def run_factor_backtest(
        calculator: Any,
        start_date: datetime,
        end_date: datetime,
        initial_balance: Optional[float] = None,
        symbols: Optional[List[str]] = None,
        capital_allocation: str = "rank_weight",
        long_count: int = 5,
        short_count: int = 5,
        verbose: bool = True,
    ) -> BacktestResult:
        """
        运行因子回测 - 使用新的多因子回测系统
        
        Args:
            calculator: 因子计算器 (AlphaCalculatorBase)
            start_date: 开始日期
            end_date: 结束日期
            initial_balance: 初始资金
            symbols: 交易对列表
            capital_allocation: 资金分配方式
            long_count: 做多数量
            short_count: 做空数量
            verbose: 是否打印进度
        
        Returns:
            BacktestResult 回测结果
        """
        from .backtest import run_backtest
        
        result = run_backtest(
            calculator=calculator,
            start_date=start_date,
            end_date=end_date,
            initial_balance=initial_balance,
            symbols=symbols,
            capital_allocation=capital_allocation,
            long_count=long_count,
            short_count=short_count,
            verbose=verbose,
        )
        
        return result
    
    @staticmethod
    def analyze_result(result: BacktestResult) -> Dict:
        """
        分析回测结果
        
        Returns:
            包含详细统计指标的字典
        """
        return BacktestAnalyzer.calculate_statistics(result)
    
    @staticmethod
    def generate_report(
        result: BacktestResult,
        output_dir: Optional[Path] = None,
        format: str = "text"
    ) -> str:
        """
        生成回测报告
        """
        if format == "text":
            report = BacktestAnalyzer.generate_report(result)
            
            if output_dir:
                output_path = Path(output_dir) / f"{result.config.name}_report.txt"
                output_path.write_text(report, encoding='utf-8')
                logger.info(f"Report saved to {output_path}")
            
            return report
        
        elif format == "json":
            if output_dir:
                output_path = Path(output_dir) / f"{result.config.name}_result.json"
                BacktestAnalyzer.export_json(result, output_path)
            
            return "JSON report generated"
        
        elif format == "csv":
            if output_dir:
                output_dir = Path(output_dir)
                BacktestAnalyzer.export_trades_csv(result, output_dir / f"{result.config.name}_trades.csv")
                BacktestAnalyzer.export_portfolio_history_csv(result, output_dir / f"{result.config.name}_portfolio.csv")
            
            return "CSV reports generated"
        
        else:
            raise ValueError(f"Unknown format: {format}")
    
    @staticmethod
    def batch_run(
        configs: List[BacktestConfig],
        strategy_func: Callable,
        interval: Optional[str] = None,
        compare: bool = True
    ) -> Dict[str, BacktestResult]:
        """
        批量运行多个回测
        """
        results = {}
        
        for config in configs:
            logger.info(f"Running backtest: {config.name}")
            try:
                result = BacktestAPI.run_backtest(config, strategy_func, interval)
                results[config.name] = result
                logger.info(f"Completed: {config.name}, Return: {result.total_return:.2f}%")
            except Exception as e:
                logger.error(f"Failed to run backtest {config.name}: {e}")
        
        if compare and len(results) > 1:
            BacktestAPI._print_comparison(results)
        
        return results
    
    @staticmethod
    def _print_comparison(results: Dict[str, BacktestResult]):
        """打印回测结果对比"""
        logger.info("\n" + "=" * 100)
        logger.info("回测结果对比")
        logger.info("=" * 100)
        
        for name, result in results.items():
            logger.info(
                f"{name:30s} | "
                f"Return: {result.total_return:8.2f}% | "
                f"Sharpe: {result.sharpe_ratio:6.2f} | "
                f"MaxDD: {result.max_drawdown:6.2f}% | "
                f"Trades: {len(result.trades):5d} | "
                f"WinRate: {result.win_rate:6.2f}%"
            )
        
        logger.info("=" * 100)


def create_backtest_config(
    name: str,
    start_date: datetime,
    end_date: datetime,
    initial_balance: Optional[float] = None,
    symbols: Optional[List[str]] = None,
    **kwargs
) -> BacktestConfig:
    """
    创建回测配置的便捷函数
    """
    if symbols is None:
        symbols = []
    
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    
    if initial_balance is None:
        initial_balance = get_default_initial_balance()
    
    return BacktestConfig(
        name=name,
        start_date=start_date,
        end_date=end_date,
        initial_balance=initial_balance,
        symbols=symbols,
        **kwargs
    )
