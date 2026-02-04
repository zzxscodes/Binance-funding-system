"""
回测API接口
提供统一的回测接口供外部调用
"""
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime, timezone
from pathlib import Path
import logging

from ..common.logger import get_logger
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
        interval: str = "5m",
    ) -> BacktestResult:
        """
        运行单周期回测
        
        Args:
            config: 回测配置
            strategy_func: 策略函数，接收(portfolio_state, klines)，返回目标持仓权重
            interval: K线周期 (e.g., "5m", "1h", "4h")
        
        Returns:
            BacktestResult 回测结果
        
        Example:
            >>> config = BacktestConfig(
            ...     name="simple_ma_strategy",
            ...     start_date=datetime(2024, 1, 1),
            ...     end_date=datetime(2024, 12, 31),
            ...     initial_balance=10000,
            ...     symbols=["BTCUSDT", "ETHUSDT"],
            ... )
            >>> 
            >>> def simple_strategy(portfolio, klines):
            ...     weights = {}
            ...     for symbol, kline in klines.items():
            ...         weights[symbol] = 0.5 if kline.close > kline.open else 0.0
            ...     return weights
            >>> 
            >>> result = BacktestAPI.run_backtest(config, simple_strategy)
        """
        try:
            # 1. 创建数据重放引擎
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
            
            # 2. 创建回测执行器
            executor = BacktestExecutor(config, replay_engine)
            
            # 3. 运行回测
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
        
        Args:
            config: 回测配置
            strategy_func: 策略函数，接收(portfolio_state, {interval: {symbol: kline}}），返回目标持仓权重
            intervals: K线周期列表 (e.g., ["5m", "1h", "4h"])
        
        Returns:
            BacktestResult 回测结果
        """
        if intervals is None:
            intervals = ["5m"]
        
        try:
            # 1. 创建多周期重放引擎
            logger.info(f"Creating multi-interval replay engine: {intervals}")
            replay_engine = MultiIntervalReplayEngine(
                symbols=config.symbols,
                start_date=config.start_date,
                end_date=config.end_date,
                intervals=intervals
            )
            
            # 获取主周期的重放引擎
            primary_engine = replay_engine.engines[intervals[0]]
            
            if not primary_engine.has_data():
                raise ValueError(f"No historical data found for symbols: {config.symbols}")
            
            # 2. 创建回测执行器
            executor = BacktestExecutor(config, primary_engine)
            
            # 3. 运行回测（包装策略函数以支持多周期）
            def wrapped_strategy(portfolio: PortfolioState, klines: Dict[str, KlineSnapshot]) -> Dict[str, float]:
                # 为每个周期构建K线数据
                multi_interval_klines = {}
                for interval in intervals:
                    engine = replay_engine.get_engine(interval)
                    multi_interval_klines[interval] = engine.get_current_snapshot()
                
                # 调用原始策略函数
                return strategy_func(portfolio, multi_interval_klines)
            
            # 运行回测
            logger.info(f"Running multi-interval backtest: {config.name}")
            result = executor.run(wrapped_strategy)
            
            return result
            
        except Exception as e:
            logger.error(f"Multi-interval backtest failed: {e}", exc_info=True)
            raise
    
    @staticmethod
    def analyze_result(result: BacktestResult) -> Dict:
        """
        分析回测结果
        
        Args:
            result: 回测结果
        
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
        
        Args:
            result: 回测结果
            output_dir: 输出目录（如果提供会保存文件）
            format: 输出格式 ("text", "json", "csv")
        
        Returns:
            报告内容（文本格式）
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
        interval: str = "5m",
        compare: bool = True
    ) -> Dict[str, BacktestResult]:
        """
        批量运行多个回测
        
        Args:
            configs: 回测配置列表
            strategy_func: 策略函数
            interval: K线周期
            compare: 是否生成对比报告
        
        Returns:
            {config_name: BacktestResult} 字典
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


# 便捷函数
def create_backtest_config(
    name: str,
    start_date: datetime,
    end_date: datetime,
    initial_balance: float = 10000.0,
    symbols: List[str] = None,
    **kwargs
) -> BacktestConfig:
    """
    创建回测配置的便捷函数
    
    Args:
        name: 回测名称
        start_date: 开始日期
        end_date: 结束日期
        initial_balance: 初始资金
        symbols: 交易对列表
        **kwargs: 其他配置参数
    
    Returns:
        BacktestConfig
    """
    if symbols is None:
        symbols = []
    
    # 确保日期是UTC时区
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    
    return BacktestConfig(
        name=name,
        start_date=start_date,
        end_date=end_date,
        initial_balance=initial_balance,
        symbols=symbols,
        **kwargs
    )
