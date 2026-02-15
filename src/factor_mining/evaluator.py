"""
因子评估器

功能：
1. IC（信息系数）计算
2. IR（信息比率）计算
3. 回测评估
4. 因子稳定性评估
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from ..common.logger import get_logger
from ..common.config import config
from ..strategy.calculator import AlphaCalculatorBase, AlphaDataView
from ..data.api import get_data_api
from .backtest_integration import FactorBacktestRunner
from .result_saver import FactorMiningResultSaver
from .utils import (
    get_interval_timedelta,
    get_default_interval,
    get_default_forward_periods,
    get_default_history_days,
    get_default_initial_balance,
)

logger = get_logger('factor_mining.evaluator')


@dataclass
class FactorMetrics:
    """因子评估指标"""
    # IC相关
    ic_mean: float = 0.0
    ic_std: float = 0.0
    ic_ir: float = 0.0  # IC的IR = IC_mean / IC_std
    ic_positive_ratio: float = 0.0  # IC为正的比例
    
    # 回测相关
    total_return: float = 0.0
    annual_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    
    # 稳定性
    ic_stability: float = 0.0  # IC稳定性的倒数（IC_std越小越稳定）
    return_stability: float = 0.0  # 收益稳定性
    
    # 其他
    coverage: float = 0.0  # 因子覆盖率（有值的symbol比例）
    timestamp: Optional[datetime] = None


class FactorEvaluator:
    """因子评估器"""
    
    def __init__(self, data_api=None, use_mock_data: bool = False):
        """
        初始化因子评估器
        
        Args:
            data_api: 数据API实例，如果为None则根据use_mock_data决定使用真实或模拟数据
            use_mock_data: 是否使用模拟数据（用于示例和测试）
        """
        if data_api is not None:
            self.data_api = data_api
        elif use_mock_data:
            from .mock_data import get_mock_data_api
            self.data_api = get_mock_data_api()
        else:
            self.data_api = get_data_api()
    
    def calculate_ic(self, factor_values: Dict[str, float], 
                   forward_returns: Dict[str, float],
                   method: str = 'pearson') -> float:
        """
        计算IC（信息系数）
        
        Args:
            factor_values: 因子值 {symbol: value}
            forward_returns: 未来收益 {symbol: return}
            method: 计算方法，'pearson' 或 'spearman'
        
        Returns:
            IC值
        """
        # 找到共同symbol
        common_symbols = set(factor_values.keys()) & set(forward_returns.keys())
        if len(common_symbols) < 2:
            return np.nan
        
        factor_vec = [factor_values[s] for s in common_symbols]
        return_vec = [forward_returns[s] for s in common_symbols]
        
        if method == 'pearson':
            ic = np.corrcoef(factor_vec, return_vec)[0, 1]
        elif method == 'spearman':
            try:
                from scipy.stats import spearmanr
                ic, _ = spearmanr(factor_vec, return_vec)
            except ImportError:
                logger.warning("scipy not available, using pearson correlation")
                ic = np.corrcoef(factor_vec, return_vec)[0, 1]
        else:
            raise ValueError(f"Unknown method: {method}")
        
        return ic if not np.isnan(ic) else 0.0
    
    def evaluate_ic_series(self, calculator: AlphaCalculatorBase,
                          start_date: datetime, end_date: datetime,
                          interval: Optional[str] = None, forward_periods: Optional[int] = None) -> List[float]:
        """
        评估IC时间序列
        
        Args:
            calculator: 因子计算器
            start_date: 开始日期
            end_date: 结束日期
            interval: 时间间隔，如果为None则从配置读取
            forward_periods: 未来收益周期数，如果为None则从配置读取
        
        Returns:
            IC时间序列
        """
        # 使用配置默认值
        if interval is None:
            interval = get_default_interval()
        if forward_periods is None:
            forward_periods = get_default_forward_periods()
        
        ic_series = []
        interval_delta = get_interval_timedelta(interval)
        
        # 获取时间标签
        current = start_date
        total_steps = int((end_date - start_date) / interval_delta)
        step_count = 0
        max_steps = 1000  # 限制最大迭代次数，避免卡住
        
        logger.info(f"开始评估IC时间序列，预计 {total_steps} 步（最多 {max_steps} 步）")
        
        while current < end_date and step_count < max_steps:
            step_count += 1
            
            # 每100步输出一次进度
            if step_count % 100 == 0:
                logger.debug(f"IC评估进度: {step_count}/{min(total_steps, max_steps)}")
            
            # 获取当前时间点的数据
            view = self._get_data_view_at_time(current, interval=interval)
            if view is None:
                current += interval_delta
                continue
            
            # 计算因子值
            try:
                factor_values = calculator.run(view)
            except Exception as e:
                logger.warning(f"计算因子失败: {e}")
                current += interval_delta
                continue
            
            if not factor_values:
                current += interval_delta
                continue
            
            # 根据interval计算未来时间
            forward_time = current + interval_delta * forward_periods
            forward_returns = self._get_forward_returns(current, forward_time, list(factor_values.keys()), interval=interval)
            
            if forward_returns:
                ic = self.calculate_ic(factor_values, forward_returns)
                if not np.isnan(ic):
                    ic_series.append(ic)
            
            current += interval_delta
        
        if step_count >= max_steps:
            logger.warning(f"IC评估达到最大步数限制 ({max_steps})，可能数据量过大")
        
        return ic_series
    
    def _get_data_view_at_time(self, timestamp: datetime, 
                               interval: Optional[str] = None,
                               history_days: Optional[int] = None) -> Optional[AlphaDataView]:
        """
        获取指定时间点的数据视图
        
        Args:
            timestamp: 时间戳
            interval: 时间间隔，如果为None则从配置读取
            history_days: 历史数据天数，如果为None则从配置读取
        
        Returns:
            数据视图
        """
        try:
            # 使用配置默认值
            if interval is None:
                interval = get_default_interval()
            if history_days is None:
                history_days = get_default_history_days()
            
            # 获取历史数据
            end_label = self.data_api._get_date_time_label_from_datetime(timestamp)
            begin_label = self.data_api._get_date_time_label_from_datetime(
                timestamp - timedelta(days=history_days)
            )
            
            bar_data = self.data_api.get_bar_between(begin_label, end_label, mode=interval)
            tran_stats = self.data_api.get_tran_stats_between(begin_label, end_label, mode=interval)
            
            # 只保留到timestamp之前的数据
            cutoff_time = timestamp.timestamp() * 1000  # 转换为毫秒
            
            filtered_bar = {}
            filtered_tran = {}
            
            for sym, df in bar_data.items():
                if 'open_time' in df.columns:
                    df_filtered = df[df['open_time'] <= cutoff_time]
                    if not df_filtered.empty:
                        filtered_bar[sym] = df_filtered
            
            for sym, df in tran_stats.items():
                if 'open_time' in df.columns:
                    df_filtered = df[df['open_time'] <= cutoff_time]
                    if not df_filtered.empty:
                        filtered_tran[sym] = df_filtered
            
            return AlphaDataView(bar_data=filtered_bar, tran_stats=filtered_tran)
        except Exception as e:
            logger.warning(f"获取数据视图失败: {e}")
            return None
    
    def _get_forward_returns(self, start_time: datetime, end_time: datetime,
                            symbols: List[str], interval: Optional[str] = None) -> Dict[str, float]:
        """
        获取未来收益
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            symbols: 交易对列表
            interval: 时间间隔，如果为None则从配置读取
        
        Returns:
            未来收益字典
        """
        try:
            # 使用配置默认值
            if interval is None:
                interval = get_default_interval()
            
            start_label = self.data_api._get_date_time_label_from_datetime(start_time)
            end_label = self.data_api._get_date_time_label_from_datetime(end_time)
            
            bar_data = self.data_api.get_bar_between(start_label, end_label, mode=interval)
            
            returns = {}
            for sym in symbols:
                if sym not in bar_data:
                    continue
                
                df = bar_data[sym]
                if df.empty or 'close' not in df.columns:
                    continue
                
                # 获取开始和结束价格
                if 'open_time' in df.columns:
                    df = df.sort_values('open_time')
                
                prices = pd.to_numeric(df['close'], errors='coerce')
                valid_prices = prices.dropna()
                
                if len(valid_prices) < 2:
                    continue
                
                start_price = float(valid_prices.iloc[0])
                end_price = float(valid_prices.iloc[-1])
                
                if start_price > 0:
                    returns[sym] = (end_price - start_price) / start_price
                else:
                    returns[sym] = 0.0
            
            return returns
        except Exception as e:
            logger.warning(f"获取未来收益失败: {e}")
            return {}
    
    def evaluate_factor(self, calculator: AlphaCalculatorBase,
                       start_date: datetime, end_date: datetime,
                       use_backtest: bool = True,
                       interval: Optional[str] = None,
                       initial_balance: Optional[float] = None) -> FactorMetrics:
        """
        评估因子
        
        Args:
            calculator: 因子计算器
            start_date: 开始日期
            end_date: 结束日期
            use_backtest: 是否使用回测评估
        
        Returns:
            因子评估指标
        """
        metrics = FactorMetrics()
        metrics.timestamp = datetime.now()
        
        # 使用配置默认值
        if interval is None:
            interval = get_default_interval()
        
        # 计算IC系列
        logger.info(f"评估因子 {calculator.name} 的IC...")
        ic_series = self.evaluate_ic_series(calculator, start_date, end_date, interval=interval)
        
        if ic_series:
            metrics.ic_mean = float(np.mean(ic_series))
            metrics.ic_std = float(np.std(ic_series))
            metrics.ic_ir = metrics.ic_mean / metrics.ic_std if metrics.ic_std > 0 else 0.0
            metrics.ic_positive_ratio = float(np.sum(np.array(ic_series) > 0) / len(ic_series))
            metrics.ic_stability = 1.0 / (metrics.ic_std + 1e-6)  # 稳定性（std越小越稳定）
        
        # 回测评估
        if use_backtest:
            logger.info(f"评估因子 {calculator.name} 的回测表现...")
            try:
                from ..backtest.backtest import FactorBacktestConfig
                
                # 使用配置默认值
                if initial_balance is None:
                    initial_balance = get_default_initial_balance()
                
                backtest_config = FactorBacktestConfig(
                    name=calculator.name,
                    start_date=start_date,
                    end_date=end_date,
                    initial_balance=initial_balance,
                    interval=interval,
                    leverage=1.0,
                    calculator_names=[calculator.name],
                )
                
                backtest_runner = FactorBacktestRunner()
                backtest_result = backtest_runner.run_backtest(backtest_config, [calculator])
                
                if backtest_result:
                    metrics.total_return = backtest_result.total_return
                    metrics.annual_return = backtest_result.annual_return
                    metrics.sharpe_ratio = backtest_result.sharpe_ratio
                    metrics.max_drawdown = backtest_result.max_drawdown
                    metrics.win_rate = backtest_result.win_rate
                    metrics.profit_factor = backtest_result.profit_factor
            except Exception as e:
                logger.warning(f"回测评估失败: {e}", exc_info=True)
        
        # 计算覆盖率
        try:
            view = self._get_data_view_at_time(end_date, interval=interval)
            if view:
                factor_values = calculator.run(view)
                total_symbols = len(list(view.iter_symbols()))
                if total_symbols > 0:
                    metrics.coverage = len(factor_values) / total_symbols
        except Exception as e:
            logger.warning(f"计算覆盖率失败: {e}")
        
        # 自动保存评估结果到data目录
        try:
            output_dir = FactorMiningResultSaver.save_evaluation_result(calculator.name, metrics)
            logger.info(f"因子评估结果已自动保存到: {output_dir}")
        except Exception as e:
            logger.warning(f"保存因子评估结果失败: {e}", exc_info=True)
        
        return metrics
    
    def compare_factors(self, calculators: List[AlphaCalculatorBase],
                       start_date: datetime, end_date: datetime,
                       interval: Optional[str] = None,
                       initial_balance: Optional[float] = None) -> pd.DataFrame:
        """
        比较多个因子
        
        Args:
            calculators: 因子计算器列表
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            比较结果DataFrame
        """
        results = []
        
        for calc in calculators:
            try:
                metrics = self.evaluate_factor(calc, start_date, end_date, 
                                              interval=interval, initial_balance=initial_balance)
                results.append({
                    'name': calc.name,
                    'ic_mean': metrics.ic_mean,
                    'ic_ir': metrics.ic_ir,
                    'ic_positive_ratio': metrics.ic_positive_ratio,
                    'total_return': metrics.total_return,
                    'sharpe_ratio': metrics.sharpe_ratio,
                    'max_drawdown': metrics.max_drawdown,
                    'coverage': metrics.coverage,
                })
            except Exception as e:
                logger.warning(f"评估因子 {calc.name} 失败: {e}")
        
        return pd.DataFrame(results)
