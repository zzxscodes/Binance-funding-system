"""
因子回测集成

功能：
1. 与回测系统集成
2. 运行因子回测
3. 生成回测报告
"""

from __future__ import annotations

from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path

from ..common.logger import get_logger
from ..strategy.calculator import AlphaCalculatorBase
from ..backtest.backtest import FactorBacktestConfig
from ..backtest.models import BacktestResult
from .result_saver import FactorMiningResultSaver

logger = get_logger('factor_mining.backtest_integration')


class FactorBacktestRunner:
    """因子回测运行器"""
    
    def __init__(self):
        pass
    
    def run_backtest(self, config: FactorBacktestConfig,
                    calculators: List[AlphaCalculatorBase]) -> Optional[BacktestResult]:
        """
        运行因子回测
        
        Args:
            config: 回测配置
            calculators: 因子计算器列表
        
        Returns:
            回测结果
        """
        try:
            from ..backtest.backtest import (
                SingleCalculatorBacktest,
                AlphaBacktest,
                run_single_calculator_backtest,
                run_alpha_backtest,
            )
            from ..backtest.models import BacktestResult
            
            if len(calculators) == 1:
                # 单个因子回测
                calculator = calculators[0]
                logger.info(f"运行单个因子回测: {calculator.name}")
                
                result_dict = run_single_calculator_backtest(
                    calculator=calculator,
                    start_date=config.start_date,
                    end_date=config.end_date,
                    initial_balance=config.initial_balance,
                    symbols=config.symbols,
                    verbose=False,
                )
                
                # 返回结果字典，包含所有指标
                if result_dict:
                    # 创建BacktestResult对象，包含所有回测指标
                    from ..backtest.models import BacktestConfig
                    result = BacktestResult(
                        config=BacktestConfig(
                            name=config.name,
                            start_date=config.start_date,
                            end_date=config.end_date,
                            initial_balance=config.initial_balance,
                            symbols=config.symbols or [],
                            leverage=config.leverage,
                            interval=config.interval,
                        ),
                        total_return=float(result_dict.get('total_return', 0.0)),
                        annual_return=float(result_dict.get('annual_return', 0.0)),
                        sharpe_ratio=float(result_dict.get('sharpe_ratio', 0.0)),
                        max_drawdown=float(result_dict.get('max_drawdown', 0.0)),
                        win_rate=float(result_dict.get('win_rate', 0.0)),
                        profit_factor=float(result_dict.get('profit_factor', 0.0)),
                        ic_mean=float(result_dict.get('ic_mean', 0.0)),
                        icir=float(result_dict.get('ic_mean', 0.0)),  # 使用ic_mean作为icir的近似
                    )
                    logger.info(f"因子回测完成: {config.name}")
                    
                    # 保存回测关联信息到factor_mining目录
                    try:
                        if len(calculators) == 1:
                            FactorMiningResultSaver.save_backtest_result(calculators[0].name, result)
                            logger.info(f"回测关联信息已保存")
                    except Exception as e:
                        logger.warning(f"保存回测关联信息失败: {e}", exc_info=True)
                    
                    return result
                else:
                    logger.warning(f"因子回测返回空结果: {config.name}")
                    return None
            else:
                # 多因子回测
                logger.info(f"运行多因子回测: {len(calculators)} 个因子")
                
                calculator_names = [calc.name for calc in calculators]
                result_dict = run_alpha_backtest(
                    start_date=config.start_date,
                    end_date=config.end_date,
                    initial_balance=config.initial_balance,
                    calculator_names=calculator_names,
                    symbols=config.symbols,
                    verbose=False,
                )
                
                # 解析回测结果
                if result_dict and 'metrics' in result_dict:
                    metrics_dict = result_dict['metrics']
                    from ..backtest.models import BacktestConfig
                    
                    # 解析字符串格式的指标
                    def parse_percent(s: str) -> float:
                        if isinstance(s, str):
                            return float(s.rstrip('%'))
                        return float(s) if s else 0.0
                    
                    def parse_float(s: str) -> float:
                        if isinstance(s, str):
                            return float(s)
                        return float(s) if s else 0.0
                    
                    result = BacktestResult(
                        config=BacktestConfig(
                            name=config.name,
                            start_date=config.start_date,
                            end_date=config.end_date,
                            initial_balance=config.initial_balance,
                            symbols=config.symbols or [],
                            leverage=config.leverage,
                            interval=config.interval,
                        ),
                        total_return=parse_percent(metrics_dict.get('收益指标', {}).get('总收益率', '0%')),
                        annual_return=parse_percent(metrics_dict.get('收益指标', {}).get('年化收益率', '0%')),
                        sharpe_ratio=parse_float(metrics_dict.get('风险指标', {}).get('夏普比率', '0')),
                        max_drawdown=parse_percent(metrics_dict.get('风险指标', {}).get('最大回撤', '0%')),
                        win_rate=parse_percent(metrics_dict.get('交易统计', {}).get('胜率', '0%')),
                        profit_factor=parse_float(metrics_dict.get('交易统计', {}).get('盈亏比', '0')),
                    )
                    logger.info(f"因子回测完成: {config.name}")
                    return result
                else:
                    logger.warning(f"因子回测返回空结果: {config.name}")
                    return None
            
        except Exception as e:
            logger.error(f"因子回测失败: {e}", exc_info=True)
            return None
    
    def batch_backtest(self, configs: List[FactorBacktestConfig],
                      calculators: List[AlphaCalculatorBase]) -> Dict[str, BacktestResult]:
        """
        批量回测
        
        Args:
            configs: 回测配置列表
            calculators: 因子计算器列表
        
        Returns:
            回测结果字典 {config_name: result}
        """
        results = {}
        
        for config in configs:
            try:
                result = self.run_backtest(config, calculators)
                if result:
                    results[config.name] = result
            except Exception as e:
                logger.warning(f"回测 {config.name} 失败: {e}")
        
        return results
