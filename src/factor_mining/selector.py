"""
因子筛选器

功能：
1. 相关性筛选
2. 稳定性筛选
3. 综合评分筛选
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

from ..common.logger import get_logger
from ..common.config import config
from ..strategy.calculator import AlphaCalculatorBase, AlphaDataView
from .evaluator import FactorMetrics, FactorEvaluator

logger = get_logger('factor_mining.selector')


class FactorSelector:
    """因子筛选器"""
    
    def __init__(self, evaluator: Optional[FactorEvaluator] = None):
        self.evaluator = evaluator or FactorEvaluator()
    
    def calculate_correlation(self, calculator1: AlphaCalculatorBase,
                            calculator2: AlphaCalculatorBase,
                            view: AlphaDataView) -> float:
        """
        计算两个因子的相关性
        
        Args:
            calculator1: 因子1
            calculator2: 因子2
            view: 数据视图
        
        Returns:
            相关系数
        """
        try:
            weights1 = calculator1.run(view)
            weights2 = calculator2.run(view)
            
            # 找到共同symbol
            common_symbols = set(weights1.keys()) & set(weights2.keys())
            if len(common_symbols) < 2:
                return 0.0
            
            vec1 = [weights1[s] for s in common_symbols]
            vec2 = [weights2[s] for s in common_symbols]
            
            corr = np.corrcoef(vec1, vec2)[0, 1]
            return float(corr) if not np.isnan(corr) else 0.0
        except Exception as e:
            logger.warning(f"计算相关性失败: {e}")
            return 0.0
    
    def filter_by_correlation(self, calculators: List[AlphaCalculatorBase],
                             view: AlphaDataView,
                             max_correlation: Optional[float] = None) -> List[AlphaCalculatorBase]:
        """
        根据相关性筛选因子
        
        Args:
            calculators: 因子列表
            view: 数据视图
            max_correlation: 最大允许相关性，如果为None则从配置读取
        
        Returns:
            筛选后的因子列表
        """
        if max_correlation is None:
            max_correlation = float(config.get('strategy.factor_mining.selection.max_correlation', 0.8))
        
        if len(calculators) <= 1:
            return calculators
        
        selected = [calculators[0]]
        
        for calc in calculators[1:]:
            # 检查与已选因子的相关性
            max_corr = 0.0
            for sel_calc in selected:
                corr = abs(self.calculate_correlation(calc, sel_calc, view))
                max_corr = max(max_corr, corr)
            
            if max_corr < max_correlation:
                selected.append(calc)
        
        logger.info(f"相关性筛选: {len(calculators)} -> {len(selected)}")
        return selected
    
    def filter_by_metrics(self, calculators: List[AlphaCalculatorBase],
                         metrics_dict: Dict[str, FactorMetrics],
                         min_ic_ir: Optional[float] = None,
                         min_coverage: Optional[float] = None,
                         min_sharpe_ratio: Optional[float] = None) -> List[AlphaCalculatorBase]:
        """
        根据评估指标筛选因子
        
        Args:
            calculators: 因子列表
            metrics_dict: 因子评估指标字典 {name: metrics}
            min_ic_ir: 最小IC IR，如果为None则从配置读取
            min_coverage: 最小覆盖率，如果为None则从配置读取
            min_sharpe_ratio: 最小Sharpe比率，如果为None则从配置读取
        
        Returns:
            筛选后的因子列表
        """
        # 使用配置默认值
        if min_ic_ir is None:
            min_ic_ir = float(config.get('strategy.factor_mining.evaluation.min_ic_ir', 0.5))
        if min_coverage is None:
            min_coverage = float(config.get('strategy.factor_mining.evaluation.min_coverage', 0.5))
        if min_sharpe_ratio is None:
            min_sharpe_ratio = float(config.get('strategy.factor_mining.evaluation.min_sharpe_ratio', 0.5))
        
        selected = []
        
        for calc in calculators:
            if calc.name not in metrics_dict:
                continue
            
            metrics = metrics_dict[calc.name]
            
            # 检查指标
            if metrics.ic_ir < min_ic_ir:
                continue
            if metrics.coverage < min_coverage:
                continue
            if metrics.sharpe_ratio < min_sharpe_ratio:
                continue
            
            selected.append(calc)
        
        logger.info(f"指标筛选: {len(calculators)} -> {len(selected)}")
        return selected
    
    def rank_factors(self, calculators: List[AlphaCalculatorBase],
                    metrics_dict: Dict[str, FactorMetrics],
                    weights: Optional[Dict[str, float]] = None) -> List[Tuple[AlphaCalculatorBase, float]]:
        """
        对因子进行排名
        
        Args:
            calculators: 因子列表
            metrics_dict: 因子评估指标字典
            weights: 指标权重 {'ic_ir': 0.4, 'sharpe_ratio': 0.3, ...}
        
        Returns:
            排名列表 [(calculator, score), ...]
        """
        if weights is None:
            # 从配置读取权重
            ranking_weights = config.get('strategy.factor_mining.selection.ranking_weights', {})
            weights = {
                'ic_ir': float(ranking_weights.get('ic_ir', 0.3)),
                'ic_positive_ratio': float(ranking_weights.get('ic_positive_ratio', 0.2)),
                'sharpe_ratio': float(ranking_weights.get('sharpe_ratio', 0.3)),
                'total_return': float(ranking_weights.get('total_return', 0.2)),
            }
        
        scores = []
        
        for calc in calculators:
            if calc.name not in metrics_dict:
                continue
            
            metrics = metrics_dict[calc.name]
            
            # 计算综合得分
            score = 0.0
            score += weights.get('ic_ir', 0.0) * metrics.ic_ir
            score += weights.get('ic_positive_ratio', 0.0) * metrics.ic_positive_ratio
            score += weights.get('sharpe_ratio', 0.0) * metrics.sharpe_ratio
            # 归一化总收益（从配置读取阈值）
            max_return_threshold = float(config.get('strategy.factor_mining.evaluation.max_return_threshold', 10.0))
            normalized_return = min(metrics.total_return, max_return_threshold) / max_return_threshold
            score += weights.get('total_return', 0.0) * normalized_return
            
            scores.append((calc, score))
        
        # 按得分排序
        scores.sort(key=lambda x: x[1], reverse=True)
        
        return scores
    
    def select_best_factors(self, calculators: List[AlphaCalculatorBase],
                           metrics_dict: Dict[str, FactorMetrics],
                           top_n: Optional[int] = None,
                           max_correlation: Optional[float] = None,
                           view: Optional[AlphaDataView] = None) -> List[AlphaCalculatorBase]:
        """
        选择最佳因子组合
        
        Args:
            calculators: 因子列表
            metrics_dict: 因子评估指标字典
            top_n: 选择前N个，如果为None则从配置读取
            max_correlation: 最大允许相关性，如果为None则从配置读取
            view: 数据视图（用于相关性计算）
        
        Returns:
            筛选后的因子列表
        """
        # 使用配置默认值
        if top_n is None:
            top_n = int(config.get('strategy.factor_mining.selection.top_n', 10))
        if max_correlation is None:
            max_correlation = float(config.get('strategy.factor_mining.selection.max_correlation', 0.8))
        
        # 先按指标筛选
        filtered = self.filter_by_metrics(calculators, metrics_dict)
        
        # 排名
        ranked = self.rank_factors(filtered, metrics_dict)
        
        # 选择前N个，并考虑相关性
        selected = []
        if view is None:
            # 如果没有view，直接返回前N个
            selected = [calc for calc, _ in ranked[:top_n]]
        else:
            # 考虑相关性
            for calc, score in ranked:
                if len(selected) >= top_n:
                    break
                
                # 检查与已选因子的相关性
                max_corr = 0.0
                for sel_calc in selected:
                    corr = abs(self.calculate_correlation(calc, sel_calc, view))
                    max_corr = max(max_corr, corr)
                
                if max_corr < max_correlation:
                    selected.append(calc)
        
        logger.info(f"选择最佳因子: {len(calculators)} -> {len(selected)}")
        return selected
