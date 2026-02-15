"""
因子挖掘结果保存器

自动将因子挖掘结果保存到data目录
"""

from __future__ import annotations

from typing import Optional, Dict, Any, TYPE_CHECKING
from pathlib import Path
import json
from datetime import datetime

from ..common.logger import get_logger
from ..common.config import config

if TYPE_CHECKING:
    from .evaluator import FactorMetrics

logger = get_logger('factor_mining.result_saver')


class FactorMiningResultSaver:
    """因子挖掘结果保存器"""
    
    @staticmethod
    def get_output_dir() -> Path:
        """从配置获取输出目录"""
        # 优先从strategy.factor_mining.generation.output_dir读取（配置文件中的实际路径）
        # 如果不存在，则尝试factor_mining.generation.output_dir（根目录下的路径）
        output_dir = config.get('strategy.factor_mining.generation.output_dir', None)
        if output_dir is None:
            output_dir = config.get('factor_mining.generation.output_dir', 'data/factor_mining')
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        return output_path
    
    @staticmethod
    def save_evaluation_result(calculator_name: str,
                               metrics: 'FactorMetrics',
                               output_dir: Optional[Path] = None) -> Path:
        """
        保存因子评估结果
        
        Args:
            calculator_name: 因子名称
            metrics: 评估指标
            output_dir: 输出目录，如果为None则使用配置的默认目录
        
        Returns:
            输出目录路径
        """
        if output_dir is None:
            output_dir = FactorMiningResultSaver.get_output_dir() / "evaluations"
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建以因子名称命名的子目录
        factor_dir = output_dir / calculator_name
        factor_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"保存因子评估结果到: {factor_dir}")
        
        # 保存JSON格式
        result_dict = {
            'calculator_name': calculator_name,
            'timestamp': datetime.now().isoformat(),
            'metrics': {
                'ic_mean': metrics.ic_mean,
                'ic_std': metrics.ic_std,
                'ic_ir': metrics.ic_ir,
                'ic_positive_ratio': metrics.ic_positive_ratio,
                'coverage': metrics.coverage,
                'sharpe_ratio': metrics.sharpe_ratio,
                'total_return': metrics.total_return,
                'max_drawdown': metrics.max_drawdown,
                'win_rate': metrics.win_rate,
            }
        }
        
        json_path = factor_dir / "evaluation_result.json"
        json_path.write_text(json.dumps(result_dict, indent=2, ensure_ascii=False), encoding='utf-8')
        logger.info(f"评估结果已保存: {json_path}")
        
        return factor_dir
    
    @staticmethod
    def save_search_results(search_results: list,
                           output_dir: Optional[Path] = None) -> Path:
        """
        保存参数搜索结果
        
        Args:
            search_results: 搜索结果列表
            output_dir: 输出目录，如果为None则使用配置的默认目录
        
        Returns:
            输出文件路径
        """
        if output_dir is None:
            output_dir = FactorMiningResultSaver.get_output_dir() / "search"
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存搜索结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = output_dir / f"search_results_{timestamp}.json"
        
        results_dict = {
            'timestamp': datetime.now().isoformat(),
            'total_results': len(search_results),
            'results': search_results
        }
        
        results_file.write_text(json.dumps(results_dict, indent=2, ensure_ascii=False), encoding='utf-8')
        logger.info(f"搜索结果已保存: {results_file}")
        
        return results_file
    
    @staticmethod
    def save_backtest_result(calculator_name: str,
                            backtest_result: Any,
                            output_dir: Optional[Path] = None) -> Path:
        """
        保存因子回测结果（回测结果会通过BacktestResultSaver自动保存，这里只是记录）
        
        Args:
            calculator_name: 因子名称
            backtest_result: 回测结果
            output_dir: 输出目录，如果为None则使用配置的默认目录
        
        Returns:
            输出目录路径
        """
        # 回测结果已经通过BacktestResultSaver自动保存到data/backtest_results
        # 这里只需要记录因子名称和回测结果的关联
        if output_dir is None:
            output_dir = FactorMiningResultSaver.get_output_dir() / "backtests"
        else:
            output_dir = Path(output_dir)
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建关联文件
        link_file = output_dir / f"{calculator_name}_backtest_link.json"
        link_dict = {
            'calculator_name': calculator_name,
            'timestamp': datetime.now().isoformat(),
            'backtest_result_dir': f"data/backtest_results/bt_{calculator_name}",
        }
        
        link_file.write_text(json.dumps(link_dict, indent=2, ensure_ascii=False), encoding='utf-8')
        logger.info(f"回测关联已保存: {link_file}")
        
        return output_dir
