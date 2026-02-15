"""
因子挖掘CLI工具

功能：
1. 因子生成
2. 因子评估
3. 因子筛选
4. 因子部署
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional

from ..common.logger import get_logger
from ..common.config import config
from ..strategy.calculator import AlphaCalculatorBase
from .generator import FactorGenerator
from .evaluator import FactorEvaluator
from .selector import FactorSelector
from .backtest_integration import FactorBacktestRunner
from .live_integration import FactorLiveIntegrator
from .result_saver import FactorMiningResultSaver
from .utils import (
    get_default_interval,
    get_default_forward_periods,
    get_default_history_days,
    get_default_initial_balance,
    get_default_lookback_bars,
)

logger = get_logger('factor_mining.cli')


class FactorMiningCLI:
    """因子挖掘CLI"""
    
    def __init__(self):
        self.generator = FactorGenerator()
        self.evaluator = FactorEvaluator()
        self.selector = FactorSelector(self.evaluator)
        self.backtest_runner = FactorBacktestRunner()
        self.integrator = FactorLiveIntegrator()
    
    def generate(self, template_name: str, output_dir: Path, **kwargs):
        """生成因子"""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        code = self.generator.generate_factor(template_name, **kwargs)
        filepath = self.generator.generate_calculator_file(
            template_name, output_dir, **kwargs
        )
        
        print(f"生成因子文件: {filepath}")
        return filepath
    
    def search(self, template_name: str, output_dir: Path, 
              start_date: datetime, end_date: datetime,
              top_n: Optional[int] = None,
              interval: Optional[str] = None,
              initial_balance: Optional[float] = None):
        """参数搜索"""
        # 使用配置默认值
        if top_n is None:
            top_n = int(config.get('strategy.factor_mining.selection.top_n', 10))
        if interval is None:
            interval = get_default_interval()
        if initial_balance is None:
            initial_balance = get_default_initial_balance()
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 获取参数组合
        param_combinations = self.generator.search_parameters(template_name)
        
        logger.info(f"找到 {len(param_combinations)} 个参数组合")
        print(f"找到 {len(param_combinations)} 个参数组合")
        
        # 评估每个组合
        results = []
        temp_calculators_dir = output_dir / "temp_calculators"
        temp_calculators_dir.mkdir(parents=True, exist_ok=True)
        
        for i, params in enumerate(param_combinations):
            logger.info(f"评估组合 {i+1}/{len(param_combinations)}: {params}")
            print(f"评估组合 {i+1}/{len(param_combinations)}: {params}")
            
            try:
                # 生成因子代码
                code = self.generator.generate_factor(template_name, **params)
                
                # 生成临时calculator文件
                filepath = self.generator.generate_calculator_file(
                    template_name, temp_calculators_dir, **params
                )
                
                # 动态加载calculator
                calculator = self._load_calculator_from_file(filepath)
                if calculator is None:
                    logger.warning(f"无法加载calculator: {filepath}")
                    continue
                
                # 评估因子
                metrics = self.evaluator.evaluate_factor(
                    calculator,
                    start_date,
                    end_date,
                    use_backtest=True,
                    interval=interval,
                    initial_balance=initial_balance
                )
                
                # 记录结果
                result = {
                    'params': params,
                    'filepath': str(filepath),
                    'metrics': {
                        'ic_mean': metrics.ic_mean,
                        'ic_ir': metrics.ic_ir,
                        'ic_positive_ratio': metrics.ic_positive_ratio,
                        'total_return': metrics.total_return,
                        'sharpe_ratio': metrics.sharpe_ratio,
                        'max_drawdown': metrics.max_drawdown,
                        'coverage': metrics.coverage,
                    },
                    'score': self._calculate_factor_score(metrics),
                }
                results.append(result)
                
                logger.info(f"组合 {i+1} 评估完成: IC IR={metrics.ic_ir:.4f}, Sharpe={metrics.sharpe_ratio:.4f}")
                
            except Exception as e:
                logger.error(f"评估组合失败: {e}", exc_info=True)
                print(f"评估失败: {e}")
        
        # 按得分排序
        results.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        # 选择前N个
        top_results = results[:top_n]
        
        # 保存结果（使用FactorMiningResultSaver保存搜索结果）
        # 准备搜索结果数据
        results_list = [{
            'params': r.get('params', {}),
            'metrics': r.get('metrics', {}),
            'score': r.get('score', 0),
        } for r in top_results]
        
        results_file = FactorMiningResultSaver.save_search_results(results_list, output_dir)
        
        # 保存完整数据到同一文件（覆盖之前保存的简化版本）
        results_data = {
            'total_combinations': len(param_combinations),
            'evaluated': len(results),
            'top_n': top_n,
            'results': top_results,
        }
        results_file.write_text(json.dumps(results_data, indent=2, default=str, ensure_ascii=False), encoding='utf-8')
        
        logger.info(f"搜索结果保存到: {results_file}")
        print(f"搜索结果保存到: {results_file}")
        print(f"评估了 {len(results)} 个组合，选择了前 {len(top_results)} 个")
        
        return top_results
    
    def _load_calculator_from_file(self, filepath: Path) -> Optional[AlphaCalculatorBase]:
        """从文件动态加载calculator"""
        try:
            import importlib.util
            import sys
            
            # 生成模块名
            module_name = f"temp_calculator_{filepath.stem}"
            
            # 加载模块
            spec = importlib.util.spec_from_file_location(module_name, filepath)
            if spec is None or spec.loader is None:
                return None
            
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            
            # 获取CALCULATOR_INSTANCE
            if hasattr(module, 'CALCULATOR_INSTANCE'):
                return module.CALCULATOR_INSTANCE
            
            # 或者查找AlphaCalculatorBase的子类
            from ..strategy.calculator import AlphaCalculatorBase
            for name, obj in module.__dict__.items():
                if (isinstance(obj, type) and 
                    issubclass(obj, AlphaCalculatorBase) and 
                    obj is not AlphaCalculatorBase):
                    return obj()
            
            return None
        except Exception as e:
            logger.error(f"加载calculator失败: {e}", exc_info=True)
            return None
    
    def _calculate_factor_score(self, metrics) -> float:
        """计算因子综合得分"""
        # 从配置读取权重
        ranking_weights = config.get('strategy.factor_mining.selection.ranking_weights', {})
        weights = {
            'ic_ir': float(ranking_weights.get('ic_ir', 0.3)),
            'ic_positive_ratio': float(ranking_weights.get('ic_positive_ratio', 0.2)),
            'sharpe_ratio': float(ranking_weights.get('sharpe_ratio', 0.3)),
            'total_return': float(ranking_weights.get('total_return', 0.2)),
        }
        
        score = 0.0
        score += weights['ic_ir'] * metrics.ic_ir
        score += weights['ic_positive_ratio'] * metrics.ic_positive_ratio
        score += weights['sharpe_ratio'] * metrics.sharpe_ratio
        
        # 归一化总收益（从配置读取阈值）
        max_return_threshold = float(config.get('strategy.factor_mining.evaluation.max_return_threshold', 10.0))
        normalized_return = min(metrics.total_return, max_return_threshold) / max_return_threshold
        score += weights['total_return'] * normalized_return
        
        return score
    
    def evaluate(self, calculator_path: Path, start_date: datetime, end_date: datetime,
                 interval: Optional[str] = None,
                 initial_balance: Optional[float] = None):
        """评估因子"""
        logger.info(f"评估因子: {calculator_path}")
        print(f"评估因子: {calculator_path}")
        print(f"时间范围: {start_date} - {end_date}")
        
        # 加载calculator
        calculator = self._load_calculator_from_file(calculator_path)
        if calculator is None:
            logger.error(f"无法加载calculator: {calculator_path}")
            print(f"错误: 无法加载calculator: {calculator_path}")
            return None
        
        print(f"加载calculator: {calculator.name}")
        
        # 使用配置默认值
        if interval is None:
            interval = get_default_interval()
        if initial_balance is None:
            initial_balance = get_default_initial_balance()
        
        # 评估因子
        logger.info(f"开始评估因子: {calculator.name}")
        metrics = self.evaluator.evaluate_factor(
            calculator,
            start_date,
            end_date,
            use_backtest=True,
            interval=interval,
            initial_balance=initial_balance
        )
        
        # 打印结果
        print("\n评估结果:")
        print(f"  IC均值: {metrics.ic_mean:.4f}")
        print(f"  IC标准差: {metrics.ic_std:.4f}")
        print(f"  IC IR: {metrics.ic_ir:.4f}")
        print(f"  IC正比例: {metrics.ic_positive_ratio:.4f}")
        print(f"  总收益率: {metrics.total_return:.2f}%")
        print(f"  年化收益率: {metrics.annual_return:.2f}%")
        print(f"  Sharpe比率: {metrics.sharpe_ratio:.4f}")
        print(f"  最大回撤: {metrics.max_drawdown:.2f}%")
        print(f"  胜率: {metrics.win_rate:.2f}%")
        print(f"  盈亏比: {metrics.profit_factor:.4f}")
        print(f"  覆盖率: {metrics.coverage:.4f}")
        
        logger.info(f"因子评估完成: {calculator.name}")
        print("评估完成")
        
        return metrics
    
    def deploy(self, calculator_paths: List[Path], enable: bool = True):
        """部署因子"""
        logger.info(f"部署 {len(calculator_paths)} 个因子...")
        print(f"部署 {len(calculator_paths)} 个因子...")
        
        calculators = []
        factor_codes = []
        calculator_names = []
        
        # 加载所有calculator
        for calc_path in calculator_paths:
            try:
                # 读取代码
                code = calc_path.read_text(encoding='utf-8')
                factor_codes.append(code)
                
                # 加载calculator实例
                calculator = self._load_calculator_from_file(calc_path)
                if calculator is None:
                    logger.warning(f"无法加载calculator: {calc_path}")
                    print(f"警告: 无法加载calculator: {calc_path}")
                    continue
                
                calculators.append(calculator)
                calculator_names.append(calculator.name)
                print(f"  加载: {calculator.name}")
                
            except Exception as e:
                logger.error(f"加载calculator失败 {calc_path}: {e}", exc_info=True)
                print(f"错误: 加载calculator失败 {calc_path}: {e}")
        
        if not calculators:
            logger.error("没有成功加载任何calculator")
            print("错误: 没有成功加载任何calculator")
            return []
        
        # 部署到实盘系统
        try:
            filepaths = self.integrator.deploy_factors(
                calculators=calculators,
                factor_codes=factor_codes,
                enable_in_config=enable
            )
            
            logger.info(f"成功部署 {len(filepaths)} 个因子")
            print(f"成功部署 {len(filepaths)} 个因子:")
            for filepath in filepaths:
                print(f"  {filepath}")
            
            if enable:
                print(f"已在配置文件中启用: {calculator_names}")
            
            print("部署完成")
            return filepaths
            
        except Exception as e:
            logger.error(f"部署因子失败: {e}", exc_info=True)
            print(f"错误: 部署因子失败: {e}")
            return []
    
    def run_cli(self):
        """运行CLI"""
        parser = argparse.ArgumentParser(description='因子挖掘系统')
        subparsers = parser.add_subparsers(dest='command', help='命令')
        
        # generate命令
        gen_parser = subparsers.add_parser('generate', help='生成因子')
        gen_parser.add_argument('template', help='模板名称')
        gen_parser.add_argument('--output', type=Path, required=True, help='输出目录')
        gen_parser.add_argument('--lookback-bars', type=int, default=None, 
                               help=f'回看K线数（默认: {get_default_lookback_bars()}）')
        
        # search命令
        search_parser = subparsers.add_parser('search', help='参数搜索')
        search_parser.add_argument('template', help='模板名称')
        search_parser.add_argument('--output', type=Path, required=True, help='输出目录')
        search_parser.add_argument('--start-date', type=str, required=True, help='开始日期')
        search_parser.add_argument('--end-date', type=str, required=True, help='结束日期')
        search_parser.add_argument('--top-n', type=int, default=None, 
                                  help=f'选择前N个（默认: {int(config.get("strategy.factor_mining.selection.top_n", 10))}）')
        search_parser.add_argument('--interval', type=str, default=None, 
                                  help=f'时间间隔（默认: {get_default_interval()}）')
        search_parser.add_argument('--initial-balance', type=float, default=None,
                                  help=f'初始余额（默认: {get_default_initial_balance()}）')
        
        # evaluate命令
        eval_parser = subparsers.add_parser('evaluate', help='评估因子')
        eval_parser.add_argument('calculator', type=Path, help='计算器文件路径')
        eval_parser.add_argument('--start-date', type=str, required=True, help='开始日期')
        eval_parser.add_argument('--end-date', type=str, required=True, help='结束日期')
        eval_parser.add_argument('--interval', type=str, default=None,
                                 help=f'时间间隔（默认: {get_default_interval()}）')
        eval_parser.add_argument('--initial-balance', type=float, default=None,
                                help=f'初始余额（默认: {get_default_initial_balance()}）')
        
        # deploy命令
        deploy_parser = subparsers.add_parser('deploy', help='部署因子')
        deploy_parser.add_argument('calculators', nargs='+', type=Path, help='计算器文件路径')
        deploy_parser.add_argument('--enable', action='store_true', help='在配置中启用')
        
        args = parser.parse_args()
        
        if args.command == 'generate':
            lookback_bars = args.lookback_bars if args.lookback_bars is not None else get_default_lookback_bars()
            self.generate(args.template, args.output, lookback_bars=lookback_bars)
        
        elif args.command == 'search':
            start_date = datetime.fromisoformat(args.start_date)
            end_date = datetime.fromisoformat(args.end_date)
            self.search(args.template, args.output, start_date, end_date, 
                       top_n=args.top_n, interval=args.interval, initial_balance=args.initial_balance)
        
        elif args.command == 'evaluate':
            start_date = datetime.fromisoformat(args.start_date)
            end_date = datetime.fromisoformat(args.end_date)
            self.evaluate(args.calculator, start_date, end_date,
                         interval=args.interval, initial_balance=args.initial_balance)
        
        elif args.command == 'deploy':
            self.deploy(args.calculators, args.enable)
        
        else:
            parser.print_help()


if __name__ == '__main__':
    cli = FactorMiningCLI()
    cli.run_cli()
