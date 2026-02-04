"""
Strategy API性能测试脚本
独立运行，不集成到系统中
测试strategy API的调用性能，统计耗时数据并分析结果落盘
"""
import asyncio
import time
import json
import statistics
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import pandas as pd

from src.api.strategy_api import StrategyAPI
from src.strategy.alpha import get_alpha_engine
from src.common.config import config
from src.common.logger import get_logger
from src.data.universe_manager import get_universe_manager

logger = get_logger('api_performance_test')


class APIPerformanceTester:
    """API性能测试器"""
    
    def __init__(self, output_dir: str = "data/api_performance"):
        """
        初始化测试器
        
        Args:
            output_dir: 输出目录
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.strategy_api = StrategyAPI()
        self.alpha_engine = get_alpha_engine()
        self.universe_manager = get_universe_manager()
        
        # 测试结果
        self.test_results: List[Dict] = []
    
    def check_data_availability(self, min_symbols: int = 5, min_days: int = 7) -> bool:
        """
        检查是否有足够的数据进行测试
        
        Args:
            min_symbols: 最小交易对数量
            min_days: 最小历史数据天数
        
        Returns:
            是否有足够数据
        """
        try:
            # 获取universe
            universe = self.universe_manager.load_universe()
            if not universe or len(universe) < min_symbols:
                logger.warning(
                    f"Insufficient symbols: {len(universe) if universe else 0} < {min_symbols}"
                )
                return False
            
            # 检查数据可用性（通过API获取数据）
            logger.info(f"Checking data availability for {len(universe)} symbols...")
            test_symbols = list(universe)[:min_symbols]  # 只检查前几个
            
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=min_days)
            
            from src.data.api import get_data_api
            data_api = get_data_api()
            
            # 检查是否有K线数据
            klines = data_api.get_klines(
                symbols=test_symbols,
                days=min_days
            )
            
            symbols_with_data = sum(1 for df in klines.values() if not df.empty)
            
            if symbols_with_data < min_symbols:
                logger.warning(
                    f"Insufficient data: {symbols_with_data}/{len(test_symbols)} symbols have data"
                )
                return False
            
            logger.info(f"Data check passed: {symbols_with_data}/{len(test_symbols)} symbols have data")
            return True
            
        except Exception as e:
            logger.error(f"Failed to check data availability: {e}", exc_info=True)
            return False
    
    def test_strategy_api_call(
        self,
        symbols: Optional[List[str]] = None,
        history_days: int = 30,
        mode: str = "5min",
        iterations: int = 10
    ) -> Dict:
        """
        测试Strategy API调用性能
        
        Args:
            symbols: 交易对列表，如果为None则使用universe
            history_days: 历史数据天数
            mode: 数据模式（5min, 1h等）
            iterations: 测试迭代次数
        
        Returns:
            测试结果字典
        """
        if symbols is None:
            universe = self.universe_manager.load_universe()
            if not universe:
                raise ValueError("No symbols available for testing")
            symbols = list(universe)
        
        logger.info(
            f"Starting API performance test: {len(symbols)} symbols, "
            f"{history_days} days, mode={mode}, {iterations} iterations"
        )
        
        timings = []
        errors = []
        
        for i in range(iterations):
            try:
                start_time = time.perf_counter()
                
                # 调用Alpha引擎（模拟Strategy API调用）
                result = self.alpha_engine.run(
                    symbols=symbols,
                    history_days=history_days,
                    mode=mode
                )
                
                elapsed = time.perf_counter() - start_time
                timings.append(elapsed)
                
                logger.debug(f"Iteration {i+1}/{iterations}: {elapsed:.3f}s")
                
            except Exception as e:
                elapsed = time.perf_counter() - start_time
                errors.append({
                    'iteration': i + 1,
                    'error': str(e),
                    'elapsed': elapsed
                })
                logger.error(f"Iteration {i+1}/{iterations} failed: {e}")
        
        # 统计结果
        if not timings:
            raise ValueError("All iterations failed, no timing data available")
        
        stats = {
            'test_config': {
                'symbols_count': len(symbols),
                'history_days': history_days,
                'mode': mode,
                'iterations': iterations,
            },
            'timings': timings,
            'statistics': {
                'count': len(timings),
                'total': sum(timings),
                'mean': statistics.mean(timings),
                'median': statistics.median(timings),
                'stdev': statistics.stdev(timings) if len(timings) > 1 else 0.0,
                'min': min(timings),
                'max': max(timings),
                'p95': self._percentile(timings, 0.95),
                'p99': self._percentile(timings, 0.99),
            },
            'errors': errors,
            'error_rate': len(errors) / iterations if iterations > 0 else 0.0,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        
        return stats
    
    def _percentile(self, data: List[float], p: float) -> float:
        """计算百分位数"""
        sorted_data = sorted(data)
        index = int(len(sorted_data) * p)
        if index >= len(sorted_data):
            index = len(sorted_data) - 1
        return sorted_data[index]
    
    def run_test_suite(
        self,
        symbols: Optional[List[str]] = None,
        test_configs: Optional[List[Dict]] = None
    ) -> Dict:
        """
        运行完整的测试套件
        
        Args:
            symbols: 交易对列表
            test_configs: 测试配置列表，每个配置包含history_days, mode, iterations
        
        Returns:
            测试结果汇总
        """
        if test_configs is None:
            # 默认测试配置
            test_configs = [
                {'history_days': 7, 'mode': '5min', 'iterations': 10},
                {'history_days': 30, 'mode': '5min', 'iterations': 10},
                {'history_days': 30, 'mode': '1h', 'iterations': 10},
                {'history_days': 90, 'mode': '5min', 'iterations': 5},
            ]
        
        if symbols is None:
            universe = self.universe_manager.load_universe()
            if not universe:
                raise ValueError("No symbols available for testing")
            symbols = list(universe)
        
        logger.info(f"Running test suite with {len(symbols)} symbols, {len(test_configs)} test configurations")
        
        all_results = []
        
        for i, test_config in enumerate(test_configs):
            logger.info(f"Test {i+1}/{len(test_configs)}: {test_config}")
            
            try:
                result = self.test_strategy_api_call(
                    symbols=symbols,
                    **test_config
                )
                all_results.append(result)
                self.test_results.append(result)
                
            except Exception as e:
                logger.error(f"Test {i+1} failed: {e}", exc_info=True)
                all_results.append({
                    'test_config': test_config,
                    'error': str(e),
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                })
        
        # 生成汇总报告
        summary = self._generate_summary(all_results)
        
        # 保存结果
        self._save_results(all_results, summary)
        
        return summary
    
    def _generate_summary(self, results: List[Dict]) -> Dict:
        """生成汇总报告"""
        summary = {
            'total_tests': len(results),
            'successful_tests': sum(1 for r in results if 'statistics' in r),
            'failed_tests': sum(1 for r in results if 'error' in r),
            'test_results': [],
            'overall_statistics': {},
            'generated_at': datetime.now(timezone.utc).isoformat(),
        }
        
        # 汇总所有成功的测试统计
        all_timings = []
        for result in results:
            if 'statistics' in result:
                stats = result['statistics']
                all_timings.extend(result['timings'])
                summary['test_results'].append({
                    'config': result['test_config'],
                    'mean': stats['mean'],
                    'median': stats['median'],
                    'min': stats['min'],
                    'max': stats['max'],
                    'p95': stats['p95'],
                })
        
        if all_timings:
            summary['overall_statistics'] = {
                'total_calls': len(all_timings),
                'mean': statistics.mean(all_timings),
                'median': statistics.median(all_timings),
                'stdev': statistics.stdev(all_timings) if len(all_timings) > 1 else 0.0,
                'min': min(all_timings),
                'max': max(all_timings),
                'p95': self._percentile(all_timings, 0.95),
                'p99': self._percentile(all_timings, 0.99),
            }
        
        return summary
    
    def _save_results(self, results: List[Dict], summary: Dict):
        """保存测试结果"""
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        
        # 保存详细结果
        detail_file = self.output_dir / f"api_performance_detail_{timestamp}.json"
        with open(detail_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"Detailed results saved to {detail_file}")
        
        # 保存汇总报告
        summary_file = self.output_dir / f"api_performance_summary_{timestamp}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logger.info(f"Summary report saved to {summary_file}")
        
        # 保存CSV格式的统计表
        if summary['test_results']:
            df = pd.DataFrame(summary['test_results'])
            csv_file = self.output_dir / f"api_performance_stats_{timestamp}.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            logger.info(f"Statistics table saved to {csv_file}")
        
        # 打印汇总
        print("\n" + "="*80)
        print("API Performance Test Summary")
        print("="*80)
        print(f"Total tests: {summary['total_tests']}")
        print(f"Successful: {summary['successful_tests']}")
        print(f"Failed: {summary['failed_tests']}")
        
        if summary['overall_statistics']:
            stats = summary['overall_statistics']
            print(f"\nOverall Statistics:")
            print(f"  Total calls: {stats['total_calls']}")
            print(f"  Mean: {stats['mean']:.3f}s")
            print(f"  Median: {stats['median']:.3f}s")
            print(f"  Min: {stats['min']:.3f}s")
            print(f"  Max: {stats['max']:.3f}s")
            print(f"  P95: {stats['p95']:.3f}s")
            print(f"  P99: {stats['p99']:.3f}s")
        
        print("\nPer-test Results:")
        for i, test_result in enumerate(summary['test_results'], 1):
            config = test_result['config']
            print(f"  Test {i}: {config['history_days']}d, {config['mode']}, {config['iterations']} iterations")
            print(f"    Mean: {test_result['mean']:.3f}s, Median: {test_result['median']:.3f}s")
            print(f"    Min: {test_result['min']:.3f}s, Max: {test_result['max']:.3f}s, P95: {test_result['p95']:.3f}s")
        
        print("="*80 + "\n")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Strategy API性能测试脚本',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  # 使用全部可用数据，执行10轮测试
  python test_api_performance.py --use-all-data --iterations 10
  
  # 测试7天、30天、90天的数据，各10轮
  python test_api_performance.py --days 7,30,90 --iterations 10
  
  # 测试最近30天的数据，执行20轮
  python test_api_performance.py --days 30 --iterations 20
  
  # 测试全部数据，同时测试5min和1h两种模式，各执行15轮
  python test_api_performance.py --use-all-data --iterations 15 --modes 5min,1h
        '''
    )
    
    parser.add_argument(
        '--days',
        type=str,
        default='7,30,90',
        help='测试的历史天数，多个值用逗号分隔，例如: 7,30,90 (默认: 7,30,90)'
    )
    
    parser.add_argument(
        '--use-all-data',
        action='store_true',
        help='使用所有可用的历史数据，忽略--days参数'
    )
    
    parser.add_argument(
        '--iterations',
        type=int,
        default=10,
        help='每个测试配置的迭代轮次 (默认: 10)'
    )
    
    parser.add_argument(
        '--modes',
        type=str,
        default='5min,1h',
        help='数据模式，多个值用逗号分隔，例如: 5min,1h (默认: 5min,1h)'
    )
    
    parser.add_argument(
        '--min-symbols',
        type=int,
        default=5,
        help='数据检查时的最小交易对数量 (默认: 5)'
    )
    
    args = parser.parse_args()
    
    tester = APIPerformanceTester()
    
    # 检查数据可用性
    if not tester.check_data_availability(min_symbols=args.min_symbols, min_days=7):
        logger.error("Insufficient data for testing. Please ensure data is available.")
        return
    
    # 生成测试配置
    if args.use_all_data:
        # 获取可用的最长历史数据
        logger.info("Using all available data for testing")
        test_days = [365]  # 假设最多有1年的数据
        test_configs = []
        modes = [m.strip() for m in args.modes.split(',')]
        
        for days in test_days:
            for mode in modes:
                test_configs.append({
                    'history_days': days,
                    'mode': mode,
                    'iterations': args.iterations,
                })
    else:
        # 使用指定的天数
        test_days = [int(d.strip()) for d in args.days.split(',')]
        modes = [m.strip() for m in args.modes.split(',')]
        test_configs = []
        
        for days in test_days:
            for mode in modes:
                test_configs.append({
                    'history_days': days,
                    'mode': mode,
                    'iterations': args.iterations,
                })
    
    logger.info(f"Generated {len(test_configs)} test configurations")
    logger.info(f"Iterations per test: {args.iterations}")
    logger.info(f"Test configurations: {test_configs}")
    
    # 运行测试套件
    try:
        summary = tester.run_test_suite(test_configs=test_configs)
        logger.info("API performance test completed successfully")
    except Exception as e:
        logger.error(f"API performance test failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
