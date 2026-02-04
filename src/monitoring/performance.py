"""
性能监控模块
收集系统各模块的运行耗时统计数据
"""
import time
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from collections import defaultdict
from contextlib import contextmanager
import threading
import concurrent.futures
import asyncio

from ..common.config import config
from ..common.logger import get_logger

logger = get_logger('performance_monitor')


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self, enabled: Optional[bool] = None):
        """
        初始化性能监控器
        
        Args:
            enabled: 是否启用性能监控，如果为None则从配置读取
        """
        if enabled is None:
            enabled = config.get('monitoring.performance.enabled', False)
        
        self.enabled = enabled
        
        # 存储每轮的性能数据
        # 格式: {round_id: {module_name: [timing_data, ...]}}
        self.round_data: Dict[str, Dict[str, List[Dict]]] = defaultdict(lambda: defaultdict(list))
        
        # 当前轮次ID
        self.current_round_id: Optional[str] = None
        
        # 线程锁
        self.lock = threading.Lock()
        
        # 输出目录
        self.output_dir = Path(config.get('monitoring.performance.output_directory', 'data/performance'))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 统计信息
        self.stats = {
            'total_rounds': 0,
            'total_measurements': 0,
            'start_time': time.time(),
        }

        # 内存与清理配置
        self.max_rounds_in_memory = config.get('monitoring.performance.max_rounds_in_memory', 100)
        self.round_age_limit_seconds = config.get('monitoring.performance.round_age_limit_seconds', 3600)
        self.auto_cleanup_interval = config.get('monitoring.performance.auto_cleanup_interval', 300)

        # 轮次时间戳（用于清理过期轮次）
        self._round_timestamps: Dict[str, float] = {}

        # 异步保存执行器（线程池），避免文件IO阻塞主线程
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        self._futures: List[concurrent.futures.Future] = []

        # 自动清理线程
        self._cleanup_thread = threading.Thread(target=self._auto_cleanup_loop, daemon=True)
        self._cleanup_thread.start()
        
        if self.enabled:
            logger.info("Performance monitoring enabled")
        else:
            logger.debug("Performance monitoring disabled")
    
    def start_round(self, round_id: Optional[str] = None):
        """
        开始新的一轮性能监控
        
        Args:
            round_id: 轮次ID，如果不指定则自动生成
        """
        if not self.enabled:
            return
        
        if round_id is None:
            round_id = f"round_{int(time.time() * 1000)}"
        
        with self.lock:
            self.current_round_id = round_id
            self.stats['total_rounds'] += 1
            # 记录轮次开始时间
            self._round_timestamps[round_id] = time.time()
            logger.debug(f"Performance monitoring round started: {round_id}")
    
    def end_round(self, save: bool = True):
        """
        结束当前轮次并保存数据
        
        Args:
            save: 是否立即保存数据
        """
        if not self.enabled or self.current_round_id is None:
            return
        
        round_id = self.current_round_id
        
        with self.lock:
            if save and round_id in self.round_data:
                # 异步保存，非阻塞
                try:
                    self._save_round_data(round_id)
                except Exception:
                    # 以防万一，仍要尝试同步保存作为兜底
                    self._save_round_data_sync(round_id)
            
            self.current_round_id = None
            logger.debug(f"Performance monitoring round ended: {round_id}")
    
    @contextmanager
    def measure(self, module_name: str, operation: str = "operation", metadata: Optional[Dict] = None):
        """
        测量代码块的执行时间（上下文管理器）
        
        Args:
            module_name: 模块名称，如 'data_layer', 'strategy_process', 'alpha_engine'
            operation: 操作名称，如 'kline_aggregation', 'strategy_calculation'
            metadata: 额外的元数据
        
        Usage:
            with performance_monitor.measure('data_layer', 'kline_aggregation'):
                # 执行代码
                pass
        """
        if not self.enabled:
            yield
            return
        
        start_time = time.perf_counter()
        start_time_monotonic = time.monotonic()
        
        try:
            yield
        finally:
            end_time = time.perf_counter()
            end_time_monotonic = time.monotonic()
            
            elapsed = end_time - start_time
            elapsed_monotonic = end_time_monotonic - start_time_monotonic
            
            self.record_timing(
                module_name=module_name,
                operation=operation,
                elapsed_seconds=elapsed,
                metadata=metadata
            )
    
    def record_timing(
        self,
        module_name: str,
        operation: str,
        elapsed_seconds: float,
        metadata: Optional[Dict] = None
    ):
        """
        记录耗时数据
        
        Args:
            module_name: 模块名称
            operation: 操作名称
            elapsed_seconds: 耗时（秒）
            metadata: 额外的元数据
        """
        if not self.enabled:
            return
        
        if self.current_round_id is None:
            # 如果没有开始轮次，自动开始一个
            self.start_round()
        
        timing_data = {
            'module': module_name,
            'operation': operation,
            'elapsed_seconds': elapsed_seconds,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'round_id': self.current_round_id,
        }
        
        if metadata:
            timing_data['metadata'] = metadata
        
        with self.lock:
            if self.current_round_id:
                self.round_data[self.current_round_id][module_name].append(timing_data)
                self.stats['total_measurements'] += 1
    
    def get_round_summary(self, round_id: Optional[str] = None) -> Dict:
        """
        获取指定轮次的统计摘要
        
        Args:
            round_id: 轮次ID，如果不指定则使用当前轮次
        
        Returns:
            统计摘要字典
        """
        if not self.enabled:
            return {}
        
        if round_id is None:
            round_id = self.current_round_id
        
        if round_id is None or round_id not in self.round_data:
            return {}
        
        with self.lock:
            round_data = self.round_data[round_id]
        
        summary = {
            'round_id': round_id,
            'modules': {},
            'total_time': 0.0,
            'module_count': len(round_data),
        }
        
        for module_name, timings in round_data.items():
            if not timings:
                continue
            
            total_time = sum(t['elapsed_seconds'] for t in timings)
            count = len(timings)
            avg_time = total_time / count if count > 0 else 0.0
            min_time = min(t['elapsed_seconds'] for t in timings)
            max_time = max(t['elapsed_seconds'] for t in timings)
            
            # 按操作分组统计
            operations = defaultdict(list)
            for t in timings:
                operations[t['operation']].append(t['elapsed_seconds'])
            
            operation_stats = {}
            for op_name, op_times in operations.items():
                operation_stats[op_name] = {
                    'count': len(op_times),
                    'total': sum(op_times),
                    'avg': sum(op_times) / len(op_times),
                    'min': min(op_times),
                    'max': max(op_times),
                }
            
            summary['modules'][module_name] = {
                'total_time': total_time,
                'count': count,
                'avg_time': avg_time,
                'min_time': min_time,
                'max_time': max_time,
                'operations': operation_stats,
            }
            
            summary['total_time'] += total_time
        
        return summary
    
    def _save_round_data(self, round_id: str):
        """异步提交保存指定轮次的数据任务（非阻塞）"""
        # 将实际写盘操作提交给线程池
        future = self._executor.submit(self._save_round_data_sync, round_id)
        self._futures.append(future)
        # 清理已完成的future列表，避免无限增长
        self._futures = [f for f in self._futures if not f.done()]


    def _save_round_data_sync(self, round_id: str):
        """同步保存指定轮次的数据（实际写盘操作）"""
        try:
            if round_id not in self.round_data:
                return

            round_data = self.round_data[round_id]

            # 生成摘要
            summary = self.get_round_summary(round_id)

            # 保存详细数据
            detail_file = self.output_dir / f"performance_detail_{round_id}.json"
            with open(detail_file, 'w', encoding='utf-8') as f:
                json.dump(round_data, f, indent=2, ensure_ascii=False)

            # 保存摘要
            summary_file = self.output_dir / f"performance_summary_{round_id}.json"
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            logger.debug(f"Performance data saved for round {round_id}")

        except Exception as e:
            logger.error(f"Failed to save performance data for round {round_id}: {e}", exc_info=True)
    
    def save_all_rounds(self):
        """保存所有轮次的数据"""
        if not self.enabled:
            return
        with self.lock:
            for round_id in list(self.round_data.keys()):
                self._save_round_data(round_id)

    def wait_for_pending_saves(self, timeout: Optional[float] = None):
        """等待所有异步保存任务完成（可选超时）"""
        start = time.time()
        for f in list(self._futures):
            remaining = None
            if timeout is not None:
                elapsed = time.time() - start
                remaining = max(0, timeout - elapsed)
            try:
                f.result(timeout=remaining)
            except concurrent.futures.TimeoutError:
                logger.warning("Timeout while waiting for pending performance save tasks")
                return False
            except Exception:
                logger.exception("Error while waiting for pending save task")
        return True

    def _auto_cleanup_loop(self):
        """后台循环，定期清理过期或超限的轮次（守护线程）"""
        try:
            while True:
                time.sleep(self.auto_cleanup_interval)
                try:
                    self.cleanup_old_rounds()
                except Exception:
                    logger.exception("Error during automatic performance data cleanup")
        except Exception:
            logger.exception("Performance monitor cleanup thread exiting unexpectedly")

    def cleanup_old_rounds(self):
        """清理过期或超出内存限制的轮次，清理前会异步保存数据"""
        if not self.enabled:
            return

        now = time.time()
        with self.lock:
            rounds = list(self._round_timestamps.items())

        # 根据时间戳和保留数量确定需删除的轮次
        to_remove = set()

        # 1) 过期轮次
        for round_id, ts in rounds:
            if now - ts > self.round_age_limit_seconds:
                to_remove.add(round_id)

        # 2) 如果数量超过限制，删除最旧的直到满足限制
        if len(rounds) - len(to_remove) > self.max_rounds_in_memory:
            # 按时间排序，删除最旧的
            sorted_rounds = sorted(rounds, key=lambda x: x[1])
            keep = self.max_rounds_in_memory
            for i, (rid, ts) in enumerate(sorted_rounds):
                if i < len(sorted_rounds) - keep:
                    to_remove.add(rid)

        if not to_remove:
            return

        # 保存并删除内存数据
        for rid in list(to_remove):
            try:
                # 异步保存
                self._save_round_data(rid)
            except Exception:
                logger.exception(f"Failed to schedule save for round {rid} during cleanup")

            with self.lock:
                if rid in self.round_data:
                    del self.round_data[rid]
                if rid in self._round_timestamps:
                    del self._round_timestamps[rid]

        logger.info(f"Cleaned up {len(to_remove)} old performance rounds from memory")
    
    def generate_analysis_report(self) -> Dict:
        """
        生成分析报告（汇总所有轮次的数据）
        
        Returns:
            分析报告字典
        """
        if not self.enabled:
            return {}
        
        with self.lock:
            all_rounds = list(self.round_data.keys())
        
        if not all_rounds:
            return {'message': 'No performance data available'}
        
        # 汇总所有模块的统计
        module_stats = defaultdict(lambda: {
            'total_time': 0.0,
            'count': 0,
            'rounds': 0,
            'operations': defaultdict(lambda: {'total': 0.0, 'count': 0, 'rounds': set()})
        })
        
        for round_id in all_rounds:
            summary = self.get_round_summary(round_id)
            if not summary:
                continue
            
            for module_name, module_data in summary.get('modules', {}).items():
                module_stats[module_name]['total_time'] += module_data['total_time']
                module_stats[module_name]['count'] += module_data['count']
                module_stats[module_name]['rounds'] += 1
                
                for op_name, op_data in module_data.get('operations', {}).items():
                    module_stats[module_name]['operations'][op_name]['total'] += op_data['total']
                    module_stats[module_name]['operations'][op_name]['count'] += op_data['count']
                    module_stats[module_name]['operations'][op_name]['rounds'].add(round_id)
        
        # 计算平均值
        report = {
            'modules': {},
            'total_rounds': len(all_rounds),
            'generated_at': datetime.now(timezone.utc).isoformat(),
        }
        
        for module_name, stats in module_stats.items():
            avg_time_per_round = stats['total_time'] / stats['rounds'] if stats['rounds'] > 0 else 0.0
            avg_time_per_operation = stats['total_time'] / stats['count'] if stats['count'] > 0 else 0.0
            
            operations_summary = {}
            for op_name, op_stats in stats['operations'].items():
                operations_summary[op_name] = {
                    'total_time': op_stats['total'],
                    'count': op_stats['count'],
                    'rounds': len(op_stats['rounds']),
                    'avg_time': op_stats['total'] / op_stats['count'] if op_stats['count'] > 0 else 0.0,
                    'avg_time_per_round': op_stats['total'] / len(op_stats['rounds']) if op_stats['rounds'] else 0.0,
                }
            
            report['modules'][module_name] = {
                'total_time': stats['total_time'],
                'total_count': stats['count'],
                'rounds': stats['rounds'],
                'avg_time_per_round': avg_time_per_round,
                'avg_time_per_operation': avg_time_per_operation,
                'operations': operations_summary,
            }
        
        # 保存分析报告
        report_file = self.output_dir / f"performance_analysis_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Performance analysis report generated: {report_file}")
        
        return report
    
    def get_stats(self) -> Dict:
        """获取监控器统计信息"""
        with self.lock:
            return {
                'enabled': self.enabled,
                'total_rounds': self.stats['total_rounds'],
                'total_measurements': self.stats['total_measurements'],
                'current_round_id': self.current_round_id,
                'runtime_seconds': time.time() - self.stats['start_time'],
            }


# 全局实例
_performance_monitor: Optional[PerformanceMonitor] = None


def get_performance_monitor() -> PerformanceMonitor:
    """获取性能监控器实例"""
    global _performance_monitor
    if _performance_monitor is None:
        _performance_monitor = PerformanceMonitor()
    return _performance_monitor
