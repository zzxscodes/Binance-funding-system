"""
Alpha编排层。

职责：
- 数据层完成后，请求一致的数据快照（例如30天）
- 运行多个计算器（线程/进程）生成每个交易对的向量
- 将向量求和得到最终结果并传递给position_generator

设计说明（性能+安全性）：
- 在Windows上，多进程需要pickle大型DataFrame（慢且占用内存）。默认使用线程。
- 数据由alpha一次性获取以保证一致性和效率。
- 计算器不得修改共享输入。如果计算器需要修改，必须通过提供的视图请求可变副本（参见AlphaDataView）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

import pandas as pd

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import to_system_symbol
from ..data.api import get_data_api, DataAPI
from ..monitoring.performance import get_performance_monitor
from .calculator import AlphaCalculatorBase, AlphaDataView
from .calculators import load_calculators

logger = get_logger("alpha")


@dataclass(frozen=True)
class AlphaResult:
    """最终求和后的向量。"""

    weights: Dict[str, float]  # 系统交易对符号 -> 权重
    per_calculator: Dict[str, Dict[str, float]]  # 计算器名称 -> 权重字典


class AlphaEngine:
    """
    Alpha引擎：获取快照 + 并发运行计算器。
    """

    def __init__(
        self,
        data_api: Optional[DataAPI] = None,
        calculators: Optional[Sequence[AlphaCalculatorBase]] = None,
    ):
        self.data_api = data_api or get_data_api()
        # 优先使用传入的calculators，否则从calculators目录动态加载
        if calculators:
            self.calculators: List[AlphaCalculatorBase] = list(calculators)
        else:
            self.calculators: List[AlphaCalculatorBase] = list(load_calculators())

        # 并发默认设置（Windows友好）
        self.concurrency = config.get("strategy.alpha.concurrency", "thread")  # thread|process|none
        self.max_workers = int(config.get("strategy.alpha.max_workers", max(1, len(self.calculators))))

        # 默认历史窗口
        self.history_days = int(config.get("strategy.history_days", 30))
        
        # 性能监控
        self.performance_monitor = get_performance_monitor()

    def _get_labels(self, history_days: int) -> tuple[str, str]:
        end_time = datetime.now(timezone.utc)
        begin_time = end_time - timedelta(days=history_days)
        begin_label = self.data_api._get_date_time_label_from_datetime(begin_time)
        end_label = self.data_api._get_date_time_label_from_datetime(end_time)
        return begin_label, end_label

    def fetch_snapshot(
        self,
        symbols: Optional[Sequence[str]] = None,
        *,
        history_days: Optional[int] = None,
        mode: str = "5min",
    ) -> AlphaDataView:
        """
        为alpha获取一致的数据快照。

        返回不可变视图（共享）。每个计算器将获得自己的AlphaDataView实例，
        如果声明了修改行为，该实例将强制执行读时复制。
        """
        history_days = self.history_days if history_days is None else int(history_days)
        begin_label, end_label = self._get_labels(history_days)

        logger.info(f"Fetching snapshot: {history_days}d, mode={mode}, {begin_label} -> {end_label}")
        
        with self.performance_monitor.measure('alpha_engine', 'fetch_bar_data', {'mode': mode, 'symbols_count': len(symbols) if symbols else 0}):
            bar_data = self.data_api.get_bar_between(begin_label, end_label, mode=mode)
        
        with self.performance_monitor.measure('alpha_engine', 'fetch_tran_stats_data', {'mode': mode, 'symbols_count': len(symbols) if symbols else 0}):
            tran_stats_data = self.data_api.get_tran_stats_between(begin_label, end_label, mode=mode)

        symbol_set = None
        if symbols:
            # DataAPI返回的键是系统交易对格式（例如 btc-usdt）。
            # 触发符号可能是 BTCUSDT / btc-usdt 等。
            symbol_set = {to_system_symbol(s) for s in symbols}
            bar_data = {k: v for k, v in bar_data.items() if k in symbol_set}
            tran_stats_data = {k: v for k, v in tran_stats_data.items() if k in symbol_set}

        # 基础共享视图：默认不复制
        return AlphaDataView(bar_data=bar_data, tran_stats=tran_stats_data, symbols=symbol_set, copy_on_read=False)

    def _executor(self):
        if self.concurrency == "none" or len(self.calculators) <= 1:
            return None
        if self.concurrency == "process":
            # 警告：在Windows上这将pickle大数据；仅在计算器自行获取数据时使用。
            return ProcessPoolExecutor(max_workers=self.max_workers)
        return ThreadPoolExecutor(max_workers=self.max_workers)

    @staticmethod
    def _sum_weights(vectors: Iterable[Dict[str, float]]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for vec in vectors:
            for sym, w in vec.items():
                if w is None:
                    continue
                try:
                    fw = float(w)
                except Exception:
                    continue
                if not (fw == fw):  # NaN
                    continue
                out[sym] = out.get(sym, 0.0) + fw
        # 丢弃接近零的噪声
        out = {k: v for k, v in out.items() if abs(v) > 1e-12}
        return out

    def run(
        self,
        symbols: Optional[Sequence[str]] = None,
        *,
        history_days: Optional[int] = None,
        mode: str = "5min",
    ) -> AlphaResult:
        """
        运行所有计算器并返回求和后的向量。
        """
        if not self.calculators:
            return AlphaResult(weights={}, per_calculator={})

        base_view = self.fetch_snapshot(symbols, history_days=history_days, mode=mode)

        # 串行路径
        if self.concurrency == "none" or len(self.calculators) == 1:
            with self.performance_monitor.measure('alpha_engine', 'calculators_execution', {'mode': 'serial', 'count': len(self.calculators)}):
                per_calc: Dict[str, Dict[str, float]] = {}
                for calc in self.calculators:
                    with self.performance_monitor.measure('alpha_engine', f'calculator_{calc.name}'):
                        view = base_view.with_copy_on_read(calc.mutates_inputs)
                        per_calc[calc.name] = calc.run(view)
            return AlphaResult(weights=self._sum_weights(per_calc.values()), per_calculator=per_calc)

        # 并发路径
        per_calc = {}
        vectors = []
        executor = self._executor()
        assert executor is not None

        t0 = datetime.now(timezone.utc)
        with self.performance_monitor.measure('alpha_engine', 'calculators_execution', {'mode': self.concurrency, 'count': len(self.calculators)}):
            try:
                futures = {}
                for calc in self.calculators:
                    view = base_view.with_copy_on_read(calc.mutates_inputs)
                    futures[executor.submit(calc.run, view)] = calc.name

                for fut in as_completed(futures):
                    name = futures[fut]
                    try:
                        vec = fut.result()
                    except Exception as e:
                        logger.error(f"Calculator failed: {name}: {e}", exc_info=True)
                        vec = {}
                    per_calc[name] = vec
                    vectors.append(vec)
            finally:
                executor.shutdown(wait=True, cancel_futures=False)

        dt = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.info(f"Alpha calculators completed: {len(self.calculators)} calcs in {dt:.2f}s")

        return AlphaResult(weights=self._sum_weights(vectors), per_calculator=per_calc)


_alpha_engine: Optional[AlphaEngine] = None


def get_alpha_engine() -> AlphaEngine:
    global _alpha_engine
    if _alpha_engine is None:
        _alpha_engine = AlphaEngine()
    return _alpha_engine

