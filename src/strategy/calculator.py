"""
Alpha计算器框架（新策略架构）。

流水线：
数据完成 -> AlphaEngine -> 多个AlphaCalculators -> 求和权重 -> PositionGenerator

关键约定：
- 计算器接收AlphaDataView，可以强制执行读时复制以实现安全修改。
- 为了性能，计算器应该是纯函数/只读（mutates_inputs=False）并避免每个交易对的重度合并。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set

import pandas as pd

from ..common.logger import get_logger
from ..data.api import get_data_api
from ..system_api import get_system_api

logger = get_logger("alpha_calculator")


@dataclass(frozen=True)
class AlphaDataView:
    """
    单个数据快照的视图。

    安全性：
    - copy_on_read=False（默认）：返回的DataFrame是共享的；将它们视为只读。
    - copy_on_read=True：返回的DataFrame是副本；对于需要修改的计算器是安全的。
    """

    bar_data: Dict[str, pd.DataFrame]  # 系统交易对符号 -> bar数据框
    tran_stats: Dict[str, pd.DataFrame]  # 系统交易对符号 -> tran_stats数据框
    symbols: Optional[Set[str]] = None  # 可选过滤器
    copy_on_read: bool = False

    def with_copy_on_read(self, enabled: bool) -> "AlphaDataView":
        return AlphaDataView(
            bar_data=self.bar_data,
            tran_stats=self.tran_stats,
            symbols=self.symbols,
            copy_on_read=bool(enabled),
        )

    def iter_symbols(self) -> Iterable[str]:
        # 保持稳定顺序：先bar键，然后是仅tran_stats的交易对。
        if self.symbols is not None:
            for s in self.bar_data.keys():
                if s in self.symbols:
                    yield s
            for s in self.tran_stats.keys():
                if s in self.symbols and s not in self.bar_data:
                    yield s
            return

        for s in self.bar_data.keys():
            yield s
        for s in self.tran_stats.keys():
            if s not in self.bar_data:
                yield s

    def get_bar(self, symbol: str, *, tail: Optional[int] = None) -> pd.DataFrame:
        df = self.bar_data.get(symbol, pd.DataFrame())
        if df.empty:
            return df
        if tail is not None and len(df) > tail:
            df = df.tail(int(tail))
        return df.copy() if self.copy_on_read else df

    def get_tran_stats(self, symbol: str, *, tail: Optional[int] = None) -> pd.DataFrame:
        df = self.tran_stats.get(symbol, pd.DataFrame())
        if df.empty:
            return df
        if tail is not None and len(df) > tail:
            df = df.tail(int(tail))
        return df.copy() if self.copy_on_read else df
    
    # ========== 数据查询接口（与StrategyAPI保持一致） ==========
    
    def get_bar_between(
        self, 
        begin_date_time_label: str, 
        end_date_time_label: str,
        mode: str = '5min'
    ) -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的bar数据
        
        Args:
            begin_date_time_label: 开始时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2025-12-22-004'
            end_date_time_label: 结束时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2026-01-10-013'
            mode: 时间周期，支持 '5min', '1h', '4h', '8h', '12h', '24h'，默认为 '5min'
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含bar表字段
            格式: {'btc-usdt': DataFrame, 'eth-usdt': DataFrame, ...}
        """
        data_api = get_data_api()
        return data_api.get_bar_between(begin_date_time_label, end_date_time_label, mode=mode)
    
    def get_tran_stats_between(
        self, 
        begin_date_time_label: str, 
        end_date_time_label: str,
        mode: str = '5min'
    ) -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的tran_stats数据
        
        Args:
            begin_date_time_label: 开始时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2025-12-22-004'
            end_date_time_label: 结束时间标签，格式 'YYYY-MM-DD-HHH'，例如 '2026-01-10-013'
            mode: 时间周期，支持 '5min', '1h', '4h', '8h', '12h', '24h'，默认为 '5min'
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含tran_stats表字段
            格式: {'btc-usdt': DataFrame, 'eth-usdt': DataFrame, ...}
        """
        data_api = get_data_api()
        return data_api.get_tran_stats_between(begin_date_time_label, end_date_time_label, mode=mode)
    
    def get_funding_rate_between(
        self, 
        begin_date_time_label: str, 
        end_date_time_label: str
    ) -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的历史资金费率数据
        
        Args:
            begin_date_time_label: 开始时间标签，格式 'YYYY-MM-DD-HHH'
            end_date_time_label: 结束时间标签，格式 'YYYY-MM-DD-HHH'
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含资金费率数据
        """
        data_api = get_data_api()
        return data_api.get_funding_rate_between(begin_date_time_label, end_date_time_label)
    
    def get_premium_index_bar_between(
        self, 
        begin_date_time_label: str, 
        end_date_time_label: str,
        mode: str = '5min'
    ) -> Dict[str, pd.DataFrame]:
        """
        获取指定时间范围内的溢价指数K线数据
        
        Args:
            begin_date_time_label: 开始时间标签，格式 'YYYY-MM-DD-HHH'
            end_date_time_label: 结束时间标签，格式 'YYYY-MM-DD-HHH'
            mode: 时间周期，目前支持 '5min'，默认为 '5min'
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含溢价指数K线数据
        """
        data_api = get_data_api()
        return data_api.get_premium_index_bar_between(begin_date_time_label, end_date_time_label, mode=mode)
    
    # ========== Universe查询接口（与StrategyAPI保持一致） ==========
    
    def get_last_universe(self, version: str = 'v1') -> List[str]:
        """
        获取最新的Universe（可交易资产列表）
        
        Args:
            version: 版本号，如 'v1', 'v2' 等，默认为 'v1'
        
        Returns:
            交易对列表，格式: ['btc-usdt', 'eth-usdt', ...]
        """
        system_api = get_system_api()
        return system_api.get_last_universe(version=version)
    
    def get_universe(self, date: Optional[str] = None, version: str = 'v1') -> List[str]:
        """
        按日期获取Universe（可交易资产列表）
        
        Args:
            date: 日期字符串，格式 'YYYY-MM-DD'，如果不指定，返回最新的
            version: 版本号，如 'v1', 'v2' 等，默认为 'v1'
        
        Returns:
            交易对列表，格式: ['btc-usdt', 'eth-usdt', ...]
        """
        system_api = get_system_api()
        return system_api.get_universe(date=date, version=version)


class AlphaCalculatorBase:
    """
    Alpha计算器基类。

    实现run(view)并返回Dict[系统交易对符号, 权重]。
    """

    name: str = "base"
    mutates_inputs: bool = False

    def run(self, view: AlphaDataView) -> Dict[str, float]:
        raise NotImplementedError
    
    def get_description(self) -> Dict:
        """
        获取策略描述信息，用于策略报告显示。
        
        子类可以重写此方法以提供自定义描述。
        默认实现会从类的docstring和属性中提取信息。
        
        Returns:
            Dict包含策略描述信息：
            {
                'description': str,  # 策略描述
                'criteria': List[str],  # 选币标准列表
                'parameters': Dict,  # 策略参数
                'strategy_direction': str,  # 策略方向（如果有）
            }
        """
        # 默认实现：从docstring提取描述
        doc = self.__class__.__doc__ or ""
        description = doc.strip().split('\n')[0] if doc else f"{self.name}策略"
        
        # 尝试从docstring中提取更详细的信息
        criteria = []
        if doc:
            lines = doc.strip().split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith('-') or line.startswith('*') or '：' in line or ':' in line:
                    criteria.append(line.lstrip('-* '))
        
        # 从实例属性中提取参数
        parameters = {}
        if hasattr(self, 'lookback_days'):
            parameters['lookback_days'] = getattr(self, 'lookback_days')
        if hasattr(self, 'max_positions'):
            parameters['max_positions'] = getattr(self, 'max_positions')
        if hasattr(self, 'min_funding_rate'):
            parameters['min_funding_rate'] = getattr(self, 'min_funding_rate')
        
        strategy_direction = None
        if hasattr(self, 'strategy_direction'):
            strategy_direction = getattr(self, 'strategy_direction')
        
        return {
            'description': description,
            'criteria': criteria if criteria else [f"{self.name}策略选币标准"],
            'parameters': parameters,
            'strategy_direction': strategy_direction
        }


def build_default_calculators() -> Sequence[AlphaCalculatorBase]:
    """
    默认计算器集合（已废弃，请使用calculators目录的动态加载机制）。

    此函数保留仅用于向后兼容。新代码应使用 src.strategy.calculators.load_calculators()。
    """
    # 尝试从calculators目录加载
    try:
        from .calculators import load_calculators
        return list(load_calculators())
    except Exception:
        # 如果加载失败，返回空列表（向后兼容）
        return []

