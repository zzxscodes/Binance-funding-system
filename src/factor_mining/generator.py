"""
因子生成器

功能：
1. 提供因子模板
2. 参数化生成因子
3. 自动生成calculator代码
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
import inspect

from ..common.logger import get_logger
from ..common.config import config
from ..strategy.calculator import AlphaCalculatorBase, AlphaDataView
from .utils import get_default_lookback_bars

logger = get_logger('factor_mining.generator')


@dataclass
class FactorTemplate:
    """因子模板"""
    name: str
    description: str
    code_template: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    parameter_ranges: Dict[str, tuple] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    
    def generate_code(self, **kwargs) -> str:
        """根据参数生成因子代码"""
        code = self.code_template
        # 替换参数
        for key, value in kwargs.items():
            code = code.replace(f"{{{key}}}", str(value))
        return code


class FactorGenerator:
    """因子生成器"""
    
    def __init__(self, templates_dir: Optional[Path] = None):
        """
        初始化因子生成器
        
        Args:
            templates_dir: 模板目录，如果为None则使用默认模板
        """
        self.templates_dir = templates_dir or Path(__file__).parent / "templates"
        self.templates: Dict[str, FactorTemplate] = {}
        self._load_default_templates()
    
    def _load_default_templates(self):
        """加载默认模板"""
        # 基础统计因子模板
        self.templates['mean_ratio'] = FactorTemplate(
            name='mean_ratio',
            description='均值比率因子',
            code_template='''"""
均值比率因子：{numerator} / {denominator} 的均值

参数:
    lookback_bars: 回看K线数量
    numerator: 分子字段
    denominator: 分母字段
"""

from __future__ import annotations
from typing import Dict, Optional
import pandas as pd
from src.strategy.calculator import AlphaCalculatorBase, AlphaDataView

CALCULATOR_INSTANCE = None


class MeanRatioCalculator(AlphaCalculatorBase):
    """均值比率因子计算器"""
    
    def __init__(self, lookback_bars: int = {lookback_bars}, 
                 numerator: str = "{numerator}",
                 denominator: str = "{denominator}",
                 name: Optional[str] = None):
        self.lookback_bars = int(lookback_bars)
        self.numerator = numerator
        self.denominator = denominator
        self.name = name or f"mean_ratio_{numerator}_{denominator}_{lookback_bars}"
        self.mutates_inputs = False
    
    @staticmethod
    def _score_symbol(bar_df: pd.DataFrame, tran_df: pd.DataFrame, 
                     numerator: str, denominator: str) -> Optional[float]:
        """计算单个交易对的分数"""
        if bar_df.empty or tran_df.empty:
            return None
        
        # 确定数据源
        if numerator in bar_df.columns:
            num_data = bar_df
        elif numerator in tran_df.columns:
            num_data = tran_df
        else:
            return None
            
        if denominator in bar_df.columns:
            den_data = bar_df
        elif denominator in tran_df.columns:
            den_data = tran_df
        else:
            return None
        
        if "open_time" not in num_data.columns or "open_time" not in den_data.columns:
            return None
        
        # 排序
        if not num_data["open_time"].is_monotonic_increasing:
            num_data = num_data.sort_values("open_time")
        if not den_data["open_time"].is_monotonic_increasing:
            den_data = den_data.sort_values("open_time")
        
        num_s = num_data.set_index("open_time")[numerator]
        den_s = den_data.set_index("open_time")[denominator]
        
        idx = num_s.index.intersection(den_s.index)
        if len(idx) == 0:
            return None
        
        num_v = pd.to_numeric(num_s.loc[idx], errors="coerce")
        den_v = pd.to_numeric(den_s.loc[idx], errors="coerce")
        
        valid = (den_v > 0) & num_v.notna() & den_v.notna()
        if int(valid.sum()) == 0:
            return None
        
        ratio = (num_v[valid] / den_v[valid]).astype(float)
        if ratio.empty:
            return None
        score = float(ratio.mean())
        if not (score == score):  # NaN
            return None
        return score
    
    def run(self, view: AlphaDataView) -> Dict[str, float]:
        """运行计算器"""
        scores: Dict[str, float] = {}
        
        for sym in view.iter_symbols():
            bar_df = view.get_bar(sym, tail=self.lookback_bars)
            tran_df = view.get_tran_stats(sym, tail=self.lookback_bars)
            sc = self._score_symbol(bar_df, tran_df, self.numerator, self.denominator)
            if sc is None:
                continue
            scores[sym] = sc
        
        if len(scores) < 2:
            return {}
        
        # 按分数排序，前一半做多，后一半做空
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        m = len(ranked)
        half = m // 2
        if half == 0:
            return {}
        
        longs = [s for s, _ in ranked[:half]]
        shorts = [s for s, _ in ranked[half:]]
        
        w_long = 1.0 / max(1, len(longs))
        w_short = -1.0 / max(1, len(shorts))
        
        out: Dict[str, float] = {}
        for s in longs:
            out[s] = out.get(s, 0.0) + w_long
        for s in shorts:
            out[s] = out.get(s, 0.0) + w_short
        return out


CALCULATOR_INSTANCE = MeanRatioCalculator(
    lookback_bars={lookback_bars},
    numerator="{numerator}",
    denominator="{denominator}",
    name="mean_ratio_{numerator}_{denominator}_{lookback_bars}"
)
''',
            parameters={
                'lookback_bars': get_default_lookback_bars(),
                'numerator': 'buy_dolvol4',
                'denominator': 'dolvol',
            },
            parameter_ranges={
                'lookback_bars': (100, 2000, 100),
                'numerator': ['buy_dolvol4', 'buy_dolvol3', 'buy_dolvol2', 'buy_dolvol1', 
                              'sell_dolvol4', 'sell_dolvol3', 'sell_dolvol2', 'sell_dolvol1',
                              'buy_volume', 'sell_volume', 'volume'],
                'denominator': ['dolvol', 'volume', 'buy_dolvol', 'sell_dolvol'],
            }
        )
        
        # 排名因子模板
        self.templates['rank'] = FactorTemplate(
            name='rank',
            description='排名因子',
            code_template='''"""
排名因子：基于{field}的排名

参数:
    lookback_bars: 回看K线数量
    field: 排名字段
"""

from __future__ import annotations
from typing import Dict, Optional
import pandas as pd
from src.strategy.calculator import AlphaCalculatorBase, AlphaDataView

CALCULATOR_INSTANCE = None


class RankCalculator(AlphaCalculatorBase):
    """排名因子计算器"""
    
    def __init__(self, lookback_bars: int = {lookback_bars},
                 field: str = "{field}",
                 name: Optional[str] = None):
        self.lookback_bars = int(lookback_bars)
        self.field = field
        self.name = name or f"rank_{field}_{lookback_bars}"
        self.mutates_inputs = False
    
    @staticmethod
    def _score_symbol(bar_df: pd.DataFrame, tran_df: pd.DataFrame, field: str) -> Optional[float]:
        """计算单个交易对的分数"""
        # 确定数据源
        if field in bar_df.columns:
            data = bar_df
        elif field in tran_df.columns:
            data = tran_df
        else:
            return None
        
        if data.empty:
            return None
        
        if "open_time" not in data.columns:
            return None
        
        if not data["open_time"].is_monotonic_increasing:
            data = data.sort_values("open_time")
        
        values = pd.to_numeric(data[field], errors="coerce")
        valid = values.notna()
        if int(valid.sum()) == 0:
            return None
        
        score = float(values[valid].mean())
        if not (score == score):  # NaN
            return None
        return score
    
    def run(self, view: AlphaDataView) -> Dict[str, float]:
        """运行计算器"""
        scores: Dict[str, float] = {}
        
        for sym in view.iter_symbols():
            bar_df = view.get_bar(sym, tail=self.lookback_bars)
            tran_df = view.get_tran_stats(sym, tail=self.lookback_bars)
            sc = self._score_symbol(bar_df, tran_df, self.field)
            if sc is None:
                continue
            scores[sym] = sc
        
        if len(scores) < 2:
            return {}
        
        # 按分数排序，前一半做多，后一半做空
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        m = len(ranked)
        half = m // 2
        if half == 0:
            return {}
        
        longs = [s for s, _ in ranked[:half]]
        shorts = [s for s, _ in ranked[half:]]
        
        w_long = 1.0 / max(1, len(longs))
        w_short = -1.0 / max(1, len(shorts))
        
        out: Dict[str, float] = {}
        for s in longs:
            out[s] = out.get(s, 0.0) + w_long
        for s in shorts:
            out[s] = out.get(s, 0.0) + w_short
        return out


CALCULATOR_INSTANCE = RankCalculator(
    lookback_bars={lookback_bars},
    field="{field}",
    name="rank_{field}_{lookback_bars}"
)
''',
            parameters={
                'lookback_bars': get_default_lookback_bars(),
                'field': 'buy_dolvol4',
            },
            parameter_ranges={
                'lookback_bars': (100, 2000, 100),
                'field': ['buy_dolvol4', 'buy_dolvol3', 'buy_dolvol2', 'buy_dolvol1',
                         'sell_dolvol4', 'sell_dolvol3', 'sell_dolvol2', 'sell_dolvol1',
                         'buy_volume', 'sell_volume', 'volume', 'dolvol'],
            }
        )
    
    def register_template(self, template: FactorTemplate):
        """注册模板"""
        self.templates[template.name] = template
        logger.info(f"注册因子模板: {template.name}")
    
    def generate_factor(self, template_name: str, **kwargs) -> str:
        """
        生成因子代码
        
        Args:
            template_name: 模板名称
            **kwargs: 模板参数
        
        Returns:
            生成的因子代码
        """
        if template_name not in self.templates:
            raise ValueError(f"模板 {template_name} 不存在")
        
        template = self.templates[template_name]
        return template.generate_code(**kwargs)
    
    def generate_calculator_file(self, template_name: str, output_path: Path, 
                                factor_name: Optional[str] = None, **kwargs) -> Path:
        """
        生成calculator文件
        
        Args:
            template_name: 模板名称
            output_path: 输出路径
            factor_name: 因子名称（用于文件名）
            **kwargs: 模板参数
        
        Returns:
            生成的文件路径
        """
        code = self.generate_factor(template_name, **kwargs)
        
        if factor_name is None:
            # 从参数生成文件名
            param_str = "_".join([f"{k}_{v}" for k, v in sorted(kwargs.items())])
            factor_name = f"{template_name}_{param_str}"
        
        # 清理文件名
        factor_name = re.sub(r'[^\w-]', '_', factor_name)
        filename = f"{factor_name}.py"
        filepath = output_path / filename
        
        # 确保目录存在
        output_path.mkdir(parents=True, exist_ok=True)
        
        filepath.write_text(code, encoding='utf-8')
        logger.info(f"生成因子文件: {filepath}")
        
        return filepath
    
    def search_parameters(self, template_name: str, 
                         param_grid: Optional[Dict[str, List[Any]]] = None) -> List[Dict[str, Any]]:
        """
        参数搜索
        
        Args:
            template_name: 模板名称
            param_grid: 参数网格，如果为None则使用模板的parameter_ranges
        
        Returns:
            参数组合列表
        """
        if template_name not in self.templates:
            raise ValueError(f"模板 {template_name} 不存在")
        
        template = self.templates[template_name]
        
        if param_grid is None:
            param_grid = {}
            for param, range_def in template.parameter_ranges.items():
                if isinstance(range_def, tuple) and len(range_def) == 3:
                    # (start, end, step)
                    start, end, step = range_def
                    param_grid[param] = list(range(int(start), int(end) + 1, int(step)))
                elif isinstance(range_def, list):
                    param_grid[param] = range_def
                else:
                    param_grid[param] = [range_def]
        
        # 生成所有参数组合
        from itertools import product
        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        
        combinations = []
        for combo in product(*param_values):
            combinations.append(dict(zip(param_names, combo)))
        
        return combinations
