"""
回测配置适配器
将BacktestConfig和全局config合并，提供给ExecutionMethodSelector使用

原因：
- ExecutionMethodSelector需要访问execution.method_selection配置（在全局config中）
- 回测系统使用BacktestConfig（dataclass，没有.get()方法）
- 此适配器合并两者，让ExecutionMethodSelector可以使用回测专用的配置
"""
from typing import Any, Optional
from ..common.config import config as global_config
from .models import BacktestConfig


class BacktestConfigAdapter:
    """
    配置适配器：将BacktestConfig和全局config合并
    
    ExecutionMethodSelector需要访问execution.method_selection配置，
    这些配置在全局config中，但回测系统使用BacktestConfig。
    此适配器合并两者：
    - execution.method_selection相关配置从全局config读取
    - 回测特定配置（如leverage）从BacktestConfig读取
    """
    
    def __init__(self, backtest_config: BacktestConfig):
        self.backtest_config = backtest_config
        self.global_config = global_config
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        获取配置值
        
        优先级：
        1. 全局config（用于execution.method_selection等执行相关配置）
        2. BacktestConfig（用于回测特定配置，如leverage）
        3. 默认值
        
        Args:
            key_path: 配置路径，如 'execution.method_selection.enabled'
            default: 默认值
            
        Returns:
            配置值
        """
        # 优先从全局config读取（特别是execution.method_selection相关配置）
        value = self.global_config.get(key_path, None)
        if value is not None:
            return value
        
        # 如果全局config没有，尝试从BacktestConfig读取
        # BacktestConfig是dataclass，需要手动映射
        if key_path == 'execution.contract_settings.leverage':
            # 杠杆从BacktestConfig读取（回测专用配置）
            return self.backtest_config.leverage
        
        # 其他execution相关配置应该从全局config读取
        if key_path.startswith('execution.'):
            return default
        
        # 默认返回None，让调用者使用default
        return default
