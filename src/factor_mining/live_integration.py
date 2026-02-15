"""
因子实盘集成

功能：
1. 自动生成calculator文件
2. 更新配置文件
3. 与实盘系统集成
"""

from __future__ import annotations

from typing import List, Optional
from pathlib import Path
import yaml

from ..common.logger import get_logger
from ..common.config import config
from ..strategy.calculator import AlphaCalculatorBase

logger = get_logger('factor_mining.live_integration')


class FactorLiveIntegrator:
    """因子实盘集成器"""
    
    def __init__(self, calculators_dir: Optional[Path] = None, config_file: Optional[Path] = None):
        """
        初始化
        
        Args:
            calculators_dir: calculators目录
            config_file: 配置文件路径
        """
        if calculators_dir is None:
            calculators_dir = Path(__file__).parent.parent / "strategy" / "calculators"
        self.calculators_dir = calculators_dir
        
        if config_file is None:
            config_file = Path(__file__).parent.parent.parent / "config" / "default.yaml"
        self.config_file = config_file
    
    def integrate_factor(self, calculator: AlphaCalculatorBase,
                        factor_code: str, factor_name: Optional[str] = None) -> Path:
        """
        集成因子到实盘系统
        
        Args:
            calculator: 因子计算器
            factor_code: 因子代码
            factor_name: 因子文件名（不含.py）
        
        Returns:
            生成的文件路径
        """
        if factor_name is None:
            factor_name = calculator.name
        
        # 清理文件名
        factor_name = factor_name.replace('/', '_').replace('\\', '_')
        filename = f"{factor_name}.py"
        filepath = self.calculators_dir / filename
        
        # 写入文件
        filepath.write_text(factor_code, encoding='utf-8')
        logger.info(f"生成因子文件: {filepath}")
        
        return filepath
    
    def update_config(self, calculator_names: List[str], enabled: bool = True):
        """
        更新配置文件
        
        Args:
            calculator_names: 计算器名称列表
            enabled: 是否启用
        """
        try:
            # 读取配置
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f)
            
            # 更新calculators配置
            if 'strategy' not in config_data:
                config_data['strategy'] = {}
            if 'calculators' not in config_data['strategy']:
                config_data['strategy']['calculators'] = {}
            
            if enabled:
                # 添加到enabled列表
                if 'enabled' not in config_data['strategy']['calculators']:
                    config_data['strategy']['calculators']['enabled'] = []
                
                enabled_list = config_data['strategy']['calculators']['enabled']
                for name in calculator_names:
                    if name not in enabled_list:
                        enabled_list.append(name)
            else:
                # 从enabled列表移除
                if 'enabled' in config_data['strategy']['calculators']:
                    enabled_list = config_data['strategy']['calculators']['enabled']
                    config_data['strategy']['calculators']['enabled'] = [
                        name for name in enabled_list if name not in calculator_names
                    ]
            
            # 写入配置
            with open(self.config_file, 'w', encoding='utf-8') as f:
                yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True)
            
            logger.info(f"更新配置文件: {self.config_file}")
            
        except Exception as e:
            logger.error(f"更新配置文件失败: {e}", exc_info=True)
    
    def deploy_factors(self, calculators: List[AlphaCalculatorBase],
                      factor_codes: List[str],
                      enable_in_config: bool = True) -> List[Path]:
        """
        部署因子到实盘系统
        
        Args:
            calculators: 因子计算器列表
            factor_codes: 因子代码列表
            enable_in_config: 是否在配置中启用
        
        Returns:
            生成的文件路径列表
        """
        filepaths = []
        calculator_names = []
        
        for calc, code in zip(calculators, factor_codes):
            try:
                filepath = self.integrate_factor(calc, code)
                filepaths.append(filepath)
                calculator_names.append(calc.name)
            except Exception as e:
                logger.error(f"部署因子 {calc.name} 失败: {e}")
        
        if enable_in_config and calculator_names:
            self.update_config(calculator_names, enabled=True)
        
        logger.info(f"部署了 {len(filepaths)} 个因子到实盘系统")
        return filepaths
