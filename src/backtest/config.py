"""
回测配置模块
定义回测系统的配置参数
"""
from typing import Dict, Any, Optional
from pathlib import Path

from ..common.config import config as system_config
from ..common.logger import get_logger

logger = get_logger('backtest_config')


class BacktestConfigManager:
    """回测配置管理器"""
    
    # 默认配置
    DEFAULT_CONFIG = {
        'backtest': {
            # 数据重放配置
            'replay': {
                'interval': '5m',  # 默认周期
                'intervals': ['5m'],  # 多周期配置
                'buffer_size': 1000,  # 缓冲区大小
            },
            
            # 执行配置
            'execution': {
                'mode': 'backtest',  # 回测模式
                'leverage': 1.0,  # 默认杠杆
                'maker_fee': 0.0002,  # 挂单手续费
                'taker_fee': 0.0004,  # 吃单手续费
                'slippage': 0.0,  # 滑点（万分位）
                'funding_rate_apply': True,  # 是否应用资金费率
            },
            
            # 回测样本
            'default_backtest': {
                'initial_balance': 10000.0,
                'symbols': ['BTCUSDT'],
                'start_date': '2024-01-01',
                'end_date': '2024-01-31',
            },
            
            # 输出配置
            'output': {
                'result_dir': 'data/backtest_results',
                'report_format': 'text',  # text, json, csv
                'save_trades': True,
                'save_portfolio_history': True,
            },
            
            # 性能配置
            'performance': {
                'cache_klines': True,  # 缓存K线数据
                'parallel_execution': False,  # 并行执行多个回测
                'max_workers': 4,  # 最大并行数
            },
        }
    }
    
    @staticmethod
    def get_config(key: str, default: Any = None) -> Any:
        """
        获取回测配置参数
        
        Args:
            key: 配置key (使用点号分隔，如 'backtest.execution.leverage')
            default: 默认值
        
        Returns:
            配置值
        """
        # 优先从系统config读取（可能被覆盖）
        try:
            value = system_config.get(key, None)
            if value is not None:
                return value
        except:
            pass
        
        # 从默认配置读取
        keys = key.split('.')
        value = BacktestConfigManager.DEFAULT_CONFIG
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, None)
                if value is None:
                    return default
            else:
                return default
        
        return value if value is not None else default
    
    @staticmethod
    def get_backtest_config() -> Dict[str, Any]:
        """获取完整的回测配置"""
        return BacktestConfigManager.get_config('backtest', BacktestConfigManager.DEFAULT_CONFIG['backtest'])
    
    @staticmethod
    def get_replay_config() -> Dict[str, Any]:
        """获取数据重放配置"""
        return BacktestConfigManager.get_config('backtest.replay', {})
    
    @staticmethod
    def get_execution_config() -> Dict[str, Any]:
        """获取执行配置"""
        return BacktestConfigManager.get_config('backtest.execution', {})
    
    @staticmethod
    def get_output_config() -> Dict[str, Any]:
        """获取输出配置"""
        return BacktestConfigManager.get_config('backtest.output', {})
    
    @staticmethod
    def get_result_dir() -> Path:
        """获取回测结果输出目录"""
        result_dir = BacktestConfigManager.get_config('backtest.output.result_dir', 'data/backtest_results')
        result_path = Path(result_dir)
        result_path.mkdir(parents=True, exist_ok=True)
        return result_path
    
    @staticmethod
    def get_default_symbols() -> list:
        """获取默认交易对列表"""
        symbols = BacktestConfigManager.get_config('backtest.default_backtest.symbols', ['BTCUSDT'])
        return symbols if isinstance(symbols, list) else [symbols]
    
    @staticmethod
    def get_default_leverage() -> float:
        """获取默认杠杆"""
        return BacktestConfigManager.get_config('backtest.execution.leverage', 1.0)
    
    @staticmethod
    def get_default_fees() -> Dict[str, float]:
        """获取默认手续费配置"""
        return {
            'maker_fee': BacktestConfigManager.get_config('backtest.execution.maker_fee', 0.0002),
            'taker_fee': BacktestConfigManager.get_config('backtest.execution.taker_fee', 0.0004),
        }
    
    @staticmethod
    def load_from_file(config_path: Path) -> Dict[str, Any]:
        """
        从文件加载回测配置
        
        Args:
            config_path: 配置文件路径（支持YAML）
        
        Returns:
            配置字典
        """
        import yaml
        
        if not config_path.exists():
            logger.warning(f"Config file not found: {config_path}")
            return BacktestConfigManager.DEFAULT_CONFIG['backtest']
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            if 'backtest' in config:
                return config['backtest']
            
            return config
        
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            return BacktestConfigManager.DEFAULT_CONFIG['backtest']
    
    @staticmethod
    def save_to_file(config: Dict[str, Any], output_path: Path):
        """
        保存配置到文件
        
        Args:
            config: 配置字典
            output_path: 输出文件路径
        """
        import yaml
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.safe_dump({'backtest': config}, f, allow_unicode=True, default_flow_style=False)
            
            logger.info(f"Config saved to {output_path}")
        
        except Exception as e:
            logger.error(f"Failed to save config to {output_path}: {e}")
