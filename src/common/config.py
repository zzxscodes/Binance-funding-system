"""
配置管理模块
"""
import os
import yaml
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime


class Config:
    """配置管理类"""
    
    _instance: Optional['Config'] = None
    _config: Dict[str, Any] = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._config:
            self._load_config()
    
    def _load_config(self):
        """加载配置文件"""
        config_path = Path(__file__).parent.parent.parent / "config" / "default.yaml"
        
        # 如果配置文件不存在，使用默认配置
        if not config_path.exists():
            self._config = self._get_default_config()
            self._save_default_config(config_path)
        else:
            with open(config_path, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f) or {}
            
            # 从环境变量覆盖敏感信息
            self._override_from_env()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """获取默认配置"""
        return {
            'binance': {
                'api_base': os.getenv('BINANCE_API_BASE', 'https://fapi.binance.com'),
                'ws_base': os.getenv('BINANCE_WS_BASE', 'wss://fstream.binance.com'),
                'api_key': os.getenv('BINANCE_API_KEY', ''),
                'api_secret': os.getenv('BINANCE_API_SECRET', ''),
            },
            'data': {
                'universe_update_time': '23:55',  # 北京时间
                'timezone': 'Asia/Shanghai',
                'kline_interval': '5m',
                'max_history_days': 30,
                'data_directory': 'data',
                'universe_directory': 'data/universe',
                'trades_directory': 'data/trades',
                'klines_directory': 'data/klines',
                'funding_rates_directory': 'data/funding_rates',
                'premium_index_directory': 'data/premium_index',
                'positions_directory': 'data/positions',
                'signals_directory': 'data/signals',
            },
            'strategy': {
                'history_days': 30,  # 最长30天
                'calculation_interval': '5m',
            },
            'execution': {
                'accounts': [
                    {'account_id': 'account1'},
                    {'account_id': 'account2'},
                    {'account_id': 'account3'},
                ]
            },
            'monitoring': {
                'check_interval': 60,  # 秒
                'alert_thresholds': {
                    'position_deviation': 0.05,  # 5%
                    'max_exposure': 1000000,  # USDT
                }
            },
            'ipc': {
                'socket_type': 'tcp',  # tcp/unix
                'tcp_host': '127.0.0.1',
                'tcp_port': 8888,
                'socket_path': '/tmp/binance_quant.sock',
            },
            'logging': {
                'level': 'INFO',
                'log_directory': 'logs',
                'log_file': 'binance_quant.log',
                'max_bytes': 10485760,  # 10MB
                'backup_count': 7,
            }
        }
    
    def _save_default_config(self, config_path: Path):
        """保存默认配置到文件"""
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(self._config, f, default_flow_style=False, allow_unicode=True)
    
    def _override_from_env(self):
        """从环境变量覆盖配置"""
        if 'BINANCE_API_KEY' in os.environ:
            self._config['binance']['api_key'] = os.environ['BINANCE_API_KEY']
        if 'BINANCE_API_SECRET' in os.environ:
            self._config['binance']['api_secret'] = os.environ['BINANCE_API_SECRET']
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        获取配置值，支持点号分隔的路径
        例如: get('binance.api_key')
        """
        keys = key_path.split('.')
        value = self._config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value
    
    def set(self, key_path: str, value: Any):
        """
        设置配置值，支持点号分隔的路径
        """
        keys = key_path.split('.')
        config = self._config
        for key in keys[:-1]:
            if key not in config:
                config[key] = {}
            config = config[key]
        config[keys[-1]] = value
    
    def reload(self):
        """重新加载配置"""
        self._config = {}
        self._load_config()
    
    def get_binance_api_base(self) -> str:
        """
        根据 execution.mode 自动获取正确的 Binance API 地址
        
        Returns:
            API base URL
        """
        execution_mode = self.get('execution.mode', 'mock')
        
        if execution_mode == 'testnet':
            # 测试网模式：使用 testnet 配置
            testnet_config = self.get('execution.testnet', {})
            return testnet_config.get('api_base', 'https://testnet.binancefuture.com')
        elif execution_mode == 'mock':
            # Mock模式：使用 mock 配置
            mock_config = self.get('execution.mock', {})
            return mock_config.get('api_base', 'https://mock.binance.com')
        elif execution_mode == 'live':
            # Live模式：使用 live 配置
            live_config = self.get('execution.live', {})
            return live_config.get('api_base', 'https://fapi.binance.com')
        else:
            # 默认配置
            return self.get('binance.api_base', 'https://fapi.binance.com')
    
    def get_binance_ws_base(self) -> str:
        """
        根据 execution.mode 自动获取正确的 Binance WebSocket 地址
        
        Returns:
            WebSocket base URL
        """
        execution_mode = self.get('execution.mode', 'mock')
        
        if execution_mode == 'testnet':
            # 测试网模式：使用 testnet 配置
            testnet_config = self.get('execution.testnet', {})
            return testnet_config.get('ws_base', 'wss://stream.binancefuture.com')
        elif execution_mode == 'mock':
            # Mock模式：使用 mock 配置
            mock_config = self.get('execution.mock', {})
            return mock_config.get('ws_base', 'wss://mock.binance.com')
        elif execution_mode == 'live':
            # Live模式：使用 live 配置
            live_config = self.get('execution.live', {})
            return live_config.get('ws_base', 'wss://fstream.binance.com')
        else:
            # 默认配置
            return self.get('binance.ws_base', 'wss://fstream.binance.com')
    
    # 数据层端点缓存（用于testnet回退到live）
    _data_layer_api_base_cache: Optional[str] = None
    _data_layer_ws_base_cache: Optional[str] = None
    _data_layer_fallback_checked: bool = False
    
    async def _test_endpoint_connectivity(self, api_base: str, timeout: float = 5.0) -> bool:
        """
        测试端点连通性
        
        Args:
            api_base: API base URL
            timeout: 超时时间（秒）
        
        Returns:
            True 如果端点可用，False 否则
        """
        try:
            # 延迟导入aiohttp，避免模块级别导入问题
            import aiohttp
            url = f"{api_base}/fapi/v1/ping"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    return response.status == 200
        except Exception:
            return False
    
    def get_binance_api_base_for_data_layer(self) -> str:
        """
        获取数据层使用的 Binance API 地址（支持 testnet 回退到 live）
        
        在 testnet 模式下，如果 testnet 端点不可用，自动回退到 live 端点
        执行层应使用 get_binance_api_base() 确保始终使用配置的端点
        
        Returns:
            API base URL
        """
        execution_mode = self.get('execution.mode', 'mock')
        
        # 如果不是 testnet 模式，直接返回正常端点
        if execution_mode != 'testnet':
            return self.get_binance_api_base()
        
        # 如果已经检查过并设置了回退，直接返回缓存的端点
        if self._data_layer_api_base_cache is not None:
            return self._data_layer_api_base_cache
        
        # 获取 testnet 和 live 端点
        testnet_config = self.get('execution.testnet', {})
        testnet_api_base = testnet_config.get('api_base', 'https://testnet.binancefuture.com')
        live_api_base = self.get('binance.api_base', 'https://fapi.binance.com')
        
        # 默认使用 testnet，实际回退会在异步检查时进行
        return testnet_api_base
    
    async def get_binance_api_base_for_data_layer_async(self) -> str:
        """
        异步获取数据层使用的 Binance API 地址（支持 testnet 回退到 live）
        
        在 testnet 模式下，会测试 testnet 端点连通性，如果失败则回退到 live 端点
        
        Returns:
            API base URL
        """
        execution_mode = self.get('execution.mode', 'mock')
        
        # 如果不是 testnet 模式，直接返回正常端点
        if execution_mode != 'testnet':
            return self.get_binance_api_base()
        
        # 如果已经检查过并设置了回退，直接返回缓存的端点
        if self._data_layer_api_base_cache is not None:
            return self._data_layer_api_base_cache
        
        # 获取 testnet 和 live 端点
        testnet_config = self.get('execution.testnet', {})
        testnet_api_base = testnet_config.get('api_base', 'https://testnet.binancefuture.com')
        live_api_base = self.get('binance.api_base', 'https://fapi.binance.com')
        
        # 测试 testnet 端点连通性
        is_testnet_available = await self._test_endpoint_connectivity(testnet_api_base, timeout=5.0)
        
        if is_testnet_available:
            # testnet 可用，使用 testnet
            self._data_layer_api_base_cache = testnet_api_base
            self._data_layer_fallback_checked = True
            return testnet_api_base
        else:
            # testnet 不可用，回退到 live 端点
            self._data_layer_api_base_cache = live_api_base
            self._data_layer_fallback_checked = True
            # 记录回退日志
            import logging
            logger = logging.getLogger('config')
            logger.warning(
                f"Testnet endpoint {testnet_api_base} is not available, "
                f"falling back to live endpoint {live_api_base} for data layer"
            )
            return live_api_base
    
    def get_binance_ws_base_for_data_layer(self) -> str:
        """
        获取数据层使用的 Binance WebSocket 地址（支持 testnet 回退到 live）
        
        在 testnet 模式下，如果 testnet 端点不可用，自动回退到 live 端点
        执行层应使用 get_binance_ws_base() 确保始终使用配置的端点
        
        Returns:
            WebSocket base URL
        """
        execution_mode = self.get('execution.mode', 'mock')
        
        # 如果不是 testnet 模式，直接返回正常端点
        if execution_mode != 'testnet':
            return self.get_binance_ws_base()
        
        # 如果已经检查过并设置了回退，直接返回缓存的端点
        if self._data_layer_ws_base_cache is not None:
            return self._data_layer_ws_base_cache
        
        # 获取 testnet 和 live 端点
        testnet_config = self.get('execution.testnet', {})
        testnet_ws_base = testnet_config.get('ws_base', 'wss://stream.binancefuture.com')
        live_ws_base = self.get('binance.ws_base', 'wss://fstream.binance.com')
        
        # 如果 API 端点已经回退到 live，WebSocket 也使用 live
        if self._data_layer_api_base_cache and 'testnet' not in self._data_layer_api_base_cache:
            self._data_layer_ws_base_cache = live_ws_base
            return live_ws_base
        
        return testnet_ws_base
    
    async def get_binance_ws_base_for_data_layer_async(self) -> str:
        """
        异步获取数据层使用的 Binance WebSocket 地址（支持 testnet 回退到 live）
        
        Returns:
            WebSocket base URL
        """
        execution_mode = self.get('execution.mode', 'mock')
        
        # 如果不是 testnet 模式，直接返回正常端点
        if execution_mode != 'testnet':
            return self.get_binance_ws_base()
        
        # 如果已经检查过并设置了回退，直接返回缓存的端点
        if self._data_layer_ws_base_cache is not None:
            return self._data_layer_ws_base_cache
        
        # 先确保 API 端点已经检查过
        await self.get_binance_api_base_for_data_layer_async()
        
        # 获取 testnet 和 live 端点
        testnet_config = self.get('execution.testnet', {})
        testnet_ws_base = testnet_config.get('ws_base', 'wss://stream.binancefuture.com')
        live_ws_base = self.get('binance.ws_base', 'wss://fstream.binance.com')
        
        # 如果 API 端点已经回退到 live，WebSocket 也使用 live
        if self._data_layer_api_base_cache and 'testnet' not in self._data_layer_api_base_cache:
            self._data_layer_ws_base_cache = live_ws_base
            return live_ws_base
        
        # 测试 WebSocket 端点（通过测试对应的 API 端点来判断）
        testnet_api_base = testnet_config.get('api_base', 'https://testnet.binancefuture.com')
        is_testnet_available = await self._test_endpoint_connectivity(testnet_api_base, timeout=5.0)
        
        if is_testnet_available:
            self._data_layer_ws_base_cache = testnet_ws_base
            return testnet_ws_base
        else:
            self._data_layer_ws_base_cache = live_ws_base
            import logging
            logger = logging.getLogger('config')
            logger.warning(
                f"Testnet WebSocket endpoint {testnet_ws_base} is not available, "
                f"falling back to live endpoint {live_ws_base} for data layer"
            )
            return live_ws_base


# 全局配置实例
config = Config()
