"""
策略API模块
封装data_api、system_api和binance_client，提供统一的策略开发接口
"""
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd

from ..data.api import get_data_api
from ..system_api import get_system_api
from ..execution.binance_client import BinanceClient
from ..common.logger import get_logger
from ..common.config import config
from ..common.utils import to_system_symbol, to_exchange_symbol

logger = get_logger('strategy_api')


class StrategyAPI:
    """策略API - 统一封装数据层、系统层和执行层接口"""
    
    def __init__(self):
        """初始化策略API"""
        self.data_api = get_data_api()
        self.system_api = get_system_api()
    
    # ========== 数据查询接口 ==========
    
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
        return self.data_api.get_bar_between(begin_date_time_label, end_date_time_label, mode=mode)
    
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
        return self.data_api.get_tran_stats_between(begin_date_time_label, end_date_time_label, mode=mode)
    
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
        return self.data_api.get_funding_rate_between(begin_date_time_label, end_date_time_label)
    
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
        return self.data_api.get_premium_index_bar_between(begin_date_time_label, end_date_time_label, mode=mode)
    
    # ========== Universe查询接口 ==========
    
    def get_last_universe(self, version: str = 'v1') -> List[str]:
        """
        获取最新的Universe（可交易资产列表）
        
        Args:
            version: 版本号，如 'v1', 'v2' 等，默认为 'v1'
        
        Returns:
            交易对列表，格式: ['btc-usdt', 'eth-usdt', ...]
        """
        return self.system_api.get_last_universe(version=version)
    
    def get_universe(self, date: Optional[str] = None, version: str = 'v1') -> List[str]:
        """
        按日期获取Universe（可交易资产列表）
        
        Args:
            date: 日期字符串，格式 'YYYY-MM-DD'，如果不指定，返回最新的
            version: 版本号，如 'v1', 'v2' 等，默认为 'v1'
        
        Returns:
            交易对列表，格式: ['btc-usdt', 'eth-usdt', ...]
        """
        return self.system_api.get_universe(date=date, version=version)
    
    # ========== 账户和持仓查询接口 ==========
    
    async def get_account_info(self, exchange: str = 'binance', account: str = 'account1') -> Dict:
        """
        获取账户信息
        
        Args:
            exchange: 交易所名称（目前只支持binance）
            account: 账户ID
        
        Returns:
            账户信息字典
        """
        return await self.system_api.get_account_info(exchange, account)
    
    async def get_position_info(self, exchange: str = 'binance', account: str = 'account1') -> List[Dict]:
        """
        获取永续合约仓位信息
        
        Args:
            exchange: 交易所名称（目前只支持binance）
            account: 账户ID
        
        Returns:
            持仓信息列表
        """
        return await self.system_api.get_position_info(exchange, account)
    
    # ========== 交易执行接口 ==========
    
    def get_binance_client(self, account: str = 'account1') -> Optional[BinanceClient]:
        """
        获取Binance客户端实例（用于执行交易）
        
        Args:
            account: 账户ID
        
        Returns:
            BinanceClient实例
        """
        try:
            execution_mode = config.get('execution.mode', 'mock')
            dry_run = config.get('execution.dry_run', False)
            
            # 获取账户配置
            mode_config = config.get(f'execution.{execution_mode}', {})
            accounts_config = mode_config.get('accounts', [])
            account_config = next((acc for acc in accounts_config if acc.get('account_id') == account), None)
            
            if not account_config:
                logger.warning(f"Account {account} not found in config")
                return None
            
            # 获取API密钥
            import os
            api_key_env = f"{account.upper()}_API_KEY"
            api_secret_env = f"{account.upper()}_API_SECRET"
            
            api_key = os.getenv(api_key_env, account_config.get('api_key', ''))
            api_secret = os.getenv(api_secret_env, account_config.get('api_secret', ''))
            
            if not api_key or not api_secret:
                logger.warning(f"API keys not found for account {account}")
                return None
            
            # 根据执行模式选择API地址
            api_base = config.get_binance_api_base()
            
            return BinanceClient(
                api_key=api_key,
                api_secret=api_secret,
                api_base=api_base,
                dry_run=dry_run
            )
        except Exception as e:
            logger.error(f"Failed to get binance client for account {account}: {e}", exc_info=True)
            return None


# 全局API实例
_strategy_api: Optional[StrategyAPI] = None


def get_strategy_api() -> StrategyAPI:
    """获取策略API实例"""
    global _strategy_api
    if _strategy_api is None:
        _strategy_api = StrategyAPI()
    return _strategy_api
