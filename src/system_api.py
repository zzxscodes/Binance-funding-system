"""
系统API模块
提供系统级查询接口，包括Universe、账户、持仓、订单等查询
"""
from typing import Dict, List, Optional
from datetime import datetime

from typing import Dict, List, Optional
from datetime import datetime

from .common.config import config
from .common.logger import get_logger
from .data.api import get_data_api
from .data.universe_manager import get_universe_manager
from .execution.binance_client import BinanceClient
from .execution.dry_run_client import DryRunBinanceClient
from .common.utils import to_system_symbol, to_exchange_symbol

logger = get_logger('system_api')


class SystemAPI:
    """系统API服务"""
    
    def __init__(self):
        """初始化系统API"""
        self.data_api = get_data_api()
        self.universe_manager = get_universe_manager()
    
    def _get_client(self, exchange: str = 'binance', account: str = 'account1') -> Optional[BinanceClient]:
        """
        获取交易客户端实例
        
        Args:
            exchange: 交易所名称（目前只支持binance）
            account: 账户ID
        
        Returns:
            BinanceClient或DryRunBinanceClient实例
        """
        try:
            execution_mode = config.get('execution.mode', 'dry-run')
            dry_run = config.get('execution.dry_run', False)
            use_api_keys = False
            use_real_exchange_info = False
            
            # 获取账户配置
            accounts_config = config.get('execution.accounts', [])
            account_config = next((acc for acc in accounts_config if acc.get('account_id') == account), None)
            
            if execution_mode == 'dry-run' and not use_api_keys:
                # 完全离线dry-run模式
                return DryRunBinanceClient(
                    account_id=account,
                    use_real_exchange_info=use_real_exchange_info
                )
            else:
                # 需要API密钥的模式
                import os
                
                # 根据execution_mode从不同的配置路径读取API keys
                api_key = None
                api_secret = None
                
                if execution_mode == 'testnet':
                    # testnet模式：从 execution.testnet.accounts 中读取账户的API keys
                    testnet_config = config.get('execution.testnet', {})
                    testnet_accounts = testnet_config.get('accounts', [])
                    testnet_account_config = next((acc for acc in testnet_accounts if acc.get('account_id') == account), None)
                    
                    # 优先使用环境变量，如果不存在则从配置文件读取
                    api_key_env = f"{account.upper()}_TESTNET_API_KEY"
                    api_secret_env = f"{account.upper()}_TESTNET_API_SECRET"
                    api_key = os.getenv(api_key_env, testnet_account_config.get('api_key', '') if testnet_account_config else '')
                    api_secret = os.getenv(api_secret_env, testnet_account_config.get('api_secret', '') if testnet_account_config else '')
                elif execution_mode == 'live':
                    # live模式：从 execution.live.accounts 中读取账户的API keys
                    live_config = config.get('execution.live', {})
                    live_accounts = live_config.get('accounts', [])
                    live_account_config = next((acc for acc in live_accounts if acc.get('account_id') == account), None)
                    
                    # 优先使用环境变量，如果不存在则从配置文件读取
                    api_key_env = f"{account.upper()}_API_KEY"
                    api_secret_env = f"{account.upper()}_API_SECRET"
                    api_key = os.getenv(api_key_env, live_account_config.get('api_key', '') if live_account_config else '')
                    api_secret = os.getenv(api_secret_env, live_account_config.get('api_secret', '') if live_account_config else '')
                else:
                    # 其他模式：从 execution.accounts 读取（兼容旧配置）
                    api_key_env = f"{account.upper()}_API_KEY"
                    api_secret_env = f"{account.upper()}_API_SECRET"
                    accounts_config = config.get('execution.accounts', [])
                    account_config = next((acc for acc in accounts_config if acc.get('account_id') == account), None)
                    api_key = os.getenv(api_key_env, account_config.get('api_key', '') if account_config else '')
                    api_secret = os.getenv(api_secret_env, account_config.get('api_secret', '') if account_config else '')
                
                if not api_key or not api_secret:
                    logger.warning(f"API keys not found for account {account} in {execution_mode} mode, using dry-run client")
                    return DryRunBinanceClient(
                        account_id=account,
                        use_real_exchange_info=use_real_exchange_info
                    )
                
                if execution_mode == 'dry-run':
                    return DryRunBinanceClient(
                        api_key=api_key,
                        api_secret=api_secret,
                        account_id=account,
                        use_real_exchange_info=use_real_exchange_info
                    )
                else:
                    # 根据 execution.mode 自动选择正确的 API 地址
                    api_base = config.get_binance_api_base()
                    if execution_mode == 'testnet':
                        # testnet模式：api_key和api_secret已经在上面读取了
                        return BinanceClient(
                            api_key=api_key,
                            api_secret=api_secret,
                            api_base=api_base,
                            dry_run=dry_run
                        )
                    elif execution_mode == 'live':
                        # live模式：从 execution.live.accounts 中读取账户的API keys
                        live_config = config.get('execution.live', {})
                        live_accounts = live_config.get('accounts', [])
                        live_account_config = next((acc for acc in live_accounts if acc.get('account_id') == account), None)
                        
                        # 优先使用环境变量，如果不存在则从配置文件读取
                        api_key_env = f"{account.upper()}_API_KEY"
                        api_secret_env = f"{account.upper()}_API_SECRET"
                        api_key = os.getenv(api_key_env, live_account_config.get('api_key', '') if live_account_config else '')
                        api_secret = os.getenv(api_secret_env, live_account_config.get('api_secret', '') if live_account_config else '')
                    
                        if not api_key or not api_secret:
                            logger.warning(f"API keys not found for {exchange}/{account} in live mode, using dry-run client")
                            return DryRunBinanceClient(
                                account_id=account,
                                use_real_exchange_info=use_real_exchange_info
                            )
                        
                        return BinanceClient(
                            api_key=api_key,
                            api_secret=api_secret,
                            api_base=api_base,
                            dry_run=dry_run
                        )
                    else:
                        # 其他模式（mock等）
                        return BinanceClient(
                            api_key=api_key,
                            api_secret=api_secret,
                            api_base=api_base,
                            dry_run=dry_run
                        )
        except Exception as e:
            logger.error(f"Failed to get client for {exchange}/{account}: {e}", exc_info=True)
            return None
    
    def get_universe(self, date: Optional[str] = None, version: str = 'v1') -> List[str]:
        """
        按日期获取Universe（可交易资产列表）
        
        Args:
            date: 日期字符串，格式 'YYYY-MM-DD'，如果不指定，返回最新的
            version: 版本号，如 'v1', 'v2' 等，默认为 'v1'
        
        Returns:
            交易对列表，格式: ['btc-usdt', 'eth-usdt', ...]
        """
        try:
            from datetime import datetime
            from .common.utils import parse_time_str
            
            target_date = None
            if date:
                try:
                    # 解析日期字符串
                    target_date = datetime.strptime(date, '%Y-%m-%d')
                except ValueError:
                    logger.error(f"Invalid date format: {date}, expected 'YYYY-MM-DD'")
                    return []
            
            universe = self.universe_manager.load_universe(date=target_date, version=version)
            # 转换为系统格式（小写+连字符）
            return [to_system_symbol(s) for s in universe]
        except Exception as e:
            logger.error(f"Failed to get universe: {e}", exc_info=True)
            return []
    
    def get_last_universe(self, version: str = 'v1') -> List[str]:
        """
        获取当前所有需要交易的交易对
        
        Args:
            version: 版本号，如 'v1', 'v2' 等，默认为 'v1'
        
        Returns:
            交易对列表，格式: ['btc-usdt', 'eth-usdt', ...]
        """
        try:
            universe = self.universe_manager.load_universe(version=version)
            # 转换为系统格式（小写+连字符）
            return [to_system_symbol(s) for s in universe]
        except Exception as e:
            logger.error(f"Failed to get last universe: {e}", exc_info=True)
            return []
    
    def get_all_uid_info(self) -> Dict[str, Dict]:
        """
        获取当前所有需要交易的交易对信息
        
        Returns:
            Dict[symbol, info_dict]，格式: {'btc-usdt': info_dict, 'eth-usdt': info_dict, ...}
        """
        try:
            universe = self.universe_manager.load_universe()
            result = {}
            
            # 加载最新的universe文件获取详细信息
            from pathlib import Path
            import csv
            
            universe_dir = Path(config.get('data.universe_directory', 'data/universe'))
            universe_dirs = sorted(universe_dir.iterdir(), reverse=True)
            
            universe_file = None
            for date_dir in universe_dirs:
                if date_dir.is_dir():
                    potential_file = date_dir / 'universe.csv'
                    if potential_file.exists():
                        universe_file = potential_file
                        break
            
            if universe_file:
                with open(universe_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        symbol = row.get('symbol', '')
                        sys_symbol = to_system_symbol(symbol)
                        if sys_symbol in [to_system_symbol(s) for s in universe]:
                            result[sys_symbol] = {
                                'symbol': sys_symbol,
                                'exchange_symbol': symbol,
                                'status': row.get('status', ''),
                                'baseAsset': row.get('baseAsset', ''),
                                'quoteAsset': row.get('quoteAsset', ''),
                                'contractType': row.get('contractType', ''),
                                'pricePrecision': int(row.get('pricePrecision', 8)),
                                'quantityPrecision': int(row.get('quantityPrecision', 8)),
                                'tickSize': float(row.get('tickSize', 0.01)),
                                'stepSize': float(row.get('stepSize', 0.01)),
                            }
            
            return result
        except Exception as e:
            logger.error(f"Failed to get all uid info: {e}", exc_info=True)
            return {}
    
    async def get_account_info(self, exchange: str = 'binance', account: str = 'account1') -> Dict:
        """
        获取账户信息
        
        Args:
            exchange: 交易所名称（目前只支持binance）
            account: 账户ID
        
        Returns:
            账户信息字典，包含余额、钱包等信息
        """
        try:
            client = self._get_client(exchange, account)
            if not client:
                return {
                    'error': f'Failed to get client for {exchange}/{account}'
                }
            
            account_info = await client.get_account_info()
            
            # 格式化返回结果
            return {
                'exchange': exchange,
                'account': account,
                'total_wallet_balance': float(account_info.get('totalWalletBalance', 0)),
                'available_balance': float(account_info.get('availableBalance', 0)),
                'total_margin_balance': float(account_info.get('totalMarginBalance', 0)),
                'total_unrealized_profit': float(account_info.get('totalUnrealizedProfit', 0)),
                'assets': account_info.get('assets', []),
                'raw_data': account_info  # 保留原始数据
            }
        except Exception as e:
            logger.error(f"Failed to get account info for {exchange}/{account}: {e}", exc_info=True)
            return {
                'error': str(e),
                'exchange': exchange,
                'account': account
            }
    
    async def get_position_info(self, exchange: str = 'binance', account: str = 'account1') -> List[Dict]:
        """
        获取永续合约仓位信息
        
        Args:
            exchange: 交易所名称（目前只支持binance）
            account: 账户ID
        
        Returns:
            持仓信息列表
        """
        try:
            client = self._get_client(exchange, account)
            if not client:
                return []
            
            positions = await client.get_positions()
            
            # 格式化返回结果
            result = []
            for pos in positions:
                result.append({
                    'symbol': pos.get('symbol', ''),
                    'system_symbol': to_system_symbol(pos.get('symbol', '')),
                    'position_amt': float(pos.get('positionAmt', 0)),
                    'entry_price': float(pos.get('entryPrice', 0)),
                    'unrealized_profit': float(pos.get('unRealizedProfit', 0)),
                    'leverage': int(pos.get('leverage', 1)),
                    'margin_type': pos.get('marginType', ''),
                    'raw_data': pos  # 保留原始数据
                })
            
            return result
        except Exception as e:
            logger.error(f"Failed to get position info for {exchange}/{account}: {e}", exc_info=True)
            return []
    
    async def get_active_order(self, exchange: str = 'binance', account: str = 'account1', symbol: Optional[str] = None) -> List[int]:
        """
        获取当前活跃订单ID列表
        
        Args:
            exchange: 交易所名称（目前只支持binance）
            account: 账户ID
            symbol: 交易对（可选），如果不指定则返回所有交易对的活跃订单
        
        Returns:
            活跃订单ID列表: [id1, id2, ...]
        """
        try:
            client = self._get_client(exchange, account)
            if not client:
                return []
            
            orders = await client.get_open_orders(symbol=symbol)
            
            # 提取订单ID
            order_ids = []
            for order in orders:
                order_id = order.get('orderId')
                if order_id:
                    order_ids.append(int(order_id))
            
            return order_ids
        except Exception as e:
            logger.error(f"Failed to get active orders for {exchange}/{account}: {e}", exc_info=True)
            return []
    
    async def cancel_all_orders(self, exchange: str = 'binance', account: str = 'account1', symbol: Optional[str] = None) -> Dict:
        """
        取消该用户账户下所有活跃订单
        
        Args:
            exchange: 交易所名称（目前只支持binance）
            account: 账户ID
            symbol: 交易对（可选），如果不指定则取消所有交易对的订单
        
        Returns:
            取消结果
        """
        try:
            client = self._get_client(exchange, account)
            if not client:
                return {
                    'error': f'Failed to get client for {exchange}/{account}',
                    'cancelled_count': 0
                }
            
            if symbol:
                # 取消指定交易对的所有订单
                result = await client.cancel_all_orders(symbol)
                return {
                    'exchange': exchange,
                    'account': account,
                    'symbol': symbol,
                    'cancelled_count': len(result) if isinstance(result, list) else 1,
                    'result': result
                }
            else:
                # 取消所有交易对的所有订单
                universe = self.get_last_universe()
                total_cancelled = 0
                results = {}
                
                for sys_symbol in universe:
                    ex_symbol = to_exchange_symbol(sys_symbol)
                    try:
                        result = await client.cancel_all_orders(ex_symbol)
                        cancelled_count = len(result) if isinstance(result, list) else 1
                        total_cancelled += cancelled_count
                        results[sys_symbol] = cancelled_count
                    except Exception as e:
                        logger.warning(f"Failed to cancel orders for {sys_symbol}: {e}")
                        results[sys_symbol] = 0
                
                return {
                    'exchange': exchange,
                    'account': account,
                    'total_cancelled': total_cancelled,
                    'results_by_symbol': results
                }
        except Exception as e:
            logger.error(f"Failed to cancel all orders for {exchange}/{account}: {e}", exc_info=True)
            return {
                'error': str(e),
                'exchange': exchange,
                'account': account,
                'cancelled_count': 0
            }


# 全局API实例
_system_api: Optional[SystemAPI] = None


def get_system_api() -> SystemAPI:
    """获取系统API实例"""
    global _system_api
    if _system_api is None:
        _system_api = SystemAPI()
    return _system_api
