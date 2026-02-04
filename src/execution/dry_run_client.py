"""
完全离线的Dry-Run客户端
不需要API密钥，模拟所有API调用，用于测试和开发
支持从配置文件读取模拟账户信息
支持使用真实交易所信息（通过API获取）
"""
import time
import aiohttp
from typing import Dict, List, Optional
from datetime import datetime, timezone

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol
from ..common.network_utils import safe_http_request, log_network_error

logger = get_logger('dry_run_client')


class DryRunBinanceClient:
    """
    完全离线的Dry-Run Binance客户端
    模拟所有API调用，不进行真实的网络请求
    用于测试和开发，不需要API密钥
    """
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, api_base: Optional[str] = None, account_id: Optional[str] = None, use_real_exchange_info: bool = False):
        """
        初始化Dry-Run客户端
        
        Args:
            api_key: API Key（可选，dry-run模式下不需要）
            api_secret: API Secret（可选，dry-run模式下不需要）
            api_base: API base url（可选，用于获取真实交易所信息）
            account_id: 账户ID（可选，用于从配置文件读取模拟账户信息）
            use_real_exchange_info: 是否使用真实交易所信息（通过API获取）
        """
        self.api_key = api_key or "DRY_RUN_KEY"
        self.api_secret = api_secret or "DRY_RUN_SECRET"
        self.api_base = api_base or config.get_binance_api_base()
        self.dry_run_mode = True
        self.account_id = account_id
        self.use_real_exchange_info = use_real_exchange_info
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 从配置文件读取模拟账户信息（如果提供了account_id）
        mock_account_info = self._load_mock_account_info(account_id)
        
        # 模拟账户数据
        self._mock_account_info = {
            'totalWalletBalance': mock_account_info.get('total_wallet_balance', 100000.0),
            'availableBalance': mock_account_info.get('available_balance', 50000.0),
            'totalMarginBalance': mock_account_info.get('total_wallet_balance', 100000.0),
            'totalUnrealizedProfit': 0.0,
            'assets': [
                {
                    'asset': 'USDT',
                    'walletBalance': mock_account_info.get('total_wallet_balance', 100000.0),
                    'availableBalance': mock_account_info.get('available_balance', 50000.0),
                }
            ]
        }
        
        # 从配置文件读取初始持仓
        initial_positions = mock_account_info.get('initial_positions', [])
        self._mock_positions: List[Dict] = []
        for pos in initial_positions:
            symbol = format_symbol(pos.get('symbol', ''))
            position_amt = float(pos.get('position_amt', 0))
            entry_price = float(pos.get('entry_price', 0))
            
            if abs(position_amt) > 1e-8:
                self._mock_positions.append({
                    'symbol': symbol,
                    'positionAmt': position_amt,
                    'entryPrice': entry_price,
                    'unRealizedProfit': 0.0,
                    'leverage': 1,
                    'marginType': 'isolated',
                })
        
        # 交易所信息（根据配置使用真实或模拟）
        if self.use_real_exchange_info:
            # 使用真实交易所信息，将在首次调用时从API获取
            self._exchange_info: Optional[Dict] = None
            self._exchange_info_loaded = False
        else:
            # 使用模拟交易所信息
            self._mock_exchange_info = {
                'symbols': [
                    {
                        'symbol': 'BTCUSDT',
                        'status': 'TRADING',
                        'baseAsset': 'BTC',
                        'quoteAsset': 'USDT',
                        'filters': [
                            {
                                'filterType': 'PRICE_FILTER',
                                'tickSize': '0.01'
                            },
                            {
                                'filterType': 'LOT_SIZE',
                                'stepSize': '0.001',
                                'minQty': '0.001'
                            }
                        ]
                    },
                    {
                        'symbol': 'ETHUSDT',
                        'status': 'TRADING',
                        'baseAsset': 'ETH',
                        'quoteAsset': 'USDT',
                        'filters': [
                            {
                                'filterType': 'PRICE_FILTER',
                                'tickSize': '0.01'
                            },
                            {
                                'filterType': 'LOT_SIZE',
                                'stepSize': '0.001',
                                'minQty': '0.001'
                            }
                        ]
                    }
                ]
            }
        
        # 订单计数器（用于生成模拟订单ID）
        self._order_id_counter = 1000000
        
        if account_id:
            if self.use_real_exchange_info:
                logger.info(f"Dry-run Binance client initialized for account {account_id} with mock account data and real exchange info")
            else:
                logger.info(f"Dry-run Binance client initialized for account {account_id} with mock data from config")
        else:
            if self.use_real_exchange_info:
                logger.info("Dry-run Binance client initialized (no API keys required, using real exchange info)")
            else:
                logger.info("Dry-run Binance client initialized (no API keys required, using default mock data)")
    
    def _load_mock_account_info(self, account_id: Optional[str]) -> Dict:
        """
        从配置文件加载模拟账户信息
        
        Args:
            account_id: 账户ID
        
        Returns:
            模拟账户信息字典
        """
        if not account_id:
            # 如果没有提供account_id，返回默认值
            return {
                'total_wallet_balance': 100000.0,
                'available_balance': 50000.0,
                'initial_positions': []
            }
        
        try:
            dry_run_config = config.get('execution.dry_run', {})
            mock_accounts = dry_run_config.get('mock_accounts', {})
            account_config = mock_accounts.get(account_id, {})
            
            if account_config:
                return {
                    'total_wallet_balance': float(account_config.get('total_wallet_balance', 100000.0)),
                    'available_balance': float(account_config.get('available_balance', 50000.0)),
                    'initial_positions': account_config.get('initial_positions', [])
                }
            else:
                logger.warning(f"Mock account config not found for {account_id}, using defaults")
                return {
                    'total_wallet_balance': 100000.0,
                    'available_balance': 50000.0,
                    'initial_positions': []
                }
        except Exception as e:
            logger.warning(f"Failed to load mock account info for {account_id}: {e}, using defaults")
            return {
                'total_wallet_balance': 100000.0,
                'available_balance': 50000.0,
                'initial_positions': []
            }
        
        # 模拟交易所信息（常用交易对）
        self._mock_exchange_info = {
            'symbols': [
                {
                    'symbol': 'BTCUSDT',
                    'status': 'TRADING',
                    'baseAsset': 'BTC',
                    'quoteAsset': 'USDT',
                    'filters': [
                        {
                            'filterType': 'PRICE_FILTER',
                            'tickSize': '0.01'
                        },
                        {
                            'filterType': 'LOT_SIZE',
                            'stepSize': '0.001',
                            'minQty': '0.001'
                        }
                    ]
                },
                {
                    'symbol': 'ETHUSDT',
                    'status': 'TRADING',
                    'baseAsset': 'ETH',
                    'quoteAsset': 'USDT',
                    'filters': [
                        {
                            'filterType': 'PRICE_FILTER',
                            'tickSize': '0.01'
                        },
                        {
                            'filterType': 'LOT_SIZE',
                            'stepSize': '0.001',
                            'minQty': '0.001'
                        }
                    ]
                }
            ]
        }
        
        # 订单计数器（用于生成模拟订单ID）
        self._order_id_counter = 1000000
        
        logger.info("Dry-run Binance client initialized (no API keys required)")
    
    async def get_account_info(self) -> Dict:
        """获取账户信息（模拟）"""
        logger.debug("DRY-RUN: get_account_info() - returning mock data")
        return self._mock_account_info.copy()
    
    async def get_positions(self) -> List[Dict]:
        """获取当前持仓（模拟）"""
        logger.debug(f"DRY-RUN: get_positions() - returning {len(self._mock_positions)} positions")
        return self._mock_positions.copy()
    
    async def get_exchange_info(self) -> Dict:
        """获取交易所信息（根据配置使用真实或模拟）"""
        if self.use_real_exchange_info:
            # 使用真实交易所信息
            if not self._exchange_info_loaded:
                await self._load_real_exchange_info()
            return self._exchange_info.copy() if self._exchange_info else {}
        else:
            # 使用模拟交易所信息
            logger.debug("DRY-RUN: get_exchange_info() - returning mock data")
            return self._mock_exchange_info.copy()
    
    async def _load_real_exchange_info(self):
        """从API加载真实交易所信息"""
        try:
            if self.session is None or self.session.closed:
                self.session = aiohttp.ClientSession()
            
            url = f"{self.api_base}/fapi/v1/exchangeInfo"
            
            exchange_info = await safe_http_request(
                self.session,
                'GET',
                url,
                max_retries=3,
                timeout=30.0,
                return_json=True,
                use_rate_limit=True  # 启用请求限流
            )
            
            self._exchange_info = exchange_info
            self._exchange_info_loaded = True
            
            logger.info(f"DRY-RUN: Loaded real exchange info from API ({len(exchange_info.get('symbols', []))} symbols)")
            
        except Exception as e:
            log_network_error(
                "Dry-run get_exchange_info",
                e,
                context={"url": url}
            )
            logger.warning(f"Failed to load real exchange info, falling back to mock data: {e}")
            # 失败时使用模拟数据
            self._exchange_info = self._get_default_mock_exchange_info()
            self._exchange_info_loaded = True
    
    def _get_default_mock_exchange_info(self) -> Dict:
        """获取默认的模拟交易所信息"""
        return {
            'symbols': [
                {
                    'symbol': 'BTCUSDT',
                    'status': 'TRADING',
                    'baseAsset': 'BTC',
                    'quoteAsset': 'USDT',
                    'filters': [
                        {
                            'filterType': 'PRICE_FILTER',
                            'tickSize': '0.01'
                        },
                        {
                            'filterType': 'LOT_SIZE',
                            'stepSize': '0.001',
                            'minQty': '0.001'
                        }
                    ]
                },
                {
                    'symbol': 'ETHUSDT',
                    'status': 'TRADING',
                    'baseAsset': 'ETH',
                    'quoteAsset': 'USDT',
                    'filters': [
                        {
                            'filterType': 'PRICE_FILTER',
                            'tickSize': '0.01'
                        },
                        {
                            'filterType': 'LOT_SIZE',
                            'stepSize': '0.001',
                            'minQty': '0.001'
                        }
                    ]
                }
            ]
        }
    
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Optional[float] = None,
        price: Optional[float] = None,
        position_side: str = 'BOTH',
        reduce_only: bool = False
    ) -> Dict:
        """
        下单（模拟，不实际下单）
        
        在dry-run模式下，模拟订单执行：
        - 生成模拟订单ID
        - 模拟订单状态（MARKET订单立即FILLED）
        - 更新模拟持仓（仅用于dry-run展示）
        """
        symbol = format_symbol(symbol)
        
        # 生成模拟订单ID
        order_id = self._order_id_counter
        self._order_id_counter += 1
        
        # 模拟订单执行（MARKET订单立即成交）
        status = 'FILLED' if order_type == 'MARKET' else 'NEW'
        
        # 更新模拟持仓（仅用于dry-run展示，不影响真实账户）
        if status == 'FILLED' and quantity:
            self._update_mock_position(symbol, side, quantity, price or 0.0)
        
        result = {
            'orderId': order_id,
            'symbol': symbol,
            'status': status,
            'side': side,
            'type': order_type,
            'executedQty': quantity if status == 'FILLED' else 0.0,
            'origQty': quantity,
            'price': price or 0.0,
            'timeInForce': 'GTC' if order_type == 'LIMIT' else None,
            'reduceOnly': reduce_only,
            'updateTime': int(time.time() * 1000),
        }
        
        logger.info(
            f"DRY-RUN: Order placed (simulated): {symbol} {side} {order_type} "
            f"qty={quantity} price={price}, orderId={order_id}, status={status}"
        )
        
        return result
    
    def _update_mock_position(self, symbol: str, side: str, quantity: float, price: float):
        """更新模拟持仓（仅用于dry-run展示）"""
        symbol = format_symbol(symbol)
        
        # 查找现有持仓
        existing_pos = None
        for pos in self._mock_positions:
            if pos.get('symbol') == symbol:
                existing_pos = pos
                break
        
        if existing_pos:
            # 更新现有持仓
            current_amt = float(existing_pos.get('positionAmt', 0))
            entry_price = float(existing_pos.get('entryPrice', 0))
            
            if side == 'BUY':
                new_amt = current_amt + quantity
            else:  # SELL
                new_amt = current_amt - quantity
            
            # 计算新的入场价格（加权平均）
            if abs(new_amt) > 1e-8:
                if current_amt * new_amt > 0:  # 同方向
                    total_value = abs(current_amt) * entry_price + quantity * price
                    new_entry_price = total_value / abs(new_amt)
                else:  # 反向或平仓
                    new_entry_price = price
                
                existing_pos['positionAmt'] = new_amt
                existing_pos['entryPrice'] = new_entry_price
            else:
                # 持仓归零，移除
                self._mock_positions.remove(existing_pos)
        else:
            # 新建持仓
            if abs(quantity) > 1e-8:
                new_pos = {
                    'symbol': symbol,
                    'positionAmt': quantity if side == 'BUY' else -quantity,
                    'entryPrice': price,
                    'unRealizedProfit': 0.0,
                    'leverage': 1,
                    'marginType': 'isolated',
                }
                self._mock_positions.append(new_pos)
    
    async def test_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Optional[float] = None,
        price: Optional[float] = None,
        position_side: str = 'BOTH',
        reduce_only: bool = False
    ) -> Dict:
        """
        测试订单（dry-run，与place_order相同，但明确标记为测试）
        """
        logger.info(f"DRY-RUN: test_order() called for {symbol} {side} {order_type}")
        # 在dry-run模式下，test_order和place_order行为相同（都不实际下单）
        return await self.place_order(symbol, side, order_type, quantity, price, position_side, reduce_only)
    
    async def cancel_order(self, symbol: str, order_id: int) -> Dict:
        """取消订单（模拟）"""
        symbol = format_symbol(symbol)
        logger.info(f"DRY-RUN: cancel_order() called for {symbol} orderId={order_id}")
        
        return {
            'orderId': order_id,
            'symbol': symbol,
            'status': 'CANCELED',
            'updateTime': int(time.time() * 1000),
        }
    
    async def get_order_status(self, symbol: str, order_id: int) -> Dict:
        """查询订单状态（模拟）"""
        symbol = format_symbol(symbol)
        logger.debug(f"DRY-RUN: get_order_status() called for {symbol} orderId={order_id}")
        
        # 模拟订单状态（假设订单已成交）
        return {
            'orderId': order_id,
            'symbol': symbol,
            'status': 'FILLED',
            'side': 'BUY',
            'type': 'MARKET',
            'executedQty': 0.1,
            'origQty': 0.1,
            'updateTime': int(time.time() * 1000),
        }
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """查询活跃订单（模拟）"""
        logger.debug(f"DRY-RUN: get_open_orders() called for symbol={symbol}")
        # 在dry-run模式下，返回空列表（假设所有订单都已成交）
        return []
    
    async def cancel_all_orders(self, symbol: str) -> List[Dict]:
        """取消指定交易对的所有活跃订单（模拟）"""
        symbol = format_symbol(symbol)
        logger.info(f"DRY-RUN: cancel_all_orders() called for {symbol}")
        return []
    
    async def change_position_mode(self, dual_side_position: bool) -> Dict:
        """设置合约持仓模式（模拟）"""
        logger.info(f"DRY-RUN: change_position_mode() called with dual_side_position={dual_side_position}")
        return {
            'code': 200,
            'msg': 'success'
        }
    
    async def change_leverage(self, symbol: str, leverage: int) -> Dict:
        """设置合约杠杆倍数（模拟）"""
        symbol = format_symbol(symbol)
        logger.info(f"DRY-RUN: change_leverage() called for {symbol} with leverage={leverage}")
        return {
            'leverage': leverage,
            'maxNotionalValue': '1000000',
            'symbol': symbol
        }
    
    async def change_margin_type(self, symbol: str, margin_type: str) -> Dict:
        """设置合约保证金模式（模拟）"""
        symbol = format_symbol(symbol)
        logger.info(f"DRY-RUN: change_margin_type() called for {symbol} with margin_type={margin_type}")
        return {
            'code': 200,
            'msg': 'success'
        }
    
    async def close(self):
        """关闭客户端"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
        logger.debug("DRY-RUN: close() called")
    
    def reset_mock_data(self):
        """重置模拟数据（用于测试）"""
        self._mock_positions = []
        self._order_id_counter = 1000000
        logger.debug("DRY-RUN: Mock data reset")
