"""
模拟逐笔成交数据采集器
在dry-run模式下使用，生成模拟的逐笔成交数据，不连接真实WebSocket
"""
import asyncio
import random
import time
from typing import List, Dict, Optional, Callable
from datetime import datetime, timezone
from collections import defaultdict
import pandas as pd

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol

logger = get_logger('mock_collector')


class MockTradeCollector:
    """
    模拟逐笔成交数据采集器
    生成模拟的逐笔成交数据，用于dry-run模式
    """
    
    def __init__(self, symbols: List[str], on_trade_callback: Optional[Callable] = None):
        """
        初始化模拟采集器
        
        Args:
            symbols: 交易对列表（大写，如 ['BTCUSDT', 'ETHUSDT']）
            on_trade_callback: 逐笔成交数据回调函数 callback(symbol: str, trade: dict)
        """
        self.symbols = [format_symbol(s) for s in symbols]
        self.on_trade_callback = on_trade_callback
        
        # 模拟价格（每个交易对的初始价格）
        self.base_prices = {
            'BTCUSDT': 50000.0,
            'ETHUSDT': 3000.0,
        }
        
        # 为每个交易对设置初始价格（如果不在base_prices中，使用默认值）
        for symbol in self.symbols:
            if symbol not in self.base_prices:
                self.base_prices[symbol] = 100.0
        
        # 当前价格（会随机波动）
        self.current_prices = self.base_prices.copy()
        
        # 统计信息
        self.stats = {
            'trades_received': defaultdict(int),
            'start_time': time.time(),
            'last_message_time': defaultdict(float),
            'reconnect_count': 0,
        }
        
        self.running = False
        self.trade_task: Optional[asyncio.Task] = None
        
        # 交易生成间隔（秒）
        self.trade_interval = 0.1  # 每100ms生成一个交易
    
    async def _generate_mock_trade(self, symbol: str) -> Dict:
        """
        生成模拟的逐笔成交数据
        
        Args:
            symbol: 交易对
        
        Returns:
            模拟的成交数据
        """
        # 价格随机波动（±0.1%）
        price_change = random.uniform(-0.001, 0.001)
        self.current_prices[symbol] *= (1 + price_change)
        
        # 确保价格不会太离谱
        base_price = self.base_prices.get(symbol, 100.0)
        if self.current_prices[symbol] < base_price * 0.5:
            self.current_prices[symbol] = base_price * 0.5
        elif self.current_prices[symbol] > base_price * 1.5:
            self.current_prices[symbol] = base_price * 1.5
        
        price = self.current_prices[symbol]
        
        # 随机生成数量
        if symbol == 'BTCUSDT':
            qty = random.uniform(0.001, 0.1)  # BTC数量
        elif symbol == 'ETHUSDT':
            qty = random.uniform(0.01, 1.0)  # ETH数量
        else:
            qty = random.uniform(0.1, 10.0)  # 其他交易对
        
        quote_qty = price * qty
        is_buyer_maker = random.choice([True, False])
        
        timestamp_ms = int(time.time() * 1000)
        trade_id = int(time.time() * 1000000) % 1000000000  # 模拟trade ID
        
        # 构建标准化交易记录（与真实TradeCollector格式一致）
        trade_record = {
            'symbol': format_symbol(symbol),
            'tradeId': trade_id,
            'price': price,
            'qty': qty,
            'quoteQty': quote_qty,
            'isBuyerMaker': is_buyer_maker,
            'ts': pd.Timestamp(timestamp_ms, unit='ms', tz='UTC'),
            'ts_ms': timestamp_ms,
            'ts_us': timestamp_ms * 1000,
        }
        
        return trade_record
    
    async def _trade_generator_loop(self):
        """交易生成循环"""
        logger.info(f"Mock trade collector started for {len(self.symbols)} symbols")
        
        while self.running:
            try:
                # 为每个交易对生成一个交易
                for symbol in self.symbols:
                    if not self.running:
                        break
                    
                    trade = await self._generate_mock_trade(symbol)
                    
                    # 更新统计
                    self.stats['trades_received'][symbol] += 1
                    self.stats['last_message_time'][symbol] = time.time()
                    
                    # 调用回调函数
                    if self.on_trade_callback:
                        try:
                            await self.on_trade_callback(symbol, trade)
                        except Exception as e:
                            logger.error(f"Error in trade callback for {symbol}: {e}", exc_info=True)
                
                # 等待一段时间后继续
                await asyncio.sleep(self.trade_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in mock trade generator: {e}", exc_info=True)
                await asyncio.sleep(1)
        
        logger.info("Mock trade collector stopped")
    
    async def update_symbols(self, new_symbols: List[str]):
        """
        更新监听的交易对列表
        
        Args:
            new_symbols: 新的交易对列表
        """
        old_symbols = set(self.symbols)
        new_symbols_formatted = [format_symbol(s) for s in new_symbols]
        new_symbols_set = set(new_symbols_formatted)
        
        if old_symbols != new_symbols_set:
            logger.info(f"Updating symbols: {len(old_symbols)} -> {len(new_symbols_set)}")
            self.symbols = new_symbols_formatted
            
            # 为新交易对设置初始价格
            for symbol in new_symbols_formatted:
                if symbol not in self.current_prices:
                    # 尝试从base_prices获取，否则使用默认值
                    if symbol in self.base_prices:
                        self.current_prices[symbol] = self.base_prices[symbol]
                    else:
                        self.current_prices[symbol] = 100.0
                        self.base_prices[symbol] = 100.0
    
    async def start(self):
        """启动模拟采集器"""
        if self.running:
            logger.warning("Mock trade collector is already running")
            return
        
        if not self.symbols:
            logger.warning("No symbols to collect, mock collector not started")
            return
        
        self.running = True
        self.trade_task = asyncio.create_task(self._trade_generator_loop())
        logger.info(f"Mock trade collector started for {len(self.symbols)} symbols")
    
    async def stop(self):
        """停止模拟采集器"""
        logger.info("Stopping mock trade collector...")
        self.running = False
        
        if self.trade_task:
            self.trade_task.cancel()
            try:
                await self.trade_task
            except asyncio.CancelledError:
                logger.debug("Mock trade collector task cancelled")
        
        logger.info("Mock trade collector stopped")
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        elapsed = time.time() - self.stats['start_time']
        total_trades = sum(self.stats['trades_received'].values())
        
        return {
            'total_trades': total_trades,
            'trades_per_second': total_trades / elapsed if elapsed > 0 else 0,
            'trades_by_symbol': dict(self.stats['trades_received']),
            'elapsed_seconds': elapsed,
        }
