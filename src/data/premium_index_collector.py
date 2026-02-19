"""
溢价指数K线数据采集模块
从Binance获取溢价指数K线数据
"""
import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import pandas as pd

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol
from ..common.network_utils import safe_http_request, log_network_error

logger = get_logger('premium_index_collector')


class PremiumIndexCollector:
    """溢价指数K线采集器"""
    
    def __init__(self):
        """初始化溢价指数采集器"""
        # 数据层支持testnet回退到live
        self.api_base = config.get_binance_api_base_for_data_layer()
    
    async def fetch_premium_index_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = '5m',
        max_retries: int = 3
    ) -> List[Dict]:
        """
        从Binance获取溢价指数K线数据
        
        Args:
            symbol: 交易对（如BTCUSDT）
            start_time: 开始时间
            end_time: 结束时间
            interval: K线周期，默认5m（5分钟）
            max_retries: 最大重试次数
        
        Returns:
            K线数据列表
        """
        # Binance溢价指数K线API: /fapi/v1/premiumIndexKlines
        url = f"{self.api_base}/fapi/v1/premiumIndexKlines"
        
        # 优化：使用列表但限制大小，避免内存累积
        all_klines = []
        max_klines_limit = 50000  # 最多保留50000条（约17天数据），超过则分批处理
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                params = {
                    'symbol': format_symbol(symbol).upper(),
                    'interval': interval,
                    'startTime': int(start_time.timestamp() * 1000),
                    'endTime': int(end_time.timestamp() * 1000),
                    'limit': 1500  # Binance API限制
                }
                
                async with aiohttp.ClientSession() as session:
                    current_start_time = start_time
                    while current_start_time < end_time:
                        # 更新参数
                        params['startTime'] = int(current_start_time.timestamp() * 1000)
                        params['endTime'] = int(end_time.timestamp() * 1000)
                        
                        # 使用safe_http_request，自动处理限流和418错误
                        try:
                            data = await safe_http_request(
                                session,
                                'GET',
                                url,
                                params=params,
                                max_retries=3,
                                timeout=60.0,
                                return_json=True,
                                use_rate_limit=True  # 启用请求限流
                            )
                            
                            if not data:
                                break
                            
                            # 优化：检查列表大小，避免内存累积过多
                            if len(all_klines) + len(data) > max_klines_limit:
                                logger.warning(
                                    f"Premium index klines for {symbol} exceeds limit ({max_klines_limit}), "
                                    f"truncating to latest data"
                                )
                                # 只保留最新的数据
                                all_klines = all_klines[-max_klines_limit:]
                            
                            all_klines.extend(data)
                            
                            # 如果返回的数据少于limit，说明已经获取完所有数据
                            if len(data) < params['limit']:
                                break
                            
                            # 更新startTime为最后一条数据的时间+1
                            last_time = data[-1][0]  # 第一条是open_time
                            current_start_time = datetime.fromtimestamp(
                                (last_time + 1) / 1000, tz=timezone.utc
                            )
                            
                            # 避免请求过快（safe_http_request已经内置限流，这里再增加一点延迟）
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            error_str = str(e)
                            # 检查是否是418错误（IP被封禁）
                            if '418' in error_str or 'banned' in error_str.lower():
                                # IP被封禁，直接抛出，让外层重试逻辑处理
                                raise
                            # 其他错误，记录并抛出，让外层重试逻辑处理
                            logger.warning(f"Failed to fetch premium index klines (batch): {e}")
                            raise
                
                # 成功获取数据
                logger.debug(f"Fetched {len(all_klines)} premium index klines for {symbol}")
                return all_klines
                
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # 指数退避
                    logger.warning(f"Failed to fetch premium index klines for {symbol} (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    log_network_error(
                        f"获取溢价指数K线数据",
                        e,
                        context={"symbol": symbol, "url": url}
                    )
        
        return []
    
    def convert_to_dataframe(self, klines: List[Dict], symbol: str) -> pd.DataFrame:
        """
        将Binance溢价指数K线数据转换为DataFrame
        
        Args:
            klines: Binance API返回的K线数据列表
            symbol: 交易对
        
        Returns:
            DataFrame，包含溢价指数K线数据
        """
        if not klines:
            return pd.DataFrame()
        
        # Binance溢价指数K线格式：
        # [open_time, open, high, low, close, volume, close_time, quote_volume, count, taker_buy_base_volume, taker_buy_quote_volume, ignore]
        records = []
        for k in klines:
            records.append({
                'symbol': format_symbol(symbol),
                'open_time': pd.Timestamp(k[0], unit='ms', tz='UTC'),
                'open': float(k[1]),
                'high': float(k[2]),
                'low': float(k[3]),
                'close': float(k[4]),
                'volume': float(k[5]),
                'close_time': pd.Timestamp(k[6], unit='ms', tz='UTC'),
                'quote_volume': float(k[7]),
                'trade_count': int(k[8]),
                'taker_buy_base_volume': float(k[9]),
                'taker_buy_quote_volume': float(k[10]),
            })
        
        df = pd.DataFrame(records)
        
        # 计算time_label（1-288）
        if 'open_time' in df.columns:
            df['time_lable'] = df['open_time'].apply(
                lambda x: int((x.hour * 60 + x.minute) / 5) + 1
            )
        
        return df
    
    async def fetch_premium_index_klines_bulk(
        self,
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        interval: str = '5m',
        max_concurrent: int = 3  # 降低并发数，避免触发限流
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取多个交易对的溢价指数K线数据
        
        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            interval: K线周期，默认5m
            max_concurrent: 最大并发数
        
        Returns:
            Dict[symbol, DataFrame]，每个交易对的溢价指数K线数据
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results = {}
        
        async def fetch_one(symbol: str):
            async with semaphore:
                try:
                    klines = await self.fetch_premium_index_klines(
                        symbol=symbol,
                        start_time=start_time,
                        end_time=end_time,
                        interval=interval
                    )
                    if klines:
                        df = self.convert_to_dataframe(klines, symbol)
                        if not df.empty:
                            results[symbol] = df
                except Exception as e:
                    logger.error(f"Failed to fetch premium index klines for {symbol}: {e}", exc_info=True)
        
        tasks = [fetch_one(symbol) for symbol in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        logger.info(f"Fetched premium index klines for {len(results)}/{len(symbols)} symbols")
        return results


# 全局实例
_premium_index_collector: Optional[PremiumIndexCollector] = None


def get_premium_index_collector() -> PremiumIndexCollector:
    """获取溢价指数采集器实例"""
    global _premium_index_collector
    if _premium_index_collector is None:
        _premium_index_collector = PremiumIndexCollector()
    return _premium_index_collector
