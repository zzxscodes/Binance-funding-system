"""
资金费率采集器
从Binance API获取历史资金费率数据
"""
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Callable
from pathlib import Path

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol
from ..common.network_utils import safe_http_request, log_network_error

logger = get_logger('funding_rate_collector')


class FundingRateCollector:
    """资金费率采集器"""
    
    def __init__(self, api_base: Optional[str] = None):
        """
        初始化资金费率采集器
        
        Args:
            api_base: Binance API base URL（可选，如果不指定则根据execution.mode自动选择）
        """
        # 如果没有指定api_base，根据执行模式自动选择（数据层支持testnet回退到live）
        if api_base is None:
            api_base = config.get_binance_api_base_for_data_layer()
            execution_mode = config.get('execution.mode', 'mock')
            logger.info(f"FundingRateCollector initialized in {execution_mode.upper()} mode, using API: {api_base}")
        
        self.api_base = api_base
        self.running = False
        self._consecutive_failures = 0  # 连续失败次数
        self._max_consecutive_failures = 3  # 最大连续失败次数，超过后切换端点（降低阈值，更快切换）
        self._original_api_base = api_base  # 保存原始端点
        self._last_403_time = 0  # 最后一次403错误的时间戳
        self._403_count = 0  # 403错误计数
    
    async def fetch_funding_rate_history(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        max_retries: int = 3
    ) -> pd.DataFrame:
        """
        从Binance API获取历史资金费率数据
        
        Args:
            symbol: 交易对（如BTCUSDT）
            start_time: 开始时间
            end_time: 结束时间
            max_retries: 最大重试次数
        
        Returns:
            资金费率DataFrame，包含字段：
            - symbol: 交易对
            - fundingTime: 资金费率时间戳（datetime）
            - fundingRate: 资金费率
            - markPrice: 标记价格
        """
        symbol = format_symbol(symbol)
        # 优化：使用列表但限制大小，避免内存累积
        all_rates = []
        max_rates_limit = 10000  # 最多保留10000条，超过则分批处理
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                # 每次重试前都重新读取api_base（支持运行时切换，确保立即生效）
                current_api_base = self.api_base
                url = f"{current_api_base}/fapi/v1/fundingRate"
                
                # Binance API限制：每次最多返回1000条
                limit = 1000
                current_start_time = start_time
                
                async with aiohttp.ClientSession() as session:
                    while current_start_time < end_time:
                        params = {
                            'symbol': symbol,
                            'startTime': int(current_start_time.timestamp() * 1000),
                            'endTime': int(end_time.timestamp() * 1000),
                            'limit': limit
                        }
                        
                        try:
                            # 每次请求前都重新读取api_base，确保使用最新端点（支持运行时切换）
                            current_api_base = self.api_base
                            url = f"{current_api_base}/fapi/v1/fundingRate"
                            
                            data = await safe_http_request(
                                session,
                                'GET',
                                url,
                                params=params,
                                max_retries=3,
                                timeout=30.0,
                                return_json=True,
                                use_rate_limit=True  # 启用请求限流
                            )
                            
                            if not data:
                                break
                            
                            # 优化：检查列表大小，避免内存累积过多
                            if len(all_rates) + len(data) > max_rates_limit:
                                logger.warning(
                                    f"Funding rate data for {symbol} exceeds limit ({max_rates_limit}), "
                                    f"truncating to latest data"
                                )
                                # 只保留最新的数据
                                all_rates = all_rates[-max_rates_limit:]
                            
                            all_rates.extend(data)
                            
                            # 如果返回的数据少于limit，说明已经获取完所有数据
                            if len(data) < limit:
                                break
                            
                            # 更新startTime为最后一条数据的时间+1
                            # 资金费率每8小时一次，所以最后一条的时间戳
                            last_funding_time = data[-1].get('fundingTime', 0)
                            if last_funding_time > 0:
                                current_start_time = datetime.fromtimestamp(
                                    last_funding_time / 1000, tz=timezone.utc
                                ) + timedelta(seconds=1)
                            else:
                                break
                            
                            # 检查端点是否已切换（支持运行时切换）
                            if self.api_base != current_api_base:
                                current_api_base = self.api_base
                                url = f"{current_api_base}/fapi/v1/fundingRate"
                                logger.info(f"Funding rate collector endpoint switched to: {current_api_base} during fetch")
                            
                            # 避免请求过快（使用限流器，这里额外增加延迟以确保安全）
                            # safe_http_request已经内置了限流，这里再增加一点延迟作为额外保护
                            # 根据官方文档，funding rate API请求权重仅为1，延迟可以较小
                            # 切换到live端点后，适当增加延迟避免触发403错误
                            # 如果最近有403错误，增加延迟
                            import time
                            current_time = time.time()
                            if self._last_403_time > 0 and (current_time - self._last_403_time) < 60:
                                # 最近1分钟内有403错误，大幅增加延迟
                                delay_time = 5.0
                            else:
                                # live端点使用更长的延迟，避免触发限流
                                delay_time = 3.0 if 'testnet' not in self.api_base else 0.5
                            await asyncio.sleep(delay_time)
                            
                        except Exception as e:
                            error_str = str(e)
                            import time
                            current_time = time.time()
                            
                            # 如果是网络超时或连接错误，记录警告但继续重试
                            if 'timeout' in error_str.lower() or 'connect' in error_str.lower() or '信号灯' in error_str:
                                logger.warning(f"Network error fetching funding rate for {symbol} (batch): {e}")
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(2 ** attempt)
                                    continue
                                else:
                                    # 最后一次尝试失败，返回空DataFrame而不是抛出异常
                                    logger.error(f"Failed to fetch funding rate for {symbol} after {max_retries} attempts: {e}")
                                    return pd.DataFrame()
                            # 处理403错误（限流或权限问题）
                            elif '403' in error_str or 'Forbidden' in error_str:
                                self._last_403_time = current_time
                                self._403_count += 1
                                logger.warning(f"403 Forbidden error for {symbol}, count: {self._403_count}")
                                
                                # 如果403错误过多，且在使用testnet端点，尝试切换到live端点
                                if self._403_count >= 3 and 'testnet' in self.api_base:
                                    await self._try_switch_to_live_endpoint()
                                
                                # 403错误需要更长的延迟
                                wait_time = min(5.0 * (2 ** attempt), 30.0)  # 最多等待30秒
                                if attempt < max_retries - 1:
                                    logger.warning(f"Waiting {wait_time:.1f}s before retry due to 403 error...")
                                    await asyncio.sleep(wait_time)
                                    continue
                                else:
                                    logger.error(f"Failed to fetch funding rate for {symbol} after {max_retries} attempts due to 403: {e}")
                                    return pd.DataFrame()
                            else:
                                logger.warning(f"Failed to fetch funding rate for {symbol} (batch): {e}")
                                # 如果是502/503错误，增加连续失败计数
                                if '502' in error_str or '503' in error_str or 'Bad Gateway' in error_str:
                                    self._consecutive_failures += 1
                                    # 如果连续失败次数过多，尝试切换到live端点
                                    if self._consecutive_failures >= self._max_consecutive_failures:
                                        await self._try_switch_to_live_endpoint()
                                
                                if attempt < max_retries - 1:
                                    await asyncio.sleep(2 ** attempt)
                                    continue
                                else:
                                    raise
                
                # 成功获取数据，转换为DataFrame
                if all_rates:
                    df = pd.DataFrame(all_rates)
                    
                    # 转换时间戳为datetime
                    if 'fundingTime' in df.columns:
                        df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms', utc=True)
                    
                    # 确保字段名一致
                    df['symbol'] = symbol
                    
                    # 选择需要的字段
                    columns = ['symbol', 'fundingTime', 'fundingRate', 'markPrice']
                    available_columns = [col for col in columns if col in df.columns]
                    df = df[available_columns]
                    
                    # 去重并按时间排序
                    if 'fundingTime' in df.columns:
                        df = df.drop_duplicates(subset=['fundingTime'], keep='last')
                        df = df.sort_values('fundingTime').reset_index(drop=True)
                    
                    logger.info(f"Fetched {len(df)} funding rates for {symbol} from {start_time.date()} to {end_time.date()}")
                    return df
                else:
                    logger.warning(f"No funding rate data found for {symbol} in the specified time range")
                    return pd.DataFrame()
                
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Failed to fetch funding rate for {symbol} (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch funding rate for {symbol} after {max_retries} attempts: {e}")
                    log_network_error(
                        f"获取资金费率数据({symbol})",
                        e,
                        context={"url": url, "symbol": symbol, "start_time": start_time, "end_time": end_time}
                    )
        
        return pd.DataFrame()
    
    async def fetch_funding_rates_bulk(
        self,
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        max_concurrent: int = 1  # 降低到1，避免触发403限流
    ) -> Dict[str, pd.DataFrame]:
        """
        批量获取多个交易对的历史资金费率数据（并发）
        
        Args:
            symbols: 交易对列表
            start_time: 开始时间
            end_time: 结束时间
            max_concurrent: 最大并发数
        
        Returns:
            Dict[symbol, DataFrame]，每个DataFrame包含该交易对的历史资金费率
        """
        if not symbols:
            return {}
        
        result = {}
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(symbol: str):
            async with semaphore:
                try:
                    # 每次调用前检查端点是否已切换（确保使用最新的端点）
                    # fetch_funding_rate_history内部也会在每次请求前检查，但这里也检查一次确保
                    df = await self.fetch_funding_rate_history(symbol, start_time, end_time)
                    # 如果成功获取数据，重置连续失败计数和403计数
                    if not df.empty:
                        self._consecutive_failures = 0
                        self._403_count = 0
                        self._last_403_time = 0
                    return symbol, df
                except Exception as e:
                    logger.error(f"Failed to fetch funding rate for {symbol}: {e}", exc_info=True)
                    return symbol, pd.DataFrame()
        
        tasks = [fetch_with_semaphore(symbol) for symbol in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for res in results:
            if isinstance(res, Exception):
                logger.error(f"Exception in bulk fetch: {res}", exc_info=True)
                continue
            
            if isinstance(res, tuple) and len(res) == 2:
                symbol, df = res
                result[symbol] = df
        
        return result
    
    async def _try_switch_to_live_endpoint(self):
        """
        尝试切换到live端点（仅在testnet模式下且遇到连续失败时）
        """
        try:
            execution_mode = config.get('execution.mode', 'mock')
            if execution_mode != 'testnet':
                return
            
            # 如果已经在使用live端点，不需要切换
            if 'testnet' not in self.api_base:
                return
            
            # 切换到live端点
            live_api_base = config.get('binance.api_base', 'https://fapi.binance.com')
            if self.api_base != live_api_base:
                old_api_base = self.api_base
                self.api_base = live_api_base
                self._consecutive_failures = 0  # 重置计数
                self._403_count = 0  # 重置403计数
                self._last_403_time = 0  # 重置403时间
                logger.warning(
                    f"Switching funding rate collector from {old_api_base} to {live_api_base} "
                    f"due to consecutive failures. All subsequent requests will use live endpoint."
                )
                # 确认切换成功
                logger.info(f"Funding rate collector endpoint successfully switched to: {self.api_base}")
                # 切换后等待一段时间，避免立即请求
                await asyncio.sleep(2.0)
        except Exception as e:
            logger.error(f"Failed to switch to live endpoint: {e}", exc_info=True)
    