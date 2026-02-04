"""
模拟数据生成器
为回测生成模拟的历史K线数据
"""
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import numpy as np

from ..common.logger import get_logger
from ..data.storage import get_data_storage

logger = get_logger('mock_data_generator')


class MockKlineGenerator:
    """模拟K线数据生成器"""
    
    def __init__(self, seed: Optional[int] = None):
        """
        初始化模拟数据生成器
        
        Args:
            seed: 随机种子，用于复现结果
        """
        if seed is not None:
            np.random.seed(seed)
        
        self.storage = get_data_storage()
    
    def generate_klines(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        initial_price: float,
        volatility: float = 0.02,
        interval_minutes: int = 5,
        trend: float = 0.0001,
    ) -> pd.DataFrame:
        """
        生成模拟K线数据（使用几何布朗运动）
        
        Args:
            symbol: 交易对
            start_date: 开始日期
            end_date: 结束日期
            initial_price: 初始价格
            volatility: 波动率（日化）
            interval_minutes: K线周期（分钟）
            trend: 趋势系数（日收益率）
        
        Returns:
            包含K线数据的DataFrame
        """
        # 计算时间步数
        interval_seconds = interval_minutes * 60
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
        total_seconds = end_ts - start_ts
        num_klines = int(total_seconds / interval_seconds) + 1
        
        # 生成时间戳
        timestamps = [start_date + timedelta(seconds=i * interval_seconds) for i in range(num_klines)]
        
        # 使用几何布朗运动生成价格
        # dS = μS*dt + σS*dW
        dt = interval_minutes / (24 * 60)  # 转换为天
        mu = trend / dt  # 调整为周期收益率
        sigma = volatility / np.sqrt(252)  # 周期波动率
        
        prices = [initial_price]
        
        for i in range(1, num_klines):
            dW = np.random.normal(0, 1)
            dS = prices[-1] * (mu * dt + sigma * np.sqrt(dt) * dW)
            new_price = max(prices[-1] + dS, initial_price * 0.5)  # 防止价格过低
            prices.append(new_price)
        
        # 为每根K线生成OHLCV数据
        kline_data = []
        
        for i in range(num_klines):
            # Open：周期开始价格
            open_price = prices[i]
            
            # 周期内的价格变动（模拟）
            intra_high = open_price * (1 + abs(np.random.normal(0, volatility / (252 ** 0.5))))
            intra_low = open_price * (1 - abs(np.random.normal(0, volatility / (252 ** 0.5))))
            
            # Close：周期结束价格
            if i + 1 < len(prices):
                close_price = prices[i + 1]
            else:
                close_price = open_price
            
            # High/Low
            high = max(open_price, close_price, intra_high)
            low = min(open_price, close_price, intra_low)
            
            # Volume（模拟成交量，与波动率相关）
            base_volume = 100 * initial_price  # 基础成交量
            volatility_factor = 1 + abs(np.random.normal(0, 0.5))
            volume = base_volume * volatility_factor
            
            # Quote asset volume（USDT成交额）
            quote_asset_volume = close_price * volume
            
            kline_data.append({
                'open_time': timestamps[i],
                'open': open_price,
                'high': high,
                'low': low,
                'close': close_price,
                'volume': volume,
                'quote_asset_volume': quote_asset_volume,
                'number_of_trades': int(volume / 1000),
                'taker_buy_base_asset_volume': volume * np.random.uniform(0.4, 0.6),
                'taker_buy_quote_asset_volume': quote_asset_volume * np.random.uniform(0.4, 0.6),
            })
        
        df = pd.DataFrame(kline_data)
        logger.info(f"Generated {len(df)} klines for {symbol}")
        
        return df
    
    def generate_and_save(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        initial_prices: Optional[Dict[str, float]] = None,
        volatilities: Optional[Dict[str, float]] = None,
        interval_minutes: int = 5,
    ):
        """
        生成模拟数据并保存到存储
        
        Args:
            symbols: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
            initial_prices: 初始价格字典
            volatilities: 波动率字典
            interval_minutes: K线周期
        """
        if initial_prices is None:
            # 使用随机初始价格
            initial_prices = {
                'BTCUSDT': np.random.uniform(45000, 55000),
                'ETHUSDT': np.random.uniform(2500, 3500),
                'BNBUSDT': np.random.uniform(600, 800),
                'ADAUSDT': np.random.uniform(0.8, 1.2),
                'XRPUSDT': np.random.uniform(2.0, 2.5),
            }
        
        if volatilities is None:
            volatilities = {symbol: 0.02 for symbol in symbols}
        
        for symbol in symbols:
            try:
                initial_price = initial_prices.get(symbol, 100.0)
                volatility = volatilities.get(symbol, 0.02)
                
                logger.info(f"Generating klines for {symbol}: price={initial_price:.2f}, vol={volatility:.4f}")
                
                # 生成K线
                klines = self.generate_klines(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    initial_price=initial_price,
                    volatility=volatility,
                    interval_minutes=interval_minutes,
                )
                
                # 保存到存储
                self.storage.save_klines(symbol=symbol, klines=klines)
                
                logger.info(f"Saved {len(klines)} klines for {symbol}")
                
            except Exception as e:
                logger.error(f"Failed to generate/save klines for {symbol}: {e}", exc_info=True)
    
    @staticmethod
    def generate_test_data(
        symbols: List[str] = None,
        start_date: datetime = None,
        end_date: datetime = None,
    ):
        """
        快速生成测试数据的便捷函数
        
        Args:
            symbols: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
        """
        if symbols is None:
            symbols = ['BTCUSDT', 'ETHUSDT']
        
        if start_date is None:
            start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
        
        if end_date is None:
            end_date = datetime(2024, 1, 31, tzinfo=timezone.utc)
        
        generator = MockKlineGenerator(seed=42)
        generator.generate_and_save(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            interval_minutes=5,
        )
        
        logger.info(f"Test data generated for {symbols}")


class MockFundingRateGenerator:
    """模拟资金费率数据生成器"""
    
    @staticmethod
    def generate_funding_rates(
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
    ) -> Dict[str, pd.DataFrame]:
        """
        生成模拟资金费率数据
        
        Args:
            symbols: 交易对列表
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            {symbol: DataFrame} 资金费率数据
        """
        funding_rates_data = {}
        
        # 资金费率每8小时更新一次
        current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        timestamps = []
        
        while current <= end_date:
            timestamps.append(current)
            current += timedelta(hours=8)
        
        for symbol in symbols:
            rates = []
            
            for ts in timestamps:
                # 模拟资金费率（通常在-0.0001到0.0001之间波动）
                rate = np.random.uniform(-0.0001, 0.0001)
                
                rates.append({
                    'timestamp': ts,
                    'funding_rate': rate,
                })
            
            funding_rates_data[symbol] = pd.DataFrame(rates)
        
        return funding_rates_data


class MockDataManager:
    """模拟数据管理器 - 生成并保存用于回测的模拟历史数据"""
    
    def __init__(self):
        """初始化模拟数据管理器"""
        self.generator = MockKlineGenerator()
        self.storage = get_data_storage()
    
    def generate_and_save_mock_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        initial_prices: Optional[Dict[str, float]] = None,
        volatilities: Optional[Dict[str, float]] = None,
        seed: Optional[int] = None,
        interval_minutes: int = 5,
    ):
        """
        生成模拟数据并保存到存储
        
        Args:
            symbols: 交易对列表 (e.g., ["BTCUSDT", "ETHUSDT"])
            start_date: 开始日期
            end_date: 结束日期
            initial_prices: 初始价格字典 {symbol: price}
            volatilities: 波动率字典 {symbol: volatility}
            seed: 随机种子，用于复现
            interval_minutes: K线周期（分钟）
        """
        if seed is not None:
            np.random.seed(seed)
        
        # 设置默认初始价格
        if initial_prices is None:
            initial_prices = {
                'BTCUSDT': 42000,
                'ETHUSDT': 2500,
                'BNBUSDT': 600,
                'ADAUSDT': 1.0,
                'XRPUSDT': 2.0,
            }
        
        # 设置默认波动率
        if volatilities is None:
            volatilities = {symbol: 0.02 for symbol in symbols}
        
        logger.info(f"Generating mock data for {symbols}: {start_date.date()} to {end_date.date()}")
        
        for symbol in symbols:
            try:
                initial_price = initial_prices.get(symbol, 100.0)
                volatility = volatilities.get(symbol, 0.02)
                
                logger.info(
                    f"Generating {symbol}: initial_price=${initial_price:.2f}, "
                    f"volatility={volatility:.4f}, interval={interval_minutes}m"
                )
                
                # 生成K线数据
                klines_df = self.generator.generate_klines(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    initial_price=initial_price,
                    volatility=volatility,
                    interval_minutes=interval_minutes
                )
                
                # 保存到存储
                self.storage.save_klines(symbol=symbol, df=klines_df)
                
                logger.info(f"✓ Saved {len(klines_df)} klines for {symbol}")
                
            except Exception as e:
                logger.error(f"✗ Failed to generate/save klines for {symbol}: {e}", exc_info=True)
                raise
        
        logger.info(f"✓ All mock data generated and saved successfully")
