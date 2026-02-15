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
from .utils import get_trading_days_per_year
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
        生成与系统期望一致的51列K线数据
        
        Args:
            symbol: 交易对
            start_date: 开始日期
            end_date: 结束日期
            initial_price: 初始价格
            volatility: 波动率（日化）
            interval_minutes: K线周期（分钟）
            trend: 趋势系数（日收益率）
        
        Returns:
            包含K线数据的DataFrame（51列，与系统期望格式一致）
        """
        from ..common.utils import format_symbol
        
        # 格式化symbol为大写格式（与storage和kline_aggregator一致）
        formatted_symbol = format_symbol(symbol)
        
        # 计算时间步数
        interval_seconds = interval_minutes * 60
        start_ts = start_date.timestamp()
        end_ts = end_date.timestamp()
        total_seconds = end_ts - start_ts
        num_klines = int(total_seconds / interval_seconds) + 1
        
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
        
        # 为每根K线生成完整的OHLCV数据（51列）
        kline_data = []
        
        for i in range(num_klines):
            # 计算时间窗口
            window_start = start_date + timedelta(seconds=i * interval_seconds)
            window_end = window_start + timedelta(seconds=interval_seconds)
            window_start_ms = int(window_start.timestamp() * 1000)
            window_end_ms = int(window_end.timestamp() * 1000)
            
            # Open：周期开始价格
            open_price = prices[i]
            
            # 周期内的价格变动（模拟）
            trading_days = get_trading_days_per_year()
            intra_high = open_price * (1 + abs(np.random.normal(0, volatility / (trading_days ** 0.5))))
            intra_low = open_price * (1 - abs(np.random.normal(0, volatility / (trading_days ** 0.5))))
            
            # Close：周期结束价格
            if i + 1 < len(prices):
                close_price = prices[i + 1]
            else:
                close_price = open_price
            
            # High/Low
            high_price = max(open_price, close_price, intra_high)
            low_price = min(open_price, close_price, intra_low)
            
            # Volume（模拟成交量，与波动率相关）
            base_volume = 100 * initial_price  # 基础成交量
            volatility_factor = 1 + abs(np.random.normal(0, 0.5))
            volume = base_volume * volatility_factor
            
            # Quote volume（USDT成交额）
            quote_volume = close_price * volume
            
            # Trade count（模拟交易笔数）
            trade_count = max(1, int(volume / 1000))
            
            # Buy/Sell volume（模拟买卖量，假设买卖比例在40%-60%之间）
            buy_ratio = np.random.uniform(0.4, 0.6)
            buy_volume = volume * buy_ratio
            sell_volume = volume * (1 - buy_ratio)
            buy_dolvol = quote_volume * buy_ratio
            sell_dolvol = quote_volume * (1 - buy_ratio)
            buy_trade_count = max(1, int(trade_count * buy_ratio))
            sell_trade_count = max(1, trade_count - buy_trade_count)
            
            # VWAP（成交量加权平均价）
            vwap = quote_volume / volume if volume > 0 else float('nan')
            
            # span_status: 如果有交易则为空字符串，无交易则为"NoTrade"
            span_status = "" if trade_count > 0 else "NoTrade"
            
            # time_lable: 每天的第几个5分钟窗口（1-288）
            day_start = window_start.replace(hour=0, minute=0, second=0, microsecond=0)
            minutes_since_midnight = (window_start - day_start).total_seconds() / 60
            time_lable = int(minutes_since_midnight // interval_minutes) + 1
            
            # tran_stats字段：按金额分档统计（模拟数据，使用随机分布）
            # 假设交易金额分布：tier1(40k以下), tier2(40k-200k), tier3(200k-1M), tier4(1M以上)
            # 为简化，我们将总成交量按比例分配到各个档位
            tier1_ratio = np.random.uniform(0.3, 0.5)  # 小额交易占30-50%
            tier2_ratio = np.random.uniform(0.2, 0.3)  # 中额交易占20-30%
            tier3_ratio = np.random.uniform(0.15, 0.25)  # 大额交易占15-25%
            tier4_ratio = 1.0 - tier1_ratio - tier2_ratio - tier3_ratio  # 超大额交易占剩余部分
            
            # 归一化比例
            total_ratio = tier1_ratio + tier2_ratio + tier3_ratio + tier4_ratio
            tier1_ratio /= total_ratio
            tier2_ratio /= total_ratio
            tier3_ratio /= total_ratio
            tier4_ratio /= total_ratio
            
            # Buy档位统计
            buy_volume1 = buy_volume * tier1_ratio
            buy_volume2 = buy_volume * tier2_ratio
            buy_volume3 = buy_volume * tier3_ratio
            buy_volume4 = buy_volume * tier4_ratio
            buy_dolvol1 = buy_dolvol * tier1_ratio
            buy_dolvol2 = buy_dolvol * tier2_ratio
            buy_dolvol3 = buy_dolvol * tier3_ratio
            buy_dolvol4 = buy_dolvol * tier4_ratio
            buy_trade_count1 = max(0, int(buy_trade_count * tier1_ratio))
            buy_trade_count2 = max(0, int(buy_trade_count * tier2_ratio))
            buy_trade_count3 = max(0, int(buy_trade_count * tier3_ratio))
            buy_trade_count4 = max(0, buy_trade_count - buy_trade_count1 - buy_trade_count2 - buy_trade_count3)
            
            # Sell档位统计
            sell_volume1 = sell_volume * tier1_ratio
            sell_volume2 = sell_volume * tier2_ratio
            sell_volume3 = sell_volume * tier3_ratio
            sell_volume4 = sell_volume * tier4_ratio
            sell_dolvol1 = sell_dolvol * tier1_ratio
            sell_dolvol2 = sell_dolvol * tier2_ratio
            sell_dolvol3 = sell_dolvol * tier3_ratio
            sell_dolvol4 = sell_dolvol * tier4_ratio
            sell_trade_count1 = max(0, int(sell_trade_count * tier1_ratio))
            sell_trade_count2 = max(0, int(sell_trade_count * tier2_ratio))
            sell_trade_count3 = max(0, int(sell_trade_count * tier3_ratio))
            sell_trade_count4 = max(0, sell_trade_count - sell_trade_count1 - sell_trade_count2 - sell_trade_count3)
            
            # 构建完整的K线数据（51列，与kline_aggregator生成的格式一致）
            kline_data.append({
                # 基础字段
                "symbol": formatted_symbol,
                "open_time": window_start,
                "close_time": window_end,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "quote_volume": quote_volume,
                "trade_count": trade_count,
                "buy_volume": buy_volume,
                "sell_volume": sell_volume,
                "interval_minutes": interval_minutes,
                # bar表字段
                "microsecond_since_trad": window_end_ms,
                "span_begin_datetime": window_start_ms,
                "span_end_datetime": window_end_ms,
                "span_status": span_status,
                "last": close_price,
                "vwap": vwap,
                "dolvol": quote_volume,
                "buydolvol": buy_dolvol,
                "selldolvol": sell_dolvol,
                "buyvolume": buy_volume,
                "sellvolume": sell_volume,
                "buytradecount": buy_trade_count,
                "selltradecount": sell_trade_count,
                "time_lable": time_lable,
                # tran_stats表字段（按金额分档统计）
                "buy_volume1": buy_volume1,
                "buy_volume2": buy_volume2,
                "buy_volume3": buy_volume3,
                "buy_volume4": buy_volume4,
                "buy_dolvol1": buy_dolvol1,
                "buy_dolvol2": buy_dolvol2,
                "buy_dolvol3": buy_dolvol3,
                "buy_dolvol4": buy_dolvol4,
                "buy_trade_count1": buy_trade_count1,
                "buy_trade_count2": buy_trade_count2,
                "buy_trade_count3": buy_trade_count3,
                "buy_trade_count4": buy_trade_count4,
                "sell_volume1": sell_volume1,
                "sell_volume2": sell_volume2,
                "sell_volume3": sell_volume3,
                "sell_volume4": sell_volume4,
                "sell_dolvol1": sell_dolvol1,
                "sell_dolvol2": sell_dolvol2,
                "sell_dolvol3": sell_dolvol3,
                "sell_dolvol4": sell_dolvol4,
                "sell_trade_count1": sell_trade_count1,
                "sell_trade_count2": sell_trade_count2,
                "sell_trade_count3": sell_trade_count3,
                "sell_trade_count4": sell_trade_count4,
            })
        
        df = pd.DataFrame(kline_data)
        logger.info(f"Generated {len(df)} klines for {symbol} with {len(df.columns)} columns")
        
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
                
                logger.info(f"[OK] Saved {len(klines_df)} klines for {symbol}")
                
            except Exception as e:
                logger.error(f"[ERROR] Failed to generate/save klines for {symbol}: {e}", exc_info=True)
                raise
        
        logger.info(f"[OK] All mock data generated and saved successfully")
