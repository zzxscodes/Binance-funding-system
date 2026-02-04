"""
K线聚合器模块
从逐笔成交数据聚合生成5分钟K线
这是核心功能，不使用交易所的K线API

使用Polars进行高性能数据处理（比pandas快10-100倍）
"""
import polars as pl
import pandas as pd  # 保留用于兼容性（时间戳转换等）
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Callable, Union
from collections import defaultdict
import asyncio

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol

logger = get_logger('kline_aggregator')


class KlineAggregator:
    """K线聚合器 - 从逐笔成交聚合生成K线（使用Polars优化）"""
    
    def __init__(self, interval_minutes: int = 5, on_kline_callback: Optional[Callable] = None):
        """
        初始化K线聚合器
        
        Args:
            interval_minutes: K线周期（分钟），默认5分钟
            on_kline_callback: K线生成回调函数 callback(symbol: str, kline: dict)
        """
        self.interval_minutes = interval_minutes
        self.interval_seconds = interval_minutes * 60
        self.on_kline_callback = on_kline_callback
        
        # 每个交易对的未完成K线数据
        # 格式: {symbol: {window_start: List[Dict]}} - 使用列表收集trades，批量转换为DataFrame
        # 优化：减少频繁的DataFrame concat操作，改为批量处理
        self.pending_trades: Dict[str, Dict[int, List[Dict]]] = defaultdict(
            lambda: defaultdict(list)
        )
        # 批量处理阈值：当列表达到此大小时，转换为DataFrame
        self._pending_trades_batch_size = 100
        
        # 每个交易对的最新K线数据
        # 格式: {symbol: pl.DataFrame}
        self.klines: Dict[str, pl.DataFrame] = {}
        
        # 统计信息
        self.stats = {
            'trades_processed': defaultdict(int),
            'klines_generated': defaultdict(int),
            'last_kline_time': defaultdict(Optional[datetime]),
        }
        
        self.running = False
    
    def _get_window_start(self, timestamp_ms: int) -> int:
        """
        根据时间戳计算K线窗口的起始时间（毫秒）
        
        Args:
            timestamp_ms: 交易时间戳（毫秒）
        
        Returns:
            窗口起始时间戳（毫秒）
        """
        # 转换为秒，向下取整到interval
        timestamp_s = timestamp_ms // 1000
        window_start_s = (timestamp_s // self.interval_seconds) * self.interval_seconds
        window_start_ms = window_start_s * 1000
        return window_start_ms
    
    def _validate_and_normalize_trade(self, trade: Dict) -> Optional[Dict]:
        """
        校验并规范化单笔成交数据。
        - 必须包含有效的时间戳（ts_ms 或 ts_us）
        - 价格、数量必须为正
        - 计算缺失的 quoteQty
        """
        ts_ms = int(trade.get('ts_ms') or 0)
        ts_us = int(trade.get('ts_us') or 0)
        if ts_ms <= 0 and ts_us > 0:
            ts_ms = ts_us // 1000
        if ts_ms <= 0:
            logger.warning(f"Skip trade without valid timestamp: {trade}")
            return None

        try:
            price = float(trade.get('price', 0))
            qty = float(trade.get('qty', 0))
        except Exception:
            logger.warning(f"Skip trade with invalid price/qty: {trade}")
            return None

        if price <= 0 or qty <= 0:
            # 静默过滤异常数据，不打印日志（减少日志噪音）
            return None

        quote_qty = trade.get('quoteQty')
        if quote_qty is None:
            quote_qty = price * qty
        else:
            try:
                quote_qty = float(quote_qty)
            except Exception:
                logger.warning(f"Skip trade with invalid quoteQty: {trade}")
                return None
            if quote_qty <= 0:
                quote_qty = price * qty

        normalized = {
            'price': price,
            'qty': qty,
            'quoteQty': float(quote_qty),
            'ts_ms': ts_ms,
            'isBuyerMaker': bool(trade.get('isBuyerMaker', False)),
        }
        return normalized
    
    def _add_trade_to_pending(self, symbol: str, normalized_trade: Dict, window_start_ms: int):
        """将交易添加到pending列表（优化：使用列表收集，批量转换为DataFrame）"""
        # 添加到列表（比频繁concat DataFrame快得多）
        trade_record = {
            'price': normalized_trade['price'],
            'qty': normalized_trade['qty'],
            'quote_qty': normalized_trade['quoteQty'],
            'ts_ms': normalized_trade['ts_ms'],
            'is_buyer_maker': normalized_trade['isBuyerMaker'],
        }
        self.pending_trades[symbol][window_start_ms].append(trade_record)

    async def add_trade(self, symbol: str, trade: Dict):
        """
        添加一笔成交数据，自动聚合到对应的K线窗口
        
        Args:
            symbol: 交易对（大写）
            trade: 成交数据，必须包含: price, qty, ts_ms (或 ts_us)
        """
        try:
            symbol = format_symbol(symbol)
            
            normalized_trade = self._validate_and_normalize_trade(trade)
            if normalized_trade is None:
                return

            ts_ms = normalized_trade['ts_ms']
            
            # 计算窗口起始时间
            window_start_ms = self._get_window_start(ts_ms)
            
            # 将交易添加到对应窗口（使用Polars DataFrame）
            self._add_trade_to_pending(symbol, normalized_trade, window_start_ms)
            self.stats['trades_processed'][symbol] += 1
            
            # 检查是否需要关闭旧的K线窗口并生成新K线
            # 如果当前窗口已关闭（时间已过下一个窗口），则聚合旧窗口
            current_window_start_ms = self._get_window_start(int(datetime.now(timezone.utc).timestamp() * 1000))
            
            # 找出所有已关闭的窗口（窗口起始时间 < 当前窗口）
            windows_to_close = [
                window_start for window_start in self.pending_trades[symbol].keys()
                if window_start < current_window_start_ms
            ]
            
            # 聚合已关闭的窗口
            for window_start_ms in windows_to_close:
                await self._aggregate_window(symbol, window_start_ms)
            
        except Exception as e:
            logger.error(f"Error adding trade for {symbol}: {e}", exc_info=True)
    
    async def _aggregate_window(self, symbol: str, window_start_ms: int, trades_override: Optional[List[Dict]] = None):
        """
        聚合指定窗口的所有交易，生成K线（使用Polars向量化操作）
        
        Args:
            symbol: 交易对
            window_start_ms: 窗口起始时间（毫秒）
            trades_override: 可选，直接使用传入的成交列表进行聚合（用于离线校验）
        """
        try:
            # 从pending列表获取trades（现在是List[Dict]）
            trades_list = self.pending_trades[symbol].pop(window_start_ms, [])
            
            if trades_override is not None:
                # 覆盖使用外部提供的成交
                trades_list = trades_override
                # 移除可能残留的pending
                if window_start_ms in self.pending_trades[symbol]:
                    self.pending_trades[symbol].pop(window_start_ms, None)
            
            # 检查是否有交易
            has_trades = len(trades_list) > 0
            
            # 批量转换为Polars DataFrame（比频繁concat快）
            if has_trades:
                trades_df = pl.DataFrame(trades_list)
            else:
                trades_df = pl.DataFrame()
            
            if has_trades:
                # 按时间排序（Polars的sort很快）
                trades_df = trades_df.sort('ts_ms')
                
                # 使用Polars向量化计算OHLCV（比pandas快10-100倍）
                agg_result = trades_df.select([
                pl.first('price').alias('open'),
                pl.max('price').alias('high'),
                pl.min('price').alias('low'),
                pl.last('price').alias('close'),
                pl.sum('qty').alias('volume'),
                pl.sum('quote_qty').alias('quote_volume'),
                pl.len().alias('trade_count'),
                
                # 买卖方向统计（向量化）
                pl.when(pl.col('is_buyer_maker') == False)
                    .then(pl.col('qty'))
                    .otherwise(0.0)
                    .sum()
                    .alias('buy_volume'),
                pl.when(pl.col('is_buyer_maker') == True)
                    .then(pl.col('qty'))
                    .otherwise(0.0)
                    .sum()
                    .alias('sell_volume'),
                pl.when(pl.col('is_buyer_maker') == False)
                    .then(pl.col('quote_qty'))
                    .otherwise(0.0)
                    .sum()
                    .alias('buy_dolvol'),
                pl.when(pl.col('is_buyer_maker') == True)
                    .then(pl.col('quote_qty'))
                    .otherwise(0.0)
                    .sum()
                    .alias('sell_dolvol'),
                pl.when(pl.col('is_buyer_maker') == False)
                    .then(1)
                    .otherwise(0)
                    .sum()
                    .alias('buy_trade_count'),
                pl.when(pl.col('is_buyer_maker') == True)
                    .then(1)
                    .otherwise(0)
                    .sum()
                    .alias('sell_trade_count'),
                ])
                
                # 提取聚合结果
                row = agg_result.row(0)
                open_price = row[0]
                high_price = row[1]
                low_price = row[2]
                close_price = row[3]
                volume = row[4]
                quote_volume = row[5]
                trade_count = row[6]
                buy_volume = row[7]
                sell_volume = row[8]
                buy_dolvol = row[9]
                sell_dolvol = row[10]
                buy_trade_count = row[11]
                sell_trade_count = row[12]
            else:
                # 无成交情况：获取上一个K线的close作为ohlc
                prev_close = None
                if symbol in self.klines and not self.klines[symbol].is_empty():
                    # 获取最新的K线
                    latest_kline = self.klines[symbol].tail(1)
                    if not latest_kline.is_empty():
                        prev_close = latest_kline['close'][0]
                
                # 如果没有上一个K线，使用0（这种情况应该很少见，通常至少有一个K线）
                if prev_close is None:
                    prev_close = 0.0
                    logger.warning(f"No previous kline found for {symbol} at window {window_start_ms}, using 0 as close price")
                
                # 无成交时，ohlc都等于上一个K线的close
                open_price = prev_close
                high_price = prev_close
                low_price = prev_close
                close_price = prev_close
                volume = 0.0
                quote_volume = 0.0
                trade_count = 0
                buy_volume = 0.0
                sell_volume = 0.0
                buy_dolvol = 0.0
                sell_dolvol = 0.0
                buy_trade_count = 0
                sell_trade_count = 0
            
            # 分档统计（使用Polars filter，比循环快很多）
            # 阈值：人民币阈值除以汇率作为美元阈值
            tier1_threshold_rmb = config.get('data.tran_stats_tier1_threshold_rmb', 40000)
            tier2_threshold_rmb = config.get('data.tran_stats_tier2_threshold_rmb', 200000)
            tier3_threshold_rmb = config.get('data.tran_stats_tier3_threshold_rmb', 1000000)
            usd_rmb_rate = config.get('data.usd_rmb_rate', 7.0)
            tier1_threshold = tier1_threshold_rmb / usd_rmb_rate
            tier2_threshold = tier2_threshold_rmb / usd_rmb_rate
            tier3_threshold = tier3_threshold_rmb / usd_rmb_rate
            
            if has_trades:
                buy_trades = trades_df.filter(pl.col('is_buyer_maker') == False)
                sell_trades = trades_df.filter(pl.col('is_buyer_maker') == True)
                
                # 买方分档统计
                buy_tier1 = buy_trades.filter(pl.col('quote_qty') <= tier1_threshold)
                buy_tier2 = buy_trades.filter(
                    (pl.col('quote_qty') > tier1_threshold) & (pl.col('quote_qty') <= tier2_threshold)
                )
                buy_tier3 = buy_trades.filter(
                    (pl.col('quote_qty') > tier2_threshold) & (pl.col('quote_qty') <= tier3_threshold)
                )
                buy_tier4 = buy_trades.filter(pl.col('quote_qty') > tier3_threshold)
                
                # 卖方分档统计
                sell_tier1 = sell_trades.filter(pl.col('quote_qty') <= tier1_threshold)
                sell_tier2 = sell_trades.filter(
                    (pl.col('quote_qty') > tier1_threshold) & (pl.col('quote_qty') <= tier2_threshold)
                )
                sell_tier3 = sell_trades.filter(
                    (pl.col('quote_qty') > tier2_threshold) & (pl.col('quote_qty') <= tier3_threshold)
                )
                sell_tier4 = sell_trades.filter(pl.col('quote_qty') > tier3_threshold)
                
                # 计算分档统计值
                buy_volume1 = float(buy_tier1['qty'].sum()) if not buy_tier1.is_empty() else 0.0
                buy_dolvol1 = float(buy_tier1['quote_qty'].sum()) if not buy_tier1.is_empty() else 0.0
                buy_trade_count1 = len(buy_tier1)
                buy_volume2 = float(buy_tier2['qty'].sum()) if not buy_tier2.is_empty() else 0.0
                buy_dolvol2 = float(buy_tier2['quote_qty'].sum()) if not buy_tier2.is_empty() else 0.0
                buy_trade_count2 = len(buy_tier2)
                buy_volume3 = float(buy_tier3['qty'].sum()) if not buy_tier3.is_empty() else 0.0
                buy_dolvol3 = float(buy_tier3['quote_qty'].sum()) if not buy_tier3.is_empty() else 0.0
                buy_trade_count3 = len(buy_tier3)
                buy_volume4 = float(buy_tier4['qty'].sum()) if not buy_tier4.is_empty() else 0.0
                buy_dolvol4 = float(buy_tier4['quote_qty'].sum()) if not buy_tier4.is_empty() else 0.0
                buy_trade_count4 = len(buy_tier4)
                
                sell_volume1 = float(sell_tier1['qty'].sum()) if not sell_tier1.is_empty() else 0.0
                sell_dolvol1 = float(sell_tier1['quote_qty'].sum()) if not sell_tier1.is_empty() else 0.0
                sell_trade_count1 = len(sell_tier1)
                sell_volume2 = float(sell_tier2['qty'].sum()) if not sell_tier2.is_empty() else 0.0
                sell_dolvol2 = float(sell_tier2['quote_qty'].sum()) if not sell_tier2.is_empty() else 0.0
                sell_trade_count2 = len(sell_tier2)
                sell_volume3 = float(sell_tier3['qty'].sum()) if not sell_tier3.is_empty() else 0.0
                sell_dolvol3 = float(sell_tier3['quote_qty'].sum()) if not sell_tier3.is_empty() else 0.0
                sell_trade_count3 = len(sell_tier3)
                sell_volume4 = float(sell_tier4['qty'].sum()) if not sell_tier4.is_empty() else 0.0
                sell_dolvol4 = float(sell_tier4['quote_qty'].sum()) if not sell_tier4.is_empty() else 0.0
                sell_trade_count4 = len(sell_tier4)
            else:
                # 无成交时，所有分档统计都为0
                buy_volume1 = buy_volume2 = buy_volume3 = buy_volume4 = 0.0
                buy_dolvol1 = buy_dolvol2 = buy_dolvol3 = buy_dolvol4 = 0.0
                buy_trade_count1 = buy_trade_count2 = buy_trade_count3 = buy_trade_count4 = 0
                sell_volume1 = sell_volume2 = sell_volume3 = sell_volume4 = 0.0
                sell_dolvol1 = sell_dolvol2 = sell_dolvol3 = sell_dolvol4 = 0.0
                sell_trade_count1 = sell_trade_count2 = sell_trade_count3 = sell_trade_count4 = 0
            
            # 计算VWAP：有成交时计算，无成交时为nan
            # 根据需求：无成交时vwap因为volume为0，所以vwap为nan
            import math
            vwap = quote_volume / volume if volume > 0 else float('nan')
            
            # 构建K线数据（匹配数据库bar表结构）
            window_start_dt = datetime.fromtimestamp(window_start_ms / 1000, tz=timezone.utc)
            window_end_ms = window_start_ms + (self.interval_minutes * 60 * 1000)
            window_end_dt = datetime.fromtimestamp(window_end_ms / 1000, tz=timezone.utc)
            
            # 计算time_lable：每天的第几个5分钟窗口（1-288，共288个）
            day_start = window_start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            minutes_since_midnight = (window_start_dt - day_start).total_seconds() / 60
            time_lable = int(minutes_since_midnight // self.interval_minutes) + 1
            
            # span_status: 如果有交易则为空字符串，无交易则为"NoTrade"
            span_status = "" if trade_count > 0 else "NoTrade"
            
            kline_data = {
                # 基础字段（兼容现有代码）
                'symbol': symbol,
                'open_time': window_start_dt,
                'close_time': window_end_dt,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'quote_volume': quote_volume,
                'trade_count': trade_count,
                'buy_volume': buy_volume,
                'sell_volume': sell_volume,
                'interval_minutes': self.interval_minutes,
                
                # bar表字段
                'microsecond_since_trad': window_end_ms,
                'span_begin_datetime': window_start_ms,
                'span_end_datetime': window_end_ms,
                'span_status': span_status,
                'last': close_price,
                'vwap': vwap,
                'dolvol': quote_volume,
                'buydolvol': buy_dolvol,
                'selldolvol': sell_dolvol,
                'buyvolume': buy_volume,
                'sellvolume': sell_volume,
                'buytradecount': buy_trade_count,
                'selltradecount': sell_trade_count,
                'time_lable': time_lable,
                
                # tran_stats表字段（按金额分档统计）
                'buy_volume1': buy_volume1, 'buy_volume2': buy_volume2,
                'buy_volume3': buy_volume3, 'buy_volume4': buy_volume4,
                'buy_dolvol1': buy_dolvol1, 'buy_dolvol2': buy_dolvol2,
                'buy_dolvol3': buy_dolvol3, 'buy_dolvol4': buy_dolvol4,
                'buy_trade_count1': buy_trade_count1, 'buy_trade_count2': buy_trade_count2,
                'buy_trade_count3': buy_trade_count3, 'buy_trade_count4': buy_trade_count4,
                'sell_volume1': sell_volume1, 'sell_volume2': sell_volume2,
                'sell_volume3': sell_volume3, 'sell_volume4': sell_volume4,
                'sell_dolvol1': sell_dolvol1, 'sell_dolvol2': sell_dolvol2,
                'sell_dolvol3': sell_dolvol3, 'sell_dolvol4': sell_dolvol4,
                'sell_trade_count1': sell_trade_count1, 'sell_trade_count2': sell_trade_count2,
                'sell_trade_count3': sell_trade_count3, 'sell_trade_count4': sell_trade_count4,
            }
            
            # 使用Polars DataFrame存储（比pandas快）
            kline_df = pl.DataFrame([kline_data])
            
            # 确保时间戳精度为纳秒（统一格式）
            if 'open_time' in kline_df.columns:
                kline_df = kline_df.with_columns(
                    pl.col('open_time').cast(pl.Datetime('ns', time_zone='UTC'))
                )
            if 'close_time' in kline_df.columns:
                kline_df = kline_df.with_columns(
                    pl.col('close_time').cast(pl.Datetime('ns', time_zone='UTC'))
                )
            
            # 更新klines DataFrame
            if symbol not in self.klines or self.klines[symbol].is_empty():
                self.klines[symbol] = kline_df
            else:
                # Polars的concat比pandas快很多
                self.klines[symbol] = pl.concat([
                    self.klines[symbol],
                    kline_df
                ])
            
            # 去除重复（按open_time去重，保留最新的）
            self.klines[symbol] = self.klines[symbol].unique(
                subset=['open_time'],
                keep='last'
            ).sort('open_time')
            
            # 更新统计
            self.stats['klines_generated'][symbol] += 1
            self.stats['last_kline_time'][symbol] = window_start_dt
            
            # 使用debug级别记录K线生成（高频路径，减少日志开销）
            # 只在每100次或主要交易对时使用info
            if self.stats['klines_generated'][symbol] % 100 == 0 or (symbol in ['BTCUSDT', 'ETHUSDT'] and self.stats['klines_generated'][symbol] % 10 == 0):
                logger.info(
                    f"{symbol} Kline[{window_start_dt}]: O={open_price}, H={high_price}, "
                    f"L={low_price}, C={close_price}, V={volume}"
                )
            else:
                logger.debug(
                    f"{symbol} Kline[{window_start_dt}]: O={open_price}, H={high_price}, "
                    f"L={low_price}, C={close_price}, V={volume}"
                )
            
            # 调用回调函数（传递dict而不是Series）
            if self.on_kline_callback:
                try:
                    await self.on_kline_callback(symbol, kline_data)
                except Exception as e:
                    logger.error(f"Error in kline callback for {symbol}: {e}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Error aggregating window for {symbol} at {window_start_ms}: {e}", exc_info=True)
    
    async def flush_pending(self, symbol: Optional[str] = None):
        """
        强制聚合所有待处理的交易（用于关闭时保存数据）
        
        Args:
            symbol: 如果指定，只处理该交易对；否则处理所有交易对
        """
        symbols_to_process = [symbol] if symbol else list(self.pending_trades.keys())
        
        for sym in symbols_to_process:
            if sym not in self.pending_trades:
                continue
            
            windows_to_close = list(self.pending_trades[sym].keys())
            for window_start_ms in windows_to_close:
                await self._aggregate_window(sym, window_start_ms)
    
    async def check_and_generate_empty_windows(self, symbols: List[str]):
        """
        检查并生成无交易的窗口K线
        
        Args:
            symbols: 需要检查的交易对列表
        """
        try:
            current_time = datetime.now(timezone.utc)
            current_timestamp = int(current_time.timestamp() * 1000)
            current_window_start_ms = self._get_window_start(current_timestamp)
            
            # 上一窗口的起始时间
            prev_window_start_ms = current_window_start_ms - (self.interval_minutes * 60 * 1000)
            
            for symbol in symbols:
                symbol = format_symbol(symbol)
                
                # 检查上一窗口是否已经有K线
                if symbol in self.klines and not self.klines[symbol].is_empty():
                    # 检查最新K线是否覆盖了上一窗口
                    latest_kline = self.klines[symbol].tail(1)
                    if not latest_kline.is_empty():
                        latest_window_start = latest_kline['span_begin_datetime'][0]
                        if latest_window_start >= prev_window_start_ms:
                            # 已经有K线覆盖了上一窗口，跳过
                            continue
                
                # 检查pending_trades中是否有该窗口的交易
                has_pending = prev_window_start_ms in self.pending_trades.get(symbol, {})
                
                # 如果没有pending交易，生成空K线
                if not has_pending:
                    await self._aggregate_window(symbol, prev_window_start_ms, trades_override=[])
                    
        except Exception as e:
            logger.error(f"Error checking and generating empty windows: {e}", exc_info=True)
    
    async def aggregate_window_from_trades(
        self,
        symbol: str,
        trades: List[Dict],
        window_start_ms: Optional[int] = None,
    ) -> Optional[Dict]:
        """
        公开的离线聚合接口：将一组成交聚合为目标窗口K线。
        - 自动过滤无效成交
        - 可指定窗口起点（毫秒）；若未指定则按成交时间计算
        - 返回生成的K线字典或None
        """
        symbol = format_symbol(symbol)
        if not trades:
            return None

        normalized: List[Dict] = []
        for t in trades:
            nt = self._validate_and_normalize_trade(t)
            if nt is None:
                continue
            ws = self._get_window_start(nt['ts_ms'])
            if window_start_ms is not None and ws != window_start_ms:
                logger.debug(f"Skip trade outside target window: ts={nt['ts_ms']} target={window_start_ms}")
                continue
            normalized.append(nt)

        if not normalized:
            return None

        # 计算目标窗口
        target_window_start = window_start_ms or self._get_window_start(normalized[0]['ts_ms'])

        # 直接调用聚合（使用trades_override）
        await self._aggregate_window(symbol, target_window_start, trades_override=normalized)

        # 返回刚生成的目标窗口K线
        if symbol in self.klines and not self.klines[symbol].is_empty():
            df = self.klines[symbol]
            # 使用Polars filter查找目标窗口
            target_rows = df.filter(
                pl.col('span_begin_datetime') == target_window_start
            )
            if not target_rows.is_empty():
                # 转换为dict返回
                return target_rows.row(0, named=True)
        return None

    def get_latest_kline(self, symbol: str) -> Optional[Dict]:
        """
        获取指定交易对的最新K线
        
        Args:
            symbol: 交易对
        
        Returns:
            最新K线（dict）或None
        """
        symbol = format_symbol(symbol)
        if symbol not in self.klines or self.klines[symbol].is_empty():
            return None
        
        # 返回最后一行作为dict
        return self.klines[symbol].tail(1).row(0, named=True)
    
    def get_klines(self, symbol: str, start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None) -> pd.DataFrame:
        """
        获取指定交易对的历史K线
        
        Args:
            symbol: 交易对
            start_time: 起始时间（可选）
            end_time: 结束时间（可选）
        
        Returns:
            K线DataFrame（pandas格式，保持兼容性），如果没有数据则返回空DataFrame
        """
        symbol = format_symbol(symbol)
        if symbol not in self.klines or self.klines[symbol].is_empty():
            return pd.DataFrame()
        
        df = self.klines[symbol]
        
        # 时间过滤（使用Polars Lazy API优化）
        if start_time or end_time:
            lazy_df = df.lazy()
            
            if start_time:
                if isinstance(start_time, datetime):
                    lazy_df = lazy_df.filter(pl.col('open_time') >= start_time)
            
            if end_time:
                if isinstance(end_time, datetime):
                    lazy_df = lazy_df.filter(pl.col('close_time') <= end_time)
            
            df = lazy_df.sort('open_time').collect()
        else:
            df = df.sort('open_time')
        
        # 转换为pandas DataFrame（保持兼容性）
        return df.to_pandas()
    
    def get_all_klines(self) -> Dict[str, pd.DataFrame]:
        """获取所有交易对的K线数据（返回pandas DataFrame保持兼容性）"""
        return {
            symbol: df.to_pandas() if not df.is_empty() else pd.DataFrame()
            for symbol, df in self.klines.items()
        }
    
    def clear_klines(self, symbol: Optional[str] = None):
        """
        清空K线数据
        
        Args:
            symbol: 如果指定，只清空该交易对；否则清空所有
        """
        if symbol:
            symbol = format_symbol(symbol)
            if symbol in self.klines:
                del self.klines[symbol]
        else:
            self.klines.clear()
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        return {
            'interval_minutes': self.interval_minutes,
            'trades_processed': dict(self.stats['trades_processed']),
            'klines_generated': dict(self.stats['klines_generated']),
            'pending_windows_count': {
                symbol: len(windows)
                for symbol, windows in self.pending_trades.items()
            },
            'klines_count': {
                symbol: len(df)
                for symbol, df in self.klines.items()
            },
            'last_kline_time': {
                symbol: dt.isoformat() if dt else None
                for symbol, dt in self.stats['last_kline_time'].items()
            }
        }
