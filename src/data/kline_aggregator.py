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
import time

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol

logger = get_logger("kline_aggregator")


class KlineAggregator:
    """K线聚合器 - 从逐笔成交聚合生成K线（使用Polars优化）"""

    def __init__(
        self, interval_minutes: int = 5, on_kline_callback: Optional[Callable] = None
    ):
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
        self.latest_seen_window_start: Dict[str, int] = {}
        # 批量处理阈值：当列表达到此大小时，转换为DataFrame
        self._pending_trades_batch_size = 100
        # 延迟关窗：允许最近N个窗口继续接收晚到成交，减少因网络抖动导致的漏算
        self.close_grace_windows = int(config.get("data.kline_close_grace_windows", 3))

        # 每个交易对的最新K线数据
        # 格式: {symbol: pl.DataFrame}
        self.klines: Dict[str, pl.DataFrame] = {}

        # K线内存限制：保留最近N条K线，防止内存无限增长
        self.max_klines_per_symbol = config.get(
            "data.kline_aggregator_max_klines", 2000
        )
        
        # Pending trades窗口数限制：每个symbol最多保留的pending窗口数，防止内存无限增长
        self.max_pending_windows_per_symbol = config.get(
            "data.kline_aggregator_max_pending_windows", 50
        )
        
        # 每个窗口内trades列表的最大大小限制（防止单个窗口内trades无限增长）
        self.max_trades_per_window = config.get(
            "data.kline_aggregator_max_trades_per_window", 10000
        )

        # 统计信息
        self.stats = {
            "trades_processed": defaultdict(int),
            "klines_generated": defaultdict(int),
            "last_kline_time": defaultdict(Optional[datetime]),
            "invalid_trades": 0,
        }
        
        # 统计信息清理：定期清理不再活跃的symbol的统计信息
        # 修复内存泄漏：更频繁的清理，避免统计信息累积
        self._stats_cleanup_interval = config.get("data.stats_cleanup_interval", 60)  # 1分钟（更频繁的清理）
        self._last_stats_cleanup_time = time.time()

        self.running = False
        self._pending_trade_columns = [
            "price",
            "qty",
            "quote_qty",
            "ts_ms",
            "is_buyer_maker",
            "trade_count",
        ]

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
        ts_ms = int(trade.get("ts_ms") or 0)
        ts_us = int(trade.get("ts_us") or 0)
        if ts_ms <= 0 and ts_us > 0:
            ts_ms = ts_us // 1000
        if ts_ms <= 0:
            logger.warning(f"Skip trade without valid timestamp: {trade}")
            return None

        try:
            price = float(trade.get("price", 0))
            qty = float(trade.get("qty", 0))
        except Exception:
            logger.warning(f"Skip trade with invalid price/qty: {trade}")
            return None

        if price <= 0 or qty <= 0:
            # 静默过滤异常数据，不打印日志（减少日志噪音）
            return None

        quote_qty = trade.get("quoteQty")
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

        last_trade_id_raw = (
            trade.get("lastTradeId")
            if trade.get("lastTradeId") is not None
            else trade.get("l")
        )
        first_trade_id_raw = (
            trade.get("firstTradeId")
            if trade.get("firstTradeId") is not None
            else trade.get("f")
        )
        try:
            last_trade_id = (
                int(last_trade_id_raw) if last_trade_id_raw is not None else None
            )
        except Exception:
            last_trade_id = None
        try:
            first_trade_id = (
                int(first_trade_id_raw) if first_trade_id_raw is not None else None
            )
        except Exception:
            first_trade_id = None
        if first_trade_id is not None and last_trade_id is not None:
            underlying_trade_count = max(1, last_trade_id - first_trade_id + 1)
        else:
            underlying_trade_count = 1

        normalized = {
            "price": price,
            "qty": qty,
            "quoteQty": float(quote_qty),
            "ts_ms": ts_ms,
            "isBuyerMaker": bool(trade.get("isBuyerMaker", False)),
            # aggTrade可用 [f, l] 还原底层真实成交笔数；无该字段时按1计
            "underlyingTradeCount": underlying_trade_count,
        }
        return normalized

    def _add_trade_to_pending(
        self, symbol: str, normalized_trade: Dict, window_start_ms: int
    ):
        """将交易添加到pending列表（优化：使用列表收集，批量转换为DataFrame）"""
        # 检查窗口内trades数量，防止单个窗口内trades无限增长
        # 极端优化：强制限制，确保不超过配置值
        window_trades = self.pending_trades[symbol][window_start_ms]
        if self.max_trades_per_window > 0 and len(window_trades) >= self.max_trades_per_window:
            # 如果达到限制，只保留最新的trades（FIFO策略）
            # 移除最旧的，保留最新的(max-1)条
            target_size = self.max_trades_per_window - 1
            remove_count = len(window_trades) - target_size
            if remove_count > 0:
                # 使用del删除最旧的数据，避免创建新列表
                del window_trades[:remove_count]
                logger.debug(
                    f"{symbol} window {window_start_ms} trades count exceeds limit "
                    f"({self.max_trades_per_window}), removed oldest {remove_count} trades"
                )
        
        # 添加到列表（比频繁concat DataFrame快得多）
        # 使用元组而非dict保存逐笔，显著降低大规模pending窗口的Python对象开销
        trade_record = (
            normalized_trade["price"],
            normalized_trade["qty"],
            normalized_trade["quoteQty"],
            normalized_trade["ts_ms"],
            normalized_trade["isBuyerMaker"],
            int(normalized_trade.get("underlyingTradeCount", 1)),
        )
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

            ts_ms = normalized_trade["ts_ms"]

            # 计算窗口起始时间
            window_start_ms = self._get_window_start(ts_ms)
            prev_seen = self.latest_seen_window_start.get(symbol, window_start_ms)
            if window_start_ms > prev_seen:
                self.latest_seen_window_start[symbol] = window_start_ms
                current_window_start_ms = window_start_ms
            else:
                current_window_start_ms = prev_seen
            if symbol not in self.latest_seen_window_start:
                self.latest_seen_window_start[symbol] = window_start_ms

            close_before_ms = current_window_start_ms - (self.close_grace_windows * self.interval_seconds * 1000)
            if window_start_ms < close_before_ms:
                # 超过grace窗口的极晚成交直接丢弃，避免用零散迟到数据反复覆盖旧窗口
                self.stats["invalid_trades"] += 1
                return

            # 将交易添加到对应窗口（使用Polars DataFrame）
            self._add_trade_to_pending(symbol, normalized_trade, window_start_ms)
            self.stats["trades_processed"][symbol] += 1

            # 检查是否需要关闭旧窗口：
            # 使用“当前成交自身时间戳”推进窗口，避免因处理延迟导致同一窗口被反复部分聚合
            # 找出所有已关闭的窗口（窗口起始时间 < 当前窗口）
            windows_to_close = [
                window_start
                for window_start in self.pending_trades[symbol].keys()
                if window_start < close_before_ms
            ]

            # 聚合已关闭的窗口（批量处理，减少开销）
            for window_start_ms in windows_to_close:
                await self._aggregate_window(symbol, window_start_ms)
            
            # 检查pending_trades窗口数，如果超过限制则清理最旧的窗口
            # 极端优化：强制限制，确保不超过配置值
            pending_windows_count = len(self.pending_trades[symbol])
            keep_windows = max(self.max_pending_windows_per_symbol, self.close_grace_windows + 2)
            if pending_windows_count > keep_windows:
                # 仅清理已经超过grace边界的旧窗口，避免提前关窗损伤精度
                sorted_windows = sorted(
                    w for w in self.pending_trades[symbol].keys() if w < close_before_ms
                )
                to_remove = min(len(sorted_windows), pending_windows_count - keep_windows)
                removed_count = 0
                for window_start in sorted_windows[:to_remove]:
                    if window_start not in self.pending_trades[symbol]:
                        continue  # 可能已被其他线程删除
                    # 先尝试聚合，如果失败则直接删除
                    try:
                        await self._aggregate_window(symbol, window_start)
                        # 聚合成功，窗口已被_aggregate_window内部的pop移除
                        removed_count += 1
                    except Exception as e:
                        # 如果聚合失败，直接删除并清理trades列表
                        # 修复内存泄漏：确保窗口和trades列表被完全清理
                        if symbol in self.pending_trades and window_start in self.pending_trades[symbol]:
                            window_trades = self.pending_trades[symbol].pop(window_start, None)
                            if window_trades:
                                window_trades.clear()
                                del window_trades
                        removed_count += 1
                        logger.debug(
                            f"Failed to aggregate window {window_start} for {symbol} during limit check, "
                            f"deleted directly: {e}"
                        )
                
                if removed_count > 0:
                    logger.debug(
                        f"{symbol}: cleaned up {removed_count} pending windows "
                        f"(kept {keep_windows} latest, "
                        f"was {pending_windows_count})"
                    )
                
                # 修复内存泄漏：清理空字典条目（如果symbol的pending_trades为空字典，删除整个条目）
                if symbol in self.pending_trades and not self.pending_trades[symbol]:
                    del self.pending_trades[symbol]
                    logger.debug(f"Removed empty pending_trades entry for {symbol}")

        except Exception as e:
            logger.error(f"Error adding trade for {symbol}: {e}", exc_info=True)

    async def _aggregate_window(
        self,
        symbol: str,
        window_start_ms: int,
        trades_override: Optional[List[Dict]] = None,
        preserve_pending: bool = False,
    ):
        """
        聚合指定窗口的所有交易，生成K线（使用Polars向量化操作）

        Args:
            symbol: 交易对
            window_start_ms: 窗口起始时间（毫秒）
            trades_override: 可选，直接使用传入的成交列表进行聚合（用于离线校验）
        """
        try:
            if trades_override is None:
                # 从pending列表获取trades（现在是List[Dict]）
                # 修复内存泄漏：确保窗口被完全移除
                if window_start_ms in self.pending_trades.get(symbol, {}):
                    trades_list = self.pending_trades[symbol].pop(window_start_ms, [])
                    # 修复内存泄漏：如果symbol的pending_trades变为空字典，删除整个条目
                    if symbol in self.pending_trades and not self.pending_trades[symbol]:
                        del self.pending_trades[symbol]
                else:
                    trades_list = []
            else:
                # 覆盖使用外部提供的成交
                trades_list = trades_override
                if not preserve_pending:
                    # 移除可能残留的pending，并清理
                    if symbol in self.pending_trades:
                        if window_start_ms in self.pending_trades[symbol]:
                            old_trades = self.pending_trades[symbol].pop(window_start_ms, None)
                            if old_trades:
                                old_trades.clear()
                                del old_trades

            # 检查是否有交易
            has_trades = len(trades_list) > 0

            # 批量转换为Polars DataFrame（比频繁concat快）
            # 注意：聚合阶段不能再做截断，否则会引入口径误差
            if has_trades:
                first_row = trades_list[0]
                if isinstance(first_row, dict):
                    trades_df = pl.DataFrame(trades_list)
                else:
                    trades_df = pl.DataFrame(
                        trades_list,
                        schema=self._pending_trade_columns,
                        orient="row",
                    )
            else:
                trades_df = pl.DataFrame()

            if has_trades:
                # 按时间排序（Polars的sort很快）
                trades_df = trades_df.sort("ts_ms")

                # 使用Polars向量化计算OHLCV（比pandas快10-100倍）
                agg_result = trades_df.select(
                    [
                        pl.first("price").alias("open"),
                        pl.max("price").alias("high"),
                        pl.min("price").alias("low"),
                        pl.last("price").alias("close"),
                        pl.sum("qty").alias("volume"),
                        pl.sum("quote_qty").alias("quote_volume"),
                        pl.sum("trade_count").alias("trade_count"),
                        # 买卖方向统计（向量化）
                        pl.when(pl.col("is_buyer_maker") == False)
                        .then(pl.col("qty"))
                        .otherwise(0.0)
                        .sum()
                        .alias("buy_volume"),
                        pl.when(pl.col("is_buyer_maker") == True)
                        .then(pl.col("qty"))
                        .otherwise(0.0)
                        .sum()
                        .alias("sell_volume"),
                        pl.when(pl.col("is_buyer_maker") == False)
                        .then(pl.col("quote_qty"))
                        .otherwise(0.0)
                        .sum()
                        .alias("buy_dolvol"),
                        pl.when(pl.col("is_buyer_maker") == True)
                        .then(pl.col("quote_qty"))
                        .otherwise(0.0)
                        .sum()
                        .alias("sell_dolvol"),
                        pl.when(pl.col("is_buyer_maker") == False)
                        .then(pl.col("trade_count"))
                        .otherwise(0)
                        .sum()
                        .alias("buy_trade_count"),
                        pl.when(pl.col("is_buyer_maker") == True)
                        .then(pl.col("trade_count"))
                        .otherwise(0)
                        .sum()
                        .alias("sell_trade_count"),
                    ]
                )

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
                        # 根据图片要求，close字段是varchar类型，需要转换为float
                        if "close" in latest_kline.columns:
                            close_val = latest_kline["close"][0]
                            try:
                                prev_close = float(close_val)
                            except (ValueError, TypeError):
                                prev_close = 0.0
                        else:
                            prev_close = 0.0

                # 如果没有上一个K线，使用0（这种情况应该很少见，通常至少有一个K线）
                if prev_close is None:
                    prev_close = 0.0
                    logger.warning(
                        f"No previous kline found for {symbol} at window {window_start_ms}, using 0 as close price"
                    )

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
            tier1_threshold_rmb = config.get(
                "data.tran_stats_tier1_threshold_rmb", 40000
            )
            tier2_threshold_rmb = config.get(
                "data.tran_stats_tier2_threshold_rmb", 200000
            )
            tier3_threshold_rmb = config.get(
                "data.tran_stats_tier3_threshold_rmb", 1000000
            )
            usd_rmb_rate = config.get("data.usd_rmb_rate", 7.0)
            tier1_threshold = tier1_threshold_rmb / usd_rmb_rate
            tier2_threshold = tier2_threshold_rmb / usd_rmb_rate
            tier3_threshold = tier3_threshold_rmb / usd_rmb_rate

            if has_trades:
                buy_trades = trades_df.filter(pl.col("is_buyer_maker") == False)
                sell_trades = trades_df.filter(pl.col("is_buyer_maker") == True)

                # 买方分档统计
                buy_tier1 = buy_trades.filter(pl.col("quote_qty") <= tier1_threshold)
                buy_tier2 = buy_trades.filter(
                    (pl.col("quote_qty") > tier1_threshold)
                    & (pl.col("quote_qty") <= tier2_threshold)
                )
                buy_tier3 = buy_trades.filter(
                    (pl.col("quote_qty") > tier2_threshold)
                    & (pl.col("quote_qty") <= tier3_threshold)
                )
                buy_tier4 = buy_trades.filter(pl.col("quote_qty") > tier3_threshold)

                # 卖方分档统计
                sell_tier1 = sell_trades.filter(pl.col("quote_qty") <= tier1_threshold)
                sell_tier2 = sell_trades.filter(
                    (pl.col("quote_qty") > tier1_threshold)
                    & (pl.col("quote_qty") <= tier2_threshold)
                )
                sell_tier3 = sell_trades.filter(
                    (pl.col("quote_qty") > tier2_threshold)
                    & (pl.col("quote_qty") <= tier3_threshold)
                )
                sell_tier4 = sell_trades.filter(pl.col("quote_qty") > tier3_threshold)

                # 计算分档统计值
                buy_volume1 = (
                    float(buy_tier1["qty"].sum()) if not buy_tier1.is_empty() else 0.0
                )
                buy_dolvol1 = (
                    float(buy_tier1["quote_qty"].sum())
                    if not buy_tier1.is_empty()
                    else 0.0
                )
                buy_trade_count1 = int(buy_tier1["trade_count"].sum()) if not buy_tier1.is_empty() else 0
                buy_volume2 = (
                    float(buy_tier2["qty"].sum()) if not buy_tier2.is_empty() else 0.0
                )
                buy_dolvol2 = (
                    float(buy_tier2["quote_qty"].sum())
                    if not buy_tier2.is_empty()
                    else 0.0
                )
                buy_trade_count2 = int(buy_tier2["trade_count"].sum()) if not buy_tier2.is_empty() else 0
                buy_volume3 = (
                    float(buy_tier3["qty"].sum()) if not buy_tier3.is_empty() else 0.0
                )
                buy_dolvol3 = (
                    float(buy_tier3["quote_qty"].sum())
                    if not buy_tier3.is_empty()
                    else 0.0
                )
                buy_trade_count3 = int(buy_tier3["trade_count"].sum()) if not buy_tier3.is_empty() else 0
                buy_volume4 = (
                    float(buy_tier4["qty"].sum()) if not buy_tier4.is_empty() else 0.0
                )
                buy_dolvol4 = (
                    float(buy_tier4["quote_qty"].sum())
                    if not buy_tier4.is_empty()
                    else 0.0
                )
                buy_trade_count4 = int(buy_tier4["trade_count"].sum()) if not buy_tier4.is_empty() else 0

                sell_volume1 = (
                    float(sell_tier1["qty"].sum()) if not sell_tier1.is_empty() else 0.0
                )
                sell_dolvol1 = (
                    float(sell_tier1["quote_qty"].sum())
                    if not sell_tier1.is_empty()
                    else 0.0
                )
                sell_trade_count1 = int(sell_tier1["trade_count"].sum()) if not sell_tier1.is_empty() else 0
                sell_volume2 = (
                    float(sell_tier2["qty"].sum()) if not sell_tier2.is_empty() else 0.0
                )
                sell_dolvol2 = (
                    float(sell_tier2["quote_qty"].sum())
                    if not sell_tier2.is_empty()
                    else 0.0
                )
                sell_trade_count2 = int(sell_tier2["trade_count"].sum()) if not sell_tier2.is_empty() else 0
                sell_volume3 = (
                    float(sell_tier3["qty"].sum()) if not sell_tier3.is_empty() else 0.0
                )
                sell_dolvol3 = (
                    float(sell_tier3["quote_qty"].sum())
                    if not sell_tier3.is_empty()
                    else 0.0
                )
                sell_trade_count3 = int(sell_tier3["trade_count"].sum()) if not sell_tier3.is_empty() else 0
                sell_volume4 = (
                    float(sell_tier4["qty"].sum()) if not sell_tier4.is_empty() else 0.0
                )
                sell_dolvol4 = (
                    float(sell_tier4["quote_qty"].sum())
                    if not sell_tier4.is_empty()
                    else 0.0
                )
                sell_trade_count4 = int(sell_tier4["trade_count"].sum()) if not sell_tier4.is_empty() else 0
            else:
                # 无成交时，所有分档统计都为0
                buy_volume1 = buy_volume2 = buy_volume3 = buy_volume4 = 0.0
                buy_dolvol1 = buy_dolvol2 = buy_dolvol3 = buy_dolvol4 = 0.0
                buy_trade_count1 = buy_trade_count2 = buy_trade_count3 = (
                    buy_trade_count4
                ) = 0
                sell_volume1 = sell_volume2 = sell_volume3 = sell_volume4 = 0.0
                sell_dolvol1 = sell_dolvol2 = sell_dolvol3 = sell_dolvol4 = 0.0
                sell_trade_count1 = sell_trade_count2 = sell_trade_count3 = (
                    sell_trade_count4
                ) = 0

            # 计算VWAP：有成交时计算，无成交时为nan
            # 根据需求：无成交时vwap因为volume为0，所以vwap为nan
            import math

            vwap = quote_volume / volume if volume > 0 else float("nan")
            
            # 注意：根据图片要求，aggtrade里面的时间是第一个交易的时间
            # 但这对K线聚合没有影响，因为我们使用窗口起始时间作为span_begin_datetime

            # 构建K线数据（匹配数据库bar表结构）
            window_start_dt = datetime.fromtimestamp(
                window_start_ms / 1000, tz=timezone.utc
            )
            window_end_ms = window_start_ms + (self.interval_minutes * 60 * 1000)
            window_end_dt = datetime.fromtimestamp(
                window_end_ms / 1000, tz=timezone.utc
            )

            # 计算time_lable：每天的第几个5分钟窗口（1-288，共288个）
            day_start = window_start_dt.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            minutes_since_midnight = (window_start_dt - day_start).total_seconds() / 60
            time_lable = int(minutes_since_midnight // self.interval_minutes) + 1

            # span_status: 如果有交易则为空字符串，无交易则为"NoTrade"
            span_status = "" if trade_count > 0 else "NoTrade"

            # 统一数值类型：价格/成交额/成交量使用float，成交笔数使用int
            open_f = float(open_price) if not math.isnan(open_price) else 0.0
            high_f = float(high_price) if not math.isnan(high_price) else 0.0
            low_f = float(low_price) if not math.isnan(low_price) else 0.0
            close_f = float(close_price) if not math.isnan(close_price) else 0.0
            last_f = close_f
            vwap_f = float(vwap) if not math.isnan(vwap) else float("nan")
            quote_volume_f = float(quote_volume) if not math.isnan(quote_volume) else 0.0
            buy_dolvol_f = float(buy_dolvol) if not math.isnan(buy_dolvol) else 0.0
            sell_dolvol_f = float(sell_dolvol) if not math.isnan(sell_dolvol) else 0.0
            volume_f = float(volume) if not math.isnan(volume) else 0.0
            buy_volume_f = float(buy_volume) if not math.isnan(buy_volume) else 0.0
            sell_volume_f = float(sell_volume) if not math.isnan(sell_volume) else 0.0

            kline_data = {
                # 基础字段（兼容现有代码，保留原始数值类型用于计算）
                "symbol": symbol,
                "open_time": window_start_dt,
                "close_time": window_end_dt,
                "quote_volume": quote_volume_f,
                "trade_count": int(trade_count),
                "interval_minutes": self.interval_minutes,
                # bar字段
                "microsecond_since_trade": window_end_ms,  # bigint: 时间戳，即span_end_datetime的时间戳
                "span_begin_datetime": window_start_ms,  # bigint: 交易数据开始的时间，分钟必须被5整除，秒
                "span_end_datetime": window_end_ms,
                "span_status": span_status,
                "last": last_f,
                "high": high_f,
                "low": low_f,
                "open": open_f,
                "close": close_f,
                "vwap": vwap_f,
                "dolvol": quote_volume_f,
                "buydolvol": buy_dolvol_f,
                "selldolvol": sell_dolvol_f,
                "volume": volume_f,
                "buyvolume": buy_volume_f,
                "sellvolume": sell_volume_f,
                "tradecount": int(trade_count),
                "buytradecount": int(buy_trade_count),
                "selltradecount": int(sell_trade_count),
                "time_lable": time_lable,  # short: 每天的第几个span
                # tran_stats字段（总体统计）
                "buy_volume": buy_volume_f,
                "buy_dolvol": buy_dolvol_f,
                "buy_trade_count": int(buy_trade_count),
                "sell_volume": sell_volume_f,
                "sell_dolvol": sell_dolvol_f,
                "sell_trade_count": int(sell_trade_count),
                # tran_stats字段（金额分档）
                "buy_volume1": float(buy_volume1),
                "buy_volume2": float(buy_volume2),
                "buy_volume3": float(buy_volume3),
                "buy_volume4": float(buy_volume4),
                "buy_dolvol1": float(buy_dolvol1),
                "buy_dolvol2": float(buy_dolvol2),
                "buy_dolvol3": float(buy_dolvol3),
                "buy_dolvol4": float(buy_dolvol4),
                "buy_trade_count1": int(buy_trade_count1),
                "buy_trade_count2": int(buy_trade_count2),
                "buy_trade_count3": int(buy_trade_count3),
                "buy_trade_count4": int(buy_trade_count4),
                "sell_volume1": float(sell_volume1),
                "sell_volume2": float(sell_volume2),
                "sell_volume3": float(sell_volume3),
                "sell_volume4": float(sell_volume4),
                "sell_dolvol1": float(sell_dolvol1),
                "sell_dolvol2": float(sell_dolvol2),
                "sell_dolvol3": float(sell_dolvol3),
                "sell_dolvol4": float(sell_dolvol4),
                "sell_trade_count1": int(sell_trade_count1),
                "sell_trade_count2": int(sell_trade_count2),
                "sell_trade_count3": int(sell_trade_count3),
                "sell_trade_count4": int(sell_trade_count4),
            }

            # 使用Polars DataFrame存储（比pandas快）
            kline_df = pl.DataFrame([kline_data])

            # 确保时间戳精度为纳秒（统一格式）
            if "open_time" in kline_df.columns:
                kline_df = kline_df.with_columns(
                    pl.col("open_time").cast(pl.Datetime("ns", time_zone="UTC"))
                )
            if "close_time" in kline_df.columns:
                kline_df = kline_df.with_columns(
                    pl.col("close_time").cast(pl.Datetime("ns", time_zone="UTC"))
                )

            # 更新klines DataFrame
            # 修复内存泄漏：确保所有DataFrame引用被正确释放，使用更激进的清理策略
            max_klines = self.max_klines_per_symbol
            
            if symbol not in self.klines or self.klines[symbol].is_empty():
                self.klines[symbol] = kline_df
            else:
                # 获取当前DataFrame
                current_df = self.klines[symbol]
                current_len = len(current_df)
                
                # 修复内存泄漏：更激进的清理策略，提前trim到60%限制（从80%降低到60%），避免内存峰值
                # 修复配置安全性：降低trim阈值，确保内存不超过1.5-2GB目标
                # 修复：当max_klines=1时，不应该提前trim（因为trim后就没有数据了）
                # 只有当max_klines > 1时才进行提前trim
                if max_klines > 1:
                    trim_threshold = max(1, int(max_klines * 0.6))  # 从80%降低到60%，更激进的清理，但至少保留1条
                    if current_len >= trim_threshold:
                        # 提前trim到60%限制，为新K线腾出空间
                        # 使用clone()确保创建新对象，然后删除旧引用
                        # 修复：保留trim_threshold条，而不是trim_threshold-1条（避免tail(0)导致空DataFrame）
                        trimmed_current = current_df.tail(trim_threshold).clone()
                        # 立即更新klines，释放旧的DataFrame
                        old_df = self.klines[symbol]
                        self.klines[symbol] = trimmed_current
                        del old_df
                        del current_df
                        current_df = trimmed_current
                        current_len = len(current_df)
                
                # 重新实现：合并新K线，确保真正释放内存
                # 使用concat合并
                new_df = pl.concat([current_df, kline_df])
                # 立即释放中间对象引用
                del current_df
                del kline_df
                
                # 强制检查：如果合并后超过限制，立即trim（在去重前）
                if len(new_df) > max_klines:
                    # 先trim到限制值，使用to_pandas再转回polars确保完全释放内存
                    old_df_in_dict = self.klines.get(symbol)
                    try:
                        # 使用to_pandas再转回polars，确保完全释放内存
                        df_pd = new_df.tail(max_klines).to_pandas()
                        trimmed_df = pl.from_pandas(df_pd)
                        del df_pd
                    except Exception:
                        # 如果转换失败，使用clone
                        trimmed_df = new_df.tail(max_klines).clone()
                    
                    # 更新并释放旧DataFrame
                    self.klines[symbol] = trimmed_df
                    del new_df
                    del trimmed_df
                    # 释放旧的DataFrame引用（如果存在且不同）
                    if old_df_in_dict is not None and old_df_in_dict is not self.klines[symbol]:
                        del old_df_in_dict
                else:
                    # 更新klines（在去重前先更新，避免去重过程中数据丢失）
                    # 使用to_pandas再转回polars，确保完全释放内存
                    old_df_in_dict = self.klines.get(symbol)
                    try:
                        df_pd = new_df.to_pandas()
                        new_df_clean = pl.from_pandas(df_pd)
                        del df_pd
                    except Exception:
                        new_df_clean = new_df.clone()
                    
                    self.klines[symbol] = new_df_clean
                    del new_df
                    del new_df_clean
                    # 释放旧的DataFrame引用（如果存在且不同）
                    if old_df_in_dict is not None and old_df_in_dict is not self.klines[symbol]:
                        del old_df_in_dict

            # 重新实现：去除重复（按open_time去重，保留最新的），确保真正释放内存
            current_len = len(self.klines[symbol])
            
            # 如果数据量较大，先trim再去重，减少处理量
            if current_len > max_klines:
                old_df = self.klines[symbol]
                # 使用to_pandas再转回polars，确保完全释放内存
                try:
                    df_pd = old_df.tail(max_klines).to_pandas()
                    trimmed_df = pl.from_pandas(df_pd)
                    del df_pd
                except Exception:
                    trimmed_df = old_df.tail(max_klines).clone()
                
                self.klines[symbol] = trimmed_df
                # 确保旧DataFrame被完全释放
                if old_df is not None and old_df is not trimmed_df:
                    del old_df
                del trimmed_df
                current_len = max_klines
            
            # 去重和排序（使用lazy API优化大数据量处理）
            # 修复内存泄漏：只在必要时进行去重，并确保释放所有中间对象
            if current_len > 50:  # 数据量大时使用lazy API
                # 使用lazy API，减少中间对象
                old_df = self.klines[symbol]
                processed_df = (
                    old_df
                    .lazy()
                    .unique(subset=["open_time"], keep="last")
                    .sort("open_time")
                    .collect()
                )
                # 使用to_pandas再转回polars，确保完全释放内存
                try:
                    df_pd = processed_df.to_pandas()
                    processed_df_clean = pl.from_pandas(df_pd)
                    del df_pd
                except Exception:
                    processed_df_clean = processed_df.clone()
                
                # 立即更新并释放旧对象
                self.klines[symbol] = processed_df_clean
                if old_df is not None and old_df is not processed_df_clean:
                    del old_df
                del processed_df
                del processed_df_clean
            elif current_len > 1:  # 只有1条数据时不需要去重
                # 直接去重和排序
                old_df = self.klines[symbol]
                processed_df = (
                    old_df
                    .unique(subset=["open_time"], keep="last")
                    .sort("open_time")
                    .clone()
                )
                # 使用to_pandas再转回polars，确保完全释放内存
                try:
                    df_pd = processed_df.to_pandas()
                    processed_df_clean = pl.from_pandas(df_pd)
                    del df_pd
                except Exception:
                    processed_df_clean = processed_df.clone()
                
                self.klines[symbol] = processed_df_clean
                if old_df is not None and old_df is not processed_df_clean:
                    del old_df
                del processed_df
                del processed_df_clean

            # 重新实现：最终强制检查，确保不超过限制，并真正释放内存
            final_len = len(self.klines[symbol])
            if final_len > max_klines:
                # 强制trim到限制值（保留最新的）
                old_df = self.klines[symbol]
                # 使用to_pandas再转回polars，确保完全释放内存
                try:
                    df_pd = old_df.tail(max_klines).to_pandas()
                    final_df = pl.from_pandas(df_pd)
                    del df_pd
                except Exception:
                    final_df = old_df.tail(max_klines).clone()
                
                self.klines[symbol] = final_df
                # 确保旧DataFrame被完全释放
                if old_df is not None and old_df is not final_df:
                    del old_df
                del final_df
                logger.warning(
                    f"{symbol} klines count ({final_len}) exceeds limit ({max_klines}), "
                    f"force trimmed to {max_klines}"
                )
            # 额外检查：即使没有超过限制，如果接近限制也记录日志（用于调试）
            elif final_len > max_klines * 0.9:
                logger.debug(
                    f"{symbol} klines count ({final_len}) is close to limit ({max_klines}), "
                    f"will be trimmed in next cleanup cycle"
                )

            # 强制垃圾回收：在关键路径上触发GC，帮助释放内存
            # 注意：只在数据量大时触发，避免频繁GC影响性能
            if final_len > 100:
                import gc
                gc.collect()

            # 重新实现：强制清理临时DataFrame和列表，确保真正释放内存
            # 修复内存泄漏：polars DataFrame的filter操作会创建新的DataFrame，需要强制释放
            # 在计算完所有统计值后立即释放，避免持有引用
            trades_count_before_cleanup = len(trades_list) if trades_list else 0
            
            if has_trades:
                # 立即释放所有临时DataFrame引用（在计算完成后立即释放）
                # 使用try-finally确保即使出错也释放内存
                try:
                    # 先释放tier DataFrame（它们持有对buy_trades/sell_trades的引用）
                    del buy_tier1
                    del buy_tier2
                    del buy_tier3
                    del buy_tier4
                    del sell_tier1
                    del sell_tier2
                    del sell_tier3
                    del sell_tier4
                    # 再释放buy_trades和sell_trades（它们持有对trades_df的引用）
                    del buy_trades
                    del sell_trades
                    # 最后释放trades_df和agg_result
                    del trades_df
                    del agg_result
                except Exception:
                    pass  # 忽略删除错误，确保继续执行
                
                # 清空trades_list，释放内存
                if trades_list:
                    trades_list.clear()
                del trades_list
            else:
                # 即使没有trades，也要清理trades_list
                if trades_list:
                    trades_list.clear()
                del trades_list
            
            # 强制GC，确保polars DataFrame的内存被真正释放
            # 注意：只在数据量大时触发，避免频繁GC影响性能
            if has_trades and trades_count_before_cleanup > 100:
                import gc
                gc.collect()
            
            # 更新统计
            self.stats["klines_generated"][symbol] += 1
            self.stats["last_kline_time"][symbol] = window_start_dt

            # 使用debug级别记录K线生成（高频路径，减少日志开销）
            # 只在每100次或主要交易对时使用info
            if self.stats["klines_generated"][symbol] % 100 == 0 or (
                symbol in ["BTCUSDT", "ETHUSDT"]
                and self.stats["klines_generated"][symbol] % 10 == 0
            ):
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
                    logger.error(
                        f"Error in kline callback for {symbol}: {e}", exc_info=True
                    )

        except Exception as e:
            logger.error(
                f"Error aggregating window for {symbol} at {window_start_ms}: {e}",
                exc_info=True,
            )

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

    async def flush_pending_snapshot(self, symbol: Optional[str] = None):
        """
        按当前pending快照聚合并更新K线，但不移除pending。
        用于周期性落盘，避免将活跃窗口分段覆盖。
        """
        symbols_to_process = [format_symbol(symbol)] if symbol else list(self.pending_trades.keys())
        for sym in symbols_to_process:
            if sym not in self.pending_trades:
                continue
            windows = self.pending_trades.get(sym, {})
            if not windows:
                continue
            for window_start_ms, window_trades in list(windows.items()):
                if not window_trades:
                    continue
                await self._aggregate_window(
                    sym,
                    window_start_ms,
                    trades_override=list(window_trades),
                    preserve_pending=True,
                )

    async def flush_closed_windows(self, now_ms: Optional[int] = None, symbol: Optional[str] = None):
        """
        仅聚合已跨过grace边界的窗口，避免对当前活跃窗口做提前聚合。

        Args:
            now_ms: 当前时间戳（毫秒），默认使用系统当前UTC时间
            symbol: 如果指定，仅处理该交易对
        """
        if now_ms is None:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        current_window_start_ms = self._get_window_start(now_ms)
        close_before_ms = current_window_start_ms - (
            self.close_grace_windows * self.interval_seconds * 1000
        )

        symbols_to_process = [format_symbol(symbol)] if symbol else list(self.pending_trades.keys())
        for sym in symbols_to_process:
            if sym not in self.pending_trades:
                continue
            windows_to_close = sorted(
                ws for ws in list(self.pending_trades[sym].keys()) if ws < close_before_ms
            )
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
            prev_window_start_ms = current_window_start_ms - (
                self.interval_minutes * 60 * 1000
            )

            for symbol in symbols:
                symbol = format_symbol(symbol)

                # 检查上一窗口是否已经有K线
                if symbol in self.klines and not self.klines[symbol].is_empty():
                    # 检查最新K线是否覆盖了上一窗口
                    latest_kline = self.klines[symbol].tail(1)
                    if not latest_kline.is_empty():
                        latest_window_start = latest_kline["span_begin_datetime"][0]
                        if latest_window_start >= prev_window_start_ms:
                            # 已经有K线覆盖了上一窗口，跳过
                            continue

                # 检查pending_trades中是否有该窗口的交易
                has_pending = prev_window_start_ms in self.pending_trades.get(
                    symbol, {}
                )

                # 如果没有pending交易，生成空K线
                if not has_pending:
                    await self._aggregate_window(
                        symbol, prev_window_start_ms, trades_override=[]
                    )

        except Exception as e:
            logger.error(
                f"Error checking and generating empty windows: {e}", exc_info=True
            )

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
            ws = self._get_window_start(nt["ts_ms"])
            if window_start_ms is not None and ws != window_start_ms:
                logger.debug(
                    f"Skip trade outside target window: ts={nt['ts_ms']} target={window_start_ms}"
                )
                continue
            normalized.append(nt)

        if not normalized:
            return None

        # 计算目标窗口
        target_window_start = window_start_ms or self._get_window_start(
            normalized[0]["ts_ms"]
        )

        # 直接调用聚合（使用trades_override）
        await self._aggregate_window(
            symbol, target_window_start, trades_override=normalized
        )

        # 返回刚生成的目标窗口K线
        if symbol in self.klines and not self.klines[symbol].is_empty():
            df = self.klines[symbol]
            # 使用Polars filter查找目标窗口
            target_rows = df.filter(
                pl.col("span_begin_datetime") == target_window_start
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

    def get_klines(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> pd.DataFrame:
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
                    lazy_df = lazy_df.filter(pl.col("open_time") >= start_time)

            if end_time:
                if isinstance(end_time, datetime):
                    lazy_df = lazy_df.filter(pl.col("close_time") <= end_time)

            df = lazy_df.sort("open_time").collect()
        else:
            df = df.sort("open_time")

        # 转换为pandas DataFrame（保持兼容性）
        return df.to_pandas()

    def get_all_klines(self) -> Dict[str, pd.DataFrame]:
        """
        获取所有交易对的K线数据（返回pandas DataFrame保持兼容性）
        
        注意：此方法会创建pandas DataFrame副本，可能占用大量内存。
        如果只是用于保存，建议直接使用self.klines（polars DataFrame）。
        
        优化：避免一次性转换所有symbol，改为按需转换，减少内存峰值。
        """
        # 优化：不一次性转换所有symbol，避免内存峰值
        # 如果调用方需要所有数据，应该直接使用self.klines（polars DataFrame）
        # 这里返回空字典，避免内存泄漏
        logger.warning(
            "get_all_klines() called - this may cause memory issues. "
            "Consider using self.klines (polars DataFrame) directly instead."
        )
        return {}  # 返回空字典，避免内存泄漏

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

    def _cleanup_stats(self):
        """清理不再活跃的symbol的统计信息"""
        # 修复内存泄漏：更频繁的清理，确保统计信息不会无限累积
        current_time = time.time()
        if current_time - self._last_stats_cleanup_time < self._stats_cleanup_interval:
            return
        
        self._last_stats_cleanup_time = current_time
        
        # 获取当前活跃的symbol（有pending_trades或klines的symbol）
        active_symbols = set(self.pending_trades.keys()) | set(self.klines.keys())
        
        # 清理统计信息中不再活跃的symbol
        # 修复内存泄漏：确保所有不活跃的symbol被完全移除
        total_cleaned = 0
        for stat_key in ["trades_processed", "klines_generated", "last_kline_time"]:
            if stat_key in self.stats:
                stats_dict = self.stats[stat_key]
                if isinstance(stats_dict, dict):
                    inactive_symbols = set(stats_dict.keys()) - active_symbols
                    for symbol in inactive_symbols:
                        # 使用pop确保完全移除
                        stats_dict.pop(symbol, None)
                    total_cleaned += len(inactive_symbols)
        
        if total_cleaned > 0:
            logger.debug(
                f"Cleaned up stats for {total_cleaned} inactive symbols "
                f"(active: {len(active_symbols)})"
            )
            # 强制垃圾回收，帮助释放内存
            import gc
            gc.collect()
    
    def force_cleanup_klines(self, max_klines: int, force_rebuild_all: bool = False) -> tuple[int, int]:
        """
        强制清理klines数据，确保真正释放内存
        
        Args:
            max_klines: 每个symbol最多保留的K线数
            force_rebuild_all: 如果为True，即使数据量在限制内也重建DataFrame以释放内存碎片
            
        Returns:
            (cleaned_count, total_trimmed): 清理的symbol数量和总trim掉的K线数
        """
        cleaned_count = 0
        total_trimmed = 0
        
        # 重新实现：清理数据并释放内存碎片
        # 关键修复：即使数据量在限制内，也要定期重建DataFrame以释放Polars的内存碎片
        all_symbols = list(self.klines.keys())
        
        for symbol in all_symbols:
            if symbol not in self.klines:
                continue
            
            df = self.klines[symbol]
            if df.is_empty():
                # 清理空DataFrame
                del self.klines[symbol]
                cleaned_count += 1
                continue
            
            current_len = len(df)
            
            # 根据max_klines确定保留的数据量
            if max_klines > 1:
                keep_count = min(current_len, max_klines)
            else:
                keep_count = min(current_len, 1)
            
            # 关键修复：如果数据量超过限制，或者force_rebuild_all为True，都需要重建DataFrame
            needs_rebuild = current_len > keep_count or force_rebuild_all
            
            if needs_rebuild:
                old_df = self.klines[symbol]

                # 小DataFrame走轻量路径，避免频繁to_pandas引入额外内存抬升
                if current_len <= 2 and keep_count <= 1:
                    new_df = old_df.tail(keep_count).rechunk()
                else:
                    # 使用to_pandas再转回polars，确保完全释放内存
                    try:
                        df_pd = old_df.tail(keep_count).to_pandas()
                        new_df = pl.from_pandas(df_pd)
                        del df_pd
                    except Exception:
                        new_df = old_df.tail(keep_count).clone()
                
                # 更新并释放旧DataFrame
                # 关键修复：先删除旧引用，再赋值新DataFrame，确保旧DataFrame可以被GC回收
                if old_df is not None and old_df is not new_df:
                    # 先删除旧引用（这样old_df就没有其他引用了）
                    del self.klines[symbol]
                    # 再赋值新DataFrame
                    self.klines[symbol] = new_df
                    # 删除old_df引用（现在old_df应该可以被GC回收了）
                    del old_df
                else:
                    self.klines[symbol] = new_df
                # 注意：不要删除new_df，因为它已经被赋值给self.klines[symbol]
                
                cleaned_count += 1
                if current_len > keep_count:
                    total_trimmed += (current_len - keep_count)
        
        return cleaned_count, total_trimmed

    def get_stats(self) -> Dict:
        """获取统计信息"""
        # 清理不再活跃的symbol的统计信息
        self._cleanup_stats()
        
        return {
            "interval_minutes": self.interval_minutes,
            "trades_processed": dict(self.stats["trades_processed"]),
            "klines_generated": dict(self.stats["klines_generated"]),
            "pending_windows_count": {
                symbol: len(windows) for symbol, windows in self.pending_trades.items()
            },
            "klines_count": {symbol: len(df) for symbol, df in self.klines.items()},
            "last_kline_time": {
                symbol: dt.isoformat() if dt else None
                for symbol, dt in self.stats["last_kline_time"].items()
            },
        }
