"""
数据存储模块
负责K线数据和逐笔成交数据的持久化存储

使用Polars进行高性能数据处理（比pandas快10-100倍）
"""

import os
import time
import pandas as pd
import polars as pl
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from pyo3_runtime import PanicException
except ImportError:
    PanicException = BaseException

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol, ensure_directory

logger = get_logger("data_storage")


class DataStorage:
    """数据存储管理器"""

    def __init__(self):
        self.data_dir = Path(config.get("data.data_directory", "data"))
        self.klines_dir = Path(config.get("data.klines_directory", "data/klines"))
        self.trades_dir = Path(config.get("data.trades_directory", "data/trades"))
        self.funding_rates_dir = Path(
            config.get("data.funding_rates_directory", "data/funding_rates")
        )
        self.premium_index_dir = Path(
            config.get("data.premium_index_directory", "data/premium_index")
        )

        # 确保目录存在
        ensure_directory(str(self.klines_dir))
        ensure_directory(str(self.trades_dir))
        ensure_directory(str(self.funding_rates_dir))
        ensure_directory(str(self.premium_index_dir))

        # 存储格式：parquet（高效压缩）或csv
        self.storage_format = "parquet"  # 可配置为 'csv' 或 'parquet'

        # Schema缓存：缓存每个symbol的schema，避免重复检查
        # 格式: {symbol: schema_dict}，schema_dict包含schema和最后更新时间
        self._schema_cache: Dict[str, Dict] = {}
        self._schema_cache_max_size = config.get('data.storage_schema_cache_max_size', 1000)

    def _get_kline_file_path(self, symbol: str, date: datetime) -> Path:
        """
        获取K线数据文件路径
        格式: data/klines/{symbol}/{YYYY-MM-DD}.parquet
        """
        symbol = format_symbol(symbol)
        date_str = date.strftime("%Y-%m-%d")
        symbol_dir = self.klines_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        return symbol_dir / f"{date_str}.{self.storage_format}"

    def _get_trade_file_path(
        self, symbol: str, date: datetime, hour: Optional[int] = None
    ) -> Path:
        """
        获取逐笔成交数据文件路径
        格式: data/trades/{symbol}/{YYYY-MM-DD}-{HH}h.parquet
        """
        symbol = format_symbol(symbol)
        date_str = date.strftime("%Y-%m-%d")
        symbol_dir = self.trades_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)

        if hour is not None:
            return symbol_dir / f"{date_str}-{hour:02d}h.{self.storage_format}"
        else:
            return symbol_dir / f"{date_str}.{self.storage_format}"

    def _get_funding_rate_file_path(self, symbol: str, date: datetime) -> Path:
        """
        获取资金费率数据文件路径
        格式: data/funding_rates/{symbol}/{YYYY-MM-DD}.parquet
        """
        symbol = format_symbol(symbol)
        date_str = date.strftime("%Y-%m-%d")
        symbol_dir = self.funding_rates_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        return symbol_dir / f"{date_str}.{self.storage_format}"

    def _get_premium_index_file_path(self, symbol: str, date: datetime) -> Path:
        """
        获取溢价指数K线数据文件路径
        格式: data/premium_index/{symbol}/{YYYY-MM-DD}.parquet
        """
        symbol = format_symbol(symbol)
        date_str = date.strftime("%Y-%m-%d")
        symbol_dir = self.premium_index_dir / symbol
        symbol_dir.mkdir(parents=True, exist_ok=True)
        return symbol_dir / f"{date_str}.{self.storage_format}"

    def save_klines(
        self,
        symbol: str,
        df: Union[pd.DataFrame, pl.DataFrame],
        date: Optional[datetime] = None,
    ):
        """
        保存K线数据（支持pandas和polars DataFrame）

        Args:
            symbol: 交易对
            df: K线DataFrame（pandas或polars），必须包含open_time等列
            date: 日期，如果不指定，从df的open_time推断
        """
        try:
            # 转换为polars DataFrame（内部统一使用polars）
            if isinstance(df, pd.DataFrame):
                if df.empty:
                    logger.warning(f"Empty DataFrame for {symbol}, skipping save")
                    return
                df_pl = pl.from_pandas(df)
            elif isinstance(df, pl.DataFrame):
                if df.is_empty():
                    logger.warning(f"Empty DataFrame for {symbol}, skipping save")
                    return
                df_pl = df
            else:
                raise TypeError(f"Unsupported DataFrame type: {type(df)}")

            # 统一时间戳精度为纳秒（避免合并时的类型不兼容）
            timestamp_cols = ["open_time", "close_time"]
            for col in timestamp_cols:
                if col in df_pl.columns:
                    # 如果列存在，确保是纳秒精度
                    df_pl = df_pl.with_columns(
                        pl.col(col).cast(pl.Datetime("ns", time_zone="UTC"))
                    )

            # 确定日期：如果数据跨多天，需要按日期分组保存
            if date is None:
                if "open_time" in df_pl.columns:
                    # 按日期分组保存
                    dates_in_data = df_pl["open_time"].dt.date().unique().to_list()
                    if len(dates_in_data) > 1:
                        # 数据跨多天，需要分别保存
                        logger.debug(
                            f"Kline data for {symbol} spans {len(dates_in_data)} days, saving to separate files"
                        )
                        for date_only in sorted(dates_in_data):
                            date_dt = datetime.combine(
                                date_only, datetime.min.time()
                            ).replace(tzinfo=timezone.utc)
                            # 过滤出该日期的数据
                            day_df = df_pl.filter(
                                pl.col("open_time").dt.date() == date_only
                            )
                            if not day_df.is_empty():
                                file_path = self._get_kline_file_path(symbol, date_dt)
                                self._save_klines_to_file(symbol, day_df, file_path)
                        return  # 已按日期分组保存，直接返回
                    else:
                        # 单日数据，使用该日期
                        min_time = df_pl["open_time"].min()
                        if isinstance(min_time, datetime):
                            date = min_time.date()
                        else:
                            date = pd.to_datetime(min_time).to_pydatetime().date()
                        date = datetime.combine(date, datetime.min.time()).replace(
                            tzinfo=timezone.utc
                        )
                else:
                    date = datetime.now(timezone.utc)

            file_path = self._get_kline_file_path(symbol, date)

            # 保存
            self._save_klines_to_file(symbol, df_pl, file_path)

        except Exception as e:
            logger.error(f"Failed to save klines for {symbol}: {e}", exc_info=True)
            raise

    def _save_klines_to_file(self, symbol: str, df_pl: pl.DataFrame, file_path: Path):
        """保存K线数据到文件（内部方法，处理文件合并）"""
        try:
            # 如果文件已存在，合并数据（使用Polars，比pandas快）
            # 添加文件锁定重试机制（Windows上可能存在文件锁定问题）
            if file_path.exists():
                max_retries = 3
                retry_delay = 0.5
                for attempt in range(max_retries):
                    try:
                        existing_df_pl = self._load_dataframe_polars(file_path)
                        # 合并并去重
                        if existing_df_pl.is_empty():
                            logger.debug(
                                f"Existing kline file is empty, will overwrite with new data: {file_path}"
                            )
                        else:
                            # Polars的concat和unique比pandas快很多
                            combined_df = pl.concat([existing_df_pl, df_pl])
                            combined_df = combined_df.unique(
                                subset=["open_time"], keep="last"
                            )
                            combined_df = combined_df.sort("open_time")
                            df_pl = combined_df
                        break  # 成功读取，退出重试循环
                    except (IOError, OSError, PermissionError) as e:
                        if attempt < max_retries - 1:
                            import time

                            logger.debug(
                                f"File locked or access error (attempt {attempt + 1}/{max_retries}) for {file_path}: {e}, "
                                f"retrying in {retry_delay}s..."
                            )
                            time.sleep(retry_delay)
                            retry_delay *= 2
                        else:
                            logger.warning(
                                f"Failed to merge existing file {file_path} after {max_retries} attempts: {e}, will overwrite"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Failed to merge existing file {file_path}: {e}, will overwrite"
                        )
                        break

            # 保存（使用Polars直接写入，比pandas快3-5倍）
            self._save_dataframe_polars(df_pl, file_path)
            logger.debug(f"Saved {len(df_pl)} klines for {symbol} to {file_path}")

        except Exception as e:
            logger.error(
                f"Failed to save klines to file {file_path}: {e}", exc_info=True
            )
            raise

    def load_klines(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        加载K线数据（返回pandas DataFrame保持兼容性）

        Args:
            symbol: 交易对
            start_date: 起始日期
            end_date: 结束日期

        Returns:
            K线DataFrame（pandas格式）
        """
        try:
            symbol = format_symbol(symbol)
            symbol_dir = self.klines_dir / symbol

            if not symbol_dir.exists():
                return pd.DataFrame()

            # 收集所有相关文件
            all_files = []
            if start_date and end_date:
                current_date = start_date.date()
                end_date_only = end_date.date()
                while current_date <= end_date_only:
                    file_path = self._get_kline_file_path(
                        symbol,
                        datetime.combine(current_date, datetime.min.time()).replace(
                            tzinfo=timezone.utc
                        ),
                    )
                    if file_path.exists():
                        all_files.append(file_path)
                    current_date += timedelta(days=1)
            else:
                # 加载所有文件
                all_files = list(symbol_dir.glob(f"*.{self.storage_format}"))

            if not all_files:
                return pd.DataFrame()

            # 使用Polars加载所有文件并合并（比pandas快）
            dfs_pl = []
            for file_path in sorted(all_files):
                try:
                    df_pl = self._load_dataframe_polars(file_path)
                    if not df_pl.is_empty():
                        dfs_pl.append(df_pl)
                except Exception as e:
                    logger.warning(f"Failed to load {file_path}: {e}")

            if not dfs_pl:
                return pd.DataFrame()

            # 统一schema（避免Int64/Int32等类型不兼容）
            # 优化：使用缓存的schema，减少重复检查
            if len(dfs_pl) > 1:
                # 尝试使用缓存的schema
                cache_key = f"{symbol}_klines"
                # 检查缓存大小，如果超过限制则清理最旧的
                if len(self._schema_cache) >= self._schema_cache_max_size:
                    # 删除最旧的缓存（按时间戳，如果没有时间戳则删除第一个），直到满足限制
                    sorted_items = sorted(
                        self._schema_cache.items(),
                        key=lambda x: x[1].get('timestamp', 0)
                    )
                    # 删除最旧的，直到满足限制（保留最新的N个）
                    to_remove = len(sorted_items) - self._schema_cache_max_size + 1
                    to_remove = max(1, to_remove)  # 至少删除1个
                    for key, _ in sorted_items[:to_remove]:
                        self._schema_cache.pop(key, None)
                
                cached_schema = self._schema_cache.get(cache_key)

                try:
                    # 尝试直接合并，如果失败则统一schema
                    combined_df_pl = pl.concat(dfs_pl)
                    # 合并成功，更新缓存
                    if (
                        cached_schema is None
                        or cached_schema.get("schema") != dfs_pl[0].schema
                    ):
                        self._schema_cache[cache_key] = {
                            "schema": dfs_pl[0].schema,
                            "timestamp": time.time(),
                        }
                except Exception as e:
                    # 如果合并失败（类型不兼容），统一schema后重试
                    logger.debug(
                        f"Schema mismatch detected for {symbol}, unifying schemas: {e}"
                    )

                    # 使用缓存的schema或第一个DataFrame的schema作为参考
                    if cached_schema and "schema" in cached_schema:
                        reference_schema = cached_schema["schema"]
                    else:
                        reference_schema = dfs_pl[0].schema
                        # 更新缓存
                        self._schema_cache[cache_key] = {
                            "schema": reference_schema,
                            "timestamp": time.time(),
                        }

                    # 将所有DataFrame转换为相同的schema
                    unified_dfs = []
                    for df in dfs_pl:
                        # 转换列类型以匹配参考schema
                        cast_exprs = []
                        for col_name, col_type in reference_schema.items():
                            if col_name in df.columns:
                                # 如果类型不匹配，进行转换
                                if df[col_name].dtype != col_type:
                                    cast_exprs.append(pl.col(col_name).cast(col_type))
                        if cast_exprs:
                            df = df.with_columns(cast_exprs)
                        unified_dfs.append(df)
                    dfs_pl = unified_dfs
                    combined_df_pl = pl.concat(dfs_pl)
            else:
                # 只有一个DataFrame，直接使用，更新缓存
                combined_df_pl = dfs_pl[0] if dfs_pl else pl.DataFrame()
                if not combined_df_pl.is_empty():
                    cache_key = f"{symbol}_klines"
                    self._schema_cache[cache_key] = {
                        "schema": combined_df_pl.schema,
                        "timestamp": time.time(),
                    }

            if combined_df_pl.is_empty():
                return pd.DataFrame()

            # combined_df_pl已经在上面统一schema时创建了
            combined_df_pl = combined_df_pl.unique(subset=["open_time"], keep="last")
            combined_df_pl = combined_df_pl.sort("open_time")

            # 时间过滤（使用Polars Lazy API优化）
            if start_date or end_date:
                lazy_df = combined_df_pl.lazy()
                if start_date and "open_time" in combined_df_pl.columns:
                    lazy_df = lazy_df.filter(pl.col("open_time") >= start_date)
                if end_date and "close_time" in combined_df_pl.columns:
                    lazy_df = lazy_df.filter(pl.col("close_time") <= end_date)
                combined_df_pl = lazy_df.collect()

            # 转换为pandas DataFrame（保持兼容性）
            return combined_df_pl.to_pandas()

        except Exception as e:
            logger.error(f"Failed to load klines for {symbol}: {e}", exc_info=True)
            return pd.DataFrame()

    def load_klines_bulk(
        self,
        symbols: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_workers: Optional[int] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        批量加载多个交易对的K线数据（并发 IO 加速）。
        返回key使用 format_symbol 后的交易对（大写）。
        """
        if not symbols:
            return {}

        # I/O 读 parquet 为主，线程池即可显著提速
        workers = max_workers
        if workers is None:
            # 默认：最多 16 线程，且不超过 symbols 数量
            workers = min(16, max(1, len(symbols)))

        result: Dict[str, pd.DataFrame] = {}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fut_map = {
                ex.submit(self.load_klines, symbol, start_date, end_date): symbol
                for symbol in symbols
            }
            for fut in as_completed(fut_map):
                symbol = fut_map[fut]
                try:
                    result[format_symbol(symbol)] = fut.result()
                except PanicException as e:
                    logger.error(f"Bulk load klines panic for {symbol}: {e}")
                    result[format_symbol(symbol)] = pd.DataFrame()
                except Exception as e:
                    logger.error(
                        f"Bulk load klines failed for {symbol}: {e}", exc_info=True
                    )
                    result[format_symbol(symbol)] = pd.DataFrame()

        return result

    def save_trades(
        self,
        symbol: str,
        df: Union[pd.DataFrame, pl.DataFrame],
        date: Optional[datetime] = None,
    ):
        """
        保存逐笔成交数据（支持pandas和polars DataFrame）

        Args:
            symbol: 交易对
            df: 成交DataFrame
            date: 日期，如果不指定，从df的ts推断
        """
        try:
            # 转换为polars DataFrame
            if isinstance(df, pd.DataFrame):
                if df.empty:
                    return
                df_pl = pl.from_pandas(df)
            elif isinstance(df, pl.DataFrame):
                if df.is_empty():
                    return
                df_pl = df
            else:
                raise TypeError(f"Unsupported DataFrame type: {type(df)}")

            # 确定日期（按小时分别保存）
            if date is None:
                if "ts" in df_pl.columns:
                    min_ts = df_pl["ts"].min()
                    if isinstance(min_ts, datetime):
                        date = min_ts
                    else:
                        date = pd.to_datetime(min_ts).to_pydatetime()
                else:
                    date = datetime.now(timezone.utc)

            # 按小时分组保存（使用Polars group_by，比pandas快）
            if "ts" in df_pl.columns:
                # 提取小时
                df_pl = df_pl.with_columns(pl.col("ts").dt.hour().alias("hour"))
                # Polars group_by返回DataFrame，需要手动迭代
                unique_hours = df_pl["hour"].unique().to_list()
                for hour in unique_hours:
                    hour_df = df_pl.filter(pl.col("hour") == hour).drop("hour")
                    file_path = self._get_trade_file_path(symbol, date, int(hour))
                    self._append_trades_polars(file_path, hour_df)
            else:
                # 如果没有时间戳，保存到当天
                file_path = self._get_trade_file_path(symbol, date)
                self._append_trades_polars(file_path, df_pl)

        except Exception as e:
            logger.error(f"Failed to save trades for {symbol}: {e}", exc_info=True)

    def _append_trades_polars(self, file_path: Path, df: pl.DataFrame):
        """使用Polars追加成交数据到文件（比pandas快）"""
        if file_path.exists():
            # 添加文件锁定重试机制
            max_retries = 3
            retry_delay = 0.5
            for attempt in range(max_retries):
                try:
                    existing_df_pl = self._load_dataframe_polars(file_path)
                    # 去重（按tradeId）
                    if "tradeId" in df.columns and "tradeId" in existing_df_pl.columns:
                        combined_df = pl.concat([existing_df_pl, df])
                        combined_df = combined_df.unique(
                            subset=["tradeId"], keep="last"
                        )
                        sort_col = "ts_ms" if "ts_ms" in combined_df.columns else "ts"
                        combined_df = combined_df.sort(sort_col)
                        df = combined_df
                    else:
                        df = pl.concat([existing_df_pl, df])
                    break  # 成功读取，退出重试循环
                except (IOError, OSError, PermissionError) as e:
                    if attempt < max_retries - 1:
                        import time

                        logger.debug(
                            f"File locked or access error (attempt {attempt + 1}/{max_retries}) for {file_path}: {e}, "
                            f"retrying in {retry_delay}s..."
                        )
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        logger.warning(
                            f"Failed to merge existing trades file {file_path} after {max_retries} attempts: {e}"
                        )
                except Exception as e:
                    logger.warning(
                        f"Failed to merge existing trades file {file_path}: {e}"
                    )
                    break

        self._save_dataframe_polars(df, file_path)

    def _save_dataframe_polars(self, df: pl.DataFrame, file_path: Path):
        """使用Polars保存DataFrame到文件（比pandas快3-5倍）
        
        使用临时文件+原子替换的方式，避免文件写入过程中被中断导致文件损坏
        """
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 使用临时文件，写入完成后再原子替换，避免文件损坏
        tmp_path = file_path.with_suffix(file_path.suffix + f".{os.getpid()}.tmp")

        try:
            if self.storage_format == "parquet":
                # 写入临时文件
                df.write_parquet(tmp_path, compression="snappy")
                # 原子替换（Windows上使用os.replace，Linux上也是原子操作）
                # os.replace是原子操作，可以避免文件写入过程中被中断导致文件损坏
                os.replace(str(tmp_path), str(file_path))
            else:
                # CSV格式也使用临时文件
                df.write_csv(tmp_path)
                os.replace(str(tmp_path), str(file_path))
        except Exception as e:
            # 如果写入失败，清理临时文件
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            raise

    def _load_dataframe_polars(self, file_path: Path) -> pl.DataFrame:
        """使用Polars从文件加载DataFrame（比pandas快）"""
        if not file_path.exists():
            return pl.DataFrame()

        try:
            if file_path.suffix == ".parquet":
                df = pl.read_parquet(file_path)
                timestamp_cols = ["open_time", "close_time"]
                for col in timestamp_cols:
                    if col in df.columns:
                        df = df.with_columns(
                            pl.col(col).cast(pl.Datetime("ns", time_zone="UTC"))
                        )
                return df
            else:
                return pl.read_csv(file_path)
        except PanicException as e:
            logger.error(f"Polars panic when loading {file_path}: {e}")
            # Polars panic通常表示文件损坏，尝试删除损坏的文件
            try:
                logger.warning(f"Corrupted parquet file detected (Polars panic), removing: {file_path}")
                file_path.unlink()
            except Exception as del_err:
                logger.error(f"Failed to remove corrupted file: {del_err}")
            return pl.DataFrame()
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            if file_path.suffix == ".parquet" and "out of specification" in str(e):
                try:
                    logger.warning(
                        f"Corrupted parquet file detected, removing: {file_path}"
                    )
                    file_path.unlink()
                except Exception as del_err:
                    logger.error(f"Failed to remove corrupted file: {del_err}")
            return pl.DataFrame()

    def _save_dataframe(self, df: pd.DataFrame, file_path: Path):
        """保存pandas DataFrame到文件（兼容性方法）"""
        # 转换为polars并保存
        df_pl = pl.from_pandas(df)
        self._save_dataframe_polars(df_pl, file_path)

    def _load_dataframe(self, file_path: Path) -> pd.DataFrame:
        """从文件加载pandas DataFrame（兼容性方法）"""
        df_pl = self._load_dataframe_polars(file_path)
        return df_pl.to_pandas() if not df_pl.is_empty() else pd.DataFrame()

    def cleanup_old_data(self, days: int = 30):
        """
        清理旧数据

        Args:
            days: 保留最近N天的数据
        """
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        removed_count = 0

        # 清理K线数据
        for symbol_dir in self.klines_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            for file_path in symbol_dir.glob("*.parquet"):
                try:
                    if file_path.stat().st_mtime < cutoff_date.timestamp():
                        file_path.unlink()
                        removed_count += 1
                except Exception as e:
                    logger.warning(f"Failed to remove {file_path}: {e}")

        # 清理成交数据
        for symbol_dir in self.trades_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            for file_path in symbol_dir.glob("*.parquet"):
                try:
                    if file_path.stat().st_mtime < cutoff_date.timestamp():
                        file_path.unlink()
                        removed_count += 1
                except Exception as e:
                    logger.warning(f"Failed to remove {file_path}: {e}")

        # 清理资金费率数据
        for symbol_dir in self.funding_rates_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            for file_path in symbol_dir.glob("*.parquet"):
                try:
                    if file_path.stat().st_mtime < cutoff_date.timestamp():
                        file_path.unlink()
                        removed_count += 1
                except Exception as e:
                    logger.warning(f"Failed to remove {file_path}: {e}")

        # 清理溢价指数K线数据
        for symbol_dir in self.premium_index_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            for file_path in symbol_dir.glob("*.parquet"):
                try:
                    if file_path.stat().st_mtime < cutoff_date.timestamp():
                        file_path.unlink()
                        removed_count += 1
                except Exception as e:
                    logger.warning(f"Failed to remove {file_path}: {e}")

        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} old data files")

        return removed_count

    def save_funding_rates(
        self,
        symbol: str,
        df: Union[pd.DataFrame, pl.DataFrame],
        date: Optional[datetime] = None,
    ):
        """
        保存资金费率数据（支持pandas和polars DataFrame）

        Args:
            symbol: 交易对
            df: 资金费率DataFrame（pandas或polars），必须包含fundingTime等列
            date: 日期，如果不指定，从df的fundingTime推断
        """
        try:
            # 转换为polars DataFrame（内部统一使用polars）
            if isinstance(df, pd.DataFrame):
                if df.empty:
                    logger.warning(f"Empty DataFrame for {symbol}, skipping save")
                    return
                df_pl = pl.from_pandas(df)
            elif isinstance(df, pl.DataFrame):
                if df.is_empty():
                    logger.warning(f"Empty DataFrame for {symbol}, skipping save")
                    return
                df_pl = df
            else:
                raise TypeError(f"Unsupported DataFrame type: {type(df)}")

            # 确定日期：如果数据跨多天，需要按日期分组保存
            # 资金费率数据应该按日期分组保存，而不是只保存到最早日期
            if date is None:
                # 如果数据跨多天，需要分别保存到不同的文件
                if "fundingTime" in df_pl.columns:
                    # 按日期分组保存
                    dates_in_data = df_pl["fundingTime"].dt.date().unique().to_list()
                    if len(dates_in_data) > 1:
                        # 数据跨多天，需要分别保存
                        logger.debug(
                            f"Funding rate data for {symbol} spans {len(dates_in_data)} days, saving to separate files"
                        )
                        for date_only in sorted(dates_in_data):
                            date_dt = datetime.combine(
                                date_only, datetime.min.time()
                            ).replace(tzinfo=timezone.utc)
                            # 过滤出该日期的数据
                            day_df = df_pl.filter(
                                pl.col("fundingTime").dt.date() == date_only
                            )
                            if not day_df.is_empty():
                                file_path = self._get_funding_rate_file_path(
                                    symbol, date_dt
                                )
                                self._save_funding_rates_to_file(
                                    symbol, day_df, file_path
                                )
                        return  # 已按日期分组保存，直接返回
                    else:
                        # 单日数据，使用该日期
                        min_time = df_pl["fundingTime"].min()
                        if isinstance(min_time, datetime):
                            date = min_time.date()
                        else:
                            date = pd.to_datetime(min_time).to_pydatetime().date()
                        date = datetime.combine(date, datetime.min.time()).replace(
                            tzinfo=timezone.utc
                        )
                else:
                    date = datetime.now(timezone.utc)

            file_path = self._get_funding_rate_file_path(symbol, date)

            # 保存（文件合并逻辑在_save_funding_rates_to_file中处理）
            self._save_funding_rates_to_file(symbol, df_pl, file_path)

        except Exception as e:
            logger.error(
                f"Failed to save funding rates for {symbol}: {e}", exc_info=True
            )
            raise

    def _save_funding_rates_to_file(
        self, symbol: str, df_pl: pl.DataFrame, file_path: Path
    ):
        """保存资金费率数据到文件（内部方法，处理文件合并）"""
        try:
            # 如果文件已存在，合并数据
            if file_path.exists():
                max_retries = 3
                retry_delay = 0.5
                for attempt in range(max_retries):
                    try:
                        existing_df_pl = self._load_dataframe_polars(file_path)
                        if existing_df_pl.is_empty():
                            logger.debug(
                                f"Existing funding rate file is empty, will overwrite with new data: {file_path}"
                            )
                        else:
                            combined_df = pl.concat([existing_df_pl, df_pl])
                            combined_df = combined_df.unique(
                                subset=["fundingTime"], keep="last"
                            )
                            combined_df = combined_df.sort("fundingTime")
                            df_pl = combined_df
                        break  # 成功读取，退出重试循环
                    except (IOError, OSError, PermissionError) as e:
                        if attempt < max_retries - 1:
                            import time

                            logger.debug(
                                f"File locked or access error (attempt {attempt + 1}/{max_retries}) for {file_path}: {e}, "
                                f"retrying in {retry_delay}s..."
                            )
                            time.sleep(retry_delay)
                            retry_delay *= 2
                        else:
                            logger.warning(
                                f"Failed to merge existing file {file_path} after {max_retries} attempts: {e}, will overwrite"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Failed to merge existing file {file_path}: {e}, will overwrite"
                        )
                        break

            # 保存
            self._save_dataframe_polars(df_pl, file_path)
            logger.debug(
                f"Saved {len(df_pl)} funding rates for {symbol} to {file_path}"
            )

        except Exception as e:
            logger.error(
                f"Failed to save funding rates to file {file_path}: {e}", exc_info=True
            )
            raise

    def load_funding_rates(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        加载资金费率数据（返回pandas DataFrame保持兼容性）

        Args:
            symbol: 交易对
            start_date: 起始日期
            end_date: 结束日期

        Returns:
            资金费率DataFrame（pandas格式）
        """
        try:
            symbol = format_symbol(symbol)
            symbol_dir = self.funding_rates_dir / symbol

            if not symbol_dir.exists():
                return pd.DataFrame()

            # 收集所有相关文件
            all_files = []
            if start_date and end_date:
                current_date = start_date.date()
                end_date_only = end_date.date()
                while current_date <= end_date_only:
                    file_path = self._get_funding_rate_file_path(
                        symbol,
                        datetime.combine(current_date, datetime.min.time()).replace(
                            tzinfo=timezone.utc
                        ),
                    )
                    if file_path.exists():
                        all_files.append(file_path)
                    current_date += timedelta(days=1)
            else:
                # 加载所有文件
                all_files = list(symbol_dir.glob(f"*.{self.storage_format}"))

            if not all_files:
                return pd.DataFrame()

            # 使用Polars加载所有文件并合并
            dfs_pl = []
            for file_path in sorted(all_files):
                try:
                    df_pl = self._load_dataframe_polars(file_path)
                    if not df_pl.is_empty():
                        dfs_pl.append(df_pl)
                except Exception as e:
                    logger.warning(f"Failed to load {file_path}: {e}")

            if not dfs_pl:
                return pd.DataFrame()

            combined_df_pl = pl.concat(dfs_pl)
            combined_df_pl = combined_df_pl.unique(subset=["fundingTime"], keep="last")
            combined_df_pl = combined_df_pl.sort("fundingTime")

            # 时间过滤
            if start_date or end_date:
                lazy_df = combined_df_pl.lazy()
                if start_date and "fundingTime" in combined_df_pl.columns:
                    lazy_df = lazy_df.filter(pl.col("fundingTime") >= start_date)
                if end_date and "fundingTime" in combined_df_pl.columns:
                    lazy_df = lazy_df.filter(pl.col("fundingTime") <= end_date)
                combined_df_pl = lazy_df.collect()

            # 转换为pandas DataFrame（保持兼容性）
            return combined_df_pl.to_pandas()

        except Exception as e:
            logger.error(
                f"Failed to load funding rates for {symbol}: {e}", exc_info=True
            )
            return pd.DataFrame()

    def load_funding_rates_bulk(
        self,
        symbols: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_workers: Optional[int] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        批量加载多个交易对的资金费率数据（并发 IO 加速）。
        返回key使用 format_symbol 后的交易对（大写）。
        """
        if not symbols:
            return {}

        workers = max_workers
        if workers is None:
            workers = min(16, max(1, len(symbols)))

        result: Dict[str, pd.DataFrame] = {}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fut_map = {
                ex.submit(self.load_funding_rates, symbol, start_date, end_date): symbol
                for symbol in symbols
            }
            for fut in as_completed(fut_map):
                symbol = fut_map[fut]
                try:
                    result[format_symbol(symbol)] = fut.result()
                except Exception as e:
                    logger.error(
                        f"Bulk load funding rates failed for {symbol}: {e}",
                        exc_info=True,
                    )
                    result[format_symbol(symbol)] = pd.DataFrame()

        return result

    def save_premium_index_klines(
        self,
        symbol: str,
        df: Union[pd.DataFrame, pl.DataFrame],
        date: Optional[datetime] = None,
    ):
        """
        保存溢价指数K线数据（支持pandas和polars DataFrame）

        Args:
            symbol: 交易对
            df: 溢价指数K线DataFrame（pandas或polars）
            date: 日期，如果不指定，从df的open_time推断
        """
        try:
            # 转换为polars DataFrame
            if isinstance(df, pd.DataFrame):
                if df.empty:
                    return
                df_pl = pl.from_pandas(df)
            elif isinstance(df, pl.DataFrame):
                if df.is_empty():
                    return
                df_pl = df
            else:
                raise TypeError(f"Unsupported DataFrame type: {type(df)}")

            # 统一时间戳精度为纳秒（避免合并时的类型不兼容）
            timestamp_cols = ["open_time", "close_time"]
            for col in timestamp_cols:
                if col in df_pl.columns:
                    # 如果列存在，确保是纳秒精度
                    df_pl = df_pl.with_columns(
                        pl.col(col).cast(pl.Datetime("ns", time_zone="UTC"))
                    )

            # 确定日期：如果数据跨多天，需要按日期分组保存
            if date is None:
                if "open_time" in df_pl.columns:
                    # 按日期分组保存
                    dates_in_data = df_pl["open_time"].dt.date().unique().to_list()
                    if len(dates_in_data) > 1:
                        # 数据跨多天，需要分别保存
                        logger.debug(
                            f"Premium index kline data for {symbol} spans {len(dates_in_data)} days, saving to separate files"
                        )
                        for date_only in sorted(dates_in_data):
                            date_dt = datetime.combine(
                                date_only, datetime.min.time()
                            ).replace(tzinfo=timezone.utc)
                            # 过滤出该日期的数据
                            day_df = df_pl.filter(
                                pl.col("open_time").dt.date() == date_only
                            )
                            if not day_df.is_empty():
                                file_path = self._get_premium_index_file_path(
                                    symbol, date_dt
                                )
                                self._save_premium_index_klines_to_file(
                                    symbol, day_df, file_path
                                )
                        return  # 已按日期分组保存，直接返回
                    else:
                        # 单日数据，使用该日期
                        min_time = df_pl["open_time"].min()
                        if isinstance(min_time, datetime):
                            date = min_time.date()
                        else:
                            date = pd.to_datetime(min_time).to_pydatetime().date()
                        date = datetime.combine(date, datetime.min.time()).replace(
                            tzinfo=timezone.utc
                        )
                else:
                    date = datetime.now(timezone.utc)

            file_path = self._get_premium_index_file_path(symbol, date)

            # 保存
            self._save_premium_index_klines_to_file(symbol, df_pl, file_path)

        except Exception as e:
            logger.error(
                f"Failed to save premium index klines for {symbol}: {e}", exc_info=True
            )
            raise

    def _save_premium_index_klines_to_file(
        self, symbol: str, df_pl: pl.DataFrame, file_path: Path
    ):
        """保存溢价指数K线数据到文件（内部方法，处理文件合并）"""
        try:
            # 如果文件已存在，合并数据
            if file_path.exists():
                max_retries = 3
                retry_delay = 0.5
                for attempt in range(max_retries):
                    try:
                        existing_df_pl = self._load_dataframe_polars(file_path)
                        if not existing_df_pl.is_empty():
                            combined_df = pl.concat([existing_df_pl, df_pl])
                            combined_df = combined_df.unique(
                                subset=["open_time"], keep="last"
                            )
                            combined_df = combined_df.sort("open_time")
                            df_pl = combined_df
                        break
                    except (IOError, OSError, PermissionError) as e:
                        if attempt < max_retries - 1:
                            import time

                            time.sleep(retry_delay)
                            retry_delay *= 2
                        else:
                            logger.warning(
                                f"Failed to merge existing file {file_path} after {max_retries} attempts: {e}"
                            )
                    except Exception as e:
                        logger.warning(
                            f"Failed to merge existing file {file_path}: {e}"
                        )
                        break

            # 保存
            self._save_dataframe_polars(df_pl, file_path)
            logger.debug(
                f"Saved {len(df_pl)} premium index klines for {symbol} to {file_path}"
            )

        except Exception as e:
            logger.error(
                f"Failed to save premium index klines to file {file_path}: {e}",
                exc_info=True,
            )
            raise

    def load_premium_index_klines(
        self,
        symbol: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        加载溢价指数K线数据（返回pandas DataFrame保持兼容性）

        Args:
            symbol: 交易对
            start_date: 起始日期
            end_date: 结束日期

        Returns:
            溢价指数K线DataFrame（pandas格式）
        """
        try:
            symbol = format_symbol(symbol)
            symbol_dir = self.premium_index_dir / symbol

            if not symbol_dir.exists():
                return pd.DataFrame()

            # 收集所有相关文件
            all_files = []
            if start_date and end_date:
                current_date = start_date.date()
                end_date_only = end_date.date()
                while current_date <= end_date_only:
                    file_path = self._get_premium_index_file_path(
                        symbol,
                        datetime.combine(current_date, datetime.min.time()).replace(
                            tzinfo=timezone.utc
                        ),
                    )
                    if file_path.exists():
                        all_files.append(file_path)
                    current_date += timedelta(days=1)
            else:
                # 加载所有文件
                all_files = list(symbol_dir.glob(f"*.{self.storage_format}"))

            if not all_files:
                return pd.DataFrame()

            # 使用Polars加载所有文件并合并
            dfs_pl = []
            for file_path in sorted(all_files):
                try:
                    df_pl = self._load_dataframe_polars(file_path)
                    if not df_pl.is_empty():
                        dfs_pl.append(df_pl)
                except Exception as e:
                    logger.warning(f"Failed to load {file_path}: {e}")

            if not dfs_pl:
                return pd.DataFrame()

            combined_df_pl = pl.concat(dfs_pl)
            combined_df_pl = combined_df_pl.unique(subset=["open_time"], keep="last")
            combined_df_pl = combined_df_pl.sort("open_time")

            # 时间过滤
            if start_date or end_date:
                lazy_df = combined_df_pl.lazy()
                if start_date and "open_time" in combined_df_pl.columns:
                    lazy_df = lazy_df.filter(pl.col("open_time") >= start_date)
                if end_date and "close_time" in combined_df_pl.columns:
                    lazy_df = lazy_df.filter(pl.col("close_time") <= end_date)
                combined_df_pl = lazy_df.collect()

            # 转换为pandas DataFrame（保持兼容性）
            return combined_df_pl.to_pandas()

        except Exception as e:
            logger.error(
                f"Failed to load premium index klines for {symbol}: {e}", exc_info=True
            )
            return pd.DataFrame()

    def load_premium_index_klines_bulk(
        self,
        symbols: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_workers: Optional[int] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        批量加载多个交易对的溢价指数K线数据（并发 IO 加速）
        """
        if not symbols:
            return {}

        workers = max_workers
        if workers is None:
            workers = min(16, max(1, len(symbols)))

        result: Dict[str, pd.DataFrame] = {}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fut_map = {
                ex.submit(
                    self.load_premium_index_klines, symbol, start_date, end_date
                ): symbol
                for symbol in symbols
            }
            for fut in as_completed(fut_map):
                symbol = fut_map[fut]
                try:
                    result[format_symbol(symbol)] = fut.result()
                except Exception as e:
                    logger.error(
                        f"Bulk load premium index klines failed for {symbol}: {e}",
                        exc_info=True,
                    )
                    result[format_symbol(symbol)] = pd.DataFrame()

        return result


# 全局实例
_data_storage: Optional[DataStorage] = None


def get_data_storage() -> DataStorage:
    """获取数据存储实例"""
    global _data_storage
    if _data_storage is None:
        _data_storage = DataStorage()
    return _data_storage
