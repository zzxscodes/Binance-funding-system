"""
策略报告生成模块
生成作业要求的所有报告内容：选币逻辑、币种列表、资费结构、开仓时间点、策略磨损分析等
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import ensure_directory
from .equity_curve import get_equity_curve_tracker
from ..data.api import get_data_api
from ..system_api import get_system_api
from ..strategy.calculators import load_calculators

logger = get_logger("strategy_report")


class StrategyReportGenerator:
    """策略报告生成器"""

    def __init__(self):
        """初始化策略报告生成器"""
        # 优先使用配置文件中的路径，如果没有则使用默认路径
        data_dir = Path(config.get("data.data_directory", "data"))

        # 策略报告目录
        reports_dir = config.get("data.strategy_reports_directory")
        if reports_dir:
            self.reports_dir = Path(reports_dir)
        else:
            self.reports_dir = data_dir / "strategy_reports"
        ensure_directory(str(self.reports_dir))

        # 持仓历史数据文件目录
        position_history_dir = config.get("data.position_history_directory")
        if position_history_dir:
            self.position_history_dir = Path(position_history_dir)
        else:
            self.position_history_dir = data_dir / "position_history"
        ensure_directory(str(self.position_history_dir))

        self.equity_curve_tracker = get_equity_curve_tracker()
        self.data_api = get_data_api()
        self.system_api = get_system_api()

        # 可配置的历史条数上限（默认1000条）
        self.position_history_limit = config.get("monitoring.position_history_limit", 500)

        # 记录开仓历史（内存缓存）
        self._position_history: Dict[str, List[Dict]] = defaultdict(list)

        # 从文件加载历史数据（如果存在）
        self._load_position_history()

    def _load_position_history(self) -> None:
        """从文件加载持仓历史数据"""
        try:
            # 加载所有账户的历史数据文件
            for history_file in self.position_history_dir.glob(
                "*_position_history.json"
            ):
                account_id = history_file.stem.replace("_position_history", "")
                try:
                    with open(history_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        records = data.get("records", [])
                        # 加载后应用历史条数限制，只保留最近的N条
                        if len(records) > self.position_history_limit:
                            records = records[-self.position_history_limit:]
                            logger.debug(
                                f"Trimmed position history for {account_id} to {len(records)} records (limit: {self.position_history_limit})"
                            )
                        self._position_history[account_id] = records
                    logger.debug(
                        f"Loaded {len(self._position_history[account_id])} position records for {account_id}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to load position history from {history_file}: {e}"
                    )
        except Exception as e:
            logger.error(f"Failed to load position history: {e}", exc_info=True)

    def _trim_position_history(self, account_id: str) -> None:
        """修剪持仓历史数据，保持在配置的限制范围内"""
        if account_id in self._position_history:
            history = self._position_history[account_id]
            if len(history) > self.position_history_limit:
                # 保留最近的N条记录
                self._position_history[account_id] = history[-self.position_history_limit:]
                logger.debug(
                    f"Trimmed position history for {account_id} to {self.position_history_limit} records"
                )

    def _save_position_history(self, account_id: str) -> None:
        """保存持仓历史数据到文件"""
        try:
            history_file = (
                self.position_history_dir / f"{account_id}_position_history.json"
            )
            data = {
                "account_id": account_id,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "records": self._position_history.get(account_id, []),
            }
            with open(history_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.debug(
                f"Saved {len(data['records'])} position records for {account_id}"
            )
        except Exception as e:
            logger.error(
                f"Failed to save position history for {account_id}: {e}", exc_info=True
            )

    def record_position_opening(
        self,
        account_id: str,
        symbol: str,
        position_amt: float,
        entry_price: float,
        timestamp: datetime,
        funding_rate: Optional[float] = None,
        metadata: Optional[Dict] = None,
    ) -> None:
        """
        记录开仓信息（同时保存到内存和文件，支持跨进程共享）

        Args:
            account_id: 账户ID
            symbol: 交易对
            position_amt: 持仓数量（正数为多，负数为空）
            entry_price: 开仓价格
            timestamp: 开仓时间
            funding_rate: 开仓时的资金费率（可选）
            metadata: 额外元数据（可选）
        """
        try:
            record = {
                "symbol": symbol,
                "position_amt": float(position_amt),
                "entry_price": float(entry_price),
                "timestamp": timestamp.isoformat(),
                "funding_rate": float(funding_rate)
                if funding_rate is not None
                else None,
                "metadata": metadata or {},
            }

            # 添加到内存
            self._position_history[account_id].append(record)

            # 修剪历史记录，保持在限制范围内
            self._trim_position_history(account_id)

            # 立即保存到文件（支持跨进程共享）
            self._save_position_history(account_id)

            logger.info(
                f"Recorded position opening for {account_id}: {symbol} {position_amt} @ {entry_price}"
            )

        except Exception as e:
            logger.error(f"Failed to record position opening: {e}", exc_info=True)

    def _get_funding_rate_structure(
        self, symbols: List[str], start_date: datetime, end_date: datetime
    ) -> Dict[str, Dict]:
        """
        获取各币种的资金费率结构

        Returns:
            Dict[symbol, Dict]包含资金费率统计信息
        """
        try:
            begin_label = self.data_api._get_date_time_label_from_datetime(start_date)
            end_label = self.data_api._get_date_time_label_from_datetime(end_date)

            funding_rates_map = self.data_api.get_funding_rate_between(
                begin_label, end_label
            )

            result = {}
            for symbol in symbols:
                if symbol not in funding_rates_map:
                    # 没有数据，记录为无数据（可能是未采集到或真的没有数据）
                    result[symbol] = {
                        "mean_rate": None,
                        "std_rate": None,
                        "min_rate": None,
                        "max_rate": None,
                        "latest_rate": None,
                        "data_points": 0,
                        "rate_history": [],
                        "status": "no_data",
                        "message": "该时间范围内无资金费率数据（可能未采集到最新数据）",
                    }
                    logger.debug(
                        f"No funding rate data found for {symbol} in time range {start_date.date()} to {end_date.date()}"
                    )
                    continue

                df = funding_rates_map[symbol]
                if df.empty or "fundingRate" not in df.columns:
                    # 数据为空或格式不正确
                    result[symbol] = {
                        "mean_rate": None,
                        "std_rate": None,
                        "min_rate": None,
                        "max_rate": None,
                        "latest_rate": None,
                        "data_points": 0,
                        "rate_history": [],
                        "status": "empty_data",
                        "message": "数据为空或格式不正确",
                    }
                    continue

                rates = pd.to_numeric(df["fundingRate"], errors="coerce").dropna()
                if len(rates) == 0:
                    # 费率数据无效
                    result[symbol] = {
                        "mean_rate": None,
                        "std_rate": None,
                        "min_rate": None,
                        "max_rate": None,
                        "latest_rate": None,
                        "data_points": 0,
                        "rate_history": [],
                        "status": "invalid_data",
                        "message": "费率数据无效",
                    }
                    continue

                result[symbol] = {
                    "mean_rate": float(rates.mean()),
                    "std_rate": float(rates.std()),
                    "min_rate": float(rates.min()),
                    "max_rate": float(rates.max()),
                    "latest_rate": float(rates.iloc[-1]),
                    "data_points": len(rates),
                    "rate_history": rates.tolist()[:20],  # 保留最近20个费率
                    "status": "ok",
                }

            return result

        except Exception as e:
            logger.error(f"Failed to get funding rate structure: {e}", exc_info=True)
            return {}

    async def _calculate_strategy_wear(
        self,
        account_id: str,
        start_date: datetime,
        end_date: datetime,
        position_openings: Optional[List[Dict]] = None,
    ) -> Dict:
        """
        计算策略磨损（手续费、滑点、费率等产生的收益损耗）

        Returns:
            Dict包含磨损分析
        """
        try:
            # 获取账户信息
            account_info = await self.system_api.get_account_info(account=account_id)
            positions = await self.system_api.get_position_info(account=account_id)

            # 获取Equity Curve
            equity_df = self.equity_curve_tracker.get_equity_curve(
                account_id, start_date, end_date
            )

            if equity_df.empty:
                return {
                    "total_fees_estimate": 0.0,
                    "slippage_estimate": 0.0,
                    "funding_fees_estimate": 0.0,
                    "total_wear": 0.0,
                    "wear_pct": 0.0,
                }

            # 使用真实交易记录计算策略磨损（从position_openings获取真实交易数据）
            # position_openings应该已经是过滤后的（只包含报告时间范围内的记录）
            if position_openings is None:
                # 如果未传入，则从历史记录中过滤
                all_openings = self._position_history.get(account_id, [])
                position_openings = []
                for opening in all_openings:
                    try:
                        opening_time = datetime.fromisoformat(
                            opening["timestamp"].replace("Z", "+00:00")
                        )
                        if start_date <= opening_time <= end_date:
                            position_openings.append(opening)
                    except Exception:
                        continue

            # 交易次数 = 策略执行期间（报告时间范围内）的开仓次数
            total_trades = len(position_openings)

            # 从真实交易记录计算手续费和滑点
            total_fees_actual = 0.0
            total_slippage_actual = 0.0

            # 从配置读取费率（如果未配置则使用默认值）
            fee_rate = config.get(
                "strategy.report.fee_rate", 0.0004
            )  # 0.04% per trade (open + close)
            slippage_rate = config.get("strategy.report.slippage_rate", 0.0001)  # 0.01%

            # 从真实交易记录计算
            for record in position_openings:
                position_value = abs(record.get("position_amt", 0)) * record.get(
                    "entry_price", 0
                )
                if position_value > 0:
                    # 手续费：每次开仓/平仓双向
                    total_fees_actual += position_value * fee_rate * 2  # 开仓+平仓
                    # 滑点
                    total_slippage_actual += position_value * slippage_rate

            # 计算真实资金费率成本（根据持仓历史）
            funding_fees_actual = 0.0
            for record in position_openings:
                if record.get("funding_rate") is not None:
                    position_value = abs(record.get("position_amt", 0)) * record.get(
                        "entry_price", 0
                    )
                    if position_value > 0:
                        # 每8小时收取一次资金费率
                        try:
                            record_time = datetime.fromisoformat(
                                record["timestamp"].replace("Z", "+00:00")
                            )
                            days_held = (
                                end_date - record_time
                            ).total_seconds() / 86400.0
                            funding_periods = max(
                                1, int(days_held * 3)
                            )  # 每天3次（每8小时）
                            funding_fees_actual += (
                                position_value
                                * abs(record["funding_rate"])
                                * funding_periods
                            )
                        except Exception as e:
                            logger.debug(
                                f"Failed to calculate funding fees for record: {e}"
                            )

            total_wear = total_fees_actual + total_slippage_actual + funding_fees_actual

            # 计算磨损百分比（相对于初始净值）
            if not equity_df.empty and "equity" in equity_df.columns:
                initial_equity = float(equity_df["equity"].iloc[0])
            elif account_info:
                # 如果Equity Curve为空，使用账户余额作为初始净值
                initial_equity = float(
                    account_info.get("totalWalletBalance", 0)
                    or account_info.get("totalMarginBalance", 0)
                    or 0
                )
            else:
                initial_equity = 0.0

            wear_pct = (
                (total_wear / initial_equity * 100.0) if initial_equity > 0 else 0.0
            )

            return {
                "total_fees": total_fees_actual,  # 真实手续费
                "slippage": total_slippage_actual,  # 真实滑点
                "funding_fees": funding_fees_actual,  # 真实资金费率成本
                "total_wear": total_wear,  # 总磨损
                "wear_pct": wear_pct,  # 磨损百分比
                "initial_equity": initial_equity,  # 初始净值
                "total_trades": total_trades,  # 交易次数
            }

        except Exception as e:
            logger.error(f"Failed to calculate strategy wear: {e}", exc_info=True)
            return {}

    async def generate_report(
        self,
        account_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict:
        """
        生成完整的策略报告

        Args:
            account_id: 账户ID
            start_date: 开始日期（可选，默认3天前）
            end_date: 结束日期（可选，默认现在）

        Returns:
            完整的策略报告字典
        """
        try:
            if end_date is None:
                end_date = datetime.now(timezone.utc)
            if start_date is None:
                start_date = end_date - timedelta(days=3)

            # 1. Equity Curve数据
            equity_df = self.equity_curve_tracker.get_equity_curve(
                account_id, start_date, end_date
            )
            equity_summary = self.equity_curve_tracker.get_summary(account_id)

            # 2. 获取持仓信息
            positions = await self.system_api.get_position_info(account=account_id)
            current_symbols = [
                p["system_symbol"]
                for p in positions
                if abs(float(p.get("position_amt", 0))) > 1e-8
            ]

            # 3. 资金费率结构
            # 资金费率结构需要使用更长的历史数据（7天）来统计，而不是程序执行期间
            # 因为资金费率需要足够的历史数据才能计算平均值、标准差等统计指标
            funding_start_date = end_date - timedelta(days=7)  # 使用最近7天的数据
            funding_structure = self._get_funding_rate_structure(
                current_symbols, funding_start_date, end_date
            )

            # 4. 开仓时间点（从文件重新加载，确保获取最新数据）
            self._load_position_history()
            all_position_openings = self._position_history.get(account_id, [])

            # 过滤时间范围：只统计策略执行期间（报告时间范围内）的开仓记录
            position_openings = []
            if all_position_openings:
                for opening in all_position_openings:
                    try:
                        opening_time = datetime.fromisoformat(
                            opening["timestamp"].replace("Z", "+00:00")
                        )
                        # 只统计报告时间范围内的开仓记录（策略执行期间）
                        if start_date <= opening_time <= end_date:
                            position_openings.append(opening)
                    except Exception as e:
                        logger.debug(f"Failed to parse timestamp for opening: {e}")
                        continue

            # 记录过滤后的开仓次数（用于日志）
            logger.info(f"Strategy execution period: {start_date} to {end_date}")
            logger.info(
                f"Filtered position openings: {len(position_openings)} out of {len(all_position_openings)} total (strategy execution period)"
            )

            # 5. 策略磨损分析
            wear_analysis = await self._calculate_strategy_wear(
                account_id, start_date, end_date, position_openings
            )

            # 6. 获取账户信息用于metadata
            account_info = await self.system_api.get_account_info(account=account_id)
            initial_capital = 0.0
            if account_info:
                initial_capital = float(
                    account_info.get("totalWalletBalance", 0)
                    or account_info.get("totalMarginBalance", 0)
                    or 0
                )
            # 如果账户信息为空，尝试从Equity Curve获取
            if (
                initial_capital <= 0
                and not equity_df.empty
                and "equity" in equity_df.columns
            ):
                initial_capital = float(equity_df["equity"].iloc[0])

            # 7. 从calculator获取策略参数（合并所有calculator的参数）
            calculator_params = self._get_calculator_params()

            # 构建完整报告（只包含实时计算的数值，不包含描述性文字）
            report = {
                "account_id": account_id,
                "report_time": datetime.now(timezone.utc).isoformat(),
                "period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat(),
                    "days": (end_date - start_date).days,
                },
                "equity_curve": {
                    "summary": equity_summary,
                    "data_points": len(equity_df),
                    "data": self._convert_dataframe_to_dict(equity_df)
                    if not equity_df.empty
                    else [],
                },
                "coin_selection": {
                    "symbols": current_symbols,
                    "symbols_count": len(current_symbols),
                    "parameters": calculator_params,  # 从calculator获取的参数
                },
                "funding_rate_structure": funding_structure,
                "position_openings": position_openings,
                "strategy_wear": wear_analysis,
                "metadata": {
                    "initial_capital": initial_capital,
                    "calculator_names": [calc.name for calc in load_calculators()],
                },
            }

            # 保存报告（覆盖模式：每个账户只保留一个最新报告）
            self._save_report(account_id, report, overwrite=True)

            return report

        except Exception as e:
            logger.error(f"Failed to generate strategy report: {e}", exc_info=True)
            return {}

    def _convert_dataframe_to_dict(self, df: pd.DataFrame) -> List[Dict]:
        """将DataFrame转换为字典列表，处理Timestamp序列化"""
        import pandas as pd

        df_copy = df.copy()
        # 将Timestamp列转换为字符串
        for col in df_copy.columns:
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return df_copy.to_dict("records")

    def _save_report(
        self, account_id: str, report: Dict, overwrite: bool = False
    ) -> None:
        """保存报告到文件"""
        try:
            if overwrite:
                # 覆盖模式：每个账户只保留一个最新报告
                filename = f"{account_id}_strategy_report_latest.json"
                file_path = self.reports_dir / filename

                # 删除该账户的旧报告文件（如果有）
                for old_file in self.reports_dir.glob(
                    f"{account_id}_strategy_report_*.json"
                ):
                    if old_file != file_path:
                        try:
                            old_file.unlink()
                        except Exception as e:
                            logger.warning(
                                f"Failed to delete old report {old_file}: {e}"
                            )
            else:
                # 时间戳模式：每次生成新文件
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                filename = f"{account_id}_strategy_report_{timestamp}.json"
                file_path = self.reports_dir / filename

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)

            logger.info(f"Saved strategy report for {account_id}: {file_path}")

        except Exception as e:
            logger.error(f"Failed to save strategy report: {e}", exc_info=True)

    def _get_calculator_params(self) -> Dict:
        """
        从所有calculator获取策略参数（合并多个calculator的参数）

        Returns:
            合并后的参数字典
        """
        try:
            calculators = load_calculators()
            if not calculators:
                return {}

            # 合并所有calculator的参数
            merged_params = {}
            for calc in calculators:
                calc_desc = calc.get_description()
                calc_params = calc_desc.get("parameters", {})

                # 合并参数（如果有多个calculator，取平均值或第一个值）
                for key, value in calc_params.items():
                    if key not in merged_params:
                        merged_params[key] = value
                    elif isinstance(value, (int, float)) and isinstance(
                        merged_params[key], (int, float)
                    ):
                        # 数值类型取平均值
                        merged_params[key] = (merged_params[key] + value) / 2
                    # 其他类型保持第一个值

            # 添加策略方向（如果有多个calculator，取第一个）
            strategy_directions = [
                calc.get_description().get("strategy_direction")
                for calc in calculators
                if calc.get_description().get("strategy_direction")
            ]
            if strategy_directions:
                merged_params["strategy_direction"] = strategy_directions[0]

            return merged_params

        except Exception as e:
            logger.warning(f"Failed to get calculator params: {e}")
            return {}

    def get_latest_report(self, account_id: str) -> Optional[Dict]:
        """获取最新的策略报告"""
        try:
            # 查找最新的报告文件
            pattern = f"{account_id}_strategy_report_*.json"
            report_files = sorted(self.reports_dir.glob(pattern), reverse=True)

            if not report_files:
                return None

            with open(report_files[0], "r", encoding="utf-8") as f:
                return json.load(f)

        except Exception as e:
            logger.error(f"Failed to get latest report: {e}", exc_info=True)
            return None


# 全局实例
_strategy_report_generator: Optional[StrategyReportGenerator] = None


def get_strategy_report_generator() -> StrategyReportGenerator:
    """获取策略报告生成器实例"""
    global _strategy_report_generator
    if _strategy_report_generator is None:
        _strategy_report_generator = StrategyReportGenerator()
    return _strategy_report_generator
