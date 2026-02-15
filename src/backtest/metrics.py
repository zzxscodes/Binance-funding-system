"""
回测评估指标模块

提供两类评估指标：
1. FactorMetrics - 单因子(Calculator)评估指标
2. AlphaMetrics - 整体Alpha(因子组合)评估指标

因子评估指标:
- IC (Information Coefficient) - 信息系数
- Rank IC - 秩相关系数
- ICIR - IC均值/IC标准差
- Selection Accuracy - 选币准确率
- Long/Short Spread - 多空spread
- Turnover - 换手率
- Decay - 因子衰减
- Group Rank - 分组收益

Alpha评估指标:
- 收益指标 - 总收益、年化收益、月度收益
- 风险指标 - 波动率、夏普比率、索提诺比率、最大回撤
- 交易统计 - 胜率、盈亏比、交易次数
- 位置统计 - 多空比率、换手率
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
from collections import defaultdict


@dataclass
class FactorMetrics:
    """
    单因子评估指标
    
    用于评估单个因子(Calculator)的预测能力
    """
    name: str = ""
    
    # IC相关指标
    ic: float = 0.0  # 信息系数
    ic_mean: float = 0.0  # IC均值
    ic_std: float = 0.0  # IC标准差
    icir: float = 0.0  # ICIR = IC均值/IC标准差
    ic_series: List[float] = field(default_factory=list)  # IC时间序列
    rank_ic: float = 0.0  # 秩相关系数
    rank_ic_mean: float = 0.0  # Rank IC均值
    rank_ic_std: float = 0.0  # Rank IC标准差
    rank_icir: float = 0.0  # Rank ICIR
    rank_ic_series: List[float] = field(default_factory=list)  # Rank IC时间序列
    
    # 选币能力指标
    selection_accuracy: float = 0.0  # 选币准确率
    long_spread: float = 0.0  # 做多组 vs 做空组收益差
    short_spread: float = 0.0  # 做空组 vs 做多组
    top_group_return: float = 0.0  # 最高分组收益
    bottom_group_return: float = 0.0  # 最低分组收益
    group_return_spread: float = 0.0  # 分组收益差
    group_returns: List[float] = field(default_factory=list)  # 各分组平均收益
    
    # 稳定性指标
    turnover: float = 0.0  # 换手率
    decay_half_life: float = 0.0  # 衰减半衰期
    stability: float = 0.0  # 稳定性
    
    # 收益贡献
    avg_weight: float = 0.0  # 平均权重
    weight_std: float = 0.0  # 权重标准差
    direction_accuracy: float = 0.0  # 方向预测准确率
    
    # 时间序列统计
    period_count: int = 0  # 评估期数
    valid_periods: int = 0  # 有效期数
    
    def to_dict(self) -> Dict:
        return {
            "因子名称": self.name,
            "IC指标": {
                "IC": f"{self.ic_mean:.4f}",
                "IC标准差": f"{self.ic_std:.4f}",
                "ICIR": f"{self.icir:.4f}",
                "Rank IC": f"{self.rank_ic_mean:.4f}",
                "Rank IC标准差": f"{self.rank_ic_std:.4f}",
                "Rank ICIR": f"{self.rank_icir:.4f}",
            },
            "选币能力": {
                "选币准确率": f"{self.selection_accuracy:.2f}%",
                "多做Spread": f"{self.long_spread:.4f}",
                "做空Spread": f"{self.short_spread:.4f}",
                "最高分组收益": f"{self.top_group_return:.4f}",
                "最低分组收益": f"{self.bottom_group_return:.4f}",
                "分组收益差": f"{self.group_return_spread:.4f}",
            },
            "稳定性": {
                "换手率": f"{self.turnover:.2f}%",
                "衰减半衰期": f"{self.decay_half_life:.1f}期",
                "稳定性": f"{self.stability:.4f}",
            },
            "其他": {
                "平均权重": f"{self.avg_weight:.4f}",
                "权重标准差": f"{self.weight_std:.4f}",
                "方向准确率": f"{self.direction_accuracy:.2f}%",
                "评估期数": self.period_count,
                "有效期数": self.valid_periods,
            }
        }
    
    def summary(self) -> Dict:
        return {
            "name": self.name,
            "ic": self.ic_mean,
            "icir": self.icir,
            "rank_ic": self.rank_ic_mean,
            "rank_icir": self.rank_icir,
            "selection_accuracy": self.selection_accuracy,
            "long_spread": self.long_spread,
            "group_return_spread": self.group_return_spread,
            "turnover": self.turnover,
        }


@dataclass
class AlphaMetrics:
    """
    整体Alpha评估指标
    
    用于评估多因子组合的整体表现
    """
    name: str = ""
    
    # 收益指标
    total_return: float = 0.0  # 总收益率
    annual_return: float = 0.0  # 年化收益率
    monthly_returns: List[float] = field(default_factory=list)  # 月度收益
    daily_returns: List[float] = field(default_factory=list)  # 日度收益
    
    # 风险指标
    volatility: float = 0.0  # 年化波动率
    downside_volatility: float = 0.0  # 下行波动率
    sharpe_ratio: float = 0.0  # 夏普比率
    sortino_ratio: float = 0.0  # 索提诺比率
    calmar_ratio: float = 0.0  # 卡玛比率
    
    # 回撤指标
    max_drawdown: float = 0.0  # 最大回撤
    max_drawdown_duration: int = 0  # 最大回撤持续期数
    avg_drawdown: float = 0.0  # 平均回撤
    drawdown_recovery: float = 0.0  # 回撤恢复时间
    
    # 交易统计
    total_trades: int = 0  # 总交易数
    winning_trades: int = 0  # 盈利交易数
    losing_trades: int = 0  # 亏损交易数
    win_rate: float = 0.0  # 胜率
    profit_factor: float = 0.0  # 盈亏比
    avg_trade: float = 0.0  # 平均每笔收益
    avg_winning_trade: float = 0.0  # 平均盈利
    avg_losing_trade: float = 0.0  # 平均亏损
    best_trade: float = 0.0  # 最佳交易
    worst_trade: float = 0.0  # 最差交易
    
    # 多空统计
    long_trades: int = 0  # 做多交易数
    short_trades: int = 0  # 做空交易数
    long_win_rate: float = 0.0  # 做多胜率
    short_win_rate: float = 0.0  # 做空胜率
    long_return: float = 0.0  # 做多总收益
    short_return: float = 0.0  # 做空总收益
    
    # 位置和换手
    avg_position_size: float = 0.0  # 平均持仓比例
    turnover_rate: float = 0.0  # 换手率
    avg_holding_periods: float = 0.0  # 平均持仓期数
    
    # 稳定性指标
    skewness: float = 0.0  # 偏度
    kurtosis: float = 0.0  # 峰度
    win_loss_streak: int = 0  # 最长连胜
    loss_win_streak: int = 0  # 最长连败
    
    # 其他
    information_ratio: float = 0.0  # 信息比率
    beta: float = 0.0  # Beta
    alpha: float = 0.0  # Alpha
    correlation_benchmark: float = 0.0  # 与基准相关性
    
    def to_dict(self) -> Dict:
        return {
            "策略名称": self.name,
            "收益指标": {
                "总收益率": f"{self.total_return:.2f}%",
                "年化收益率": f"{self.annual_return:.2f}%",
                "年化波动率": f"{self.volatility:.2f}%",
                "月度收益标准差": f"{np.std(self.monthly_returns) * 100:.2f}%" if self.monthly_returns else "N/A",
            },
            "风险调整收益": {
                "夏普比率": f"{self.sharpe_ratio:.2f}",
                "索提诺比率": f"{self.sortino_ratio:.2f}",
                "卡玛比率": f"{self.calmar_ratio:.2f}",
                "信息比率": f"{self.information_ratio:.2f}",
            },
            "回撤指标": {
                "最大回撤": f"{self.max_drawdown:.2f}%",
                "最大回撤持续期": f"{self.max_drawdown_duration}期",
                "平均回撤": f"{self.avg_drawdown:.2f}%",
            },
            "交易统计": {
                "总交易数": self.total_trades,
                "盈利交易": self.winning_trades,
                "亏损交易": self.losing_trades,
                "胜率": f"{self.win_rate:.2f}%",
                "盈亏比": f"{self.profit_factor:.2f}",
                "平均每笔": f"{self.avg_trade:.2f}%",
                "最佳交易": f"{self.best_trade:.2f}%",
                "最差交易": f"{self.worst_trade:.2f}%",
            },
            "多空表现": {
                "做多交易数": self.long_trades,
                "做空交易数": self.short_trades,
                "做多胜率": f"{self.long_win_rate:.2f}%",
                "做空胜率": f"{self.short_win_rate:.2f}%",
                "做多总收益": f"{self.long_return:.2f}%",
                "做空总收益": f"{self.short_return:.2f}%",
            },
            "持仓统计": {
                "平均持仓比例": f"{self.avg_position_size:.2f}%",
                "换手率": f"{self.turnover_rate:.2f}%",
                "平均持仓期数": f"{self.avg_holding_periods:.1f}",
            },
            "分布特征": {
                "偏度": f"{self.skewness:.4f}",
                "峰度": f"{self.kurtosis:.4f}",
                "最长连胜": self.win_loss_streak,
                "最长连败": self.loss_win_streak,
            }
        }
    
    def summary(self) -> Dict:
        return {
            "name": self.name,
            "total_return": self.total_return,
            "annual_return": self.annual_return,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "max_drawdown": self.max_drawdown,
            "win_rate": self.win_rate,
            "profit_factor": self.profit_factor,
            "total_trades": self.total_trades,
        }


class FactorEvaluator:
    """
    因子评估器
    
    用于计算单个因子的各种评估指标
    """
    
    @staticmethod
    def calculate_ic(
        factor_values: pd.Series,
        returns: pd.Series,
        method: str = "spearman"
    ) -> float:
        """
        计算信息系数 (Information Coefficient)
        
        Args:
            factor_values: 因子值
            returns: 未来收益
            method: 相关性方法 ("pearson" or "spearman")
        
        Returns:
            IC值
        """
        if len(factor_values) < 10:
            return 0.0
        
        common_idx = factor_values.index.intersection(returns.index)
        if len(common_idx) < 10:
            return 0.0
        
        f = factor_values.loc[common_idx].dropna()
        r = returns.loc[common_idx].dropna()
        
        common_idx = f.index.intersection(r.index)
        if len(common_idx) < 10:
            return 0.0
        
        f = f.loc[common_idx]
        r = r.loc[common_idx]
        
        if method == "spearman":
            return f.corr(r, method="spearman")
        else:
            return f.corr(r, method="pearson")
    
    @staticmethod
    def calculate_ic_series(
        factor_data: Dict[datetime, pd.Series],
        returns_data: Dict[datetime, pd.Series],
        method: str = "spearman"
    ) -> List[float]:
        """
        计算时间序列IC
        
        Args:
            factor_data: {timestamp: factor_series}
            returns_data: {timestamp: returns_series}
            method: 相关性方法
        
        Returns:
            IC时间序列
        """
        ic_series = []
        for ts in factor_data.keys():
            if ts in returns_data:
                ic = FactorEvaluator.calculate_ic(
                    factor_data[ts],
                    returns_data[ts],
                    method
                )
                if not np.isnan(ic):
                    ic_series.append(ic)
        return ic_series
    
    @staticmethod
    def evaluate_factor(
        name: str,
        factor_weights: Dict[datetime, Dict[str, float]],
        next_returns: Dict[datetime, Dict[str, float]],
        groups: int = 5,
    ) -> FactorMetrics:
        """
        综合评估因子
        
        Args:
            name: 因子名称
            factor_weights: {timestamp: {symbol: weight}}
            next_returns: {timestamp: {symbol: next_return}}
            groups: 分组数量
        
        Returns:
            FactorMetrics
        """
        metrics = FactorMetrics(name=name)
        
        if not factor_weights or not next_returns:
            return metrics
        
        # 计算IC时间序列
        ic_list = []
        rank_ic_list = []
        valid_periods = 0
        
        for ts in sorted(factor_weights.keys()):
            if ts in next_returns:
                weights = factor_weights[ts]
                returns = next_returns[ts]
                
                if not weights or not returns:
                    continue
                
                common_symbols = set(weights.keys()) & set(returns.keys())
                if len(common_symbols) < 10:
                    continue
                
                f_series = pd.Series({s: weights[s] for s in common_symbols})
                r_series = pd.Series({s: returns[s] for s in common_symbols})
                
                ic = FactorEvaluator.calculate_ic(f_series, r_series, "pearson")
                rank_ic = FactorEvaluator.calculate_ic(f_series, r_series, "spearman")
                
                if not np.isnan(ic):
                    ic_list.append(ic)
                if not np.isnan(rank_ic):
                    rank_ic_list.append(rank_ic)
                
                valid_periods += 1
        
        metrics.period_count = len(factor_weights)
        metrics.valid_periods = valid_periods
        
        # IC统计
        if ic_list:
            metrics.ic_mean = np.mean(ic_list)
            metrics.ic_std = np.std(ic_list)
            metrics.icir = metrics.ic_mean / metrics.ic_std if metrics.ic_std > 0 else 0
            metrics.ic = metrics.ic_mean
            metrics.ic_series = ic_list
        
        if rank_ic_list:
            metrics.rank_ic_mean = np.mean(rank_ic_list)
            metrics.rank_ic_std = np.std(rank_ic_list)
            metrics.rank_icir = metrics.rank_ic_mean / metrics.rank_ic_std if metrics.rank_ic_std > 0 else 0
            metrics.rank_ic = metrics.rank_ic_mean
            metrics.rank_ic_series = rank_ic_list
        
        # 计算分组收益
        group_returns = [[] for _ in range(groups)]
        all_weights = []
        
        for ts in sorted(factor_weights.keys()):
            if ts in next_returns:
                weights = factor_weights[ts]
                returns = next_returns[ts]
                
                common = list(set(weights.keys()) & set(returns.keys()))
                if len(common) < groups:
                    continue
                
                sorted_symbols = sorted(common, key=lambda s: weights.get(s, 0), reverse=True)
                group_size = len(sorted_symbols) // groups
                
                for i in range(groups):
                    start = i * group_size
                    end = start + group_size if i < groups - 1 else len(sorted_symbols)
                    group_symbols = sorted_symbols[start:end]
                    
                    if group_symbols:
                        avg_ret = np.mean([returns[s] for s in group_symbols])
                        group_returns[i].append(avg_ret)
                
                for s in common:
                    all_weights.append(weights.get(s, 0))
        
        # 分组收益统计
        if group_returns[0]:
            metrics.top_group_return = np.mean(group_returns[0])
        if group_returns[-1]:
            metrics.bottom_group_return = np.mean(group_returns[-1])
        if metrics.top_group_return and metrics.bottom_group_return:
            metrics.group_return_spread = metrics.top_group_return - metrics.bottom_group_return
        
        # 保存分组平均收益
        metrics.group_returns = [np.mean(r) if r else 0 for r in group_returns]
        
        # 选币准确率 (用分组差作为代理)
        if metrics.group_return_spread > 0:
            metrics.selection_accuracy = min(100, metrics.group_return_spread * 100)
        
        # 多空spread
        if group_returns[0] and groups >= 2:
            long_ret = np.mean(group_returns[0])
            short_ret = np.mean(group_returns[-1]) if group_returns[-1] else 0
            metrics.long_spread = long_ret - short_ret
        
        # 权重统计
        if all_weights:
            metrics.avg_weight = np.mean(all_weights)
            metrics.weight_std = np.std(all_weights)
        
        # 换手率 (简化版: 权重变化比例)
        if len(factor_weights) >= 2:
            turnover_list = []
            prev_weights = None
            for ts in sorted(factor_weights.keys())[:100]:  # 最多取100个样本
                curr_weights = factor_weights[ts]
                if prev_weights is not None:
                    common = set(prev_weights.keys()) & set(curr_weights.keys())
                    if common:
                        diff = sum(abs(curr_weights.get(s, 0) - prev_weights.get(s, 0)) for s in common)
                        turnover_list.append(diff)
                prev_weights = curr_weights
            if turnover_list:
                metrics.turnover = np.mean(turnover_list) * 100
        
        return metrics


class AlphaEvaluator:
    """
    Alpha评估器
    
    用于计算整体策略的评估指标
    """
    
    TRADING_DAYS_PER_YEAR = 252
    
    @staticmethod
    def calculate_returns_series(portfolio_values: List[float]) -> List[float]:
        """计算收益率序列"""
        if len(portfolio_values) < 2:
            return []
        
        returns = []
        for i in range(1, len(portfolio_values)):
            ret = (portfolio_values[i] - portfolio_values[i-1]) / portfolio_values[i-1]
            returns.append(ret)
        return returns
    
    @staticmethod
    def calculate_max_drawdown(portfolio_values: List[float]) -> Tuple[float, int]:
        """
        计算最大回撤和持续期数
        
        Returns:
            (最大回撤, 持续期数)
        """
        if len(portfolio_values) < 2:
            return 0.0, 0
        
        max_dd = 0.0
        max_dd_duration = 0
        current_dd_duration = 0
        
        cummax = np.maximum.accumulate(portfolio_values)
        drawdowns = (np.array(portfolio_values) - cummax) / cummax
        
        for i in range(1, len(drawdowns)):
            if drawdowns[i] < max_dd:
                max_dd = drawdowns[i]
            
            if drawdowns[i] < 0:
                current_dd_duration += 1
                if current_dd_duration > max_dd_duration:
                    max_dd_duration = current_dd_duration
            else:
                current_dd_duration = 0
        
        return abs(max_dd) * 100, max_dd_duration
    
    @staticmethod
    def evaluate_alpha(
        name: str,
        portfolio_values: List[float],
        trades: List[Dict] = None,
        initial_balance: Optional[float] = None,
        period: str = "daily",
    ) -> AlphaMetrics:
        """
        综合评估Alpha策略
        
        Args:
            name: 策略名称
            portfolio_values: 组合净值时间序列
            trades: 交易记录列表
            initial_balance: 初始资金
            period: 周期 ("daily" or "period")
        
        Returns:
            AlphaMetrics
        """
        metrics = AlphaMetrics(name=name)
        
        if len(portfolio_values) < 2:
            return metrics
        
        returns = AlphaEvaluator.calculate_returns_series(portfolio_values)
        
        if not returns:
            return metrics
        
        metrics.daily_returns = returns
        
        # 收益指标
        final_value = portfolio_values[-1]
        metrics.total_return = (final_value - initial_balance) / initial_balance * 100
        
        periods_per_year = AlphaEvaluator.TRADING_DAYS_PER_YEAR if period == "daily" else 12
        n_periods = len(returns)
        years = n_periods / periods_per_year
        
        if years > 0:
            metrics.annual_return = ((final_value / initial_balance) ** (1/years) - 1) * 100
        
        # 月度收益
        if period == "daily":
            df = pd.DataFrame({
                'date': range(len(portfolio_values)),
                'value': portfolio_values
            })
            # 简化处理，按固定天数分月
            days_per_month = 21
            for i in range(0, len(portfolio_values), days_per_month):
                if i + days_per_month < len(portfolio_values):
                    month_ret = (portfolio_values[min(i + days_per_month, len(portfolio_values)-1)] - portfolio_values[i]) / portfolio_values[i]
                    metrics.monthly_returns.append(month_ret * 100)
        
        # 风险指标
        if len(returns) > 1:
            daily_vol = np.std(returns)
            metrics.volatility = daily_vol * np.sqrt(periods_per_year) * 100
            
            negative_returns = [r for r in returns if r < 0]
            if negative_returns:
                downside_vol = np.std(negative_returns)
                metrics.downside_volatility = downside_vol * np.sqrt(periods_per_year) * 100
            
            # 夏普比率
            if metrics.volatility > 0:
                metrics.sharpe_ratio = (np.mean(returns) * periods_per_year) / (daily_vol * np.sqrt(periods_per_year))
            
            # 索提诺比率
            if metrics.downside_volatility > 0:
                metrics.sortino_ratio = (np.mean(returns) * periods_per_year) / (metrics.downside_volatility / 100)
        
        # 回撤指标
        metrics.max_drawdown, metrics.max_drawdown_duration = AlphaEvaluator.calculate_max_drawdown(portfolio_values)
        
        cummax = np.maximum.accumulate(portfolio_values)
        drawdowns = (np.array(portfolio_values) - cummax) / cummax * 100
        metrics.avg_drawdown = np.mean([abs(d) for d in drawdowns if d < 0])
        
        # 卡玛比率
        if metrics.max_drawdown > 0:
            metrics.calmar_ratio = metrics.annual_return / metrics.max_drawdown
        
        # 交易统计
        if trades:
            metrics.total_trades = len(trades)
            
            winning = [t for t in trades if t.get('pnl', 0) > 0]
            losing = [t for t in trades if t.get('pnl', 0) < 0]
            
            metrics.winning_trades = len(winning)
            metrics.losing_trades = len(losing)
            
            if metrics.total_trades > 0:
                metrics.win_rate = len(winning) / metrics.total_trades * 100
            
            gross_profit = sum(t.get('pnl', 0) for t in winning) if winning else 0
            gross_loss = abs(sum(t.get('pnl', 0) for t in losing)) if losing else 0
            
            if gross_loss > 0:
                metrics.profit_factor = gross_profit / gross_loss
            
            if trades:
                pnls = [t.get('pnl', 0) for t in trades]
                metrics.avg_trade = np.mean(pnls) / initial_balance * 100 if initial_balance > 0 else 0
                metrics.avg_winning_trade = np.mean([t.get('pnl', 0) for t in winning]) if winning else 0
                metrics.avg_losing_trade = np.mean([t.get('pnl', 0) for t in losing]) if losing else 0
                metrics.best_trade = max(pnls) / initial_balance * 100 if pnls else 0
                metrics.worst_trade = min(pnls) / initial_balance * 100 if pnls else 0
            
            # 多空统计
            long_trades = [t for t in trades if t.get('side', '').upper() == 'LONG']
            short_trades = [t for t in trades if t.get('side', '').upper() == 'SHORT']
            
            metrics.long_trades = len(long_trades)
            metrics.short_trades = len(short_trades)
            
            if long_trades:
                metrics.long_win_rate = len([t for t in long_trades if t.get('pnl', 0) > 0]) / len(long_trades) * 100
            if short_trades:
                metrics.short_win_rate = len([t for t in short_trades if t.get('pnl', 0) > 0]) / len(short_trades) * 100
        
        # 分布特征
        if len(returns) > 2:
            metrics.skewness = pd.Series(returns).skew()
            metrics.kurtosis = pd.Series(returns).kurtosis()
        
        # 连胜连败
        current_streak = 0
        max_win_streak = 0
        max_loss_streak = 0
        
        for ret in returns:
            if ret > 0:
                current_streak = current_streak + 1 if current_streak > 0 else 1
                max_win_streak = max(max_win_streak, current_streak)
            elif ret < 0:
                current_streak = current_streak - 1 if current_streak < 0 else -1
                max_loss_streak = max(max_loss_streak, abs(current_streak))
            else:
                current_streak = 0
        
        metrics.win_loss_streak = max_win_streak
        metrics.loss_win_streak = max_loss_streak
        
        return metrics
    
    @staticmethod
    def compare_alphas(
        results: Dict[str, Dict]
    ) -> pd.DataFrame:
        """
        对比多个Alpha策略
        
        Args:
            results: {name: {'metrics': AlphaMetrics, 'portfolio_values': [...]}}
        
        Returns:
            对比DataFrame
        """
        comparison = []
        
        for name, result in results.items():
            m = result['metrics']
            comparison.append({
                'Name': name,
                'Total Return': f"{m.total_return:.2f}%",
                'Annual Return': f"{m.annual_return:.2f}%",
                'Sharpe': f"{m.sharpe_ratio:.2f}",
                'Sortino': f"{m.sortino_ratio:.2f}",
                'Max Drawdown': f"{m.max_drawdown:.2f}%",
                'Win Rate': f"{m.win_rate:.2f}%",
                'Profit Factor': f"{m.profit_factor:.2f}",
                'Trades': m.total_trades,
            })
        
        return pd.DataFrame(comparison)


def evaluate_factor_ic(
    factor_data: Dict[datetime, pd.Series],
    returns_data: Dict[datetime, pd.Series],
) -> Dict:
    """
    快速评估因子IC
    
    Args:
        factor_data: 因子值数据
        returns_data: 收益数据
    
    Returns:
        IC评估结果
    """
    ic_series = FactorEvaluator.calculate_ic_series(factor_data, returns_data, "spearman")
    
    if not ic_series:
        return {"error": "No valid IC data"}
    
    return {
        "ic_mean": np.mean(ic_series),
        "ic_std": np.std(ic_series),
        "icir": np.mean(ic_series) / np.std(ic_series) if np.std(ic_series) > 0 else 0,
        "ic_series": ic_series,
        "positive_ic_ratio": len([ic for ic in ic_series if ic > 0]) / len(ic_series),
        "significant_ic_ratio": len([ic for ic in ic_series if abs(ic) > 0.05]) / len(ic_series),
    }


def evaluate_factor_group_return(
    factor_weights: Dict[datetime, Dict[str, float]],
    next_returns: Dict[datetime, Dict[str, float]],
    groups: int = 5,
) -> Dict:
    """
    评估因子分组收益
    
    Args:
        factor_weights: 因子权重
        next_returns: 未来收益
        groups: 分组数
    
    Returns:
        分组收益评估
    """
    group_returns = [[] for _ in range(groups)]
    group_counts = [0] * groups
    
    for ts in sorted(factor_weights.keys()):
        if ts not in next_returns:
            continue
        
        weights = factor_weights[ts]
        returns = next_returns[ts]
        
        common = list(set(weights.keys()) & set(returns.keys()))
        if len(common) < groups:
            continue
        
        sorted_symbols = sorted(common, key=lambda s: weights.get(s, 0), reverse=True)
        group_size = len(sorted_symbols) // groups
        
        for i in range(groups):
            start = i * group_size
            end = start + group_size if i < groups - 1 else len(sorted_symbols)
            group_symbols = sorted_symbols[start:end]
            
            if group_symbols:
                avg_ret = np.mean([returns[s] for s in group_symbols])
                group_returns[i].append(avg_ret)
                group_counts[i] += 1
    
    result = {
        "groups": groups,
        "avg_returns": [np.mean(r) if r else 0 for r in group_returns],
        "std_returns": [np.std(r) if r else 0 for r in group_returns],
        "counts": group_counts,
        "long_short_spread": (np.mean(group_returns[0]) if group_returns[0] else 0) - 
                            (np.mean(group_returns[-1]) if group_returns[-1] else 0),
    }
    
    return result


__all__ = [
    'FactorMetrics',
    'AlphaMetrics',
    'FactorEvaluator',
    'AlphaEvaluator',
    'evaluate_factor_ic',
    'evaluate_factor_group_return',
]
