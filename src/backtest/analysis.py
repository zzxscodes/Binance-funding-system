"""
回测统计分析模块
对回测结果进行详细的统计分析和生成报告
"""
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import statistics
import json
from pathlib import Path

import pandas as pd

from ..common.logger import get_logger
from .models import BacktestResult, Trade, OrderSide

logger = get_logger('analysis')


class BacktestAnalyzer:
    """回测结果分析器"""
    
    @staticmethod
    def calculate_statistics(result: BacktestResult) -> Dict:
        """
        计算详细的回测统计指标
        
        Returns:
            包含所有统计指标的字典
        """
        stats = {
            'summary': BacktestAnalyzer._calculate_summary(result),
            'returns': BacktestAnalyzer._calculate_returns(result),
            'risk_metrics': BacktestAnalyzer._calculate_risk_metrics(result),
            'trade_statistics': BacktestAnalyzer._calculate_trade_statistics(result),
            'symbol_breakdown': BacktestAnalyzer._calculate_symbol_breakdown(result),
        }
        
        return stats
    
    @staticmethod
    def _calculate_summary(result: BacktestResult) -> Dict:
        """计算摘要统计"""
        if not result.portfolio_history:
            return {}
        
        first_state = result.portfolio_history[0]
        last_state = result.portfolio_history[-1]
        
        return {
            'backtest_name': result.config.name,
            'start_date': result.config.start_date.isoformat(),
            'end_date': result.config.end_date.isoformat(),
            'initial_balance': result.config.initial_balance,
            'final_balance': last_state.total_balance,
            'total_pnl': last_state.total_pnl,
            'total_return_pct': result.total_return,
            'annual_return_pct': result.annual_return,
            'backtest_days': (result.config.end_date - result.config.start_date).days,
            'execution_time_seconds': result.execution_time_seconds,
        }
    
    @staticmethod
    def _calculate_returns(result: BacktestResult) -> Dict:
        """计算收益相关指标"""
        if not result.portfolio_history:
            return {}
        
        portfolio_values = [s.total_balance for s in result.portfolio_history]
        daily_returns = []
        
        for i in range(1, len(portfolio_values)):
            daily_return = (portfolio_values[i] - portfolio_values[i-1]) / portfolio_values[i-1]
            daily_returns.append(daily_return)
        
        if not daily_returns:
            return {
                'mean_daily_return': 0.0,
                'std_daily_return': 0.0,
                'sharpe_ratio': 0.0,
                'sortino_ratio': 0.0,
            }
        
        mean_return = statistics.mean(daily_returns)
        std_return = statistics.stdev(daily_returns) if len(daily_returns) > 1 else 0.0
        
        # 计算夏普比率（假设252个交易日）
        sharpe = (mean_return / std_return * (252 ** 0.5)) if std_return > 0 else 0.0
        
        # 计算Sortino比率（只考虑负收益的波动率）
        negative_returns = [r for r in daily_returns if r < 0]
        downside_std = statistics.stdev(negative_returns) if len(negative_returns) > 1 else 0.0
        sortino = (mean_return / downside_std * (252 ** 0.5)) if downside_std > 0 else 0.0
        
        return {
            'mean_daily_return': mean_return,
            'std_daily_return': std_return,
            'sharpe_ratio': sharpe,
            'sortino_ratio': sortino,
        }
    
    @staticmethod
    def _calculate_risk_metrics(result: BacktestResult) -> Dict:
        """计算风险相关指标"""
        if not result.portfolio_history:
            return {}
        
        cumulative_pnls = [s.total_pnl for s in result.portfolio_history]
        
        # 计算最大回撤
        running_max = 0
        max_dd = 0
        max_dd_pct = 0
        
        for pnl in cumulative_pnls:
            running_max = max(running_max, pnl)
            drawdown = running_max - pnl
            if drawdown > max_dd:
                max_dd = drawdown
                max_dd_pct = (drawdown / (result.config.initial_balance + running_max)) * 100 if (result.config.initial_balance + running_max) > 0 else 0
        
        # 计算恢复时间（从最大回撤恢复到新高点所需的时间步数）
        recovery_time = 0
        for i in range(len(cumulative_pnls)):
            if cumulative_pnls[i] < running_max and i + 1 < len(cumulative_pnls):
                if cumulative_pnls[i + 1] >= running_max:
                    recovery_time = i + 1
                    break
        
        return {
            'max_drawdown_pct': max_dd_pct,
            'max_drawdown_absolute': max_dd,
            'recovery_time_steps': recovery_time,
            'var_95': result.total_return - 1.96 * result.total_return,  # 简化计算
            'cvar_95': result.total_return - 2.33 * result.total_return,  # 简化计算
        }
    
    @staticmethod
    def _calculate_trade_statistics(result: BacktestResult) -> Dict:
        """计算交易相关统计"""
        if not result.trades:
            return {
                'total_trades': 0,
                'winning_trades': 0,
                'losing_trades': 0,
                'win_rate_pct': 0.0,
                'avg_win': 0.0,
                'avg_loss': 0.0,
                'profit_factor': 0.0,
                'avg_trade_pnl': 0.0,
                'max_consecutive_wins': 0,
                'max_consecutive_losses': 0,
            }
        
        trades = result.trades
        winning = [t for t in trades if t.pnl > 1e-8]
        losing = [t for t in trades if t.pnl < -1e-8]
        
        win_rate = (len(winning) / len(trades) * 100) if trades else 0
        avg_win = statistics.mean([t.pnl for t in winning]) if winning else 0
        avg_loss = statistics.mean([t.pnl for t in losing]) if losing else 0
        profit_factor = (sum(t.pnl for t in winning) / abs(sum(t.pnl for t in losing))) if losing and sum(t.pnl for t in losing) != 0 else 0
        
        # 计算连胜和连败
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0
        
        for trade in trades:
            if trade.pnl > 1e-8:
                current_wins += 1
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
                current_losses = 0
            elif trade.pnl < -1e-8:
                current_losses += 1
                max_consecutive_losses = max(max_consecutive_losses, current_losses)
                current_wins = 0
        
        return {
            'total_trades': len(trades),
            'winning_trades': len(winning),
            'losing_trades': len(losing),
            'win_rate_pct': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'avg_trade_pnl': statistics.mean([t.pnl for t in trades]),
            'max_trade_pnl': max([t.pnl for t in trades]) if trades else 0,
            'min_trade_pnl': min([t.pnl for t in trades]) if trades else 0,
            'max_consecutive_wins': max_consecutive_wins,
            'max_consecutive_losses': max_consecutive_losses,
        }
    
    @staticmethod
    def _calculate_symbol_breakdown(result: BacktestResult) -> Dict[str, Dict]:
        """按交易对计算统计"""
        symbol_trades = {}
        
        for trade in result.trades:
            if trade.symbol not in symbol_trades:
                symbol_trades[trade.symbol] = []
            symbol_trades[trade.symbol].append(trade)
        
        breakdown = {}
        
        for symbol, trades in symbol_trades.items():
            winning = [t for t in trades if t.pnl > 1e-8]
            losing = [t for t in trades if t.pnl < -1e-8]
            
            breakdown[symbol] = {
                'trades': len(trades),
                'winning': len(winning),
                'losing': len(losing),
                'win_rate_pct': (len(winning) / len(trades) * 100) if trades else 0,
                'total_pnl': sum(t.pnl for t in trades),
                'avg_pnl': statistics.mean([t.pnl for t in trades]) if trades else 0,
                'volume_usdt': sum(t.trade_value for t in trades),
            }
        
        return breakdown
    
    @staticmethod
    def generate_report(result: BacktestResult, output_path: Optional[Path] = None) -> str:
        """
        生成可读的回测报告
        
        Returns:
            报告文本内容
        """
        lines = []
        lines.append("=" * 80)
        lines.append(f"回测报告: {result.config.name}")
        lines.append("=" * 80)
        lines.append("")
        
        # 基本信息
        lines.append("【基本信息】")
        lines.append(f"回测周期: {result.config.start_date.date()} 至 {result.config.end_date.date()}")
        lines.append(f"初始资金: ${result.config.initial_balance:,.2f}")
        lines.append(f"交易对: {', '.join(result.config.symbols)}")
        lines.append(f"杠杆倍数: {result.config.leverage}x")
        lines.append("")
        
        # 收益统计
        if result.portfolio_history:
            final_balance = result.portfolio_history[-1].total_balance
            lines.append("【收益统计】")
            lines.append(f"最终资金: ${final_balance:,.2f}")
            lines.append(f"总收益: ${final_balance - result.config.initial_balance:,.2f}")
            lines.append(f"总收益率: {result.total_return:.2f}%")
            lines.append(f"年化收益率: {result.annual_return:.2f}%")
            lines.append("")
        
        # 风险指标
        lines.append("【风险指标】")
        lines.append(f"最大回撤: {result.max_drawdown:.2f}%")
        lines.append(f"夏普比率: {result.sharpe_ratio:.2f}")
        lines.append("")
        
        # 交易统计
        lines.append("【交易统计】")
        lines.append(f"总交易数: {len(result.trades)}")
        lines.append(f"胜率: {result.win_rate:.2f}%")
        lines.append(f"平均每笔交易PnL: ${result.avg_profit_per_trade:,.2f}")
        lines.append(f"最大单笔利润: ${result.max_profit_per_trade:,.2f}")
        lines.append(f"最大单笔亏损: ${result.max_loss_per_trade:,.2f}")
        lines.append(f"盈亏比: {result.profit_factor:.2f}")
        lines.append("")
        
        # 执行信息
        lines.append("【执行信息】")
        lines.append(f"执行耗时: {result.execution_time_seconds:.2f}秒")
        lines.append("")
        
        report_text = "\n".join(lines)
        
        if output_path:
            output_path.write_text(report_text, encoding='utf-8')
            logger.info(f"Report saved to {output_path}")
        
        return report_text
    
    @staticmethod
    def export_trades_csv(result: BacktestResult, output_path: Path):
        """导出交易记录为CSV"""
        trades_data = []
        
        for trade in result.trades:
            trades_data.append({
                'trade_id': trade.trade_id,
                'symbol': trade.symbol,
                'side': trade.side.value,
                'quantity': trade.quantity,
                'price': trade.price,
                'executed_at': trade.executed_at.isoformat(),
                'commission': trade.commission,
                'pnl': trade.pnl,
                'trade_value': trade.trade_value,
            })
        
        df = pd.DataFrame(trades_data)
        df.to_csv(output_path, index=False)
        logger.info(f"Trades exported to {output_path}")
    
    @staticmethod
    def export_portfolio_history_csv(result: BacktestResult, output_path: Path):
        """导出账户历史为CSV"""
        history_data = []
        
        for state in result.portfolio_history:
            history_data.append({
                'timestamp': state.timestamp.isoformat(),
                'total_balance': state.total_balance,
                'available_balance': state.available_balance,
                'used_margin': state.used_margin,
                'total_pnl': state.total_pnl,
                'realized_pnl': state.realized_pnl,
                'unrealized_pnl': state.unrealized_pnl,
                'open_positions': len([p for p in state.positions.values() if p.quantity != 0]),
                'open_orders': len(state.open_orders),
            })
        
        df = pd.DataFrame(history_data)
        df.to_csv(output_path, index=False)
        logger.info(f"Portfolio history exported to {output_path}")
    
    @staticmethod
    def export_json(result: BacktestResult, output_path: Path):
        """导出完整的回测结果为JSON"""
        result_dict = result.to_dict()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result_dict, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Result exported to {output_path}")
