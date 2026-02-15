"""
回测图表生成模块

提供两类图表生成：
1. 因子评估图表 - generate_factor_charts()
2. Alpha评估图表 - generate_alpha_charts()
"""
from __future__ import annotations
import sys
if sys.version_info >= (3, 11):
    from typing import Self
else:
    Self = None

from pathlib import Path
from typing import TYPE_CHECKING, Dict

import logging

logger = logging.getLogger('charts')

if TYPE_CHECKING:
    from src.backtest.metrics import FactorMetrics
    from src.backtest.models import BacktestResult


def _check_deps():
    """检查依赖"""
    try:
        import matplotlib  # noqa: F401
        import numpy as np  # noqa: F401
        import pandas as pd  # noqa: F401
        return True
    except ImportError:
        logger.warning("需要安装: pip install pandas numpy matplotlib")
        return False


def generate_factor_charts(
    metrics: FactorMetrics,
    output_dir: str = "results",
    name: str = "factor"
) -> Dict[str, str]:
    """
    生成因子评估图表
    
    生成的图表:
    - {name}_ic_series.png: IC时间序列
    - {name}_ic_distribution.png: IC分布
    - {name}_group_returns.png: 分组收益
    - {name}_metrics_summary.png: 指标汇总
    """
    if not _check_deps():
        return {}
    
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    result = {}
    
    ic_series = getattr(metrics, 'ic_series', None) or getattr(metrics, 'ic_list', [])
    
    if ic_series:
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        
        axes[0, 0].plot(ic_series, 'b-', linewidth=1)
        axes[0, 0].axhline(y=0, color='gray', linestyle='--')
        axes[0, 0].axhline(y=0.03, color='green', alpha=0.5, label='IC>0.03')
        axes[0, 0].axhline(y=-0.03, color='red', alpha=0.5, label='IC<-0.03')
        axes[0, 0].set_title('IC Time Series')
        axes[0, 0].set_xlabel('Period')
        axes[0, 0].set_ylabel('IC')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
        
        axes[0, 1].hist(ic_series, bins=25, color='steelblue', edgecolor='white', alpha=0.7)
        axes[0, 1].axvline(x=np.mean(ic_series), color='red', linestyle='--', label=f'Mean={np.mean(ic_series):.4f}')
        axes[0, 1].set_title('IC Distribution')
        axes[0, 1].set_xlabel('IC')
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)
        
        group_rets = getattr(metrics, 'group_returns', []) or getattr(metrics, 'avg_returns', [])
        if group_rets:
            axes[1, 0].bar(range(1, len(group_rets) + 1), [r * 100 for r in group_rets], color='steelblue')
            axes[1, 0].axhline(y=0, color='gray')
            axes[1, 0].set_title('Group Returns')
            axes[1, 0].set_xlabel('Group (1=Top)')
            axes[1, 0].set_ylabel('Return (%)')
            axes[1, 0].grid(True, alpha=0.3, axis='y')
        
        data = [
            ['IC Mean', f"{getattr(metrics, 'ic_mean', 0):.4f}"],
            ['IC Std', f"{getattr(metrics, 'ic_std', 0):.4f}"],
            ['ICIR', f"{getattr(metrics, 'icir', 0):.4f}"],
            ['Rank IC', f"{getattr(metrics, 'rank_ic_mean', 0):.4f}"],
            ['Selection Acc', f"{getattr(metrics, 'selection_accuracy', 0):.2f}%"],
            ['Long Spread', f"{getattr(metrics, 'long_spread', 0):.4f}"],
        ]
        
        axes[1, 1].axis('off')
        tbl = axes[1, 1].table(cellText=data, colLabels=['Metric', 'Value'], loc='center', cellLoc='center')
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(10)
        tbl.scale(1.2, 1.5)
        axes[1, 1].set_title('Factor Metrics', fontsize=12, fontweight='bold', pad=10)
        
        plt.suptitle(f'Factor Analysis: {name}', fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        p = out / f"{name}_factor_charts.png"
        plt.savefig(p, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        result['factor_charts'] = str(p)
    
    return result


def generate_alpha_charts(
    result: BacktestResult,
    output_dir: str = "results",
    name: str = "alpha"
) -> Dict[str, str]:
    """
    生成Alpha评估图表
    
    生成的图表:
    - {name}_equity_curve.png: 净值曲线
    - {name}_drawdown.png: 回撤分析
    - {name}_trades.png: 交易分析
    - {name}_summary.png: 综合汇总
    """
    if not _check_deps():
        return {}
    
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    result_files = {}
    
    initial = getattr(result.config, 'initial_balance', None)
    if initial is None:
        initial = get_default_initial_balance()
    
    values = []
    for state in result.portfolio_history:
        if isinstance(state, dict):
            values.append(state.get('total_balance', initial))
        else:
            values.append(getattr(state, 'total_balance', initial))
    
    if not values:
        values = [initial] * 100
    
    equity = np.array(values)
    cumulative = (equity - initial) / initial * 100
    cummax = np.maximum.accumulate(equity)
    drawdown = (equity - cummax) / cummax * 100
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(cumulative, 'b-', linewidth=1.5, label='Equity')
    ax.fill_between(range(len(cumulative)), 0, cumulative,
                   where=(cumulative >= 0), color='green', alpha=0.3)
    ax.fill_between(range(len(cumulative)), 0, cumulative,
                   where=(cumulative < 0), color='red', alpha=0.3)
    ax.axhline(y=0, color='gray', linestyle='--')
    ax.set_title(f'Equity Curve: {name}')
    ax.set_xlabel('Period')
    ax.set_ylabel('Return (%)')
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.text(0.99, 0.97,
            f'Reach: {cumulative[-1]:.2f}%\nSharpe: {result.sharpe_ratio:.2f}\nMaxDD: {result.max_drawdown:.2f}%',
            transform=ax.transAxes, fontsize=9, verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))
    
    p = out / f"{name}_equity_curve.png"
    plt.savefig(p, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    result_files['equity_curve'] = str(p)
    
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.fill_between(range(len(drawdown)), 0, drawdown, color='red', alpha=0.5)
    ax.plot(drawdown, 'r-', linewidth=1)
    ax.set_title(f'Drawdown: {name}')
    ax.set_xlabel('Period')
    ax.set_ylabel('Drawdown (%)')
    ax.grid(True, alpha=0.3)
    ax.text(0.99, 0.95, f'MaxDD: {result.max_drawdown:.2f}%',
            transform=ax.transAxes, fontsize=10, verticalalignment='top', horizontalalignment='right')
    
    p = out / f"{name}_drawdown.png"
    plt.savefig(p, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    result_files['drawdown'] = str(p)
    
    if result.trades:
        pnls = [t.pnl for t in result.trades]
        cum_pnl = np.cumsum(pnls)
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        
        axes[0, 0].plot(cum_pnl, 'b-', linewidth=1.5)
        axes[0, 0].fill_between(range(len(cum_pnl)), 0, cum_pnl,
                                where=(np.array(cum_pnl) >= 0), color='green', alpha=0.3)
        axes[0, 0].fill_between(range(len(cum_pnl)), 0, cum_pnl,
                                where=(np.array(cum_pnl) < 0), color='red', alpha=0.3)
        axes[0, 0].axhline(y=0, color='gray', linestyle='--')
        axes[0, 0].set_title('Cumulative PnL')
        axes[0, 0].set_xlabel('Trade')
        axes[0, 0].set_ylabel('PnL')
        axes[0, 0].grid(True, alpha=0.3)
        
        colors = ['green' if p > 0 else 'red' for p in pnls]
        axes[0, 1].bar(range(len(pnls)), pnls, color=colors, alpha=0.7)
        axes[0, 1].axhline(y=0, color='black', linewidth=0.5)
        axes[0, 1].set_title('Trade PnL')
        axes[0, 1].set_xlabel('Trade')
        axes[0, 1].set_ylabel('PnL')
        axes[0, 1].grid(True, alpha=0.3, axis='y')
        
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        axes[1, 0].hist(wins, bins=15, color='green', alpha=0.7, label='Wins')
        if losses:
            axes[1, 0].hist(losses, bins=15, color='red', alpha=0.7, label='Losses')
        axes[1, 0].axvline(x=0, color='black', linestyle='--')
        axes[1, 0].set_title('PnL Distribution')
        axes[1, 0].set_xlabel('PnL')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)
        
        sym_pnl = {}
        for t in result.trades:
            if t.symbol not in sym_pnl:
                sym_pnl[t.symbol] = []
            sym_pnl[t.symbol].append(t.pnl)
        
        syms = list(sym_pnl.keys())[:15]
        avg_pnls = [np.mean(sym_pnl[s]) for s in syms]
        bar_colors = ['green' if p > 0 else 'red' for p in avg_pnls]
        axes[1, 1].barh(range(len(syms)), avg_pnls, color=bar_colors, alpha=0.7)
        axes[1, 1].set_yticks(range(len(syms)))
        axes[1, 1].set_yticklabels(syms)
        axes[1, 1].axvline(x=0, color='gray', linestyle='--')
        axes[1, 1].set_title('Avg PnL by Symbol')
        axes[1, 1].set_xlabel('Avg PnL')
        axes[1, 1].grid(True, alpha=0.3, axis='x')
        
        plt.suptitle(f'Trades Analysis: {name}', fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        p = out / f"{name}_trades.png"
        plt.savefig(p, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        result_files['trades'] = str(p)
    
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.axis('off')
    
    data = [
        ['Total Return', f"{result.total_return:.2f}%"],
        ['Annual Return', f"{result.annual_return:.2f}%"],
        ['Sharpe Ratio', f"{result.sharpe_ratio:.2f}"],
        ['Sortino Ratio', f"{result.sortino_ratio:.2f}"],
        ['Max Drawdown', f"{result.max_drawdown:.2f}%"],
        ['Win Rate', f"{result.win_rate:.2f}%"],
        ['Profit Factor', f"{result.profit_factor:.2f}"],
        ['Total Trades', str(len(result.trades))],
        ['IC', f"{result.ic_mean:.4f}"],
        ['Rank IC', f"{result.rank_ic_mean:.4f}"],
        ['Selection Acc', f"{result.selection_accuracy:.2f}%"],
    ]
    
    tbl = ax.table(cellText=data, colLabels=['Metric', 'Value'], loc='center', cellLoc='center')
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(11)
    tbl.scale(1.3, 2)
    
    for i in range(len(data) + 1):
        for j in range(2):
            cell = tbl[(i, j)]
            if i == 0:
                cell.set_facecolor('#4472C4')
                cell.set_text_props(color='white', fontweight='bold')
            elif i % 2 == 0:
                cell.set_facecolor('#E8ECEE')
    
    ax.set_title(f'Backtest Summary: {name}', fontsize=16, fontweight='bold', pad=20)
    
    p = out / f"{name}_summary.png"
    plt.savefig(p, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    result_files['summary'] = str(p)
    
    return result_files


def export_csv(result: BacktestResult, output_dir: str = "results", name: str = "backtest"):
    """导出回测数据为CSV"""
    if not _check_deps():
        return None, None
    
    import pandas as pd
    
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    
    portfolio_data = []
    for state in result.portfolio_history:
        if isinstance(state, dict):
            ts = state.get('timestamp', '')
            portfolio_data.append({
                'timestamp': ts.isoformat() if hasattr(ts, 'isoformat') else str(ts),
                'total_balance': state.get('total_balance', 0),
                'total_pnl': state.get('total_pnl', 0),
            })
        else:
            ts = getattr(state, 'timestamp', '')
            portfolio_data.append({
                'timestamp': ts.isoformat() if hasattr(ts, 'isoformat') else str(ts),
                'total_balance': getattr(state, 'total_balance', 0),
                'total_pnl': getattr(state, 'total_pnl', 0),
            })
    
    pf = out / f"{name}_portfolio.csv"
    pd.DataFrame(portfolio_data).to_csv(pf, index=False)
    
    trades_data = []
    for t in result.trades:
        trades_data.append({
            'symbol': t.symbol,
            'side': t.side.value if hasattr(t.side, 'value') else str(t.side),
            'quantity': t.quantity,
            'price': t.price,
            'pnl': t.pnl,
        })
    
    tf = out / f"{name}_trades.csv"
    pd.DataFrame(trades_data).to_csv(tf, index=False)
    
    return str(pf), str(tf)


__all__ = ['generate_factor_charts', 'generate_alpha_charts', 'export_csv']
