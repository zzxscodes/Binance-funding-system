import json
import pandas as pd
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='matplotlib')

def load_report(file_path):
    """Load strategy report JSON"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def plot_period(report):
    """Plot report period info - FINAL FIX: No overlap, no truncation"""
    period = report.get('period', {})
    if not period:
        print("Warning: No period data")
        return
    
    # 获取数据并处理长字符串
    start_time = period.get('start', 'N/A')
    end_time = period.get('end', 'N/A')
    days = period.get('days', 'N/A')
    
    # 预处理时间戳：确保不被截断
    max_width = 30  # 设置最大宽度
    if len(start_time) > max_width:
        start_time = start_time[:max_width-3] + "..."
    if len(end_time) > max_width:
        end_time = end_time[:max_width-3] + "..."
    
    df = pd.DataFrame([{
        'Start Time': start_time,
        'End Time': end_time,
        'Days Covered': str(days)
    }])
    
    fig, ax = plt.subplots(figsize=(10, 2.8), dpi=150)
    ax.axis('off')
    
    # ✅ 关键修复：精确控制表格位置和大小
    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc='center',
        loc='center',
        cellColours=[['#e6f7ff'] * len(df.columns)],
        bbox=[0.05, 0.18, 0.9, 0.7]  # 精确边界框
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.8)
    
    # ✅ 标题独立定位（避免与表格冲突）
    ax.text(0.5, 0.97, 'Report Period', transform=ax.transAxes,
            ha='center', va='top', fontsize=14, fontweight='bold')
    
    # ✅ 添加边距防止裁剪
    plt.tight_layout(pad=3.0)
    plt.savefig('report_period.png', dpi=150, bbox_inches='tight', pad_inches=0.8)
    plt.close()
    print("✓ Saved: report_period.png")

def plot_coin_selection(report):
    """Plot coin selection list and parameters"""
    coin_sel = report.get('coin_selection', {})
    symbols = coin_sel.get('symbols', [])
    count = coin_sel.get('symbols_count', 0)
    
    if not symbols:
        print("Warning: No coin selection data")
        return
    
    # Create coin list table
    df_coins = pd.DataFrame({'Selected Coins': symbols})
    
    # Prepare parameters summary
    params = coin_sel.get('parameters', {})
    param_lines = [f"Total Coins: {count}"]
    if params:
        param_lines.append("Strategy Parameters:")
        for k, v in list(params.items())[:5]:
            param_lines.append(f"  {k}: {v}")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, max(3, len(symbols)*0.4)))
    ax1.axis('off')
    ax2.axis('off')
    
    # Left: Coin list table
    table1 = ax1.table(cellText=df_coins.values, colLabels=df_coins.columns, 
                      cellLoc='center', loc='center')
    table1.auto_set_font_size(False)
    table1.set_fontsize(9)
    table1.scale(1.2, 1.5)
    
    # Right: Parameters summary
    stats_text = "\n".join(param_lines)
    ax2.text(0.1, 0.5, stats_text, fontsize=9, va='center', 
             family='monospace',
             bbox=dict(boxstyle='round', facecolor='#f0f8ff', alpha=0.8))
    
    plt.suptitle('Coin Selection Logic and List', fontsize=15, fontweight='bold')
    plt.tight_layout()
    plt.savefig('coin_selection.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("✓ Saved: coin_selection.png")

def plot_funding_rate_structure(report):
    """Plot funding rate structure for symbols"""
    funding_data = report.get('funding_rate_structure', {})
    valid_symbols = [s for s, d in funding_data.items() if d.get('status') == 'ok']
    
    if not valid_symbols:
        print("Warning: No valid funding rate data")
        return
    
    n_plots = min(4, len(valid_symbols))  # Limit to avoid overload
    fig, axes = plt.subplots(n_plots, 1, figsize=(10, 3.5 * n_plots))
    if n_plots == 1:
        axes = [axes]
    
    for idx, symbol in enumerate(valid_symbols[:n_plots]):
        data = funding_data[symbol]
        rates = data['rate_history']
        
        axes[idx].plot(rates, marker='o', linewidth=2, markersize=4, 
                      label=f'Recent Rates (n={len(rates)})')
        axes[idx].axhline(y=data['mean_rate'], color='r', linestyle='--', 
                         label=f"Mean: {data['mean_rate']:.6f}")
        axes[idx].axhline(y=data['min_rate'], color='g', linestyle=':', alpha=0.7)
        axes[idx].axhline(y=data['max_rate'], color='g', linestyle=':', alpha=0.7)
        
        axes[idx].set_title(f'{symbol} Funding Rate Analysis (Last 20 Periods)', fontsize=11, fontweight='bold')
        axes[idx].set_xlabel('Rate Period Index')
        axes[idx].set_ylabel('Rate Value')
        axes[idx].grid(True, alpha=0.3, linestyle='--')
        axes[idx].legend(loc='best', fontsize=8)
        
        stats_text = (f"Mean: {data['mean_rate']:.6f} | Std: {data['std_rate']:.6f}\n"
                     f"Range: [{data['min_rate']:.6f}, {data['max_rate']:.6f}] | "
                     f"Latest: {data['latest_rate']:.6f}")
        axes[idx].text(0.02, 0.98, stats_text, transform=axes[idx].transAxes,
                      fontsize=8, verticalalignment='top',
                      bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.suptitle('Funding Rate Structure Analysis', fontsize=14, fontweight='bold')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('funding_rates.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: funding_rates.png (showing {n_plots}/{len(valid_symbols)} symbols)")

def plot_position_openings(report):
    """Plot position openings table"""
    openings = report.get('position_openings', [])
    if not openings:
        print("Warning: No position openings data")
        return
    
    df = pd.DataFrame(openings)
    required_cols = ['timestamp', 'symbol', 'position_amt', 'entry_price', 'funding_rate']
    display_cols = [c for c in required_cols if c in df.columns]
    df = df[display_cols].copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp', ascending=False).head(15)
    
    # Format values
    if 'position_amt' in df.columns:
        df['position_amt'] = df['position_amt'].apply(lambda x: f"{x:+.2f}")
    if 'entry_price' in df.columns:
        df['entry_price'] = df['entry_price'].apply(lambda x: f"{x:.4f}")
    if 'funding_rate' in df.columns:
        df['funding_rate'] = df['funding_rate'].apply(lambda x: f"{x:.6f}" if pd.notnull(x) else "N/A")
    
    fig, ax = plt.subplots(figsize=(11, min(8, len(df)*0.6 + 1)))
    ax.axis('off')
    table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1.1, 1.4)
    
    # Highlight header and symbol column
    for (i, j), cell in table.get_celld().items():
        if i == 0:
            cell.set_facecolor('#2c7bb6')
            cell.set_text_props(color='white', weight='bold')
        elif j == df.columns.get_loc('symbol'):
            cell.set_facecolor('#e3f2fd')
    
    plt.title(f'Position Openings ({len(openings)} total, showing latest 15)', 
              fontsize=13, fontweight='bold', pad=15)
    plt.tight_layout()
    plt.savefig('position_openings.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("✓ Saved: position_openings.png")

def plot_strategy_wear(report):
    """Plot strategy wear analysis"""
    wear = report.get('strategy_wear', {})
    if not wear or wear.get('total_wear', 0) <= 0:
        print("Warning: No valid strategy wear data")
        return
    
    categories = ['Trading Fees', 'Slippage Loss', 'Funding Fees']
    values = [
        wear.get('total_fees', 0),
        wear.get('slippage', 0),
        wear.get('funding_fees', 0)
    ]
    
    non_zero = [(c, v) for c, v in zip(categories, values) if v > 0]
    if not non_zero:
        print("Warning: All wear components are zero")
        return
    
    labels, sizes = zip(*non_zero)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    
    # Pie chart
    colors = ['#ff9999', '#66b3ff', '#99ff99'][:len(labels)]
    wedges, texts, autotexts = ax1.pie(sizes, labels=labels, autopct='%1.1f%%', 
                                       startangle=90, colors=colors, textprops={'fontsize': 9})
    ax1.set_title('Strategy Wear Composition', fontsize=12, fontweight='bold')
    
    # Metrics table
    wear_data = [
        ['Total Wear', f"{wear.get('total_wear', 0):.2f} USDT"],
        ['Wear Percentage', f"{wear.get('wear_pct', 0):.2f}%"],
        ['Total Trades', str(wear.get('total_trades', 0))],
        ['Initial Equity', f"{wear.get('initial_equity', 0):.2f} USDT"]
    ]
    table = ax2.table(cellText=wear_data, colLabels=['Metric', 'Value'], cellLoc='center',
                     loc='center', cellColours=[['#f5f5f5', '#e6f7ff']] * len(wear_data))
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1.2, 1.8)
    ax2.axis('off')
    ax2.set_title('Key Wear Metrics', fontsize=12, fontweight='bold')
    
    plt.suptitle('Strategy Wear Analysis', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig('strategy_wear.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("✓ Saved: strategy_wear.png")

def plot_metadata(report):
    """Plot metadata table - FIXED: No overlap"""
    meta = report.get('metadata', {})
    if not meta:
        print("Warning: No metadata")
        return
    
    df = pd.DataFrame({
        'Attribute': list(meta.keys()),
        'Value': [str(v) for v in meta.values()]
    })
    
    # ✅ 动态高度 + 预留标题/边距空间
    fig_height = max(3.5, len(meta) * 0.65 + 1.2)
    fig, ax = plt.subplots(figsize=(8.5, fig_height), dpi=150)
    ax.axis('off')
    
    # ✅ 精确控制表格区域
    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc='left',
        loc='center',
        cellColours=[['#fafafa', '#ffffff']] * len(df),
        bbox=[0.03, 0.08, 0.94, 0.87]  # 为标题和底部留足空间
    )
    table.auto_set_font_size(False)
    table.set_fontsize(9.5)
    table.scale(1.15, 1.6)
    
    # ✅ 标题独立定位
    ax.text(0.5, 0.985, 'Report Metadata', transform=ax.transAxes,
            ha='center', va='top', fontsize=13, fontweight='bold')
    
    plt.savefig('metadata.png', dpi=150, bbox_inches='tight', pad_inches=0.7)
    plt.close()
    print("✓ Saved: metadata.png")

def main():
    """Main execution flow"""
    report_path = "data/strategy_reports/account1_strategy_report_latest.json"  # ADJUST IF NEEDED
    
    try:
        report = load_report(report_path)
        account_id = report.get('account_id', 'Unknown')
        report_time = report.get('report_time', 'N/A')[:19]  # Truncate timezone
        print(f"\n{'='*60}")
        print(f"SUCCESS: Loaded report for account '{account_id}'")
        print(f"Report generated: {report_time}")
        print(f"{'='*60}\n")
        
        # Generate all visualizations (skip equity curve per requirement)
        plot_period(report)
        plot_coin_selection(report)      # CRITICAL: Includes coin list
        plot_funding_rate_structure(report)
        plot_position_openings(report)
        plot_strategy_wear(report)
        plot_metadata(report)
        
        print(f"\n{'='*60}")
        print("ALL VISUALIZATIONS GENERATED SUCCESSFULLY!")
        print("Saved files:")
        print("  - report_period.png")
        print("  - coin_selection.png      <-- Contains coin list & selection logic")
        print("  - funding_rates.png")
        print("  - position_openings.png")
        print("  - strategy_wear.png")
        print("  - metadata.png")
        print("Note: Equity Curve intentionally skipped per requirements")
        print(f"{'='*60}\n")
        
    except FileNotFoundError:
        print(f"ERROR: Report file not found: '{report_path}'")
        print("Hint: Check filename or use absolute path")
    except json.JSONDecodeError:
        print(f"ERROR: Invalid JSON format in '{report_path}'")
    except Exception as e:
        print(f"ERROR during visualization: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()