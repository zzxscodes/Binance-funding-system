import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np
import pandas as pd
import os

def generate_equity_curve(json_file_path, output_image_path=None):
    """
    从JSON文件生成equity curve图表
    
    参数:
    json_file_path: JSON数据文件路径
    output_image_path: 输出图片路径，如果为None则只显示不保存
    """
    try:
        # 读取JSON数据
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        
        # 提取时间和净值数据
        timestamps = []
        equities = []
        total_balances = []
        unrealized_pnls = []
        
        for entry in data:
            # 转换时间戳格式
            try:
                # 处理ISO格式时间戳
                dt = datetime.fromisoformat(entry['timestamp'].replace('Z', '+00:00'))
            except:
                # 尝试其他时间格式
                dt = datetime.strptime(entry['timestamp'], '%Y-%m-%dT%H:%M:%S.%f+00:00')
            
            timestamps.append(dt)
            equities.append(float(entry['equity']))
            total_balances.append(float(entry['total_balance']))
            unrealized_pnls.append(float(entry['unrealized_pnl']))
        
        # 转换为pandas DataFrame便于处理
        df = pd.DataFrame({
            'timestamp': timestamps,
            'equity': equities,
            'total_balance': total_balances,
            'unrealized_pnl': unrealized_pnls
        })
        
        # 按时间排序
        df = df.sort_values('timestamp')
        
        # 计算收益率
        initial_equity = df['equity'].iloc[0]
        df['return_pct'] = (df['equity'] - initial_equity) / initial_equity * 100
        
        # 创建图表
        plt.figure(figsize=(14, 8))
        
        # 绘制净值曲线
        plt.plot(df['timestamp'], df['equity'], 
                 linewidth=2.5, color='#1f77b4', label='Equity')
        
        # 绘制总余额作为参考
        plt.plot(df['timestamp'], df['total_balance'], 
                 linewidth=1.5, color='#ff7f0e', linestyle='--', label='Total Balance')
        
        # 填充净值与初始值之间的区域
        plt.fill_between(df['timestamp'], initial_equity, df['equity'], 
                        where=(df['equity'] >= initial_equity), 
                        interpolate=True, color='#2ca02c', alpha=0.3,
                        label='Profit Area')
        plt.fill_between(df['timestamp'], initial_equity, df['equity'], 
                        where=(df['equity'] < initial_equity), 
                        interpolate=True, color='#d62728', alpha=0.3,
                        label='Loss Area')
        
        # 添加每日收益率标注
        max_points = 20  # 限制标注点数量
        step = max(1, len(df) // max_points)
        
        for i in range(0, len(df), step):
            plt.annotate(f"{df['return_pct'].iloc[i]:.2f}%",
                         (df['timestamp'].iloc[i], df['equity'].iloc[i]),
                         xytext=(0, 10), textcoords='offset points',
                         ha='center', fontsize=8,
                         bbox=dict(boxstyle='round,pad=0.3', fc='yellow', alpha=0.5))
        
        # 设置日期格式
        date_format = mdates.DateFormatter('%m-%d %H:%M')
        plt.gca().xaxis.set_major_formatter(date_format)
        plt.gcf().autofmt_xdate()  # 自动旋转日期标签
        
        # 添加网格、标题和标签
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.title('Trading Account Equity Curve', fontsize=16, fontweight='bold')
        plt.xlabel('Date & Time', fontsize=12)
        plt.ylabel('Equity (USDT)', fontsize=12)
        plt.legend(loc='best')
        
        # 设置Y轴范围，增加一些边距
        y_min = min(min(equities), min(total_balances)) * 0.99
        y_max = max(max(equities), max(total_balances)) * 1.01
        plt.ylim(y_min, y_max)
        
        # 添加统计信息文本框
        stats_text = (
            f"Initial Equity: {initial_equity:.2f} USDT\n"
            f"Final Equity: {df['equity'].iloc[-1]:.2f} USDT\n"
            f"Total Return: {df['return_pct'].iloc[-1]:.2f}%\n"
            f"Max Equity: {max(equities):.2f} USDT\n"
            f"Min Equity: {min(equities):.2f} USDT\n"
            f"Volatility: {np.std(df['return_pct']):.2f}%"
        )
        
        plt.gcf().text(0.02, 0.02, stats_text, 
                      bbox=dict(facecolor='white', alpha=0.8, boxstyle='round,pad=0.5'),
                      fontsize=10, family='monospace')
        
        # 保存图片
        if output_image_path:
            plt.savefig(output_image_path, dpi=300, bbox_inches='tight')
            print(f"图表已保存至: {output_image_path}")
        
        # 显示图表
        plt.tight_layout()
        plt.show()
        
        return df
        
    except Exception as e:
        print(f"处理数据时出错: {str(e)}")
        return None

if __name__ == "__main__":
    # 使用示例
    json_file = "data/equity_curve/account1_equity_curve.json"  # 替换为您的文件路径
    output_image = "equity_curve.png"  # 输出图片路径
    
    # 检查文件是否存在
    if not os.path.exists(json_file):
        print(f"错误：文件 {json_file} 不存在。")
        print("请提供正确的文件路径。")
    else:
        # 生成图表
        result_df = generate_equity_curve(json_file, output_image)
        
        if result_df is not None:
            print("\n数据处理完成。净值统计数据：")
            print(f"数据点数量: {len(result_df)}")
            print(f"初始净值: {result_df['equity'].iloc[0]:.2f}")
            print(f"最终净值: {result_df['equity'].iloc[-1]:.2f}")
            print(f"总收益率: {result_df['return_pct'].iloc[-1]:.2f}%")