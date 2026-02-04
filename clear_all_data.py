#!/usr/bin/env python3
"""
清空所有数据文件和日志
保留目录结构，只删除内容
"""
import os
import shutil
from pathlib import Path

def clear_directory(dir_path: Path, keep_structure: bool = True):
    """清空目录内容，保留顶层目录结构"""
    if not dir_path.exists():
        return
    
    # 递归删除所有文件和子目录
    for item in dir_path.rglob('*'):
        try:
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                # 先尝试删除目录（如果为空会自动删除）
                try:
                    item.rmdir()
                except OSError:
                    # 目录不为空，稍后处理
                    pass
        except Exception as e:
            print(f"Warning: Failed to delete {item}: {e}")
    
    # 删除所有空子目录（从最深开始）
    for root, dirs, files in os.walk(dir_path, topdown=False):
        root_path = Path(root)
        if root_path != dir_path:  # 不删除顶层目录
            try:
                if not any(root_path.iterdir()):  # 如果目录为空
                    root_path.rmdir()
            except Exception:
                pass

def main():
    """主函数"""
    script_dir = Path(__file__).parent
    
    # 数据目录列表（从配置中提取）
    data_dirs = [
        "data/universe",
        "data/trades",
        "data/klines",
        "data/funding_rates",
        "data/premium_index",
        "data/positions",
        "data/signals",
        "data/equity_curve",
        "data/position_history",
        "data/strategy_reports",
        "data/api_performance",
        "data/performance",
    ]
    
    # 清空数据目录
    print("Clearing data directories...")
    for data_dir in data_dirs:
        dir_path = script_dir / data_dir
        if dir_path.exists():
            print(f"  Clearing {data_dir}...")
            clear_directory(dir_path, keep_structure=True)
        else:
            print(f"  {data_dir} does not exist, skipping...")
    
    # 清空data根目录下的文件（如system.pids）
    data_root = script_dir / "data"
    if data_root.exists():
        print("Clearing data root directory files...")
        for item in data_root.iterdir():
            if item.is_file():
                try:
                    item.unlink()
                    print(f"  Deleted {item.name}")
                except Exception as e:
                    print(f"  Warning: Failed to delete {item.name}: {e}")
    
    # 清空日志目录
    logs_dir = script_dir / "logs"
    if logs_dir.exists():
        print("Clearing logs directory...")
        clear_directory(logs_dir, keep_structure=True)
        print(f"  Cleared {logs_dir}")
    else:
        print("  logs directory does not exist, skipping...")
    
    print("\nAll data files and logs cleared successfully!")

if __name__ == "__main__":
    main()
