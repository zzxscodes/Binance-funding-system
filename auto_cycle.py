#!/usr/bin/env python3
"""
# 后台运行
nohup python3 auto_cycle.py &
自动化循环脚本：启动 -> 等待3天 -> 强制关闭
完全静默运行，无任何输出
"""
import subprocess
import sys
import time
import os
from pathlib import Path

# 重定向所有输出到 /dev/null
if sys.platform != 'win32':
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(os.devnull, 'w')

def run_silent(cmd):
    """静默执行命令，不影响子进程的文件操作"""
    try:
        # 只抑制脚本本身的输出，不影响子进程的文件操作
        # 子进程会独立打开自己的日志文件，不受父进程stdout影响
        subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            cwd=Path(__file__).parent  # 确保工作目录正确
        )
    except Exception:
        pass

def main():
    # 切换到脚本所在目录
    script_dir = Path(__file__).parent
    os.chdir(script_dir)
    
    # 启动系统
    run_silent(f"{sys.executable} start_all.py")
    
    # 等待3天（259200秒）
    time.sleep(259200)
    
    # 强制关闭系统
    run_silent(f"{sys.executable} stop_all.py")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        run_silent(f"{sys.executable} stop_all.py")
        sys.exit(0)
    except Exception:
        run_silent(f"{sys.executable} stop_all.py")
        sys.exit(1)
