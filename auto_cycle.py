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
import fcntl
from pathlib import Path

# 单实例保护：使用文件锁防止多个实例同时运行
LOCK_FILE = Path(__file__).parent / "data" / "auto_cycle.lock"

def acquire_lock():
    """获取文件锁，如果已有实例在运行则退出"""
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = None
    try:
        lock_fd = open(LOCK_FILE, 'w')
        if sys.platform != 'win32':
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            # Windows: 使用简单的文件存在检查
            if LOCK_FILE.exists():
                # 检查PID是否还在运行
                try:
                    with open(LOCK_FILE, 'r') as f:
                        pid = int(f.read().strip())
                    os.kill(pid, 0)  # 检查进程是否存在
                    sys.exit(0)  # 进程存在，退出
                except (OSError, ValueError):
                    pass  # 进程不存在，继续
        
        # 写入当前PID
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return lock_fd
    except (IOError, OSError):
        # 无法获取锁，说明已有实例在运行
        if lock_fd:
            lock_fd.close()
        sys.exit(0)

def release_lock(lock_fd):
    """释放文件锁"""
    try:
        if lock_fd:
            if sys.platform != 'win32':
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            lock_fd.close()
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
    except Exception:
        pass

# 获取锁
lock_fd = acquire_lock()

# 重定向所有输出到 /dev/null（但保留错误日志到文件）
if sys.platform != 'win32':
    log_file = Path(__file__).parent / "logs" / "auto_cycle.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    sys.stdout = open(os.devnull, 'w')
    sys.stderr = open(log_file, 'a')  # 错误日志写入文件，方便调试

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
        # 用户中断：不触发stop_all.py，只释放锁
        release_lock(lock_fd)
        sys.exit(0)
    except Exception as e:
        # 异常：记录错误但不触发stop_all.py（避免误关闭）
        try:
            import traceback
            error_msg = f"auto_cycle.py异常: {e}\n{traceback.format_exc()}\n"
            if sys.platform != 'win32':
                log_file = Path(__file__).parent / "logs" / "auto_cycle.log"
                with open(log_file, 'a') as f:
                    f.write(error_msg)
        except Exception:
            pass
        release_lock(lock_fd)
        sys.exit(1)
    finally:
        release_lock(lock_fd)