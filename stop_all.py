"""
跨平台停止所有进程脚本
支持 Windows, Linux, macOS
"""
import sys
import os
import time
import signal
from pathlib import Path


def is_process_running(pid):
    """检查进程是否在运行"""
    if sys.platform == 'win32':
        try:
            import subprocess
            result = subprocess.run(
                ['tasklist', '/FI', f'PID eq {pid}'],
                capture_output=True,
                text=True,
                timeout=5
            )
            return str(pid) in result.stdout
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)  # 发送信号0检查进程是否存在
            return True
        except OSError:
            return False


def find_processes():
    """查找所有系统相关进程"""
    processes = []
    
    # 首先尝试从PID文件读取
    pid_file = Path("data/system.pids")
    if pid_file.exists():
        try:
            with open(pid_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and line.isdigit():
                        pid = int(line)
                        # 验证进程是否还在运行
                        if is_process_running(pid):
                            processes.append(pid)
            if processes:
                return processes
        except Exception:
            pass
    
    # 如果PID文件不存在或无效，使用进程名查找
    if sys.platform == 'win32':
        # Windows: 使用PowerShell查找进程（包括cmd和python进程）
        try:
            import subprocess
            # 查找所有包含src.processes的进程（包括cmd.exe和python.exe）
            ps_cmd = '''
            Get-WmiObject Win32_Process | Where-Object { 
                $_.CommandLine -like "*src.processes*" -or 
                $_.CommandLine -like "*start_all*" -or
                ($_.Name -eq "python.exe" -and $_.CommandLine -like "*src.processes*")
            } | Select-Object -ExpandProperty ProcessId
            '''
            result = subprocess.run(
                ['powershell', '-Command', ps_cmd],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore',
                timeout=10
            )
            
            for line in result.stdout.strip().split('\n'):
                line = line.strip()
                if line and line.isdigit():
                    pid = int(line)
                    if pid not in processes:
                        processes.append(pid)
            
            # 如果PowerShell方法没找到，尝试wmic
            if not processes:
                try:
                    result = subprocess.run(
                        ['wmic', 'process', 'where', 'name="python.exe"', 'get', 'processid,commandline', '/format:csv'],
                        capture_output=True,
                        text=True,
                        encoding='utf-8',
                        errors='ignore',
                        timeout=10
                    )
                    
                    for line in result.stdout.split('\n'):
                        if 'src.processes' in line:
                            parts = line.split(',')
                            for i, part in enumerate(parts):
                                if i > 0 and part.strip().isdigit():
                                    try:
                                        pid = int(part.strip())
                                        if pid not in processes:
                                            processes.append(pid)
                                    except ValueError:
                                        continue
                except Exception:
                    pass
        except Exception as e:
            print(f"Warning: Failed to list processes: {e}")
    else:
        # Linux/macOS: 使用ps命令
        try:
            import subprocess
            result = subprocess.run(
                ['ps', 'aux'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            for line in result.stdout.split('\n'):
                if 'src.processes' in line and ('python' in line.lower() or 'python3' in line.lower()):
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            pid = int(parts[1])
                            if pid not in processes:
                                processes.append(pid)
                        except (ValueError, IndexError):
                            continue
        except Exception as e:
            print(f"Warning: Failed to list processes: {e}")
    
    return processes


def stop_processes_gracefully(processes):
    """优雅关闭进程"""
    if not processes:
        print("No running processes found.")
        return
    
    print(f"Found {len(processes)} process(es) to stop")
    print("")
    
    if sys.platform == 'win32':
        # Windows: 使用taskkill发送关闭信号
        try:
            import subprocess
            for pid in processes:
                print(f"  Stopping PID {pid}...")
                try:
                    subprocess.run(
                        ['taskkill', '/PID', str(pid), '/T'],
                        capture_output=True,
                        timeout=5
                    )
                except subprocess.TimeoutExpired:
                    print(f"    Warning: Timeout stopping PID {pid}")
                except Exception as e:
                    print(f"    Warning: Failed to stop PID {pid}: {e}")
        except Exception as e:
            print(f"Error stopping processes: {e}")
    else:
        # Linux/macOS: 发送SIGTERM信号
        for pid in processes:
            print(f"  Stopping PID {pid}...")
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                print(f"    Process {pid} already terminated")
            except PermissionError:
                print(f"    Permission denied for PID {pid}")
            except Exception as e:
                print(f"    Error stopping PID {pid}: {e}")


def stop_processes_forcefully(remaining_processes):
    """强制关闭进程"""
    if not remaining_processes:
        return
    
    print("Some processes are still running, force stopping...")
    
    if sys.platform == 'win32':
        # Windows: 使用taskkill强制关闭
        try:
            import subprocess
            for pid in remaining_processes:
                print(f"  Force stopping PID {pid}...")
                try:
                    subprocess.run(
                        ['taskkill', '/F', '/PID', str(pid), '/T'],
                        capture_output=True,
                        timeout=5
                    )
                except Exception as e:
                    print(f"    Warning: Could not force stop PID {pid}: {e}")
        except Exception as e:
            print(f"Error force stopping processes: {e}")
    else:
        # Linux/macOS: 发送SIGKILL信号
        for pid in remaining_processes:
            print(f"  Force stopping PID {pid}...")
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # 进程已经终止
            except Exception as e:
                print(f"    Warning: Could not force stop PID {pid}: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("Stopping All Processes - Graceful Shutdown")
    print("=" * 60)
    print("")
    
    # 查找所有相关进程
    print("Finding running processes...")
    processes = find_processes()
    
    if not processes:
        print("No running processes found.")
        print("")
        print("=" * 60)
        print("Shutdown Complete")
        print("=" * 60)
        return
    
    # 优雅关闭
    print("Attempting graceful shutdown...")
    print("")
    stop_processes_gracefully(processes)
    
    # 等待进程退出
    print("")
    print("Waiting for processes to exit gracefully (5 seconds)...")
    time.sleep(5)
    
    # 检查是否还有进程在运行
    remaining_processes = find_processes()
    
    if remaining_processes:
        # 强制关闭
        stop_processes_forcefully(remaining_processes)
        time.sleep(1)
    
    # 清理PID文件
    pid_file = Path("data/system.pids")
    if pid_file.exists():
        try:
            pid_file.unlink()
            print("Cleaned up PID file.")
        except Exception:
            pass
    
    print("")
    print("All processes stopped.")
    print("")
    print("=" * 60)
    print("Shutdown Complete")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nShutdown interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}")
        sys.exit(1)
