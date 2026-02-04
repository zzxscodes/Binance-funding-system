"""
动态启动所有进程，从配置文件读取账户列表
跨平台支持：Windows, Linux, macOS
只启动配置完整且有效的账户
"""
import yaml
import subprocess
import sys
import os
import time
from pathlib import Path

# 添加src目录到路径
sys.path.insert(0, str(Path(__file__).parent))
from src.common.account_validator import get_valid_accounts, validate_all_accounts

def get_python_executable():
    """获取Python可执行文件路径"""
    if sys.platform == 'win32':
        # Windows: 尝试使用虚拟环境中的Python
        venv_python = Path("quant/Scripts/python.exe")
        if venv_python.exists():
            return str(venv_python)
        return "python"
    else:
        # Linux/macOS: 尝试使用虚拟环境中的Python
        venv_python = Path("quant/bin/python")
        if venv_python.exists():
            return str(venv_python)
        return "python3"

def main():
    # 读取配置
    config_file = Path("config/default.yaml")
    if not config_file.exists():
        print(f"Error: Config file not found: {config_file}")
        sys.exit(1)
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    # 获取执行模式
    execution_mode = config.get('execution', {}).get('mode', 'mock')
    print(f"Execution mode: {execution_mode}")
    
    # 从对应模式的配置中获取账户列表
    mode_config = config.get('execution', {}).get(execution_mode, {})
    if not mode_config:
        print(f"Error: Configuration for mode '{execution_mode}' not found")
        sys.exit(1)
    
    # 验证并获取有效的账户列表（只包含配置完整的账户）
    valid_accounts, invalid_accounts = validate_all_accounts(execution_mode)
    
    print(f"Found {len(valid_accounts)} valid account(s) in {execution_mode} mode: {valid_accounts}")
    
    if invalid_accounts:
        print(f"\nWarning: {len(invalid_accounts)} account(s) skipped due to incomplete configuration:")
        for account_id, error_msg in invalid_accounts:
            if account_id:
                print(f"  - {account_id}: {error_msg}")
            else:
                print(f"  - {error_msg}")
        print("")
    
    if not valid_accounts:
        print(f"Error: No valid accounts found in {execution_mode} mode.")
        print("Please configure at least one account with complete settings.")
        sys.exit(1)
    
    accounts = valid_accounts
    
    # 获取Python可执行文件
    python_exe = get_python_executable()
    
    # 启动进程
    processes = [
        ("Event Coordinator", f"{python_exe} -m src.processes.event_coordinator"),
        ("Data Layer", f"{python_exe} -m src.processes.data_layer"),
        ("Strategy Process", f"{python_exe} -m src.processes.strategy_process --mode wait"),
    ]
    
    # 为每个账户启动执行进程
    for account in accounts:
        processes.append(
            (f"Execution {account}", f"{python_exe} -m src.processes.execution_process --account {account}")
        )
    
    processes.append(
        ("Monitoring", f"{python_exe} -m src.processes.monitoring_process")
    )
    
    print("\nStarting processes...")
    
    if sys.platform == 'win32':
        # Windows: 在新窗口中启动
        for name, cmd in processes:
            print(f"  Starting {name}...")
            # 使用引号包裹窗口标题，避免被当作命令
            subprocess.Popen(
                f'start "{name}" cmd /k "{cmd}"',
                shell=True
            )
            time.sleep(1)
        print(f"\nAll {len(processes)} processes started in separate windows.")
    else:
        # Linux/macOS: 后台运行并记录日志
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        # 保存进程PID到文件，方便stop_all.py使用
        pid_file = Path("data/system.pids")
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pids = []
        
        for name, cmd in processes:
            print(f"  Starting {name}...")
            # 生成日志文件名
            log_name = name.lower().replace(' ', '_')
            log_file = logs_dir / f"{log_name}.log"
            
            # 后台运行并重定向输出
            try:
                proc = subprocess.Popen(
                    cmd.split(),
                    stdout=open(log_file, 'w'),
                    stderr=subprocess.STDOUT,
                    start_new_session=True  # 创建新的进程组
                )
                pids.append(proc.pid)
                print(f"    Started with PID {proc.pid}")
            except Exception as e:
                print(f"    Error starting {name}: {e}")
            time.sleep(1)
        
        # 保存PID到文件
        if pids:
            with open(pid_file, 'w') as f:
                for pid in pids:
                    f.write(f"{pid}\n")
        
        print(f"\nAll {len(processes)} processes started in background.")
        print(f"Logs are written to logs/ directory")
        print(f"Process PIDs saved to {pid_file}")
        print("Use 'python stop_all.py' to stop all processes.")

if __name__ == "__main__":
    main()
