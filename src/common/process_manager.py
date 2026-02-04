"""
进程管理工具
用于检测和管理系统进程状态
跨平台支持：Windows, Linux, macOS
"""
import sys
import subprocess
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from .logger import get_logger

logger = get_logger('process_manager')


class ProcessManager:
    """进程管理器"""
    
    PROCESS_NAMES = {
        'event_coordinator': 'src.processes.event_coordinator',
        'data_layer': 'src.processes.data_layer',
        'strategy_process': 'src.processes.strategy_process',
        'execution_process': 'src.processes.execution_process',
        'monitoring_process': 'src.processes.monitoring_process',
    }
    
    @staticmethod
    def _extract_account_id(cmdline: str) -> Optional[str]:
        """
        从命令行中提取账户ID（用于执行进程）
        
        Args:
            cmdline: 进程命令行
        
        Returns:
            账户ID，如果没有则返回None
        """
        import re
        # 查找 --account 参数
        match = re.search(r'--account\s+(\w+)', cmdline)
        if match:
            return match.group(1)
        return None
    
    @staticmethod
    def find_processes() -> Dict[str, List[Dict]]:
        """
        查找所有系统相关进程
        
        Returns:
            Dict[process_name, List[Dict]]: 进程信息列表
            每个Dict包含: pid, name, cmdline, status, account_id (如果是执行进程)
        """
        processes = {name: [] for name in ProcessManager.PROCESS_NAMES.keys()}
        
        if sys.platform == 'win32':
            # Windows: 使用PowerShell查找进程
            try:
                ps_cmd = '''
                Get-WmiObject Win32_Process | Where-Object { 
                    $_.CommandLine -like "*src.processes*"
                } | Select-Object ProcessId, Name, CommandLine | ConvertTo-Json
                '''
                result = subprocess.run(
                    ['powershell', '-Command', ps_cmd],
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='ignore',
                    timeout=10
                )
                
                if result.returncode == 0 and result.stdout.strip():
                    import json
                    try:
                        # PowerShell可能返回数组或单个对象
                        data = json.loads(result.stdout)
                        if not isinstance(data, list):
                            data = [data]
                        
                        for proc in data:
                            cmdline = proc.get('CommandLine', '')
                            pid = proc.get('ProcessId')
                            name = proc.get('Name', '')
                            
                            if not pid or not cmdline:
                                continue
                            
                            # Windows: 过滤掉cmd.exe进程（start命令创建的包装进程）
                            # 只统计实际的python.exe进程
                            if sys.platform == 'win32' and name.lower() in ['cmd.exe', 'powershell.exe']:
                                continue
                            
                            # 匹配进程类型
                            for proc_name, pattern in ProcessManager.PROCESS_NAMES.items():
                                if pattern in cmdline:
                                    proc_info = {
                                        'pid': pid,
                                        'name': name,
                                        'cmdline': cmdline,
                                        'status': 'running'
                                    }
                                    
                                    # 如果是执行进程，提取账户ID
                                    if proc_name == 'execution_process':
                                        account_id = ProcessManager._extract_account_id(cmdline)
                                        if account_id:
                                            proc_info['account_id'] = account_id
                                    
                                    # 去重：检查是否已经存在相同的PID（避免重复计数）
                                    existing_pids = [p['pid'] for p in processes[proc_name]]
                                    if pid not in existing_pids:
                                        processes[proc_name].append(proc_info)
                                    break
                    except json.JSONDecodeError:
                        pass
            except Exception as e:
                pass
        else:
            # Linux/macOS: 使用ps命令
            try:
                result = subprocess.run(
                    ['ps', 'aux'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                if result.returncode == 0:
                    for line in result.stdout.split('\n'):
                        if 'src.processes' in line and ('python' in line.lower() or 'python3' in line.lower()):
                            parts = line.split()
                            if len(parts) >= 2:
                                try:
                                    pid = int(parts[1])
                                    cmdline = ' '.join(parts[10:]) if len(parts) > 10 else ' '.join(parts)
                                    
                                    # 匹配进程类型
                                    for proc_name, pattern in ProcessManager.PROCESS_NAMES.items():
                                        if pattern in cmdline:
                                            proc_info = {
                                                'pid': pid,
                                                'name': parts[0] if parts else 'python',
                                                'cmdline': cmdline,
                                                'status': 'running'
                                            }
                                            
                                            # 如果是执行进程，提取账户ID
                                            if proc_name == 'execution_process':
                                                account_id = ProcessManager._extract_account_id(cmdline)
                                                if account_id:
                                                    proc_info['account_id'] = account_id
                                            
                                            # 去重：检查是否已经存在相同的PID（避免重复计数）
                                            existing_pids = [p['pid'] for p in processes[proc_name]]
                                            if pid not in existing_pids:
                                                processes[proc_name].append(proc_info)
                                            break
                                except (ValueError, IndexError):
                                    continue
            except Exception as e:
                pass
        
        return processes
    
    @staticmethod
    def get_process_status() -> Dict[str, Dict]:
        """
        获取所有进程的状态
        
        Returns:
            Dict[process_name, status_info]:
            - status: 'running', 'stopped', 'unknown'
            - count: 运行中的进程数量
            - processes: 进程详情列表
            - accounts: 对于执行进程，按账户分组的进程列表
        """
        found_processes = ProcessManager.find_processes()
        status = {}
        
        for proc_name in ProcessManager.PROCESS_NAMES.keys():
            proc_list = found_processes.get(proc_name, [])
            
            # 去重逻辑
            if len(proc_list) > 1:
                if proc_name == 'execution_process':
                    # 执行进程：按账户分组后去重（每个账户保留PID最大的）
                    # 这样可以正确处理多账户场景，同时处理同一账户的重复进程
                    accounts_dict = {}
                    for proc in proc_list:
                        account_id = proc.get('account_id', 'unknown')
                        if account_id not in accounts_dict:
                            accounts_dict[account_id] = []
                        accounts_dict[account_id].append(proc)
                    
                    # 每个账户只保留PID最大的进程
                    deduplicated_list = []
                    for account_id, account_procs in accounts_dict.items():
                        if len(account_procs) > 1:
                            # 按PID排序，保留最大的（最新的）
                            account_procs = sorted(account_procs, key=lambda x: x.get('pid', 0), reverse=True)
                            deduplicated_list.append(account_procs[0])
                            logger.debug(
                                f"Multiple {proc_name} processes for account {account_id} detected, "
                                f"keeping only PID {account_procs[0].get('pid')}"
                            )
                        else:
                            deduplicated_list.append(account_procs[0])
                    
                    proc_list = deduplicated_list
                else:
                    # 其他进程：简单去重（只保留PID最大的）
                    # 这些进程类型应该只有1个实例
                    proc_list = sorted(proc_list, key=lambda x: x.get('pid', 0), reverse=True)
                    proc_list = [proc_list[0]]
                    logger.debug(
                        f"Multiple {proc_name} processes detected, keeping only PID {proc_list[0].get('pid')}"
                    )
            
            count = len(proc_list)
            
            proc_status = {
                'status': 'running' if count > 0 else 'stopped',
                'count': count,
                'processes': proc_list
            }
            
            # 对于执行进程，按账户分组（用于前端显示）
            if proc_name == 'execution_process' and proc_list:
                accounts = {}
                for proc in proc_list:
                    account_id = proc.get('account_id', 'unknown')
                    if account_id not in accounts:
                        accounts[account_id] = []
                    accounts[account_id].append(proc)
                proc_status['accounts'] = accounts
                proc_status['account_count'] = len(accounts)
            
            status[proc_name] = proc_status
        
        return status
    
    @staticmethod
    def is_process_running(process_name: str) -> bool:
        """
        检查指定进程是否运行
        
        Args:
            process_name: 进程名称（如 'event_coordinator'）
        
        Returns:
            bool: 是否运行
        """
        if process_name not in ProcessManager.PROCESS_NAMES:
            return False
        
        found_processes = ProcessManager.find_processes()
        return len(found_processes.get(process_name, [])) > 0
    
    @staticmethod
    def get_system_info() -> Dict:
        """
        获取系统信息
        
        Returns:
            Dict: 系统信息
        """
        import platform
        import os
        
        info = {
            'platform': platform.system(),
            'platform_version': platform.version(),
            'python_version': platform.python_version(),
        }
        
        # 尝试获取 psutil 信息（推荐方式）
        try:
            import psutil
            mem = psutil.virtual_memory()
            info.update({
                'cpu_count': psutil.cpu_count(logical=True),
                'cpu_count_physical': psutil.cpu_count(logical=False),
                'memory_total': mem.total,
                'memory_available': mem.available,
                'memory_used': mem.used,
                'memory_percent': mem.percent,
                'memory_free': mem.free,
            })
        except ImportError:
            # psutil未安装，尝试使用系统命令获取信息
            if sys.platform == 'win32':
                # Windows: 使用 wmic 命令
                try:
                    # CPU 核心数
                    result = subprocess.run(
                        ['wmic', 'cpu', 'get', 'NumberOfCores', '/value'],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0:
                        for line in result.stdout.split('\n'):
                            if 'NumberOfCores=' in line:
                                cpu_cores = line.split('=')[1].strip()
                                if cpu_cores.isdigit():
                                    info['cpu_count'] = int(cpu_cores)
                                    break
                    
                    # 内存信息
                    result = subprocess.run(
                        ['wmic', 'computersystem', 'get', 'TotalPhysicalMemory', '/value'],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0:
                        for line in result.stdout.split('\n'):
                            if 'TotalPhysicalMemory=' in line:
                                total_mem = line.split('=')[1].strip()
                                if total_mem.isdigit():
                                    info['memory_total'] = int(total_mem)
                                    break
                except Exception:
                    pass
            else:
                # Linux/macOS: 使用系统命令
                try:
                    # CPU 核心数
                    result = subprocess.run(
                        ['nproc'],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0:
                        cpu_count = result.stdout.strip()
                        if cpu_count.isdigit():
                            info['cpu_count'] = int(cpu_count)
                    
                    # 内存信息 (Linux)
                    if sys.platform == 'linux':
                        try:
                            with open('/proc/meminfo', 'r') as f:
                                for line in f:
                                    if line.startswith('MemTotal:'):
                                        mem_kb = int(line.split()[1])
                                        info['memory_total'] = mem_kb * 1024
                                    elif line.startswith('MemAvailable:'):
                                        mem_kb = int(line.split()[1])
                                        info['memory_available'] = mem_kb * 1024
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception as e:
            info['psutil_error'] = str(e)
        
        return info
