"""
测试停止脚本功能
"""
import pytest
import subprocess
import sys
from pathlib import Path


def test_stop_all_script_exists():
    """测试停止脚本存在"""
    stop_script = Path("stop_all.py")
    assert stop_script.exists(), "stop_all.py should exist"
    print("[OK] stop_all.py exists")


def test_stop_all_script_importable():
    """测试停止脚本可以导入"""
    try:
        import stop_all
        assert hasattr(stop_all, 'find_processes')
        assert hasattr(stop_all, 'stop_processes_gracefully')
        assert hasattr(stop_all, 'stop_processes_forcefully')
        print("[OK] stop_all.py is importable and has required functions")
    except ImportError as e:
        pytest.fail(f"Failed to import stop_all: {e}")


def test_find_processes_function():
    """测试find_processes函数"""
    import stop_all
    
    # 测试函数可以运行（不抛出异常）
    try:
        processes = stop_all.find_processes()
        assert isinstance(processes, list)
        print(f"[OK] find_processes() returns list with {len(processes)} processes")
    except Exception as e:
        pytest.fail(f"find_processes() raised exception: {e}")


@pytest.mark.skipif(sys.platform != 'win32', reason="Windows-specific test")
def test_stop_all_powershell_method():
    """测试Windows PowerShell查找进程方法"""
    import subprocess
    
    # 测试PowerShell命令可以执行
    ps_cmd = '''
    Get-WmiObject Win32_Process | Where-Object { 
        $_.CommandLine -like "*src.processes*"
    } | Select-Object -ExpandProperty ProcessId
    '''
    
    try:
        result = subprocess.run(
            ['powershell', '-Command', ps_cmd],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore',
            timeout=10
        )
        # 应该能执行，即使没有找到进程
        assert result.returncode == 0 or result.returncode is None
        print("[OK] PowerShell method works")
    except Exception as e:
        pytest.fail(f"PowerShell method failed: {e}")
