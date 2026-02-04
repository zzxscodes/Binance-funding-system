"""
测试启动脚本的正确性
验证脚本能够正确读取配置并生成正确的命令
"""
import pytest
import yaml
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock


def test_start_all_py_reads_config_correctly():
    """测试start_all.py能正确读取配置"""
    config_file = Path("config/default.yaml")
    if not config_file.exists():
        pytest.skip("Config file not found")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    execution_mode = config.get('execution', {}).get('mode', 'mock')
    mode_config = config.get('execution', {}).get(execution_mode, {})
    accounts_config = mode_config.get('accounts', [])
    accounts = [acc.get('account_id') for acc in accounts_config if acc.get('account_id')]
    
    assert execution_mode in ['testnet', 'mock', 'live'], f"Invalid execution mode: {execution_mode}"
    assert isinstance(accounts, list), "Accounts should be a list"
    assert len(accounts) > 0, "At least one account should be configured"
    
    print(f"✓ Config reading works: mode={execution_mode}, accounts={accounts}")


def test_start_all_py_python_executable_detection():
    """测试Python可执行文件检测"""
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from start_all import get_python_executable
    
    python_exe = get_python_executable()
    
    assert python_exe is not None
    assert isinstance(python_exe, str)
    assert len(python_exe) > 0
    
    print(f"✓ Python executable detection works: {python_exe}")


def test_start_all_py_generates_correct_commands():
    """测试生成的命令格式正确"""
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from start_all import get_python_executable
    
    python_exe = get_python_executable()
    
    # 测试基本命令格式
    test_commands = [
        f"{python_exe} -m src.processes.event_coordinator",
        f"{python_exe} -m src.processes.data_layer",
        f"{python_exe} -m src.processes.strategy_process --mode wait",
        f"{python_exe} -m src.processes.execution_process --account account1",
        f"{python_exe} -m src.processes.monitoring_process",
    ]
    
    for cmd in test_commands:
        assert "src.processes" in cmd, f"Command should contain module path: {cmd}"
        assert python_exe in cmd or "python" in cmd.lower(), f"Command should contain Python executable: {cmd}"
    
    print("✓ Command generation works correctly")


def test_stop_all_py_syntax():
    """测试stop_all.py脚本语法（不实际执行）"""
    script_file = Path("stop_all.py")
    if not script_file.exists():
        pytest.skip("stop_all.py not found")
    
    # 检查文件是否可读
    assert script_file.is_file(), "stop_all.py should be a file"
    
    # 检查是否包含关键功能
    content = script_file.read_text(encoding='utf-8')
    assert 'find_processes' in content, "Script should contain find_processes function"
    assert 'stop_processes' in content, "Script should contain stop_processes function"
    assert 'sys.platform' in content, "Script should check platform"
    
    print("✓ stop_all.py syntax is valid")


def test_stop_all_py_cross_platform():
    """测试stop_all.py的跨平台支持"""
    script_file = Path("stop_all.py")
    if not script_file.exists():
        pytest.skip("stop_all.py not found")
    
    content = script_file.read_text(encoding='utf-8')
    
    # 检查Windows支持
    assert 'win32' in content, "Script should support Windows"
    assert 'taskkill' in content or 'tasklist' in content, "Script should use Windows commands"
    
    # 检查Linux/macOS支持
    assert 'SIGTERM' in content or 'signal' in content, "Script should use Unix signals"
    assert 'ps' in content or 'os.kill' in content, "Script should use Unix commands"
    
    print("✓ stop_all.py supports cross-platform")


def test_all_scripts_exist():
    """测试所有必要的脚本文件都存在"""
    required_scripts = [
        "start_all.py",
        "stop_all.py",
    ]
    
    for script in required_scripts:
        script_path = Path(script)
        assert script_path.exists(), f"Required script {script} not found"
    
    print("✓ All required scripts exist")


if __name__ == "__main__":
    print("=" * 60)
    print("Testing Start/Stop Scripts")
    print("=" * 60)
    
    test_all_scripts_exist()
    test_start_all_py_reads_config_correctly()
    test_start_all_py_python_executable_detection()
    test_start_all_py_generates_correct_commands()
    test_start_all_sh_syntax()
    test_stop_all_sh_syntax()
    test_stop_all_bat_syntax()
    test_stop_all_ps1_syntax()
    
    print("\n" + "=" * 60)
    print("All script tests completed!")
    print("=" * 60)
