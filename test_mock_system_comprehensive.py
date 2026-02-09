"""
Mock模式系统全面测试脚本
1. 生成模拟数据（K线、资金费率）
2. 启动整个系统（5个进程）
3. 监控执行日志
4. 审查系统执行流程
"""
import asyncio
import sys
import time
import subprocess
import signal
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import yaml

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from src.backtest.mock_data import MockDataManager, MockFundingRateGenerator
from src.data.storage import get_data_storage
from src.common.config import config
from src.common.logger import get_logger

logger = get_logger('mock_system_test')


class MockSystemTester:
    """Mock模式系统测试器"""
    
    def __init__(self):
        self.storage = get_data_storage()
        self.mock_data_manager = MockDataManager()
        self.processes: List[subprocess.Popen] = []
        self.logs_dir = Path("logs")
        self.logs_dir.mkdir(exist_ok=True)
        
    def generate_mock_data(self):
        """生成模拟数据"""
        logger.info("=" * 80)
        logger.info("步骤1: 生成模拟数据")
        logger.info("=" * 80)
        
        # 测试用的交易对
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT']
        
        # 生成最近7天的数据
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)
        
        # 初始价格
        initial_prices = {
            'BTCUSDT': 50000.0,
            'ETHUSDT': 3000.0,
            'BNBUSDT': 600.0,
            'SOLUSDT': 100.0,
            'ADAUSDT': 1.0,
        }
        
        # 波动率
        volatilities = {
            'BTCUSDT': 0.02,
            'ETHUSDT': 0.025,
            'BNBUSDT': 0.03,
            'SOLUSDT': 0.04,
            'ADAUSDT': 0.035,
        }
        
        logger.info(f"生成K线数据: {start_date.date()} 到 {end_date.date()}")
        self.mock_data_manager.generate_and_save_mock_data(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            initial_prices=initial_prices,
            volatilities=volatilities,
            seed=42,  # 固定种子，便于复现
            interval_minutes=5,
        )
        
        # 生成资金费率数据
        logger.info("生成资金费率数据...")
        funding_rates_data = MockFundingRateGenerator.generate_funding_rates(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
        )
        
        # 保存资金费率数据
        for symbol, df in funding_rates_data.items():
            # 转换为标准格式
            df['fundingTime'] = df['timestamp']
            df['fundingRate'] = df['funding_rate']
            df = df[['fundingTime', 'fundingRate']]
            self.storage.save_funding_rates(symbol=symbol, df=df)
            logger.info(f"  保存 {symbol}: {len(df)} 条资金费率记录")
        
        logger.info("[OK] 模拟数据生成完成")
    
    def update_config_for_mock(self):
        """更新配置文件以启用mock模式"""
        logger.info("=" * 80)
        logger.info("步骤2: 更新配置文件")
        logger.info("=" * 80)
        
        config_file = Path("config/default.yaml")
        if not config_file.exists():
            logger.error(f"配置文件不存在: {config_file}")
            return False
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        # 确保execution.mode设置为mock
        if 'execution' not in config_data:
            config_data['execution'] = {}
        config_data['execution']['mode'] = 'mock'
        
        # 确保mock配置存在
        if 'mock' not in config_data.get('execution', {}):
            config_data['execution']['mock'] = {
                'api_base': 'https://mock.binance.com',
                'ws_base': 'wss://mock.binance.com',
                'accounts': [
                    {
                        'account_id': 'account1',
                        'total_wallet_balance': 100000.0,
                        'available_balance': 50000.0,
                        'initial_positions': []
                    }
                ]
            }
        
        # 确保strategy配置启用我们的calculator
        if 'strategy' not in config_data:
            config_data['strategy'] = {}
        if 'calculators' not in config_data['strategy']:
            config_data['strategy']['calculators'] = {}
        
        # 确保enabled是列表类型
        enabled = config_data['strategy']['calculators'].get('enabled')
        if enabled is None or not isinstance(enabled, list):
            config_data['strategy']['calculators']['enabled'] = []
        
        # 添加我们的mock_test_calculator
        enabled_list = config_data['strategy']['calculators']['enabled']
        if 'mock_test_calculator' not in enabled_list:
            enabled_list.append('mock_test_calculator')
        
        # 保存配置
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config_data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        
        logger.info("[OK] 配置文件已更新")
        return True
    
    def start_system(self):
        """启动整个系统"""
        logger.info("=" * 80)
        logger.info("步骤3: 启动系统进程")
        logger.info("=" * 80)
        
        # 获取Python可执行文件
        python_exe = self._get_python_executable()
        
        # 启动进程
        processes_to_start = [
            ("Event Coordinator", f"{python_exe} -m src.processes.event_coordinator"),
            ("Data Layer", f"{python_exe} -m src.processes.data_layer"),
            ("Strategy Process", f"{python_exe} -m src.processes.strategy_process --mode wait"),
            ("Execution account1", f"{python_exe} -m src.processes.execution_process --account account1"),
            ("Monitoring", f"{python_exe} -m src.processes.monitoring_process"),
        ]
        
        logger.info(f"启动 {len(processes_to_start)} 个进程...")
        
        for name, cmd in processes_to_start:
            logger.info(f"  启动 {name}...")
            try:
                if sys.platform == 'win32':
                    # Windows: 在新窗口中启动
                    proc = subprocess.Popen(
                        f'start "{name}" cmd /k "{cmd}"',
                        shell=True
                    )
                else:
                    # Linux/macOS: 后台运行并记录日志
                    log_file = self.logs_dir / f"{name.lower().replace(' ', '_')}.log"
                    proc = subprocess.Popen(
                        cmd.split(),
                        stdout=open(log_file, 'w'),
                        stderr=subprocess.STDOUT,
                        start_new_session=True
                    )
                    logger.info(f"    PID: {proc.pid}, 日志: {log_file}")
                
                self.processes.append(proc)
                time.sleep(2)  # 等待进程启动
            except Exception as e:
                logger.error(f"  启动 {name} 失败: {e}")
        
        logger.info("[OK] 所有进程已启动")
        logger.info(f"  进程数量: {len(self.processes)}")
        logger.info(f"  日志目录: {self.logs_dir}")
    
    def monitor_system(self, duration_seconds: int = 300):
        """监控系统运行"""
        logger.info("=" * 80)
        logger.info(f"步骤4: 监控系统运行 ({duration_seconds}秒)")
        logger.info("=" * 80)
        
        start_time = time.time()
        check_interval = 10  # 每10秒检查一次
        
        while time.time() - start_time < duration_seconds:
            elapsed = int(time.time() - start_time)
            remaining = duration_seconds - elapsed
            
            logger.info(f"运行时间: {elapsed}秒 / {duration_seconds}秒 (剩余 {remaining}秒)")
            
            # 检查日志文件
            self._check_logs()
            
            time.sleep(check_interval)
        
        logger.info("[OK] 监控完成")
    
    def _check_logs(self):
        """检查日志文件"""
        log_files = list(self.logs_dir.glob("*.log"))
        if log_files:
            logger.info(f"  发现 {len(log_files)} 个日志文件")
            for log_file in log_files[-3:]:  # 只显示最近3个
                try:
                    size = log_file.stat().st_size
                    logger.info(f"    {log_file.name}: {size} bytes")
                except:
                    pass
    
    def stop_system(self):
        """停止系统"""
        logger.info("=" * 80)
        logger.info("步骤5: 停止系统")
        logger.info("=" * 80)
        
        # 尝试使用stop_all.py
        try:
            stop_script = Path("stop_all.py")
            if stop_script.exists():
                logger.info("使用 stop_all.py 停止系统...")
                subprocess.run([sys.executable, str(stop_script)], timeout=10)
            else:
                logger.warning("stop_all.py 不存在，手动停止进程...")
                for proc in self.processes:
                    try:
                        proc.terminate()
                    except:
                        pass
        except Exception as e:
            logger.error(f"停止系统时出错: {e}")
        
        logger.info("[OK] 系统已停止")
    
    def _get_python_executable(self):
        """获取Python可执行文件路径"""
        if sys.platform == 'win32':
            venv_python = Path("quant/Scripts/python.exe")
            if venv_python.exists():
                return str(venv_python)
            return "python"
        else:
            venv_python = Path("quant/bin/python")
            if venv_python.exists():
                return str(venv_python)
            return "python3"
    
    def analyze_logs(self):
        """分析日志文件"""
        logger.info("=" * 80)
        logger.info("步骤6: 分析执行日志")
        logger.info("=" * 80)
        
        log_files = list(self.logs_dir.glob("*.log"))
        if not log_files:
            logger.warning("未找到日志文件")
            return
        
        logger.info(f"找到 {len(log_files)} 个日志文件")
        
        # 分析每个日志文件
        for log_file in sorted(log_files):
            logger.info(f"\n分析: {log_file.name}")
            try:
                with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()
                    
                logger.info(f"  总行数: {len(lines)}")
                
                # 查找关键信息
                keywords = ['ERROR', 'WARNING', 'INFO', '订单', '持仓', '权重', '计算']
                for keyword in keywords:
                    count = sum(1 for line in lines if keyword in line.upper())
                    if count > 0:
                        logger.info(f"  {keyword}: {count} 次")
                
                # 显示最后几行
                if lines:
                    logger.info(f"  最后5行:")
                    for line in lines[-5:]:
                        logger.info(f"    {line.strip()}")
            except Exception as e:
                logger.error(f"  读取日志失败: {e}")


def main():
    """主函数"""
    tester = MockSystemTester()
    
    try:
        # 1. 生成模拟数据
        tester.generate_mock_data()
        
        # 2. 更新配置
        if not tester.update_config_for_mock():
            logger.error("配置更新失败，退出")
            return
        
        # 3. 启动系统
        tester.start_system()
        
        # 4. 监控系统
        tester.monitor_system(duration_seconds=300)  # 运行5分钟
        
        # 5. 分析日志
        tester.analyze_logs()
        
    except KeyboardInterrupt:
        logger.info("\n收到中断信号，停止系统...")
    except Exception as e:
        logger.error(f"测试过程中出错: {e}", exc_info=True)
    finally:
        # 6. 停止系统
        tester.stop_system()
    
    logger.info("=" * 80)
    logger.info("测试完成")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
