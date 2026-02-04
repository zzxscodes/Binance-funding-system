"""
监控进程（Process 5）
持续运行，监控系统状态和账户信息
提供 HTTP API 和 Web 前端界面
"""
import asyncio
import signal
import sys
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timezone

from ..common.config import config
from ..common.logger import get_logger
from ..common.ipc import IPCClient, MessageType
from ..common.process_manager import ProcessManager
from ..execution.binance_client import BinanceClient
from ..monitoring.metrics import get_metrics_calculator
from ..monitoring.alert import get_alert_manager
from ..monitoring.equity_curve import get_equity_curve_tracker
from ..monitoring.performance import get_performance_monitor
from ..strategy.position_generator import get_position_generator

logger = get_logger('monitoring_process')

# FastAPI imports (optional, graceful fallback if not installed)
try:
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.staticfiles import StaticFiles
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    logger.warning("FastAPI not available, HTTP API will be disabled")


class MonitoringProcess:
    """监控进程"""
    
    def __init__(self):
        # 记录启动时间（用于计算运行时间）
        self.start_time = time.time()
        
        # 获取执行模式
        execution_mode = config.get('execution.mode', 'mock')
        
        # 从对应模式的配置中获取账户列表
        mode_config = config.get(f'execution.{execution_mode}', {})
        self.accounts_config = mode_config.get('accounts', [])
        self.account_clients: Dict[str, BinanceClient] = {}
        
        # 初始化账户客户端
        self._init_account_clients()
        
        # 初始化指标计算器
        self.metrics_calculator = get_metrics_calculator()
        
        # 初始化告警管理器
        self.alert_manager = get_alert_manager()
        
        # 初始化Equity Curve追踪器
        self.equity_curve_tracker = get_equity_curve_tracker()
        
        # 初始化目标持仓加载器
        self.position_generator = get_position_generator()
        
        # 初始化性能监控器
        self.performance_monitor = get_performance_monitor()
        
        # IPC客户端（可选，用于接收订单执行事件）
        self.ipc_client: Optional[IPCClient] = None
        
        # 状态
        self.running = False
        self.check_interval = config.get('monitoring.check_interval', 60)  # 秒
        
        # 监控数据
        self.monitoring_data: Dict[str, Dict] = {}  # account_id -> metrics
        
        # HTTP API 服务器
        self.http_server = None
        self.http_port = config.get('monitoring.http_port', 8080)
        self.app = None
        if FASTAPI_AVAILABLE:
            self._init_http_server()
    
    def _init_account_clients(self):
        """初始化账户客户端"""
        import os
        
        # 获取执行模式
        execution_mode = config.get('execution.mode', 'mock')
        dry_run_flag = config.get('execution.dry_run', False)
        
        # 获取对应模式的配置
        mode_config = config.get(f'execution.{execution_mode}', {})
        
        for account_config in self.accounts_config:
            account_id = account_config.get('account_id')
            if not account_id:
                continue
            
            try:
                if execution_mode == 'testnet':
                    # Testnet模式：使用testnet端点和API密钥
                    api_base = mode_config.get('api_base', 'https://testnet.binancefuture.com')
                    
                    # 优先级：环境变量 > 配置文件
                    api_key_env = f"{account_id.upper()}_TESTNET_API_KEY"
                    api_secret_env = f"{account_id.upper()}_TESTNET_API_SECRET"
                    
                    api_key = os.getenv(api_key_env, account_config.get('api_key', ''))
                    api_secret = os.getenv(api_secret_env, account_config.get('api_secret', ''))
                    
                    if api_key and api_secret:
                        client = BinanceClient(api_key, api_secret, api_base=api_base, dry_run=dry_run_flag)
                        self.account_clients[account_id] = client
                        logger.info(f"Initialized testnet client for account {account_id} (dry_run={dry_run_flag})")
                    else:
                        logger.warning(f"Testnet mode requires API keys for account {account_id}, skipping")
                        
                elif execution_mode == 'mock':
                    # Mock模式：使用模拟端点和模拟账户信息
                    api_base = mode_config.get('api_base', 'https://mock.binance.com')
                    
                    from ..execution.dry_run_client import DryRunBinanceClient
                    client = DryRunBinanceClient(
                        account_id=account_id,
                        api_base=api_base,
                        use_real_exchange_info=False  # Mock模式不使用真实交易所信息
                    )
                    self.account_clients[account_id] = client
                    logger.info(f"Initialized mock client for account {account_id}")
                    
                elif execution_mode == 'live':
                    # Live模式：使用实盘端点和真实API密钥
                    api_base = mode_config.get('api_base', 'https://fapi.binance.com')
                    
                    # 优先级：环境变量 > 配置文件
                    api_key_env = f"{account_id.upper()}_API_KEY"
                    api_secret_env = f"{account_id.upper()}_API_SECRET"
                    
                    api_key = os.getenv(api_key_env, account_config.get('api_key', ''))
                    api_secret = os.getenv(api_secret_env, account_config.get('api_secret', ''))
                    
                    if api_key and api_secret:
                        client = BinanceClient(api_key, api_secret, api_base=api_base, dry_run=dry_run_flag)
                        self.account_clients[account_id] = client
                        if not dry_run_flag:
                            logger.warning(f"Initialized live client for account {account_id} (REAL TRADING ENABLED!)")
                        else:
                            logger.info(f"Initialized live client for account {account_id} (with DRY-RUN enabled)")
                    else:
                        logger.warning(f"Live mode requires API keys for account {account_id}, skipping")
            except Exception as e:
                logger.error(f"Failed to initialize client for account {account_id}: {e}")
    
    async def _connect_ipc(self):
        """连接IPC服务器（事件协调进程）"""
        try:
            self.ipc_client = IPCClient()
            await self.ipc_client.connect()
            logger.info("Connected to event coordinator via IPC for monitoring")
        except Exception as e:
            logger.warning(f"Failed to connect to event coordinator: {e}")
            logger.warning("Will continue without IPC connection")
    
    async def _convert_weights_to_quantities(
        self,
        account_id: str,
        client: BinanceClient,
        target_positions_weights: Dict[str, float],
        account_info: Dict
    ) -> Dict[str, float]:
        """
        将权重转换为实际数量（与执行进程保持一致）
        
        Args:
            account_id: 账户ID
            client: Binance客户端
            target_positions_weights: Dict[symbol, weight]，目标持仓权重
            account_info: 账户信息
        
        Returns:
            Dict[symbol, quantity]，转换后的实际数量
        """
        try:
            if not target_positions_weights:
                return {}
            
            # 获取账户余额
            total_balance = float(account_info.get('totalWalletBalance', 0) or 
                                 account_info.get('totalMarginBalance', 0) or 0)
            
            if total_balance <= 0:
                logger.warning(f"Invalid account balance for {account_id}: {total_balance}, using weights as quantities")
                return target_positions_weights
            
            # 计算可用于交易的金额
            available_capital = total_balance
            
            # 归一化权重：计算总权重（绝对值之和），然后归一化
            total_weight = sum(abs(w) for w in target_positions_weights.values())
            if total_weight > 1e-8:
                # 归一化权重，使总权重为1.0（100%）
                normalized_weights = {symbol: w / total_weight for symbol, w in target_positions_weights.items()}
            else:
                # 如果总权重为0，使用原始权重
                normalized_weights = target_positions_weights
                logger.debug("Total weight is 0, using original weights")
            
            # 转换权重为数量
            target_positions = {}
            
            for symbol, weight in normalized_weights.items():
                if abs(weight) < 1e-8:  # 权重为0，跳过
                    continue
                
                # 计算该交易对应该分配的USDT金额（使用归一化后的权重）
                target_notional = abs(weight) * available_capital
                
                # 获取当前价格
                try:
                    current_price = await client.get_symbol_price(symbol)
                    if current_price and current_price > 0:
                        # 计算数量
                        quantity = target_notional / current_price
                        # 根据方向设置正负（使用归一化后的权重）
                        quantity = quantity if weight > 0 else -quantity
                        target_positions[symbol] = quantity
                    else:
                        logger.debug(f"Could not get price for {symbol}, using weight as quantity")
                        # 如果无法获取价格，使用原始权重（可能是数量而不是权重）
                        target_positions[symbol] = weight
                except Exception as e:
                    logger.debug(f"Failed to convert weight to quantity for {symbol}: {e}, using original value")
                    target_positions[symbol] = weight
            
            logger.info(
                f"Converted weights to quantities for {account_id}: {len(target_positions)} symbols, "
                f"total_balance={total_balance:.2f} USDT, "
                f"total_weight={total_weight:.4f} (normalized to 1.0)"
            )
            
            return target_positions
            
        except Exception as e:
            logger.error(f"Failed to convert weights to quantities for {account_id}: {e}", exc_info=True)
            # 如果转换失败，返回原始值（可能是数量而不是权重）
            return target_positions_weights
    
    async def _monitor_account(self, account_id: str) -> Dict:
        """
        监控单个账户
        
        Args:
            account_id: 账户ID
        
        Returns:
            监控指标字典
        """
        try:
            client = self.account_clients.get(account_id)
            if not client:
                logger.warning(f"No client found for account {account_id}")
                return {}
            
            # 1. 获取账户信息
            account_info = await client.get_account_info()
            
            # 2. 获取持仓
            positions = await client.get_positions()
            
            # 3. 计算账户指标
            account_metrics = self.metrics_calculator.calculate_account_metrics(
                account_info, positions
            )
            
            # 4. 加载目标持仓（如果有）
            target_positions_weights = {}
            try:
                # 查找最新的目标持仓文件
                positions_dir = Path(config.get('data.positions_directory', 'data/positions'))
                if positions_dir.exists():
                    target_files = sorted(
                        positions_dir.glob(f"{account_id}_target_positions_*.json"),
                        reverse=True
                    )
                    if target_files:
                        latest_file = target_files[0]
                        target_data = self.position_generator.load_target_positions(str(latest_file))
                        if target_data and 'positions' in target_data:
                            for pos in target_data['positions']:
                                symbol = pos.get('symbol', '').upper()
                                target_pos = float(pos.get('target_position', 0.0))
                                target_positions_weights[symbol] = target_pos
            except Exception as e:
                logger.warning(f"Failed to load target positions for {account_id}: {e}")
            
            # 5. 将权重转换为实际数量（与执行进程保持一致）
            target_positions = await self._convert_weights_to_quantities(
                account_id, client, target_positions_weights, account_info
            )
            
            # 6. 计算持仓偏差
            current_positions = {
                pos.get('symbol', '').upper(): float(pos.get('positionAmt', 0))
                for pos in positions
            }
            
            deviation_metrics = self.metrics_calculator.calculate_position_deviation(
                current_positions, target_positions
            )
            
            # 6. 组装监控数据
            monitoring_data = {
                'account_id': account_id,
                'account_metrics': account_metrics,
                'exposure': account_metrics.get('exposure', {}),
                'deviation': deviation_metrics,
                'positions_count': len(positions),
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
            
            # 7. 检查告警
            alerts = []
            
            # 持仓偏差告警
            alerts.extend(self.alert_manager.check_position_deviation(deviation_metrics, account_id))
            
            # 敞口告警
            exposure = account_metrics.get('exposure', {})
            alerts.extend(self.alert_manager.check_exposure(exposure, account_id))
            
            # 账户健康度告警
            alerts.extend(self.alert_manager.check_account_health(account_metrics, account_id))
            
            monitoring_data['alerts'] = alerts
            monitoring_data['alerts_count'] = len(alerts)
            
            # 记录Equity Curve快照
            try:
                timestamp = datetime.now(timezone.utc)
                self.equity_curve_tracker.record_snapshot(
                    account_id=account_id,
                    timestamp=timestamp,
                    total_balance=account_metrics.get('total_wallet_balance', 0),
                    available_balance=account_metrics.get('available_balance', 0),
                    unrealized_pnl=account_metrics.get('total_unrealized_pnl', 0),
                    margin_balance=account_metrics.get('margin_balance'),  # 直接使用totalMarginBalance作为净值
                    positions=positions,
                    metadata={
                        'exposure': exposure,
                        'deviation': deviation_metrics,
                        'alerts_count': len(alerts)
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to record equity curve snapshot for {account_id}: {e}")
            
            return monitoring_data
            
        except Exception as e:
            logger.error(f"Failed to monitor account {account_id}: {e}", exc_info=True)
            return {
                'account_id': account_id,
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
    
    async def _monitor_all_accounts(self):
        """监控所有账户"""
        tasks = []
        
        for account_id in self.account_clients.keys():
            task = self._monitor_account(account_id)
            tasks.append(task)
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 更新监控数据
            for result in results:
                if isinstance(result, dict) and 'account_id' in result:
                    account_id = result['account_id']
                    self.monitoring_data[account_id] = result
    
    def _print_monitoring_summary(self):
        """打印监控摘要"""
        logger.info("=" * 80)
        logger.info("Monitoring Summary")
        logger.info("=" * 80)
        
        for account_id, data in self.monitoring_data.items():
            if 'error' in data:
                logger.warning(f"Account {account_id}: Error - {data['error']}")
                continue
            
            account_metrics = data.get('account_metrics', {})
            exposure = data.get('exposure', {})
            deviation = data.get('deviation', {})
            alerts_count = data.get('alerts_count', 0)
            
            logger.info(f"Account {account_id}:")
            logger.info(
                f"  Balance: {account_metrics.get('total_wallet_balance', 0):,.2f} USDT "
                f"(Available: {account_metrics.get('available_balance', 0):,.2f})"
            )
            logger.info(
                f"  Unrealized PnL: {account_metrics.get('total_unrealized_pnl', 0):,.2f} USDT"
            )
            logger.info(
                f"  Positions: {account_metrics.get('open_positions_count', 0)} "
                f"(Value: {account_metrics.get('position_value', 0):,.2f} USDT)"
            )
            logger.info(
                f"  Exposure: Net={exposure.get('net_exposure', 0):,.2f}, "
                f"Total={exposure.get('total_exposure', 0):,.2f}"
            )
            logger.info(
                f"  Deviation: Avg={deviation.get('avg_deviation', 0):.4f}, "
                f"Max={deviation.get('max_deviation', 0):.4f}"
            )
            
            if alerts_count > 0:
                logger.warning(f"  Alerts: {alerts_count}")
            
            logger.info("-" * 80)
        
        logger.info("=" * 80)
    
    async def start(self):
        """启动监控进程"""
        if self.running:
            logger.warning("Monitoring process is already running")
            return
        
        logger.info("Starting monitoring process...")
        self.running = True
        
        if not self.account_clients:
            logger.warning("No account clients initialized, monitoring will not run")
            logger.warning("Please check account configuration in config/default.yaml")
            # 即使没有账户客户端，也继续启动（但不执行监控循环）
        
        # 连接IPC（可选）
        try:
            await self._connect_ipc()
        except Exception as e:
            logger.warning(f"IPC connection failed: {e}")
        
        # 启动 HTTP 服务器（在后台任务中）
        if FASTAPI_AVAILABLE and self.app:
            logger.info(f"HTTP API server will start on http://0.0.0.0:{self.http_port}")
    
    async def run(self):
        """运行监控循环"""
        if not self.account_clients:
            logger.warning("No account clients available, monitoring loop will not run")
            logger.warning("Monitoring process will keep running but will not monitor any accounts")
            # 保持进程运行但不执行监控
            while self.running:
                await asyncio.sleep(self.check_interval)
            return
        
        logger.info(
            f"Monitoring process running, checking every {self.check_interval} seconds..."
        )
        
        # 启动 HTTP 服务器（在后台任务中）
        if FASTAPI_AVAILABLE and self.app:
            asyncio.create_task(self._start_http_server())
        
        try:
            while self.running:
                # 监控所有账户
                await self._monitor_all_accounts()
                
                # 打印摘要
                self._print_monitoring_summary()
                
                # 等待下次检查
                await asyncio.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Error in monitoring loop: {e}", exc_info=True)
    
    async def stop(self):
        """停止监控进程"""
        logger.info("Stopping monitoring process...")
        self.running = False
        
        # 停止 HTTP 服务器
        await self._stop_http_server()
        
        # 关闭所有账户客户端
        for account_id, client in self.account_clients.items():
            try:
                await client.close()
            except Exception as e:
                logger.error(f"Failed to close client for account {account_id}: {e}")
        
        # 断开IPC连接
        if self.ipc_client:
            await self.ipc_client.disconnect()
        
        logger.info("Monitoring process stopped")
    
    def get_monitoring_data(self) -> Dict:
        """获取监控数据"""
        return self.monitoring_data.copy()
    
    def _init_http_server(self):
        """初始化 HTTP 服务器"""
        if not FASTAPI_AVAILABLE:
            return
        
        self.app = FastAPI(title="Trading System Monitor", version="1.0.0")
        
        # CORS middleware
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # API routes
        @self.app.get("/")
        async def root():
            """返回 HTML 前端页面"""
            html_path = Path(__file__).parent.parent.parent / "web" / "monitor.html"
            if html_path.exists():
                return HTMLResponse(content=html_path.read_text(encoding='utf-8'))
            else:
                return HTMLResponse(content="<h1>Monitor API</h1><p>Web interface not found. API is available at /api/*</p>")
        
        @self.app.get("/api/status")
        async def get_status():
            """获取系统状态"""
            return JSONResponse({
                "status": "running" if self.running else "stopped",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "processes": ProcessManager.get_process_status(),
                "system_info": ProcessManager.get_system_info(),
            })
        
        @self.app.get("/api/accounts")
        async def get_accounts():
            """获取所有账户的监控数据"""
            return JSONResponse(self.monitoring_data)
        
        @self.app.get("/api/accounts/{account_id}")
        async def get_account(account_id: str):
            """获取指定账户的监控数据"""
            if account_id in self.monitoring_data:
                return JSONResponse(self.monitoring_data[account_id])
            else:
                return JSONResponse({"error": f"Account {account_id} not found"}, status_code=404)
        
        @self.app.get("/api/processes")
        async def get_processes():
            """获取所有进程状态"""
            return JSONResponse(ProcessManager.get_process_status())
        
        @self.app.get("/api/system")
        async def get_system():
            """获取系统信息"""
            return JSONResponse(ProcessManager.get_system_info())
        
        @self.app.get("/api/equity-curve/{account_id}")
        async def get_equity_curve(account_id: str):
            """获取Equity Curve数据"""
            try:
                from datetime import timedelta
                import pandas as pd
                end_date = datetime.now(timezone.utc)
                start_date = end_date - timedelta(days=7)  # 最近7天
                
                df = self.equity_curve_tracker.get_equity_curve(account_id, start_date, end_date)
                if df.empty:
                    return JSONResponse({"data": [], "summary": {}})
                
                # 将Timestamp列转换为字符串，避免JSON序列化错误
                df_copy = df.copy()
                for col in df_copy.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                
                summary = self.equity_curve_tracker.get_summary(account_id)
                # 清理数据中的NaN和Inf值，避免JSON序列化错误
                from ..common.utils import sanitize_for_json
                data_dict = df_copy.to_dict('records')
                cleaned_data = sanitize_for_json(data_dict)
                cleaned_summary = sanitize_for_json(summary)
                return JSONResponse({
                    "data": cleaned_data,
                    "summary": cleaned_summary
                })
            except Exception as e:
                logger.error(f"Failed to get equity curve: {e}", exc_info=True)
                return JSONResponse({"error": str(e)}, status_code=500)
        
        @self.app.get("/api/strategy-report/{account_id}")
        async def get_strategy_report(account_id: str):
            """获取策略报告"""
            try:
                from ..monitoring.strategy_report import get_strategy_report_generator
                from datetime import timedelta
                
                report_generator = get_strategy_report_generator()
                end_date = datetime.now(timezone.utc)
                
                # 从Equity Curve获取程序执行期间的开始时间（程序启动后的第一次快照时间）
                # 这样统计的是程序执行期间的开仓和交易，而不是所有历史记录
                equity_tracker = report_generator.equity_curve_tracker
                equity_df = equity_tracker.get_equity_curve(account_id)
                
                if not equity_df.empty and 'timestamp' in equity_df.columns:
                    # 获取Equity Curve的最早记录时间（程序启动后的第一次快照）
                    earliest_equity_time = equity_df['timestamp'].min()
                    if isinstance(earliest_equity_time, str):
                        earliest_equity_time = datetime.fromisoformat(earliest_equity_time.replace('Z', '+00:00'))
                    elif hasattr(earliest_equity_time, 'to_pydatetime'):
                        earliest_equity_time = earliest_equity_time.to_pydatetime()
                    
                    # 使用Equity Curve的最早记录时间作为程序执行期间的开始
                    start_date = earliest_equity_time
                    logger.info(f"Using equity curve earliest time as program execution start: {start_date}")
                else:
                    # 如果没有Equity Curve数据，从持仓历史中获取最早的开仓时间
                    report_generator._load_position_history()
                    all_openings = report_generator._position_history.get(account_id, [])
                    
                    if all_openings:
                        earliest_time = None
                        for opening in all_openings:
                            try:
                                opening_time = datetime.fromisoformat(opening['timestamp'].replace('Z', '+00:00'))
                                if earliest_time is None or opening_time < earliest_time:
                                    earliest_time = opening_time
                            except Exception:
                                continue
                        
                        if earliest_time:
                            start_date = earliest_time
                            logger.info(f"Using position history earliest time as program execution start: {start_date}")
                        else:
                            # 如果没有有效时间，使用默认3天
                            start_date = end_date - timedelta(days=3)
                    else:
                        # 如果没有开仓记录，使用默认3天
                        start_date = end_date - timedelta(days=3)
                
                report = await report_generator.generate_report(account_id, start_date, end_date)
                # 清理报告中的NaN和Inf值，避免JSON序列化错误
                from ..common.utils import sanitize_for_json
                cleaned_report = sanitize_for_json(report)
                return JSONResponse(cleaned_report)
            except Exception as e:
                logger.error(f"Failed to get strategy report: {e}", exc_info=True)
                return JSONResponse({"error": str(e)}, status_code=500)
        
        # 性能监控综合 API 端点（前端使用）
        @self.app.get("/api/performance")
        async def get_performance():
            """获取完整的性能监控数据（从文件系统读取各进程生成的性能数据）"""
            try:
                from pathlib import Path
                import json
                
                performance_dir = Path(config.get('monitoring.performance.output_directory', 'data/performance'))
                
                # 统计信息
                stats = {
                    "enabled": True,
                    "total_rounds": 0,
                    "total_measurements": 0,
                    "process_contributions": {}
                }
                
                # 获取所有性能摘要文件
                summary_files = list(performance_dir.glob('performance_summary_*.json'))
                current_round = {}
                
                if summary_files:
                    # 按修改时间排序，获取最新的文件
                    summary_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
                    latest_summary_file = summary_files[0]
                    
                    try:
                        with open(latest_summary_file, 'r', encoding='utf-8') as f:
                            latest_summary = json.load(f)
                        
                        # 从摘要文件中获取统计信息
                        stats['total_rounds'] = len(summary_files)  # 总轮次 = 文件数
                        
                        # 统计总测量次数
                        modules = latest_summary.get('modules', {})
                        total_measurements = sum(m.get('count', 0) for m in modules.values())
                        stats['total_measurements'] = total_measurements
                        
                        # 使用摘要文件中的 modules 结构作为 current_round
                        # 这样前端期望的格式就对了
                        current_round = {
                            "modules": modules
                        }
                    except Exception as e:
                        logger.warning(f"Failed to read latest performance file: {e}")
                
                # 统计各进程的贡献
                detail_files = list(performance_dir.glob('performance_detail_*.json'))
                process_stats = {}
                for detail_file in detail_files:
                    try:
                        with open(detail_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            for module_name in data.keys():
                                process_name = module_name
                                if process_name not in process_stats:
                                    process_stats[process_name] = {"count": 0, "measurements": 0}
                                process_stats[process_name]["count"] += 1
                                if isinstance(data[module_name], list):
                                    process_stats[process_name]["measurements"] += len(data[module_name])
                    except:
                        pass
                
                # 生成简单的分析报告
                analysis = {
                    "message": f"Collected performance data from {len(detail_files)} rounds"
                }
                
                if stats['total_rounds'] > 0:
                    modules_analyzed = ", ".join(list(process_stats.keys())[:5])
                    analysis['message'] = f"Monitored {stats['total_rounds']} rounds, {stats['total_measurements']} measurements across {len(process_stats)} modules"
                
                return JSONResponse({
                    "enabled": True,
                    "statistics": {
                        "total_rounds": stats['total_rounds'],
                        "total_measurements": stats['total_measurements'],
                        "runtime_seconds": time.time() - self.start_time,
                    },
                    "current_round": current_round,
                    "analysis": analysis,
                })
            except Exception as e:
                logger.error(f"Failed to get performance data: {e}", exc_info=True)
                return JSONResponse({"enabled": True, "error": str(e), "statistics": {"total_rounds": 0, "total_measurements": 0}})
        
        # 性能监控 API 端点
        @self.app.get("/api/performance/rounds")
        async def get_performance_rounds():
            """获取所有性能监控轮次列表"""
            stats = self.performance_monitor.get_stats()
            return JSONResponse({
                "enabled": stats['enabled'],
                "total_rounds": stats['total_rounds'],
                "total_measurements": stats['total_measurements'],
                "current_round_id": stats['current_round_id'],
            })
        
        @self.app.get("/api/performance/round/{round_id}/summary")
        async def get_round_summary(round_id: str):
            """获取指定轮次的摘要统计"""
            summary = self.performance_monitor.get_round_summary(round_id)
            if summary:
                return JSONResponse(summary)
            else:
                return JSONResponse({"error": f"Round {round_id} not found"}, status_code=404)
        
        @self.app.post("/api/performance/save")
        async def save_all_performance_data():
            """立即保存所有性能数据"""
            try:
                self.performance_monitor.save_all_rounds()
                return JSONResponse({"status": "success", "message": "Performance data saved"})
            except Exception as e:
                logger.error(f"Failed to save performance data: {e}", exc_info=True)
                return JSONResponse({"error": str(e)}, status_code=500)
        
        @self.app.get("/api/performance/analysis")
        async def get_performance_analysis():
            """生成并返回性能分析报告"""
            try:
                report = self.performance_monitor.generate_analysis_report()
                return JSONResponse(report)
            except Exception as e:
                logger.error(f"Failed to generate performance analysis: {e}", exc_info=True)
                return JSONResponse({"error": str(e)}, status_code=500)
        
        logger.info(f"HTTP API server initialized on port {self.http_port}")
    
    async def _start_http_server(self):
        """启动 HTTP 服务器"""
        if not FASTAPI_AVAILABLE or not self.app:
            return
        
        config_uvicorn = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=self.http_port,
            log_level="info",
            access_log=False,
        )
        self.http_server = uvicorn.Server(config_uvicorn)
        await self.http_server.serve()
    
    async def _stop_http_server(self):
        """停止 HTTP 服务器"""
        if self.http_server:
            self.http_server.should_exit = True


async def main():
    """主函数"""
    process = MonitoringProcess()
    
    # 信号处理
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        asyncio.create_task(process.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        await process.start()
        await process.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Monitoring process error: {e}", exc_info=True)
    finally:
        await process.stop()


if __name__ == "__main__":
    asyncio.run(main())
