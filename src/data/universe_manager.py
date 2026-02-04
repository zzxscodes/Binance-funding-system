"""
Universe管理模块
每天23:55（北京时间）更新可交易资产列表
"""
import os
import csv
import asyncio
import aiohttp
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Set, Optional
import time

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import beijing_now, beijing_to_utc, parse_time_str, format_symbol
from ..common.network_utils import safe_http_request, log_network_error

logger = get_logger('universe_manager')


class UniverseManager:
    """Universe管理器"""
    
    def __init__(self):
        # 根据 execution.mode 自动选择正确的地址（数据层支持testnet回退到live）
        self.api_base = config.get_binance_api_base_for_data_layer()
        self.universe_dir = Path(config.get('data.universe_directory', 'data/universe'))
        self.universe_dir.mkdir(parents=True, exist_ok=True)
        self.current_universe: Set[str] = set()
        self.running = False
        self.update_task: Optional[asyncio.Task] = None
    
    def _get_universe_file_path(self, date: datetime, version: str = 'v1') -> Path:
        """
        获取指定日期的Universe文件路径
        例如: 2026-01-19 23:55获取，生成 2026-01-20/v1/universe.csv
        
        Args:
            date: 日期
            version: 版本号，如 'v1', 'v2' 等
        """
        # 如果是晚上23:55之后获取，文件应该是第二天的
        beijing_time = beijing_now()
        if beijing_time.hour >= 23:
            date = beijing_time + timedelta(days=1)
        
        date_str = date.strftime('%Y-%m-%d')
        date_dir = self.universe_dir / date_str / version
        date_dir.mkdir(parents=True, exist_ok=True)
        return date_dir / 'universe.csv'
    
    async def fetch_universe_from_binance(self) -> List[Dict]:
        """
        从Binance获取所有USDT本位永续合约
        返回合约列表，每个合约包含symbol等信息
        """
        url = f"{self.api_base}/fapi/v1/exchangeInfo"
        
        try:
            async with aiohttp.ClientSession() as session:
                data = await safe_http_request(
                    session,
                    'GET',
                    url,
                    max_retries=3,
                    timeout=30.0,
                    return_json=True
                )
                symbols = data.get('symbols', [])
                
                # 筛选永续合约且报价资产为USDT
                perpetual_usdt = []
                for symbol_info in symbols:
                    contract_type = symbol_info.get('contractType', '')
                    quote_asset = symbol_info.get('quoteAsset', '')
                    status = symbol_info.get('status', '')
                    
                    # 只选择永续合约、USDT报价、且状态为TRADING的
                    if (contract_type == 'PERPETUAL' and 
                        quote_asset == 'USDT' and 
                        status == 'TRADING'):
                        perpetual_usdt.append(symbol_info)
                
                logger.info(f"成功获取 {len(perpetual_usdt)} 个USDT永续合约")
                return perpetual_usdt
                    
        except Exception as e:
            log_network_error(
                "获取Universe",
                e,
                context={"url": url, "api_base": self.api_base}
            )
            raise
    
    def save_universe(self, symbols_info: List[Dict], file_path: Path):
        """
        保存Universe到CSV文件
        """
        if not symbols_info:
            logger.warning("No symbols to save")
            return
        
        # 确定CSV字段
        fieldnames = ['symbol', 'status', 'baseAsset', 'quoteAsset', 'contractType',
                     'pricePrecision', 'quantityPrecision', 'tickSize', 'stepSize']
        
        # 准备数据
        rows = []
        symbol_set = set()
        
        for symbol_info in symbols_info:
            symbol = format_symbol(symbol_info.get('symbol', ''))
            if symbol in symbol_set:
                continue
            
            symbol_set.add(symbol)
            
            # 获取价格精度和数量精度
            price_precision = 8
            qty_precision = 8
            tick_size = 0.01
            step_size = 0.01
            
            for filter_item in symbol_info.get('filters', []):
                filter_type = filter_item.get('filterType', '')
                if filter_type == 'PRICE_FILTER':
                    tick_size = float(filter_item.get('tickSize', '0.01'))
                    price_precision = len(str(tick_size).rstrip('0').split('.')[-1])
                elif filter_type == 'LOT_SIZE':
                    step_size = float(filter_item.get('stepSize', '0.01'))
                    qty_precision = len(str(step_size).rstrip('0').split('.')[-1])
            
            rows.append({
                'symbol': symbol,
                'status': symbol_info.get('status', ''),
                'baseAsset': symbol_info.get('baseAsset', ''),
                'quoteAsset': symbol_info.get('quoteAsset', ''),
                'contractType': symbol_info.get('contractType', ''),
                'pricePrecision': price_precision,
                'quantityPrecision': qty_precision,
                'tickSize': tick_size,
                'stepSize': step_size,
            })
        
        # 写入CSV
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        logger.info(f"Saved {len(rows)} symbols to {file_path}")
        self.current_universe = symbol_set
    
    def load_universe(self, date: Optional[datetime] = None, version: str = 'v1') -> Set[str]:
        """
        加载指定日期的Universe
        如果不指定日期，加载最新的
        
        Args:
            date: 日期，如果不指定，返回最新的
            version: 版本号，如 'v1', 'v2' 等，默认为 'v1'
        """
        if date is None:
            # 查找最新的Universe文件（按日期和版本）
            universe_dirs = sorted(self.universe_dir.iterdir(), reverse=True)
            for date_dir in universe_dirs:
                if date_dir.is_dir():
                    # 检查指定版本
                    version_dir = date_dir / version
                    universe_file = version_dir / 'universe.csv'
                    if universe_file.exists():
                        return self._load_universe_from_file(universe_file)
                    # 如果没有找到指定版本，尝试查找任何版本（向后兼容）
                    for v_dir in date_dir.iterdir():
                        if v_dir.is_dir():
                            universe_file = v_dir / 'universe.csv'
                            if universe_file.exists():
                                logger.warning(f"Using universe from {v_dir.name} instead of {version}")
                                return self._load_universe_from_file(universe_file)
            return set()
        else:
            universe_file = self._get_universe_file_path(date, version)
            if universe_file.exists():
                return self._load_universe_from_file(universe_file)
            return set()
    
    def _load_universe_from_file(self, file_path: Path) -> Set[str]:
        """从CSV文件加载Universe"""
        symbols = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = format_symbol(row.get('symbol', ''))
                    if symbol:
                        symbols.add(symbol)
            
            self.current_universe = symbols
            logger.info(f"Loaded {len(symbols)} symbols from {file_path}")
            return symbols
        except Exception as e:
            logger.error(f"Failed to load universe from {file_path}: {e}", exc_info=True)
            return set()
    
    async def update_universe(self, version: str = 'v1'):
        """
        更新Universe（主动调用）
        
        Args:
            version: 版本号，如 'v1', 'v2' 等，默认为 'v1'
        """
        try:
            logger.info(f"Starting universe update (version: {version})...")
            symbols_info = await self.fetch_universe_from_binance()
            file_path = self._get_universe_file_path(beijing_now(), version)
            self.save_universe(symbols_info, file_path)
            logger.info(f"Universe update completed successfully (version: {version})")
            return list(self.current_universe)
        except Exception as e:
            logger.error(f"Universe update failed: {e}", exc_info=True)
            raise
    
    async def _scheduled_update(self):
        """定时更新Universe"""
        update_time_str = config.get('data.universe_update_time', '23:55')
        
        while self.running:
            try:
                now_beijing = beijing_now()
                next_update = parse_time_str(update_time_str, 'Asia/Shanghai')
                
                # 如果今天的时间已过，计算下次更新时间
                if next_update <= now_beijing:
                    next_update = next_update + timedelta(days=1)
                
                # 等待到更新时间
                wait_seconds = (next_update - now_beijing).total_seconds()
                logger.info(f"Next universe update scheduled at {next_update} (in {wait_seconds/3600:.2f} hours)")
                
                # 等待
                await asyncio.sleep(min(wait_seconds, 86400))  # 最多等待24小时
                
                if not self.running:
                    break
                
                # 执行更新
                await self.update_universe()
                
            except Exception as e:
                logger.error(f"Error in scheduled universe update: {e}", exc_info=True)
                # 出错后等待1小时再试
                await asyncio.sleep(3600)
    
    async def start(self):
        """启动定时更新"""
        self.running = True
        # 立即执行一次更新
        try:
            await self.update_universe()
        except Exception as e:
            logger.error(f"Initial universe update failed: {e}")
        
        # 启动定时任务
        self.update_task = asyncio.create_task(self._scheduled_update())
        logger.info("Universe manager started")
    
    async def stop(self):
        """停止定时更新"""
        self.running = False
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                # 任务被取消属于正常流程，记录debug方便定位关闭顺序问题
                logger.debug("Universe manager update task cancelled during stop()")
        logger.info("Universe manager stopped")


# 全局实例
_universe_manager: Optional[UniverseManager] = None


def get_universe_manager() -> UniverseManager:
    """获取Universe管理器实例"""
    global _universe_manager
    if _universe_manager is None:
        _universe_manager = UniverseManager()
    return _universe_manager
