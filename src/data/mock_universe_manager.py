"""
模拟Universe管理器
在dry-run模式下使用，提供模拟的交易对列表，不从Binance API获取
"""
import csv
from pathlib import Path
from typing import List, Dict, Set, Optional
from datetime import datetime

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol

logger = get_logger('mock_universe_manager')


class MockUniverseManager:
    """
    模拟Universe管理器
    在dry-run模式下使用，提供模拟的交易对列表
    """
    
    def __init__(self):
        self.universe_dir = Path(config.get('data.universe_directory', 'data/universe'))
        self.universe_dir.mkdir(parents=True, exist_ok=True)
        
        # 模拟的交易对列表（默认包含常见的交易对）
        self.mock_symbols = [
            'BTCUSDT',
            'ETHUSDT',
            'BNBUSDT',
            'SOLUSDT',
            'ADAUSDT',
            'XRPUSDT',
            'DOGEUSDT',
            'DOTUSDT',
            'MATICUSDT',
            'AVAXUSDT',
        ]
        
        self.current_universe: Set[str] = set(self.mock_symbols)
        self.running = False
    
    def _get_universe_file_path(self, date: datetime) -> Path:
        """获取指定日期的Universe文件路径"""
        date_str = date.strftime('%Y-%m-%d')
        date_dir = self.universe_dir / date_str
        date_dir.mkdir(parents=True, exist_ok=True)
        return date_dir / 'universe.csv'
    
    def load_universe(self) -> Set[str]:
        """
        加载Universe（优先从文件加载，如果文件不存在则使用模拟数据）
        
        Returns:
            交易对集合
        """
        try:
            # 尝试从最新的文件加载
            if self.universe_dir.exists():
                date_dirs = sorted([d for d in self.universe_dir.iterdir() if d.is_dir()], reverse=True)
                for date_dir in date_dirs:
                    universe_file = date_dir / 'universe.csv'
                    if universe_file.exists():
                        symbols = set()
                        with open(universe_file, 'r', encoding='utf-8') as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                symbol = format_symbol(row.get('symbol', ''))
                                if symbol:
                                    symbols.add(symbol)
                        
                        if symbols:
                            self.current_universe = symbols
                            logger.info(f"Loaded universe from file: {len(symbols)} symbols")
                            return symbols
            
            # 如果文件不存在，使用模拟数据
            logger.info(f"Using mock universe: {len(self.mock_symbols)} symbols")
            self.current_universe = set(self.mock_symbols)
            return self.current_universe
            
        except Exception as e:
            logger.warning(f"Failed to load universe, using mock data: {e}")
            self.current_universe = set(self.mock_symbols)
            return self.current_universe
    
    async def update_universe(self):
        """
        更新Universe（在dry-run模式下，只保存模拟数据到文件）
        """
        try:
            # 使用模拟的交易对列表
            symbols = self.mock_symbols
            
            # 保存到文件
            date = datetime.now()
            universe_file = self._get_universe_file_path(date)
            
            with open(universe_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['symbol'])
                for symbol in symbols:
                    writer.writerow([symbol])
            
            self.current_universe = set(symbols)
            logger.info(f"Mock universe saved: {len(symbols)} symbols to {universe_file}")
            
        except Exception as e:
            logger.error(f"Failed to update mock universe: {e}", exc_info=True)
    
    async def start(self):
        """启动Universe管理器（dry-run模式下无需定时更新）"""
        self.running = True
        logger.info("Mock universe manager started (no scheduled updates in dry-run mode)")
    
    async def stop(self):
        """停止Universe管理器"""
        self.running = False
        logger.info("Mock universe manager stopped")
