"""
日志管理模块
"""
import os
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from typing import Optional
from datetime import datetime
import sys

from .config import config


class Logger:
    """日志管理类"""
    
    _loggers: dict[str, logging.Logger] = {}
    
    @classmethod
    def get_logger(cls, name: str = 'binance_quant') -> logging.Logger:
        """获取或创建logger"""
        if name not in cls._loggers:
            cls._loggers[name] = cls._create_logger(name)
        return cls._loggers[name]
    
    @classmethod
    def _create_logger(cls, name: str) -> logging.Logger:
        """创建logger实例"""
        logger = logging.getLogger(name)
        
        # 避免重复添加handler
        if logger.handlers:
            return logger
        
        # 设置日志级别
        log_level = config.get('logging.level', 'INFO')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # 创建formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 控制台handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 文件handler（按天轮转）
        log_dir = Path(config.get('logging.log_directory', 'logs'))
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = TimedRotatingFileHandler(
            log_file,
            when='midnight',
            interval=1,
            backupCount=config.get('logging.backup_count', 7),
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # 错误日志单独文件
        error_log_file = log_dir / f"{name}_error_{datetime.now().strftime('%Y%m%d')}.log"
        error_handler = TimedRotatingFileHandler(
            error_log_file,
            when='midnight',
            interval=1,
            backupCount=config.get('logging.backup_count', 7),
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)
        
        return logger


def get_logger(name: str = 'binance_quant') -> logging.Logger:
    """获取logger的便捷函数"""
    return Logger.get_logger(name)
