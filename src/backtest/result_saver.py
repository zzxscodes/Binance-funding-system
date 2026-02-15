"""
回测结果保存器

自动将回测结果保存到data目录
"""

from __future__ import annotations

from typing import Optional
from pathlib import Path
from datetime import datetime

from ..common.logger import get_logger
from .models import BacktestResult
from .config import BacktestConfigManager
from .analysis import BacktestAnalyzer
from .utils import (
    get_backtest_result_dir,
    get_save_json,
    get_save_csv,
    get_save_report,
    get_save_config,
)

logger = get_logger('backtest.result_saver')


class BacktestResultSaver:
    """回测结果保存器"""
    
    @staticmethod
    def save_result(result: BacktestResult, 
                   output_dir: Optional[Path] = None,
                   save_json: bool = True,
                   save_csv: bool = True,
                   save_report: bool = True) -> Path:
        """
        保存回测结果到data目录
        
        Args:
            result: 回测结果
            output_dir: 输出目录，如果为None则使用配置的默认目录
            save_json: 是否保存JSON格式
            save_csv: 是否保存CSV格式
            save_report: 是否保存文本报告
        
        Returns:
            输出目录路径
        """
        # 获取输出目录
        if output_dir is None:
            output_dir = get_backtest_result_dir()
        else:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建以回测名称命名的子目录
        result_name = result.config.name
        result_dir = output_dir / result_name
        result_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"保存回测结果到: {result_dir}")
        
        # 保存JSON格式
        if save_json:
            json_path = result_dir / "result.json"
            BacktestAnalyzer.export_json(result, json_path)
            logger.info(f"JSON结果已保存: {json_path}")
        
        # 保存CSV格式
        if save_csv:
            trades_path = result_dir / "trades.csv"
            portfolio_path = result_dir / "portfolio.csv"
            BacktestAnalyzer.export_trades_csv(result, trades_path)
            BacktestAnalyzer.export_portfolio_history_csv(result, portfolio_path)
            logger.info(f"CSV结果已保存: {trades_path}, {portfolio_path}")
        
        # 保存文本报告
        if save_report:
            report_path = result_dir / "report.txt"
            report = BacktestAnalyzer.generate_report(result)
            report_path.write_text(report, encoding='utf-8')
            logger.info(f"文本报告已保存: {report_path}")
        
        # 保存配置信息（如果启用）
        if get_save_config():
            config_path = result_dir / "config.json"
            config_dict = {
                'name': result.config.name,
                'start_date': result.config.start_date.isoformat(),
                'end_date': result.config.end_date.isoformat(),
                'initial_balance': result.config.initial_balance,
                'symbols': result.config.symbols,
                'leverage': result.config.leverage,
                'maker_fee': result.config.maker_fee,
                'taker_fee': result.config.taker_fee,
                'interval': result.config.interval,
                'capital_allocation': result.config.capital_allocation,
                'long_count': result.config.long_count,
                'short_count': result.config.short_count,
            }
            config_path.write_text(json.dumps(config_dict, indent=2, ensure_ascii=False), encoding='utf-8')
            logger.info(f"配置信息已保存: {config_path}")
        
        return result_dir
    
    @staticmethod
    def save_result_auto(result: BacktestResult) -> Path:
        """
        自动保存回测结果（使用配置的默认设置）
        
        Args:
            result: 回测结果
        
        Returns:
            输出目录路径
        """
        # 从配置读取保存选项
        save_json = get_save_json()
        save_csv = get_save_csv()
        save_report = get_save_report()
        
        return BacktestResultSaver.save_result(
            result=result,
            output_dir=None,
            save_json=save_json,
            save_csv=save_csv,
            save_report=save_report
        )
