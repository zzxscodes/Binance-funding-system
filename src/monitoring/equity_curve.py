"""
Equity Curve追踪模块
记录策略净值变化，用于生成Equity Curve图表
"""

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timezone
from collections import defaultdict

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import ensure_directory

logger = get_logger('equity_curve')


class EquityCurveTracker:
    """Equity Curve追踪器"""
    
    def __init__(self):
        """初始化Equity Curve追踪器"""
        # 优先使用配置文件中的路径，如果没有则使用默认路径
        equity_curve_dir = config.get('data.equity_curve_directory')
        if equity_curve_dir:
            self.equity_curve_dir = Path(equity_curve_dir)
        else:
            data_dir = Path(config.get('data.data_directory', 'data'))
            self.equity_curve_dir = data_dir / 'equity_curve'
        ensure_directory(str(self.equity_curve_dir))
        
        # 内存缓存：{account_id: List[Dict]}（覆盖模式：单个文件包含所有历史数据）
        self._cache: Dict[str, List[Dict]] = defaultdict(list)
        self._cache_lock = defaultdict(lambda: False)
        
        # 启动时加载单个文件的数据到内存缓存（覆盖模式）
        self._load_equity_curve_to_cache()
    
    def record_snapshot(
        self,
        account_id: str,
        timestamp: datetime,
        total_balance: float,
        available_balance: float,
        unrealized_pnl: float,
        positions: Optional[List[Dict]] = None,
        metadata: Optional[Dict] = None,
        margin_balance: Optional[float] = None  # 保留参数以兼容，但不再使用（净值使用totalWalletBalance）
    ) -> None:
        """
        记录账户快照
        
        Args:
            account_id: 账户ID
            timestamp: 时间戳
            total_balance: 总余额
            available_balance: 可用余额
            unrealized_pnl: 未实现盈亏
            positions: 持仓列表（可选）
            metadata: 额外元数据（可选）
        """
        try:
            snapshot = {
                'timestamp': timestamp.isoformat(),
                'total_balance': float(total_balance),
                'available_balance': float(available_balance),
                'unrealized_pnl': float(unrealized_pnl),
                # 净值计算：使用总余额（包含未实现盈亏），更能反映账户的真实价值
                # 总余额 = 初始余额 + 已实现盈亏 + 未实现盈亏
                # 优点：更能反映账户真实价值，如果持仓有浮盈，总余额会更高，更能看出策略的潜在收益
                # 缺点：受价格波动影响，可能因为持仓浮盈浮亏而波动
                'equity': float(total_balance),  # 使用总余额，包含已实现和未实现盈亏，更能反映账户真实价值
                'positions_count': len(positions) if positions else 0,
                'metadata': metadata or {}
            }
            
            # 如果有持仓信息，计算持仓价值
            if positions:
                position_value = sum(
                    abs(float(p.get('positionAmt', 0))) * float(p.get('entryPrice', 0))
                    for p in positions
                    if abs(float(p.get('positionAmt', 0))) > 1e-8
                )
                snapshot['position_value'] = position_value
            
            # 添加到内存缓存
            if account_id not in self._cache:
                self._cache[account_id] = []
            
            self._cache[account_id].append(snapshot)
            
            # 定期保存到文件（每10条记录保存一次）
            if len(self._cache[account_id]) % 10 == 0:
                self._save_to_file(account_id)
            
        except Exception as e:
            logger.error(f"Failed to record snapshot for {account_id}: {e}", exc_info=True)
    
    def _load_equity_curve_to_cache(self) -> None:
        """启动时加载单个Equity Curve文件的数据到内存缓存（覆盖模式）"""
        try:
            # 查找所有账户的单个文件
            pattern = "*_equity_curve.json"
            for file_path in self.equity_curve_dir.glob(pattern):
                try:
                    # 从文件名提取账户ID
                    account_id = file_path.stem.replace('_equity_curve', '')
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if data:
                        self._cache[account_id] = data
                        logger.debug(f"Loaded {len(data)} equity curve records for {account_id} from {file_path.name}")
                except Exception as e:
                    logger.warning(f"Failed to load equity curve from {file_path.name}: {e}")
        except Exception as e:
            logger.error(f"Failed to load equity curve data: {e}", exc_info=True)
    
    def _save_to_file(self, account_id: str) -> None:
        """保存账户的Equity Curve数据到文件（单个文件覆盖模式）"""
        try:
            if account_id not in self._cache:
                return
            
            # 单个文件：每个账户只有一个文件
            filename = f"{account_id}_equity_curve.json"
            file_path = self.equity_curve_dir / filename
            
            # 读取现有数据（如果存在）
            existing_data = []
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                except Exception as e:
                    logger.warning(f"Failed to read existing equity curve file: {e}")
            
            # 合并数据（去重）：现有数据 + 内存缓存中的新数据
            existing_timestamps = {item['timestamp'] for item in existing_data}
            new_data = [
                item for item in self._cache[account_id]
                if item['timestamp'] not in existing_timestamps
            ]
            
            # 合并并排序
            all_data = existing_data + new_data
            all_data.sort(key=lambda x: x['timestamp'])
            
            # 覆盖保存到单个文件
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Saved equity curve for {account_id}: {len(all_data)} total records ({len(new_data)} new) (single file overwrite mode)")
            
        except Exception as e:
            logger.error(f"Failed to save equity curve for {account_id}: {e}", exc_info=True)
    
    def get_equity_curve(
        self,
        account_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        获取Equity Curve数据
        
        Args:
            account_id: 账户ID
            start_date: 开始日期（可选）
            end_date: 结束日期（可选）
        
        Returns:
            DataFrame包含timestamp, equity, total_balance等列
        """
        try:
            # 从单个文件加载数据
            filename = f"{account_id}_equity_curve.json"
            file_path = self.equity_curve_dir / filename
            
            data = []
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                except Exception as e:
                    logger.warning(f"Failed to read equity curve file {filename}: {e}")
            
            # 合并内存缓存（去重）
            if account_id in self._cache:
                existing_timestamps = {item['timestamp'] for item in data}
                cache_data = [
                    item for item in self._cache[account_id]
                    if item['timestamp'] not in existing_timestamps
                ]
                data.extend(cache_data)
            
            if not data:
                return pd.DataFrame()
            
            # 转换为DataFrame
            df = pd.DataFrame(data)
            
            # 转换时间戳
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp').reset_index(drop=True)
            
            # 时间过滤
            if start_date:
                df = df[df['timestamp'] >= start_date]
            if end_date:
                df = df[df['timestamp'] <= end_date]
            
            # 计算收益率（相对于初始净值）
            if len(df) > 0 and 'equity' in df.columns:
                initial_equity = df['equity'].iloc[0]
                if initial_equity > 0:
                    df['return_pct'] = (df['equity'] / initial_equity - 1.0) * 100.0
                else:
                    df['return_pct'] = 0.0
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get equity curve for {account_id}: {e}", exc_info=True)
            return pd.DataFrame()
    
    def save_all(self) -> None:
        """保存所有账户的Equity Curve数据"""
        try:
            for account_id in self._cache:
                self._save_to_file(account_id)
        except Exception as e:
            logger.error(f"Failed to save all equity curves: {e}", exc_info=True)
    
    def get_summary(self, account_id: str) -> Dict:
        """
        获取Equity Curve摘要信息
        
        Returns:
            Dict包含: initial_equity, current_equity, total_return, max_drawdown等
        """
        try:
            df = self.get_equity_curve(account_id)
            
            if df.empty or 'equity' not in df.columns:
                return {
                    'initial_equity': 0.0,
                    'current_equity': 0.0,
                    'total_return': 0.0,
                    'total_return_pct': 0.0,
                    'max_drawdown': 0.0,
                    'max_drawdown_pct': 0.0,
                    'data_points': 0
                }
            
            equity_series = df['equity']
            initial_equity = float(equity_series.iloc[0])
            current_equity = float(equity_series.iloc[-1])
            total_return = current_equity - initial_equity
            total_return_pct = (total_return / initial_equity * 100.0) if initial_equity > 0 else 0.0
            
            # 计算最大回撤
            cumulative_max = equity_series.expanding().max()
            drawdown = equity_series - cumulative_max
            max_drawdown = float(drawdown.min())
            max_drawdown_pct = (max_drawdown / initial_equity * 100.0) if initial_equity > 0 else 0.0
            
            return {
                'initial_equity': initial_equity,
                'current_equity': current_equity,
                'total_return': total_return,
                'total_return_pct': total_return_pct,
                'max_drawdown': max_drawdown,
                'max_drawdown_pct': max_drawdown_pct,
                'data_points': len(df),
                'start_time': df['timestamp'].iloc[0].isoformat() if 'timestamp' in df.columns else None,
                'end_time': df['timestamp'].iloc[-1].isoformat() if 'timestamp' in df.columns else None
            }
            
        except Exception as e:
            logger.error(f"Failed to get equity curve summary for {account_id}: {e}", exc_info=True)
            return {}


# 全局实例
_equity_curve_tracker: Optional[EquityCurveTracker] = None


def get_equity_curve_tracker() -> EquityCurveTracker:
    """获取Equity Curve追踪器实例"""
    global _equity_curve_tracker
    if _equity_curve_tracker is None:
        _equity_curve_tracker = EquityCurveTracker()
    return _equity_curve_tracker
