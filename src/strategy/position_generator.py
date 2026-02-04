"""
目标持仓生成模块
将策略计算结果保存为目标持仓文件
"""
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Sequence
from datetime import datetime, timezone

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import ensure_directory
from ..common.utils import to_exchange_symbol

logger = get_logger('position_generator')


class PositionGenerator:
    """目标持仓生成器"""
    
    def __init__(self):
        positions_dir = config.get('data.positions_directory', 'data/positions')
        self.positions_dir = Path(positions_dir)
        ensure_directory(str(self.positions_dir))
        
        # 交易信号文件目录
        signals_dir = config.get('data.signals_directory', 'data/signals')
        self.signals_dir = Path(signals_dir)
        ensure_directory(str(self.signals_dir))
    
    def save_target_positions(
        self,
        target_positions: Dict[str, pd.DataFrame],
        timestamp: Optional[datetime] = None
    ) -> Dict[str, str]:
        """
        保存目标持仓到文件
        
        Args:
            target_positions: Dict[account_id, DataFrame]，目标持仓
            timestamp: 时间戳，如果不指定使用当前时间
        
        Returns:
            Dict[account_id, file_path]，保存的文件路径
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        file_paths = {}
        
        try:
            for account_id, df in target_positions.items():
                if df.empty:
                    logger.warning(f"Empty positions for account {account_id}, skipping save")
                    continue
                
                # 生成文件名
                timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
                filename = f"{account_id}_target_positions_{timestamp_str}.json"
                file_path = self.positions_dir / filename
                
                # 转换为JSON格式
                positions_dict = df.to_dict('records')
                
                # 保存为JSON
                output_data = {
                    'account_id': account_id,
                    'timestamp': timestamp.isoformat(),
                    'positions_count': len(positions_dict),
                    'positions': positions_dict
                }
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)
                
                file_paths[account_id] = str(file_path)
                logger.info(
                    f"Saved target positions for {account_id}: {len(positions_dict)} positions -> {file_path}"
                )
            
            # 生成交易信号文件
            signal_file_paths = self._generate_trading_signals(target_positions, timestamp)
            
            return file_paths
            
        except Exception as e:
            logger.error(f"Failed to save target positions: {e}", exc_info=True)
            raise
    
    def _generate_trading_signals(
        self,
        target_positions: Dict[str, pd.DataFrame],
        timestamp: Optional[datetime] = None
    ) -> Dict[str, str]:
        """
        生成交易信号文件
        
        Args:
            target_positions: Dict[account_id, DataFrame]，目标持仓
            timestamp: 时间戳，如果不指定使用当前时间
            
        Returns:
            Dict[account_id, signal_file_path]，交易信号文件路径
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        
        signal_file_paths = {}
        
        try:
            for account_id, df in target_positions.items():
                if df.empty:
                    logger.debug(f"Empty positions for account {account_id}, skipping signal generation")
                    continue
                
                # 生成信号文件名
                timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
                signal_filename = f"{account_id}_trading_signals_{timestamp_str}.json"
                signal_file_path = self.signals_dir / signal_filename
                
                # 构建交易信号列表
                signals = []
                for _, row in df.iterrows():
                    symbol = row.get('symbol', '')
                    target_position = float(row.get('target_position', 0.0))
                    
                    # 确定交易方向
                    if target_position > 0:
                        direction = 'LONG'  # 做多
                    elif target_position < 0:
                        direction = 'SHORT'  # 做空
                    else:
                        direction = 'FLAT'  # 平仓
                    
                    signal = {
                        'symbol': symbol,
                        'target_position': target_position,
                        'direction': direction,
                        'timestamp': timestamp.isoformat()
                    }
                    signals.append(signal)
                
                # 保存交易信号文件
                signal_data = {
                    'account_id': account_id,
                    'timestamp': timestamp.isoformat(),
                    'signals_count': len(signals),
                    'signals': signals
                }
                
                with open(signal_file_path, 'w', encoding='utf-8') as f:
                    json.dump(signal_data, f, indent=2, ensure_ascii=False)
                
                signal_file_paths[account_id] = str(signal_file_path)
                logger.info(
                    f"Generated trading signals for {account_id}: {len(signals)} signals -> {signal_file_path}"
                )
            
            return signal_file_paths
            
        except Exception as e:
            logger.error(f"Failed to generate trading signals: {e}", exc_info=True)
            # 不抛出异常，因为目标持仓已经保存成功
            return signal_file_paths

    def build_target_positions_from_weights(
        self,
        weights: Dict[str, float],
        accounts: Sequence[str],
        timestamp: Optional[datetime] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Convert per-symbol weights (system symbol -> weight) into the downstream
        target-positions format expected by execution layer.

        Notes:
        - The same portfolio is applied to all accounts (current system behavior).
        - Symbols are converted to exchange symbol format (e.g. btc-usdt -> BTCUSDT).
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        # 预先构建行一次，然后为所有账户重用
        rows = []
        for sym, w in (weights or {}).items():
            if w is None:
                continue
            try:
                fw = float(w)
            except Exception:
                continue
            if not (fw == fw):  # NaN
                continue
            if abs(fw) < 1e-12:
                continue
            rows.append(
                {
                    "symbol": to_exchange_symbol(sym),
                    "target_position": fw,
                    "timestamp": timestamp.isoformat(),
                }
            )

        out: Dict[str, pd.DataFrame] = {}
        for account_id in accounts:
            if rows:
                out[account_id] = pd.DataFrame(rows)
            else:
                out[account_id] = pd.DataFrame(columns=["symbol", "target_position", "timestamp"])
        return out
    
    def load_target_positions(self, file_path: str) -> Dict:
        """
        从文件加载目标持仓
        
        Args:
            file_path: 文件路径
        
        Returns:
            目标持仓数据字典
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to load target positions from {file_path}: {e}", exc_info=True)
            raise


# 全局实例
_position_generator: Optional[PositionGenerator] = None


def get_position_generator() -> PositionGenerator:
    """获取目标持仓生成器实例"""
    global _position_generator
    if _position_generator is None:
        _position_generator = PositionGenerator()
    return _position_generator
