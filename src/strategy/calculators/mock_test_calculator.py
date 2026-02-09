"""
Mock测试专用Calculator
用于在mock模式下测试整个系统的执行流程
实现简单的动量策略，便于追踪和审查
"""
from __future__ import annotations
from typing import Dict
from ...strategy.calculator import AlphaCalculatorBase, AlphaDataView

class MockTestCalculator(AlphaCalculatorBase):
    """
    Mock测试专用计算器
    
    策略逻辑：
    - 计算过去N根K线的价格动量
    - 如果价格上涨，做多；如果下跌，做空
    - 权重与动量强度成正比
    """
    
    def __init__(self, name: str = "mock_test_calculator", lookback_bars: int = 10):
        """
        初始化计算器
        
        Args:
            name: 计算器名称
            lookback_bars: 回看K线数量
        """
        self.name = name
        self.lookback_bars = lookback_bars
        self.mutates_inputs = False  # 不修改输入数据
    
    def run(self, view: AlphaDataView) -> Dict[str, float]:
        """
        执行计算，返回权重字典
        
        Args:
            view: 数据视图
            
        Returns:
            Dict[symbol, weight]，权重范围通常在-1到1之间
        """
        import logging
        calc_logger = logging.getLogger('mock_test_calculator')
        
        weights: Dict[str, float] = {}
        
        # 调试：检查view中的数据
        symbols_list = list(view.iter_symbols())
        calc_logger.info(f"MockTestCalculator: Processing {len(symbols_list)} symbols: {symbols_list[:5]}...")
        calc_logger.info(f"MockTestCalculator: bar_data keys: {list(view.bar_data.keys())[:5] if view.bar_data else 'empty'}")
        
        for sym in view.iter_symbols():
            try:
                # 获取K线数据
                bar_df = view.get_bar(sym, tail=self.lookback_bars + 1)
                
                calc_logger.debug(f"MockTestCalculator: {sym} - bar_df shape: {bar_df.shape if not bar_df.empty else 'empty'}")
                
                if bar_df.empty or len(bar_df) < self.lookback_bars + 1:
                    # 数据不足，权重为0
                    calc_logger.warning(f"MockTestCalculator: {sym} - insufficient data: {len(bar_df)} < {self.lookback_bars + 1}")
                    weights[sym] = 0.0
                    continue
                
                # 计算动量：当前价格相对于N根K线前的价格变化
                current_close = float(bar_df['close'].iloc[-1])
                past_close = float(bar_df['close'].iloc[-self.lookback_bars - 1])
                
                if past_close <= 0:
                    weights[sym] = 0.0
                    continue
                
                # 计算收益率
                returns = (current_close - past_close) / past_close
                
                # 计算波动率（用于归一化）
                price_changes = bar_df['close'].pct_change().dropna()
                if len(price_changes) > 0:
                    volatility = price_changes.std()
                    if volatility > 0:
                        # 归一化：收益率 / 波动率
                        normalized_momentum = returns / volatility
                        # 限制权重范围在-1到1之间
                        weight = max(-1.0, min(1.0, normalized_momentum * 2))
                        weights[sym] = weight
                        calc_logger.info(f"MockTestCalculator: {sym} - calculated weight: {weight:.6f} (returns={returns:.6f}, volatility={volatility:.6f})")
                    else:
                        calc_logger.warning(f"MockTestCalculator: {sym} - zero volatility")
                        weights[sym] = 0.0
                else:
                    calc_logger.warning(f"MockTestCalculator: {sym} - no price changes")
                    weights[sym] = 0.0
                    
            except Exception as e:
                # 发生错误时，该交易对权重为0
                weights[sym] = 0.0
                calc_logger.error(f"MockTestCalculator: Error calculating weight for {sym}: {e}", exc_info=True)
        
        non_zero_count = sum(1 for w in weights.values() if abs(w) > 1e-6)
        calc_logger.info(f"MockTestCalculator: Completed - {non_zero_count}/{len(weights)} symbols with non-zero weights")
        return weights

# 导出计算器实例
CALCULATOR_INSTANCE = MockTestCalculator(lookback_bars=10)
