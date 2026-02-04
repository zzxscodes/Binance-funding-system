"""
示例：优先使用本地真实历史K线数据进行回测；若未找到数据则回退到模拟数据生成。

用法：将真实历史K线文件（parquet或csv，包含open_time/open/high/low/close/volume）
放在 `data/klines/{SYMBOL}/` 下，按日期分片存放（或使用数据存储工具导入）。

脚本会自动检测是否有真实数据；没有则生成模拟数据并保存到存储中以便后续使用。
"""

from datetime import datetime, timezone
from src.backtest import (
    BacktestAPI,
    create_backtest_config,
    MockDataManager,
    PortfolioState,
    DataReplayEngine,
)
from src.data.storage import get_data_storage
import logging

logger = logging.getLogger('strategy_backtest_example')
logging.basicConfig(level=logging.INFO)


SYMBOL = "BTCUSDT"
START = datetime(2024, 1, 1, tzinfo=timezone.utc)
END = datetime(2024, 1, 31, tzinfo=timezone.utc)


def ensure_data_available(symbol: str, start: datetime, end: datetime) -> bool:
    """检查存储中是否有指定symbol在日期范围内的历史K线数据"""
    # 使用 DataReplayEngine 尝试加载数据
    engine = DataReplayEngine(symbols=[symbol], start_date=start, end_date=end, interval="5m")
    if engine.has_data():
        logger.info(f"Found real data for {symbol} in storage")
        return True

    logger.info(f"No real data found for {symbol} between {start.date()} and {end.date()}")
    return False


def import_csv_to_storage(sample_csv_path, symbol: str):
    """辅助：如果你有CSV文件，可以使用 DataStorage.save_klines 将其导入到存储。

    CSV 必须包含列：open_time, open, high, low, close, volume
    open_time 列应为可解析的时间字符串或时间戳。
    """
    storage = get_data_storage()
    import pandas as pd

    df = pd.read_csv(sample_csv_path, parse_dates=['open_time'])
    # 保存到存储（DataStorage 会按日期分片）
    storage.save_klines(symbol=symbol, df=df)
    logger.info(f"Imported CSV to storage for {symbol}: {sample_csv_path}")


def main():
    # 1) 检查真实数据
    if not ensure_data_available(SYMBOL, START, END):
        # 2) 若无真实数据，生成并保存模拟数据（一次性）
        logger.info("Generating mock data and saving to storage...")
        manager = MockDataManager()
        manager.generate_and_save_mock_data(
            symbols=[SYMBOL],
            start_date=START,
            end_date=END,
            initial_prices={SYMBOL: 42000},
            seed=42
        )

    # 3) 创建回测配置
    config = create_backtest_config(
        name="momentum_demo",
        start_date=START,
        end_date=END,
        initial_balance=10000.0,
        symbols=[SYMBOL]
    )

    # 4) 定义策略
    def momentum_strategy(portfolio: PortfolioState, klines: dict) -> dict:
        weights = {}
        for symbol, kline in klines.items():
            weights[symbol] = 1.0 if kline.close > kline.open else 0.0
        return weights

    # 5) 运行回测（BacktestAPI 会创建并使用 DataReplayEngine）
    result = BacktestAPI.run_backtest(config, momentum_strategy)

    # 6) 输出结果
    print(f"总交易数: {result.total_trades}")
    print(f"总收益率: {result.total_return:.2f}%")
    print(f"夏普比率: {result.sharpe_ratio:.2f}")


if __name__ == "__main__":
    main()