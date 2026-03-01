#!/usr/bin/env python3
"""
对比本地聚合K线与Binance官方K线在重叠字段上的一致性。
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import os
import sys

import aiohttp
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.common.config import config  # noqa: E402
from src.data.storage import get_data_storage  # noqa: E402
from src.common.utils import format_symbol  # noqa: E402


def _floor_5m(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc)
    return dt.replace(second=0, microsecond=0) - timedelta(minutes=dt.minute % 5)


async def _fetch_binance_kline(symbol: str, open_ms: int):
    api_base = config.get_binance_api_base()
    url = f"{api_base}/fapi/v1/klines"
    params = {
        "symbol": format_symbol(symbol),
        "interval": "5m",
        "startTime": open_ms,
        "endTime": open_ms + 1,
        "limit": 1,
    }
    headers = {"User-Agent": "kline-compare/1.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        last_err = None
        for i in range(5):
            try:
                async with session.get(url, params=params, timeout=15) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(1.5 * (i + 1))
                        continue
                    resp.raise_for_status()
                    data = await resp.json()
                    break
            except Exception as e:
                last_err = e
                await asyncio.sleep(0.8 * (i + 1))
        else:
            raise RuntimeError(f"Failed to fetch Binance kline after retries: {last_err}")
    if not data:
        raise RuntimeError("Binance returned empty kline list")
    return data[0]


def _to_float(v) -> float:
    if isinstance(v, (int, float)):
        return float(v)
    return float(str(v))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--rtol", type=float, default=1e-6)
    parser.add_argument("--allow-zero-bar", action="store_true")
    args = parser.parse_args()

    storage = get_data_storage()
    # 使用当前5分钟边界作为上界，匹配storage中 close_time <= end_time 的过滤逻辑
    end_time = _floor_5m(datetime.now(timezone.utc))
    start_time = end_time - timedelta(days=2)
    symbol = format_symbol(args.symbol)

    local_df = storage.load_klines(symbol, start_time, end_time)
    if local_df.empty:
        raise RuntimeError(f"No local klines for {symbol} in [{start_time}, {end_time}]")

    local_df = local_df.sort_values("open_time")
    if not args.allow_zero_bar:
        local_df = local_df[(local_df["trade_count"] > 0) & (local_df["volume"] > 0)]
        if local_df.empty:
            raise RuntimeError(f"Only zero/no-trade local klines for {symbol} in [{start_time}, {end_time}]")
    row = local_df.iloc[-1]
    open_time = pd.to_datetime(row["open_time"], utc=True)
    open_ms = int(open_time.timestamp() * 1000)

    b = await _fetch_binance_kline(symbol, open_ms)

    # Binance字段索引:
    # [0 open_time,1 open,2 high,3 low,4 close,5 volume,6 close_time,7 quote_volume,8 trade_count,...]
    checks = {
        "open": (_to_float(row["open"]), _to_float(b[1])),
        "high": (_to_float(row["high"]), _to_float(b[2])),
        "low": (_to_float(row["low"]), _to_float(b[3])),
        "close": (_to_float(row["close"]), _to_float(b[4])),
        "volume": (_to_float(row["volume"]), _to_float(b[5])),
        "quote_volume": (_to_float(row["quote_volume"]), _to_float(b[7])),
        "trade_count": (_to_float(row["trade_count"]), _to_float(b[8])),
    }

    print(f"symbol={symbol} open_time={open_time.isoformat()}")
    all_ok = True
    for k, (local_v, remote_v) in checks.items():
        diff = abs(local_v - remote_v)
        tol = args.rtol * max(1.0, abs(remote_v))
        ok = diff <= tol
        all_ok = all_ok and ok
        print(f"{k:12s} local={local_v:.10f} binance={remote_v:.10f} diff={diff:.10f} tol={tol:.10f} ok={ok}")

    if not all_ok:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())

