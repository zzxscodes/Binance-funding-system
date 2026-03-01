#!/usr/bin/env python3
"""
使用 Binance 官方 aggTrades 直接聚合并对比 Binance 官方 klines。
用于验证“聚合算法本身”是否与交易所口径一致。
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import math
import os
import sys

import aiohttp

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.common.config import config  # noqa: E402
from src.common.utils import format_symbol  # noqa: E402


def _floor_5m(dt: datetime) -> datetime:
    dt = dt.astimezone(timezone.utc)
    return dt.replace(second=0, microsecond=0) - timedelta(minutes=dt.minute % 5)


async def _get_json(session: aiohttp.ClientSession, url: str, params: dict):
    async with session.get(url, params=params, timeout=20) as resp:
        resp.raise_for_status()
        return await resp.json()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--rtol", type=float, default=1e-6)
    args = parser.parse_args()

    symbol = format_symbol(args.symbol)
    api_base = config.get_binance_api_base()
    end_open = _floor_5m(datetime.now(timezone.utc)) - timedelta(minutes=5)
    start_ms = int(end_open.timestamp() * 1000)
    end_ms = start_ms + 5 * 60 * 1000 - 1

    async with aiohttp.ClientSession() as session:
        kl = await _get_json(
            session,
            f"{api_base}/fapi/v1/klines",
            {"symbol": symbol, "interval": "5m", "startTime": start_ms, "endTime": start_ms + 1, "limit": 1},
        )
        if not kl:
            raise RuntimeError("empty kline")
        k = kl[0]

        # aggTrades 分页拉取
        trades = []
        from_id = None
        while True:
            params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
            if from_id is not None:
                params["fromId"] = from_id
            batch = await _get_json(session, f"{api_base}/fapi/v1/aggTrades", params)
            if not batch:
                break
            trades.extend(batch)
            if len(batch) < 1000:
                break
            from_id = int(batch[-1]["a"]) + 1

    if not trades:
        raise RuntimeError("empty aggTrades in selected window")

    prices = [float(t["p"]) for t in trades]
    qtys = [float(t["q"]) for t in trades]
    quote_qtys = [float(t["p"]) * float(t["q"]) for t in trades]

    agg = {
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "volume": sum(qtys),
        "quote_volume": sum(quote_qtys),
        "trade_count": float(len(trades)),
    }
    ref = {
        "open": float(k[1]),
        "high": float(k[2]),
        "low": float(k[3]),
        "close": float(k[4]),
        "volume": float(k[5]),
        "quote_volume": float(k[7]),
        "trade_count": float(k[8]),
    }

    print(f"symbol={symbol} window_open={end_open.isoformat()} trades={len(trades)}")
    ok_all = True
    for key in ["open", "high", "low", "close", "volume", "quote_volume", "trade_count"]:
        a = agg[key]
        b = ref[key]
        diff = abs(a - b)
        tol = args.rtol * max(1.0, abs(b))
        ok = diff <= tol or (math.isclose(a, b, rel_tol=args.rtol, abs_tol=tol))
        ok_all = ok_all and ok
        print(f"{key:12s} agg={a:.10f} kline={b:.10f} diff={diff:.10f} tol={tol:.10f} ok={ok}")

    if not ok_all:
        raise SystemExit(2)


if __name__ == "__main__":
    asyncio.run(main())

