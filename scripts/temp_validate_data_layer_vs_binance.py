#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.kline_aggregator import KlineAggregator  # noqa: E402


INTERVAL_MS = 5 * 60 * 1000


@dataclass
class CompareResult:
    symbol: str
    window_start_ms: int
    passed: bool
    reason: str
    details: Dict[str, Any]


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return float("nan")


def _is_close(a: float, b: float, rel_tol: float, abs_tol: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    return math.isclose(a, b, rel_tol=rel_tol, abs_tol=abs_tol)


async def _fetch_exchange_symbols(
    session: aiohttp.ClientSession, api_base: str
) -> List[str]:
    url = f"{api_base}/fapi/v1/exchangeInfo"
    async with session.get(url, timeout=20) as resp:
        resp.raise_for_status()
        payload = await resp.json()
    symbols: List[str] = []
    for item in payload.get("symbols", []):
        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("contractType") == "PERPETUAL"
        ):
            symbols.append(str(item["symbol"]).upper())
    symbols.sort()
    return symbols


async def _fetch_official_kline(
    session: aiohttp.ClientSession, api_base: str, symbol: str, open_ms: int
) -> Optional[List[Any]]:
    url = f"{api_base}/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": "5m",
        "startTime": open_ms,
        "endTime": open_ms + INTERVAL_MS,
        "limit": 1,
    }
    async with session.get(url, params=params, timeout=20) as resp:
        resp.raise_for_status()
        rows = await resp.json()
    if not rows:
        return None
    return rows[0]


async def _fetch_window_aggtrades(
    session: aiohttp.ClientSession,
    api_base: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    max_pages: int,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Return (agg_trades, truncated).
    """
    url = f"{api_base}/fapi/v1/aggTrades"
    all_rows: List[Dict[str, Any]] = []
    seen_ids = set()

    params: Dict[str, Any] = {
        "symbol": symbol,
        "startTime": start_ms,
        "endTime": end_ms - 1,
        "limit": 1000,
    }
    pages = 0
    truncated = False
    last_id: Optional[int] = None

    while True:
        pages += 1
        async with session.get(url, params=params, timeout=20) as resp:
            resp.raise_for_status()
            rows = await resp.json()

        if not rows:
            break

        for row in rows:
            row_id = int(row["a"])
            row_t = int(row["T"])
            if row_t < start_ms or row_t >= end_ms:
                continue
            if row_id in seen_ids:
                continue
            seen_ids.add(row_id)
            all_rows.append(row)

        if len(rows) < 1000:
            break
        if pages >= max_pages:
            truncated = True
            break

        next_id = int(rows[-1]["a"]) + 1
        if last_id is not None and next_id <= last_id:
            truncated = True
            break
        last_id = next_id
        params = {
            "symbol": symbol,
            "fromId": next_id,
            "endTime": end_ms - 1,
            "limit": 1000,
        }

    all_rows.sort(key=lambda x: (int(x["T"]), int(x["a"])))
    return all_rows, truncated


def _to_aggregator_trade(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "price": _to_float(row["p"]),
        "qty": _to_float(row["q"]),
        "quoteQty": _to_float(row["p"]) * _to_float(row["q"]),
        "ts_ms": int(row["T"]),
        "isBuyerMaker": bool(row["m"]),
        "f": int(row["f"]),
        "l": int(row["l"]),
    }


def _compare_fields(
    local_kline: Dict[str, Any],
    official_kline: List[Any],
    rel_tol: float,
    abs_tol: float,
) -> Tuple[bool, Dict[str, Any]]:
    fields = {
        "open": (_to_float(local_kline.get("open")), _to_float(official_kline[1])),
        "high": (_to_float(local_kline.get("high")), _to_float(official_kline[2])),
        "low": (_to_float(local_kline.get("low")), _to_float(official_kline[3])),
        "close": (_to_float(local_kline.get("close")), _to_float(official_kline[4])),
        "volume": (_to_float(local_kline.get("volume")), _to_float(official_kline[5])),
        "quote_volume": (
            _to_float(local_kline.get("quote_volume")),
            _to_float(official_kline[7]),
        ),
    }

    details: Dict[str, Any] = {}
    passed = True
    for name, (lv, rv) in fields.items():
        ok = _is_close(lv, rv, rel_tol=rel_tol, abs_tol=abs_tol)
        details[name] = {
            "local": lv,
            "official": rv,
            "ok": ok,
            "diff": lv - rv,
        }
        if not ok:
            passed = False

    local_count = int(local_kline.get("trade_count", 0))
    official_count = int(official_kline[8])
    count_ok = local_count == official_count
    details["trade_count"] = {
        "local": local_count,
        "official": official_count,
        "ok": count_ok,
        "diff": local_count - official_count,
    }
    if not count_ok:
        passed = False

    return passed, details


async def _validate_one(
    session: aiohttp.ClientSession,
    api_base: str,
    symbol: str,
    window_start_ms: int,
    rel_tol: float,
    abs_tol: float,
    max_pages: int,
) -> CompareResult:
    official = await _fetch_official_kline(session, api_base, symbol, window_start_ms)
    if official is None:
        return CompareResult(symbol, window_start_ms, False, "official_kline_missing", {})

    agg_rows, truncated = await _fetch_window_aggtrades(
        session,
        api_base,
        symbol,
        window_start_ms,
        window_start_ms + INTERVAL_MS,
        max_pages=max_pages,
    )
    if truncated:
        return CompareResult(symbol, window_start_ms, False, "aggtrades_truncated", {})

    official_trade_count = int(official[8])
    if official_trade_count == 0:
        return CompareResult(symbol, window_start_ms, True, "skip_zero_trade_window", {})

    if not agg_rows:
        return CompareResult(symbol, window_start_ms, False, "aggtrades_empty", {})

    trades = [_to_aggregator_trade(r) for r in agg_rows]
    agg = KlineAggregator(interval_minutes=5)
    # Use the same ingest path as data_layer runtime (add_trade -> pending -> _aggregate_window).
    for tr in trades:
        await agg.add_trade(symbol, tr)
    await agg._aggregate_window(symbol, window_start_ms)

    local_kline: Optional[Dict[str, Any]] = agg.get_latest_kline(symbol)
    if local_kline and int(local_kline.get("span_begin_datetime", -1)) != window_start_ms:
        local_kline = None

    if not local_kline:
        return CompareResult(symbol, window_start_ms, False, "local_aggregate_none", {})

    ok, details = _compare_fields(local_kline, official, rel_tol=rel_tol, abs_tol=abs_tol)
    reason = "ok" if ok else "field_mismatch"
    return CompareResult(symbol, window_start_ms, ok, reason, details)


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Temporary validator: compare data_layer kline aggregation with Binance official klines"
    )
    parser.add_argument("--api-base", default="https://fapi.binance.com")
    parser.add_argument("--symbols", type=int, default=60, help="Number of symbols to validate")
    parser.add_argument("--window-count", type=int, default=2, help="How many historical windows per symbol")
    parser.add_argument(
        "--skip-recent-windows",
        type=int,
        default=2,
        help="Skip newest N closed windows to avoid in-flight updates",
    )
    parser.add_argument("--max-pages", type=int, default=30, help="Max aggTrades pages per window")
    parser.add_argument("--rel-tol", type=float, default=1e-9)
    parser.add_argument("--abs-tol", type=float, default=1e-9)
    parser.add_argument("--concurrency", type=int, default=8)
    args = parser.parse_args()

    now_ms = int(time.time() * 1000)
    current_window_start = (now_ms // INTERVAL_MS) * INTERVAL_MS
    target_windows = [
        current_window_start - (args.skip_recent_windows + i + 1) * INTERVAL_MS
        for i in range(args.window_count)
    ]

    connector = aiohttp.TCPConnector(limit=max(8, args.concurrency))
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        symbols = await _fetch_exchange_symbols(session, args.api_base)
        symbols = symbols[: args.symbols]

        sem = asyncio.Semaphore(args.concurrency)
        results: List[CompareResult] = []

        async def run_one(sym: str, ws: int):
            async with sem:
                res = await _validate_one(
                    session=session,
                    api_base=args.api_base,
                    symbol=sym,
                    window_start_ms=ws,
                    rel_tol=args.rel_tol,
                    abs_tol=args.abs_tol,
                    max_pages=args.max_pages,
                )
                results.append(res)

        tasks = [asyncio.create_task(run_one(sym, ws)) for sym in symbols for ws in target_windows]
        await asyncio.gather(*tasks)

    total = len(results)
    passed = sum(1 for r in results if r.passed and r.reason == "ok")
    skipped = sum(1 for r in results if r.passed and r.reason.startswith("skip_"))
    failed = total - passed - skipped

    reason_count: Dict[str, int] = {}
    for r in results:
        reason_count[r.reason] = reason_count.get(r.reason, 0) + 1

    print("=== data_layer aggregation accuracy check (temporary script) ===")
    print(f"symbols={len(symbols)} windows_per_symbol={len(target_windows)} total_checks={total}")
    print(f"passed={passed} skipped={skipped} failed={failed}")
    print(f"pass_rate={(passed / max(1, (total - skipped)) * 100):.2f}% (excluding skipped)")
    print("reason_breakdown:", reason_count)

    mismatches = [r for r in results if not r.passed and r.reason == "field_mismatch"]
    if mismatches:
        print("\nTop mismatches (up to 10):")
        for r in mismatches[:10]:
            print(f"- {r.symbol} window={r.window_start_ms}")
            for k, d in r.details.items():
                if not d.get("ok", True):
                    print(
                        f"    {k}: local={d['local']} official={d['official']} diff={d['diff']}"
                    )

    other_failures = [r for r in results if not r.passed and r.reason != "field_mismatch"]
    if other_failures:
        print("\nNon-field failures (up to 20):")
        for r in other_failures[:20]:
            print(f"- {r.symbol} window={r.window_start_ms} reason={r.reason}")

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
