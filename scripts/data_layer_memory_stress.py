#!/usr/bin/env python3
"""
Isolated data_layer memory stress test.

- Runs in mock mode only (isolated process).
- Builds 540-symbol universe by default.
- Collects RSS and aggregator stats periodically.
- Writes a JSON report into logs/.
"""

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import psutil

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import config
from src.processes import data_layer as data_layer_module
from src.processes.data_layer import DataLayerProcess


class NoopIPCClient:
    """Isolated stress test should not depend on IPC server availability."""

    async def connect(self):
        return None

    async def disconnect(self):
        return None

    async def send_message(self, _message):
        return None

    async def send_data_complete(self, _timestamp, _symbols):
        return None

    async def send_target_position_ready(self, _account_id, _file_path):
        return None

    async def send_order_executed(self, _account_id, _order_info):
        return None


def build_symbols(count: int) -> List[str]:
    base = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    if count <= len(base):
        return base[:count]
    extra_count = count - len(base)
    extra = [f"MOCK{i:04d}USDT" for i in range(1, extra_count + 1)]
    return base + extra


def write_universe(symbols: List[str]) -> Path:
    day_dir = PROJECT_ROOT / "data" / "universe" / datetime.now().strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    target = day_dir / "universe.csv"
    with open(target, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol"])
        for symbol in symbols:
            writer.writerow([symbol])
    return target


def pending_trade_count(pending_trades: Dict[str, Dict[int, List]]) -> int:
    total = 0
    for windows in pending_trades.values():
        for trades in windows.values():
            total += len(trades)
    return total


async def run(args: argparse.Namespace) -> Path:
    symbols = build_symbols(args.symbols)
    universe_file = write_universe(symbols)

    # Runtime overrides for isolated test process
    config.set("execution.mode", "mock")
    config.set("data.kline_reconcile_enabled", False)
    config.set("data.save_interval", args.save_interval)
    config.set("data.memory_cleanup_interval", args.memory_cleanup_interval)
    config.set("data.kline_aggregator_max_klines", args.max_klines)
    config.set("data.kline_aggregator_max_pending_windows", args.max_pending_windows)
    config.set("data.kline_aggregator_max_trades_per_window", args.max_trades_per_window)
    config.set("data.kline_close_grace_windows", args.close_grace_windows)
    config.set("data.trades_buffer_max_size", args.trades_buffer_max_size)
    config.set("data.trades_buffer_total_max_size", args.trades_buffer_total_max_size)

    data_layer_module.IPCClient = NoopIPCClient
    process = DataLayerProcess()
    local_proc = psutil.Process(os.getpid())
    started = time.time()
    metrics = []

    print("=" * 88)
    print("data_layer isolated memory stress started")
    print(f"universe_file={universe_file}")
    print(
        f"symbols={len(symbols)} duration={args.duration}s sample={args.sample_interval}s "
        f"save_interval={args.save_interval}s max_klines={args.max_klines}"
    )
    print("=" * 88)

    run_error = None
    await process.start()
    try:
        while True:
            elapsed = time.time() - started
            if elapsed >= args.duration:
                break

            rss_mb = local_proc.memory_info().rss / 1024 / 1024
            row = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "elapsed_sec": round(elapsed, 2),
                "rss_mb": round(rss_mb, 2),
                "pending_symbols": 0,
                "pending_windows": 0,
                "pending_trades_total": 0,
                "klines_symbols": 0,
                "klines_total": 0,
            }
            if process.kline_aggregator:
                agg = process.kline_aggregator
                row["pending_symbols"] = len(agg.pending_trades)
                row["pending_windows"] = sum(len(v) for v in agg.pending_trades.values())
                row["pending_trades_total"] = pending_trade_count(agg.pending_trades)
                row["klines_symbols"] = len(agg.klines)
                row["klines_total"] = sum(len(df) for df in agg.klines.values() if not df.is_empty())

            metrics.append(row)
            print(
                f"[{row['elapsed_sec']:7.2f}s] rss={row['rss_mb']:8.2f}MB "
                f"pending_symbols={row['pending_symbols']:4} pending_windows={row['pending_windows']:5} "
                f"pending_trades={row['pending_trades_total']:9} klines_total={row['klines_total']:6}"
            )
            await asyncio.sleep(args.sample_interval)
    except Exception as e:
        run_error = str(e)
    finally:
        if args.fast_stop:
            tasks_to_cancel = list(process.tasks)
            for task in tasks_to_cancel:
                try:
                    task.cancel()
                except Exception:
                    pass
            if tasks_to_cancel:
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            if process.collector:
                await process.collector.stop()
            if process.universe_manager:
                await process.universe_manager.stop()
            if process.ipc_client:
                await process.ipc_client.disconnect()
            process.running = False
        else:
            await process.stop()

    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    report = logs_dir / f"data_layer_memory_stress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report, "w", encoding="utf-8") as f:
        json.dump(
            {
                "config": vars(args),
                "symbols": len(symbols),
                "report_generated_at": datetime.now(timezone.utc).isoformat(),
                "run_error": run_error,
                "metrics": metrics,
            },
            f,
            indent=2,
            ensure_ascii=True,
        )
    print(f"report_saved={report}")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Isolated data_layer memory stress test")
    parser.add_argument("--symbols", type=int, default=540)
    parser.add_argument("--duration", type=int, default=420)
    parser.add_argument("--sample-interval", type=int, default=20)
    parser.add_argument("--save-interval", type=int, default=20)
    parser.add_argument("--memory-cleanup-interval", type=int, default=30)
    parser.add_argument("--max-klines", type=int, default=8)
    parser.add_argument("--max-pending-windows", type=int, default=8)
    parser.add_argument("--max-trades-per-window", type=int, default=0)
    parser.add_argument("--close-grace-windows", type=int, default=6)
    parser.add_argument("--trades-buffer-max-size", type=int, default=200)
    parser.add_argument("--trades-buffer-total-max-size", type=int, default=80000)
    parser.add_argument("--fast-stop", action="store_true", default=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    asyncio.run(run(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
