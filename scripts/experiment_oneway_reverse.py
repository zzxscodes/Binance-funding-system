#!/usr/bin/env python3
"""
实验：验证单向持仓模式下，是否可用一笔订单直接“反向开仓”。

流程：
1) 切到 one-way (dualSidePosition=false)
2) 对单一合约用最小可用下单量开仓（1x杠杆）
3) 尝试用一笔反向单直接翻仓
4) 收集结果并强制回到空仓，撤销挂单
"""

from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal, ROUND_DOWN
import os
import sys
from typing import Dict, Optional

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.common.config import config  # noqa: E402
from src.common.utils import format_symbol  # noqa: E402
from src.execution.binance_client import BinanceClient  # noqa: E402


def _dec(v) -> Decimal:
    return Decimal(str(v))


def _round_step(value: float, step: float) -> float:
    v = _dec(value)
    s = _dec(step)
    if s <= 0:
        return float(v)
    n = (v / s).to_integral_value(rounding=ROUND_DOWN)
    return float(n * s)


def _load_account_creds(account_id: str) -> Dict[str, str]:
    mode = str(config.get("execution.mode", "live"))
    mode_cfg = config.get(f"execution.{mode}", {}) or {}
    accounts = mode_cfg.get("accounts", []) or []
    acc = next((a for a in accounts if a.get("account_id") == account_id), None)
    if not acc:
        raise RuntimeError(f"account {account_id} not found in execution.{mode}.accounts")

    api_key = os.getenv(f"{account_id.upper()}_API_KEY", acc.get("api_key", ""))
    api_secret = os.getenv(f"{account_id.upper()}_API_SECRET", acc.get("api_secret", ""))
    if not api_key or not api_secret:
        raise RuntimeError("missing api credentials")

    return {
        "api_key": api_key,
        "api_secret": api_secret,
        "api_base": mode_cfg.get("api_base", config.get("binance.api_base")),
    }


async def _get_symbol_filters(client: BinanceClient, symbol: str):
    ex = await client.get_exchange_info()
    for s in ex.get("symbols", []):
        if format_symbol(s.get("symbol", "")) == symbol:
            step = 0.001
            min_qty = 0.001
            min_notional = 5.0
            for f in s.get("filters", []):
                t = f.get("filterType")
                if t == "LOT_SIZE":
                    step = float(f.get("stepSize", step))
                    min_qty = float(f.get("minQty", min_qty))
                elif t == "MIN_NOTIONAL":
                    min_notional = float(f.get("notional", min_notional))
            return step, min_qty, min_notional
    raise RuntimeError(f"symbol {symbol} not found in exchange info")


async def _find_position_amt(client: BinanceClient, symbol: str) -> float:
    for p in await client.get_positions():
        if format_symbol(p.get("symbol", "")) == symbol:
            return float(p.get("positionAmt", 0) or 0)
    return 0.0


async def _wait_order_filled(client: BinanceClient, symbol: str, order_id: int, timeout_s: float = 12.0):
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < timeout_s:
        st = await client.get_order_status(symbol, order_id)
        status = str(st.get("status", "")).upper()
        if status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}:
            return st
        await asyncio.sleep(0.5)
    return await client.get_order_status(symbol, order_id)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", default="account1")
    parser.add_argument("--symbol", default="DOGEUSDT")
    args = parser.parse_args()

    creds = _load_account_creds(args.account)
    symbol = format_symbol(args.symbol)
    client = BinanceClient(
        api_key=creds["api_key"],
        api_secret=creds["api_secret"],
        api_base=creds["api_base"],
        dry_run=False,
    )

    open_qty: Optional[float] = None
    try:
        # one-way mode + 1x leverage
        mode_res = await client.change_position_mode(False)
        print("set one-way:", mode_res)
        await client.change_leverage(symbol, 1)

        step, min_qty, min_notional = await _get_symbol_filters(client, symbol)
        px = await client.get_symbol_price(symbol)
        if not px or px <= 0:
            raise RuntimeError("invalid symbol price")

        qty = max(min_qty, (min_notional / px) * 1.05)
        qty = _round_step(qty, step)
        if qty < min_qty:
            qty = min_qty
        open_qty = qty
        print(f"symbol={symbol} price={px} step={step} min_qty={min_qty} min_notional={min_notional} open_qty={open_qty}")

        # step-1: open long
        o1 = await client.place_order(
            symbol=symbol,
            side="BUY",
            order_type="MARKET",
            quantity=open_qty,
            position_side="BOTH",
            reduce_only=False,
        )
        print("open order:", o1)
        o1_st = await _wait_order_filled(client, symbol, int(o1["orderId"]))
        print("open order status:", o1_st)

        # step-2: try reverse in one order
        reverse_qty = _round_step(open_qty * 2.0, step)
        print(f"try single reverse SELL qty={reverse_qty}")
        reverse_err = None
        reverse_res = None
        try:
            reverse_res = await client.place_order(
                symbol=symbol,
                side="SELL",
                order_type="MARKET",
                quantity=reverse_qty,
                position_side="BOTH",
                reduce_only=False,
            )
            reverse_res = await _wait_order_filled(client, symbol, int(reverse_res["orderId"]))
        except Exception as e:
            reverse_err = str(e)
        print("reverse_result:", reverse_res)
        print("reverse_error:", reverse_err)

        pos_amt = await _find_position_amt(client, symbol)
        print("position_after_reverse:", pos_amt)

    finally:
        # cleanup: flat position + cancel opens
        try:
            pos_amt = await _find_position_amt(client, symbol)
            if abs(pos_amt) > 1e-12:
                side = "SELL" if pos_amt > 0 else "BUY"
                step, _, _ = await _get_symbol_filters(client, symbol)
                flat_qty = _round_step(abs(pos_amt), step)
                if flat_qty > 0:
                    flat_res = await client.place_order(
                        symbol=symbol,
                        side=side,
                        order_type="MARKET",
                        quantity=flat_qty,
                        position_side="BOTH",
                        reduce_only=True,
                    )
                    flat_st = await _wait_order_filled(client, symbol, int(flat_res["orderId"]))
                    print("flatten_result:", flat_st)
            await client.cancel_all_orders(symbol)
        except Exception as e:
            print("cleanup_error:", e)
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())

