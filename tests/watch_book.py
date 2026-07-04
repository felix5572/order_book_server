#!/usr/bin/env python3
"""订阅行情, 实时展示订单簿买卖前五档(默认 ETH, USDC 计价)。

    python watch_book.py [--coin ETH] [--levels 5] [--url ws://localhost:8000/ws]

表头带新鲜度: block_time = 该簿的块时间;book_age = 现在−块时间(订到的簿有多旧);
node_age = 现在−节点 fast_block_times 尾行块时间(节点落后链多少, 追平期这个大)。
Ctrl-C 退出。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

FAST_BLOCK_DIR = Path.home() / "hl/data/node_fast_block_times"


def node_tip_ms() -> float | None:
    """节点最新处理块的块时间(ms), 读当天/昨天 date 文件尾行;不在节点机上跑则 None。"""
    for days_back in (0, 1):
        day = datetime.fromtimestamp(time.time() - 86400 * days_back, timezone.utc)
        p = FAST_BLOCK_DIR / day.strftime("%Y%m%d")
        if not p.is_file():
            continue
        with open(p, "rb") as f:
            f.seek(max(0, os.fstat(f.fileno()).st_size - 8192))
            lines = [l for l in f.read().splitlines() if l.strip()]
        if not lines:
            continue
        head, frac = json.loads(lines[-1])["block_time"].split(".")
        dt = datetime.fromisoformat(f"{head}.{frac[:6]}").replace(tzinfo=timezone.utc)
        return dt.timestamp() * 1000
    return None


def render(coin: str, data: dict) -> None:
    now_ms = time.time() * 1000
    t = data["time"]
    bids, asks = data["levels"][0], data["levels"][1]
    tip = node_tip_ms()

    fmt_hms = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%S.%f")[:-3]

    print("\x1b[2J\x1b[H", end="")  # 清屏回到左上
    print(f"{coin}(USDC)")
    print(f"  本地时间   {fmt_hms(now_ms)} UTC")
    print(f"  簿块时间   {fmt_hms(t)} UTC   ← 现在 {(now_ms - t) / 1000:7.3f}s(订到的簿有多旧)")
    if tip is not None:
        print(f"  节点块时间 {fmt_hms(tip)} UTC   ← 现在 {(now_ms - tip) / 1000:7.3f}s(节点落后链多少)"
              f"   簿落后节点 {(tip - t) / 1000:.3f}s")
    print()
    print(f"{'':>14}  {'价格':>12}  {'数量':>12}  档位单数")
    for lv in reversed(asks):
        print(f"{'卖':>14}  {lv['px']:>12}  {lv['sz']:>12}  {lv['n']}")
    if bids and asks:
        spread = float(asks[0]["px"]) - float(bids[0]["px"])
        mid = (float(asks[0]["px"]) + float(bids[0]["px"])) / 2
        print(f"{'—— spread':>14}  {spread:>12.4f}  ({spread / mid * 1e4:.2f} bps)")
    for lv in bids:
        print(f"{'买':>14}  {lv['px']:>12}  {lv['sz']:>12}  {lv['n']}")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--coin", default="ETH")
    ap.add_argument("--levels", type=int, default=5)
    ap.add_argument("--url", default="ws://localhost:8000/ws")
    args = ap.parse_args()

    sub = {"type": "l2Book", "coin": args.coin, "nSigFigs": None, "nLevels": None, "mantissa": None}
    async with websockets.connect(args.url, open_timeout=10) as ws:
        await ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("channel") == "l2Book":
                data = msg["data"]
                data["levels"] = [side[: args.levels] for side in data["levels"]]
                render(args.coin, data)
            elif msg.get("channel") == "error":
                raise SystemExit(f"订阅被拒: {msg}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
