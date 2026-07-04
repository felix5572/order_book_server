#!/usr/bin/env python3
"""订阅行情, 实时展示订单簿买卖前五档(默认 ETH, USDC 计价)。Ctrl-C 退出。

    python tests/watch_book.py [--coin ETH] [--levels 5] [--url ws://localhost:8000/ws]
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
REDRAW_MIN_INTERVAL = 0.25  # 秒。l2Book 帧率高, 限频重画防眼花


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

    hms = lambda ms: datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%H:%M:%S.%f")[:-3]

    # 光标回左上角, 每行覆盖写并清行尾(\x1b[K)—— 不整屏闪
    lines = [
        f"{coin}/USDC   本地 {hms(now_ms)}   订单簿 {hms(t)}(旧 {(now_ms - t) / 1000:.2f}s)"
        + ("" if tip is None else f"   节点 {hms(tip)}(旧 {(now_ms - tip) / 1000:.2f}s)"),
        "",
        f"{'':>6}  {'价格':>12}  {'数量':>12}  单数",
    ]
    for lv in reversed(asks):
        lines.append(f"{'卖':>6}  {lv['px']:>12}  {lv['sz']:>12}  {lv['n']}")
    if bids and asks:
        spread = float(asks[0]["px"]) - float(bids[0]["px"])
        mid = (float(asks[0]["px"]) + float(bids[0]["px"])) / 2
        lines.append(f"{'——':>6}  {spread:>12.4f}  ({spread / mid * 1e4:.2f} bps)")
    for lv in bids:
        lines.append(f"{'买':>6}  {lv['px']:>12}  {lv['sz']:>12}  {lv['n']}")

    print("\x1b[H" + "\n".join(f"{l}\x1b[K" for l in lines) + "\x1b[J", end="", flush=True)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--coin", default="ETH")
    ap.add_argument("--levels", type=int, default=5)
    ap.add_argument("--url", default="ws://localhost:8000/ws")
    args = ap.parse_args()

    sub = {"type": "l2Book", "coin": args.coin, "nSigFigs": None, "nLevels": None, "mantissa": None}
    async with websockets.connect(args.url, open_timeout=10) as ws:
        await ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
        print("\x1b[2J", end="")  # 只在启动时整屏清一次
        last_draw = 0.0
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("channel") == "l2Book":
                if time.time() - last_draw < REDRAW_MIN_INTERVAL:
                    continue
                last_draw = time.time()
                data = msg["data"]
                data["levels"] = [side[: args.levels] for side in data["levels"]]
                render(args.coin, data)
            elif msg.get("channel") == "error":
                raise SystemExit(f"订阅被拒: {msg}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print()
