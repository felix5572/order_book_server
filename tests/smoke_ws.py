#!/usr/bin/env python3
"""order_book_server 冒烟探针:逐 channel 订阅并等首帧, 全部收到即 PASS。

    python tests/smoke_ws.py [--url ws://localhost:8000] [--coin XMR] [--spot @260] \
                             [--oracle-coin flx:XMR] [--timeout 30]

覆盖: bbo / l2Book / trades / bookDiffs / oracle(HIP-3)。trades 低频资产可能超时,
单独标 WARN 不算失败(无成交=无帧是合法状态);其余 channel 超时 = FAIL。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

import websockets


async def probe(url: str, sub: dict, timeout: float) -> tuple[str, str]:
    """(结果, 详情)。结果: ok / timeout / error。"""
    label = sub["type"]
    try:
        async with websockets.connect(url, open_timeout=10) as ws:
            await ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
            deadline = asyncio.get_event_loop().time() + timeout
            while True:
                remain = deadline - asyncio.get_event_loop().time()
                if remain <= 0:
                    return "timeout", f"{label}: {timeout}s 内无数据帧"
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=remain))
                ch = msg.get("channel")
                if ch == "subscriptionResponse":
                    continue
                if ch == "error":
                    return "error", f"{label}: 服务端拒绝 {msg}"
                preview = json.dumps(msg)[:180]
                return "ok", f"{label}: channel={ch} {preview}"
    except Exception as e:  # 冒烟工具: 连接层错误如实报
        return "error", f"{label}: {type(e).__name__}: {e}"


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="ws://localhost:8000/ws")
    ap.add_argument("--coin", default="XMR")
    ap.add_argument("--spot", default="@260")
    ap.add_argument("--oracle-coin", default="flx:XMR")
    ap.add_argument("--timeout", type=float, default=30)
    args = ap.parse_args()

    subs = [
        {"type": "bbo", "coin": args.coin},
        {"type": "l2Book", "coin": args.coin, "nSigFigs": None, "nLevels": None, "mantissa": None},
        {"type": "l2Book", "coin": args.spot, "nSigFigs": None, "nLevels": None, "mantissa": None},
        {"type": "bookDiffs", "coin": args.coin},
        {"type": "trades", "coin": args.coin},
        {"type": "oracle", "coins": [args.oracle_coin]},
    ]
    results = await asyncio.gather(*(probe(args.url, s, args.timeout) for s in subs))

    failed = 0
    for sub, (status, detail) in zip(subs, results):
        soft = sub["type"] == "trades" and status == "timeout"  # 无成交=无帧, 合法
        tag = "PASS" if status == "ok" else ("WARN" if soft else "FAIL")
        print(f"[{tag}] {detail}")
        if tag == "FAIL":
            failed += 1
    print(f"smoke: {len(subs) - failed}/{len(subs)} 通过")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
