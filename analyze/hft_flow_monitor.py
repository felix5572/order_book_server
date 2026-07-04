"""
HFT Flow Monitor v1.24 - 零权重原始版 (Native Event Count)

1. 移除所有权重计分：所有事件 (Open/Cancel/Fill) 均为 +1 计数，不再区分 Taker/Maker 权重。
2. 修复 Ratio 报错：确保 Layout 比例为整数 (5:6)。
3. 成交方向透显：保持 Taker ↔ Maker 的方向标注，用于视觉参考而非计分。
4. 增强稳定性：WebSocket 自动重连及异常捕获。
"""

import asyncio
import json
import os
import sys
import argparse
import time
from collections import deque, Counter
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from rich.live import Live
from rich.table import Table
from rich.layout import Layout
from rich.panel import Panel
from rich.console import Console
from rich.text import Text
from rich import box
import websockets

class MonitorState:
    def __init__(self, max_logs: int, watch_addresses: List[str] = None):
        self.all_orders = {}
        self.last_update_ts = None
        self.order_flow = deque(maxlen=max_logs)
        self.trade_flow = deque(maxlen=max_logs)
        self.mm_activity = Counter() # 仅存储原始事件计数
        self.mm_history = {}
        self.spread = 0.0
        self.height = 0
        self.target_coin = ""
        self.max_logs = max_logs
        self.watch_addresses = [a.lower() for a in (watch_addresses or [])]

    def get_best_orders(self):
        all_objs = list(self.all_orders.values())
        b_raw = [o for o in all_objs if o['side'] == 'B']
        a_raw = [o for o in all_objs if o['side'] == 'A']
        sorted_bids = sorted(b_raw, key=lambda x: (-float(x['limitPx']), x.get('timestamp') or 0))
        sorted_asks = sorted(a_raw, key=lambda x: (float(x['limitPx']), x.get('timestamp') or 0))
        return sorted_bids[:20], sorted_asks[:20]

def format_ts_full(ts):
    if not ts: return "N/A"
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return dt.strftime("%H:%M:%S.%f")[:-3]

def process_ws_message(state: MonitorState, msg: str):
    try:
        data = json.loads(msg)
    except: return
    channel = data.get("channel")
    if not channel: return
    
    # --- l4Book Channel (Direct Event Counter) ---
    if channel == "l4Book":
        payload = data["data"]
        if "Snapshot" in payload:
            snap = payload["Snapshot"]
            state.height = snap["height"]; state.last_update_ts = snap["time"]
            state.all_orders.clear()
            for side_idx in [0, 1]:
                for o in snap["levels"][side_idx]: state.all_orders[o["oid"]] = o
        elif "Updates" in payload:
            upd = payload["Updates"]; state.height = upd["height"]; state.last_update_ts = upd["time"]
            ts_str = format_ts_full(state.last_update_ts)
            temp_side_map = {}
            for status in upd.get("order_statuses", []):
                o = status["order"]; user = status["user"]
                st = status["status"]; oid, side = o["oid"], o["side"]
                temp_side_map[oid] = side
                p, s = o["limitPx"], o["sz"]
                side_txt, side_clr = ("买入", "green") if side == "B" else ("卖出", "red")
                log_prefix = f"[dim]{ts_str}[/] "
                
                if st == "filled":
                    state.trade_flow.append(f"{log_prefix}💰 [[bold cyan]STATUS:FILLED[/]] {side_txt}已填满 @ {p} ({s}) | [dim]{user}[/]")
                elif st in ["open", "canceled"]:
                    c = "green" if st == "open" else "yellow"
                    state.order_flow.append(f"{log_prefix}[bold {c}][{st.upper()}][/] {side_txt} @ {p} ({s}) | [dim]{user}[/]")
                else:
                    state.order_flow.append(f"{log_prefix}[bold red][{st.upper()}][/] {side_txt} @ {p} ({s}) | [dim]{user}[/]")

                # 原始计数 + 1
                u_l = user.lower()
                state.mm_activity[u_l] += 1
                if u_l not in state.mm_history: state.mm_history[u_l] = deque(maxlen=30)
                state.mm_history[u_l].append(f"{log_prefix}[{st.upper()}] {side_txt} @ {p} ({s})")

            for diff in upd.get("book_diffs", []):
                oid = diff["oid"]; raw = diff.get("raw_book_diff")
                if not raw: continue
                if raw == "remove" or (isinstance(raw, dict) and "remove" in raw):
                    state.all_orders.pop(oid, None)
                elif isinstance(raw, dict):
                    side = state.all_orders.get(oid, {}).get('side') or temp_side_map.get(oid)
                    if side:
                        new_sz = raw["new"]["sz"] if "new" in raw else (raw["update"].get("newSz") if "update" in raw else None)
                        if new_sz:
                            state.all_orders[oid] = { "oid": oid, "user": diff["user"], "limitPx": diff["px"], "sz": new_sz, "side": side, "timestamp": state.last_update_ts }

    # --- Trades Channel (Direct Event Counter) ---
    elif channel == "trades":
        for t in data.get("data", []):
            ts_str = format_ts_full(t["time"]); users = t.get("users", [])
            if len(users) < 2: continue
            if t["side"] == "B":
                taker, maker = users[0], users[1]; t_side, t_clr = "吃多 (买入)", "green"
            else:
                taker, maker = users[1], users[0]; t_side, t_clr = "砸盘 (卖出)", "red"
            
            pfx = f"[dim]{ts_str}[/] "
            state.trade_flow.append(f"{pfx}💎 [[bold bright_green]FILL[/]] [bold {t_clr}]{t_side}[/] {taker} ↔ {maker} @ [bold yellow]{t['px']}[/] ({t['sz']})")
            
            # Taker 和 Maker 均单纯 +1 计数
            for u in [taker, maker]:
                u_l = u.lower()
                state.mm_activity[u_l] += 1
                if u_l not in state.mm_history: state.mm_history[u_l] = deque(maxlen=30)
                state.mm_history[u_l].append(f"{pfx}[{ 'TAKER' if u==taker else 'MAKER' }] {t_side} @ {t['px']} ({t['sz']})")

    b_o, a_o = state.get_best_orders()
    if b_o and a_o: state.spread = float(a_o[0]['limitPx']) - float(b_o[0]['limitPx'])

def make_layout() -> Layout:
    l = Layout()
    l.split_column(Layout(name="header", size=3), Layout(name="main", ratio=2), Layout(name="footer", ratio=1))
    l["main"].split_row( Layout(name="book", ratio=1), Layout(name="mm_stats", ratio=1) )
    l["footer"].split_row( Layout(name="intent", ratio=5), Layout(name="ex", ratio=6) )
    return l

def render_ui(s: MonitorState, layout: Layout):
    ts = format_ts_full(s.last_update_ts)
    hdr = Table.grid(expand=True); hdr.add_column(ratio=1); hdr.add_column(justify="right")
    hdr.add_row(f"[bold white]⚡ HFT FLOW MONITOR v1.24[/] [bold yellow]{s.target_coin}[/]", f"[dim]Block: {s.height} | Spd: {s.spread:.4f} | {ts}Z[/]")
    layout["header"].update(Panel(hdr, style="blue"))

    bt = Table(box=box.SIMPLE, show_header=True, expand=True, header_style="bold yellow", padding=(0, 1))
    bt.add_column("Price", justify="right", style="bold"); bt.add_column("Size", justify="right"); bt.add_column("T", justify="center", style="dim"); bt.add_column("Owner", justify="left")
    b, a = s.get_best_orders()
    for o in a[:20][::-1]: bt.add_row(f"[red]{o['limitPx']}[/]", f"{float(o['sz']):.2f}", format_ts_full(o.get('timestamp'))[-8:], f"[dim]{o['user']}[/]")
    m = (float(a[0]['limitPx']) + float(b[0]['limitPx'])) / 2.0 if b and a else 0.0
    bt.add_row(f"[bold yellow]{m:.4f}[/]", "---", "---", "[bold blue]MID[/]")
    for o in b[:20]: bt.add_row(f"[green]{o['limitPx']}[/]", f"{float(o['sz']):.2f}", format_ts_full(o.get('timestamp'))[-8:], f"[dim]{o['user']}[/]")
    layout["book"].update(Panel(bt, title="FIFO Individual Matrix", border_style="cyan"))

    targets = []
    for addr in s.watch_addresses: targets.append((addr, "WATCHED"))
    top = sorted(s.mm_activity.items(), key=lambda x: x[1], reverse=True)
    for u, _ in top:
        if len(targets) >= 2: break
        if u not in [t[0] for t in targets]: targets.append((u, "AUTO-TOP"))
    grid = Table.grid(expand=True, padding=(0, 2)); grid.add_column(ratio=1); grid.add_column(ratio=1)
    for u, label in targets:
        hist = list(s.mm_history.get(u, []))[-20:]
        h_txt = f"[{'bold cyan' if label=='WATCHED' else 'bold yellow'}]{label}[/] [bold white]({s.mm_activity.get(u,0)} Acts)[/]\n[dim]{u}[/]\n" + "─"*30
        grid.add_row(Text.from_markup(f"{h_txt}\n" + "\n".join(hist)))
    layout["mm_stats"].update(Panel(grid, title="Top Active Players (Event Count)", border_style="magenta"))
    layout["intent"].update(Panel(Text.from_markup("\n".join(s.order_flow)), title="Order Intent", border_style="yellow"))
    layout["ex"].update(Panel(Text.from_markup("\n".join(s.trade_flow)), title="Execution Flow", border_style="green"))

async def watchdog(args):
    while True:
        try:
            state = MonitorState(args.logs, args.watch); state.target_coin = args.coin; layout = make_layout()
            async with websockets.connect(args.url, max_size=100*1024*1024) as ws:
                await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l4Book", "coin": state.target_coin}}))
                await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": state.target_coin}}))
                with Live(layout, refresh_per_second=10, screen=True) as live:
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=20.0); process_ws_message(state, msg)
                        except asyncio.TimeoutError: pass
                        render_ui(state, layout)
        except Exception as e:
            print(f"Connection lost. Reconnecting... ({e})")
            await asyncio.sleep(2)

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("coin", nargs='?', default="BTC"); p.add_argument("-w", "--watch", nargs='*')
    p.add_argument("-u", "--url", default=os.environ.get("WS_URL", "ws://localhost:8000/ws")); p.add_argument("-l", "--logs", type=int, default=20)
    args = p.parse_args(); asyncio.run(watchdog(args))
