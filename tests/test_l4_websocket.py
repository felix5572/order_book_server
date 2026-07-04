"""
WebSocket 连通性测试和 L4 数据结构演示

这个测试脚本演示：
1. 连接到 order_book_server 的 WebSocket
2. 订阅 L4Book 数据流
3. 展示完整的 L4 订单簿数据结构

运行前提：
- order_book_server 已启动: cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000
- 或者使用环境变量指定地址: WS_URL=ws://localhost:8000/ws python test_l4_websocket.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import websockets
from websockets.client import WebSocketClientProtocol


class L4WebSocketClient:
    """L4 订单簿 WebSocket 客户端"""

    def __init__(self, ws_url: str = "ws://localhost:8000/ws"):
        self.ws_url = ws_url
        self.ws: Optional[WebSocketClientProtocol] = None
        self.message_count = 0

    async def connect(self) -> bool:
        """连接到 WebSocket 服务器"""
        try:
            print(f"🔗 正在连接到 {self.ws_url}...")
            # 增加 max_size 限制，因为 L4 快照数据非常大（通常超过 1MB）
            self.ws = await websockets.connect(self.ws_url, max_size=100 * 1024 * 1024)
            print("✅ WebSocket 连接成功！\n")
            return True
        except Exception as e:
            print(f"❌ 连接失败: {e}")
            return False

    async def subscribe_l4book(self, coin: str = "BTC") -> bool:
        """订阅 L4Book 数据流"""
        if not self.ws:
            print("❌ WebSocket 未连接")
            return False

        subscription_msg = {
            "method": "subscribe",
            "subscription": {"type": "l4Book", "coin": coin},
        }

        try:
            print(f"📡 订阅 L4Book: {coin}")
            await self.ws.send(json.dumps(subscription_msg))
            print("✅ 订阅消息已发送\n")
            return True
        except Exception as e:
            print(f"❌ 订阅失败: {e}")
            return False

    async def receive_messages(self, max_messages: int = 3, timeout: int = 30):
        """接收并展示消息"""
        if not self.ws:
            return

        print(f"📥 等待接收消息 (最多 {max_messages} 条，超时 {timeout} 秒)...\n")
        print("=" * 80)

        try:
            while self.message_count < max_messages:
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=timeout)
                    self.message_count += 1
                    self._display_message(message)
                except asyncio.TimeoutError:
                    print(f"\n⏱️  超时 ({timeout} 秒) - 未收到新消息")
                    break

        except Exception as e:
            print(f"\n❌ 接收消息时出错: {e}")

    def _display_message(self, message: str):
        """格式化显示消息"""
        print(f"\n📨 消息 #{self.message_count}")
        print("-" * 80)

        try:
            data = json.loads(message)
            channel = data.get("channel", "unknown")
            print(f"频道: {channel}")

            if channel == "subscriptionResponse":
                self._display_subscription_response(data)
            elif channel == "l4Book":
                self._display_l4book(data)
            else:
                print(f"原始数据: {json.dumps(data, indent=2, ensure_ascii=False)}")

        except json.JSONDecodeError:
            print(f"原始消息: {message}")

        print("=" * 80)

    def _display_subscription_response(self, data: Dict[str, Any]):
        """展示订阅响应"""
        response_data = data.get("data", {})
        method = response_data.get("method", "unknown")
        subscription = response_data.get("subscription", {})

        print(f"✅ 订阅确认")
        print(f"  方法: {method}")
        print(f"  类型: {subscription.get('type')}")
        print(f"  币种: {subscription.get('coin')}")

    def _display_l4book(self, data: Dict[str, Any]):
        """展示 L4Book 数据结构 (根据 Rust 源码精确对齐)"""
        l4_payload = data.get("data", {})
        
        if not isinstance(l4_payload, dict):
            return

        # 1. 匹配 Snapshot (TitleCase)
        if "Snapshot" in l4_payload:
            self._display_snapshot(l4_payload["Snapshot"])
            
        # 2. 匹配 Updates (TitleCase)
        elif "Updates" in l4_payload:
            self._display_updates(l4_payload["Updates"])
        else:
            # 只有在完全无法识别时才简短打印
            keys = list(l4_payload.keys())
            print(f"⚠️  未知内部协议结构 | Keys: {keys}")

    def _display_snapshot(self, snapshot: Dict[str, Any]):
        """展示 L4 快照 (完全精确版)"""
        coin = snapshot.get("coin", "N/A")
        raw_ts = snapshot.get("time")
        ts_utc = self._format_timestamp(raw_ts)
        print(f"\n" + "=" * 100)
        print(f"🛰️  L4 精确盘口快照 | 标的: {coin} | UTC: {ts_utc} | Raw: {raw_ts}")
        print("=" * 100)

        levels = snapshot.get("levels", [[], []])
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []

        asks_sorted = sorted(asks, key=lambda x: float(x.get("limitPx", 0)))
        bids_sorted = sorted(bids, key=lambda x: float(x.get("limitPx", 0)), reverse=True)

        print("\n🔴 卖盘盘口 (Lowest Asks):")
        for i, o in enumerate(asks_sorted[:20], 1):
            user = o.get("user") or "N/A"
            ts = self._format_timestamp(o.get("timestamp"))
            print(f"  #{i:<2} {o.get('limitPx'):<10} | {o.get('sz'):<10} | User: {user} | OID: {o.get('oid')} | T: {ts}")

        print("-" * 60)
        print("🟢 买盘盘口 (Highest Bids):")
        for i, o in enumerate(bids_sorted[:20], 1):
            user = o.get("user") or "N/A"
            ts = self._format_timestamp(o.get("timestamp"))
            print(f"  #{i:<2} {o.get('limitPx'):<10} | {o.get('sz'):<10} | User: {user} | OID: {o.get('oid')} | T: {ts}")
        print("=" * 100 + "\n")

    def _display_updates(self, updates: Dict[str, Any]):
        """展示实时交易进展 (UTC + 完整地址)"""
        raw_ts = updates.get("time")
        formatted = self._format_timestamp(raw_ts)
        ts_utc = formatted.split(" ")[1] if " " in formatted else formatted
        
        # 1. 解析订单状态变化
        order_statuses = updates.get("order_statuses", [])
        if order_statuses:
            print(f"⚡ [实时意图 | 区块 {updates.get('height')} | UTC {ts_utc}]")
            for status in order_statuses:
                st = status.get("status", "unknown")
                order = status.get("order", {})
                user = status.get("user") or "N/A"
                side = "买入" if order.get("side") == "B" else "卖出"
                px = order.get("limitPx", "N/A")
                sz = order.get("sz", "N/A")
                
                if st == "open":
                    print(f"  🟢 [Opened]   用户 {user} 成功挂出 {side}单 @ {px} ({sz})")
                elif st == "filled":
                    print(f"  💰 [Filled]   用户 {user} 的 {side}单 已成交! @ {px} ({sz})")
                elif st == "canceled":
                    print(f"  🚫 [Canceled] 用户 {user} 撤回了 @ {px} 的单子 (OID: {order.get('oid')})")
                elif "Rejected" in st:
                    print(f"  ⚠️ [Rejected] 用户 {user} 尝试 {side} 失败 ({st}) @ {px}")
                else:
                    print(f"  ℹ️ [{st:^8}] 用户 {user} {side}单状态变化 @ {px}")

        # 2. 订单簿差异
        book_diffs = updates.get("book_diffs", [])
        if book_diffs:
            print(f"📊 [盘口微调: {len(book_diffs)} 处]")
            for diff in book_diffs[:5]:
                raw = diff.get("rawBookDiff", {})
                px = diff.get("px", "N/A")
                user = diff.get("user", "N/A")
                if "new" in raw: op = "新增"
                elif "update" in raw: op = "调整"
                else: op = "移除"
                print(f"    - {op} @ {px} (User: {user} | OID: {diff.get('oid')})")
        print("-" * 80)

    @staticmethod
    def _format_timestamp(ts: Optional[int]) -> str:
        """格式化时间戳为 UTC (毫秒)"""
        if ts is None:
            return "N/A"
        try:
            # 使用 UTC 时间
            dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"
        except Exception:
            return str(ts)

    @staticmethod
    def _format_timestamp_str(ts_str: Optional[str]) -> str:
        """格式化时间戳字符串"""
        if not ts_str:
            return "N/A"
        try:
            # 处理类似 "2025-01-21T19:48:36.123456" 的格式
            return ts_str.replace("T", " ")[:23]
        except Exception:
            return ts_str

    async def close(self):
        """关闭连接"""
        if self.ws:
            await self.ws.close()
            print("\n👋 WebSocket 连接已关闭")


async def test_l4_websocket(
    ws_url: str = "ws://localhost:8000/ws", coin: str = "BTC", max_messages: int = 3
):
    """
    测试 L4 WebSocket 连接

    Args:
        ws_url: WebSocket 服务器地址
        coin: 币种
        max_messages: 最多接收的消息数
    """
    print("=" * 80)
    print("🧪 L4 WebSocket 连通性测试")
    print("=" * 80)
    print()

    client = L4WebSocketClient(ws_url)

    try:
        # 1. 连接测试
        if not await client.connect():
            sys.exit(1)

        # 2. 订阅测试
        if not await client.subscribe_l4book(coin):
            sys.exit(1)

        # 3. 接收并展示数据
        await client.receive_messages(max_messages=max_messages)

        print("\n✅ 测试完成！")
        print("\n📋 总结:")
        print(f"  - WebSocket 连接: ✅ 成功")
        print(f"  - L4Book 订阅: ✅ 成功")
        print(f"  - 接收消息数: {client.message_count}")

    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断")
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        await client.close()


if __name__ == "__main__":
    # 默认值
    ws_url = "ws://localhost:8000/ws"
    coin = "BTC"
    max_messages = 4

    # 处理命令行参数
    if len(sys.argv) > 1:
        # 如果第一个参数看起来像币种而不是 URL（不包含 ://）
        if "://" not in sys.argv[1]:
            coin = sys.argv[1]
            if len(sys.argv) > 2:
                max_messages = int(sys.argv[2])
        else:
            ws_url = sys.argv[1]
            if len(sys.argv) > 2:
                coin = sys.argv[2]
            if len(sys.argv) > 3:
                max_messages = int(sys.argv[3])

    # 环境变量仍可覆盖
    ws_url = os.environ.get("WS_URL", ws_url)
    coin = os.environ.get("COIN", coin)
    max_messages = int(os.environ.get("MAX_MESSAGES", str(max_messages)))

    print(f"🚀 启动看板:")
    print(f"  目标服务器: {ws_url}")
    print(f"  监控标的:   {coin}")
    print(f"  持续消息数: {max_messages}")
    print(f"  (提示: 可以使用 'python {sys.argv[0]} SILVER' 切换标的)\n")

    asyncio.run(test_l4_websocket(ws_url, coin, max_messages))
