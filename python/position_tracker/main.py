#!/usr/bin/env python3
"""
Position Tracker — 本地持仓簿
==============================
订阅 fill.option.qqq → 维护持仓 → 发布 position.option.qqq
策略引擎订阅 position.option.qqq 后可做策略专属平仓决策。

用法:
  python python/position_tracker/main.py
"""
import asyncio
import json
import os
import time
from datetime import datetime, timezone

import nats

NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class PositionBook:
    """线程安全的本地持仓簿"""

    def __init__(self):
        self.positions: dict[str, dict] = {}  # order_id → position

    def upsert(self, fill: dict):
        """从 fill 事件更新/创建持仓"""
        oid = fill.get("order_id", "")
        if not oid:
            return

        instrument = fill.get("instrument", {})
        symbol = instrument.get("symbol", "")
        side = fill.get("side", "")
        qty = fill.get("quantity", "1")
        price = fill.get("price", "0")
        source_signal_id = fill.get("source_signal_id", "")

        # 从 source_signal_id 提取 strategy_id
        # 格式: signal-{strategy_id}-{ts}-{random}
        strategy_id = "unknown"
        parts = source_signal_id.split("-") if source_signal_id else []
        if len(parts) >= 4 and parts[0] == "signal":
            strategy_id = "-".join(parts[1:-2])

        # 判断是开仓还是平仓
        # SELL side 可能是平仓也可能是开空（ThetaHarvest 用 SELL 开空）
        if side.upper() == "SELL":
            # 退场：source_signal_id = "exit-{original_order_id}"
            if source_signal_id.startswith("exit-"):
                original_oid = source_signal_id[5:]  # 去掉 "exit-" 前缀
                if original_oid in self.positions:
                    del self.positions[original_oid]
                else:
                    # 带腿编号后缀的 fallback（如 "exit-live-xxx-L0"）
                    for pid in list(self.positions.keys()):
                        if pid in original_oid:
                            del self.positions[pid]
            else:
                # SELL without exit- prefix → 开空仓（ThetaHarvest 等策略）
                strike = instrument.get("strike", "0")
                option_right = instrument.get("option_right", "")
                self.positions[oid] = {
                    "order_id": oid,
                    "symbol": symbol,
                    "strategy_id": strategy_id,
                    "side": side,
                    "entry_price": float(price),
                    "quantity": int(float(qty)),
                    "strike": strike,
                    "option_right": option_right,
                    "entry_time": fill.get("filled_at", now_iso()),
                    "pnl_pct": 0.0,
                }
        else:
            # 开仓
            strike = instrument.get("strike", "0")
            option_right = instrument.get("option_right", "")
            self.positions[oid] = {
                "order_id": oid,
                "symbol": symbol,
                "strategy_id": strategy_id,
                "side": side,
                "entry_price": float(price),
                "quantity": int(float(qty)),
                "strike": strike,
                "option_right": option_right,
                "entry_time": fill.get("filled_at", now_iso()),
                "pnl_pct": 0.0,
            }

    def update_prices(self, quotes: dict[str, float]):
        """根据最新报价更新浮盈"""
        for pos in self.positions.values():
            sym = pos["symbol"]
            if sym in quotes:
                current = quotes[sym]
                entry = pos["entry_price"]
                if entry > 0:
                    pos["pnl_pct"] = round((current - entry) / entry * 100, 2)

    def snapshot(self) -> list[dict]:
        """返回当前持仓快照，自动过滤已过期期权"""
        today_str = datetime.now(timezone.utc).strftime("%y%m%d")  # 如 "260602"
        active = []
        for pos in self.positions.values():
            sym = pos.get("symbol", "")
            # 过滤过期期权：symbol 含 YYMMDD 日期，小于今天 = 已过期
            # 如 QQQ260601C747000.US → 260601 < 260602 → 跳过
            import re
            m = re.search(r'(\d{6})[CP]', sym)
            if m:
                expiry = m.group(1)
                if expiry < today_str:
                    continue  # 过期，跳过
            active.append(pos)
        return active


async def main():
    nc = await nats.connect(NATS_URL)
    book = PositionBook()
    quotes_cache: dict[str, float] = {}

    # ── 订阅成交 ──
    async def on_fill(msg):
        try:
            fill = json.loads(msg.data.decode())
        except json.JSONDecodeError:
            return
        book.upsert(fill)
        await publish_snapshot(nc, book)

    # ── 订阅行情（更新浮盈）──
    async def on_quote(msg):
        try:
            data = json.loads(msg.data.decode())
        except json.JSONDecodeError:
            return
        symbol = data.get("symbol", "")
        price = data.get("last_done")
        if symbol and price is not None:
            quotes_cache[symbol] = float(price)

    await nc.subscribe("fill.option.qqq", cb=on_fill)
    await nc.subscribe("quote.option.>", cb=on_quote)
    print("[position_tracker] 订阅 fill + quote，就绪")

    # ── 每 5 秒推一次含浮盈的持仓快照 ──
    async def publish_loop():
        while True:
            await asyncio.sleep(5)
            book.update_prices(quotes_cache)
            await publish_snapshot(nc, book)

    await publish_loop()


async def publish_snapshot(nc, book: PositionBook):
    positions = book.snapshot()
    msg = {
        "positions": positions,
        "count": len(positions),
        "timestamp": now_iso(),
    }
    await nc.publish("position.option.qqq", json.dumps(msg).encode())


if __name__ == "__main__":
    asyncio.run(main())
