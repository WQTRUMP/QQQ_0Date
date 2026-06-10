#!/usr/bin/env python3
"""
Trade Logger — QQQ 交易事件收集器
=================================
旁路监听 NATS 所有交易事件，结构化写入 SQLite。
不改动任何现有服务代码。

订阅主题:
  signal.option.*        → 策略信号
  risk.option.*          → 风控决策
  order.intent.option.*  → 订单意图
  order.ack.option.*     → 订单确认
  fill.option.qqq        → 成交
  (exit orders 走 order.intent.option.*)

SQLite 表:
  trade_events — 原始事件（一行一个事件）
  trade_summary — 交易汇总（按 signal_id 聚合：入场/离场/PnL）

用法:
  python python/trade_logger/main.py
"""

import asyncio
import json
import os
import sqlite3
import threading
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Optional

import nats

# ── 配置 ──────────────────────────────────────────────

NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
LOG_DIR = Path(__file__).parent.parent.parent / "logs"
DB_PATH = LOG_DIR / "trades.db"

SUBJECTS = [
    "signal.option.>",       # 策略信号
    "risk.option.>",         # 风控报告
    "order.intent.option.>", # 订单意图 (含开仓+平仓)
    "order.ack.option.>",    # 订单确认
    "fill.option.qqq",       # 成交
]

# ── SQLite 初始化 ────────────────────────────────────

def init_db():
    db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("""
        CREATE TABLE IF NOT EXISTS trade_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            event_type TEXT NOT NULL,
            session_date TEXT NOT NULL,
            subject TEXT NOT NULL,
            signal_id TEXT,
            intent_id TEXT,
            order_id TEXT,
            strategy TEXT,
            symbol TEXT,
            instrument TEXT,
            side TEXT,
            quantity REAL,
            price REAL,
            confidence REAL,
            decision TEXT,
            exit_reason TEXT,
            raw TEXT NOT NULL
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_events_date ON trade_events(session_date, event_type)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_events_signal ON trade_events(signal_id)")
    db.execute("""
        CREATE TABLE IF NOT EXISTS trade_summary (
            signal_id TEXT PRIMARY KEY,
            session_date TEXT NOT NULL,
            strategy TEXT,
            symbol TEXT,
            signal_action TEXT,
            confidence REAL,
            risk_decision TEXT,
            entry_ts TEXT,
            entry_price REAL,
            entry_qty REAL,
            exit_ts TEXT,
            exit_price REAL,
            exit_reason TEXT,
            pnl REAL,
            pnl_pct REAL,
            held_minutes INTEGER
        )
    """)
    db.commit()
    return db


# ── 事件插入 ─────────────────────────────────────────

def insert_event(db: sqlite3.Connection, subject: str, payload: bytes):
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return

    now = datetime.now(timezone.utc)
    ts = now.isoformat().replace("+00:00", "Z")
    session_date = now.strftime("%Y-%m-%d")

    # 事件类型
    if "signal" in subject:
        event_type = "signal"
    elif "risk" in subject:
        event_type = "risk"
    elif "order.intent" in subject:
        event_type = "order_intent"
    elif "order.ack" in subject:
        event_type = "order_ack"
    elif "fill" in subject:
        event_type = "fill"
    else:
        event_type = "unknown"

    signal_id = data.get("signal_id") or data.get("source_signal_id") or ""
    intent_id = data.get("intent_id") or ""
    order_id = data.get("order_id") or ""
    instrument = data.get("instrument", {})
    strategy = data.get("strategy_id") or ""

    symbol = instrument.get("symbol", "") if isinstance(instrument, dict) else ""
    side = data.get("side") or data.get("action") or ""
    quantity = _to_float(data.get("quantity"))
    price = _to_float(data.get("price") or data.get("reference_price") or data.get("entry_price"))
    confidence = _to_float(data.get("confidence"))
    decision = ""
    exit_reason = ""

    if event_type == "risk":
        decision = data.get("decision") or ""
        exit_reason = data.get("reason") or ""

    db.execute(
        """INSERT INTO trade_events
           (ts, event_type, session_date, subject, signal_id, intent_id, order_id,
            strategy, symbol, instrument, side, quantity, price, confidence,
            decision, exit_reason, raw)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            ts, event_type, session_date, subject, signal_id, intent_id, order_id,
            strategy, symbol, json.dumps(instrument, ensure_ascii=False),
            side, quantity, price, confidence, decision, exit_reason,
            payload.decode(errors="replace"),
        ),
    )
    db.commit()

    # ── 更新交易汇总 ──
    _update_summary(db, event_type, data, signal_id, symbol, strategy, side, quantity, price, ts)


def _to_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _update_summary(db, event_type, data, signal_id, symbol, strategy, side, qty, price, ts):
    """根据事件类型更新 trade_summary 表"""
    if not signal_id:
        return

    if event_type == "signal":
        # 新建或更新信号记录
        db.execute(
            """INSERT OR REPLACE INTO trade_summary
               (signal_id, session_date, strategy, symbol, signal_action, confidence, risk_decision)
               VALUES (?, date('now'), ?, ?, ?, ?, 'pending')""",
            (
                signal_id,
                strategy,
                symbol,
                data.get("action") or "",
                _to_float(data.get("confidence")),
            ),
        )
        db.commit()

    elif event_type == "risk":
        decision = data.get("decision") or ""
        reason = data.get("reason") or ""
        db.execute(
            "UPDATE trade_summary SET risk_decision=?, exit_reason=COALESCE(exit_reason, ?) WHERE signal_id=? AND exit_ts IS NULL",
            (decision, reason, signal_id),
        )
        db.commit()

    elif event_type == "fill":
        # 区分开仓成交 vs 平仓成交
        is_exit = data.get("is_exit", False) or "exit" in signal_id.lower()
        if is_exit:
            canonical_signal_id = _resolve_summary_signal_id(db, signal_id)
            # 平仓成交 → 更新离场信息
            db.execute(
                """UPDATE trade_summary SET
                   exit_ts=?, exit_price=?, entry_qty=COALESCE(entry_qty, ?)
                   WHERE signal_id=? AND exit_ts IS NULL""",
                (ts, price, qty, canonical_signal_id),
            )
            db.commit()
            # 计算 PnL（如果已有 entry_price）
            _calc_pnl(db, canonical_signal_id)
        else:
            # 开仓成交 → 更新入场信息
            db.execute(
                """UPDATE trade_summary SET
                   entry_ts=?, entry_price=?, entry_qty=?, symbol=COALESCE(symbol, ?)
                   WHERE signal_id=? AND entry_ts IS NULL""",
                (ts, price, qty, symbol, signal_id),
            )
            db.commit()

    elif event_type == "order_intent":
        # 检查是否是平仓单
        if "exit" in signal_id.lower():
            reason = data.get("reason") or ""
            canonical_signal_id = _resolve_summary_signal_id(db, signal_id)
            db.execute(
                "UPDATE trade_summary SET exit_reason=? WHERE signal_id=? AND exit_reason IS NULL",
                (reason, canonical_signal_id),
            )
            db.commit()


def _resolve_summary_signal_id(db, signal_id: str) -> str:
    """将 exit-* 信号归一到开仓 summary 主键。"""
    if not signal_id or not signal_id.startswith("exit-"):
        return signal_id

    base_signal_id = signal_id[5:]
    candidates = [base_signal_id]

    if "-" in base_signal_id:
        prefix_candidate = base_signal_id.rsplit("-", 1)[0]
        if prefix_candidate != base_signal_id:
            candidates.append(prefix_candidate)

    for candidate in candidates:
        row = db.execute(
            "SELECT signal_id FROM trade_summary WHERE signal_id=?",
            (candidate,),
        ).fetchone()
        if row:
            return row[0]

    prefix = candidates[-1] + "%"
    row = db.execute(
        "SELECT signal_id FROM trade_summary WHERE signal_id LIKE ? ORDER BY signal_id LIMIT 1",
        (prefix,),
    ).fetchone()
    if row:
        return row[0]

    return base_signal_id


def _calc_pnl(db, signal_id: str):
    """计算已完成的交易盈亏"""
    cur = db.execute(
        "SELECT signal_id, signal_action, entry_price, exit_price, entry_qty FROM trade_summary WHERE signal_id=? AND entry_price IS NOT NULL AND exit_price IS NOT NULL",
        (signal_id,),
    )
    # 根据方向计算 PnL（期权合约乘数 ×100）
    row = cur.fetchone()
    if not row:
        return
    sid, signal_action, entry, exit_px, qty = row
    if not entry or not exit_px or not qty:
        return

    is_sell = str(signal_action or "").upper() == "SELL"
    if is_sell:
        pnl = (entry - exit_px) * qty * 100  # 卖开仓 → 跌赚
        pnl_pct = ((entry - exit_px) / entry * 100) if entry != 0 else 0
    else:
        pnl = (exit_px - entry) * qty * 100  # 买开仓 → 涨赚
        pnl_pct = ((exit_px - entry) / entry * 100) if entry != 0 else 0

    # 平仓的 entry_ts 和 exit_ts
    cur2 = db.execute(
        "SELECT entry_ts, exit_ts FROM trade_summary WHERE signal_id=?",
        (signal_id,),
    )
    row2 = cur2.fetchone()
    held_minutes = None
    if row2 and row2[0] and row2[1]:
        try:
            t1 = datetime.fromisoformat(row2[0].replace("Z", "+00:00"))
            t2 = datetime.fromisoformat(row2[1].replace("Z", "+00:00"))
            held_minutes = int((t2 - t1).total_seconds() / 60)
        except (ValueError, TypeError):
            pass

    db.execute(
        "UPDATE trade_summary SET pnl=?, pnl_pct=?, held_minutes=? WHERE signal_id=?",
        (round(pnl, 2), round(pnl_pct, 2), held_minutes, sid),
    )
    db.commit()
    print(f"[logger] 💰 PnL: {sid}  ${pnl:+.2f} ({pnl_pct:+.1f}%)  held={held_minutes}min")


# ── 主逻辑 ────────────────────────────────────────────

async def main():
    db = init_db()
    print(f"[logger] SQLite: {DB_PATH}")
    print(f"[logger] 连接 NATS: {NATS_URL}")

    nc = await nats.connect(NATS_URL)

    async def handler(msg):
        insert_event(db, msg.subject, msg.data)

    for subj in SUBJECTS:
        await nc.subscribe(subj, cb=handler)
        print(f"[logger] 订阅: {subj}")

    print("[logger] ✅ 交易日志收集器就绪，等待事件...")

    stop = asyncio.Event()
    await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
