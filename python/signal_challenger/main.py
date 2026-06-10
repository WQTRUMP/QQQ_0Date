#!/usr/bin/env python3
"""
Signal Challenger — 信号快杠精
===============================
实时拦截策略信号，过滤脏数据/过期报价/IV异常跳变。
纯内存计算，无API调用无数据库，单次检查 <1ms。

数据流:
  raw.signal.option.* → Challenger → signal.option.* (给风控)
                                    → challenge.rejected.* (毙掉的信号)

仅检查两件事（QQQ流动性足够大，不需检查价差/成交量）:
  ① 报价新鲜度 — 最近一次 quote 距今超过阈值？
  ② IV 异常跳变 — 当前 IV 和上一次报价IV差异过大？

用法:
  python python/signal_challenger/main.py
"""

import asyncio
import json
import os
import time

import nats

# ── 配置 ──────────────────────────────────────────────

NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
RAW_SIGNAL_SUBJECT = os.getenv("RAW_SIGNAL_SUBJECT", "raw.signal.option.>")
QUOTE_SUBJECT = os.getenv("QUOTE_SUBJECT", "quote.option.>")
MAX_QUOTE_AGE_MS = int(os.getenv("CHALLENGE_MAX_QUOTE_AGE_MS", "500"))
MAX_IV_JUMP_PCT = float(os.getenv("CHALLENGE_MAX_IV_JUMP_PCT", "0.30"))


# ── 全局缓存 ──────────────────────────────────────────

# symbol → 最近一次 quote 到达的 UNIX 时间戳
latest_quote_ts: dict[str, float] = {}
# symbol → 最近一次报价的 IV
latest_iv: dict[str, float] = {}


# ── 主逻辑 ────────────────────────────────────────────

async def main():
    nc = await nats.connect(NATS_URL)

    # ── 行情监听（仅更新缓存） ──
    async def on_quote(msg):
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        symbol = data.get("symbol", "")
        if not symbol:
            return
        latest_quote_ts[symbol] = time.time()
        iv = data.get("implied_volatility") or data.get("iv")
        if iv is not None:
            latest_iv[symbol] = float(iv)

    await nc.subscribe(QUOTE_SUBJECT, cb=on_quote)
    print(f"[challenger] 行情监听: {QUOTE_SUBJECT}")

    # ── 信号挑战 ──
    async def challenge(msg):
        start_ns = time.perf_counter_ns()

        try:
            signal = json.loads(msg.data)
        except json.JSONDecodeError:
            return

        signal_id = signal.get("signal_id", "?")
        instrument = signal.get("instrument", {})
        symbol = instrument.get("symbol", "")

        # ① 报价新鲜度 — 期权的报价不像正股那么快，用 QQQ.US 新鲜度兜底
        # QQQ 正股每秒多笔成交，新鲜度 > 500ms 说明连接断了
        last_ts = latest_quote_ts.get("QQQ.US", 0)
        # 如果正股行情正常，期权信号也认为有效（信号来自 Greeks 计算，不依赖个股期权tick）
        if last_ts == 0:
            # 兜底：尝试信号自身的 symbol
            last_ts = latest_quote_ts.get(symbol, 0)
        if last_ts == 0:
            # 冷启动兜底：取任意 quote 的最大时间戳（刚启动时 QQQ.US 可能还没缓存）
            if latest_quote_ts:
                # 兜底：取所有已缓存 symbol 的最新时间戳；空字典时返回 0
                last_ts = max(latest_quote_ts.values()) if latest_quote_ts else 0

        age_ms = (time.time() - last_ts) * 1000 if last_ts > 0 else float("inf")

        if last_ts == 0 or age_ms > MAX_QUOTE_AGE_MS:
            safe_age = round(age_ms) if age_ms != float("inf") else 999999
            elapsed_ns = time.perf_counter_ns() - start_ns
            print(
                f"[challenger] ❌ 过期 {signal_id} "
                f"({safe_age}ms>{MAX_QUOTE_AGE_MS}ms) "
                f"耗时:{elapsed_ns/1000:.0f}μs → 毙掉"
            )
            await nc.publish(
                "challenge.rejected.option",
                json.dumps({
                    "signal_id": signal_id,
                    "reason": "stale_quote",
                    "age_ms": safe_age,
                }).encode(),
            )
            return

        # ② IV 异常跳变
        current_iv = signal.get("implied_volatility") or signal.get("iv")
        if current_iv is not None and symbol in latest_iv:
            prev = latest_iv[symbol]
            if prev > 0:
                jump = abs(float(current_iv) - prev) / prev
                if jump > MAX_IV_JUMP_PCT:
                    elapsed_ns = time.perf_counter_ns() - start_ns
                    print(
                        f"[challenger] ❌ IV跳变 {signal_id} "
                        f"({jump*100:.1f}%>{MAX_IV_JUMP_PCT*100:.0f}%) "
                        f"耗时:{elapsed_ns/1000:.0f}μs → 毙掉"
                    )
                    await nc.publish(
                        "challenge.rejected.option",
                        json.dumps({
                            "signal_id": signal_id,
                            "reason": "iv_spike",
                            "jump_pct": round(jump * 100, 1),
                        }).encode(),
                    )
                    return

        # ── 通过 → 转发 ──
        symbol_key = "".join(ch for ch in symbol.lower() if ch.isalnum())
        asset_key = instrument.get("asset_class", "option").lower()
        out_subject = f"signal.{asset_key}.{symbol_key}"
        await nc.publish(out_subject, msg.data)

        elapsed_ns = time.perf_counter_ns() - start_ns
        print(
            f"[challenger] ✅ {signal_id} → {out_subject} "
            f"({elapsed_ns/1000:.0f}μs)"
        )

    await nc.subscribe(RAW_SIGNAL_SUBJECT, cb=challenge)
    print(f"[challenger] 信号拦截: {RAW_SIGNAL_SUBJECT} → signal.option.*")
    print("[challenger] ✅ 就绪")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())