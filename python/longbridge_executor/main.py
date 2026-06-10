#!/usr/bin/env python3
"""
Longbridge Executor — QQQ 期权实盘执行网关
===========================================
替换原 Paper Execution Gateway（services/execution_gateway）

数据流:
  NATS order.intent.* → Longbridge submit_order → NATS order.ack.*
  Longbridge Trade Push   → NATS fill.*

依赖: pip install longbridge nats-py python-dotenv
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

import nats
import redis.asyncio as aioredis
from longbridge.openapi import (
    Config, TradeContext, OrderType, OrderSide, TimeInForceType,
    PushOrderChanged, TopicType, OAuthBuilder,
)


# ── 环境变量加载 ────────────────────────────────────────

def load_env():
    env_file = Path(__file__).parent.parent.parent / ".env.longbridge"
    if not env_file.exists():
        print(f"[WARN] 未找到 {env_file}")
        return
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and val and key not in os.environ:
                os.environ[key] = val


load_env()

NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
ORDER_INTENT_SUBJECT = os.getenv("ORDER_INTENT_SUBJECT", "order.intent.option.>")
EXECUTION_MODE = os.getenv("EXECUTION_MODE", "paper")  # "live" 或 "paper"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ── Longbridge 认证 ────────────────────────────────────

def build_config() -> Config:
    client_id = os.getenv("LONGBRIDGE_OAUTH_CLIENT_ID", "")
    if client_id:
        oauth = OAuthBuilder(client_id).build(
            lambda url: print(f"[auth] 请打开浏览器授权:\n{url}")
        )
        return Config.from_oauth(oauth)
    return Config.from_apikey_env()


# ── 期权 symbol 转换 ───────────────────────────────────

def qqq_option_symbol(instrument: dict) -> str:
    """将内部 Instrument JSON 转为 Longbridge option symbol

    两种输入格式均兼容：
      A) symbol 已是完整长桥格式: "QQQ260601C493000.US" → 直接用
      B) 拆分格式: {symbol:"QQQ", strike:"493", option_right:"CALL", expiry:"20260601"}
    """
    sym = instrument.get("symbol", "QQQ")

    # 已经是长桥期权格式 → 直接用
    if len(sym) > 10 and ".US" in sym.upper() and any(c in sym.upper() for c in ("C", "P")):
        return sym.upper()

    expiry = instrument.get("expiry", "")
    strike_str = str(instrument.get("strike", ""))
    right = instrument.get("option_right", "")

    expiry_short = expiry[2:] if len(expiry) >= 8 else expiry
    right_char = "C" if str(right).upper() == "CALL" else "P"
    strike_int = int(float(strike_str) * 1000) if strike_str else 0

    return f"{sym.upper()}{expiry_short}{right_char}{strike_int:06d}.US"


# ── 订单意图 → Longbridge 参数映射 ─────────────────────

def map_order_type(ot: str) -> OrderType:
    mapping = {
        "MARKET": OrderType.MO,
        "LIMIT": OrderType.LO,
    }
    return mapping.get(ot.upper(), OrderType.MO)  # 默认市价单


def map_order_side(side: str) -> OrderSide:
    return OrderSide.Buy if side.upper() == "BUY" else OrderSide.Sell


# ── 期权 symbol 解析 ───────────────────────────────────

def parse_option_symbol(sym: str) -> Optional[dict]:
    """解析 QQQ260602C745000.US → {underlying, expiry, right, strike}"""
    sym = sym.upper().replace(".US", "")
    # 找 C 或 P 的位置
    for i, ch in enumerate(sym):
        if ch in ("C", "P") and i >= 9:  # QQQ + 6位日期 = 9
            underlying = sym[:i-6]
            expiry = sym[i-6:i]
            right = ch  # C or P
            try:
                strike = float(sym[i+1:]) / 1000.0
            except (ValueError, IndexError):
                return None
            return {"underlying": underlying, "expiry": expiry, "right": right, "strike": strike}
    return None


def build_protection_symbol(short_sym: str, wing_width: float) -> Optional[str]:
    """从卖腿 symbol 推导保护腿 symbol"""
    parsed = parse_option_symbol(short_sym)
    if not parsed:
        return None
    underlying = parsed["underlying"]
    expiry = parsed["expiry"]
    right = parsed["right"]
    strike = parsed["strike"]
    # CALL credit spread: 保护腿 = buy higher strike CALL (strike + width)
    # PUT credit spread: 保护腿 = buy lower strike PUT (strike - width)
    wing_strike = strike + wing_width if right == "C" else strike - wing_width
    wing_strike_int = int(round(wing_strike * 1000))
    return f"{underlying}{expiry}{right}{wing_strike_int:06d}.US"


# ── 断腿自愈 ───────────────────────────────────────────

async def repair_orphan_spreads(trade_ctx, nc, wing_width: float):
    """启动时扫描长桥持仓，补缺失信用价差保护腿"""
    print("[executor] 🔍 扫描持仓，检测断腿（孤儿卖腿）...")
    try:
        positions_resp = trade_ctx.stock_positions()
    except Exception as e:
        print(f"[executor] ⚠️ 持仓扫描失败: {e}")
        return

    # stock_positions() 返回 StockPositionsResponse，遍历 channels → positions
    held: dict[str, dict] = {}
    for channel in (positions_resp.channels or []):
        for pos in (channel.positions or []):
            sym = getattr(pos, 'symbol', '')
            qty = int(getattr(pos, 'quantity', 0) or 0)
            if qty <= 0 or not sym:
                continue
            held[sym] = {"symbol": sym, "quantity": qty}

    if not held:
        print("[executor] ✅ 无持仓，无需修复")
        return

    # 找孤儿卖腿：查双向（±3pt），任一方向有配对就跳过
    orphans = []
    for sym, info in held.items():
        parsed = parse_option_symbol(sym)
        if not parsed:
            continue
        # 正向：找保护腿
        protection_sym = build_protection_symbol(sym, wing_width)
        # 反向：找配对卖腿（当前持仓可能是保护腿）
        reverse_sym = build_protection_symbol(sym, -wing_width) if wing_width > 0 else None
        paired = (protection_sym and protection_sym in held) or (reverse_sym and reverse_sym in held)
        if not paired and protection_sym:
            orphans.append((sym, info, protection_sym or sym, parsed))

    if not orphans:
        print(f"[executor] ✅ 持仓完整 ({len(held)}个)，无断腿")
        return

    print(f"[executor] 🔴 发现 {len(orphans)} 个断腿！补保护腿...")
    for short_sym, info, protection_sym, parsed in orphans:
        qty = info["quantity"]
        print(f"[executor] 🔧 补保护腿: {short_sym} → BUY {protection_sym} x{qty}")
        try:
            resp = trade_ctx.submit_order(
                symbol=protection_sym,
                order_type=OrderType.MO,       # 市价单
                side=OrderSide.Buy,
                submitted_quantity=Decimal(qty),
                time_in_force=TimeInForceType.Day,
            )
            oid = getattr(resp, 'order_id', '?')
            print(f"[executor] ✅ 保护腿已补: {protection_sym} x{qty} → {oid}")
            # 发 fill 到 NATS 让 Risk Engine / PositionTracker 感知
            fill = {
                "order_id": oid,
                "instrument": {
                    "symbol": protection_sym,
                    "strike": str(parsed["strike"] + wing_width if parsed["right"] == "C" else parsed["strike"] - wing_width),
                    "option_right": "CALL" if parsed["right"] == "C" else "PUT",
                    "expiry": f"20{parsed['expiry']}",
                },
                "source_signal_id": f"repair-orphan-{short_sym}",
                "side": "BUY",
                "quantity": str(qty),
                "price": "0",
                "filled_at": now_iso(),
                "leg": 2,
                "total_legs": 2,
            }
            await nc.publish("fill.option.qqq", json.dumps(fill).encode())
        except Exception as e:
            print(f"[executor] ❌ 补保护腿失败 {protection_sym}: {e}")


# ── 仓位对账 ───────────────────────────────────────────

async def reconcile_positions_loop(trade_ctx, nc):
    """每 30s 拉长桥真实持仓，与本地 PositionTracker 对账"""
    await asyncio.sleep(30)  # 启动后先等 30s 让 PositionTracker 初始化
    local_positions: list[dict] = []

    async def on_local_positions(msg):
        nonlocal local_positions
        try:
            data = json.loads(msg.data.decode())
            local_positions = data.get("positions", [])
        except Exception:
            pass

    await nc.subscribe("position.option.qqq", cb=on_local_positions)

    while True:
        await asyncio.sleep(30)
        try:
            broker_positions = trade_ctx.stock_positions()
        except Exception as e:
            print(f"[executor] ⚠️ 对账拉取失败: {e}")
            continue

        # 券商持仓: symbol → quantity（仅 QQQ 相关，排除其他品种如 TSLA）
        broker_book: dict[str, int] = {}
        for channel in (broker_positions.channels or []):
            for pos in (channel.positions or []):
                sym = getattr(pos, 'symbol', '')
                qty = int(getattr(pos, 'quantity', 0) or 0)
                if qty > 0 and sym and 'QQQ' in sym.upper():
                    broker_book[sym] = qty

        # 本地持仓: symbol → quantity（仅 QQQ 相关）
        local_book: dict[str, int] = {}
        for p in local_positions:
            sym = p.get("symbol", "")
            qty = int(p.get("quantity", 0) or 0)
            if qty > 0 and sym and 'QQQ' in sym.upper():
                local_book[sym] = local_book.get(sym, 0) + qty

        # Diff
        broker_syms = set(broker_book.keys())
        local_syms = set(local_book.keys())

        only_broker = broker_syms - local_syms
        only_local = local_syms - broker_syms

        if only_broker:
            details = ", ".join(f"{s}({broker_book[s]})" for s in only_broker)
            print(f"[executor] ⚠️ 券商多了 {len(only_broker)} 个持仓: {details}")
        if only_local:
            details = ", ".join(f"{s}({local_book[s]})" for s in only_local)
            print(f"[executor] ⚠️ 本地多了 {len(only_local)} 个幽灵持仓: {details}")

        if not only_broker and not only_local:
            print(f"[executor] ✅ 仓位对账一致 (券商{len(broker_book)} 本地{len(local_book)})")

async def main():
    mode = EXECUTION_MODE
    print(f"[executor] 模式: {mode}")
    print(f"[executor] 连接 NATS: {NATS_URL}")

    nc = await nats.connect(NATS_URL)

    # 连接 Redis（持久化去重）
    redis_conn = aioredis.from_url(REDIS_URL)
    await redis_conn.ping()
    print(f"[executor] ✅ Redis 已连接: {REDIS_URL}")

    # 连接 Longbridge（仅实盘需要）
    trade_ctx: Optional[TradeContext] = None
    if mode == "live":
        print("[executor] 连接 Longbridge Trade...")
        config = build_config()
        trade_ctx = TradeContext(config)

        # 订阅交易推送（成交通知）
        # 订单映射: order_id → intent context (用于 fill 回填 instrument)
        order_map: dict[str, dict] = {}

        # ── 信用价差下单状态机（WS 推送 → asyncio.Event 驱动）──
        # order_id → (event, final_status)
        _order_events: dict[str, tuple[asyncio.Event, str]] = {}
        loop = asyncio.get_running_loop()

        def on_order_changed(event: PushOrderChanged):
            try:
                status_raw = getattr(event, 'status', '')
                oid = getattr(event, 'order_id', '')
                status = str(status_raw)  # OrderStatus 枚举 → 字符串
                print(f"[executor] 🔔 raw callback: oid={oid} status={status}")

                # ── 触发等待中的协程（线程安全：用 call_soon_threadsafe）──
                # status 格式: "OrderStatus.Filled" / "OrderStatus.Rejected" 等
                if 'Filled' in status or 'Rejected' in status or 'Cancelled' in status or 'Expired' in status:
                    entry = _order_events.get(oid)
                    print(f"[executor] 🔔 回调 oid={oid} status={status} entry={'found' if entry else 'MISSING'} keys={list(_order_events.keys())[:3]}")
                    if entry:
                        evt, _ = entry
                        print(f"[executor] 🔔 准备触发 evt.set() is_set={evt.is_set()}")
                        _order_events[oid] = (evt, status)
                        loop.call_soon_threadsafe(evt.set)
                        print(f"[executor] 🔔 evt.set() 已调度")
            except Exception as e:
                print(f"[executor] ❌ 回调异常: {e}")

            # ── 成交发布 fill（线程安全：run_coroutine_threadsafe 确保在事件循环执行）──
            if 'Filled' in status:
                ctx = order_map.get(oid, {})
                fill = {
                    "order_id": oid,
                    "instrument": ctx.get("instrument", {}),
                    "source_signal_id": ctx.get("source_signal_id", ""),
                    "side": ctx.get("side", getattr(event, 'side', '')),
                    "quantity": str(getattr(event, 'executed_quantity', ctx.get("quantity", "1"))),
                    "price": str(getattr(event, 'executed_price', '0')),
                    "filled_at": now_iso(),
                    "leg": ctx.get("leg", 1),
                    "total_legs": ctx.get("total_legs", 1),
                    "is_exit": ctx.get("exit_reason") is not None,
                }
                asyncio.run_coroutine_threadsafe(
                    nc.publish("fill.option.qqq", json.dumps(fill).encode()),
                    loop,
                )

        trade_ctx.set_on_order_changed(on_order_changed)
        trade_ctx.subscribe([TopicType.Private])
        print("[executor] ✅ Longbridge 交易推送已订阅")

        # ── 启动断腿自愈：扫长桥持仓，补缺失保护腿 ──
        # ⚠️ SPREAD_WING_WIDTH 必须与策略引擎的价差宽度保持一致。
        #    如果策略侧使用不同的 wing width（如 ThetaHarvest SPREAD_WING_WIDTH=3.0），
        #    修改此处环境变量时必须同步更新策略配置，否则补腿行权价会错位。
        SPREAD_WING_WIDTH = float(os.getenv("SPREAD_WING_WIDTH", "3.0"))
        await repair_orphan_spreads(trade_ctx, nc, SPREAD_WING_WIDTH)

        # ── 每 30s 仓位对账：长桥真实持仓 vs 本地 PositionTracker ──
        asyncio.create_task(reconcile_positions_loop(trade_ctx, nc))

    # 按 NATS 订单意图
    processed_intents: set[str] = set()

    async def handle_order_intent(msg):
        payload = msg.data.decode()
        try:
            intent = json.loads(payload)
        except json.JSONDecodeError:
            print(f"[executor] 无法解析订单意图: {payload[:100]}")
            return
        intent_id = intent.get("intent_id", "")

        # ── 去重：内存 + Redis 双重检查 ──
        if intent_id in processed_intents:
            print(f"[executor] 🔁 重复订单（内存） {intent_id}，跳过")
            return
        redis_key = f"executor:intent:{intent_id}"
        if await redis_conn.exists(redis_key):
            print(f"[executor] 🔁 重复订单（Redis） {intent_id}，跳过")
            return
        processed_intents.add(intent_id)
        # 持久化到 Redis，24h TTL
        await redis_conn.setex(redis_key, 86400, "1")

        instrument = intent.get("instrument", {})
        spread_wing = intent.get("spread_wing")  # 信用价差保护腿
        order_id = f"{'live' if mode == 'live' else 'paper'}-{int(time.time() * 1000)}"

        legs = [(instrument, intent.get("side", "BUY"))]
        if spread_wing:
            # 保护腿：与主腿方向相反
            wing_side = "SELL" if intent.get("side", "BUY") == "BUY" else "BUY"
            # ── 信用价差：先买保护腿再卖主腿，避免裸卖被拒 ──
            # 主腿 SELL → 保护腿 BUY → 先下 BUY 保护腿，再下 SELL
            # 主腿 BUY  → 保护腿 SELL → 保持原顺序
            if intent.get("side", "BUY") == "SELL":
                legs = [(spread_wing, wing_side), (instrument, intent.get("side", "BUY"))]  # BUY 先
                print(f"[executor] 信用价差: 先买保护腿{wing_side} → 再卖主腿")
            else:
                legs.append((spread_wing, wing_side))
                print(f"[executor] 信用价差: 主腿{legs[0][1]} + 保护腿{wing_side}")

        print(f"[executor] 收到订单: {intent_id} ({len(legs)}腿)")

        qty = Decimal(str(intent.get("quantity", "0")))
        order_type_str = intent.get("order_type", "MARKET")
        limit_price = intent.get("limit_price")
        is_spread = len(legs) >= 2 and mode == "live" and trade_ctx

        all_ok = True
        submitted_orders: list[str] = []

        for i, (inst, side_str) in enumerate(legs):
            leg_order_id = f"{order_id}-L{i}" if len(legs) > 1 else order_id

            # ── 价差：腿0下单前预注册成交事件（腿1需要等腿0成交）──
            if is_spread and i == 0:
                # 下单后立即用真实 order_id 注册，PushOrderChanged 会触发
                _spread_leg0_evt = asyncio.Event()

            # ── 价差：等腿0成交后再下腿1 ──
            if is_spread and i == 1:
                # 等待腿0的 WS 确认（腿0下单时已注册 _order_events）
                leg0_oid = submitted_orders[0] if submitted_orders else None
                if leg0_oid:
                    # 复用腿0已注册的 event，不要新建（否则覆盖导致丢事件）
                    entry = _order_events.get(leg0_oid)
                    if entry:
                        evt, _ = entry
                    else:
                        evt = asyncio.Event()
                        _order_events[leg0_oid] = (evt, "")
                    print(f"[executor] ⏳ 等待腿0 {leg0_oid} 成交确认…")
                    try:
                        await asyncio.wait_for(evt.wait(), timeout=5.0)
                        _, final_status = _order_events.get(leg0_oid, (evt, "Timeout"))
                        if 'Filled' not in final_status:
                            print(f"[executor] ❌ 腿0状态={final_status}，取消腿1下单")
                            all_ok = False
                            ack = {
                                "order_id": leg_order_id,
                                "intent_id": intent_id,
                                "status": "CANCELLED",
                                "reason": f"腿0{final_status}，腿1取消",
                                "leg": i + 1,
                                "total_legs": len(legs),
                                "created_at": now_iso(),
                            }
                            await nc.publish("order.ack.option.qqq", json.dumps(ack).encode())
                            break
                        print(f"[executor] ✅ 腿0已成交，继续下腿1")
                    except asyncio.TimeoutError:
                        # 超时后重新检查腿0状态 —— WS 回调可能在超时边界后到达
                        entry = _order_events.get(leg0_oid)
                        if entry:
                            _, final_status = entry
                            if 'Filled' in final_status:
                                # 腿0在超时后到但已成交 → 继续下腿1
                                print(f"[executor] ⏰ 腿0超时但已成交({final_status})，继续下腿1")
                            else:
                                print(f"[executor] ⏰ 腿0确认超时5s(状态={final_status})，取消腿1")
                                all_ok = False
                                _order_events.pop(leg0_oid, None)
                                ack = {
                                    "order_id": leg_order_id,
                                    "intent_id": intent_id,
                                    "status": "CANCELLED",
                                    "reason": f"腿0确认超时(状态={final_status})，腿1取消",
                                    "leg": i + 1,
                                    "total_legs": len(legs),
                                    "created_at": now_iso(),
                                }
                                await nc.publish("order.ack.option.qqq", json.dumps(ack).encode())
                                break
                        else:
                            print(f"[executor] ⏰ 腿0确认超时5s(事件已丢失)，取消腿1")
                            all_ok = False
                            ack = {
                                "order_id": leg_order_id,
                                "intent_id": intent_id,
                                "status": "CANCELLED",
                                "reason": "腿0确认超时，腿1取消",
                                "leg": i + 1,
                                "total_legs": len(legs),
                                "created_at": now_iso(),
                            }
                            await nc.publish("order.ack.option.qqq", json.dumps(ack).encode())
                            break
                    finally:
                        _order_events.pop(leg0_oid, None)
                else:
                    print(f"[executor] ⚠️ 腿0未提交，跳过腿1")
                    all_ok = False
                    break

            if mode == "live" and trade_ctx:
                try:
                    lb_symbol = qqq_option_symbol(inst)
                    side = map_order_side(side_str)
                    ot = map_order_type(order_type_str)
                    resp = trade_ctx.submit_order(
                        symbol=lb_symbol,
                        order_type=ot,
                        side=side,
                        submitted_quantity=qty,
                        time_in_force=TimeInForceType.Day,
                        submitted_price=Decimal(str(limit_price)) if limit_price else None,
                    )
                    leg_order_id = resp.order_id if hasattr(resp, 'order_id') else leg_order_id
                    submitted_orders.append(leg_order_id)
                    # ── 价差腿0：用真实 order_id 注册成交等待事件 ──
                    if is_spread and i == 0:
                        _order_events[leg_order_id] = (_spread_leg0_evt, "")
                        print(f"[executor] 📝 注册腿0事件: {leg_order_id} → _order_events 共{len(_order_events)}条")
                    # 保存映射：长桥 order_id → intent context
                    order_map[leg_order_id] = {
                        "instrument": inst,
                        "source_signal_id": intent.get("source_signal_id", ""),
                        "exit_reason": intent.get("exit_reason"),
                        "side": side_str,
                        "quantity": str(qty),
                        "leg": i + 1,
                        "total_legs": len(legs),
                    }
                    status = "FILLED" if 'Filled' in str(getattr(resp, 'status', '')) else "ACCEPTED"
                    reason = f"Longbridge 实盘已提交 (腿{i+1}/{len(legs)})"
                    print(f"[executor] 📤 腿{i+1} 下单: {lb_symbol} {side_str} x{qty} → {leg_order_id}")
                except Exception as e:
                    all_ok = False
                    status = "REJECTED"
                    reason = f"腿{i+1} 下单失败: {e}"
                    print(f"[executor] ❌ 腿{i+1} 失败: {e}")
                    break
            else:
                status = "ACCEPTED"
                reason = f"paper execution (腿{i+1}/{len(legs)})"

            # 每条腿发 ack
            ack = {
                "order_id": leg_order_id,
                "intent_id": intent_id,
                "status": status,
                "reason": reason,
                "leg": i + 1,
                "total_legs": len(legs),
                "created_at": now_iso(),
            }
            await nc.publish("order.ack.option.qqq", json.dumps(ack).encode())

            # Paper 模式发 fill
            if mode == "paper":
                fill = {
                    "order_id": leg_order_id,
                    "instrument": inst,
                    "source_signal_id": intent.get("source_signal_id", ""),
                    "side": side_str,
                    "quantity": str(qty),
                    "price": str(limit_price or intent.get("reference_price", "0")),
                    "filled_at": now_iso(),
                    "leg": i + 1,
                    "total_legs": len(legs),
                    "is_exit": intent.get("exit_reason") is not None,
                }
                await nc.publish("fill.option.qqq", json.dumps(fill).encode())
                print(f"[executor] 📝 Paper 成交: {leg_order_id} (腿{i+1})")

        if not all_ok and mode == "live":
            print(f"[executor] ⚠️ 价差部分失败，需人工检查")

    await nc.subscribe(ORDER_INTENT_SUBJECT, cb=handle_order_intent)
    print(f"[executor] 订阅 {ORDER_INTENT_SUBJECT}，等待订单...")

    # 保持运行
    stop = asyncio.Event()
    await stop.wait()


if __name__ == "__main__":
    asyncio.run(main())
