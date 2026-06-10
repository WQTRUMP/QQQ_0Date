#!/usr/bin/env python3
"""
Market Regime — 市场体制感知 + 动态策略权重
=============================================
盘前 30 分钟收集数据 → 判断日内风格 → 动态分配三策略权重
日内每 30 秒刷新，感知趋势/波动变化即时换挡。

输入 (NATS):
  quote.option.qqqus    → QQQ 正股 last_done
  state.option.qqq      → adx, atr, trend_slope, trend_score
  greeks.option.qqq     → 期权链 (取 Put/Call 比、ATM IV)
  risk.option.qqq       → 风控决策 (连续亏损熔断)

输出 (NATS):
  regime.option.qqq     → {regime, weights, circuit_breaker, details}
  每 30 秒发布一次

用法:
  python python/market_regime/main.py
"""

import asyncio
import json
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import nats
from python.common.bootstrap import connect_nats_with_retry


# ── 配置 ──────────────────────────────────────────────

NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
PUBLISH_INTERVAL_SEC = int(os.getenv("REGIME_INTERVAL_SEC", "10"))
OPENING_PERIOD_MIN = int(os.getenv("OPENING_PERIOD_MIN", "30"))
CIRCUIT_BREAKER_LOSSES = int(os.getenv("CIRCUIT_BREAKER_LOSSES", "3"))


# ── 滑动窗口 ──────────────────────────────────────────

class SlidingWindow:
    def __init__(self, maxlen: int = 60):
        self.data = deque(maxlen=maxlen)

    def push(self, value: float):
        self.data.append(value)

    def latest(self) -> Optional[float]:
        return self.data[-1] if self.data else None

    def slope(self, n: int = 10) -> float:
        """最近 n 个点的线性斜率（简化：首尾差/时间）"""
        if len(self.data) < n:
            return 0.0
        recent = list(self.data)[-n:]
        return (recent[-1] - recent[0]) / max(n, 1)

    def mean(self, n: int = 10) -> float:
        if not self.data:
            return 0.0
        recent = list(self.data)[-n:]
        return sum(recent) / len(recent)

    def max_val(self) -> float:
        return max(self.data) if self.data else 0.0

    def min_val(self) -> float:
        return min(self.data) if self.data else 0.0

    def pct_change(self, n: int = 5) -> float:
        """最近 n 个点的变化百分比"""
        if len(self.data) < n:
            return 0.0
        recent = list(self.data)[-n:]
        if recent[0] == 0:
            return 0.0
        return (recent[-1] - recent[0]) / abs(recent[0])


# ── 全局状态 ──────────────────────────────────────────

prices = SlidingWindow(120)       # 30s × 120 = 1小时
vix_vals = SlidingWindow(60)      # 30 分钟
adx_vals = SlidingWindow(60)
opening_high: float = 0.0
opening_low: float = float("inf")
opening_minutes_elapsed: int = 0
last_30s_ts: float = 0.0
consecutive_losses: int = 0
circuit_breaker_active: bool = False
circuit_breaker_until: float = 0.0
# ── Gamma Flip & Max Pain ──
gamma_flip: float = 0.0           # 做市商 Gamma 翻转点
max_pain: float = 0.0             # 最大痛点
greeks_received: bool = False     # 是否已收到 Greeks 数据
# ── 仓位感知 ──
current_position_count: int = 0   # 当前持仓数（来自 PositionTracker）
MAX_POSITIONS = int(os.getenv("POSITION_SIZE", "3"))


# ── 体制判断 ──────────────────────────────────────────

def detect_regime() -> dict:
    global opening_minutes_elapsed

    price = prices.latest() or 0.0
    vix = vix_vals.latest() or 20.0
    adx = adx_vals.latest() or 15.0
    adx_slope = adx_vals.slope(10)
    vix_change_5m = vix_vals.pct_change(10)  # 10 × 30s = 5min
    price_change_5m = prices.pct_change(10)
    trend_score = 0.5  # 来自 state，默认中性

    # ── 先判断是否在开盘区间内 ──
    outside_opening_range = (
        opening_minutes_elapsed >= OPENING_PERIOD_MIN
        and opening_high > 0
        and (price > opening_high or price < opening_low)
    )

    # ── 体制分类 ──
    if circuit_breaker_active and time.time() < circuit_breaker_until:
        return {
            "regime": "circuit_breaker",
            "weights": {"momentum": 0.0, "theta_harvest": 0.0, "gamma_scalp": 0.0, "price_action": 0.0},
            "circuit_breaker": True,
            "reason": f"连续亏损熔断，{int(circuit_breaker_until - time.time())}秒后恢复",
        }

    # 基准 Price Action 权重：Al Brooks 在所有体制都有一席之地
    _pa = 0.10

    # VIX > 30 → 恐慌/高波日
    if vix > 30:
        if adx > 22 and abs(price_change_5m) > 0.003:
            return _regime("high_vol_trend", momentum=0.6, theta=0.0, gamma=0.35, price_action=0.05,
                          reason=f"VIX{int(vix)} 高波趋势 ADX{adx:.0f}")
        else:
            return _regime("high_vol_chop", momentum=0.05, theta=0.0, gamma=0.85, price_action=0.1,
                          reason=f"VIX{int(vix)} 高波震荡 → GammaScalp")

    # VIX > 22 → 偏高的波动
    if vix > 22:
        if adx > 22 and outside_opening_range:
            return _regime("trending", momentum=0.5, theta=0.15, gamma=0.25, price_action=_pa,
                          reason=f"VIX{int(vix)} ADX{adx:.0f} 突破开盘区间 → 动量优先")
        elif adx > 22:
            return _regime("trending", momentum=0.4, theta=0.25, gamma=0.25, price_action=_pa,
                          reason=f"VIX{int(vix)} ADX{adx:.0f} 趋势中")
        else:
            return _regime("volatile_sideways", momentum=0.15, theta=0.25, gamma=0.5, price_action=_pa,
                          reason=f"VIX{int(vix)} 中波震荡")

    # 15 < VIX < 22 → 正常日
    if vix >= 15:
        if adx > 25 and abs(price_change_5m) > 0.002:
            return _regime("trending", momentum=0.55, theta=0.20, gamma=0.15, price_action=_pa,
                          reason=f"VIX{int(vix)} ADX{adx:.0f} 强趋势 → 动量主攻")
        elif adx > 20:
            return _regime("mild_trend", momentum=0.35, theta=0.35, gamma=0.20, price_action=_pa,
                          reason=f"VIX{int(vix)} ADX{adx:.0f} 温和趋势")
        elif outside_opening_range:
            return _regime("breakout", momentum=0.45, theta=0.20, gamma=0.25, price_action=_pa,
                          reason=f"VIX{int(vix)} 突破开盘区间")
        else:
            return _regime("sideways", momentum=0.10, theta=0.70, gamma=0.10, price_action=_pa,
                          reason=f"VIX{int(vix)} ADX{adx:.0f} 横盘 → θ收割主攻")

    # VIX < 15 → 沉睡日
    # 沉睡日也给动量留条缝：ADX>20 说明有隐秘趋势，不能全禁
    if adx > 25:
        regime = _regime("low_vol_drift", momentum=0.25, theta=0.55, gamma=0.0, price_action=0.20,
                         reason=f"VIX{int(vix)} 沉睡日 ADX{adx:.0f} 暗藏趋势 → 动量+PA开缝")
    elif adx > 20:
        regime = _regime("low_vol_drift", momentum=0.15, theta=0.65, gamma=0.0, price_action=0.20,
                         reason=f"VIX{int(vix)} 沉睡日 ADX{adx:.0f} 微趋势 → PA可参与")
    else:
        regime = _regime("low_vol_drift", momentum=0.0, theta=0.70, gamma=0.0, price_action=0.30,
                         reason=f"VIX{int(vix)} 沉睡日 ADX{adx:.0f} 横盘 → PA+θ收割")

    # ── Gamma Flip 换挡：价格跌破 GF → 做市商 short gamma → 波动放大 ──
    regime = _apply_gamma_flip_adjustment(regime, price)

    # ── 仓位感知：持仓满则压制 ThetaHarvest，权重转给动量 ──
    if current_position_count >= MAX_POSITIONS:
        w = regime["weights"]
        th_current = w.get("theta_harvest", 0)
        if th_current > 0:
            # ThetaHarvest 权重减半，差额转给动量（买方策略不受持仓限制）
            cut = th_current * 0.5
            w["theta_harvest"] = round(max(0, th_current - cut), 2)
            w["momentum"] = round(min(1.0, w.get("momentum", 0) + cut), 2)
            regime["weights"] = w
            regime["reason"] += f" 仓位满({current_position_count}/{MAX_POSITIONS})→θ减半"

    return regime


def _apply_gamma_flip_adjustment(regime: dict, price: float) -> dict:
    """Gamma Flip 感知：做市商 Gamma 姿态影响波动率结构"""
    global gamma_flip, greeks_received

    if not greeks_received or gamma_flip <= 0 or price <= 0:
        return regime  # 数据不足，不做调整

    gf_dist_pct = (price - gamma_flip) / gamma_flip * 100  # 价格距 GF 的百分比
    w = dict(regime["weights"])  # 复制当前权重（含 price_action）

    if price < gamma_flip:  # ⚠️ 跌破 GF — dealers short gamma — 波动放大
        # GammaScalp 提权（做市商追涨杀跌放大波动）
        gs_boost = min(0.4, abs(gf_dist_pct) * 0.05)
        w["gamma_scalp"] = min(1.0, round(w["gamma_scalp"] + gs_boost, 2))
        w["momentum"] = round(max(0.05, w["momentum"] - gs_boost * 0.4), 2)
        w["theta_harvest"] = round(max(0.0, w["theta_harvest"] - gs_boost * 0.2), 2)
        w["price_action"] = round(max(0.0, w.get("price_action", 0.1) - gs_boost * 0.1), 2)
        note = f" 跌破GF({gamma_flip:.0f},距{gf_dist_pct:.1f}%)→GS+{gs_boost:.0%}"
        regime["reason"] += note

    elif gf_dist_pct > 0.5:  # ✅ 价格远高于 GF — dealers long gamma — 波动压制
        # ThetaHarvest 提权（做市商对冲压制波动）
        th_boost = min(0.3, gf_dist_pct * 0.03)
        w["theta_harvest"] = min(1.0, round(w["theta_harvest"] + th_boost, 2))
        w["gamma_scalp"] = round(max(0.0, w["gamma_scalp"] - th_boost * 0.6), 2)
        w["price_action"] = round(max(0.0, w.get("price_action", 0.1) - th_boost * 0.2), 2)
        note = f" 高于GF({gamma_flip:.0f},距{gf_dist_pct:.1f}%)→TH+{th_boost:.0%}"
        regime["reason"] += note

    regime["weights"] = w
    regime["gamma_flip"] = gamma_flip
    regime["gf_dist_pct"] = round(gf_dist_pct, 2)
    regime["max_pain"] = max_pain
    return regime


def _regime(name: str, momentum: float, theta: float, gamma: float, price_action: float, reason: str) -> dict:
    return {
        "regime": name,
        "weights": {
            "momentum": round(momentum, 2),
            "theta_harvest": round(theta, 2),
            "gamma_scalp": round(gamma, 2),
            "price_action": round(price_action, 2),
        },
        "circuit_breaker": False,
        "reason": reason,
    }


# ── 货币化信号 ──────────────────────────────────────

def build_regime_message(regime: dict) -> dict:
    return {
        **regime,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "details": {
            "price": prices.latest(),
            "vix": vix_vals.latest(),
            "adx": adx_vals.latest(),
            "adx_slope": round(adx_vals.slope(10), 3),
            "vix_change_5m_pct": round(vix_vals.pct_change(10) * 100, 1),
            "price_change_5m_pct": round(prices.pct_change(10) * 100, 2),
            "opening_high": opening_high,
            "opening_low": opening_low,
            "opening_minutes": opening_minutes_elapsed,
            "consecutive_losses": consecutive_losses,
            "gamma_flip": gamma_flip,
            "max_pain": max_pain,
            "gf_dist_pct": regime.get("gf_dist_pct", 0),
        },
    }


# ── 主逻辑 ────────────────────────────────────────────

async def main():
    global opening_high, opening_low, opening_minutes_elapsed
    global last_30s_ts, consecutive_losses, circuit_breaker_active, circuit_breaker_until

    nc = await connect_nats_with_retry(NATS_URL, "market_regime")
    start_time = time.time()

    # ── 行情（QQQ 正股 + 期权）──
    async def on_quote(msg):
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        price = data.get("last_done")
        if price is not None:
            prices.push(float(price))

    # ── VIX proxy：QQQ ATM 期权 IV 加权平均 ──
    # VIX.US 长桥无行情，改用 0DTE ATM call+put IV 平均 ×100 做 VIX 代理
    ATM_IV_STRIKE_RANGE = 5.0  # ATM 判定范围 ±$5
    atm_call_iv: float = 0.0
    atm_put_iv: float = 0.0
    atm_call_ts: float = 0.0
    atm_put_ts: float = 0.0

    # ── 真实 VIX（VIX.US 正股报价，长桥暂不可用，保留订阅等待未来支持）──
    async def on_vix(msg):
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        price = data.get("last_done")
        if price is not None:
            vix_vals.push(float(price))  # VIX 指数价格，如 14.5

    # ── ATM 期权 IV 提取（VIX proxy）──
    async def on_option(msg):
        nonlocal atm_call_iv, atm_put_iv, atm_call_ts, atm_put_ts
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        ext = data.get("option_extend")
        if not ext:
            return
        iv_str = ext.get("implied_volatility")
        strike_str = ext.get("strike_price")
        direction = ext.get("direction", "")
        if not iv_str or not strike_str:
            return
        try:
            iv = float(iv_str)
            strike = float(strike_str)
        except (ValueError, TypeError):
            return
        current_price = prices.latest()
        if current_price is None or current_price <= 0:
            return
        # 只取 ATM（行权价在现价 ±$5 范围内）
        if abs(strike - current_price) > ATM_IV_STRIKE_RANGE:
            return
        now = time.time()
        if direction.upper() == "C":
            atm_call_iv = iv
            atm_call_ts = now
        elif direction.upper() == "P":
            atm_put_iv = iv
            atm_put_ts = now
        # 两个都有且新鲜（10s内）→ 平均当 VIX proxy
        if atm_call_iv > 0 and atm_put_iv > 0:
            if now - atm_call_ts < 10 and now - atm_put_ts < 10:
                avg_iv = (atm_call_iv + atm_put_iv) / 2
                vix_vals.push(avg_iv * 100)  # IV decimal → VIX scale

    # ── 状态（Realtime Engine 输出） ──
    async def on_state(msg):
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        adx = data.get("adx")
        if adx is not None:
            adx_vals.push(float(adx))

    # ── Greeks（Gamma Flip & Max Pain）──
    async def on_greeks(msg):
        global gamma_flip, max_pain, greeks_received
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        gf = data.get("gamma_flip")
        if gf is not None:
            try:
                gf_val = float(gf)
                if gf_val > 0:
                    gamma_flip = gf_val
                    greeks_received = True
            except (ValueError, TypeError):
                pass
        mp = data.get("max_pain")
        if mp is not None:
            try:
                mp_val = float(mp)
                if mp_val > 0:
                    max_pain = mp_val
            except (ValueError, TypeError):
                pass

    # ── 仓位感知（持仓满时压制 ThetaHarvest）──
    async def on_position(msg):
        global current_position_count
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return
        count = data.get("count", 0)
        if isinstance(count, (int, float)):
            current_position_count = int(count)

    # ── 风控决策 ──
    async def on_risk(msg):
        global consecutive_losses, circuit_breaker_active, circuit_breaker_until
        try:
            data = json.loads(msg.data)
        except json.JSONDecodeError:
            return

        # 检测止损离场（亏损 = 连续失败+1）
        reason = data.get("reason", "")
        if "止损" in str(reason) or "stop" in str(reason).lower():
            consecutive_losses += 1
            print(f"[regime] ⚠️ 连续亏损: {consecutive_losses}/{CIRCUIT_BREAKER_LOSSES}")
            if consecutive_losses >= CIRCUIT_BREAKER_LOSSES:
                circuit_breaker_active = True
                circuit_breaker_until = time.time() + 1800  # 30 分钟
                print(f"[regime] 🛑 熔断！暂停 30 分钟")

        # 止盈 = 重置
        if "止盈" in str(reason) or "take_profit" in str(reason).lower():
            consecutive_losses = 0
            if circuit_breaker_active and time.time() >= circuit_breaker_until:
                circuit_breaker_active = False
                print("[regime] ✅ 熔断解除")

    # ── 订阅 ──
    await nc.subscribe("quote.option.qqqus", cb=on_quote)       # QQQ 正股 (Gateway 发 qqqus)
    await nc.subscribe("quote.option.vix", cb=on_vix)            # VIX.US 真实指数（长桥暂不可用）
    await nc.subscribe("quote.option.>", cb=on_option)           # 期权 IV → VIX proxy（仅 ATM ±$5）
    await nc.subscribe("state.option.qqq", cb=on_state)
    await nc.subscribe("greeks.option.qqq", cb=on_greeks)
    await nc.subscribe("risk.option.>", cb=on_risk)
    await nc.subscribe("position.option.qqq", cb=on_position)    # 仓位感知
    print("[regime] 订阅: qqq + vix(fb) + option(IV→VIX) + state + greeks + risk + pos")

    # ── 定时发布 ──
    async def publish_loop():
        global opening_high, opening_low, opening_minutes_elapsed, last_30s_ts

        while True:
            await asyncio.sleep(PUBLISH_INTERVAL_SEC)
            now = time.time()
            elapsed_min = (now - start_time) / 60

            # 更新开盘区间（前 30 分钟）
            if elapsed_min <= OPENING_PERIOD_MIN:
                opening_minutes_elapsed = int(elapsed_min)
                price = prices.latest() or 0.0
                if price > 0:
                    opening_high = max(opening_high, price)
                    opening_low = min(opening_low, price)
            else:
                opening_minutes_elapsed = OPENING_PERIOD_MIN

            regime = detect_regime()
            msg = build_regime_message(regime)
            await nc.publish("regime.option.qqq", json.dumps(msg).encode())

            # 简化日志
            w = regime["weights"]
            print(
                f"[regime] {regime['regime']:20s} | "
                f"M:{w['momentum']:.0%} T:{w['theta_harvest']:.0%} G:{w['gamma_scalp']:.0%} | "
                f"{regime.get('reason','')[:50]}"
            )

    print(f"[regime] ✅ 就绪，每{PUBLISH_INTERVAL_SEC}秒发布 regime.option.qqq")
    await publish_loop()


if __name__ == "__main__":
    asyncio.run(main())
