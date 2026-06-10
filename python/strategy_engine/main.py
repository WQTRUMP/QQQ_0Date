"""
QQQ 0DTE 策略引擎 v3

支持 STRATEGY_MODE:
  momentum     — 多条件共振动量策略（Donchian+放量+ADX+Delta筛选+冷却）
  theta_harvest — Θ 收割（卖 OTM 吃时间衰减）
  gamma_scalp  — Gamma Scalping（ATM Straddle 做 Gamma）

策略层职责：纯信号触发，不管理持仓上限。持仓限制由风控层负责。
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

import nats


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def decimal_from_dict(d: dict, key: str) -> Decimal:
    val = d.get(key)
    if val is None:
        return Decimal("0")
    try:
        return Decimal(str(val))
    except (ValueError, TypeError, InvalidOperation):
        return Decimal("0")


# ── 体制权重 ──────────────────────────────────────────

# 当前体制对各策略的权重（由 market_regime 服务每 30 秒发布）
_regime_weights: dict[str, float] = {
    "momentum": 1.0,
    "theta_harvest": 1.0,
    "gamma_scalp": 1.0,
    "price_action": 1.0,
}

# 市场结构指标（Gamma Flip + Max Pain，同样来自 regime 消息）
_market_structure: dict[str, float] = {
    "gamma_flip": 0.0,
    "max_pain": 0.0,
    "gf_dist_pct": 0.0,
    "price": 0.0,
}

# 本策略的持仓列表（由 position_tracker 推送）
_my_positions: list[dict] = []


def get_my_positions() -> list[dict]:
    """获取本策略当前持仓（供 PriceAction 等做退出决策）"""
    return _my_positions


def get_regime_weight(strategy_id: str) -> float:
    """获取当前体制下某策略的权重（0=禁用, 1=全力）"""
    for key, weight in _regime_weights.items():
        if key in strategy_id:
            return weight
    return 1.0


def get_market_structure() -> dict[str, float]:
    """获取当前市场结构指标（Gamma Flip、Max Pain 等）"""
    return dict(_market_structure)


async def subscribe_regime(nc) -> None:
    """订阅体制信号，更新权重 + 市场结构"""
    async def on_regime(msg):
        global _regime_weights, _market_structure
        try:
            data = json.loads(msg.data)
            weights = data.get("weights", {})
            if weights:
                _regime_weights.update(weights)
            details = data.get("details", {})
            if details:
                _market_structure["gamma_flip"] = details.get("gamma_flip", _market_structure["gamma_flip"])
                _market_structure["max_pain"] = details.get("max_pain", _market_structure["max_pain"])
                _market_structure["gf_dist_pct"] = details.get("gf_dist_pct", _market_structure["gf_dist_pct"])
                _market_structure["price"] = details.get("price", _market_structure["price"])
        except (json.JSONDecodeError, KeyError):
            pass

    await nc.subscribe("regime.option.qqq", cb=on_regime)
    print("[strategy] 订阅 regime.option.qqq（体制权重门 + GammaFlip/MaxPain）")


async def subscribe_positions(nc, strategy_id: str) -> None:
    """订阅持仓推送，筛选本策略的持仓"""
    async def on_positions(msg):
        global _my_positions
        try:
            data = json.loads(msg.data)
            all_positions = data.get("positions", [])
            _my_positions = [
                p for p in all_positions
                if p.get("strategy_id", "") == strategy_id
            ]
        except (json.JSONDecodeError, KeyError):
            pass

    await nc.subscribe("position.option.qqq", cb=on_positions)
    print(f"[strategy] 订阅 position.option.qqq（本策略持仓: {strategy_id}）")


# ── 策略基类 ──────────────────────────────────────────


class BaseStrategy:
    def build_signal(self, snapshot: dict) -> dict | None:
        raise NotImplementedError

    def reset(self) -> None:
        pass

    def on_init(self, init_data: dict) -> None:
        pass


# ── 多条件共振动量策略 ──────────────────────────────────


class MomentumStrategy(BaseStrategy):
    """多条件共振入场

    条件:
      ① Donchian Channel 突破: 现价 > 20周期最高 → 做多, < 20周期最低 → 做空
      ② 放量确认: 当前成交量 > 20周期均量
      ③ 趋势过滤: ADX > 20
      ④ Delta 筛选: 选 Delta 0.3-0.5 的 ATM Call/Put
      ⑤ 冷却期: 同方向信号间隔 ≥ 2 分钟
    """

    def __init__(self, strategy_id: str) -> None:
        self.strategy_id = strategy_id
        # 最新 state 数据（含技术指标）
        self._state: Optional[dict] = None
        # 最新 greeks 数据（含期权列表和 Delta）
        self._greeks: Optional[dict] = None
        # 冷却追踪: (方向, 时间戳)
        self._last_signal_time: dict[str, float] = {}
        self._cooling_secs = int(os.getenv("SIGNAL_COOLING_SECS", "120"))

    def reset(self) -> None:
        self._state = None
        self._greeks = None
        self._last_signal_time.clear()
        print("[reset] MomentumStrategy: 状态已重置")

    # ── 供外部调用的数据喂入 ──

    def on_state(self, data: dict) -> None:
        self._state = data

    def on_greeks(self, data: dict) -> None:
        self._greeks = data

    # ── 信号生成 ──

    def build_signal(self, _snapshot: dict = None) -> dict | None:
        """每次 tick 尝试生成信号"""
        state = self._state
        greeks = self._greeks
        if state is None or greeks is None:
            return None

        # 提取技术指标
        price = decimal_from_dict(state, "last_price")
        donchian_high = state.get("donchian_high")
        donchian_low = state.get("donchian_low")
        adx = state.get("adx")
        volume_avg = state.get("volume_avg_20")

        # 指标不足 → 不交易
        if donchian_high is None or donchian_low is None or adx is None or volume_avg is None:
            return None
        donchian_high = Decimal(str(donchian_high))
        donchian_low = Decimal(str(donchian_low))
        adx = Decimal(str(adx))
        volume_avg = Decimal(str(volume_avg))

        # ── 条件 ② 放量（近似：有交易即算放量，因为实时 tick volume 不是 K 线成交量）──
        # 注：实时行情中的 volume 是当日累计成交量，不是分钟成交量。
        # 这里简化：只要有价有量就过放量关。真正的分钟量确认在 RealtimeEngine 侧完成。
        has_volume = True  # RealtimeEngine 侧已在 K 线中校验

        # ── 条件 ③ 趋势过滤 ──
        if adx < Decimal("20"):
            return None

        # ── 条件 ① Donchian 突破 ──
        action = "HOLD"
        if price > donchian_high:
            action = "BUY"
        elif price < donchian_low:
            action = "SELL"

        if action == "HOLD":
            return None

        # ── 条件 ⑤ 冷却期（检查但不扣计时器——等信号完全构建成功再扣）──
        now_ts = time.time()
        last_ts = self._last_signal_time.get(action, 0)
        if now_ts - last_ts < self._cooling_secs:
            return None

        # ── 条件 ④ Delta 筛选 ──
        option = self._select_option_by_delta(greeks, price, action)
        if option is None:
            return None

        # ── 计算信心分 ──
        # ADX 越高越有信心（趋势强），突破幅度越大越有信心
        breakout_strength = abs(price - donchian_high) if action == "BUY" else abs(price - donchian_low)
        adx_score = min(Decimal("0.3"), (adx - Decimal("20")) / Decimal("40"))  # ADX 20→0, 60→0.3
        breakout_score = min(Decimal("0.4"), breakout_strength / price * Decimal("100"))  # 突破幅度
        confidence = Decimal("0.60") + adx_score + breakout_score
        confidence = min(Decimal("0.95"), confidence)

        instrument = {
            "asset_class": "OPTION",
            "symbol": option["symbol"],
            "strike": str(option["strike"]),
            "option_right": option["option_right"],
            "expiry": option.get("expiry", ""),
        }

        # 信号构建完成，扣冷却计时器
        self._last_signal_time[action] = now_ts

        return {
            "signal_id": f"signal-{self.strategy_id}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
            "strategy_id": self.strategy_id,
            "instrument": instrument,
            "action": action,
            "confidence": str(confidence),
            "reference_price": str(price),
            "reason": (
                f"Donchian突破: 价格{price} {'上破' if action=='BUY' else '下破'}"
                f"{'高' if action=='BUY' else '低'}点{donchian_high if action=='BUY' else donchian_low}, "
                f"ADX={adx:.1f}, Delta≈{option['delta']:.2f}, "
                f"选{option['option_right']}@{option['strike']}"
            ),
            "created_at": now_iso(),
        }

    def _select_option_by_delta(self, greeks: dict, underlying: Decimal, action: str) -> Optional[dict]:
        """从 greeks 快照中选 Delta 0.3-0.5 的 ATM 期权"""
        rows = greeks.get("rows", [])
        if not rows:
            return None

        # 从 greeks 快照推导到期日（与 ThetaHarvest 一致，GreekRow 无 symbol/expiry 字段）
        generated_at = greeks.get("generated_at", "")
        expiry_date = generated_at[:10].replace("-", "") if generated_at else ""

        target_right = "CALL" if action == "BUY" else "PUT"
        best = None
        best_score = Decimal("999")

        for row in rows:
            if row.get("option_right") != target_right:
                continue
            delta_val = row.get("delta")
            if delta_val is None:
                continue
            delta_val = Decimal(str(delta_val))
            delta_abs = abs(delta_val)
            # 使用 Delta 绝对值统一筛选，兼容 signed / unsigned put delta。
            if delta_abs < Decimal("0.3") or delta_abs > Decimal("0.5"):
                continue
            # 选最接近 ATM 的（行权价离现价最近）
            strike = Decimal(str(row.get("strike", "0")))
            dist = abs(strike - underlying)
            score = dist + abs(delta_abs - Decimal("0.4")) * Decimal("10")  # Delta 0.4 最优
            if score < best_score:
                best_score = score
                # 构建期权 symbol（格式: QQQ{YYMMDD}{C/P}{STRIKE*1000:06d}.US）
                opt_right_char = "C" if target_right == "CALL" else "P"
                strike_int = int(float(strike) * 1000)
                opt_symbol = f"QQQ{expiry_date[2:]}{opt_right_char}{strike_int:06d}.US" if expiry_date else ""
                best = {
                    "symbol": opt_symbol,
                    "strike": str(strike),
                    "option_right": target_right,
                    "delta": float(delta_val),
                    "expiry": expiry_date,
                }

        return best


# ── 原有动量策略（简单版，保留兼容）───────────────────────


class SimpleMomentumStrategy(BaseStrategy):
    def __init__(self, strategy_id: str, threshold_bps: Decimal) -> None:
        self.strategy_id = strategy_id
        self.threshold_bps = threshold_bps
        self.last_price_by_symbol: dict[str, Decimal] = {}

    def build_signal(self, state: dict) -> dict | None:
        instrument = state["instrument"]
        symbol = instrument["symbol"]
        price = decimal_from_dict(state, "last_price")
        previous_price = self.last_price_by_symbol.get(symbol)
        self.last_price_by_symbol[symbol] = price

        if previous_price is not None and previous_price > 0:
            change_bps = (price - previous_price) / previous_price * Decimal("10000")
            if change_bps >= self.threshold_bps:
                return self._build_signal_dict(instrument, "BUY", "0.70", str(price),
                    f"动量上行 {change_bps:.4f} bps")
            elif change_bps <= -self.threshold_bps:
                return self._build_signal_dict(instrument, "SELL", "0.70", str(price),
                    f"动量下行 {change_bps:.4f} bps")
        return None

    def reset(self) -> None:
        self.last_price_by_symbol.clear()

    def _build_signal_dict(self, instrument: dict, action: str, confidence: str, ref_price: str, reason: str) -> dict:
        return {
            "signal_id": f"signal-{self.strategy_id}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
            "strategy_id": self.strategy_id,
            "instrument": instrument,
            "action": action,
            "confidence": confidence,
            "reference_price": ref_price,
            "reason": reason,
            "created_at": now_iso(),
        }


# ── Theta 收割策略 v2（书本优化）──────────────────────────


class ThetaHarvestStrategy(BaseStrategy):
    """卖虚值期权吃时间衰减（v2：Natenberg + Sinclair + Augen）

    改进:
      - Θ 窗口缩到到期前最后 2 小时（Natenberg：Θ 衰减在末段加速）
      - IV 百分位过滤（Sinclair：IV rank > 70% 才卖，避免卖在 IV 低谷）
      - OTM 距离随 IV 动态调整（Passarelli：高 IV 卖更远）
      - 过期时间写入信号 → 风控侧最后 1 小时收紧止损（Augen）
    """

    def __init__(self, strategy_id: str, min_theta: Decimal = Decimal("0.05"), max_hours: Decimal = Decimal("2")) -> None:
        self.strategy_id = strategy_id
        self.min_theta = min_theta
        self.max_hours = max_hours  # Natenberg: 只做末段
        self.atr = Decimal("0.4")  # 日内分钟 ATR 默认值（~$0.4），RealtimeEngine init 会覆盖
        self.hv = Decimal("0.25")
        self.trend_slope = Decimal("0")
        self.trend_score = Decimal("0.5")
        # 信用价差宽度（点数）
        self.spread_wing_width = Decimal(os.getenv("SPREAD_WING_WIDTH", "3.0"))
        # IV 百分位追踪（最近 20 个快照的 ATM IV）
        self._iv_history: list[Decimal] = []
        self._iv_maxlen = 20
        # 冷却追踪：按 symbol 独立冷却，避免同一行权价反复发信号
        self._last_signal_time: dict[str, float] = {}
        self._cooling_secs = int(os.getenv("SIGNAL_COOLING_SECS", "120"))

    def on_init(self, init_data: dict) -> None:
        atr_val = decimal_from_dict(init_data, "atr")
        # 只接受合理的日内 ATR（0.05-5.0），拒绝日线级或零值 ATR
        if Decimal("0.05") <= atr_val <= Decimal("5.0"):
            self.atr = atr_val
        self.hv = decimal_from_dict(init_data, "historical_volatility")
        self.trend_slope = decimal_from_dict(init_data, "trend_slope")
        self.trend_score = decimal_from_dict(init_data, "trend_score")
        print(f"[init] ThetaHarvest v2: HV={self.hv:.2%}, ATR={self.atr}, max={self.max_hours}h")

    def reset(self) -> None:
        self._iv_history.clear()

    def _atm_iv(self, rows: list[dict], underlying: Decimal) -> Decimal | None:
        """取最接近现价的期权的 IV 作为 ATM IV"""
        best_dist = Decimal("999999")
        best_iv = None
        for row in rows:
            strike = decimal_from_dict(row, "strike")
            dist = abs(strike - underlying)
            if dist < best_dist:
                best_dist = dist
                best_iv = decimal_from_dict(row, "iv")
        return best_iv

    def _iv_percentile(self, current_iv: Decimal) -> Decimal:
        """当前 IV 在历史中的百分位 (0~1)"""
        if len(self._iv_history) < 5:
            return Decimal("0.5")  # 数据不足时中性
        below = sum(1 for v in self._iv_history if v <= current_iv)
        return Decimal(str(below)) / Decimal(str(len(self._iv_history)))

    def build_signal(self, snapshot: dict) -> dict | None:
        instrument = snapshot["instrument"]
        rows = snapshot.get("rows", [])
        underlying = decimal_from_dict(snapshot, "underlying_price")
        hours = Decimal(str(snapshot.get("hours_to_expiry", "0")))

        # Natenberg: 只在 Θ 加速衰减的末段交易
        if hours > self.max_hours:
            return None

        # 追踪 ATM IV 历史
        atm_iv = self._atm_iv(rows, underlying)
        if atm_iv is not None:
            self._iv_history.append(atm_iv)
            if len(self._iv_history) > self._iv_maxlen:
                self._iv_history.pop(0)

        # Sinclair: IV 百分位过滤——只在 IV 偏贵时卖
        if atm_iv is not None and len(self._iv_history) >= 5:
            iv_pct = self._iv_percentile(atm_iv)
            if iv_pct < Decimal("0.40"):
                return None  # IV rank < 40%，不够贵，不卖（0DTE日内IV天然衰减）

        # Passarelli: OTM 距离随 IV 动态调整
        # IV 高 → 市场预期波动大 → 卖更远才安全
        iv_mult = Decimal("1.0")
        if atm_iv is not None:
            iv_mult = max(Decimal("0.8"), min(Decimal("2.0"), atm_iv / Decimal("0.25")))
        safe_distance = self.atr * iv_mult

        best_row, best_theta, best_score = None, Decimal("0"), Decimal("0")
        # ── Max Pain 偏好：到期日磁力效应，靠近 Max Pain 的腿加分 ──
        ms = get_market_structure()
        mp = ms.get("max_pain", 0.0)
        mp_bonus_factor = Decimal("0")
        if mp > 0:
            mp_dist = abs(underlying - Decimal(str(mp)))
            mp_bonus_factor = max(Decimal("0"), Decimal("0.15") - mp_dist * Decimal("0.003"))
            # mp_bonus_factor: 0.15 (正好在 MP) → 0 (离 MP 太远)
        for row in rows:
            row_theta = abs(decimal_from_dict(row, "theta"))
            strike = decimal_from_dict(row, "strike")
            is_otm = (row["option_right"] == "CALL" and strike > underlying + safe_distance) or (
                row["option_right"] == "PUT" and strike < underlying - safe_distance)
            if not is_otm:
                continue
            if self.trend_score > Decimal("0.6"):
                if self.trend_slope > Decimal("0") and row["option_right"] == "CALL":
                    continue
                if self.trend_slope < Decimal("0") and row["option_right"] == "PUT":
                    continue
            # Max Pain 加分：越靠近 Max Pain 的腿，到期日收敛概率越高
            mp_bonus = Decimal("0")
            if mp > 0:
                strike_dist_to_mp = abs(strike - Decimal(str(mp)))
                mp_bonus = max(Decimal("0"), Decimal("0.12") - strike_dist_to_mp * Decimal("0.004"))
            score = row_theta + mp_bonus
            if row_theta >= self.min_theta and score > best_score:
                best_theta, best_row, best_score = row_theta, row, score

        if best_row is None:
            return None

        # ── 冷却期：同一行权价+方向独立冷却，避免反复发信号 ──
        best_strike = decimal_from_dict(best_row, "strike")
        best_right = best_row["option_right"]
        cooldown_key = f"{best_right}:{best_strike}"
        now_ts = time.time()
        last_ts = self._last_signal_time.get(cooldown_key, 0)
        if now_ts - last_ts < self._cooling_secs:
            return None
        self._last_signal_time[cooldown_key] = now_ts

        # ── 信用价差：配对保护腿 ──
        spread_wing = None
        if best_right == "CALL":
            wing_strike_target = best_strike + self.spread_wing_width
            # 找 > 卖腿行权价的 CALL
            wing_candidates = [
                r for r in rows
                if r["option_right"] == "CALL" and decimal_from_dict(r, "strike") >= wing_strike_target
            ]
        else:
            wing_strike_target = best_strike - self.spread_wing_width
            wing_candidates = [
                r for r in rows
                if r["option_right"] == "PUT" and decimal_from_dict(r, "strike") <= wing_strike_target
            ]
        # 从 Greeks 快照推导到期日（0DTE = 当天）
        expiry_date = snapshot.get("generated_at", "")[:10].replace("-", "")

        if wing_candidates:
            # 选最接近目标行权价的
            wing_candidates.sort(key=lambda r: abs(decimal_from_dict(r, "strike") - wing_strike_target))
            wing_row = wing_candidates[0]
            wing_strike = decimal_from_dict(wing_row, "strike")
            # 构建保护腿 symbol
            wing_right_char = "C" if best_right == "CALL" else "P"
            wing_strike_int = int(float(wing_strike) * 1000)
            wing_symbol = f"QQQ{expiry_date[2:]}{wing_right_char}{wing_strike_int:06d}.US"
            spread_wing = {
                "asset_class": "OPTION",
                "symbol": wing_symbol,
                "strike": str(wing_strike),
                "option_right": best_right,
                "expiry": expiry_date,
            }
            max_loss = (abs(wing_strike - best_strike) * Decimal("100")).to_eng_string()
        else:
            # 没有保护腿 = 不构成信用价差，放弃此信号
            return None

        # Augen: 最后 1 小时信心分更低（风险更大），但给风控信号收紧止损
        remaining_hours = float(hours)
        last_hour_penalty = Decimal("0.10") if remaining_hours < 1.0 else Decimal("0")
        confidence = min(Decimal("0.85"), best_theta * Decimal("10")) - last_hour_penalty
        # 置信度缩放原理：乘数 10 使得 theta=0.085 达到 max 0.85。
        # 深度 OTM 如 theta=0.005 → 0.05 置信度（底线），这符合策略预期：
        # 深虚值风险低但收益也低，不应给出虚高信心。底线 0.05 防止零置信。
        confidence = max(Decimal("0.05"), confidence)  # 底线 5%，防止负信心

        iv_info = f" IVpct={float(iv_pct):.0%}" if atm_iv and len(self._iv_history) >= 5 else ""
        mp_info = f" MP@{mp:.0f}" if mp > 0 else ""

        # 构建期权 symbol（格式: QQQ{YYMMDD}{C/P}{STRIKE*1000:06d}.US）
        opt_right_char = "C" if best_row["option_right"] == "CALL" else "P"
        strike_int = int(float(best_row["strike"]) * 1000)
        opt_symbol = f"QQQ{expiry_date[2:]}{opt_right_char}{strike_int:06d}.US"

        instrument_out = dict(instrument)
        # 填入选中期权的完整信息（Executor 下单需要）
        instrument_out["symbol"] = opt_symbol
        instrument_out["strike"] = best_row["strike"]
        instrument_out["option_right"] = best_row["option_right"]
        instrument_out["expiry"] = expiry_date
        instrument_out["hours_to_expiry"] = str(remaining_hours)

        # 构建信号
        sig = {
            "signal_id": f"signal-{self.strategy_id}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
            "strategy_id": self.strategy_id,
            "instrument": instrument_out,
            "action": "SELL",
            "confidence": str(confidence),
            "reference_price": str(underlying),
            "reason": (
                f"Θ收割: 卖{best_row['option_right']} {best_row['strike']}, "
                f"Theta={best_theta:.4f}, 剩{remaining_hours:.1f}h, OTM距={safe_distance}"
                f"{iv_info}{mp_info}, ⚡价差宽{self.spread_wing_width}pt, 最大亏${max_loss}"
            ),
            "created_at": now_iso(),
            "hours_to_expiry": str(remaining_hours),
        }
        if spread_wing:
            sig["spread_wing"] = spread_wing
        return sig


# ── Gamma Scalp 策略 v2（书本优化）─────────────────────────


class GammaScalpStrategy(BaseStrategy):
    """ATM Straddle 做 Gamma（v2：Sinclair + Natenberg + Passarelli）

    改进:
      - 入场算预期 Gamma 利润 vs Θ 衰减成本（Sinclair：½×Γ×(ΔS)² > Θ）
      - 最低波动阈值过滤（Natenberg：波动太低 Gamma 不够覆盖 Θ）
      - 流动性/未平仓检查（Passarelli：避免价差宽的冷门合约）
      - IV 百分位替代固定比值（防止买在高 IV 环境）
    """

    def __init__(self, strategy_id: str, min_gamma: Decimal = Decimal("0.02")) -> None:
        self.strategy_id = strategy_id
        self.min_gamma = min_gamma
        self.hv = Decimal("0.25")
        self.trend_score = Decimal("0.5")
        self.atr = Decimal("0.4")  # 日内分钟 ATR 默认值（~$0.4），RealtimeEngine init 会覆盖
        # IV 百分位追踪
        self._iv_history: list[Decimal] = []
        self._iv_maxlen = 20

    def on_init(self, init_data: dict) -> None:
        self.hv = decimal_from_dict(init_data, "historical_volatility")
        self.trend_score = decimal_from_dict(init_data, "trend_score")
        atr_val = decimal_from_dict(init_data, "atr")
        # 只接受合理的日内 ATR（0.05-5.0），拒绝日线级或零值 ATR
        if Decimal("0.05") <= atr_val <= Decimal("5.0"):
            self.atr = atr_val
        print(f"[init] GammaScalp v2: HV={self.hv:.2%}, trend={self.trend_score:.2f}, ATR={self.atr}")

    def reset(self) -> None:
        self._iv_history.clear()

    def _atm_iv(self, rows: list[dict], underlying: Decimal) -> Decimal | None:
        best_dist = Decimal("999999")
        best_iv = None
        for row in rows:
            dist = abs(decimal_from_dict(row, "strike") - underlying)
            if dist < best_dist:
                best_dist = dist
                best_iv = decimal_from_dict(row, "iv")
        return best_iv

    def _iv_percentile(self, current_iv: Decimal) -> Decimal:
        if len(self._iv_history) < 5:
            return Decimal("0.5")
        below = sum(1 for v in self._iv_history if v <= current_iv)
        return Decimal(str(below)) / Decimal(str(len(self._iv_history)))

    def _expected_gamma_profit(self, gamma: Decimal, atr: Decimal, theta: Decimal, spread_pct: Decimal) -> Decimal:
        """Sinclair：预计 Gamma 利润 = ½×Γ×(ΔS)²×100 - Θ_hourly - 价差成本

        gamma = delta 变化 / $1 标的变化（标准 B-S gamma）
        atr   = 预期波动点数
        theta = 日度 theta（除以 6.5 得到小时 theta）
        """
        # ½ × gamma × (move_points)² × 100 shares/contract
        gamma_pnl = Decimal("0.5") * gamma * atr * atr * Decimal("100")
        # 小时 theta + 双边价差估算
        cost = abs(theta) / Decimal("6.5") + spread_pct * Decimal("2")
        return gamma_pnl - cost

    def build_signal(self, snapshot: dict) -> dict | None:
        # Natenberg: 趋势太强不适合 Gamma Scalp
        if self.trend_score > Decimal("0.7"):
            return None

        instrument = snapshot["instrument"]
        rows = snapshot.get("rows", [])
        underlying = decimal_from_dict(snapshot, "underlying_price")

        # ── Gamma Flip 感知：价格 < GF → 做市商 short gamma → 波动放大 → 加大 GammaScalp ──
        ms = get_market_structure()
        gf = ms.get("gamma_flip", 0.0)
        gf_boost = Decimal("0")  # 额外信心加成
        gf_reason = ""
        if gf > 0 and float(underlying) < gf:
            # 价格跌破 GF：做市商从 long gamma 转 short gamma
            # → 对冲行为从「高抛低吸」变「追涨杀跌」→ 波动放大
            # GammaScalp 的利润与波动平方成正比 → 这是最肥的时段
            dist_pct = abs(float(underlying) - gf) / gf * 100
            gf_boost = Decimal(str(min(0.25, dist_pct * 0.03)))  # 最多 +0.25 信心
            gf_reason = f" ⚡GF:{gf:.0f}({float(underlying)-gf:+.1f})→GS"

        # Natenberg: 最低波动阈值——波动太低 Gamma 利润不够覆盖 Θ
        if hasattr(self, 'atr') and self.atr < Decimal("0.3") and gf_boost < Decimal("0.15"):
            return None  # 无 GF boost 且 ATR 太低时跳过，有 GF boost 时放宽

        # 追踪 ATM IV 历史
        atm_iv = self._atm_iv(rows, underlying)
        if atm_iv is not None:
            self._iv_history.append(atm_iv)
            if len(self._iv_history) > self._iv_maxlen:
                self._iv_history.pop(0)

        # Sinclair: IV 百分位过滤——不买贵的
        if atm_iv is not None and len(self._iv_history) >= 5:
            iv_pct = self._iv_percentile(atm_iv)
            if iv_pct > Decimal("0.50"):
                return None  # IV 偏贵，不适合买

        # 从 greeks 快照推导到期日（GreekRow 无 symbol/expiry，需手动构建）
        generated_at = snapshot.get("generated_at", "")
        expiry_date = generated_at[:10].replace("-", "") if generated_at else ""

        best_pair, best_gamma, best_score = None, Decimal("0"), Decimal("-999")
        for row in rows:
            row_gamma = decimal_from_dict(row, "gamma")
            strike = decimal_from_dict(row, "strike")
            if abs(strike - underlying) > Decimal("2"):
                continue

            # 跳过 IV/theta 缺失的行（GreekRow 一定有这些字段）
            row_theta = decimal_from_dict(row, "theta")
            if row_theta == Decimal("0"):
                continue

            # Sinclair: 计算预期 Gamma 利润
            spread_est = Decimal("0.05")  # 预估价差 5¢
            expected_profit = self._expected_gamma_profit(row_gamma, self.atr, row_theta, spread_est)

            # 预期利润必须 > 0 且 Gamma 够大
            if expected_profit <= Decimal("0") or row_gamma < self.min_gamma:
                continue

            # 评分：预期利润 + Gamma 加权
            score = expected_profit * Decimal("10") + row_gamma
            if score > best_score:
                best_score = score
                best_gamma = row_gamma
                best_pair = row

        if best_pair is None:
            return None

        expected_pnl = self._expected_gamma_profit(
            best_gamma, self.atr,
            decimal_from_dict(best_pair, "theta"),
            Decimal("0.05")
        )
        iv_info = f" IVpct={float(iv_pct):.0%}" if atm_iv and len(self._iv_history) >= 5 else ""

        # 构建期权 symbol（格式: QQQ{YYMMDD}{C/P}{STRIKE*1000:06d}.US）
        best_strike = decimal_from_dict(best_pair, "strike")
        best_right = best_pair.get("option_right", "CALL")
        opt_right_char = "C" if best_right == "CALL" else "P"
        strike_int = int(float(best_strike) * 1000)
        opt_symbol = f"QQQ{expiry_date[2:]}{opt_right_char}{strike_int:06d}.US" if expiry_date else ""

        instrument_out = dict(instrument)
        instrument_out["symbol"] = opt_symbol
        instrument_out["strike"] = str(best_strike)
        instrument_out["option_right"] = best_right
        instrument_out["expiry"] = expiry_date

        return {
            "signal_id": f"signal-{self.strategy_id}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
            "strategy_id": self.strategy_id,
            "instrument": instrument_out,
            "action": "BUY",
            "confidence": str(min(Decimal("0.85"), best_gamma * Decimal("10") + gf_boost)),
            "reference_price": str(underlying),
            "reason": (
                f"Gamma Scalp: ATM Straddle @{best_pair['strike']}, "
                f"Γ={best_gamma:.4f}, E[PnL]≈{expected_pnl:.3f}, ATR={self.atr}{iv_info}{gf_reason}"
            ),
            "created_at": now_iso(),
        }


# ── 价格行为策略 v1（Al Brooks：二次进场 + 反转bar）─────────

class PriceActionStrategy(BaseStrategy):
    """Al Brooks 价格行为：二次进场 + 高概率反转bar

    二次进场 (Second Entry):
      ① 判定趋势方向（连续 higher highs/lows 或 lower highs/lows）
      ② 第一次回调（逆趋势）→ 第一次回弹（顺趋势）→ 跳过
      ③ 第二次回调 → 第二次回弹 = 信号（高胜率入场）

    反转bar (Reversal Bar):
      ① bar 实体 > 近20根均值的 2 倍
      ② 收盘在极端（上影/下影 < 实体的 20%）
      ③ 出现在关键位（开盘区间边界 / 日内高低点）
    """

    def __init__(self, strategy_id: str) -> None:
        self.strategy_id = strategy_id
        # K 线记忆：最近 60 根 1m bar
        self.bars: list[dict] = []          # [{o, h, l, c, v, ts}]
        self.max_bars = 60
        # 基本面数据
        self._state: Optional[dict] = None
        self._greeks: Optional[dict] = None
        # 二次进场状态机
        self._se_state = "wait_trend"       # wait_trend | wait_pullback_1 | wait_bounce_1 | wait_pullback_2 | wait_bounce_2
        self._trend_direction = ""          # "up" / "down"
        self._pullback_low: Optional[Decimal] = None   # 回调低点（多头）或高点（空头）
        self._first_bounce_target = Decimal("0")
        # 反转冷却
        self._last_reversal_ts: float = 0
        self._reversal_cooldown_sec = 120   # 2 分钟
        # 门限
        self._min_bars_for_trend = 10
        self._reversal_range_mult = Decimal("2.0")
        self._reversal_wick_max_pct = Decimal("0.20")
        # 日内关键位
        self._day_high: Optional[Decimal] = None
        self._day_low: Optional[Decimal] = None
        self._opening_high = Decimal("0")
        self._opening_low = Decimal("0")

    def reset(self) -> None:
        self.bars.clear()
        self._state = None
        self._greeks = None
        self._se_state = "wait_trend"
        self._trend_direction = ""
        self._day_high = None
        self._day_low = None
        print("[reset] PriceActionStrategy: 状态已重置")

    def on_state(self, data: dict) -> None:
        self._state = data
        hi = data.get("high")
        lo = data.get("low")
        if hi is not None:
            hd = Decimal(str(hi))
            if self._day_high is None or hd > self._day_high:
                self._day_high = hd
        if lo is not None:
            ld = Decimal(str(lo))
            if self._day_low is None or ld < self._day_low:
                self._day_low = ld

    def on_greeks(self, data: dict) -> None:
        self._greeks = data

    def on_kline(self, data: dict) -> None:
        """接收 RealtimeEngine 的 1m K 线"""
        try:
            bar = {
                "o": Decimal(str(data.get("open", 0))),
                "h": Decimal(str(data.get("high", 0))),
                "l": Decimal(str(data.get("low", 0))),
                "c": Decimal(str(data.get("close", 0))),
                "v": int(data.get("volume", 0) or 0),
                "ts": data.get("timestamp", time.time()),
            }
            if bar["o"] <= Decimal("0") or bar["c"] <= Decimal("0"):
                return
            self.bars.append(bar)
            if len(self.bars) > self.max_bars:
                self.bars = self.bars[-self.max_bars:]
        except (ValueError, TypeError):
            pass

    def build_signal(self, _snapshot=None) -> dict | None:
        if len(self.bars) < self._min_bars_for_trend:
            return None
        greeks = self._greeks
        if greeks is None:
            return None

        # ── 二次进场 ──
        se_signal = self._check_second_entry()
        if se_signal:
            return se_signal

        # ── 反转bar ──
        rb_signal = self._check_reversal_bar()
        if rb_signal:
            return rb_signal

        return None

    # ── 趋势检测 ─────────────────────────────────────

    def _detect_trend(self) -> str:
        """用最近 20 根 bar 的摆荡结构判断趋势"""
        if len(self.bars) < 20:
            return "neutral"
        recent = self.bars[-20:]
        highs = [b["h"] for b in recent]
        lows = [b["l"] for b in recent]
        # 找摆荡点（局部极值）
        swing_highs = []
        swing_lows = []
        for i in range(2, len(recent) - 2):
            if recent[i]["h"] >= recent[i - 1]["h"] and recent[i]["h"] >= recent[i - 2]["h"] \
               and recent[i]["h"] >= recent[i + 1]["h"] and recent[i]["h"] >= recent[i + 2]["h"]:
                swing_highs.append(recent[i]["h"])
            if recent[i]["l"] <= recent[i - 1]["l"] and recent[i]["l"] <= recent[i - 2]["l"] \
               and recent[i]["l"] <= recent[i + 1]["l"] and recent[i]["l"] <= recent[i + 2]["l"]:
                swing_lows.append(recent[i]["l"])
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            hh = swing_highs[-1] > swing_highs[-2]
            hl = swing_lows[-1] > swing_lows[-2]
            if hh and hl:
                return "up"
            lh = swing_highs[-1] < swing_highs[-2]
            ll = swing_lows[-1] < swing_lows[-2]
            if lh and ll:
                return "down"
        # 回退：均线斜率
        closes = [b["c"] for b in recent]
        ma_fast = sum(closes[-5:], Decimal("0")) / 5
        ma_slow = sum(closes[-15:], Decimal("0")) / 15
        if ma_fast > ma_slow * Decimal("1.002"):
            return "up"
        if ma_fast < ma_slow * Decimal("0.998"):
            return "down"
        return "neutral"

    # ── 二次进场 ─────────────────────────────────────

    def _check_second_entry(self) -> dict | None:
        trend = self._detect_trend()
        if trend == "neutral":
            self._se_state = "wait_trend"
            return None
        if trend != self._trend_direction:
            self._trend_direction = trend
            self._se_state = "wait_pullback_1"
            return None

        bar = self.bars[-1]
        price = bar["c"]
        greeks = self._greeks
        rows = greeks.get("rows", [])
        underlying = Decimal(str(greeks.get("underlying_price", str(price))))

        if self._se_state == "wait_pullback_1":
            # 等价格逆趋势回调
            # 日内高低点未就绪时跳过
            if self._day_high is None or self._day_low is None:
                pulled_back = False
            else:
                pulled_back = (trend == "up" and price < self._day_high * Decimal("0.998")) or \
                              (trend == "down" and price > self._day_low * Decimal("1.002"))
            if pulled_back:
                self._pullback_low = price
                self._se_state = "wait_bounce_1"

        elif self._se_state == "wait_bounce_1":
            # 第一次回弹（不理，跳过）
            if self._pullback_low is None:
                bounced = False
            else:
                bounced = (trend == "up" and price > self._pullback_low) or \
                          (trend == "down" and price < self._pullback_low)
            if bounced:
                self._first_bounce_target = price
                self._se_state = "wait_pullback_2"

        elif self._se_state == "wait_pullback_2":
            # 等第二次回调
            pulled_back_2 = (trend == "up" and price < self._first_bounce_target) or \
                            (trend == "down" and price > self._first_bounce_target)
            if pulled_back_2:
                self._pullback_low = price
                self._se_state = "wait_bounce_2"

        elif self._se_state == "wait_bounce_2":
            # 第二次回弹 = 信号！
            if self._pullback_low is None:
                bounced_2 = False
            else:
                bounced_2 = (trend == "up" and price > self._pullback_low) or \
                            (trend == "down" and price < self._pullback_low)
            if bounced_2:
                self._se_state = "wait_pullback_1"  # 重置
                return self._build_signal(trend, underlying, rows, "second_entry")

        return None

    # ── 反转bar ─────────────────────────────────────

    def _check_reversal_bar(self) -> dict | None:
        if len(self.bars) < 20:
            return None
        now = time.time()
        if now - self._last_reversal_ts < self._reversal_cooldown_sec:
            return None  # 冷却中

        bar = self.bars[-1]
        o, h, l, c = bar["o"], bar["h"], bar["l"], bar["c"]
        body = abs(c - o)
        if body <= Decimal("0"):
            return None

        # 1. bar 实体 > 近20根均值的 2 倍
        recent_bodies = [abs(b["o"] - b["c"]) for b in self.bars[-21:-1]]
        mean_body = sum(recent_bodies, Decimal("0")) / len(recent_bodies)
        if mean_body <= Decimal("0") or body < mean_body * self._reversal_range_mult:
            return None

        # 2. 收盘在极端（上影/下影短）
        is_bullish = c > o
        upper_wick = h - (c if is_bullish else o)
        lower_wick = (o if is_bullish else c) - l
        has_small_wick = (is_bullish and upper_wick < body * self._reversal_wick_max_pct) or \
                         (not is_bullish and lower_wick < body * self._reversal_wick_max_pct)
        if not has_small_wick:
            return None

        # 3. 在关键位附近（日内高点 / 低点 / 开盘区间边界）
        greeks = self._greeks
        rows = greeks.get("rows", [])
        underlying = Decimal(str(greeks.get("underlying_price", str(c))))

        near_key_level = False
        reversal_dir = ""
        # 顶部反转（大阴线在日内高点附近）
        if not is_bullish and self._day_high is not None and self._day_high > Decimal("0") and c < o:
            dist_to_high = (self._day_high - c) / self._day_high
            if dist_to_high < Decimal("0.003"):  # 0.3% 内
                near_key_level = True
                reversal_dir = "down"
        # 底部反转（大阳线在日内低点附近）
        if is_bullish and self._day_low is not None and self._day_low > Decimal("0") and c > o:
            dist_to_low = (c - self._day_low) / self._day_low
            if dist_to_low < Decimal("0.003"):
                near_key_level = True
                reversal_dir = "up"

        if not near_key_level:
            return None

        self._last_reversal_ts = now
        return self._build_signal(reversal_dir, underlying, rows, "reversal_bar")

    # ── 构建信号（选腿）───────────────────────────────

    def _build_signal(self, direction: str, underlying: Decimal,
                      rows: list[dict], trigger: str) -> dict | None:
        """根据方向从期权链选最优腿"""
        if not rows:
            return None

        # 从 greeks 快照推导到期日（GreekRow 无 symbol/expiry 字段）
        greeks = self._greeks or {}
        generated_at = greeks.get("generated_at", "")
        expiry_date = generated_at[:10].replace("-", "") if generated_at else ""

        instrument = {
            "asset_class": "OPTION",
            "symbol": "QQQ",
            "expiry": expiry_date,
        }

        option_right = "CALL" if direction == "up" else "PUT"
        # 选 ATM 或微虚值的腿（Delta 0.35~0.55）
        best_row, best_score = None, Decimal("999")
        for row in rows:
            if row.get("option_right", "") != option_right:
                continue
            strike = decimal_from_dict(row, "strike")
            delta = decimal_from_dict(row, "delta")
            dist = abs(strike - underlying)
            # Delta 筛选：做多选 0.35~0.55，做空选 -0.55~-0.35
            delta_ok = (direction == "up" and Decimal("0.30") <= delta <= Decimal("0.60")) or \
                       (direction == "down" and Decimal("-0.60") <= delta <= Decimal("-0.30"))
            if not delta_ok:
                continue
            if dist < best_score:
                best_score, best_row = dist, row

        if best_row is None:
            return None

        action = "BUY"  # 单腿买
        delta_val = float(decimal_from_dict(best_row, "delta"))
        confidence = Decimal("0.55")  # 价格行为默认 55% 信心
        if trigger == "second_entry":
            confidence = Decimal("0.65")  # 二次进场更高
        elif trigger == "reversal_bar":
            confidence = Decimal("0.60")

        instrument["symbol"] = best_row.get("symbol", "")  # fallback; 下面用构造值覆盖
        # 构造正确的期权 symbol（GreekRow 无 symbol 字段，必须手动构建）
        opt_right_char = "C" if option_right == "CALL" else "P"
        strike_val = best_row.get("strike", "0")
        strike_int = int(float(strike_val) * 1000)
        instrument["symbol"] = f"QQQ{expiry_date[2:]}{opt_right_char}{strike_int:06d}.US" if expiry_date else ""
        instrument["strike"] = str(strike_val)
        instrument["expiry"] = expiry_date
        instrument["option_right"] = option_right

        trigger_label = "二次进场" if trigger == "second_entry" else "反转Bar"
        return {
            "signal_id": f"signal-{self.strategy_id}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
            "strategy_id": self.strategy_id,
            "instrument": instrument,
            "action": action,
            "confidence": str(confidence),
            "reference_price": str(underlying),
            "reason": (
                f"PA·{trigger_label}: {option_right} @{best_row['strike']}, "
                f"Δ={delta_val:+.2f}, 方向={direction}, "
                f"bar={float(self.bars[-1]['c']):.2f}"
            ),
            "created_at": now_iso(),
        }


# ── 主入口 ────────────────────────────────────────────


async def main() -> None:
    nats_url = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
    strategy_mode = os.getenv("STRATEGY_MODE", "momentum")
    nc = await nats.connect(nats_url)

    # 订阅体制权重（在策略逻辑之前，确保不丢第一条 regime）
    await subscribe_regime(nc)

    # 订阅持仓（策略专属退出用）
    # 策略 ID 默认值与各策略构造函数保持一致
    _default_ids = {
        "theta_harvest": "theta_harvest_v0",
        "gamma_scalp": "gamma_scalp_v0",
        "price_action": "price_action_v1",
        "momentum": "momentum_v1",
    }
    strategy_id_for_positions = os.getenv("STRATEGY_ID", _default_ids.get(strategy_mode, "unknown"))
    await subscribe_positions(nc, strategy_id_for_positions)

    qqq_subject_key = "qqq"
    status_subject = f"market.option.{qqq_subject_key}.status"
    init_subject = f"init.option.{qqq_subject_key}"

    # 策略实例
    if strategy_mode == "theta_harvest":
        data_subject = os.getenv("GREEKS_SUBJECT", "greeks.option.qqq")
        strategy = ThetaHarvestStrategy(
            strategy_id=os.getenv("STRATEGY_ID", "theta_harvest_v0"),
            min_theta=Decimal(os.getenv("MIN_THETA", "0.05")),
            max_hours=Decimal(os.getenv("MAX_HOURS", "4")),
        )
    elif strategy_mode == "gamma_scalp":
        data_subject = os.getenv("GREEKS_SUBJECT", "greeks.option.qqq")
        strategy = GammaScalpStrategy(
            strategy_id=os.getenv("STRATEGY_ID", "gamma_scalp_v0"),
            min_gamma=Decimal(os.getenv("MIN_GAMMA", "0.02")),
        )
    elif strategy_mode == "price_action":
        greeks_subject = os.getenv("GREEKS_SUBJECT", "greeks.option.qqq")
        kline_subject = os.getenv("KLINE_SUBJECT", "kline.option.qqq")
        strategy = PriceActionStrategy(
            strategy_id=os.getenv("STRATEGY_ID", "price_action_v1"),
        )

        # 订阅体制权重（确保 price_action 也受 regime 门控制）
        await subscribe_regime(nc)
        # 订阅持仓（策略专属退出用）
        await subscribe_positions(nc, os.getenv("STRATEGY_ID", "price_action_v1"))

        print(f"strategy_engine v4: mode=price_action (Al Brooks)\n  greeks: {greeks_subject}\n  kline: {kline_subject}\n  status: {status_subject}")

        ready = {"greeks": False, "kline": False}

        async def handle_greeks(msg) -> None:
            data = json.loads(msg.data.decode())
            strategy.on_greeks(data)
            ready["greeks"] = True
            if ready["kline"]:
                signal = strategy.build_signal()
                if signal: await publish_signal(nc, signal)

        async def handle_kline(msg) -> None:
            data = json.loads(msg.data.decode())
            strategy.on_kline(data)
            ready["kline"] = True
            if ready["greeks"]:
                signal = strategy.build_signal()
                if signal: await publish_signal(nc, signal)

        async def handle_status(msg) -> None:
            data = json.loads(msg.data.decode())
            event = data.get("event", "")
            if event in ("OPEN", "CLOSE"):
                strategy.reset()
                print(f"[status] {event} — PA策略已重置")

        async def handle_init(msg) -> None:
            data = json.loads(msg.data.decode())
            strategy.on_init(data)

        await nc.subscribe(greeks_subject, cb=handle_greeks)
        await nc.subscribe(kline_subject, cb=handle_kline)
        await nc.subscribe(status_subject, cb=handle_status)
        await nc.subscribe(init_subject, cb=handle_init)

        print("strategy_engine 就绪，等待价格行为信号…")
        await asyncio.Event().wait()
        return
    else:
        # 动量模式：双订阅（state + greeks）
        state_subject = os.getenv("STATE_SUBJECT", "state.option.qqq")
        greeks_subject = os.getenv("GREEKS_SUBJECT", "greeks.option.qqq")
        strategy = MomentumStrategy(strategy_id=os.getenv("STRATEGY_ID", "momentum_v1"))

        print(f"strategy_engine v3: mode=momentum (多条件共振)\n  state: {state_subject}\n  greeks: {greeks_subject}\n  status: {status_subject}")

        async def handle_state(msg) -> None:
            data = json.loads(msg.data.decode())
            strategy.on_state(data)
            signal = strategy.build_signal()
            if signal: await publish_signal(nc, signal)

        async def handle_greeks(msg) -> None:
            data = json.loads(msg.data.decode())
            strategy.on_greeks(data)
            signal = strategy.build_signal()
            if signal: await publish_signal(nc, signal)

        async def handle_status(msg) -> None:
            data = json.loads(msg.data.decode())
            event = data.get("event", "")
            if event in ("OPEN", "CLOSE"):
                strategy.reset()
                print(f"[status] {event} — 策略已重置")

        async def handle_init(msg) -> None:
            data = json.loads(msg.data.decode())
            strategy.on_init(data)

        await nc.subscribe(state_subject, cb=handle_state)
        await nc.subscribe(greeks_subject, cb=handle_greeks)
        await nc.subscribe(status_subject, cb=handle_status)
        await nc.subscribe(init_subject, cb=handle_init)

        print("strategy_engine 就绪，等待多条件共振信号…")
        await asyncio.Event().wait()
        return

    # theta_harvest / gamma_scalp 逻辑（不变）
    print(f"strategy_engine v3: mode={strategy_mode}\n  data: {data_subject}\n  status: {status_subject}")

    async def handle_data(msg) -> None:
        data = json.loads(msg.data.decode())
        signal = strategy.build_signal(data)
        if signal: await publish_signal(nc, signal)

    async def handle_status(msg) -> None:
        data = json.loads(msg.data.decode())
        event = data.get("event", "")
        if event in ("OPEN", "CLOSE"):
            strategy.reset()
            print(f"[status] {event} — 策略已重置")

    async def handle_init(msg) -> None:
        data = json.loads(msg.data.decode())
        strategy.on_init(data)

    await nc.subscribe(data_subject, cb=handle_data)
    await nc.subscribe(status_subject, cb=handle_status)
    await nc.subscribe(init_subject, cb=handle_init)

    print("strategy_engine 就绪，等待消息…")
    await asyncio.Event().wait()


# ── 全局信号去重：按 (strategy_id, symbol) 冷却，防止多进程/快速重发 ──
_PUBLISHED_SIGNALS: dict[str, float] = {}
_PUBLISH_COOLDOWN_SECS = int(os.getenv("PUBLISH_COOLDOWN_SECS", "90"))


async def publish_signal(nc, signal: dict) -> None:
    instrument = signal["instrument"]
    symbol_key = "".join(ch for ch in instrument["symbol"].lower() if ch.isalnum())
    asset_key = instrument.get("asset_class", "option").lower()
    subject = f"raw.signal.{asset_key}.{symbol_key}"

    # ── 体制权重门：仅做允许/禁止判断，不缩放信心分 ──
    # 策略自身的信心计算已足够，体制权重只控制开/关
    strategy_id = signal.get("strategy_id", "")
    weight = get_regime_weight(strategy_id)
    if weight <= 0:
        return  # 体制不允许此策略，静默跳过

    # ── 全局去重：同一 (strategy, symbol) 在冷却期内不重发 ──
    dedup_key = f"{strategy_id}:{symbol_key}"
    now_ts = time.time()
    last_ts = _PUBLISHED_SIGNALS.get(dedup_key, 0)
    if now_ts - last_ts < _PUBLISH_COOLDOWN_SECS:
        return
    _PUBLISHED_SIGNALS[dedup_key] = now_ts

    # 信心分保持策略原值，风控用 MIN_CONFIDENCE 统一门槛

    await nc.publish(subject, json.dumps(signal).encode())
    print(f"[signal] {subject}: {signal['action']} — {signal['reason']}")


if __name__ == "__main__":
    asyncio.run(main())
