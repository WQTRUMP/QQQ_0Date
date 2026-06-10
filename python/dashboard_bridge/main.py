#!/usr/bin/env python3
"""
Dashboard Bridge — NATS → WebSocket 桥接 + Black-Scholes Greeks 实时计算

职责：
  1. 订阅 NATS 主题（quote.option.* / kline.option.qqq）
  2. 用 Black-Scholes 模型实时计算每个期权的 Delta/Gamma/Theta/Vega
  3. 聚合成看板需要的 JSON state
  4. WebSocket 推送给浏览器 / 同时 serve 看板 HTML

启动：
  cd QQQ_Single && source .env.longbridge && python python/dashboard_bridge/main.py
"""

import asyncio
import json
import os
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── 长桥 Trade API (账户余额) ─────────────────────────────────
try:
    from longbridge.openapi import Config as LBConfig, TradeContext as LBTradeContext
    LONG_AVAILABLE = True
except ImportError:
    LONG_AVAILABLE = False

import nats
from websockets.asyncio.server import serve as ws_serve
from websockets.exceptions import ConnectionClosed
from python.common.bootstrap import connect_nats_with_retry

# ── 带外静态文件服务（极简：在 WebSocket handshake 之前拦截 GET） ──
# 我们直接用 websockets 的 process_request 钩子来处理 HTTP GET

try:
    from scipy.stats import norm as scipy_norm
except ImportError:
    scipy_norm = None

from websockets import Response
from websockets.datastructures import Headers
import email.utils
import http

# ── 配置 ──────────────────────────────────────────────────
NATS_URL    = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
WS_HOST     = os.getenv("BRIDGE_HOST", "0.0.0.0")
WS_PORT     = int(os.getenv("BRIDGE_PORT", "8765"))
RISK_FREE   = float(os.getenv("RISK_FREE_RATE", "0.05"))  # 5% 无风险利率
PUSH_MS     = float(os.getenv("BRIDGE_PUSH_MS", "500"))    # 推送间隔（毫秒）

# HTML 看板文件路径
DASHBOARD_HTML = Path(
    os.getenv(
        "DASHBOARD_HTML",
        str(Path(__file__).resolve().parents[2] / "QQQ_0DTE_Dashboard.html"),
    )
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [bridge] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("dashboard-bridge")

# ── Black-Scholes Greeks ──────────────────────────────────

def bs_greeks(S, K, T, r, sigma, is_call):
    """
    Black-Scholes Greeks for European options.
    Returns (price, delta, gamma, theta, vega).
    S       = 标的价格
    K       = 行权价
    T       = 到期时间（年）
    r       = 无风险利率
    sigma   = 隐含波动率（小数，如 0.25 = 25%）
    is_call = True → Call, False → Put
    """
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        # 兜底：给最小 T 防止 Greeks 全 0（盘后/过期期权仍能展示）
        T = max(T, 1.0 / (365.25 * 24))  # 最小 1 小时
    if sigma <= 0:
        sigma = 0.15  # 默认 15% IV
    if S <= 0 or K <= 0:
        price = max(0, (S - K) if is_call else (K - S))
        delta = 1.0 if (is_call and S >= K) else (-1.0 if (not is_call and K >= S) else 0.0)
        return price, delta, 0.0, 0.0, 0.0

    if scipy_norm is None:
        # 无 scipy 时的极简近似（误差 ~5% 在 ATM 附近）
        d1_num = (S / K) if is_call else (K / S)
        d1 = (d1_num - 1) * (1 / (sigma * T**0.5))
        d2 = d1 - sigma * T**0.5
        nd1 = 0.5 + 0.5 * (d1 / (1 + abs(d1)))  # 粗近似
        nd2 = 0.5 + 0.5 * (d2 / (1 + abs(d2)))
        price = S * nd1 - K * nd2 if is_call else K * (1 - nd2) - S * (1 - nd1)
        return max(0, price), nd1 if is_call else nd1 - 1, 0.01, -0.001, 0.001

    d1 = (np.log(S / K) + (r + sigma**2 / 2) * T) / (sigma * T**0.5)
    d2 = d1 - sigma * T**0.5

    nd1_cdf = scipy_norm.cdf(d1)
    nd2_cdf = scipy_norm.cdf(d2)
    nd1_pdf = scipy_norm.pdf(d1)

    if is_call:
        price   = S * nd1_cdf - K * np.exp(-r * T) * nd2_cdf
        delta   = nd1_cdf
    else:
        price   = K * np.exp(-r * T) * (1 - nd2_cdf) - S * (1 - nd1_cdf)
        delta   = nd1_cdf - 1

    gamma   = nd1_pdf / (S * sigma * T**0.5)
    theta   = (-S * nd1_pdf * sigma / (2 * T**0.5)
               - r * K * np.exp(-r * T) * (nd2_cdf if is_call else (1 - nd2_cdf))) / 365
    vega    = S * nd1_pdf * T**0.5 / 100  # 1% IV 变化的 Vega

    return max(0, price), delta, max(0, gamma), theta, max(0, vega)

# 需要 numpy（scipy 依赖它）
try:
    import numpy as np
except ImportError:
    np = None

# ── 状态管理 ──────────────────────────────────────────────

class DashboardState:
    """线程安全的状态聚合器"""

    def __init__(self):
        self.qqq_price    = 0.0
        self.qqq_change   = 0.0
        self.qqq_prev_close = 0.0
        self.qqq_volume   = 0
        self.qqq_high     = 0.0
        self.qqq_low      = 0.0
        self.vix_price    = 0.0
        self.vix_change   = 0.0
        self.qqq_open     = 0.0
        self.iv_rank      = 50  # 占位
        self.expiry_date  = ""
        self.expiry_dt    = None  # datetime
        self.market_open  = False  # 美股是否开盘
        self.session_start = None  # 今日开盘时间（ET）

        # ── 账户资产 ──
        self.total_assets    = 0.0   # 总资产
        self.total_cash      = 0.0   # 现金
        self.available_cash  = 0.0   # 可用资金
        self.market_value    = 0.0   # 持仓市值
        self.buying_power    = 0.0   # 购买力
        self.margin_call     = 0.0   # 保证金
        self.unrealized_pnl  = 0.0   # 未实现盈亏
        self.currency        = "USD"
        self.account_updated = 0.0   # 最后更新时间戳

        # 期权快照: symbol_key → option data
        self.options: dict[str, dict] = {}
        # ── 市场体制（来自 market_regime）──
        self.regime: dict = {}
        # ── 最新信号（最多保留 5 条）──
        self.signals: list[dict] = []
        # ── 当前持仓（来自 position_tracker）──
        self.positions: list[dict] = []
        self._lock = asyncio.Lock()

    async def update_qqq(self, data: dict):
        async with self._lock:
            try:
                px = float(data.get("last_done", 0))
                if px <= 0: return
                prev = self.qqq_price
                self.qqq_price = px
                if prev > 0:
                    self.qqq_change = px - prev
                self.qqq_volume   = int(data.get("volume", 0))
                self.qqq_high     = float(data.get("high", 0) or px)
                self.qqq_low      = float(data.get("low", 0) or px)
                self.qqq_open     = float(data.get("open", 0) or px)
                prev_close = float(data.get("prev_close", 0))
                if prev_close > 0:
                    self.qqq_prev_close = prev_close
                self.market_open = True
            except (ValueError, TypeError):
                pass

    async def update_vix(self, data: dict):
        async with self._lock:
            try:
                px = float(data.get("last_done", 0))
                if px > 0:
                    self.vix_price = px
            except (ValueError, TypeError):
                pass

    async def update_option(self, symbol_key: str, data: dict):
        async with self._lock:
            try:
                last_done = float(data.get("last_done", 0))
                volume    = int(data.get("volume", 0))
                ext = data.get("option_extend", {}) or {}
                iv   = float(ext.get("implied_volatility", "0") or "0")
                oi   = int(ext.get("open_interest", 0) or 0)
                strike = float(ext.get("strike_price", "0") or "0")
                direction = ext.get("direction", "")
                expiry    = ext.get("expiry_date", "")
                hv = float(ext.get("historical_volatility", "0") or "0")
                symbol_full = data.get("symbol", "")

                self.options[symbol_key] = {
                    "symbol":     symbol_full,
                    "strike":     strike,
                    "direction":  direction,  # "C" or "P"
                    "last":       last_done,
                    "volume":     volume,
                    "oi":         oi,
                    "iv":         iv,
                    "hv":         hv,
                    "expiry":     expiry,
                    "updated_at": time.time(),
                }
                if expiry and not self.expiry_date:
                    self.expiry_date = expiry
                    try:
                        self.expiry_dt = datetime.strptime(expiry, "%Y%m%d")
                    except ValueError:
                        pass

                # 兜底：若 QQQ 现价为 0，从期权链估算（取最小 bid-ask spread 的行权价）
                if self.qqq_price <= 0 and len(self.options) >= 4:
                    self._derive_qqq_from_options()
            except (ValueError, TypeError):
                pass

    def _derive_qqq_from_options(self):
        """从期权链估算 QQQ 现价"""
        calls_by_strike = {}
        puts_by_strike = {}
        for key, opt in self.options.items():
            s = opt["strike"]
            if opt["direction"].upper() == "C":
                calls_by_strike.setdefault(s, []).append(opt)
            else:
                puts_by_strike.setdefault(s, []).append(opt)

        # 找 call/put 都有的行权价，选最近更新的
        best_strike = 0
        best_time = 0.0
        for s in calls_by_strike:
            if s in puts_by_strike:
                for opt in calls_by_strike[s] + puts_by_strike[s]:
                    if opt.get("updated_at", 0) > best_time:
                        best_strike = s
                        best_time = opt["updated_at"]

        if best_strike > 0 and self.qqq_price <= 0:
            self.qqq_price = best_strike
            log.info(f"QQQ 现价推定: ${best_strike:.2f} (基于期权链 ATM)")

    async def update_kline(self, data: dict):
        """K线可用于更新日内高低点"""
        async with self._lock:
            try:
                hi = float(data.get("high", 0))
                lo = float(data.get("low", 0))
                cl = float(data.get("close", 0))
                if hi > 0 and self.qqq_high > 0:
                    self.qqq_high = max(self.qqq_high, hi)
                elif hi > 0:
                    self.qqq_high = hi
                if lo > 0 and self.qqq_low > 0:
                    self.qqq_low = min(self.qqq_low, lo)
                elif lo > 0:
                    self.qqq_low = lo
            except (ValueError, TypeError):
                pass

    async def update_regime(self, data: dict):
        """market_regime 体制更新"""
        async with self._lock:
            self.regime = data

    async def update_signal(self, data: dict):
        """策略信号（来自 signal.option.*）"""
        async with self._lock:
            self.signals.insert(0, data)
            if len(self.signals) > 5:
                self.signals = self.signals[:5]

    async def update_positions(self, data: dict):
        """持仓快照（来自 position.option.qqq）"""
        async with self._lock:
            self.positions = data.get("positions", [])

    def _compute_greeks(self, strike: float, direction: str, iv: float) -> dict:
        """为单个期权计算 BS Greeks"""
        S = self.qqq_price
        if S <= 0 or strike <= 0 or iv <= 0:
            return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0}

        # 计算到期时间 T（年）
        if self.expiry_dt:
            now_utc = datetime.now(timezone.utc)
            # 期权到期通常是美东 16:00
            expiry_et = self.expiry_dt.replace(
                hour=16, minute=0, second=0, tzinfo=timezone(timedelta(hours=-4))
            )
            T = max(0, (expiry_et - now_utc).total_seconds() / (365.25 * 24 * 3600))
        else:
            # 默认 1 天
            T = 1.0 / 365.25

        is_call = (direction.upper() == "C")
        try:
            price, delta, gamma, theta, vega = bs_greeks(S, strike, T, RISK_FREE, iv, is_call)
        except Exception:
            return {"delta": 0, "gamma": 0, "theta": 0, "vega": 0}

        return {
            "price": round(price, 4),
            "delta": round(delta, 4),
            "gamma": round(gamma, 4),
            "theta": round(theta, 4),
            "vega":  round(vega, 4),
        }

    async def snapshot(self) -> dict:
        """生成发给前端的完整状态快照"""
        async with self._lock:
            px = self.qqq_price
            prev_close = self.qqq_prev_close
            chg = px - prev_close if prev_close > 0 else 0
            chg_pct = (chg / prev_close * 100) if prev_close > 0 else 0

            # ── 构建期权链 ──
            strikes_map: dict[float, dict] = {}
            for key, opt in self.options.items():
                s = opt["strike"]
                if s <= 0: continue
                if s not in strikes_map:
                    strikes_map[s] = {"strike": s, "call": None, "put": None}
                greeks = self._compute_greeks(s, opt["direction"], opt["iv"])
                leg = {
                    "symbol":  opt["symbol"],
                    "bid":     round(opt["last"], 4),     # 目前用 last_done 近似 bid
                    "ask":     round(opt["last"] * 1.005, 4) if opt["last"] > 0 else 0,  # 简易 spread
                    "volume":  opt["volume"],
                    "oi":      opt["oi"],
                    "iv":      round(opt["iv"], 4),
                    **greeks,
                }
                if opt["direction"].upper() == "C":
                    strikes_map[s]["call"] = leg
                else:
                    strikes_map[s]["put"] = leg

            # 按行权价排序
            chain = [v for _, v in sorted(strikes_map.items())]

            # ── 量化指标（基于期权链计算的占位） ──
            net_gamma = sum(
                (c["call"]["gamma"] if c["call"] else 0) -
                (c["put"]["gamma"] if c["put"] else 0)
                for c in chain
            ) * 100  # 放大到 $M 量级便于显示
            total_call_oi = sum(c["call"]["oi"] for c in chain if c["call"])
            total_put_oi  = sum(c["put"]["oi"] for c in chain if c["put"])
            cpr = round(total_call_oi / total_put_oi, 2) if total_put_oi > 0 else 1.0

            # ── Theta / IV 衰减进度 ──
            theta_pct = 100
            iv_decay_pct = 0
            if self.expiry_dt:
                now_et = datetime.now(timezone(timedelta(hours=-4)))
                market_open_et = self.expiry_dt.replace(
                    hour=9, minute=30, second=0, tzinfo=timezone(timedelta(hours=-4))
                )
                market_close_et = self.expiry_dt.replace(
                    hour=16, minute=0, second=0, tzinfo=timezone(timedelta(hours=-4))
                )
                total_minutes = (market_close_et - market_open_et).total_seconds() / 60
                elapsed = (now_et - market_open_et).total_seconds() / 60
                if total_minutes > 0:
                    theta_pct = max(0, min(100, round((1 - elapsed / total_minutes) * 100)))
                    iv_decay_pct = max(0, min(100, round(elapsed / total_minutes * 100)))

            return {
                "ts": time.time(),
                "underlying": {
                    "symbol":     "QQQ",
                    "price":      round(px, 2),
                    "change":     round(chg, 2),
                    "change_pct": round(chg_pct, 2),
                    "volume":     self.qqq_volume,
                    "high":       round(self.qqq_high, 2) if self.qqq_high else round(px, 2),
                    "low":        round(self.qqq_low, 2) if self.qqq_low else round(px, 2),
                    "open":       round(self.qqq_open, 2),
                    "prev_close": round(prev_close, 2),
                    "vix":        round(self.vix_price, 2),
                    "iv_rank":    self.iv_rank,
                    "market_open": self.market_open,
                },
                "chain": chain,
                "quant": {
                    "net_gamma":   round(net_gamma, 1),
                    "gex":         round(net_gamma / 100, 2),
                    "cpr":         cpr,
                    "gamma_flip":  round(self.regime.get("gamma_flip", px), 2),
                    "max_pain":    round(self.regime.get("max_pain", px), 2),
                    "gf_dist_pct": round(self.regime.get("gf_dist_pct", 0), 2),
                },
                "theta_pct":    theta_pct,
                "iv_decay_pct": iv_decay_pct,
                "signals":      self.signals,
                "positions":    self.positions,
                "market_regime": {
                    "regime":      self.regime.get("regime", "unknown"),
                    "reason":      self.regime.get("reason", ""),
                    "weights":     self.regime.get("weights", {}),
                    "circuit_breaker": self.regime.get("circuit_breaker", False),
                    "confidence":  self._signal_confidence(),
                },
                "account": {
                    "total_assets":    round(self.total_assets, 2),
                    "total_cash":      round(self.total_cash, 2),
                    "available_cash":  round(self.available_cash, 2),
                    "market_value":    round(self.market_value, 2),
                    "buying_power":    round(self.buying_power, 2),
                    "margin_call":     round(self.margin_call, 2),
                    "unrealized_pnl":  round(self.unrealized_pnl, 2),
                    "currency":        self.currency,
                    "updated":         self.account_updated,
                },
            }

    def _signal_confidence(self) -> int:
        """最新信号的最高置信度"""
        if not self.signals:
            return 0
        return max(int(float(s.get("confidence", 0)) * 100) for s in self.signals)


# ── 账户余额拉取 ─────────────────────────────────────────────

async def account_loop(state: DashboardState):
    """每 30 秒从长桥拉取账户余额（复用 TradeContext）"""
    if not LONG_AVAILABLE:
        log.warning("longbridge SDK 未安装，跳过账户拉取")
        return

    config = None
    try:
        env_file = Path(__file__).parent.parent.parent / ".env.longbridge"
        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, val = line.partition("=")
                    key, val = key.strip(), val.strip().strip('"').strip("'")
                    if key and val and key not in os.environ:
                        os.environ[key] = val
        config = LBConfig.from_apikey_env()
    except Exception as e:
        log.error(f"长桥 Config 创建失败: {e}")
        return

    # 复用 TradeContext，避免每 30s 重建连接
    trade_ctx = LBTradeContext(config)

    while True:
        try:
            balance = trade_ctx.account_balance()
            if balance and len(balance) > 0:
                b = balance[0]
                # 可用资金：取 USD 的 cash_info
                avail = 0.0
                if getattr(b, "cash_infos", None):
                    for ci in b.cash_infos:
                        if getattr(ci, "currency", "") == "USD":
                            avail = _safe_float(ci.available_cash)
                            break
                    if avail == 0.0 and len(b.cash_infos) > 0:
                        avail = _safe_float(b.cash_infos[0].available_cash)

                async with state._lock:
                    state.total_assets   = _safe_float(b.net_assets)
                    state.total_cash     = _safe_float(b.total_cash)
                    state.available_cash = avail
                    state.market_value   = state.total_assets - state.total_cash
                    state.buying_power   = _safe_float(b.buy_power)
                    state.margin_call    = _safe_float(b.margin_call)
                    state.unrealized_pnl = 0.0  # 长桥不直接提供 unrealized_pnl
                    state.currency       = getattr(b.cash_infos[0], "currency", "USD") if b.cash_infos else "USD"
                    state.account_updated = time.time()
                log.info(f"账户: 总${state.total_assets:,.0f} 可用${state.available_cash:,.0f} 持仓${state.market_value:,.0f}")
        except Exception as e:
            log.warning(f"账户拉取失败: {e}")

        await asyncio.sleep(30)


def _safe_float(v) -> float:
    try:
        return float(str(v))
    except (ValueError, TypeError):
        return 0.0


# ── WebSocket + HTTP 服务器 ──────────────────────────────────


class DashboardServer:
    """WebSocket 推送服务器 + HTML 静态文件"""

    def __init__(self, state: DashboardState):
        self.state = state
        self.clients: set = set()

    async def http_handler(self, connection, request):
        """process_request 钩子：拦截 HTTP GET 返回 HTML"""
        # WebSocket 升级请求 → 放行
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return None
        if request.path == "/healthz":
            body = json.dumps({
                "status": "ok",
                "html_present": DASHBOARD_HTML.exists(),
                "clients": len(self.clients),
            }).encode("utf-8")
            headers = Headers([
                ("Date", email.utils.formatdate(usegmt=True)),
                ("Connection", "close"),
                ("Content-Length", str(len(body))),
                ("Content-Type", "application/json; charset=utf-8"),
            ])
            return Response(200, "OK", headers, body)
        if request.path == "/" or request.path == "/index.html":
            if DASHBOARD_HTML.exists():
                html = DASHBOARD_HTML.read_text(encoding="utf-8")
                body = html.encode("utf-8")
                headers = Headers([
                    ("Date", email.utils.formatdate(usegmt=True)),
                    ("Connection", "close"),
                    ("Content-Length", str(len(body))),
                    ("Content-Type", "text/html; charset=utf-8"),
                ])
                return Response(200, "OK", headers, body)
            else:
                body = (
                    "<!doctype html><html><body><h1>Dashboard HTML missing</h1>"
                    "<p>Set DASHBOARD_HTML or add QQQ_0DTE_Dashboard.html at repo root.</p>"
                    "</body></html>"
                ).encode("utf-8")
                headers = Headers([
                    ("Date", email.utils.formatdate(usegmt=True)),
                    ("Connection", "close"),
                    ("Content-Length", str(len(body))),
                    ("Content-Type", "text/html; charset=utf-8"),
                ])
                return Response(200, "OK", headers, body)
        # 非 WebSocket 的普通 HTTP 请求 → 404，避免触发升级失败
        return connection.respond(404, "Not Found")

    async def ws_handler(self, websocket):
        """WebSocket 连接处理"""
        self.clients.add(websocket)
        addr = websocket.remote_address
        log.info(f"浏览器已连接: {addr} (共 {len(self.clients)} 个客户端)")
        try:
            # 立即发送当前状态
            snapshot = await self.state.snapshot()
            await websocket.send(json.dumps(snapshot))

            # 保持连接，等待客户端消息（目前只是心跳）
            async for _ in websocket:
                pass
        except ConnectionClosed:
            pass
        except Exception as e:
            log.warning(f"WebSocket 错误: {e}")
        finally:
            self.clients.discard(websocket)
            log.info(f"浏览器断开: {addr} (剩余 {len(self.clients)} 个客户端)")

    async def broadcast_loop(self):
        """定时向所有客户端推送状态"""
        while True:
            await asyncio.sleep(PUSH_MS / 1000.0)
            if not self.clients:
                continue
            try:
                snapshot = await self.state.snapshot()
                data = json.dumps(snapshot)
                # 并发推送给所有客户端
                disconnected = set()
                for ws in self.clients:
                    try:
                        await ws.send(data)
                    except Exception:
                        disconnected.add(ws)
                self.clients -= disconnected
                if disconnected:
                    log.info(f"清理 {len(disconnected)} 个断连客户端")
            except Exception as e:
                log.error(f"广播失败: {e}")


# ── NATS 订阅 ───────────────────────────────────────────────

async def nats_loop(state: DashboardState):
    """订阅 NATS 并更新状态"""
    nc = await connect_nats_with_retry(NATS_URL, "dashboard_bridge")

    async def on_qqq(msg):
        try:
            data = json.loads(msg.data.decode())
            await state.update_qqq(data)
        except (json.JSONDecodeError, Exception):
            pass

    async def on_vix(msg):
        try:
            data = json.loads(msg.data.decode())
            await state.update_vix(data)
        except (json.JSONDecodeError, Exception):
            pass

    async def on_option(msg):
        try:
            data = json.loads(msg.data.decode())
            # 从 subject 提取 symbol_key
            # subject 格式: quote.option.{symbol_key}
            subject = msg.subject
            if subject.startswith("quote.option."):
                symbol_key = subject[len("quote.option."):]
                # 跳过 qqq 和 vix（已单独处理）
                if symbol_key in ("qqqus", "vix", ""):
                    return
                await state.update_option(symbol_key, data)
        except (json.JSONDecodeError, Exception):
            pass

    async def on_kline(msg):
        try:
            data = json.loads(msg.data.decode())
            await state.update_kline(data)
        except (json.JSONDecodeError, Exception):
            pass

    async def on_regime(msg):
        try:
            data = json.loads(msg.data.decode())
            await state.update_regime(data)
        except (json.JSONDecodeError, Exception):
            pass

    async def on_signal(msg):
        try:
            data = json.loads(msg.data.decode())
            await state.update_signal(data)
        except (json.JSONDecodeError, Exception):
            pass

    async def on_positions(msg):
        try:
            data = json.loads(msg.data.decode())
            await state.update_positions(data)
        except (json.JSONDecodeError, Exception):
            pass

    # 订阅所有相关主题
    await nc.subscribe("quote.option.qqqus", cb=on_qqq)
    await nc.subscribe("quote.option.vix",   cb=on_vix)
    await nc.subscribe("quote.option.>",     cb=on_option)
    await nc.subscribe("kline.option.qqq",   cb=on_kline)
    await nc.subscribe("regime.option.qqq",  cb=on_regime)
    await nc.subscribe("signal.option.>",    cb=on_signal)
    await nc.subscribe("position.option.qqq", cb=on_positions)

    log.info("NATS 订阅就绪: quote + kline + regime + signal + positions")

    # ── 每 15 秒推 buying_power 到 NATS（供风控引擎消费）──
    async def publish_balance():
        while True:
            await asyncio.sleep(15)
            bp = state.buying_power
            payload = json.dumps({
                "buying_power": bp,
                "available_cash": state.available_cash,
                "total_assets": state.total_assets,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            await nc.publish("balance.option.qqq", payload.encode())

    asyncio.create_task(publish_balance())

    # 永久保持连接
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        await nc.drain()
        log.info("NATS 已断开")


# ── 主入口 ──────────────────────────────────────────────────

async def main():
    log.info(f"Dashboard Bridge 启动中...")
    log.info(f"  NATS: {NATS_URL}")
    log.info(f"  WS:   {WS_HOST}:{WS_PORT}")
    log.info(f"  HTML: {DASHBOARD_HTML}")

    state = DashboardState()
    server = DashboardServer(state)

    # 并行跑：NATS 订阅 + WebSocket 服务 + 定时广播 + 账户拉取
    tasks = [
        asyncio.create_task(nats_loop(state)),
        asyncio.create_task(server.broadcast_loop()),
        asyncio.create_task(account_loop(state)),
    ]
    # WebSocket 服务器（同时处理 HTTP GET 和 WS 升级）
    ws_server = await ws_serve(
        server.ws_handler,
        WS_HOST, WS_PORT,
        process_request=server.http_handler,
    )
    log.info(f"看板地址: http://localhost:{WS_PORT}")
    tasks.append(asyncio.create_task(ws_server.serve_forever()))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
