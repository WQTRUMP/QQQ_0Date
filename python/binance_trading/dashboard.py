"""
[INPUT]: 依赖运行时对象的 snapshot/ready、托管保护生命周期和根目录 Binance_Dashboard.html 静态页
[OUTPUT]: 对外提供 DashboardSnapshotSource、DashboardServer 与 create_dashboard_server 本地监控接口，投影保护状态但不暴露订单身份
[POS]: binance_trading 的只读展示边界，将交易运行时快照收敛为固定字段并默认拒绝就绪
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import enum
import json
import logging
import math
import socket
import threading
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple
from urllib.parse import urlsplit


LOGGER = logging.getLogger(__name__)
SNAPSHOT_SCHEMA_VERSION = "binance-dashboard-v1"
LOOPBACK_HOSTS = frozenset(("127.0.0.1", "::1", "localhost"))
SENSITIVE_KEYS = frozenset(
    (
        "api_key",
        "api_secret",
        "authorization",
        "listen_key",
        "password",
        "secret",
        "signature",
        "token",
    )
)
SENSITIVE_KEYS_NORMALIZED = frozenset(key.replace("_", "") for key in SENSITIVE_KEYS)
SECURITY_HEADERS = {
    "Cache-Control": "no-store, max-age=0",
    "Content-Security-Policy": (
        "default-src 'none'; script-src 'self'; style-src 'self'; "
        "connect-src 'self'; img-src 'self' data:; font-src 'none'; "
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'; "
        "object-src 'none'"
    ),
    "Cross-Origin-Opener-Policy": "same-origin",
    "Permissions-Policy": "camera=(), microphone=(), geolocation=(), payment=()",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}


DASHBOARD_CSS = r"""
:root {
  color-scheme: dark;
  --bg: #07100f;
  --panel: #0d1917;
  --panel-2: #10211e;
  --line: #203632;
  --text: #e8f2ef;
  --muted: #8ca39d;
  --accent: #f0b90b;
  --positive: #2bd9a8;
  --negative: #ff6276;
  --warning: #f7c85a;
  --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  min-width: 320px;
  background:
    radial-gradient(circle at 75% -10%, rgba(240, 185, 11, .11), transparent 33rem),
    linear-gradient(180deg, #07100f 0%, #060c0b 100%);
  color: var(--text);
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
.shell { width: min(1480px, 100%); margin: 0 auto; padding: 22px; }
.topbar { display: flex; align-items: flex-start; justify-content: space-between; gap: 20px; margin-bottom: 18px; }
.eyebrow { color: var(--accent); font: 700 11px/1.4 var(--mono); letter-spacing: .16em; text-transform: uppercase; }
h1 { margin: 5px 0 4px; font-size: clamp(24px, 4vw, 40px); letter-spacing: -.035em; }
.subtitle { color: var(--muted); font-size: 13px; }
.status-cluster { display: flex; flex-wrap: wrap; justify-content: flex-end; gap: 8px; }
.pill { border: 1px solid var(--line); border-radius: 999px; padding: 8px 11px; background: rgba(13, 25, 23, .9); color: var(--muted); font: 700 11px/1 var(--mono); }
.pill[data-state="ready"] { border-color: rgba(43, 217, 168, .48); color: var(--positive); }
.pill[data-state="blocked"] { border-color: rgba(255, 98, 118, .48); color: var(--negative); }
.pill[data-state="waiting"] { border-color: rgba(247, 200, 90, .45); color: var(--warning); }
.grid { display: grid; grid-template-columns: repeat(12, minmax(0, 1fr)); gap: 12px; }
.panel { grid-column: span 4; min-width: 0; border: 1px solid var(--line); border-radius: 14px; background: linear-gradient(145deg, rgba(16, 33, 30, .96), rgba(10, 20, 18, .96)); box-shadow: 0 16px 36px rgba(0,0,0,.18); overflow: hidden; }
.panel.wide { grid-column: span 8; }
.panel.full { grid-column: 1 / -1; }
.panel-head { display: flex; justify-content: space-between; align-items: baseline; gap: 12px; padding: 14px 16px; border-bottom: 1px solid var(--line); }
.panel-head h2 { margin: 0; font-size: 13px; letter-spacing: .04em; }
.source { color: var(--muted); font: 10px/1.3 var(--mono); text-transform: uppercase; }
.metrics { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); }
.metric { min-height: 93px; padding: 15px 16px; border-right: 1px solid var(--line); border-bottom: 1px solid var(--line); }
.metric:nth-child(2n) { border-right: 0; }
.metric-label { display: block; margin-bottom: 9px; color: var(--muted); font: 10px/1.3 var(--mono); letter-spacing: .08em; text-transform: uppercase; }
.metric-value { display: block; overflow: hidden; color: var(--text); font: 700 clamp(16px, 2.2vw, 25px)/1.15 var(--mono); text-overflow: ellipsis; white-space: nowrap; }
.book { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; padding: 18px 16px; }
.book-cell { min-width: 0; }
.book-cell .label { color: var(--muted); font: 10px/1.3 var(--mono); text-transform: uppercase; }
.book-cell .value { display: block; margin-top: 7px; font: 700 20px/1.15 var(--mono); overflow: hidden; text-overflow: ellipsis; }
.bid { color: var(--positive); }
.ask { color: var(--negative); }
.risk-body { padding: 16px; }
.risk-banner { display: flex; justify-content: space-between; gap: 16px; padding: 14px; border: 1px solid var(--line); border-radius: 10px; background: rgba(7, 16, 15, .6); }
.risk-state { font: 800 15px/1.2 var(--mono); text-transform: uppercase; }
.risk-reason { margin-top: 6px; color: var(--muted); font-size: 12px; }
.risk-values { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; margin-top: 10px; }
.risk-value { padding: 10px 12px; border: 1px solid var(--line); border-radius: 9px; }
.risk-value span { display: block; color: var(--muted); font: 10px/1.2 var(--mono); text-transform: uppercase; }
.risk-value strong { display: block; margin-top: 6px; font: 700 15px/1 var(--mono); }
.table-wrap { overflow: auto; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th { padding: 10px 13px; color: var(--muted); font: 10px/1.2 var(--mono); text-align: right; text-transform: uppercase; white-space: nowrap; }
td { padding: 11px 13px; border-top: 1px solid var(--line); font-family: var(--mono); text-align: right; white-space: nowrap; }
th:first-child, td:first-child { text-align: left; }
.empty { padding: 25px 16px; color: var(--muted); font-size: 12px; text-align: center; }
.events { max-height: 290px; overflow: auto; }
.event { display: grid; grid-template-columns: 145px 90px 1fr; gap: 12px; padding: 11px 16px; border-top: 1px solid var(--line); font-size: 12px; }
.event:first-child { border-top: 0; }
.event-time, .event-kind { color: var(--muted); font-family: var(--mono); }
.event-message { min-width: 0; overflow-wrap: anywhere; }
.footer { display: flex; justify-content: space-between; gap: 20px; margin-top: 14px; color: var(--muted); font: 10px/1.4 var(--mono); }
@media (max-width: 980px) { .panel, .panel.wide { grid-column: span 6; } .panel.full { grid-column: 1 / -1; } }
@media (max-width: 680px) { .shell { padding: 14px; } .topbar { display: block; } .status-cluster { justify-content: flex-start; margin-top: 14px; } .panel, .panel.wide, .panel.full { grid-column: 1 / -1; } .event { grid-template-columns: 1fr; gap: 4px; } .footer { display: block; } .footer span { display: block; margin-top: 5px; } }
"""


DASHBOARD_JS = r"""
(() => {
  "use strict";

  const byId = (id) => document.getElementById(id);
  const finite = (value) => value !== null && value !== undefined && value !== "" && Number.isFinite(Number(value));
  const number = (value, digits = 2) => {
    if (!finite(value)) return "--";
    const parsed = Number(value);
    return parsed.toLocaleString("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits });
  };
  const compact = (value) => {
    if (!finite(value)) return "--";
    const parsed = Number(value);
    return parsed.toLocaleString("en-US", { maximumFractionDigits: 8 });
  };
  const percent = (value) => {
    if (!finite(value)) return "--";
    const parsed = Number(value);
    return `${(parsed * 100).toFixed(4)}%`;
  };
  const utc = (value) => {
    if (value === null || value === undefined || value === "") return "--";
    const parsed = typeof value === "number" && value < 100000000000 ? value * 1000 : value;
    const date = new Date(parsed);
    return Number.isNaN(date.getTime()) ? String(value) : date.toISOString().replace("T", " ").replace(".000Z", "Z");
  };
  const set = (id, value) => { const node = byId(id); if (node) node.textContent = value; };
  const pick = (object, keys) => {
    for (const key of keys) if (object && object[key] !== null && object[key] !== undefined) return object[key];
    return null;
  };

  function renderPositions(rows) {
    const body = byId("positions-body");
    const empty = byId("positions-empty");
    body.replaceChildren();
    const positions = Array.isArray(rows) ? rows : [];
    empty.hidden = positions.length > 0;
    for (const position of positions) {
      const cells = [
        pick(position, ["symbol"]),
        pick(position, ["side", "position_side"]),
        pick(position, ["quantity", "position_amt", "qty"]),
        pick(position, ["entry_price"]),
        pick(position, ["mark_price"]),
        pick(position, ["liquidation_price"]),
        pick(position, ["leverage"]),
        pick(position, ["unrealized_pnl", "unrealised_pnl"]),
      ];
      const row = document.createElement("tr");
      cells.forEach((value, index) => {
        const cell = document.createElement("td");
        cell.textContent = index < 2 ? (value ?? "--") : compact(value);
        row.appendChild(cell);
      });
      body.appendChild(row);
    }
  }

  function renderEvents(rows) {
    const container = byId("events");
    container.replaceChildren();
    const events = Array.isArray(rows) ? rows.slice(-50).reverse() : [];
    if (events.length === 0) {
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = "No runtime events";
      container.appendChild(empty);
      return;
    }
    for (const item of events) {
      const row = document.createElement("div");
      row.className = "event";
      const time = document.createElement("span");
      time.className = "event-time";
      time.textContent = utc(pick(item, ["timestamp", "time", "event_time"]));
      const kind = document.createElement("span");
      kind.className = "event-kind";
      kind.textContent = String(pick(item, ["kind", "type", "level"]) ?? "runtime");
      const message = document.createElement("span");
      message.className = "event-message";
      message.textContent = String(pick(item, ["message", "reason", "detail"]) ?? JSON.stringify(item));
      row.append(time, kind, message);
      container.appendChild(row);
    }
  }

  function render(snapshot) {
    const market = snapshot.market || {};
    const book = market.book || {};
    const mark = market.mark || {};
    const account = snapshot.account || {};
    const risk = snapshot.risk || {};
    set("mode", String(snapshot.mode || "paper").toUpperCase());
    set("symbol", market.symbol || "BTCUSDT");
    set("contract", market.contract_type || "PERPETUAL");
    set("generated-at", utc(snapshot.generated_at));
    set("book-updated", utc(book.updated_at));
    set("bid-price", number(book.bid_price));
    set("bid-qty", compact(book.bid_qty));
    set("ask-price", number(book.ask_price));
    set("ask-qty", compact(book.ask_qty));
    const bid = Number(book.bid_price);
    const ask = Number(book.ask_price);
    set("spread", finite(book.bid_price) && finite(book.ask_price) ? number(ask - bid) : "--");
    set("mark-price", number(mark.mark_price));
    set("index-price", number(mark.index_price));
    set("funding-rate", percent(mark.funding_rate));
    set("next-funding", utc(mark.next_funding_time));
    set("mark-updated", utc(mark.updated_at));
    set("wallet-balance", number(account.wallet_balance));
    set("margin-balance", number(account.margin_balance));
    set("available-balance", number(account.available_balance));
    set("unrealized-pnl", number(account.unrealized_pnl));
    set("risk-state", String(risk.state || "blocked").toUpperCase());
    set("risk-reason", risk.reason || "Risk state unavailable");
    set("can-open", risk.can_open === true ? "YES" : "NO");
    set("daily-drawdown", percent(risk.daily_drawdown_pct));
    const protection = snapshot.protection || {};
    set("protection-state", String(protection.state || "UNAVAILABLE").toUpperCase());
    set("protection-winner", protection.winner || "--");
    const readiness = byId("readiness");
    readiness.textContent = snapshot.ready === true ? "READY" : "BLOCKED";
    readiness.dataset.state = snapshot.ready === true ? "ready" : "blocked";
    renderPositions(snapshot.positions);
    renderEvents(snapshot.events);
  }

  async function poll() {
    try {
      const response = await fetch("/api/snapshot", { cache: "no-store", credentials: "same-origin" });
      if (!response.ok) throw new Error(`snapshot HTTP ${response.status}`);
      render(await response.json());
      const connection = byId("connection");
      connection.textContent = "CONNECTED";
      connection.dataset.state = "ready";
    } catch (error) {
      const connection = byId("connection");
      connection.textContent = "DISCONNECTED";
      connection.dataset.state = "blocked";
      set("risk-reason", error instanceof Error ? error.message : "Snapshot unavailable");
    }
  }

  function tickClock() { set("utc-clock", new Date().toISOString().replace("T", " ").replace(".000Z", "Z")); }
  tickClock();
  poll();
  window.setInterval(tickClock, 1000);
  window.setInterval(poll, 1500);
})();
"""


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _plain_mapping(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        candidate = to_dict()
        if isinstance(candidate, Mapping):
            return dict(candidate)
    return {}


def _is_sensitive_key(key: Any) -> bool:
    rendered = str(key).lower()
    normalized = "".join(character for character in rendered if character.isalnum())
    return rendered in SENSITIVE_KEYS or normalized in SENSITIVE_KEYS_NORMALIZED


def _safe_value(value: Any, key: str = "") -> Any:
    if _is_sensitive_key(key):
        return None
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (dt.datetime, dt.date)):
        rendered = value.isoformat()
        return rendered.replace("+00:00", "Z")
    if isinstance(value, enum.Enum):
        return _safe_value(value.value, key)
    if dataclasses.is_dataclass(value):
        return _safe_value(dataclasses.asdict(value), key)
    if isinstance(value, Mapping):
        return {
            str(child_key): _safe_value(child_value, str(child_key))
            for child_key, child_value in value.items()
            if not _is_sensitive_key(child_key)
        }
    if isinstance(value, (list, tuple, set)):
        return [_safe_value(item) for item in value]
    return str(value)


def _first(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _market_section(raw: Mapping[str, Any]) -> Dict[str, Any]:
    market = _plain_mapping(raw.get("market"))
    book = _plain_mapping(market.get("book") or raw.get("book"))
    mark = _plain_mapping(market.get("mark") or raw.get("mark"))
    return {
        "symbol": "BTCUSDT",
        "contract_type": "PERPETUAL",
        "book": {
            "bid_price": _first(book, "bid_price", "bidPrice"),
            "bid_qty": _first(book, "bid_qty", "bid_quantity", "bidQty"),
            "ask_price": _first(book, "ask_price", "askPrice"),
            "ask_qty": _first(book, "ask_qty", "ask_quantity", "askQty"),
            "updated_at": _first(book, "updated_at", "event_time", "transaction_time"),
        },
        "mark": {
            "mark_price": _first(mark, "mark_price", "markPrice"),
            "index_price": _first(mark, "index_price", "indexPrice"),
            "funding_rate": _first(mark, "funding_rate", "last_funding_rate", "fundingRate"),
            "next_funding_time": _first(mark, "next_funding_time", "nextFundingTime"),
            "updated_at": _first(mark, "updated_at", "event_time"),
        },
    }


def _account_section(raw: Mapping[str, Any]) -> Dict[str, Any]:
    account = _plain_mapping(raw.get("account"))
    ledger = _plain_mapping(raw.get("ledger"))
    source = dict(raw)
    source.update(ledger)
    source.update(account)
    return {
        "asset": "USDT",
        "wallet_balance": _first(source, "wallet_balance", "walletBalance", "cash"),
        "available_balance": _first(source, "available_balance", "availableBalance", "cash"),
        "margin_balance": _first(source, "margin_balance", "marginBalance", "equity"),
        "unrealized_pnl": _first(source, "unrealized_pnl", "unrealised_pnl", "totalUnrealizedProfit"),
        "realized_pnl": _first(source, "realized_pnl", "realised_pnl"),
    }


def _positions_section(raw: Mapping[str, Any]) -> List[Dict[str, Any]]:
    candidate = raw.get("positions")
    if candidate is None:
        account = _plain_mapping(raw.get("account"))
        candidate = account.get("positions")
    if candidate is None:
        ledger = _plain_mapping(raw.get("ledger"))
        candidate = ledger.get("position")
    if isinstance(candidate, Mapping):
        mapped_positions = list(candidate.values())
        if mapped_positions and all(
            isinstance(item, Mapping) or dataclasses.is_dataclass(item)
            for item in mapped_positions
        ):
            candidate = mapped_positions
        else:
            candidate = [candidate]
    elif dataclasses.is_dataclass(candidate):
        candidate = [candidate]
    if not isinstance(candidate, (list, tuple)):
        return []
    positions: List[Dict[str, Any]] = []
    for item in candidate:
        position = _plain_mapping(item)
        if not position:
            continue
        symbol = str(_first(position, "symbol") or "BTCUSDT").upper()
        if symbol != "BTCUSDT":
            continue
        position["symbol"] = "BTCUSDT"
        positions.append(position)
    return positions


def _events_section(raw: Mapping[str, Any]) -> List[Dict[str, Any]]:
    candidate = raw.get("events")
    if not isinstance(candidate, (list, tuple)):
        return []
    events: List[Dict[str, Any]] = []
    for item in candidate[-100:]:
        event = _plain_mapping(item)
        events.append(event if event else {"message": str(item)})
    return events


def _protection_section(raw: Mapping[str, Any]) -> Dict[str, Any]:
    protection = _plain_mapping(raw.get("protection"))
    raw_legs = protection.get("legs")
    legs = []
    if isinstance(raw_legs, (list, tuple)):
        for item in raw_legs[:2]:
            leg = _plain_mapping(item)
            if leg:
                legs.append(
                    {
                        "kind": _first(leg, "kind"),
                        "phase": _first(leg, "phase"),
                        "trigger_price": _first(leg, "trigger_price"),
                        "algo_status": _first(leg, "algo_status"),
                    }
                )
    return {
        "state": str(_first(protection, "state") or "UNAVAILABLE"),
        "winner": _first(protection, "winner"),
        "legs": legs,
    }


class DashboardSnapshotSource:
    """把宽松的运行时快照收敛为只读、无密钥、默认拒绝的 Dashboard 合同。"""

    def __init__(self, runtime: Any = None, mode: str = "paper", unavailable_reason: str = "runtime unavailable"):
        self._runtime = runtime
        self._mode = mode if mode in ("paper", "testnet") else "paper"
        self._unavailable_reason = unavailable_reason
        self._last_error: Optional[str] = None

    def _runtime_ready(self, raw: Optional[Mapping[str, Any]] = None) -> bool:
        if self._runtime is None:
            return False
        for name in ("ready", "is_ready"):
            check = getattr(self._runtime, name, None)
            if callable(check):
                try:
                    return check() is True
                except Exception as exc:  # Dashboard 边界必须把异常收敛为拒绝状态。
                    self._last_error = "%s: %s" % (type(exc).__name__, exc)
                    return False
        return bool(raw and raw.get("ready") is True)

    def is_ready(self) -> bool:
        return self._runtime_ready()

    def snapshot(self) -> Dict[str, Any]:
        raw: Dict[str, Any] = {}
        if self._runtime is not None:
            snapshot = getattr(self._runtime, "snapshot", None)
            if callable(snapshot):
                try:
                    raw = _plain_mapping(snapshot())
                    self._last_error = None
                except Exception as exc:  # 快照失败不应击穿 HTTP 服务。
                    self._last_error = "%s: %s" % (type(exc).__name__, exc)

        ready = self._runtime_ready(raw)
        raw_risk = _plain_mapping(raw.get("risk"))
        raw_ledger = _plain_mapping(raw.get("ledger"))
        daily_drawdown = _first(raw_risk, "daily_drawdown_pct")
        if daily_drawdown is None:
            daily_drawdown = _first(raw_ledger, "daily_drawdown_pct")
        if daily_drawdown is None:
            daily_drawdown = _first(raw, "daily_drawdown_pct")
        reason = _first(raw_risk, "reason", "message")
        if not ready:
            reason = self._last_error or reason or self._unavailable_reason
        explicit_can_open = raw_risk.get("can_open") is True
        risk = {
            "state": str(
                _first(raw_risk, "state", "status")
                or ("ready" if ready and explicit_can_open else "blocked")
            ),
            "can_open": bool(ready and explicit_can_open),
            "reason": str(reason or ("risk gate allows entries" if explicit_can_open else "risk gate blocks entries")),
            "daily_drawdown_pct": daily_drawdown,
        }
        if not ready:
            risk["state"] = "blocked"
            risk["can_open"] = False

        result = {
            "schema_version": SNAPSHOT_SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "mode": self._mode,
            "ready": ready,
            "market": _market_section(raw),
            "account": _account_section(raw),
            "positions": _positions_section(raw),
            "protection": _protection_section(raw),
            "risk": risk,
            "events": _events_section(raw),
        }
        return _safe_value(result)


class _DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class _DashboardIPv6HTTPServer(_DashboardHTTPServer):
    address_family = socket.AF_INET6


class _DashboardRequestHandler(BaseHTTPRequestHandler):
    server_version = "BinanceDashboard/1"
    sys_version = ""

    @property
    def dashboard(self) -> _DashboardHTTPServer:
        return self.server  # type: ignore[return-value]

    def log_message(self, format_string: str, *args: Any) -> None:
        LOGGER.debug("dashboard request: " + format_string, *args)

    def _request_is_local(self) -> bool:
        host_header = self.headers.get("Host", "")
        try:
            parsed_host = urlsplit("//" + host_header)
            expected_port = int(self.dashboard.server_address[1])
            if parsed_host.hostname not in LOOPBACK_HOSTS:
                return False
            if parsed_host.port is not None and parsed_host.port != expected_port:
                return False
        except (TypeError, ValueError):
            return False

        origin = self.headers.get("Origin")
        if origin:
            try:
                parsed_origin = urlsplit(origin)
                if parsed_origin.scheme != "http" or parsed_origin.hostname not in LOOPBACK_HOSTS:
                    return False
                if parsed_origin.port != expected_port:
                    return False
            except (TypeError, ValueError):
                return False
        return True

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for name, value in SECURITY_HEADERS.items():
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _send_json(self, status: int, payload: Mapping[str, Any]) -> None:
        body = json.dumps(_safe_value(payload), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def do_HEAD(self) -> None:
        self.do_GET()

    def do_GET(self) -> None:
        if not self._request_is_local():
            self._send_json(403, {"status": "forbidden"})
            return

        path = urlsplit(self.path).path
        if path == "/healthz":
            self._send_json(
                200,
                {
                    "status": "ok",
                    "service": "binance-dashboard",
                    "html_present": bool(getattr(self.dashboard, "dashboard_html", b"")),
                },
            )
            return
        if path == "/readyz":
            ready = self.dashboard.snapshot_source.is_ready()
            self._send_json(200 if ready else 503, {"status": "ready" if ready else "blocked", "ready": ready})
            return
        if path == "/api/snapshot":
            self._send_json(200, self.dashboard.snapshot_source.snapshot())
            return
        if path in ("/", "/Binance_Dashboard.html"):
            self._send(200, self.dashboard.dashboard_html, "text/html; charset=utf-8")
            return
        if path == "/assets/dashboard.css":
            self._send(200, DASHBOARD_CSS.encode("utf-8"), "text/css; charset=utf-8")
            return
        if path == "/assets/dashboard.js":
            self._send(200, DASHBOARD_JS.encode("utf-8"), "text/javascript; charset=utf-8")
            return
        self._send_json(404, {"status": "not_found"})

    def do_POST(self) -> None:
        self._send_json(405, {"status": "method_not_allowed"})


class DashboardServer:
    """仅绑定 IPv4/IPv6 loopback 的轻量 HTTP 服务生命周期封装。"""

    def __init__(
        self,
        snapshot_source: DashboardSnapshotSource,
        html_path: Path,
        host: str = "127.0.0.1",
        port: int = 8765,
    ):
        normalized_host = "127.0.0.1" if host == "localhost" else host
        if normalized_host not in ("127.0.0.1", "::1"):
            raise ValueError("dashboard host must be 127.0.0.1, ::1 or localhost")
        if port < 0 or port > 65535:
            raise ValueError("dashboard port must be between 0 and 65535")
        resolved_html = Path(html_path).resolve()
        if not resolved_html.is_file():
            raise FileNotFoundError("dashboard HTML not found: %s" % resolved_html)

        server_class = _DashboardIPv6HTTPServer if normalized_host == "::1" else _DashboardHTTPServer
        server = server_class((normalized_host, port), _DashboardRequestHandler)
        server.snapshot_source = snapshot_source  # type: ignore[attr-defined]
        server.dashboard_html = resolved_html.read_bytes()  # type: ignore[attr-defined]
        self._server = server
        self._thread: Optional[threading.Thread] = None

    @property
    def address(self) -> Tuple[str, int]:
        host, port = self._server.server_address[:2]
        return str(host), int(port)

    @property
    def base_url(self) -> str:
        host, port = self.address
        rendered_host = "[%s]" % host if ":" in host else host
        return "http://%s:%d" % (rendered_host, port)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="binance-dashboard",
            daemon=True,
        )
        self._thread.start()

    def serve_forever(self) -> None:
        self._server.serve_forever(poll_interval=0.25)

    def stop(self) -> None:
        if self._thread and self._thread.is_alive():
            self._server.shutdown()
            self._thread.join(timeout=5)
        self._server.server_close()
        self._thread = None


def create_dashboard_server(
    runtime: Any = None,
    mode: str = "paper",
    host: str = "127.0.0.1",
    port: int = 8765,
    html_path: Optional[Path] = None,
    unavailable_reason: str = "runtime unavailable",
) -> DashboardServer:
    """从运行时对象构建只读 Dashboard；不接受外部网卡绑定。"""
    resolved_html = html_path or Path(__file__).resolve().parents[2] / "Binance_Dashboard.html"
    source = DashboardSnapshotSource(runtime=runtime, mode=mode, unavailable_reason=unavailable_reason)
    return DashboardServer(source, resolved_html, host=host, port=port)
