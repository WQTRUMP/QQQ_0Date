"""
[INPUT]: 依赖 Binance_Dashboard.html 与 binance_trading.dashboard 的快照适配、loopback HTTP 服务
[OUTPUT]: 验证页面语义、CSP、敏感字段屏蔽、就绪状态和本地绑定的回归合同
[POS]: python/tests 的 Binance 展示边界合同测试，不连接交易所也不启动交易循环
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import http.client
import json
import re
import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlsplit

from python.binance_trading.dashboard import DashboardSnapshotSource, create_dashboard_server


ROOT = Path(__file__).resolve().parents[2]
HTML_PATH = ROOT / "Binance_Dashboard.html"


class _ReadyRuntime:
    def ready(self) -> bool:
        return True

    def snapshot(self) -> Dict[str, Any]:
        return {
            "market": {
                "book": {
                    "bid_price": Decimal("60000.10"),
                    "bid_qty": Decimal("1.25"),
                    "ask_price": Decimal("60000.20"),
                    "ask_qty": Decimal("0.75"),
                    "event_time": 1_700_000_000_000,
                },
                "mark": {
                    "mark_price": Decimal("60000.15"),
                    "index_price": Decimal("60001.00"),
                    "funding_rate": Decimal("0.0001"),
                    "next_funding_time": 1_700_000_100_000,
                },
            },
            "ledger": {
                "cash": Decimal("9950"),
                "equity": Decimal("10025"),
                "unrealized_pnl": Decimal("75"),
            },
            "positions": [
                {
                    "symbol": "BTCUSDT",
                    "side": "LONG",
                    "quantity": Decimal("0.01"),
                    "entry_price": Decimal("59000"),
                    "mark_price": Decimal("60000.15"),
                },
                {"symbol": "ETHUSDT", "quantity": Decimal("1")},
            ],
            "risk": {
                "state": "ready",
                "can_open": True,
                "daily_drawdown_pct": Decimal("0.01"),
            },
            "protection": {
                "state": "ARMED",
                "winner": None,
                "legs": [
                    {"kind": "STOP", "phase": "OPEN", "trigger_price": Decimal("59000")},
                    {"kind": "TARGET", "phase": "OPEN", "trigger_price": Decimal("62000")},
                ],
                "clientAlgoId": "must-not-be-projected",
            },
            "events": [
                {
                    "kind": "signal",
                    "message": "1m EMA5/13 crossover",
                    "api_secret": "must-not-leak",
                    "nested": {
                        "token": "also-secret",
                        "listenKey": "camel-secret",
                        "safe": "visible",
                    },
                }
            ],
        }


class _BlockedRuntime:
    def ready(self) -> bool:
        return False

    def snapshot(self) -> Dict[str, Any]:
        return {"risk": {"state": "waiting", "can_open": True, "reason": "market stream stale"}}


def _fetch(base_url: str, path: str) -> Tuple[int, Dict[str, str], bytes]:
    target = urlsplit(base_url)
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=3)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, dict(response.headers.items()), response.read()
    finally:
        connection.close()


def _server(runtime: Any):
    server = create_dashboard_server(runtime=runtime, port=0, html_path=HTML_PATH)
    server.start()
    return server


class BinanceDashboardTests(unittest.TestCase):
    def test_html_is_binance_perpetual_utc_only_and_has_no_inline_code(self) -> None:
        html = HTML_PATH.read_text(encoding="utf-8")
        self.assertIn("BTCUSDT", html)
        self.assertIn("PERPETUAL", html)
        self.assertIn("UTC", html)
        self.assertIn("Closed 1m", html)
        self.assertIn("EMA5/13 entry", html)
        self.assertIn("ATR14 risk", html)
        self.assertIn("Mark price", html)
        self.assertIn("Funding rate", html)
        self.assertIn("Liquidation", html)
        self.assertIn("Leverage", html)
        self.assertNotIn("unsafe-inline", html)
        self.assertNotIn("unsafe-eval", html)
        self.assertIsNone(re.search(r"<style\b", html, re.IGNORECASE))
        self.assertIsNone(re.search(r"<script(?![^>]*\bsrc=)[^>]*>", html, re.IGNORECASE))
        self.assertIsNone(re.search(r"\son[a-z]+\s*=", html, re.IGNORECASE))

    def test_snapshot_is_fixed_scope_serializable_and_redacts_secrets(self) -> None:
        snapshot = DashboardSnapshotSource(_ReadyRuntime(), mode="testnet").snapshot()
        encoded = json.dumps(snapshot)
        self.assertEqual(snapshot["schema_version"], "binance-dashboard-v1")
        self.assertIs(snapshot["ready"], True)
        self.assertEqual(snapshot["market"]["symbol"], "BTCUSDT")
        self.assertEqual(snapshot["market"]["contract_type"], "PERPETUAL")
        self.assertEqual(snapshot["market"]["book"]["bid_price"], "60000.10")
        self.assertEqual(snapshot["account"]["margin_balance"], "10025")
        self.assertEqual([position["symbol"] for position in snapshot["positions"]], ["BTCUSDT"])
        self.assertIs(snapshot["risk"]["can_open"], True)
        self.assertEqual(snapshot["protection"]["state"], "ARMED")
        self.assertEqual([leg["kind"] for leg in snapshot["protection"]["legs"]], ["STOP", "TARGET"])
        self.assertNotIn("must-not-be-projected", encoded)
        self.assertNotIn("must-not-leak", encoded)
        self.assertNotIn("also-secret", encoded)
        self.assertNotIn("camel-secret", encoded)
        self.assertNotIn("api_secret", encoded)
        self.assertNotIn("token", encoded)

    def test_blocked_runtime_cannot_open_even_if_raw_risk_claims_true(self) -> None:
        snapshot = DashboardSnapshotSource(_BlockedRuntime()).snapshot()
        self.assertIs(snapshot["ready"], False)
        self.assertEqual(snapshot["risk"]["state"], "blocked")
        self.assertIs(snapshot["risk"]["can_open"], False)
        self.assertEqual(snapshot["risk"]["reason"], "market stream stale")

    def test_http_routes_security_headers_and_readiness(self) -> None:
        server = _server(_ReadyRuntime())
        try:
            status, headers, health_body = _fetch(server.base_url, "/healthz")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(health_body)["status"], "ok")
            self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
            self.assertNotIn("unsafe-inline", headers["Content-Security-Policy"])
            self.assertEqual(headers["X-Content-Type-Options"], "nosniff")

            ready_status, _, ready_body = _fetch(server.base_url, "/readyz")
            self.assertEqual(ready_status, 200)
            self.assertIs(json.loads(ready_body)["ready"], True)

            snapshot_status, snapshot_headers, snapshot_body = _fetch(server.base_url, "/api/snapshot")
            self.assertEqual(snapshot_status, 200)
            self.assertTrue(snapshot_headers["Cache-Control"].startswith("no-store"))
            self.assertEqual(json.loads(snapshot_body)["market"]["symbol"], "BTCUSDT")

            html_status, _, html_body = _fetch(server.base_url, "/")
            self.assertEqual(html_status, 200)
            self.assertIn(b"BTCUSDT", html_body)
            self.assertEqual(_fetch(server.base_url, "/assets/dashboard.css")[0], 200)
            self.assertEqual(_fetch(server.base_url, "/assets/dashboard.js")[0], 200)
        finally:
            server.stop()

    def test_readyz_returns_503_for_blocked_runtime(self) -> None:
        server = _server(_BlockedRuntime())
        try:
            status, _, body = _fetch(server.base_url, "/readyz")
            self.assertEqual(status, 503)
            self.assertEqual(json.loads(body), {"status": "blocked", "ready": False})
        finally:
            server.stop()

    def test_dns_rebinding_host_and_cross_origin_requests_are_rejected(self) -> None:
        server = _server(_ReadyRuntime())
        connection: Optional[http.client.HTTPConnection] = None
        origin_connection: Optional[http.client.HTTPConnection] = None
        try:
            host, port = server.address
            connection = http.client.HTTPConnection(host, port, timeout=3)
            connection.request("GET", "/healthz", headers={"Host": "attacker.invalid"})
            response = connection.getresponse()
            self.assertEqual(response.status, 403)
            response.read()

            origin_connection = http.client.HTTPConnection(host, port, timeout=3)
            origin_connection.request(
                "GET",
                "/healthz",
                headers={"Origin": "https://attacker.invalid"},
            )
            origin_response = origin_connection.getresponse()
            self.assertEqual(origin_response.status, 403)
            origin_response.read()
        finally:
            if connection is not None:
                connection.close()
            if origin_connection is not None:
                origin_connection.close()
            server.stop()

    def test_non_loopback_bindings_are_rejected(self) -> None:
        for host in ("0.0.0.0", "::", "192.0.2.10", ""):
            with self.subTest(host=host):
                with self.assertRaisesRegex(ValueError, "dashboard host"):
                    create_dashboard_server(runtime=_ReadyRuntime(), host=host, port=0, html_path=HTML_PATH)

    def test_ipv6_loopback_binding_is_supported(self) -> None:
        server = create_dashboard_server(runtime=_ReadyRuntime(), host="::1", port=0, html_path=HTML_PATH)
        server.start()
        try:
            self.assertTrue(server.base_url.startswith("http://[::1]:"))
            status, _, body = _fetch(server.base_url, "/healthz")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body)["status"], "ok")
        finally:
            server.stop()


if __name__ == "__main__":
    unittest.main()
