"""
[INPUT]: 依赖 Binance Demo REST/Algo 客户端、可注入 transport 与 exchangeInfo 样例
[OUTPUT]: 验证 HMAC、Algo 查改、418/429 全局冷却、clientAlgoId 与 POST 未知结果不盲重投契约
[POS]: Binance 交易所防腐层的无网络契约回归，分离提交与只读/撤销失败语义
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import unittest
import urllib.parse
from unittest import mock
from decimal import Decimal

from python.binance_trading.config import BinanceConfig
from python.binance_trading.exchange import (
    BinanceApiError,
    BinanceError,
    BinanceFuturesClient,
    BinanceRateLimitError,
    BinanceSubmissionUnknown,
    BinanceTransportError,
    HttpResponse,
    parse_symbol_rules,
)


def testnet_config() -> BinanceConfig:
    return BinanceConfig.from_mapping(
        {
            "EXECUTION_MODE": "testnet",
            "BINANCE_API_KEY": "demo-key",
            "BINANCE_API_SECRET": "demo-secret",
            "TRADING_ENABLED": "true",
            "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
        }
    )


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def request(self, method, url, headers, body, timeout):
        self.requests.append((method, url, dict(headers), body, timeout))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def response(status, payload, headers=None):
    return HttpResponse(status=status, headers=headers or {}, body=json.dumps(payload).encode())


class BinanceExchangeTests(unittest.TestCase):
    def test_rate_limit_response_opens_global_gate_and_never_becomes_submission_unknown(self):
        now = [100.0]
        transport = FakeTransport(
            [
                HttpResponse(429, {"rEtRy-AfTeR": "3"}, b"truncated"),
                response(200, {"serverTime": 10_000}),
            ]
        )
        client = BinanceFuturesClient(
            testnet_config(),
            transport=transport,
            clock_ms=lambda: 10_000,
            monotonic=lambda: now[0],
        )

        with self.assertRaises(BinanceRateLimitError) as caught:
            client.new_order(
                {
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "type": "MARKET",
                    "quantity": "0.01",
                    "newClientOrderId": "rate-limited-entry",
                }
            )
        self.assertFalse(caught.exception.locally_blocked)
        self.assertEqual(caught.exception.retry_after_seconds, 3.0)
        self.assertNotIsInstance(caught.exception, BinanceSubmissionUnknown)

        for call in (
            lambda: client.public_get("/fapi/v1/time"),
            lambda: client.signed_request("GET", "/fapi/v3/account"),
        ):
            with self.assertRaises(BinanceRateLimitError) as blocked:
                call()
            self.assertTrue(blocked.exception.locally_blocked)
        self.assertEqual(len(transport.requests), 1)

        now[0] += 3.0
        self.assertEqual(client.public_get("/fapi/v1/time")["serverTime"], 10_000)
        self.assertEqual(len(transport.requests), 2)

    def test_rate_limit_missing_or_invalid_header_uses_conservative_fallback(self):
        for status, headers, expected in (
            (429, {}, 60.0),
            (429, {"Retry-After": "n/a"}, 60.0),
            (418, {}, 120.0),
        ):
            with self.subTest(status=status, headers=headers):
                transport = FakeTransport(
                    [response(status, {"code": -1003, "msg": "slow down"}, headers)]
                )
                client = BinanceFuturesClient(
                    testnet_config(),
                    transport=transport,
                    clock_ms=lambda: 10_000,
                    monotonic=lambda: 50.0,
                )

                with self.assertRaises(BinanceRateLimitError) as caught:
                    client.public_get("/fapi/v1/time")
                self.assertEqual(caught.exception.status, status)
                self.assertEqual(caught.exception.code, -1003)
                self.assertEqual(caught.exception.retry_after_seconds, expected)
                with self.assertRaises(BinanceRateLimitError):
                    client.exchange_info()
                self.assertEqual(len(transport.requests), 1)

    def test_signed_request_uses_hmac_and_server_time_offset(self):
        transport = FakeTransport(
            [
                response(200, {"serverTime": 12_000}),
                response(200, {"dualSidePosition": False}),
            ]
        )
        times = iter([10_000, 10_200, 10_300])
        config = testnet_config()
        client = BinanceFuturesClient(config, transport=transport, clock_ms=lambda: next(times))

        self.assertEqual(client.sync_time(), 1_900)
        self.assertFalse(client.position_mode())

        _, url, headers, _, _ = transport.requests[1]
        query = urllib.parse.urlsplit(url).query
        unsigned, signature = query.rsplit("&signature=", 1)
        expected = hmac.new(
            b"demo-secret", unsigned.encode("ascii"), hashlib.sha256
        ).hexdigest()
        self.assertEqual(signature, expected)
        self.assertEqual(headers["X-MBX-APIKEY"], "demo-key")
        self.assertIn("timestamp=12200", unsigned)

    def test_exchange_info_requires_trading_usdt_perpetual_and_parses_steps(self):
        payload = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {
                            "filterType": "LOT_SIZE",
                            "stepSize": "0.001",
                            "minQty": "0.001",
                            "maxQty": "1000",
                        },
                        {"filterType": "MIN_NOTIONAL", "notional": "5"},
                    ],
                }
            ]
        }

        rules = parse_symbol_rules(payload, "BTCUSDT")

        self.assertEqual(rules.tick_size, Decimal("0.10"))
        self.assertEqual(rules.floor_quantity(Decimal("1.2349")), Decimal("1.234"))
        payload["symbols"][0]["contractType"] = "CURRENT_QUARTER"
        with self.assertRaisesRegex(Exception, "永续"):
            parse_symbol_rules(payload, "BTCUSDT")

    def test_http_408_unknown_5xx_and_transport_timeout_require_query_without_retry(self):
        params = {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "IOC",
            "quantity": "0.01",
            "price": "60000",
            "newClientOrderId": "signal-entry-1",
        }
        for result in (
            response(408, {"code": -1000, "msg": "backend timeout"}),
            response(500, {"code": -1000, "msg": "Internal error"}),
            response(502, {"code": -1000, "msg": "Bad gateway"}),
            response(503, {"code": -1000, "msg": "Unknown error"}),
            response(504, {"code": -1000, "msg": "Gateway timeout"}),
            response(599, {"code": -1000, "msg": "Unknown server error"}),
            BinanceTransportError("timeout"),
        ):
            with self.subTest(result=type(result).__name__):
                transport = FakeTransport([result])
                client = BinanceFuturesClient(
                    testnet_config(), transport=transport, clock_ms=lambda: 10_000
                )
                with self.assertRaises(BinanceSubmissionUnknown) as caught:
                    client.new_order(params)
                self.assertEqual(caught.exception.client_order_id, "signal-entry-1")
                self.assertEqual(len(transport.requests), 1)

    def test_official_definitive_503_variants_are_explicit_failures(self):
        params = {
            "symbol": "BTCUSDT",
            "side": "BUY",
            "type": "MARKET",
            "quantity": "0.01",
            "newClientOrderId": "definitive-503",
        }
        variants = (
            (-1000, "Service Unavailable."),
            (-1000, "Internal error; unable to process your request. Please try again."),
            (
                -1008,
                "Request throttled by system-level protection. Reduce-only/close-position orders are exempt. Please try again.",
            ),
        )
        for code, message in variants:
            with self.subTest(code=code, message=message):
                transport = FakeTransport([response(503, {"code": code, "msg": message})])
                client = BinanceFuturesClient(
                    testnet_config(), transport=transport, clock_ms=lambda: 10_000
                )
                with self.assertRaises(BinanceApiError) as caught:
                    client.new_order(params)
                self.assertEqual(caught.exception.status, 503)
                self.assertEqual(len(transport.requests), 1)

    def test_order_any_invalid_json_response_is_submission_unknown(self):
        for status in (200, 400, 502):
            with self.subTest(status=status):
                transport = FakeTransport(
                    [HttpResponse(status=status, headers={}, body=b"upstream unavailable")]
                )
                client = BinanceFuturesClient(
                    testnet_config(), transport=transport, clock_ms=lambda: 10_000
                )

                with self.assertRaises(BinanceSubmissionUnknown) as caught:
                    client.new_order(
                        {
                            "symbol": "BTCUSDT",
                            "side": "BUY",
                            "type": "MARKET",
                            "quantity": "0.01",
                            "newClientOrderId": "signal-entry-invalid-json",
                        }
                    )

                self.assertEqual(
                    caught.exception.client_order_id, "signal-entry-invalid-json"
                )
                self.assertEqual(len(transport.requests), 1)

    def test_truncated_order_response_body_is_submission_unknown(self):
        class TruncatedResponse:
            status = 200
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                raise http.client.IncompleteRead(b'{"orderId":')

        client = BinanceFuturesClient(testnet_config(), clock_ms=lambda: 10_000)
        with mock.patch("urllib.request.urlopen", return_value=TruncatedResponse()):
            with self.assertRaises(BinanceSubmissionUnknown) as caught:
                client.new_order(
                    {
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "type": "MARKET",
                        "quantity": "0.01",
                        "newClientOrderId": "truncated-response",
                    }
                )
        self.assertEqual(caught.exception.client_order_id, "truncated-response")

    def test_isolated_margin_is_idempotent_and_leverage_response_must_match(self):
        transport = FakeTransport(
            [
                response(400, {"code": -4046, "msg": "No need to change margin type."}),
                response(200, {"symbol": "BTCUSDT", "leverage": 2}),
            ]
        )
        client = BinanceFuturesClient(
            testnet_config(), transport=transport, clock_ms=lambda: 10_000
        )

        margin = client.change_margin_type("BTCUSDT", "ISOLATED")
        leverage = client.change_leverage("BTCUSDT", 2)

        self.assertTrue(margin["already_configured"])
        self.assertEqual(leverage["leverage"], 2)
        self.assertIn("/fapi/v1/marginType?", transport.requests[0][1])
        self.assertIn("marginType=ISOLATED", transport.requests[0][1])
        self.assertIn("/fapi/v1/leverage?", transport.requests[1][1])

    def test_open_orders_queries_the_entire_account(self):
        transport = FakeTransport(
            [response(200, [{"symbol": "ETHUSDT", "clientOrderId": "manual-order"}])]
        )
        client = BinanceFuturesClient(
            testnet_config(), transport=transport, clock_ms=lambda: 10_000
        )

        self.assertEqual(client.open_orders()[0]["symbol"], "ETHUSDT")
        self.assertIn("/fapi/v1/openOrders?", transport.requests[0][1])
        self.assertNotIn("symbol=", transport.requests[0][1])

    def test_algo_order_uses_official_endpoint_identity_and_signed_contract(self):
        transport = FakeTransport(
            [
                response(
                    200,
                    {
                        "algoId": 42,
                        "clientAlgoId": "protect-stop-1",
                        "algoStatus": "NEW",
                    },
                )
            ]
        )
        client = BinanceFuturesClient(
            testnet_config(), transport=transport, clock_ms=lambda: 10_000
        )

        payload = client.new_algo_order(
            {
                "algoType": "CONDITIONAL",
                "symbol": "BTCUSDT",
                "side": "SELL",
                "type": "STOP_MARKET",
                "positionSide": "BOTH",
                "quantity": "0.01",
                "triggerPrice": "59000",
                "workingType": "MARK_PRICE",
                "reduceOnly": True,
                "clientAlgoId": "protect-stop-1",
            }
        )

        self.assertEqual(payload["algoId"], 42)
        method, url, headers, body, _ = transport.requests[0]
        self.assertEqual(method, "POST")
        self.assertIsNone(body)
        self.assertIn("/fapi/v1/algoOrder?", url)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        self.assertEqual(query["algoType"], ["CONDITIONAL"])
        self.assertEqual(query["clientAlgoId"], ["protect-stop-1"])
        self.assertEqual(query["reduceOnly"], ["true"])
        self.assertIn("signature", query)
        self.assertEqual(headers["X-MBX-APIKEY"], "demo-key")

    def test_algo_client_identity_and_close_all_contract_fail_closed(self):
        client = BinanceFuturesClient(
            testnet_config(), transport=FakeTransport([]), clock_ms=lambda: 10_000
        )
        base = {
            "algoType": "CONDITIONAL",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "type": "STOP_MARKET",
            "triggerPrice": "59000",
            "closePosition": True,
        }
        for invalid_id in ("", "x" * 37, "bad id"):
            with self.subTest(client_algo_id=invalid_id), self.assertRaisesRegex(
                ValueError, "clientAlgoId"
            ):
                client.new_algo_order(dict(base, clientAlgoId=invalid_id))

        with self.assertRaisesRegex(ValueError, "quantity/reduceOnly"):
            client.new_algo_order(dict(base, clientAlgoId="close-all-1", quantity="0.01"))
        with self.assertRaisesRegex(ValueError, "方向"):
            client.new_algo_order(
                dict(base, clientAlgoId="close-all-2", side="SELL", positionSide="SHORT")
            )
        with self.assertRaisesRegex(ValueError, "Hedge Mode"):
            client.new_algo_order(
                dict(
                    base,
                    clientAlgoId="hedge-reduce-only",
                    closePosition=False,
                    quantity="0.01",
                    positionSide="LONG",
                    reduceOnly=False,
                )
            )
        self.assertEqual(client.transport.requests, [])

    def test_algo_post_ambiguous_codes_status_and_json_are_submission_unknown(self):
        request = {
            "algoType": "CONDITIONAL",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "type": "TAKE_PROFIT_MARKET",
            "quantity": "0.01",
            "triggerPrice": "65000",
            "reduceOnly": True,
            "clientAlgoId": "protect-target-1",
        }
        cases = (
            response(408, {"code": -1000, "msg": "timeout"}),
            response(400, {"code": -1006, "msg": "unexpected response"}),
            response(400, {"code": -1007, "msg": "execution status unknown"}),
            response(503, {"code": -1007, "msg": "Service Unavailable"}),
            response(503, {"code": -1000, "msg": "Unknown error"}),
            BinanceTransportError("connection reset"),
            TimeoutError("timeout"),
            HttpResponse(status=200, headers={}, body=b'{"algoId":'),
            HttpResponse(status=200, headers={}, body=b"[]"),
        )
        for result in cases:
            with self.subTest(result=repr(result)):
                transport = FakeTransport([result])
                client = BinanceFuturesClient(
                    testnet_config(), transport=transport, clock_ms=lambda: 10_000
                )
                with self.assertRaises(BinanceSubmissionUnknown) as caught:
                    client.new_algo_order(request)
                self.assertEqual(caught.exception.client_order_id, "protect-target-1")
                self.assertEqual(len(transport.requests), 1)

    def test_algo_query_cancel_and_open_orders_never_use_post_unknown_semantics(self):
        transport = FakeTransport(
            [
                response(200, {"algoId": 42, "algoStatus": "NEW"}),
                response(200, {"algoId": 42, "code": "200", "msg": "success"}),
                response(200, [{"algoId": 43, "symbol": "BTCUSDT"}]),
                response(503, {"code": -1000, "msg": "Unknown error"}),
                HttpResponse(status=200, headers={}, body=b"truncated"),
                BinanceTransportError("timeout"),
            ]
        )
        client = BinanceFuturesClient(
            testnet_config(), transport=transport, clock_ms=lambda: 10_000
        )

        self.assertEqual(client.query_algo_order(client_algo_id="protect-stop-1")["algoId"], 42)
        self.assertEqual(client.cancel_algo_order(algo_id=42)["code"], "200")
        self.assertEqual(client.open_algo_orders("BTCUSDT", "CONDITIONAL")[0]["algoId"], 43)
        with self.assertRaises(BinanceApiError):
            client.query_algo_order(algo_id=42)
        with self.assertRaises(BinanceError) as malformed:
            client.cancel_algo_order(client_algo_id="protect-stop-1")
        self.assertNotIsInstance(malformed.exception, BinanceSubmissionUnknown)
        with self.assertRaises(BinanceTransportError) as transport_error:
            client.open_algo_orders("BTCUSDT")
        self.assertNotIsInstance(transport_error.exception, BinanceSubmissionUnknown)

        methods_and_paths = [
            (method, urllib.parse.urlsplit(url).path) for method, url, _, _, _ in transport.requests
        ]
        self.assertEqual(
            methods_and_paths[:3],
            [
                ("GET", "/fapi/v1/algoOrder"),
                ("DELETE", "/fapi/v1/algoOrder"),
                ("GET", "/fapi/v1/openAlgoOrders"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
