"""
[INPUT]: 依赖 binance_trading.broker、OrderIntent 与脚本化 Binance Demo client
[OUTPUT]: 验证 paper 多空/reduce-only 执行、手续费 PnL、-1006/-1007/408/未知 5xx 恢复及明确 503 失败分类
[POS]: tests 的 Binance broker 回归；防止退出反向建仓、未知提交盲重投或明确失败永久冻结
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from python.binance_trading.broker import (
    RETRYABLE_SERVER_FAILURE,
    BinanceTestnetBroker,
    PaperBroker,
    normalize_client_order_id,
)
from python.binance_trading.exchange import BinanceSubmissionUnknown
from python.binance_trading.ledger import PaperLedger
from python.binance_trading.models import Direction, OrderIntent


NOW = datetime(2026, 8, 11, 10, 0, tzinfo=timezone.utc)


def intent(
    client_order_id="entry-1",
    direction=Direction.LONG,
    side="BUY",
    quantity="0.1",
    reduce_only=False,
):
    return OrderIntent(
        client_order_id=client_order_id,
        symbol="BTCUSDT",
        direction=direction,
        side=side,
        quantity=Decimal(quantity),
        order_type="MARKET",
        limit_price=None,
        reduce_only=reduce_only,
        created_at=NOW,
        signal_id="signal-1",
    )


class PaperBrokerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.ledger = PaperLedger(
            Path(self.temporary.name) / "paper.db", initial_balance="1000"
        )
        self.broker = PaperBroker(
            self.ledger, fee_bps="10", clock=lambda: NOW
        )

    def tearDown(self):
        self.ledger.close()
        self.temporary.cleanup()

    def test_open_long_and_reduce_only_exit_charge_exact_fees(self):
        opened = self.broker.open_long("BTCUSDT", "2", "100", "long-1")
        closed = self.broker.exit_position(
            "BTCUSDT", "110", "exit-1", quantity="3"
        )
        snapshot = self.ledger.snapshot(now=NOW)

        self.assertEqual(opened.executed_quantity, Decimal("2"))
        self.assertEqual(closed.executed_quantity, Decimal("2"))
        self.assertEqual(snapshot["realized_pnl"], Decimal("20"))
        self.assertEqual(snapshot["fees"], Decimal("0.42"))
        self.assertEqual(snapshot["wallet_balance"], Decimal("1019.58"))
        self.assertEqual(snapshot["open_position_count"], 0)

    def test_open_short_and_buy_exit_have_symmetric_pnl(self):
        self.broker.open_short("ETHUSDT", "1.5", "200", "short-1")
        self.broker.exit_position("ETHUSDT", "180", "short-exit")

        snapshot = self.ledger.snapshot(now=NOW)
        self.assertEqual(snapshot["realized_pnl"], Decimal("30"))
        self.assertEqual(snapshot["fees"], Decimal("0.57"))
        self.assertEqual(snapshot["wallet_balance"], Decimal("1029.43"))

    def test_exit_on_flat_position_is_rejected_before_fill(self):
        with self.assertRaisesRegex(ValueError, "reduce-only"):
            self.broker.exit_position("BTCUSDT", "100", "flat-exit")
        self.assertIsNone(self.ledger.get_fill("flat-exit"))

    def test_paper_submission_is_idempotent_across_restart_semantics(self):
        order = intent()
        first = self.broker.submit_order(order, fill_price="100")
        replay = self.broker.submit_order(order, fill_price="100")

        self.assertEqual(first.status, "FILLED")
        self.assertEqual(replay.reason, "IDEMPOTENT_REPLAY")
        self.assertEqual(self.ledger.get_position("BTCUSDT").quantity, Decimal("0.1"))


class ScriptedClient:
    def __init__(self, submit_result=None, submit_error=None, query_result=None, query_error=None):
        self.submit_result = submit_result
        self.submit_error = submit_error
        self.query_result = query_result
        self.query_error = query_error
        self.submissions = []
        self.queries = []

    def new_order(self, **params):
        self.submissions.append(params)
        if self.submit_error is not None:
            raise self.submit_error
        return self.submit_result

    def query_order(self, **params):
        self.queries.append(params)
        if self.query_error is not None:
            raise self.query_error
        return self.query_result


class HttpServerError(RuntimeError):
    def __init__(self, status_code, message=None, code=None):
        super().__init__(message or "%s server error" % status_code)
        self.status_code = status_code
        self.message = message or str(self)
        self.code = code


class OrderNotFound(RuntimeError):
    code = -2013


class BinanceTestnetBrokerTests(unittest.TestCase):
    def response(self, client_id="entry-1", status="NEW"):
        return {
            "clientOrderId": client_id,
            "orderId": 123,
            "status": status,
            "executedQty": "0",
            "avgPrice": "0",
            "updateTime": int(NOW.timestamp() * 1000),
        }

    def test_open_orders_force_one_way_and_client_id_wire_limit(self):
        long_id = "strategy-run-with-a-client-order-id-that-is-far-too-long-for-binance"
        expected_id = normalize_client_order_id(long_id)
        client = ScriptedClient(submit_result=self.response(expected_id))
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)

        result = broker.open_long("BTCUSDT", "0.001", long_id)

        self.assertEqual(len(client.submissions), 1)
        params = client.submissions[0]
        self.assertLessEqual(len(params["newClientOrderId"]), 36)
        self.assertEqual(params["positionSide"], "BOTH")
        self.assertNotIn("reduceOnly", params)
        self.assertEqual(params["side"], "BUY")
        self.assertEqual(result.client_order_id, expected_id)

    def test_reduce_only_exit_uses_opposite_side(self):
        client = ScriptedClient(submit_result=self.response("exit-1"))
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)

        broker.exit_position(
            "BTCUSDT", Direction.LONG, "0.1", client_order_id="exit-1"
        )

        params = client.submissions[0]
        self.assertEqual(params["side"], "SELL")
        self.assertIs(params["reduceOnly"], True)
        self.assertEqual(params["positionSide"], "BOTH")
        self.assertEqual(params["newOrderRespType"], "RESULT")

    def test_limit_orders_are_ioc_and_request_synchronous_result(self):
        client = ScriptedClient(submit_result=self.response("limit-1"))
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)
        order = OrderIntent(
            client_order_id="limit-1",
            symbol="BTCUSDT",
            direction=Direction.LONG,
            side="BUY",
            quantity=Decimal("0.01"),
            order_type="LIMIT",
            limit_price=Decimal("60000"),
            reduce_only=False,
            created_at=NOW,
            signal_id="signal-limit",
        )

        broker.submit_order(order)

        params = client.submissions[0]
        self.assertEqual(params["timeInForce"], "IOC")
        self.assertEqual(params["newOrderRespType"], "RESULT")

    def test_timeout_queries_once_and_never_resubmits(self):
        client = ScriptedClient(
            submit_error=TimeoutError("read timed out"),
            query_result=self.response("entry-1", status="FILLED"),
        )
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)

        result = broker.submit_order(intent())

        self.assertEqual(len(client.submissions), 1)
        self.assertEqual(len(client.queries), 1)
        self.assertEqual(client.queries[0]["origClientOrderId"], "entry-1")
        self.assertEqual(result.status, "FILLED")
        self.assertEqual(result.reason, "RECOVERED_AFTER_UNCERTAIN_SUBMIT")

    def test_http_408_and_unknown_5xx_query_once_and_never_resubmit(self):
        for status_code in (408, 500, 502, 504, 599):
            with self.subTest(status_code=status_code):
                client = ScriptedClient(
                    submit_error=HttpServerError(status_code),
                    query_result=self.response("entry-1", status="FILLED"),
                )
                broker = BinanceTestnetBroker(client, clock=lambda: NOW)

                result = broker.submit_order(intent())

                self.assertEqual(len(client.submissions), 1)
                self.assertEqual(len(client.queries), 1)
                self.assertEqual(client.queries[0]["origClientOrderId"], "entry-1")
                self.assertEqual(result.status, "FILLED")
                self.assertEqual(result.reason, "RECOVERED_AFTER_UNCERTAIN_SUBMIT")

    def test_execution_unknown_api_codes_query_once_even_when_wrapped_as_4xx(self):
        for code in (-1006, -1007):
            with self.subTest(code=code):
                client = ScriptedClient(
                    submit_error=HttpServerError(400, "execution status unknown", code),
                    query_result=self.response("entry-1", status="FILLED"),
                )
                broker = BinanceTestnetBroker(client, clock=lambda: NOW)

                result = broker.submit_order(intent())

                self.assertEqual(len(client.submissions), 1)
                self.assertEqual(len(client.queries), 1)
                self.assertEqual(result.status, "FILLED")
                self.assertEqual(result.reason, "RECOVERED_AFTER_UNCERTAIN_SUBMIT")

    def test_official_definitive_503_failure_does_not_query(self):
        client = ScriptedClient(
            submit_error=HttpServerError(503, "Service Unavailable.", -1000),
        )
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)

        result = broker.submit_order(intent())

        self.assertEqual(len(client.submissions), 1)
        self.assertEqual(client.queries, [])
        self.assertEqual(result.status, "REJECTED")
        self.assertEqual(result.reason, RETRYABLE_SERVER_FAILURE)
        self.assertFalse(result.submission_unknown)

    def test_5xx_non_terminal_query_remains_submission_unknown(self):
        client = ScriptedClient(
            submit_error=HttpServerError(502),
            query_result=self.response("entry-1", status="NEW"),
        )
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)

        result = broker.submit_order(intent())

        self.assertEqual(len(client.submissions), 1)
        self.assertEqual(len(client.queries), 1)
        self.assertEqual(result.status, "NEW")
        self.assertTrue(result.submission_unknown)
        self.assertEqual(result.reason, "SUBMISSION_UNCERTAIN_NON_TERMINAL:NEW")

    def test_exchange_client_mapping_and_positional_query_contract(self):
        class ExchangeShapeClient:
            def __init__(self):
                self.submissions = []
                self.queries = []
                self.cancellations = []

            def new_order(self, params):
                self.submissions.append(params)
                raise BinanceSubmissionUnknown(params["newClientOrderId"], "timeout")

            def query_order(self, symbol, client_order_id):
                self.queries.append((symbol, client_order_id))
                return {
                    "clientOrderId": client_order_id,
                    "orderId": 123,
                    "status": "FILLED",
                    "executedQty": "0",
                    "avgPrice": "0",
                    "updateTime": int(NOW.timestamp() * 1000),
                }

            def cancel_order(self, symbol, client_order_id):
                self.cancellations.append((symbol, client_order_id))
                return {"status": "CANCELED"}

        client = ExchangeShapeClient()
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)

        result = broker.submit_order(intent())
        broker.cancel_order("BTCUSDT", "entry-1")

        self.assertEqual(len(client.submissions), 1)
        self.assertEqual(client.queries, [("BTCUSDT", "entry-1")])
        self.assertEqual(client.cancellations, [("BTCUSDT", "entry-1")])
        self.assertEqual(result.reason, "RECOVERED_AFTER_UNCERTAIN_SUBMIT")

    def test_503_then_not_found_returns_unknown_without_retry(self):
        client = ScriptedClient(
            submit_error=HttpServerError(503),
            query_error=OrderNotFound("unknown order"),
        )
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)

        result = broker.submit_order(intent())

        self.assertEqual(len(client.submissions), 1)
        self.assertEqual(len(client.queries), 1)
        self.assertEqual(result.status, "UNKNOWN")
        self.assertTrue(result.submission_unknown)
        self.assertEqual(result.reason, "SUBMISSION_UNCERTAIN_ORDER_NOT_FOUND")

    def test_restart_recovery_queries_once_and_never_submits(self):
        client = ScriptedClient(query_result=self.response("entry-1", status="FILLED"))
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)

        result = broker.recover_order("BTCUSDT", "entry-1")

        self.assertEqual(client.submissions, [])
        self.assertEqual(len(client.queries), 1)
        self.assertEqual(result.status, "FILLED")
        self.assertEqual(result.reason, "RECOVERED_AFTER_RESTART")

    def test_definitive_rejection_is_not_queried_or_retried(self):
        client = ScriptedClient(submit_error=ValueError("bad quantity"))
        broker = BinanceTestnetBroker(client, clock=lambda: NOW)

        with self.assertRaisesRegex(ValueError, "bad quantity"):
            broker.submit_order(intent())
        self.assertEqual(len(client.submissions), 1)
        self.assertEqual(client.queries, [])


if __name__ == "__main__":
    unittest.main()
