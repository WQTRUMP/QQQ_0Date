"""
[INPUT]: 依赖 ProtectionCoordinator、RuntimeStateStore 与脚本化 Binance Algo REST 事实
[OUTPUT]: 验证 STOP 优先双腿布防、限频同身份安全重试、未知提交只查、部分成交收敛、单赢家撤兄弟和零仓清理
[POS]: Binance 托管保护网络编排的无网络回归；与 protection_state 的纯事务测试分离
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal

from python.binance_trading.exchange import BinanceRateLimitError, BinanceSubmissionUnknown
from python.binance_trading.models import Direction, ExecutionResult, OrderIntent
from python.binance_trading.protection import ProtectionCoordinator
from python.binance_trading.state import PositionPlan, RuntimeStateStore


NOW = datetime(2026, 8, 11, 12, tzinfo=timezone.utc)


def plan() -> PositionPlan:
    return PositionPlan(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        quantity=Decimal("0.01"),
        entry_price=Decimal("60000"),
        stop_price=Decimal("59000"),
        target_price=Decimal("62000"),
        signal_id="signal-1",
        entry_order_id="entry-1",
        opened_at=NOW,
        updated_at=NOW,
    )


class AlgoClient:
    def __init__(self) -> None:
        self.submissions = []
        self.queries = []
        self.cancellations = []
        self.orders = {}
        self.submit_errors = {}
        self.query_errors = {}
        self.cancel_errors = {}

    @staticmethod
    def payload(params, status="NEW", algo_id=1):
        return {
            "algoId": algo_id,
            "clientAlgoId": params["clientAlgoId"],
            "algoType": "CONDITIONAL",
            "symbol": params["symbol"],
            "side": params["side"],
            "positionSide": "BOTH",
            "orderType": params["type"],
            "triggerPrice": params["triggerPrice"],
            "workingType": "MARK_PRICE",
            "closePosition": True,
            "priceProtect": False,
            "algoStatus": status,
        }

    def new_algo_order(self, params):
        self.submissions.append(dict(params))
        client_id = params["clientAlgoId"]
        error = self.submit_errors.get(client_id)
        if error is not None:
            raise error
        payload = self.payload(params, algo_id=len(self.submissions))
        self.orders[client_id] = payload
        return dict(payload)

    def query_algo_order(self, client_algo_id=None, algo_id=None):
        del algo_id
        self.queries.append(client_algo_id)
        error = self.query_errors.get(client_algo_id)
        if error is not None:
            raise error
        return dict(self.orders[client_algo_id])

    def cancel_algo_order(self, client_algo_id=None, algo_id=None):
        del algo_id
        self.cancellations.append(client_algo_id)
        error = self.cancel_errors.get(client_algo_id)
        if error is not None:
            raise error
        payload = dict(self.orders[client_algo_id])
        payload["algoStatus"] = "CANCELED"
        self.orders[client_algo_id] = payload
        return {
            "algoId": payload["algoId"],
            "clientAlgoId": client_algo_id,
            "code": "200",
            "msg": "success",
        }

    def open_algo_orders(self, symbol=None, algo_type=None, algo_id=None):
        del symbol, algo_type, algo_id
        return [
            dict(row)
            for row in self.orders.values()
            if row["algoStatus"] == "NEW"
        ]


class ProtectionCoordinatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.store = RuntimeStateStore(":memory:")
        self.plan = plan()
        intent = OrderIntent(
            client_order_id="entry-1",
            symbol="BTCUSDT",
            direction=Direction.LONG,
            side="BUY",
            quantity=Decimal("0.01"),
            order_type="LIMIT",
            limit_price=Decimal("60000"),
            reduce_only=False,
            created_at=NOW,
            signal_id="signal-1",
            stop_price=Decimal("59000"),
            target_price=Decimal("62000"),
        )
        result = ExecutionResult(
            client_order_id="entry-1",
            status="FILLED",
            order_id="101",
            executed_quantity=Decimal("0.01"),
            average_price=Decimal("60000"),
            observed_at=NOW,
        )
        self.store.prepare_order(intent)
        self.store.mark_dispatch_uncertain("entry-1", NOW)
        self.store.record_result(result)
        self.store.apply_entry_plan("entry-1", self.plan)
        self.client = AlgoClient()
        self.coordinator = ProtectionCoordinator(
            self.client, self.store, clock=lambda: NOW
        )

    def tearDown(self) -> None:
        self.store.close()

    def sync(self, rows=()):
        return self.coordinator.synchronize(
            plans={"BTCUSDT": self.plan},
            position_amounts={"BTCUSDT": Decimal("0.01")},
            managed_symbols={"BTCUSDT"},
            open_algo_rows=rows,
        )

    def test_arms_stop_before_target_with_exact_close_all_contract(self) -> None:
        result = self.sync()

        self.assertEqual(result.reason, "")
        self.assertEqual(result.armed_symbols, ("BTCUSDT",))
        self.assertEqual([row["type"] for row in self.client.submissions], [
            "STOP_MARKET",
            "TAKE_PROFIT_MARKET",
        ])
        for row in self.client.submissions:
            self.assertEqual(row["positionSide"], "BOTH")
            self.assertEqual(row["workingType"], "MARK_PRICE")
            self.assertIs(row["closePosition"], True)
            self.assertNotIn("quantity", row)
            self.assertNotIn("reduceOnly", row)
        self.assertEqual(self.store.get_protection_set("entry-1").state, "ARMED")

    def test_unknown_submit_queries_original_identity_once_before_target(self) -> None:
        from python.binance_trading.protection import build_protection_legs

        stop_id = build_protection_legs(self.plan)[0].client_algo_id
        stop_params = {
            "algoType": "CONDITIONAL",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "positionSide": "BOTH",
            "type": "STOP_MARKET",
            "triggerPrice": "59000",
            "workingType": "MARK_PRICE",
            "closePosition": True,
            "priceProtect": False,
            "clientAlgoId": stop_id,
            "newOrderRespType": "RESULT",
        }
        self.client.orders[stop_id] = self.client.payload(stop_params)
        self.client.submit_errors[stop_id] = BinanceSubmissionUnknown(stop_id, "timeout")

        result = self.sync()

        self.assertEqual(result.reason, "")
        self.assertEqual(self.client.queries, [stop_id])
        self.assertEqual(sum(row["clientAlgoId"] == stop_id for row in self.client.submissions), 1)
        self.assertEqual(len(self.client.submissions), 2)

    def test_rate_limited_algo_returns_to_prepared_and_retries_same_identity(self) -> None:
        from python.binance_trading.protection import build_protection_legs

        stop_id = build_protection_legs(self.plan)[0].client_algo_id
        self.client.submit_errors[stop_id] = BinanceRateLimitError(
            429, -1003, "slow down", 60, locally_blocked=True
        )

        blocked = self.sync()

        self.assertEqual(self.store.get_protection_leg("entry-1", "STOP").phase, "PREPARED")
        self.assertIn("protection_not_armed:BTCUSDT:STOP:PREPARED", blocked.reason)
        self.assertEqual([row["clientAlgoId"] for row in self.client.submissions], [stop_id])
        self.client.submit_errors.pop(stop_id)

        armed = self.sync()

        self.assertEqual(armed.reason, "")
        self.assertEqual(self.store.get_protection_set("entry-1").state, "ARMED")
        self.assertEqual(
            [row["clientAlgoId"] for row in self.client.submissions].count(stop_id), 2
        )

    def test_filled_entry_waits_for_position_risk_visibility_without_deleting_plan(self) -> None:
        pending = self.coordinator.synchronize(
            plans={"BTCUSDT": self.plan},
            position_amounts={"BTCUSDT": Decimal("0")},
            managed_symbols=set(),
            open_algo_rows=(),
            pending_symbols={"BTCUSDT"},
        )

        self.assertEqual(pending.reason, "order_pending:BTCUSDT")
        self.assertEqual(self.store.get_plan("BTCUSDT"), self.plan)
        self.assertEqual(self.client.submissions, [])

        converged = self.sync()
        self.assertEqual(converged.reason, "")
        self.assertEqual(len(self.client.submissions), 2)

    def test_unknown_submit_query_failure_never_submits_target_or_retries_stop(self) -> None:
        from python.binance_trading.protection import build_protection_legs

        stop_id = build_protection_legs(self.plan)[0].client_algo_id
        self.client.submit_errors[stop_id] = BinanceSubmissionUnknown(stop_id, "timeout")
        self.client.query_errors[stop_id] = RuntimeError("query unavailable")

        first = self.sync()
        second = self.sync()

        self.assertIn("protection_not_armed:BTCUSDT:STOP:SUBMIT_UNKNOWN", first.reason)
        self.assertEqual(second.reason, "protection_query_unknown:BTCUSDT:STOP")
        self.assertEqual(len(self.client.submissions), 1)
        self.assertEqual(self.client.queries, [stop_id, stop_id])

    def test_unmanaged_or_mutated_open_algo_order_blocks(self) -> None:
        self.sync()
        rogue = dict(next(iter(self.client.orders.values())))
        rogue["clientAlgoId"] = "manual-condition"
        mutated = dict(next(iter(self.client.orders.values())))
        mutated["triggerPrice"] = "58000"

        rogue_result = self.sync((rogue,))
        mutated_result = self.sync((mutated,))

        self.assertEqual(rogue_result.reason, "unmanaged_algo_order:manual-condition")
        self.assertTrue(mutated_result.reason.startswith("algo_order_mismatch:"))

    def test_missing_open_leg_with_failed_query_blocks_armed_state(self) -> None:
        self.sync()
        stop, target = self.store.protection_legs("entry-1")
        self.client.query_errors[stop.spec.client_algo_id] = RuntimeError("query down")

        result = self.sync((dict(self.client.orders[target.spec.client_algo_id]),))

        self.assertEqual(result.reason, "protection_query_unknown:BTCUSDT:STOP")

    def test_triggered_stop_claims_single_winner_and_cancels_target(self) -> None:
        self.sync()
        legs = self.store.protection_legs("entry-1")
        stop, target = legs
        self.client.orders[stop.spec.client_algo_id]["algoStatus"] = "FINISHED"
        rows = [dict(self.client.orders[target.spec.client_algo_id])]

        result = self.sync(rows)

        bundle = self.store.get_protection_set("entry-1")
        self.assertEqual(bundle.winner_kind, "STOP")
        self.assertEqual(
            self.store.get_protection_leg("entry-1", "TARGET").phase,
            "CANCELED",
        )
        self.assertEqual(self.client.cancellations, [target.spec.client_algo_id])
        self.assertEqual(result.reason, "protection_exit_pending:BTCUSDT")

    def test_native_partial_fill_records_cumulative_fact_and_shrinks_plan(self) -> None:
        self.sync()
        stop, target = self.store.protection_legs("entry-1")
        self.client.orders[stop.spec.client_algo_id].update(
            {
                "algoStatus": "FINISHED",
                "actualOrderId": 9001,
                "actualQty": "0.006",
                "actualPrice": "58990",
            }
        )

        result = self.coordinator.synchronize(
            plans={"BTCUSDT": self.plan},
            position_amounts={"BTCUSDT": Decimal("0.004")},
            managed_symbols=set(),
            open_algo_rows=[dict(self.client.orders[target.spec.client_algo_id])],
        )

        stop = self.store.get_protection_leg("entry-1", "STOP")
        self.assertEqual(result.reason, "protection_exit_pending:BTCUSDT")
        self.assertEqual(stop.actual_order_id, "9001")
        self.assertEqual(stop.cumulative_filled_quantity, Decimal("0.006"))
        self.assertEqual(self.store.get_plan("BTCUSDT").quantity, Decimal("0.004"))

    def test_incomplete_fill_fact_cannot_mutate_phase_or_cancel_sibling(self) -> None:
        self.sync()
        stop, target = self.store.protection_legs("entry-1")
        self.client.orders[stop.spec.client_algo_id].update(
            {"algoStatus": "FINISHED", "actualQty": "0.006"}
        )

        with self.assertRaisesRegex(ValueError, "actualPrice"):
            self.coordinator.synchronize(
                plans={"BTCUSDT": self.plan},
                position_amounts={"BTCUSDT": Decimal("0.004")},
                managed_symbols=set(),
                open_algo_rows=[dict(self.client.orders[target.spec.client_algo_id])],
            )

        self.assertEqual(
            [leg.phase for leg in self.store.protection_legs("entry-1")],
            ["OPEN", "OPEN"],
        )
        self.assertEqual(self.client.cancellations, [])

    def test_canceled_algo_with_fill_keeps_terminal_phase_and_closes_residual(self) -> None:
        self.sync()
        stop, target = self.store.protection_legs("entry-1")
        self.client.orders[stop.spec.client_algo_id].update(
            {
                "algoStatus": "CANCELED",
                "actualOrderId": 9002,
                "actualQty": "0.006",
                "actualPrice": "58980",
            }
        )

        result = self.coordinator.synchronize(
            plans={"BTCUSDT": self.plan},
            position_amounts={"BTCUSDT": Decimal("0.004")},
            managed_symbols=set(),
            open_algo_rows=[dict(self.client.orders[target.spec.client_algo_id])],
        )

        stop = self.store.get_protection_leg("entry-1", "STOP")
        self.assertEqual(result.reason, "protection_exit_pending:BTCUSDT")
        self.assertEqual(stop.phase, "CANCELED")
        self.assertEqual(stop.cumulative_filled_quantity, Decimal("0.006"))
        self.assertEqual(self.store.get_plan("BTCUSDT").quantity, Decimal("0.004"))
        self.assertEqual(self.client.cancellations, [target.spec.client_algo_id])

    def test_local_winner_keeps_native_legs_until_position_is_flat(self) -> None:
        self.sync()
        self.coordinator.claim_local_exit(self.plan)

        result = self.coordinator.synchronize(
            plans={"BTCUSDT": self.plan},
            position_amounts={"BTCUSDT": Decimal("0.01")},
            managed_symbols={"BTCUSDT"},
            open_algo_rows=self.client.open_algo_orders(),
        )

        self.assertEqual(result.reason, "protection_exit_pending:BTCUSDT")
        self.assertEqual(self.client.cancellations, [])
        self.assertEqual(
            [leg.phase for leg in self.store.protection_legs("entry-1")],
            ["OPEN", "OPEN"],
        )

    def test_flat_account_cancels_sibling_then_removes_plan_and_bundle(self) -> None:
        self.sync()
        stop, target = self.store.protection_legs("entry-1")
        self.client.orders[stop.spec.client_algo_id]["algoStatus"] = "FINISHED"
        rows = [dict(self.client.orders[target.spec.client_algo_id])]

        result = self.coordinator.synchronize(
            plans={"BTCUSDT": self.plan},
            position_amounts={"BTCUSDT": Decimal("0")},
            managed_symbols=set(),
            open_algo_rows=rows,
        )

        self.assertEqual(result.reason, "")
        self.assertIsNone(self.store.get_plan("BTCUSDT"))
        self.assertIsNone(self.store.get_protection_set("entry-1"))
        self.assertEqual(self.store.protection_legs("entry-1"), ())

    def test_flat_account_retains_bundle_while_actual_normal_order_is_open(self) -> None:
        self.sync()

        result = self.coordinator.synchronize(
            plans={"BTCUSDT": self.plan},
            position_amounts={"BTCUSDT": Decimal("0")},
            managed_symbols=set(),
            open_algo_rows=self.client.open_algo_orders(),
            normal_orders_clear=False,
        )

        self.assertEqual(result.reason, "hosted_exit_order_pending:BTCUSDT")
        self.assertIsNotNone(self.store.get_plan("BTCUSDT"))
        self.assertIsNotNone(self.store.get_protection_set("entry-1"))


if __name__ == "__main__":
    unittest.main()
