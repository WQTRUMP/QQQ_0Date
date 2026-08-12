"""
[INPUT]: 依赖 RuntimeStateStore、保护腿持久化契约与 SQLite 跨进程恢复
[OUTPUT]: 验证入场+两腿原子写、phase/revision CAS、胜者互斥、LOCAL 退出后置撤单屏障、部分成交幂等与重启恢复
[POS]: Binance 交易所托管保护单状态机的崩溃安全回归边界，不触发网络
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from python.binance_trading.models import Direction, ExecutionResult, OrderIntent
from python.binance_trading.protection_state import ProtectionLegSpec
from python.binance_trading.state import PositionPlan, RuntimeStateStore


NOW = datetime(2026, 8, 11, 13, tzinfo=timezone.utc)


def entry_plan(order_id: str = "entry-1") -> PositionPlan:
    return PositionPlan(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        quantity=Decimal("0.02"),
        entry_price=Decimal("60000"),
        stop_price=Decimal("59000"),
        target_price=Decimal("62000"),
        signal_id="signal-1",
        entry_order_id=order_id,
        opened_at=NOW,
        updated_at=NOW,
    )


def protection_legs(order_id: str = "entry-1"):
    return (
        ProtectionLegSpec(
            entry_order_id=order_id,
            symbol="BTCUSDT",
            kind="STOP",
            client_algo_id="pp-entry-1-stop",
            order_type="STOP_MARKET",
            trigger_price=Decimal("59000"),
            side="SELL",
            request_fingerprint="sha256:stop",
        ),
        ProtectionLegSpec(
            entry_order_id=order_id,
            symbol="BTCUSDT",
            kind="TARGET",
            client_algo_id="pp-entry-1-target",
            order_type="TAKE_PROFIT_MARKET",
            trigger_price=Decimal("62000"),
            side="SELL",
            request_fingerprint="sha256:target",
        ),
    )


def prepare_filled_entry(store: RuntimeStateStore, order_id: str = "entry-1") -> None:
    intent = OrderIntent(
        client_order_id=order_id,
        symbol="BTCUSDT",
        direction=Direction.LONG,
        side="BUY",
        quantity=Decimal("0.02"),
        order_type="LIMIT",
        limit_price=Decimal("60010"),
        reduce_only=False,
        created_at=NOW,
        signal_id="signal-1",
        stop_price=Decimal("59000"),
        target_price=Decimal("62000"),
    )
    store.prepare_order(intent)
    store.mark_dispatch_uncertain(order_id, NOW)
    store.record_result(
        ExecutionResult(
            client_order_id=order_id,
            status="FILLED",
            order_id="exchange-entry-1",
            executed_quantity=Decimal("0.02"),
            average_price=Decimal("60000"),
            observed_at=NOW,
        )
    )


class BinanceProtectionStateTests(unittest.TestCase):
    def test_entry_plan_two_legs_and_applied_are_one_transaction(self):
        store = RuntimeStateStore(":memory:")
        prepare_filled_entry(store)

        store.apply_entry_plan("entry-1", entry_plan(), protection_legs())

        self.assertEqual(store.get_order("entry-1").phase, "APPLIED")
        self.assertEqual(store.get_plan("BTCUSDT"), entry_plan())
        self.assertEqual(store.get_protection_set("entry-1").state, "PREPARED")
        self.assertEqual(
            [leg.spec.kind for leg in store.protection_legs("entry-1")],
            ["STOP", "TARGET"],
        )
        store.close()

    def test_invalid_second_leg_rolls_back_plan_bundle_and_applied(self):
        store = RuntimeStateStore(":memory:")
        prepare_filled_entry(store)
        stop, target = protection_legs()
        duplicate_stop = replace(
            target,
            kind="STOP",
            order_type="STOP_MARKET",
            trigger_price=Decimal("59000"),
        )

        with self.assertRaisesRegex(ValueError, "STOP/TARGET"):
            store.apply_entry_plan("entry-1", entry_plan(), (stop, duplicate_stop))

        self.assertEqual(store.get_order("entry-1").phase, "RESULT")
        self.assertIsNone(store.get_plan("BTCUSDT"))
        self.assertIsNone(store.get_protection_set("entry-1"))
        self.assertEqual(store.protection_legs("entry-1"), ())
        store.close()

    def test_phase_and_revision_updates_are_compare_and_swap(self):
        store = RuntimeStateStore(":memory:")
        prepare_filled_entry(store)
        store.apply_entry_plan("entry-1", entry_plan(), protection_legs())

        self.assertTrue(
            store.transition_protection_leg(
                "entry-1", "STOP", "PREPARED", "SUBMIT_UNKNOWN", NOW
            )
        )
        self.assertFalse(
            store.transition_protection_leg(
                "entry-1", "STOP", "PREPARED", "SUBMIT_UNKNOWN", NOW
            )
        )
        self.assertTrue(
            store.transition_protection_leg(
                "entry-1",
                "STOP",
                "SUBMIT_UNKNOWN",
                "OPEN",
                NOW,
                algo_id="algo-stop",
                algo_status="NEW",
            )
        )
        self.assertTrue(
            store.transition_protection_set("entry-1", "PREPARED", "ARMING", 0, NOW)
        )
        self.assertFalse(
            store.transition_protection_set("entry-1", "PREPARED", "ARMING", 0, NOW)
        )
        self.assertEqual(store.get_protection_set("entry-1").revision, 1)
        store.close()

    def test_first_winner_is_stable_and_sibling_becomes_cancel_unknown(self):
        store = RuntimeStateStore(":memory:")
        prepare_filled_entry(store)
        store.apply_entry_plan("entry-1", entry_plan(), protection_legs())
        for kind in ("STOP", "TARGET"):
            store.transition_protection_leg(
                "entry-1", kind, "PREPARED", "SUBMIT_UNKNOWN", NOW
            )
            store.transition_protection_leg(
                "entry-1", kind, "SUBMIT_UNKNOWN", "OPEN", NOW
            )

        self.assertTrue(
            store.claim_protection_winner(
                "entry-1", "STOP", NOW, expected_revision=0
            )
        )
        self.assertFalse(store.claim_protection_winner("entry-1", "TARGET", NOW))

        protection_set = store.get_protection_set("entry-1")
        self.assertEqual(protection_set.winner_kind, "STOP")
        self.assertEqual(protection_set.state, "EXITING")
        self.assertEqual(protection_set.revision, 1)
        self.assertEqual(store.get_protection_leg("entry-1", "STOP").phase, "TRIGGERED")
        self.assertEqual(
            store.get_protection_leg("entry-1", "TARGET").phase,
            "CANCEL_UNKNOWN",
        )
        store.close()

    def test_local_winner_defers_native_cancellation_until_explicit_barrier(self):
        store = RuntimeStateStore(":memory:")
        prepare_filled_entry(store)
        store.apply_entry_plan("entry-1", entry_plan(), protection_legs())
        for kind in ("STOP", "TARGET"):
            store.transition_protection_leg(
                "entry-1", kind, "PREPARED", "SUBMIT_UNKNOWN", NOW
            )
            store.transition_protection_leg(
                "entry-1", kind, "SUBMIT_UNKNOWN", "OPEN", NOW
            )

        self.assertTrue(store.claim_protection_winner("entry-1", "LOCAL", NOW))

        claimed = store.get_protection_set("entry-1")
        self.assertEqual(claimed.winner_kind, "LOCAL")
        self.assertEqual(claimed.state, "EXITING")
        self.assertEqual(claimed.revision, 1)
        self.assertEqual(
            [leg.phase for leg in store.protection_legs("entry-1")],
            ["OPEN", "OPEN"],
        )

        self.assertTrue(store.begin_local_protection_cancellation("entry-1", NOW))
        canceling = store.get_protection_set("entry-1")
        self.assertEqual(canceling.state, "CANCELING")
        self.assertEqual(canceling.revision, 2)
        self.assertEqual(
            [leg.phase for leg in store.protection_legs("entry-1")],
            ["CANCEL_UNKNOWN", "CANCEL_UNKNOWN"],
        )

        self.assertFalse(store.begin_local_protection_cancellation("entry-1", NOW))
        self.assertEqual(store.get_protection_set("entry-1").revision, 2)
        self.assertEqual(
            [leg.phase for leg in store.protection_legs("entry-1")],
            ["CANCEL_UNKNOWN", "CANCEL_UNKNOWN"],
        )
        store.close()

    def test_local_cancellation_barrier_rejects_native_winner(self):
        store = RuntimeStateStore(":memory:")
        prepare_filled_entry(store)
        store.apply_entry_plan("entry-1", entry_plan(), protection_legs())
        store.claim_protection_winner("entry-1", "STOP", NOW)

        with self.assertRaisesRegex(ValueError, "LOCAL"):
            store.begin_local_protection_cancellation("entry-1", NOW)
        store.close()

    def test_partial_fill_delta_is_idempotent_and_state_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.db"
            store = RuntimeStateStore(path)
            prepare_filled_entry(store)
            store.apply_entry_plan("entry-1", entry_plan(), protection_legs())
            self.assertEqual(
                store.record_protection_fill(
                    "entry-1", "STOP", "0.005", "58990", NOW,
                    actual_order_id="actual-stop",
                ),
                Decimal("0.005"),
            )
            self.assertEqual(
                store.record_protection_fill(
                    "entry-1", "STOP", "0.005", "58990", NOW,
                    actual_order_id="actual-stop",
                ),
                Decimal("0"),
            )
            store.close()

            recovered = RuntimeStateStore(path)
            leg = recovered.get_protection_leg("entry-1", "STOP")
            self.assertEqual(leg.phase, "TRIGGERED")
            self.assertEqual(leg.actual_order_id, "actual-stop")
            self.assertEqual(leg.cumulative_filled_quantity, Decimal("0.005"))
            self.assertEqual(recovered.get_protection_set("entry-1").state, "PREPARED")
            recovered.close()


if __name__ == "__main__":
    unittest.main()
