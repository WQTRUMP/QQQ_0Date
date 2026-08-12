"""
[INPUT]: 依赖 Binance RuntimeStateStore 的 SQLite quick_check、订单阶段、保护计划与 UTC 日基线
[OUTPUT]: 验证启动完整性失败关闭、跨重启恢复、原子应用、数量收敛、并发删除不复活、退出计数和日基线不变性
[POS]: Binance 派单与持仓保护恢复真源的启动门禁与事务回归
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import tempfile
import unittest
import sqlite3
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from python.binance_trading.models import Direction, ExecutionResult, OrderIntent
from python.binance_trading.state import PositionPlan, RuntimeStateStore


NOW = datetime(2026, 8, 11, 12, tzinfo=timezone.utc)


class _QuickCheckResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _QuickCheckConnection:
    def __init__(self, rows=(), error=None):
        self.rows = rows
        self.error = error
        self.row_factory = None
        self.closed = False

    def execute(self, statement):
        if statement != "PRAGMA quick_check":
            raise AssertionError("quick_check 失败后不得继续初始化")
        if self.error is not None:
            raise self.error
        return _QuickCheckResult(self.rows)

    def close(self):
        self.closed = True


def plan(order_id: str = "order-1") -> PositionPlan:
    return PositionPlan(
        symbol="BTCUSDT",
        direction=Direction.LONG,
        quantity=Decimal("0.01"),
        entry_price=Decimal("60000"),
        stop_price=Decimal("59000"),
        target_price=Decimal("62000"),
        signal_id="signal-1",
        entry_order_id=order_id,
        opened_at=NOW,
        updated_at=NOW,
    )


def entry_intent(order_id: str = "order-1") -> OrderIntent:
    return OrderIntent(
        client_order_id=order_id,
        symbol="BTCUSDT",
        direction=Direction.LONG,
        side="BUY",
        quantity=Decimal("0.01"),
        order_type="LIMIT",
        limit_price=Decimal("60010"),
        reduce_only=False,
        created_at=NOW,
        signal_id="signal-1",
        stop_price=Decimal("59000"),
        target_price=Decimal("62000"),
    )


def fill(order_id: str = "order-1", quantity: str = "0.01") -> ExecutionResult:
    return ExecutionResult(
        client_order_id=order_id,
        status="FILLED",
        order_id="exchange-1",
        executed_quantity=Decimal(quantity),
        average_price=Decimal("60000"),
        observed_at=NOW,
    )


class BinanceRuntimeStateTests(unittest.TestCase):
    def test_normal_startup_passes_sqlite_quick_check(self):
        store = RuntimeStateStore(":memory:")
        result = store._connection.execute("PRAGMA quick_check").fetchall()
        self.assertEqual([row[0] for row in result], ["ok"])
        store.close()

    def test_failed_quick_check_closes_connection_before_schema_write(self):
        connection = _QuickCheckConnection(rows=(("page 2 is never used",),))
        with patch(
            "python.binance_trading.state.sqlite3.connect", return_value=connection
        ):
            with self.assertRaisesRegex(RuntimeError, "quick_check 失败"):
                RuntimeStateStore(":memory:")
        self.assertTrue(connection.closed)

    def test_quick_check_exception_is_wrapped_and_closes_connection(self):
        connection = _QuickCheckConnection(
            error=sqlite3.DatabaseError("injected corruption")
        )
        with patch(
            "python.binance_trading.state.sqlite3.connect", return_value=connection
        ):
            with self.assertRaisesRegex(RuntimeError, "quick_check 执行失败"):
                RuntimeStateStore(":memory:")
        self.assertTrue(connection.closed)

    def test_corrupt_sqlite_file_is_rejected_at_startup(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "corrupt-state.db"
            path.write_bytes(b"not-a-sqlite-database")
            with self.assertRaisesRegex(RuntimeError, "quick_check 执行失败"):
                RuntimeStateStore(path)

    def test_plan_and_day_baseline_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.db"
            store = RuntimeStateStore(path)
            store.save_plan(plan())
            self.assertEqual(store.ensure_day_baseline("10000", NOW), Decimal("10000"))
            self.assertEqual(store.ensure_day_baseline("9000", NOW), Decimal("10000"))
            store.close()

            recovered = RuntimeStateStore(path)
            self.assertEqual(recovered.get_plan("btcusdt"), plan())
            self.assertTrue(recovered.delete_plan("BTCUSDT", "order-1"))
            self.assertIsNone(recovered.get_plan("BTCUSDT"))
            recovered.close()

    def test_same_entry_can_reduce_quantity_but_other_entry_cannot_overwrite(self):
        store = RuntimeStateStore(":memory:")
        original = plan()
        store.save_plan(original)
        store.save_plan(replace(original, quantity=Decimal("0.005")))
        self.assertEqual(store.get_plan("BTCUSDT").quantity, Decimal("0.005"))
        with self.assertRaisesRegex(ValueError, "只能向下"):
            store.save_plan(replace(original, quantity=Decimal("0.006")))
        with self.assertRaisesRegex(ValueError, "覆盖"):
            store.save_plan(plan("order-2"))
        store.close()

    def test_update_only_shrink_never_resurrects_concurrently_deleted_plan(self):
        store = RuntimeStateStore(":memory:")
        stale = plan()
        store.save_plan(stale)
        self.assertTrue(store.delete_plan(stale.symbol, stale.entry_order_id))

        updated = store.shrink_plan_quantity(stale, Decimal("0.004"), NOW)

        self.assertIsNone(updated)
        self.assertIsNone(store.get_plan(stale.symbol))
        store.close()

    def test_entry_result_and_protection_plan_apply_atomically(self):
        store = RuntimeStateStore(":memory:")
        intent = entry_intent()
        self.assertEqual(store.prepare_order(intent).phase, "PREPARED")
        self.assertEqual(store.mark_dispatch_uncertain(intent.client_order_id, NOW).phase, "DISPATCH_UNCERTAIN")
        self.assertEqual(store.record_result(fill()).phase, "RESULT")

        store.apply_entry_plan(intent.client_order_id, plan())

        self.assertEqual(store.get_order(intent.client_order_id).phase, "APPLIED")
        self.assertEqual(store.get_plan("BTCUSDT"), plan())
        self.assertEqual(store.unresolved_orders(), ())
        store.close()

    def test_partial_exit_application_is_idempotent(self):
        store = RuntimeStateStore(":memory:")
        store.save_plan(plan())
        intent = OrderIntent(
            client_order_id="exit-1",
            symbol="BTCUSDT",
            direction=Direction.LONG,
            side="SELL",
            quantity=Decimal("0.01"),
            order_type="MARKET",
            limit_price=None,
            reduce_only=True,
            created_at=NOW,
            signal_id="signal-1",
        )
        self.assertEqual(store.exit_attempt_count("BTCUSDT", "signal-1"), 0)
        store.prepare_order(intent)
        self.assertEqual(store.exit_attempt_count("BTCUSDT", "signal-1"), 1)
        store.mark_dispatch_uncertain("exit-1", NOW)
        result = fill("exit-1", "0.004")
        store.record_result(result)

        store.apply_exit_fill("exit-1", "BTCUSDT", "order-1", Decimal("0.006"), NOW)
        store.apply_exit_fill("exit-1", "BTCUSDT", "order-1", Decimal("0.006"), NOW)

        self.assertEqual(store.get_plan("BTCUSDT").quantity, Decimal("0.006"))
        self.assertEqual(store.get_order("exit-1").phase, "APPLIED")
        store.close()


if __name__ == "__main__":
    unittest.main()
