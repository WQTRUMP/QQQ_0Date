"""
[INPUT]: 依赖 binance_trading.ledger 的 SQLite quick_check、成交事务与 Decimal 单向持仓会计
[OUTPUT]: 验证 paper 账本启动完整性失败关闭、幂等、回滚、多空精确 PnL、UTC 日基线和重启恢复
[POS]: tests 的 Binance paper 账本门禁与会计回归；保护资金真源不被损坏库、重复成交或内存状态污染
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import tempfile
import unittest
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from python.binance_trading.ledger import PaperLedger


DAY_ONE = datetime(2026, 8, 11, 23, 59, tzinfo=timezone.utc)
DAY_TWO = datetime(2026, 8, 12, 0, 1, tzinfo=timezone.utc)


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


class BinanceLedgerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "paper.db"
        self.ledger = PaperLedger(self.path, initial_balance="1000")

    def tearDown(self):
        self.ledger.close()
        self.temporary.cleanup()

    def test_normal_startup_passes_sqlite_quick_check(self):
        result = self.ledger._connection.execute("PRAGMA quick_check").fetchall()
        self.assertEqual([row[0] for row in result], ["ok"])

    def test_failed_quick_check_closes_connection_before_schema_write(self):
        connection = _QuickCheckConnection(rows=(("wrong # of entries in index",),))
        with patch(
            "python.binance_trading.ledger.sqlite3.connect", return_value=connection
        ):
            with self.assertRaisesRegex(RuntimeError, "quick_check 失败"):
                PaperLedger(":memory:", initial_balance="1000")
        self.assertTrue(connection.closed)

    def test_quick_check_exception_is_wrapped_and_closes_connection(self):
        connection = _QuickCheckConnection(
            error=sqlite3.DatabaseError("injected corruption")
        )
        with patch(
            "python.binance_trading.ledger.sqlite3.connect", return_value=connection
        ):
            with self.assertRaisesRegex(RuntimeError, "quick_check 执行失败"):
                PaperLedger(":memory:", initial_balance="1000")
        self.assertTrue(connection.closed)

    def test_corrupt_sqlite_file_is_rejected_at_startup(self):
        path = Path(self.temporary.name) / "corrupt-paper.db"
        path.write_bytes(b"not-a-sqlite-database")
        with self.assertRaisesRegex(RuntimeError, "quick_check 执行失败"):
            PaperLedger(path, initial_balance="1000")

    def test_long_fill_and_exit_have_exact_fee_and_pnl(self):
        self.ledger.record_fill(
            "open-long",
            "BTCUSDT",
            "BUY",
            "2",
            "100",
            fee_rate="0.001",
            executed_at=DAY_ONE,
        )
        closing = self.ledger.record_fill(
            "close-long",
            "BTCUSDT",
            "SELL",
            "2",
            "110",
            fee_rate="0.001",
            reduce_only=True,
            executed_at=DAY_ONE,
        )

        snapshot = self.ledger.snapshot(now=DAY_ONE)
        self.assertEqual(closing.realized_pnl, Decimal("20"))
        self.assertEqual(closing.fee, Decimal("0.22"))
        self.assertEqual(snapshot["realized_pnl"], Decimal("20"))
        self.assertEqual(snapshot["fees"], Decimal("0.42"))
        self.assertEqual(snapshot["wallet_balance"], Decimal("1019.58"))
        self.assertEqual(snapshot["open_position_count"], 0)

    def test_short_exit_and_oversized_reduce_only_are_clipped(self):
        self.ledger.record_fill(
            "open-short", "ETHUSDT", "SELL", "3", "100", executed_at=DAY_ONE
        )
        partial = self.ledger.record_fill(
            "short-partial",
            "ETHUSDT",
            "BUY",
            "2",
            "90",
            reduce_only=True,
            executed_at=DAY_ONE,
        )
        final = self.ledger.record_fill(
            "short-final",
            "ETHUSDT",
            "BUY",
            "5",
            "90",
            reduce_only=True,
            executed_at=DAY_ONE,
        )

        self.assertEqual(partial.realized_pnl, Decimal("20"))
        self.assertEqual(self.ledger.get_position("ETHUSDT").quantity, Decimal("0"))
        self.assertEqual(final.requested_quantity, Decimal("5"))
        self.assertEqual(final.quantity, Decimal("1"))
        self.assertEqual(final.realized_pnl, Decimal("10"))

    def test_client_order_id_is_transactionally_idempotent(self):
        first = self.ledger.record_fill(
            "same-id",
            "BTCUSDT",
            "BUY",
            "0.125",
            "40000",
            fee_rate="0.0004",
            executed_at=DAY_ONE,
        )
        before = self.ledger.snapshot(mark_price="40000", now=DAY_ONE)
        replay = self.ledger.record_fill(
            "same-id",
            "BTCUSDT",
            "BUY",
            "0.125",
            "40000",
            fee_rate="0.0004",
            executed_at=DAY_TWO,
        )
        after = self.ledger.snapshot(mark_price="40000", now=DAY_ONE)

        self.assertFalse(first.idempotent)
        self.assertTrue(replay.idempotent)
        self.assertEqual(before["wallet_balance"], after["wallet_balance"])
        self.assertEqual(self.ledger.get_position("BTCUSDT").quantity, Decimal("0.125"))
        with self.assertRaisesRegex(ValueError, "幂等键"):
            self.ledger.record_fill(
                "same-id", "BTCUSDT", "BUY", "0.126", "40000", executed_at=DAY_ONE
            )

    def test_invalid_reduce_only_rolls_back_all_tables(self):
        before = self.ledger.snapshot(now=DAY_ONE)
        with self.assertRaisesRegex(ValueError, "reduce-only"):
            self.ledger.record_fill(
                "bad-exit",
                "BTCUSDT",
                "SELL",
                "1",
                "100",
                reduce_only=True,
                executed_at=DAY_ONE,
            )

        self.assertIsNone(self.ledger.get_fill("bad-exit"))
        after = self.ledger.snapshot(now=DAY_ONE)
        self.assertEqual(before["wallet_balance"], after["wallet_balance"])
        self.assertEqual(before["positions"], after["positions"])

    def test_utc_baseline_is_created_once_and_survives_restart(self):
        self.ledger.record_fill(
            "carry-position",
            "BTCUSDT",
            "BUY",
            "2",
            "100",
            fee_rate="0.001",
            executed_at=DAY_ONE,
        )
        first_day = self.ledger.snapshot(mark_price="110", now=DAY_ONE)
        second_day = self.ledger.snapshot(mark_price="120", now=DAY_TWO)
        later = self.ledger.snapshot(mark_price="90", now=DAY_TWO)

        self.assertEqual(first_day["day_start_equity"], Decimal("1000"))
        self.assertEqual(second_day["day_start_equity"], Decimal("1039.8"))
        self.assertEqual(later["day_start_equity"], Decimal("1039.8"))
        self.assertGreater(later["daily_drawdown_pct"], Decimal("0"))

        self.ledger.close()
        self.ledger = PaperLedger(self.path, initial_balance="1000")
        recovered = self.ledger.snapshot(mark_price="90", now=DAY_TWO)
        self.assertEqual(recovered["day_start_equity"], Decimal("1039.8"))
        self.assertEqual(recovered["positions"]["BTCUSDT"]["quantity"], Decimal("2"))
        self.assertEqual(recovered["fees"], Decimal("0.2"))

    def test_restart_rejects_a_different_initial_balance(self):
        self.ledger.close()
        with self.assertRaisesRegex(ValueError, "initial_balance"):
            PaperLedger(self.path, initial_balance="2000")
        self.ledger = PaperLedger(self.path, initial_balance="1000")


if __name__ == "__main__":
    unittest.main()
