import importlib.util
import sqlite3
import sys
import types
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path("/workspace/project/python")


def load_module(name: str, relative_path: str):
    sys.modules.setdefault("nats", types.SimpleNamespace())
    module_path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


position_tracker = load_module("position_tracker_main", "position_tracker/main.py")
strategy_engine = load_module("strategy_engine_main", "strategy_engine/main.py")
trade_logger = load_module("trade_logger_main", "trade_logger/main.py")
bootstrap = load_module("bootstrap_module", "common/bootstrap.py")


class PositionTrackerTests(unittest.TestCase):
    def test_exit_fill_with_entry_signal_id_removes_position(self):
        book = position_tracker.PositionBook()
        book.upsert(
            {
                "order_id": "live-short-signal-1",
                "side": "SELL",
                "quantity": "1",
                "price": "1.25",
                "source_signal_id": "signal-theta_harvest-1718030000000-abcd1234",
                "instrument": {"symbol": "QQQ", "strike": "500", "option_right": "PUT"},
            }
        )

        book.upsert(
            {
                "order_id": "live-cover-signal-1",
                "side": "BUY",
                "quantity": "1",
                "price": "0.55",
                "source_signal_id": "signal-theta_harvest-1718030000000-abcd1234",
                "is_exit": True,
                "instrument": {"symbol": "QQQ", "strike": "500", "option_right": "PUT"},
            }
        )

        self.assertEqual(book.positions, {})

    def test_buy_exit_removes_short_position(self):
        book = position_tracker.PositionBook()
        book.upsert(
            {
                "order_id": "live-short-1",
                "side": "SELL",
                "quantity": "1",
                "price": "1.25",
                "source_signal_id": "signal-theta_harvest-1-abc",
                "instrument": {"symbol": "QQQ", "strike": "500", "option_right": "PUT"},
            }
        )

        book.upsert(
            {
                "order_id": "live-cover-1",
                "side": "BUY",
                "quantity": "1",
                "price": "0.55",
                "source_signal_id": "exit-live-short-1",
                "is_exit": True,
                "instrument": {"symbol": "QQQ", "strike": "500", "option_right": "PUT"},
            }
        )

        self.assertEqual(book.positions, {})

    def test_short_position_pnl_pct_uses_short_formula(self):
        book = position_tracker.PositionBook()
        book.upsert(
            {
                "order_id": "live-short-2",
                "side": "SELL",
                "quantity": "1",
                "price": "1.00",
                "source_signal_id": "signal-theta_harvest-2-abc",
                "instrument": {"symbol": "QQQOPT", "strike": "500", "option_right": "PUT"},
            }
        )

        book.update_prices({"QQQOPT": 0.8})

        self.assertEqual(book.positions["live-short-2"]["pnl_pct"], 20.0)


class StrategyEngineTests(unittest.TestCase):
    def test_decimal_from_dict_handles_invalid_operation(self):
        value = strategy_engine.decimal_from_dict({"delta": "bad"}, "delta")
        self.assertEqual(str(value), "0")

    def test_put_delta_selection_accepts_negative_delta(self):
        strategy = strategy_engine.MomentumStrategy("momentum")
        option = strategy._select_option_by_delta(
            {
                "generated_at": "2026-06-10T14:30:00Z",
                "rows": [
                    {"option_right": "PUT", "delta": "-0.42", "strike": "500"},
                    {"option_right": "PUT", "delta": "-0.20", "strike": "499"},
                ],
            },
            strategy_engine.Decimal("500"),
            "SELL",
        )

        self.assertIsNotNone(option)
        self.assertEqual(option["option_right"], "PUT")
        self.assertEqual(option["strike"], "500")


class TradeLoggerTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute(
            """
            CREATE TABLE trade_summary (
                signal_id TEXT PRIMARY KEY,
                session_date TEXT NOT NULL,
                strategy TEXT,
                symbol TEXT,
                signal_action TEXT,
                confidence REAL,
                risk_decision TEXT,
                entry_ts TEXT,
                entry_price REAL,
                entry_qty REAL,
                exit_ts TEXT,
                exit_price REAL,
                exit_reason TEXT,
                pnl REAL,
                pnl_pct REAL,
                held_minutes INTEGER
            )
            """
        )

    def test_exit_fill_updates_canonical_signal_and_settles_short_pnl(self):
        self.db.execute(
            """
            INSERT INTO trade_summary (
                signal_id, session_date, strategy, symbol, signal_action,
                confidence, risk_decision, entry_ts, entry_price, entry_qty
            ) VALUES (?, date('now'), ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "signal-theta_harvest-1718030000000-abcd1234",
                "theta_harvest",
                "QQQOPT",
                "SELL",
                0.8,
                "approved",
                "2026-06-10T14:00:00Z",
                1.2,
                1,
            ),
        )
        self.db.commit()

        trade_logger._update_summary(
            self.db,
            "fill",
            {"is_exit": True},
            "exit-signal-theta_harvest-1718030000000-abcd1234-L0",
            "QQQOPT",
            "theta_harvest",
            "BUY",
            1,
            0.4,
            "2026-06-10T14:10:00Z",
        )

        row = self.db.execute(
            "SELECT exit_price, pnl, pnl_pct, held_minutes FROM trade_summary WHERE signal_id=?",
            ("signal-theta_harvest-1718030000000-abcd1234",),
        ).fetchone()

        self.assertEqual(row, (0.4, 80.0, 66.67, 10))


class BootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_nats_with_retry_retries_before_success(self):
        attempts = {"count": 0}
        expected_conn = types.SimpleNamespace(connected_url="nats://127.0.0.1:4222")

        async def flaky_connect(url):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise RuntimeError("not ready")
            return expected_conn

        nats_stub = types.SimpleNamespace(connect=mock.AsyncMock(side_effect=flaky_connect))
        with mock.patch.object(bootstrap, "nats", nats_stub):
            with mock.patch.object(bootstrap.asyncio, "sleep", new=mock.AsyncMock()) as sleep_mock:
                conn = await bootstrap.connect_nats_with_retry(
                    "nats://127.0.0.1:4222",
                    "test_service",
                    retries=3,
                    delay_sec=0.01,
                )

        self.assertIs(conn, expected_conn)
        self.assertEqual(attempts["count"], 3)
        self.assertEqual(sleep_mock.await_count, 2)


if __name__ == "__main__":
    unittest.main()
