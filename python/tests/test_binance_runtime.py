"""
[INPUT]: 依赖 BinanceRuntime、共享 Demo 公共 API 测试桩、真实 paper ledger/state/strategy/risk/broker
[OUTPUT]: 验证未闭 REST K 线、真实 1m 交叉单次派发、保护几何、周期校时、REST 冷却与 paper 离线交易闭环
[POS]: python/tests 的 Binance paper/通用 runtime 组合回归；testnet 恢复与托管保护边界由 test_binance_runtime_safety.py 承载
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import asyncio
import tempfile
import threading
import unittest
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest import mock

from python.binance_trading.config import BinanceConfig
from python.binance_trading.broker import RETRYABLE_SERVER_FAILURE, PaperBroker, normalize_client_order_id
from python.binance_trading.exchange import BinanceApiError, BinanceRateLimitError
from python.binance_trading.ledger import PaperLedger
from python.binance_trading.models import (
    BookTicker,
    Direction,
    ExecutionResult,
    Kline,
    MarkPrice,
    OrderIntent,
)
from python.binance_trading.runtime import BinanceRuntime
from python.binance_trading.state import PositionPlan, RuntimeStateStore
from python.binance_trading.strategy import TradeSignal
from python.tests.binance_runtime_support import NOW, PublicDemoClient, TestnetTradingClient, UnusedStream


class BinanceRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.config = BinanceConfig.from_mapping(
            {
                "TRADING_ENABLED": "true",
                "BINANCE_PAPER_DB_PATH": str(Path(self.temporary.name) / "paper.db"),
                "BINANCE_HISTORY_LIMIT": "50",
            }
        )
        self.client = PublicDemoClient()
        self.runtime = BinanceRuntime(
            self.config,
            client=self.client,
            market_stream=UnusedStream(),
            clock=lambda: NOW,
        )
        self.runtime._started = True

    async def asyncTearDown(self) -> None:
        self.runtime.stop()
        self.temporary.cleanup()

    async def test_account_loop_honors_rate_limit_deadline_and_stop_interrupts_wait(self) -> None:
        self.runtime._async_stop = asyncio.Event()
        waits = []

        async def rejected_recovery():
            raise BinanceRateLimitError(
                429,
                -1003,
                "slow down",
                37.0,
                locally_blocked=True,
            )

        real_wait_for = asyncio.wait_for

        async def observe_wait(awaitable, timeout):
            waits.append(timeout)
            self.runtime._async_stop.set()
            return await real_wait_for(awaitable, timeout=0.1)

        self.runtime._recover_orders = rejected_recovery
        with mock.patch("python.binance_trading.runtime.asyncio.wait_for", new=observe_wait):
            await self.runtime._account_loop()

        self.assertEqual(waits, [37.0])
        self.assertEqual(
            self.runtime._reconcile_reason,
            "account_poll_failed:BinanceRateLimitError",
        )

    async def test_periodic_clock_sync_failure_retries_next_account_poll(self) -> None:
        self.runtime.stop()
        monotonic = [0.0]
        offsets = [0, RuntimeError("clock unavailable"), 25]
        sync_calls = []

        def sync_time():
            sync_calls.append(monotonic[0])
            outcome = offsets.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        client = PublicDemoClient()
        client.sync_time = sync_time
        self.runtime = BinanceRuntime(
            self.config,
            client=client,
            market_stream=UnusedStream(),
            clock=lambda: NOW,
            monotonic=lambda: monotonic[0],
        )
        self.runtime._started = True
        await self.runtime.bootstrap_once()
        monotonic[0] = 299.0
        self.assertFalse(await self.runtime._clock_sync.synchronize())
        monotonic[0] = 300.0
        self.runtime._async_stop = asyncio.Event()
        waits = []
        reasons = []
        remaining = []

        async def advance_wait(awaitable, timeout):
            waits.append(timeout)
            reasons.append(self.runtime._reconcile_reason)
            remaining.append(self.runtime._clock_sync.remaining_seconds)
            monotonic[0] += timeout
            if len(waits) == 2:
                self.runtime._async_stop.set()
                return await awaitable
            awaitable.close()
            raise asyncio.TimeoutError

        with mock.patch("python.binance_trading.runtime.asyncio.wait_for", new=advance_wait):
            await self.runtime._account_loop()

        self.assertEqual(sync_calls, [0.0, 300.0, 315.0])
        self.assertEqual(reasons[0], "account_poll_failed:RuntimeError")
        self.assertEqual(remaining, [0.0, 300.0])
        self.assertEqual(waits, [15.0, 15.0])
        self.assertEqual(self.runtime._clock_sync.offset_ms, 25)

    async def test_timestamp_error_resync_failure_stays_due_for_next_poll(self) -> None:
        self.runtime.stop()
        monotonic = [0.0]
        offsets = [0, RuntimeError("clock unavailable"), 42]
        sync_calls = []

        def sync_time():
            sync_calls.append(monotonic[0])
            outcome = offsets.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        client = PublicDemoClient()
        client.sync_time = sync_time
        self.runtime = BinanceRuntime(
            self.config,
            client=client,
            market_stream=UnusedStream(),
            clock=lambda: NOW,
            monotonic=lambda: monotonic[0],
        )
        self.runtime._started = True
        await self.runtime.bootstrap_once()
        self.runtime._async_stop = asyncio.Event()
        recovery_calls = 0
        observed_sync_counts = []

        async def timestamp_once():
            nonlocal recovery_calls
            recovery_calls += 1
            if recovery_calls == 1:
                raise BinanceApiError(400, -1021, "timestamp outside recvWindow")

        async def advance_wait(awaitable, timeout):
            observed_sync_counts.append(len(sync_calls))
            monotonic[0] += timeout
            if len(observed_sync_counts) == 2:
                self.runtime._async_stop.set()
                return await awaitable
            awaitable.close()
            raise asyncio.TimeoutError

        self.runtime._recover_orders = timestamp_once
        with mock.patch("python.binance_trading.runtime.asyncio.wait_for", new=advance_wait):
            await self.runtime._account_loop()

        self.assertEqual(observed_sync_counts, [2, 3])
        self.assertEqual(sync_calls, [0.0, 0.0, 15.0])
        self.assertEqual(self.runtime._clock_sync.offset_ms, 42)
        self.assertEqual(self.runtime._clock_sync.remaining_seconds, 285.0)

    async def test_paper_mark_stop_executes_during_public_rest_cooldown(self) -> None:
        await self.runtime.bootstrap_once()
        await self.runtime.handle_event(
            BookTicker(
                symbol="BTCUSDT",
                bid_price=Decimal("60000"),
                bid_quantity=Decimal("2"),
                ask_price=Decimal("60001"),
                ask_quantity=Decimal("2"),
                event_time=NOW,
                transaction_time=NOW,
                update_id=1,
            )
        )
        await self.runtime.handle_event(
            MarkPrice(
                symbol="BTCUSDT",
                mark_price=Decimal("60000.5"),
                index_price=Decimal("60000.4"),
                funding_rate=Decimal("0"),
                next_funding_time=NOW + timedelta(hours=4),
                event_time=NOW,
            )
        )
        self.client.rate_limit_remaining_seconds = 60.0
        raw = TradeSignal(
            symbol="BTCUSDT",
            interval="1m",
            direction="LONG",
            bar_start_time=int((NOW - timedelta(minutes=1)).timestamp() * 1000),
            atr=Decimal("100"),
        )

        await self.runtime._enter_from_signal(raw)

        plan = self.runtime._state_store.get_plan("BTCUSDT")
        self.assertIsNotNone(plan)
        self.assertEqual(self.runtime._ledger.get_position("BTCUSDT").side, "LONG")
        await self.runtime.handle_event(
            MarkPrice(
                symbol="BTCUSDT",
                mark_price=plan.stop_price - Decimal("1"),
                index_price=plan.stop_price - Decimal("1"),
                funding_rate=Decimal("0"),
                next_funding_time=NOW + timedelta(hours=4),
                event_time=NOW + timedelta(seconds=1),
            )
        )

        self.assertEqual(self.runtime._ledger.get_position("BTCUSDT").side, "FLAT")
        self.assertIsNone(self.runtime._state_store.get_plan("BTCUSDT"))
        self.assertGreater(
            self.runtime._state_store.exit_attempt_count(plan.symbol, plan.signal_id), 0
        )

    async def test_testnet_rest_cooldown_creates_no_entry_or_local_exit_attempt(self) -> None:
        self.runtime.stop()
        config = BinanceConfig.from_mapping(
            {
                "EXECUTION_MODE": "testnet",
                "BINANCE_API_KEY": "demo-key",
                "BINANCE_API_SECRET": "demo-secret",
                "TRADING_ENABLED": "true",
                "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
                "BINANCE_TESTNET_DB_PATH": str(
                    Path(self.temporary.name) / "cooldown-testnet.db"
                ),
                "BINANCE_HISTORY_LIMIT": "50",
            }
        )
        client = TestnetTradingClient()
        self.runtime = BinanceRuntime(
            config,
            client=client,
            market_stream=UnusedStream(),
            clock=lambda: NOW,
        )
        self.runtime._started = True
        await self.runtime.bootstrap_once()
        await self.runtime.handle_event(
            BookTicker(
                symbol="BTCUSDT",
                bid_price=Decimal("60000"),
                bid_quantity=Decimal("2"),
                ask_price=Decimal("60001"),
                ask_quantity=Decimal("2"),
                event_time=NOW,
                transaction_time=NOW,
                update_id=1,
            )
        )
        await self.runtime.handle_event(
            MarkPrice(
                symbol="BTCUSDT",
                mark_price=Decimal("60000.5"),
                index_price=Decimal("60000.4"),
                funding_rate=Decimal("0"),
                next_funding_time=NOW + timedelta(hours=4),
                event_time=NOW,
            )
        )
        signal = TradeSignal(
            symbol="BTCUSDT",
            interval="1m",
            direction="LONG",
            bar_start_time=int((NOW - timedelta(minutes=1)).timestamp() * 1000),
            atr=Decimal("100"),
        )
        client.rate_limit_remaining_seconds = 60.0
        self.assertFalse(self.runtime.ready())

        await self.runtime._enter_from_signal(signal)

        entry_id = normalize_client_order_id(
            "e-BTCUSDT-%d-L" % signal.bar_start_time
        )
        self.assertIsNone(self.runtime._state_store.get_order(entry_id))
        self.assertEqual(client.normal_submissions, [])
        client.rate_limit_remaining_seconds = 0.0
        await self.runtime._enter_from_signal(signal)
        plan = self.runtime._state_store.get_plan("BTCUSDT")
        self.assertIsNotNone(plan)
        self.assertEqual(len(client.normal_submissions), 1)
        self.assertEqual(len(client.algo_submissions), 2)
        client.rate_limit_remaining_seconds = 60.0

        await self.runtime._fail_closed_exit(plan, "PROTECTION_SYNC_FAILED:BinanceRateLimitError")

        self.assertIsNone(
            self.runtime._state_store.get_protection_set(plan.entry_order_id).winner_kind
        )
        self.assertEqual(
            self.runtime._state_store.exit_attempt_count(plan.symbol, plan.signal_id),
            0,
        )

        await self.runtime.handle_event(
            MarkPrice(
                symbol="BTCUSDT",
                mark_price=plan.target_price + Decimal("1"),
                index_price=plan.target_price + Decimal("1"),
                funding_rate=Decimal("0"),
                next_funding_time=NOW + timedelta(hours=4),
                event_time=NOW + timedelta(seconds=1),
            )
        )

        self.assertEqual(len(client.normal_submissions), 1)
        self.assertEqual(client.algo_cancellations, [])
        self.assertEqual(
            self.runtime._state_store.exit_attempt_count(plan.symbol, plan.signal_id),
            0,
        )
        self.assertEqual(
            self.runtime._state_store.get_protection_set(plan.entry_order_id).state,
            "ARMED",
        )

    async def test_worker_thread_is_non_daemon(self) -> None:
        self.runtime.start()
        self.assertFalse(self.runtime._thread.daemon)
        self.runtime.stop()

    async def test_history_drops_current_unfinished_rest_bar(self) -> None:
        await self.runtime.bootstrap_once()

        strategy = self.runtime._strategies["BTCUSDT"].snapshot()
        expected_last_open = self.client.rows[-2][0]
        self.assertEqual(strategy.last_bar_start_time, expected_last_open)
        self.assertNotEqual(strategy.last_bar_start_time, self.client.rows[-1][0])

    async def test_live_closed_one_minute_cross_dispatches_exactly_once(self) -> None:
        await self.runtime.bootstrap_once()
        live_now = NOW + timedelta(seconds=35)
        self.runtime.clock = lambda: live_now
        await self.runtime.handle_event(
            BookTicker(
                "BTCUSDT", Decimal("60000"), Decimal("2"),
                Decimal("60001"), Decimal("2"), live_now, live_now, 1,
            )
        )
        await self.runtime.handle_event(
            MarkPrice(
                "BTCUSDT", Decimal("60000.5"), Decimal("60000.4"),
                Decimal("0"), live_now + timedelta(hours=4), live_now,
            )
        )
        opened = NOW.replace(second=0, microsecond=0)
        closed_bar = Kline(
            symbol="BTCUSDT",
            interval="1m",
            open_time=opened,
            close_time=opened + timedelta(seconds=59, milliseconds=999),
            open=Decimal("59980"),
            high=Decimal("60000"),
            low=Decimal("9990"),
            close=Decimal("10000"),
            volume=Decimal("10"),
            quote_volume=Decimal("100000"),
            trades=100,
            closed=True,
        )

        await self.runtime.handle_event(closed_bar)
        await self.runtime.handle_event(closed_bar)

        plan = self.runtime._state_store.get_plan("BTCUSDT")
        self.assertIsNotNone(plan)
        self.assertEqual(plan.direction, Direction.SHORT)
        self.assertEqual(
            sum(event["message"] == "entry_filled" for event in self.runtime.snapshot()["events"]),
            1,
        )

    async def test_stop_keeps_store_and_thread_handle_when_worker_is_still_alive(self) -> None:
        class AliveWorker:
            def __init__(self) -> None:
                self.join_timeout = None

            def join(self, timeout=None):
                self.join_timeout = timeout

            def is_alive(self):
                return True

        await self.runtime.bootstrap_once()
        worker = AliveWorker()
        self.runtime._thread = worker

        with self.assertRaisesRegex(RuntimeError, "仍在运行"):
            self.runtime.stop()

        self.assertIs(self.runtime._thread, worker)
        self.assertFalse(self.runtime._closed)
        self.assertEqual(self.runtime._sticky_reason, "shutdown_incomplete:worker_alive")
        self.runtime._thread = None

    async def test_stop_from_worker_thread_never_closes_recovery_store(self) -> None:
        await self.runtime.bootstrap_once()
        worker = threading.current_thread()
        self.runtime._thread = worker

        with self.assertRaisesRegex(RuntimeError, "自身线程"):
            self.runtime.stop()

        self.assertIs(self.runtime._thread, worker)
        self.assertFalse(self.runtime._closed)
        self.assertEqual(self.runtime._sticky_reason, "shutdown_incomplete:self_thread")
        self.runtime._thread = None

    async def test_definitive_retryable_exit_failure_keeps_plan_without_unknown_latch(self) -> None:
        await self.runtime.bootstrap_once()
        plan = PositionPlan(
            symbol="BTCUSDT",
            direction=Direction.LONG,
            quantity=Decimal("0.01"),
            entry_price=Decimal("60000"),
            stop_price=Decimal("59000"),
            target_price=Decimal("62000"),
            signal_id="signal-retry",
            entry_order_id="entry-retry",
            opened_at=NOW - timedelta(minutes=1),
            updated_at=NOW - timedelta(minutes=1),
        )
        self.runtime._state_store.save_plan(plan)
        intent = OrderIntent(
            client_order_id="exit-retry-1",
            symbol="BTCUSDT",
            direction=Direction.LONG,
            side="SELL",
            quantity=plan.quantity,
            order_type="MARKET",
            limit_price=None,
            reduce_only=True,
            created_at=NOW,
            signal_id=plan.signal_id,
            reason="STOP",
        )
        result = ExecutionResult(
            client_order_id=intent.client_order_id,
            status="REJECTED",
            order_id="",
            executed_quantity=Decimal("0"),
            average_price=Decimal("0"),
            observed_at=NOW,
            reason=RETRYABLE_SERVER_FAILURE,
        )
        self.runtime._state_store.prepare_order(intent)
        self.runtime._state_store.mark_dispatch_uncertain(intent.client_order_id, NOW)
        self.runtime._state_store.record_result(result)

        await self.runtime._apply_exit_result(plan, result, "STOP")

        self.assertIsNotNone(self.runtime._state_store.get_plan("BTCUSDT"))
        self.assertEqual(self.runtime._state_store.get_order(intent.client_order_id).phase, "APPLIED")
        self.assertNotIn("BTCUSDT", self.runtime._uncertain_symbols)
        self.assertEqual(self.runtime._sticky_reason, "")

    async def test_conflicting_closed_bar_latches_runtime_safety_block(self) -> None:
        await self.runtime.bootstrap_once()
        row = self.client.rows[-2]
        conflicting = Kline(
            symbol="BTCUSDT",
            interval="1m",
            open_time=datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc),
            close_time=datetime.fromtimestamp(row[6] / 1000, tz=timezone.utc),
            open=Decimal(row[1]),
            high=Decimal(row[2]) + Decimal("1"),
            low=Decimal(row[3]),
            close=Decimal(row[4]),
            volume=Decimal(row[5]),
            quote_volume=Decimal(row[7]),
            trades=row[8],
            closed=True,
        )

        with self.assertRaisesRegex(ValueError, "冲突"):
            await self.runtime.handle_event(conflicting)

        self.assertTrue(self.runtime._sticky_reason.startswith("market_data_conflict:BTCUSDT"))
        safety_latch = self.runtime._sticky_reason
        self.runtime._mark_uncertain("BTCUSDT", "RECOVERY_ORDER_NOT_FOUND")
        self.runtime._clear_uncertain("BTCUSDT")
        self.assertEqual(self.runtime._sticky_reason, safety_latch)

    async def test_signal_enters_and_mark_target_exits_with_durable_journal(self) -> None:
        await self.runtime.bootstrap_once()
        book = BookTicker(
            symbol="BTCUSDT",
            bid_price=Decimal("60000"),
            bid_quantity=Decimal("2"),
            ask_price=Decimal("60001"),
            ask_quantity=Decimal("2"),
            event_time=NOW,
            transaction_time=NOW,
            update_id=1,
        )
        mark = MarkPrice(
            symbol="BTCUSDT",
            mark_price=Decimal("60000.5"),
            index_price=Decimal("60000.4"),
            funding_rate=Decimal("0.0001"),
            next_funding_time=NOW + timedelta(hours=4),
            event_time=NOW,
        )
        await self.runtime.handle_event(book)
        await self.runtime.handle_event(mark)
        self.assertTrue(self.runtime.ready())

        raw_signal = TradeSignal(
            symbol="BTCUSDT",
            interval="1m",
            direction="LONG",
            bar_start_time=int((NOW - timedelta(minutes=1)).timestamp() * 1000),
            atr=Decimal("100"),
        )
        await self.runtime._enter_from_signal(raw_signal)

        plan = self.runtime._state_store.get_plan("BTCUSDT")
        self.assertIsNotNone(plan)
        self.assertGreater(plan.quantity, Decimal("0"))
        self.assertEqual(self.runtime._ledger.get_position("BTCUSDT").side, "LONG")
        entry_record = self.runtime._state_store.get_order(plan.entry_order_id)
        self.assertEqual(entry_record.phase, "APPLIED")

        exit_book = BookTicker(
            symbol="BTCUSDT",
            bid_price=plan.target_price + Decimal("5"),
            bid_quantity=Decimal("2"),
            ask_price=plan.target_price + Decimal("5.1"),
            ask_quantity=Decimal("2"),
            event_time=NOW,
            transaction_time=NOW,
            update_id=2,
        )
        exit_mark = MarkPrice(
            symbol="BTCUSDT",
            mark_price=plan.target_price + Decimal("5"),
            index_price=plan.target_price + Decimal("4.9"),
            funding_rate=Decimal("0.0001"),
            next_funding_time=NOW + timedelta(hours=4),
            event_time=NOW + timedelta(seconds=1),
        )
        await self.runtime.handle_event(exit_book)
        await self.runtime.handle_event(exit_mark)

        self.assertEqual(self.runtime._ledger.get_position("BTCUSDT").side, "FLAT")
        self.assertIsNone(self.runtime._state_store.get_plan("BTCUSDT"))
        self.assertEqual(self.runtime._state_store.unresolved_orders(), ())
        self.assertEqual(len(self.runtime.snapshot()["positions"]), 0)

    async def test_extreme_atr_is_rejected_before_order_journal_or_fill(self) -> None:
        await self.runtime.bootstrap_once()
        await self.runtime.handle_event(
            BookTicker(
                symbol="BTCUSDT",
                bid_price=Decimal("60000"),
                bid_quantity=Decimal("2"),
                ask_price=Decimal("60001"),
                ask_quantity=Decimal("2"),
                event_time=NOW,
                transaction_time=NOW,
                update_id=1,
            )
        )
        await self.runtime.handle_event(
            MarkPrice(
                symbol="BTCUSDT",
                mark_price=Decimal("60000.5"),
                index_price=Decimal("60000.4"),
                funding_rate=Decimal("0"),
                next_funding_time=NOW + timedelta(hours=4),
                event_time=NOW,
            )
        )
        signal = TradeSignal(
            symbol="BTCUSDT",
            direction="SHORT",
            bar_start_time=int((NOW - timedelta(minutes=1)).timestamp() * 1000),
            atr=Decimal("50000"),
        )

        await self.runtime._enter_from_signal(signal)

        client_id = normalize_client_order_id(
            "e-BTCUSDT-%d-S" % signal.bar_start_time
        )
        self.assertIsNone(self.runtime._state_store.get_order(client_id))
        self.assertEqual(self.runtime._ledger.get_position("BTCUSDT").side, "FLAT")
        self.assertIn(
            "INVALID_PROTECTION_GEOMETRY",
            [event["message"] for event in self.runtime.snapshot()["events"]],
        )
        await self.runtime._enter_from_signal(replace(signal, atr=Decimal("0.1")))
        self.assertIsNone(self.runtime._state_store.get_order(client_id))
        self.assertIn(
            "INVALID_ENTRY_GEOMETRY",
            [event["message"] for event in self.runtime.snapshot()["events"]],
        )

    async def test_carried_paper_position_waits_for_live_mark_before_utc_baseline(self) -> None:
        self.runtime.stop()
        path = Path(self.temporary.name) / "carried.db"
        config = BinanceConfig.from_mapping(
            {
                "BINANCE_PAPER_DB_PATH": str(path),
                "BINANCE_HISTORY_LIMIT": "50",
            }
        )
        opened_at = NOW - timedelta(days=1)
        ledger = PaperLedger(path, initial_balance="10000")
        PaperBroker(ledger, clock=lambda: opened_at).open_long(
            "BTCUSDT", "0.01", "59000", "old-entry", now=opened_at
        )
        store = RuntimeStateStore(path)
        store.save_plan(
            PositionPlan(
                symbol="BTCUSDT",
                direction=Direction.LONG,
                quantity=Decimal("0.01"),
                entry_price=Decimal("59000"),
                stop_price=Decimal("58000"),
                target_price=Decimal("61000"),
                signal_id="old-signal",
                entry_order_id="old-entry",
                opened_at=opened_at,
                updated_at=opened_at,
            )
        )
        ledger.close()
        store.close()
        self.runtime = BinanceRuntime(
            config,
            client=PublicDemoClient(),
            market_stream=UnusedStream(),
            clock=lambda: NOW,
        )
        self.runtime._started = True

        await self.runtime.bootstrap_once()

        self.assertIsNone(self.runtime._account)
        self.assertEqual(self.runtime._reconcile_reason, "paper_mark_missing:BTCUSDT")
        connection = sqlite3.connect(str(path))
        try:
            dates = [row[0] for row in connection.execute("SELECT utc_date FROM daily_baselines")]
        finally:
            connection.close()
        self.assertNotIn(NOW.date().isoformat(), dates)

        await self.runtime.handle_event(
            MarkPrice(
                symbol="BTCUSDT",
                mark_price=Decimal("60000"),
                index_price=Decimal("60000"),
                funding_rate=Decimal("0"),
                next_funding_time=NOW + timedelta(hours=4),
                event_time=NOW,
            )
        )
        self.assertIsNotNone(self.runtime._account)

    async def test_stale_carried_mark_cannot_seed_next_utc_day_baseline(self) -> None:
        self.runtime.stop()
        path = Path(self.temporary.name) / "stale-midnight.db"
        before_midnight = datetime(2026, 8, 11, 23, 59, 50, tzinfo=timezone.utc)
        after_midnight = datetime(2026, 8, 12, 0, 0, 10, tzinfo=timezone.utc)
        config = BinanceConfig.from_mapping(
            {
                "BINANCE_PAPER_DB_PATH": str(path),
                "PAPER_STARTING_EQUITY": "1000",
                "MAX_BOOK_AGE_SECONDS": "3",
            }
        )
        ledger = PaperLedger(path, initial_balance="1000")
        ledger.record_fill(
            "midnight-entry",
            "BTCUSDT",
            "BUY",
            "1",
            "100",
            executed_at=before_midnight - timedelta(minutes=1),
        )
        store = RuntimeStateStore(path)
        store.save_plan(
            PositionPlan(
                symbol="BTCUSDT",
                direction=Direction.LONG,
                quantity=Decimal("1"),
                entry_price=Decimal("100"),
                stop_price=Decimal("50"),
                target_price=Decimal("200"),
                signal_id="midnight-signal",
                entry_order_id="midnight-entry",
                opened_at=before_midnight - timedelta(minutes=1),
                updated_at=before_midnight - timedelta(minutes=1),
            )
        )
        ledger.close()
        store.close()
        self.runtime = BinanceRuntime(
            config,
            client=PublicDemoClient(),
            market_stream=UnusedStream(),
            clock=lambda: after_midnight,
        )
        self.runtime._started = True
        self.runtime._ensure_components()
        self.runtime._marks["BTCUSDT"] = MarkPrice(
            symbol="BTCUSDT",
            mark_price=Decimal("90"),
            index_price=Decimal("90"),
            funding_rate=Decimal("0"),
            next_funding_time=after_midnight + timedelta(hours=4),
            event_time=before_midnight,
        )

        self.runtime._refresh_paper_account()

        self.assertIsNone(self.runtime._account)
        self.assertEqual(self.runtime._reconcile_reason, "paper_mark_stale:BTCUSDT")
        connection = sqlite3.connect(str(path))
        try:
            row = connection.execute(
                "SELECT equity FROM daily_baselines WHERE utc_date = '2026-08-12'"
            ).fetchone()
        finally:
            connection.close()
        self.assertIsNone(row)

        self.runtime._marks["BTCUSDT"] = replace(
            self.runtime._marks["BTCUSDT"],
            mark_price=Decimal("110"),
            index_price=Decimal("110"),
            event_time=after_midnight,
        )
        self.runtime._refresh_paper_account()

        self.assertEqual(self.runtime._account.day_start_equity, Decimal("1010"))


if __name__ == "__main__":
    unittest.main()
