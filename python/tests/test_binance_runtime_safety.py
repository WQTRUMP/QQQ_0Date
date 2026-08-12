"""
[INPUT]: 依赖 BinanceRuntime、共享 testnet 交易所桩、真实 RuntimeStateStore 与 durable dispatch/保护单组合
[OUTPUT]: 验证活动委托阻断、行情单调性、未知入/出场恢复、保护同步限频闩、持久未决平仓清理屏障、部分成交精确清仓，以及 fallback 明确拒单保留保护并安全重试
[POS]: python/tests 的 testnet 运行时安全组合回归，与 test_binance_runtime.py 的 paper/通用行为分层
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from python.binance_trading.config import BinanceConfig
from python.binance_trading.exchange import BinanceApiError, BinanceRateLimitError
from python.binance_trading.models import (
    BookTicker,
    Direction,
    ExecutionResult,
    MarkPrice,
    OrderIntent,
)
from python.binance_trading.protection import build_protection_legs
from python.binance_trading.runtime import BinanceRuntime
from python.binance_trading.state import PositionPlan, RuntimeStateStore
from python.binance_trading.strategy import TradeSignal
from python.tests.binance_runtime_support import (
    NOW,
    PublicDemoClient,
    TestnetAccountClient,
    TestnetTradingClient,
    UnusedStream,
)


class BinanceRuntimeSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        config = BinanceConfig.from_mapping(
            {
                "TRADING_ENABLED": "true",
                "BINANCE_PAPER_DB_PATH": str(Path(self.temporary.name) / "paper.db"),
                "BINANCE_HISTORY_LIMIT": "50",
            }
        )
        self.runtime = BinanceRuntime(
            config,
            client=PublicDemoClient(),
            market_stream=UnusedStream(),
            clock=lambda: NOW,
        )
        self.runtime._started = True

    async def asyncTearDown(self) -> None:
        self.runtime.stop()
        self.temporary.cleanup()

    def install_managed_testnet_position(
        self, client: TestnetTradingClient, database_name: str
    ) -> PositionPlan:
        self.runtime.stop()
        config = BinanceConfig.from_mapping(
            {
                "EXECUTION_MODE": "testnet",
                "BINANCE_API_KEY": "demo-key",
                "BINANCE_API_SECRET": "demo-secret",
                "TRADING_ENABLED": "true",
                "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
                "BINANCE_TESTNET_DB_PATH": str(
                    Path(self.temporary.name) / database_name
                ),
            }
        )
        self.runtime = BinanceRuntime(
            config,
            client=client,
            market_stream=UnusedStream(),
            clock=lambda: NOW,
        )
        self.runtime._started = True
        self.runtime._ensure_components()
        plan = PositionPlan(
            symbol="BTCUSDT",
            direction=Direction.LONG,
            quantity=Decimal("0.01"),
            entry_price=Decimal("60000"),
            stop_price=Decimal("59000"),
            target_price=Decimal("62000"),
            signal_id="managed-signal",
            entry_order_id="managed-entry",
            opened_at=NOW,
            updated_at=NOW,
        )
        intent = OrderIntent(
            client_order_id=plan.entry_order_id,
            symbol=plan.symbol,
            direction=plan.direction,
            side="BUY",
            quantity=plan.quantity,
            order_type="LIMIT",
            limit_price=plan.entry_price,
            reduce_only=False,
            created_at=NOW,
            signal_id=plan.signal_id,
            stop_price=plan.stop_price,
            target_price=plan.target_price,
        )
        result = ExecutionResult(
            client_order_id=plan.entry_order_id,
            status="FILLED",
            order_id="7001",
            executed_quantity=plan.quantity,
            average_price=plan.entry_price,
            observed_at=NOW,
        )
        store = self.runtime._state_store
        store.prepare_order(intent)
        store.mark_dispatch_uncertain(intent.client_order_id, NOW)
        store.record_result(result)
        store.apply_entry_plan(
            intent.client_order_id,
            plan,
            protection_legs=build_protection_legs(plan),
        )
        client.position_amount = plan.quantity
        client.entry_price = plan.entry_price
        armed = self.runtime._protection.synchronize(
            plans={plan.symbol: plan},
            position_amounts={plan.symbol: plan.quantity},
            managed_symbols={plan.symbol},
            open_algo_rows=(),
        )
        self.assertEqual(armed.reason, "")
        return plan

    async def test_testnet_active_order_blocks_reconciliation(self) -> None:
        self.runtime.stop()
        config = BinanceConfig.from_mapping(
            {
                "EXECUTION_MODE": "testnet",
                "BINANCE_API_KEY": "demo-key",
                "BINANCE_API_SECRET": "demo-secret",
                "TRADING_ENABLED": "true",
                "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
                "BINANCE_TESTNET_DB_PATH": str(Path(self.temporary.name) / "testnet.db"),
                "BINANCE_HISTORY_LIMIT": "50",
            }
        )
        client = TestnetAccountClient(
            [{"symbol": "ETHUSDT", "clientOrderId": "manual-gtc", "status": "NEW"}]
        )
        self.runtime = BinanceRuntime(
            config,
            client=client,
            market_stream=UnusedStream(),
            clock=lambda: NOW,
        )
        self.runtime._started = True

        await self.runtime.bootstrap_once()

        self.assertEqual(
            self.runtime._reconcile_reason,
            "active_open_order:ETHUSDT:manual-gtc",
        )
        self.assertEqual(client.config_changes, [])
        self.assertFalse(self.runtime.ready())

    async def test_testnet_entry_arms_native_algo_legs_and_local_fallback_cancels_them(self) -> None:
        self.runtime.stop()
        config = BinanceConfig.from_mapping(
            {
                "EXECUTION_MODE": "testnet",
                "BINANCE_API_KEY": "demo-key",
                "BINANCE_API_SECRET": "demo-secret",
                "TRADING_ENABLED": "true",
                "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
                "BINANCE_TESTNET_DB_PATH": str(Path(self.temporary.name) / "native.db"),
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
        signal = TradeSignal(
            symbol="BTCUSDT",
            interval="1m",
            direction="LONG",
            bar_start_time=int((NOW - timedelta(minutes=1)).timestamp() * 1000),
            atr=Decimal("100"),
        )

        await self.runtime._enter_from_signal(signal)

        plan = self.runtime._state_store.get_plan("BTCUSDT")
        self.assertIsNotNone(plan)
        self.assertEqual(len(client.algo_submissions), 2)
        self.assertEqual(
            self.runtime._state_store.get_protection_set(plan.entry_order_id).state,
            "ARMED",
        )
        self.assertEqual(self.runtime._reconcile_reason, "")

        exit_mark = MarkPrice(
            symbol="BTCUSDT",
            mark_price=plan.target_price + Decimal("1"),
            index_price=plan.target_price + Decimal("1"),
            funding_rate=Decimal("0.0001"),
            next_funding_time=NOW + timedelta(hours=4),
            event_time=NOW + timedelta(seconds=1),
        )
        client.mark_price = exit_mark.mark_price
        await self.runtime.handle_event(exit_mark)

        self.assertEqual(client.position_amount, Decimal("0"))
        self.assertTrue(client.normal_submissions[-1]["reduceOnly"])
        self.assertEqual(len(client.algo_cancellations), 2)
        self.assertIsNone(self.runtime._state_store.get_plan("BTCUSDT"))
        self.assertEqual(self.runtime._state_store.protection_sets(), ())

    async def test_protection_sync_rate_limit_latches_until_successful_refresh(self) -> None:
        class RateLimitedProtectionClient(TestnetTradingClient):
            fail_next_open_algo = False

            def open_algo_orders(self):
                if self.fail_next_open_algo:
                    self.fail_next_open_algo = False
                    self.rate_limit_remaining_seconds = 1.0
                    raise BinanceRateLimitError(
                        429, -1003, "slow down", 1.0, locally_blocked=False
                    )
                return super().open_algo_orders()

        self.runtime.stop()
        client = RateLimitedProtectionClient()
        config = BinanceConfig.from_mapping(
            {
                "EXECUTION_MODE": "testnet",
                "BINANCE_API_KEY": "demo-key",
                "BINANCE_API_SECRET": "demo-secret",
                "TRADING_ENABLED": "true",
                "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
                "BINANCE_TESTNET_DB_PATH": str(Path(self.temporary.name) / "sync-latch.db"),
            }
        )
        self.runtime = BinanceRuntime(
            config, client=client, market_stream=UnusedStream(), clock=lambda: NOW
        )
        self.runtime._started = True
        await self.runtime.bootstrap_once()
        await self.runtime.handle_event(
            BookTicker(
                "BTCUSDT", Decimal("60000"), Decimal("2"), Decimal("60001"),
                Decimal("2"), NOW, NOW, 1,
            )
        )
        await self.runtime.handle_event(
            MarkPrice(
                "BTCUSDT", Decimal("60000.5"), Decimal("60000.4"), Decimal("0"),
                NOW + timedelta(hours=4), NOW,
            )
        )
        signal = TradeSignal(
            symbol="BTCUSDT",
            direction="LONG",
            bar_start_time=int((NOW - timedelta(minutes=1)).timestamp() * 1000),
            atr=Decimal("100"),
        )
        client.fail_next_open_algo = True

        with self.assertRaises(BinanceRateLimitError):
            await self.runtime._enter_from_signal(signal)

        self.assertEqual(len(client.normal_submissions), 1)
        self.assertTrue(self.runtime._reconcile_reason.startswith("protection_sync_deferred:"))
        client.rate_limit_remaining_seconds = 0.0
        self.assertFalse(self.runtime.ready())
        await self.runtime._enter_from_signal(
            replace(signal, bar_start_time=signal.bar_start_time + 60_000)
        )
        self.assertEqual(len(client.normal_submissions), 1)
        await self.runtime._refresh_testnet_account()
        self.assertEqual(self.runtime._reconcile_reason, "")
        self.assertEqual(len(client.algo_submissions), 2)

    async def test_testnet_open_orders_poll_failure_immediately_blocks(self) -> None:
        self.runtime.stop()
        config = BinanceConfig.from_mapping(
            {
                "EXECUTION_MODE": "testnet",
                "BINANCE_API_KEY": "demo-key",
                "BINANCE_API_SECRET": "demo-secret",
                "TRADING_ENABLED": "true",
                "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
                "BINANCE_TESTNET_DB_PATH": str(Path(self.temporary.name) / "poll.db"),
                "ACCOUNT_POLL_SECONDS": "1",
                "BINANCE_HISTORY_LIMIT": "50",
            }
        )
        client = TestnetAccountClient([])
        self.runtime = BinanceRuntime(
            config,
            client=client,
            market_stream=UnusedStream(),
            clock=lambda: NOW,
        )
        self.runtime._started = True
        await self.runtime.bootstrap_once()
        self.assertEqual(self.runtime._reconcile_reason, "")
        client.open_orders_error = RuntimeError("open orders unavailable")
        self.runtime._async_stop = asyncio.Event()

        task = asyncio.create_task(self.runtime._account_loop())
        for _ in range(100):
            if self.runtime._reconcile_reason.startswith("account_poll_failed:"):
                break
            await asyncio.sleep(0.001)
        self.runtime._async_stop.set()
        await task

        self.assertEqual(self.runtime._reconcile_reason, "account_poll_failed:RuntimeError")
        self.assertFalse(self.runtime.ready())

    async def test_stale_or_conflicting_mark_cannot_trigger_protection_exit(self) -> None:
        client = TestnetTradingClient()
        plan = self.install_managed_testnet_position(client, "mark-ordering.db")
        await self.runtime._refresh_testnet_account()
        fresh = MarkPrice(
            symbol=plan.symbol,
            mark_price=plan.entry_price,
            index_price=plan.entry_price,
            funding_rate=Decimal("0"),
            next_funding_time=NOW + timedelta(hours=4),
            event_time=NOW,
        )
        await self.runtime.handle_event(fresh)
        submissions = len(client.normal_submissions)
        stale = MarkPrice(
            symbol=plan.symbol,
            mark_price=plan.stop_price - Decimal("1"),
            index_price=plan.stop_price - Decimal("1"),
            funding_rate=Decimal("0"),
            next_funding_time=NOW + timedelta(hours=2),
            event_time=NOW - timedelta(hours=2),
        )

        await self.runtime.handle_event(stale)

        self.assertEqual(self.runtime._marks[plan.symbol], fresh)
        self.assertEqual(len(client.normal_submissions), submissions)
        self.assertEqual(client.position_amount, plan.quantity)
        conflicting = MarkPrice(
            symbol=plan.symbol,
            mark_price=plan.stop_price - Decimal("2"),
            index_price=plan.stop_price - Decimal("2"),
            funding_rate=Decimal("0"),
            next_funding_time=fresh.next_funding_time,
            event_time=fresh.event_time,
        )
        with self.assertRaisesRegex(ValueError, "冲突"):
            await self.runtime.handle_event(conflicting)
        self.assertEqual(self.runtime._marks[plan.symbol], fresh)
        self.assertEqual(len(client.normal_submissions), submissions)
        self.assertTrue(
            self.runtime._sticky_reason.startswith("market_data_conflict:BTCUSDT:mark_price")
        )

    async def test_running_account_loop_converges_nonterminal_order_without_resubmit(self) -> None:
        class RecoveringClient(TestnetTradingClient):
            def __init__(self):
                super().__init__()
                self.query_count = 0

            def query_order(self, symbol, client_order_id):
                del symbol
                self.query_count += 1
                terminal = self.query_count >= 2
                if terminal:
                    self.position_amount = Decimal("0.01")
                    self.entry_price = Decimal("60000")
                return {
                    "clientOrderId": client_order_id,
                    "orderId": 77,
                    "status": "FILLED" if terminal else "NEW",
                    "executedQty": "0.01" if terminal else "0",
                    "avgPrice": "60000" if terminal else "0",
                    "updateTime": int(NOW.timestamp() * 1000),
                }

            def new_order(self, params):
                raise AssertionError("恢复路径不得重投原订单")

        self.runtime.stop()
        path = Path(self.temporary.name) / "recovery.db"
        config = BinanceConfig.from_mapping(
            {
                "EXECUTION_MODE": "testnet",
                "BINANCE_API_KEY": "demo-key",
                "BINANCE_API_SECRET": "demo-secret",
                "TRADING_ENABLED": "true",
                "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
                "BINANCE_TESTNET_DB_PATH": str(path),
                "ACCOUNT_POLL_SECONDS": "1",
                "BINANCE_HISTORY_LIMIT": "50",
            }
        )
        store = RuntimeStateStore(path)
        pending = OrderIntent(
            client_order_id="recover-entry",
            symbol="BTCUSDT",
            direction=Direction.LONG,
            side="BUY",
            quantity=Decimal("0.01"),
            order_type="LIMIT",
            limit_price=Decimal("60000"),
            reduce_only=False,
            created_at=NOW,
            signal_id="recover-signal",
            stop_price=Decimal("59000"),
            target_price=Decimal("62000"),
        )
        store.prepare_order(pending)
        store.mark_dispatch_uncertain(pending.client_order_id, NOW)
        client = RecoveringClient()
        self.runtime = BinanceRuntime(
            config,
            client=client,
            market_stream=UnusedStream(),
            state_store=store,
            clock=lambda: NOW,
        )
        self.runtime._started = True
        await self.runtime.bootstrap_once()
        self.assertEqual(client.query_count, 1)
        self.assertEqual(client.config_changes, [])
        self.assertIn("BTCUSDT", self.runtime._uncertain_symbols)
        self.runtime._async_stop = asyncio.Event()

        task = asyncio.create_task(self.runtime._account_loop())
        for _ in range(200):
            record = store.get_order("recover-entry")
            bundle = store.get_protection_set("recover-entry")
            if record is not None and record.phase == "APPLIED" and bundle is not None:
                break
            await asyncio.sleep(0.001)
        self.runtime._async_stop.set()
        await task

        self.assertEqual(client.query_count, 2)
        self.assertEqual(store.get_order("recover-entry").phase, "APPLIED")
        self.assertEqual(store.get_protection_set("recover-entry").state, "ARMED")
        self.assertNotIn("BTCUSDT", self.runtime._uncertain_symbols)
        self.assertEqual(self.runtime._sticky_reason, "")

    async def test_flat_account_cannot_delete_plan_before_unknown_exit_becomes_visible(self) -> None:
        class EventuallyVisibleExitClient(TestnetTradingClient):
            def __init__(self):
                super().__init__()
                self.query_count = 0

            def query_order(self, symbol, client_order_id):
                del symbol
                self.query_count += 1
                if self.query_count == 1:
                    raise BinanceApiError(400, -2013, "Order does not exist")
                return {
                    "clientOrderId": client_order_id,
                    "orderId": 9009,
                    "status": "FILLED",
                    "executedQty": "0.01",
                    "avgPrice": "60000",
                    "updateTime": int(NOW.timestamp() * 1000),
                }

        client = EventuallyVisibleExitClient()
        plan = self.install_managed_testnet_position(client, "late-exit-query.db")
        exit_intent = OrderIntent(
            client_order_id="late-visible-exit",
            symbol=plan.symbol,
            direction=plan.direction,
            side="SELL",
            quantity=plan.quantity,
            order_type="MARKET",
            limit_price=None,
            reduce_only=True,
            created_at=NOW,
            signal_id=plan.signal_id,
            reason="TARGET",
        )
        store = self.runtime._state_store
        store.prepare_order(exit_intent)
        store.mark_dispatch_uncertain(exit_intent.client_order_id, NOW)
        client.position_amount = Decimal("0")

        await self.runtime._recover_orders()
        await self.runtime._refresh_testnet_account()

        self.assertIsNotNone(store.get_plan(plan.symbol))
        self.assertIsNotNone(store.get_protection_set(plan.entry_order_id))
        self.assertEqual(
            store.get_order(exit_intent.client_order_id).phase,
            "DISPATCH_UNCERTAIN",
        )

        await self.runtime._recover_orders()
        await self.runtime._refresh_testnet_account()

        self.assertEqual(store.get_order(exit_intent.client_order_id).phase, "APPLIED")
        self.assertIsNone(store.get_plan(plan.symbol))
        self.assertIsNone(store.get_protection_set(plan.entry_order_id))
        self.assertNotIn(plan.symbol, self.runtime._uncertain_symbols)
        self.assertEqual(self.runtime._sticky_reason, "")

    async def test_terminal_exit_cannot_clear_other_durable_unknown_for_symbol(self) -> None:
        client = TestnetTradingClient()
        plan = self.install_managed_testnet_position(client, "parallel-exits.db")
        await self.runtime._claim_local_protection(plan)
        store = self.runtime._state_store
        unknown = OrderIntent(
            client_order_id="parallel-exit-unknown",
            symbol=plan.symbol,
            direction=plan.direction,
            side="SELL",
            quantity=plan.quantity,
            order_type="MARKET",
            limit_price=None,
            reduce_only=True,
            created_at=NOW,
            signal_id=plan.signal_id,
            reason="TARGET",
        )
        rejected = OrderIntent(
            client_order_id="parallel-exit-rejected",
            symbol=plan.symbol,
            direction=plan.direction,
            side="SELL",
            quantity=plan.quantity,
            order_type="MARKET",
            limit_price=None,
            reduce_only=True,
            created_at=NOW + timedelta(milliseconds=1),
            signal_id=plan.signal_id,
            reason="PROTECTION_FAIL",
        )
        store.prepare_order(unknown)
        store.mark_dispatch_uncertain(unknown.client_order_id, NOW)
        self.runtime._mark_uncertain(plan.symbol, "FIRST_EXIT_UNKNOWN")
        store.prepare_order(rejected)
        store.mark_dispatch_uncertain(rejected.client_order_id, NOW)
        result = ExecutionResult(
            client_order_id=rejected.client_order_id,
            status="REJECTED",
            order_id="",
            executed_quantity=Decimal("0"),
            average_price=Decimal("0"),
            observed_at=NOW,
            reason="DEFINITIVE_REJECTION:BinanceApiError",
        )
        store.record_result(result)

        await self.runtime._apply_exit_result(plan, result, "PROTECTION_FAIL")

        self.assertEqual(store.get_order(rejected.client_order_id).phase, "APPLIED")
        self.assertEqual(store.get_order(unknown.client_order_id).phase, "DISPATCH_UNCERTAIN")
        self.assertIn(plan.symbol, self.runtime._uncertain_symbols)
        submissions = len(client.normal_submissions)
        await self.runtime._refresh_testnet_account()
        self.assertEqual(len(client.normal_submissions), submissions)
        self.assertEqual(client.position_amount, plan.quantity)
        self.assertIsNotNone(store.get_plan(plan.symbol))
        self.assertTrue(self.runtime._order_unknown_reason.startswith("order_state_unknown:"))

    async def test_native_protection_rejection_immediately_flattens_reduce_only(self) -> None:
        class RejectingProtectionClient(TestnetTradingClient):
            def new_algo_order(self, params):
                self.algo_submissions.append(dict(params))
                raise BinanceApiError(400, -2021, "Order would immediately trigger")

        self.runtime.stop()
        config = BinanceConfig.from_mapping(
            {
                "EXECUTION_MODE": "testnet",
                "BINANCE_API_KEY": "demo-key",
                "BINANCE_API_SECRET": "demo-secret",
                "TRADING_ENABLED": "true",
                "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
                "BINANCE_TESTNET_DB_PATH": str(Path(self.temporary.name) / "reject.db"),
                "BINANCE_HISTORY_LIMIT": "50",
            }
        )
        client = RejectingProtectionClient()
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
                funding_rate=Decimal("0.0001"),
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

        await self.runtime._enter_from_signal(signal)

        self.assertEqual(client.position_amount, Decimal("0"))
        self.assertEqual(len(client.normal_submissions), 2)
        self.assertTrue(client.normal_submissions[-1]["reduceOnly"])
        self.assertEqual(len(client.algo_submissions), 1)
        self.assertIsNone(self.runtime._state_store.get_plan("BTCUSDT"))
        self.assertTrue(
            any(
                "fail_closed_exit:protection_not_armed" in event["message"]
                for event in self.runtime._events
            )
        )

    async def test_native_partial_fill_exits_only_authoritative_remaining_quantity(self) -> None:
        client = TestnetTradingClient()
        plan = self.install_managed_testnet_position(client, "partial-native.db")
        stop = self.runtime._state_store.get_protection_leg(plan.entry_order_id, "STOP")
        target = self.runtime._state_store.get_protection_leg(plan.entry_order_id, "TARGET")
        client.algo_orders[stop.spec.client_algo_id].update(
            {
                "algoStatus": "FINISHED",
                "actualOrderId": 9001,
                "actualQty": "0.006",
                "actualPrice": "58990",
            }
        )
        client.position_amount = Decimal("0.004")

        await self.runtime._refresh_testnet_account()

        self.assertEqual(client.position_amount, Decimal("0"))
        self.assertEqual(client.normal_submissions[-1]["quantity"], "0.004")
        self.assertTrue(client.normal_submissions[-1]["reduceOnly"])
        self.assertEqual(client.algo_cancellations, [target.spec.client_algo_id])
        self.assertIsNone(self.runtime._state_store.get_plan(plan.symbol))

    async def test_rejected_local_fallback_keeps_native_protection_open(self) -> None:
        class RejectingExitClient(TestnetTradingClient):
            def new_order(self, params):
                if params.get("reduceOnly"):
                    self.normal_submissions.append(dict(params))
                    raise BinanceApiError(400, -2022, "ReduceOnly Order is rejected")
                return super().new_order(params)

        client = RejectingExitClient()
        plan = self.install_managed_testnet_position(client, "rejected-fallback.db")

        await self.runtime._claim_local_protection(plan)
        await self.runtime._exit(plan, "TARGET", plan.target_price)
        await self.runtime._refresh_testnet_account()

        bundle = self.runtime._state_store.get_protection_set(plan.entry_order_id)
        self.assertEqual(client.position_amount, plan.quantity)
        self.assertEqual(bundle.winner_kind, "LOCAL")
        self.assertEqual(client.algo_cancellations, [])
        self.assertEqual(
            [leg.phase for leg in self.runtime._state_store.protection_legs(plan.entry_order_id)],
            ["OPEN", "OPEN"],
        )
        self.assertNotIn(plan.symbol, self.runtime._uncertain_symbols)
        self.assertEqual(len(client.normal_submissions), 2)
        await self.runtime._refresh_testnet_account()
        self.assertEqual(len(client.normal_submissions), 3)
        self.assertEqual(
            len({row["newClientOrderId"] for row in client.normal_submissions}),
            3,
        )

        stop = self.runtime._state_store.get_protection_leg(plan.entry_order_id, "STOP")
        client.algo_orders[stop.spec.client_algo_id].update(
            {
                "algoStatus": "FINISHED",
                "actualOrderId": 9003,
                "actualQty": "0.006",
                "actualPrice": "58990",
            }
        )
        client.position_amount = Decimal("0.004")
        await self.runtime._refresh_testnet_account()

        self.assertEqual(
            self.runtime._state_store.get_plan(plan.symbol).quantity,
            Decimal("0.004"),
        )
        self.assertEqual(client.normal_submissions[-1]["quantity"], "0.004")
        self.assertEqual(client.algo_cancellations, [])


if __name__ == "__main__":
    unittest.main()
