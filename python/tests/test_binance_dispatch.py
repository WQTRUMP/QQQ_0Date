"""
[INPUT]: 依赖 DurableOrderDispatcher、RuntimeStateStore 与脚本化 testnet broker
[OUTPUT]: 验证持久化先于发送、跨连接 CAS 只选一个派发者、未知提交跨重启只查询一次、PREPARED 未派发可安全放弃
[POS]: python/tests 的 Binance 派单崩溃恢复合同，禁止用进程重启换取重复订单权限
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import unittest
import tempfile
import threading
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from python.binance_trading.dispatch import DurableOrderDispatcher
from python.binance_trading.models import Direction, ExecutionResult, OrderIntent
from python.binance_trading.state import RuntimeStateStore


NOW = datetime(2026, 8, 11, 12, tzinfo=timezone.utc)


def intent(order_id: str = "entry-1") -> OrderIntent:
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


class UnknownThenRecoveredBroker:
    def __init__(self) -> None:
        self.submissions = 0
        self.queries = 0

    def submit_order(self, order: OrderIntent) -> ExecutionResult:
        self.submissions += 1
        return ExecutionResult(
            client_order_id=order.client_order_id,
            status="UNKNOWN",
            order_id="",
            executed_quantity=Decimal("0"),
            average_price=Decimal("0"),
            observed_at=NOW,
            reason="SUBMISSION_UNCERTAIN",
            submission_unknown=True,
        )

    def recover_order(self, symbol: str, client_order_id: str) -> ExecutionResult:
        self.queries += 1
        return ExecutionResult(
            client_order_id=client_order_id,
            status="FILLED",
            order_id="exchange-1",
            executed_quantity=Decimal("0.01"),
            average_price=Decimal("60000"),
            observed_at=NOW,
            reason="RECOVERED_AFTER_RESTART",
        )


class BinanceDurableDispatchTests(unittest.TestCase):
    def test_two_connections_can_claim_only_one_dispatch_winner(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dispatch.db"
            barrier = threading.Barrier(2)

            class BarrierStore(RuntimeStateStore):
                def prepare_order(self, order):
                    record = super().prepare_order(order)
                    barrier.wait(timeout=2)
                    return record

            class CountingBroker(UnknownThenRecoveredBroker):
                def __init__(self):
                    super().__init__()
                    self.lock = threading.Lock()

                def submit_order(self, order):
                    with self.lock:
                        self.submissions += 1
                    return ExecutionResult(
                        client_order_id=order.client_order_id,
                        status="FILLED",
                        order_id="one-winner",
                        executed_quantity=order.quantity,
                        average_price=Decimal("60000"),
                        observed_at=NOW,
                    )

            stores = (BarrierStore(path), BarrierStore(path))
            broker = CountingBroker()
            dispatchers = tuple(
                DurableOrderDispatcher(
                    mode="testnet", state_store=store, broker=broker, clock=lambda: NOW
                )
                for store in stores
            )
            results = []
            errors = []

            def submit(dispatcher):
                try:
                    results.append(
                        dispatcher.submit(intent(), paper_price=Decimal("60000"))
                    )
                except BaseException as exc:
                    errors.append(exc)

            threads = [threading.Thread(target=submit, args=(item,)) for item in dispatchers]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)

            self.assertEqual(errors, [])
            self.assertEqual(broker.submissions, 1)
            self.assertEqual(len(results), 2)
            self.assertEqual(stores[0].get_order("entry-1").phase, "RESULT")
            for store in stores:
                store.close()

    def test_unknown_submit_is_queried_after_restart_and_never_resubmitted(self):
        store = RuntimeStateStore(":memory:")
        broker = UnknownThenRecoveredBroker()
        dispatcher = DurableOrderDispatcher(
            mode="testnet",
            state_store=store,
            broker=broker,
            clock=lambda: NOW,
        )

        unknown = dispatcher.submit(intent(), paper_price=Decimal("60000"))
        recovered = dispatcher.recover()

        self.assertTrue(unknown.submission_unknown)
        self.assertEqual(broker.submissions, 1)
        self.assertEqual(broker.queries, 1)
        self.assertEqual(len(recovered), 1)
        self.assertEqual(recovered[0].result.status, "FILLED")
        self.assertEqual(store.get_order("entry-1").phase, "RESULT")
        store.close()

    def test_prepared_without_dispatch_barrier_is_safely_abandoned(self):
        store = RuntimeStateStore(":memory:")
        broker = UnknownThenRecoveredBroker()
        store.prepare_order(intent())
        dispatcher = DurableOrderDispatcher(
            mode="testnet",
            state_store=store,
            broker=broker,
            clock=lambda: NOW,
        )

        self.assertEqual(dispatcher.recover(), ())
        self.assertEqual(store.get_order("entry-1").phase, "APPLIED")
        self.assertEqual(broker.submissions, 0)
        self.assertEqual(broker.queries, 0)
        store.close()


if __name__ == "__main__":
    unittest.main()
