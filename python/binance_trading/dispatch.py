"""
[INPUT]: 依赖已验证订单意图、paper/testnet broker、PaperLedger 与 RuntimeStateStore 的订单阶段事务
[OUTPUT]: 提供 DurableOrderDispatcher 和 RecoveryItem，在任何网络调用前固化 DISPATCH_UNCERTAIN，并按 clientOrderId 恢复而不重投
[POS]: binance_trading 的持久化派单边界；隔离“是否可再次发送”的证明，使 runtime 只处理明确结果对保护计划的应用
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Optional, Tuple

from .ledger import LedgerFill, PaperLedger
from .models import ExecutionResult, OrderIntent
from .state import OrderJournalRecord, RuntimeStateStore


ZERO = Decimal("0")
TERMINAL_NO_FILL = frozenset(("CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED"))
TERMINAL_STATUSES = frozenset(("FILLED",)) | TERMINAL_NO_FILL


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


@dataclass(frozen=True)
class RecoveryItem:
    record: OrderJournalRecord
    result: ExecutionResult


class DurableOrderDispatcher:
    """Persist-before-send adapter; a non-terminal response remains non-resubmittable."""

    def __init__(
        self,
        *,
        mode: str,
        state_store: RuntimeStateStore,
        broker: Any,
        ledger: Optional[PaperLedger] = None,
        clock: Any,
    ) -> None:
        if mode not in ("paper", "testnet"):
            raise ValueError("dispatcher mode 只允许 paper/testnet")
        if mode == "paper" and ledger is None:
            raise ValueError("paper dispatcher 必须持有 ledger")
        self.mode = mode
        self.state_store = state_store
        self.broker = broker
        self.ledger = ledger
        self.clock = clock

    @staticmethod
    def _normalize_result(result: ExecutionResult) -> ExecutionResult:
        if result.status in TERMINAL_STATUSES:
            return result
        reason = result.reason or "NON_TERMINAL_RESPONSE:%s" % result.status
        return replace(result, reason=reason, submission_unknown=True)

    @staticmethod
    def _paper_result(fill: LedgerFill) -> ExecutionResult:
        return ExecutionResult(
            client_order_id=fill.client_order_id,
            status="FILLED",
            order_id=fill.exchange_order_id,
            executed_quantity=fill.quantity,
            average_price=fill.price,
            observed_at=_parse_utc(fill.executed_at),
            reason="RECOVERED_FROM_PAPER_LEDGER",
        )

    def _actual_submit(
        self,
        intent: OrderIntent,
        paper_price: Decimal,
        mark_prices: Optional[Mapping[str, Decimal]],
    ) -> ExecutionResult:
        if self.mode == "paper":
            return self.broker.submit_order(
                intent,
                fill_price=paper_price,
                observed_at=self.clock(),
                mark_prices=mark_prices,
            )
        return self.broker.submit_order(intent)

    def submit(
        self,
        intent: OrderIntent,
        *,
        paper_price: Decimal,
        mark_prices: Optional[Mapping[str, Decimal]] = None,
    ) -> ExecutionResult:
        record = self.state_store.prepare_order(intent)
        if record.phase in ("RESULT", "APPLIED"):
            if record.result is None:
                raise RuntimeError("订单阶段已有结果但载荷缺失")
            return record.result
        if record.phase == "DISPATCH_UNCERTAIN":
            if record.result is not None:
                return record.result
            return ExecutionResult(
                client_order_id=intent.client_order_id,
                status="UNKNOWN",
                order_id="",
                executed_quantity=ZERO,
                average_price=ZERO,
                observed_at=self.clock(),
                reason="DURABLE_DISPATCH_UNCERTAIN",
                submission_unknown=True,
            )
        record, claimed = self.state_store.claim_dispatch_uncertain(
            intent.client_order_id, self.clock()
        )
        if not claimed:
            if record.result is not None:
                return record.result
            return ExecutionResult(
                client_order_id=intent.client_order_id,
                status="UNKNOWN",
                order_id="",
                executed_quantity=ZERO,
                average_price=ZERO,
                observed_at=self.clock(),
                reason="DURABLE_DISPATCH_UNCERTAIN",
                submission_unknown=True,
            )
        try:
            result = self._actual_submit(intent, paper_price, mark_prices)
        except Exception as exc:
            if self.mode == "paper" and self.ledger is not None:
                fill = self.ledger.get_fill(intent.client_order_id)
                if fill is not None:
                    result = self._paper_result(fill)
                else:
                    result = self._definitive_rejection(intent, exc)
            else:
                result = self._definitive_rejection(intent, exc)
        result = self._normalize_result(result)
        self.state_store.record_result(result)
        return result

    def _definitive_rejection(
        self, intent: OrderIntent, error: BaseException
    ) -> ExecutionResult:
        return ExecutionResult(
            client_order_id=intent.client_order_id,
            status="REJECTED",
            order_id="",
            executed_quantity=ZERO,
            average_price=ZERO,
            observed_at=self.clock(),
            reason="DEFINITIVE_REJECTION:%s" % type(error).__name__,
        )

    def recover(self) -> Tuple[RecoveryItem, ...]:
        recovered = []
        for record in self.state_store.unresolved_orders():
            if record.phase == "PREPARED":
                self.state_store.abandon_prepared(record.intent.client_order_id, self.clock())
                continue
            if record.phase == "RESULT":
                if record.result is None:
                    raise RuntimeError("RESULT 订单缺少结果载荷")
                recovered.append(RecoveryItem(record, record.result))
                continue
            if self.mode == "paper":
                fill = self.ledger.get_fill(record.intent.client_order_id) if self.ledger else None
                result = self._paper_result(fill) if fill is not None else ExecutionResult(
                    client_order_id=record.intent.client_order_id,
                    status="CANCELED",
                    order_id="",
                    executed_quantity=ZERO,
                    average_price=ZERO,
                    observed_at=self.clock(),
                    reason="RECOVERED_NO_LOCAL_FILL",
                )
            else:
                recover_order = getattr(self.broker, "recover_order", None)
                if not callable(recover_order):
                    raise RuntimeError("testnet broker 缺少只查不重投的恢复能力")
                result = recover_order(record.intent.symbol, record.intent.client_order_id)
            result = self._normalize_result(result)
            updated = self.state_store.record_result(result)
            recovered.append(RecoveryItem(updated, result))
        return tuple(recovered)


__all__ = [
    "DurableOrderDispatcher",
    "RecoveryItem",
    "TERMINAL_NO_FILL",
    "TERMINAL_STATUSES",
]
