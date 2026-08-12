"""
[INPUT]: 依赖 Binance Algo REST 客户端、RuntimeStateStore 的保护 CAS、已成交 PositionPlan 与账户活动 Algo 事实
[OUTPUT]: 提供 build_protection_legs、ProtectionCoordinator 与 ProtectionSyncResult，完成限频安全重试、两腿提交、部分成交收敛、单赢家与延后撤销
[POS]: binance_trading 的交易所托管保护编排层；普通订单仍由 dispatch 管理，本模块不猜测 User Data Stream 地址也不把未知提交重投
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional, Sequence, Set, Tuple

from .broker import normalize_client_order_id
from .exchange import (
    BinanceApiError,
    BinanceError,
    BinanceRateLimitError,
    BinanceSubmissionUnknown,
    BinanceTransportError,
)
from .protection_state import (
    PROTECTION_TERMINAL_PHASES,
    ProtectionLegRecord,
    ProtectionLegSpec,
)
from .state import PositionPlan, RuntimeStateStore


ZERO = Decimal("0")
OPEN_ALGO_STATUSES = frozenset(("NEW",))
TRIGGERED_ALGO_STATUSES = frozenset(("TRIGGERING", "TRIGGERED", "FINISHED"))
TERMINAL_ALGO_PHASES = {
    "CANCELED": "CANCELED",
    "REJECTED": "FAILED",
    "EXPIRED": "EXPIRED",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _decimal(value: Any, name: str) -> Decimal:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("%s 必须是有限十进制数" % name) from exc
    if not parsed.is_finite():
        raise ValueError("%s 必须是有限十进制数" % name)
    return parsed


def _field(row: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return default


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().casefold() == "true"


def _price_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _request_for_spec(spec: ProtectionLegSpec) -> Dict[str, Any]:
    return {
        "algoType": "CONDITIONAL",
        "symbol": spec.symbol,
        "side": spec.side,
        "positionSide": "BOTH",
        "type": spec.order_type,
        "triggerPrice": _price_text(spec.trigger_price),
        "workingType": "MARK_PRICE",
        "closePosition": True,
        "priceProtect": False,
        "clientAlgoId": spec.client_algo_id,
        "newOrderRespType": "RESULT",
    }


def _fingerprint(params: Mapping[str, Any]) -> str:
    canonical = json.dumps(dict(params), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_protection_legs(plan: PositionPlan) -> Tuple[ProtectionLegSpec, ...]:
    """Derive two stable close-all identities from an immutable filled entry."""

    if not isinstance(plan, PositionPlan):
        raise TypeError("托管保护只接受 PositionPlan")
    result = []
    for kind, order_type, trigger, prefix in (
        ("STOP", "STOP_MARKET", plan.stop_price, "ps"),
        ("TARGET", "TAKE_PROFIT_MARKET", plan.target_price, "pt"),
    ):
        client_id = normalize_client_order_id(
            "%s-%s" % (prefix, plan.entry_order_id), prefix=prefix
        )
        provisional = ProtectionLegSpec(
            entry_order_id=plan.entry_order_id,
            symbol=plan.symbol,
            kind=kind,
            client_algo_id=client_id,
            order_type=order_type,
            trigger_price=trigger,
            side=plan.direction.exit_side,
            request_fingerprint="pending",
        )
        params = _request_for_spec(provisional)
        result.append(
            ProtectionLegSpec(
                entry_order_id=plan.entry_order_id,
                symbol=plan.symbol,
                kind=kind,
                client_algo_id=client_id,
                order_type=order_type,
                trigger_price=trigger,
                side=plan.direction.exit_side,
                request_fingerprint=_fingerprint(params),
            )
        )
    return tuple(result)


@dataclass(frozen=True)
class ProtectionSyncResult:
    reason: str
    armed_symbols: Tuple[str, ...]
    open_algo_count: int


class ProtectionCoordinator:
    """Crash-safe OCO-like coordinator for Binance's two independent Algo orders."""

    def __init__(
        self,
        client: Any,
        state_store: RuntimeStateStore,
        clock: Any = _utc_now,
    ) -> None:
        required = (
            "new_algo_order",
            "query_algo_order",
            "cancel_algo_order",
            "open_algo_orders",
        )
        missing = [name for name in required if not callable(getattr(client, name, None))]
        if missing:
            raise TypeError("Demo client 缺少 Algo 能力: %s" % ",".join(missing))
        self.client = client
        self.store = state_store
        self.clock = clock

    @staticmethod
    def _payload_identity(payload: Mapping[str, Any]) -> str:
        return str(_field(payload, "clientAlgoId", "client_algo_id", default="") or "")

    @staticmethod
    def _validate_payload(leg: ProtectionLegRecord, payload: Mapping[str, Any]) -> None:
        if not isinstance(payload, Mapping):
            raise ValueError("Algo order 事实必须是 object")
        spec = leg.spec
        actual = {
            "client_id": ProtectionCoordinator._payload_identity(payload),
            "symbol": str(_field(payload, "symbol", default="")).upper(),
            "side": str(_field(payload, "side", default="")).upper(),
            "order_type": str(_field(payload, "orderType", "type", default="")).upper(),
            "position_side": str(_field(payload, "positionSide", default="")).upper(),
            "working_type": str(_field(payload, "workingType", default="")).upper(),
        }
        expected = {
            "client_id": spec.client_algo_id,
            "symbol": spec.symbol,
            "side": spec.side,
            "order_type": spec.order_type,
            "position_side": "BOTH",
            "working_type": "MARK_PRICE",
        }
        if actual != expected:
            raise ValueError("Algo order 身份/方向/触发类型与保护计划不一致")
        trigger = _decimal(_field(payload, "triggerPrice", default="0"), "triggerPrice")
        if trigger != spec.trigger_price:
            raise ValueError("Algo order 触发价与冻结计划不一致")
        if not _bool(_field(payload, "closePosition", default=False)):
            raise ValueError("Algo order 必须为 closePosition")
        if _bool(_field(payload, "reduceOnly", default=False)):
            raise ValueError("closePosition Algo 不得同时声明 reduceOnly")
        algo_type = _field(payload, "algoType", default="CONDITIONAL")
        if str(algo_type).upper() != "CONDITIONAL":
            raise ValueError("Algo order 必须属于 CONDITIONAL service")

    def _set_state(self, entry_order_id: str, target: str) -> None:
        record = self.store.get_protection_set(entry_order_id)
        if record is None or record.state == target:
            return
        self.store.transition_protection_set(
            entry_order_id,
            record.state,
            target,
            record.revision,
            self.clock(),
        )

    def _transition_from_payload(
        self, leg: ProtectionLegRecord, payload: Mapping[str, Any]
    ) -> ProtectionLegRecord:
        self._validate_payload(leg, payload)
        status = str(_field(payload, "algoStatus", "status", default="")).upper()
        if status in OPEN_ALGO_STATUSES:
            target = "OPEN"
        elif status in TRIGGERED_ALGO_STATUSES:
            target = "TRIGGERED"
        elif status in TERMINAL_ALGO_PHASES:
            target = TERMINAL_ALGO_PHASES[status]
        else:
            raise ValueError("未知 Algo status: %s" % (status or "MISSING"))
        algo_id = _field(payload, "algoId", "algo_id")
        actual_order_id = _field(payload, "actualOrderId", "actual_order_id")
        if actual_order_id in (None, "", 0, "0", -1, "-1"):
            actual_order_id = None
        actual_quantity_raw = _field(
            payload, "actualQty", "actual_quantity", default=None
        )
        actual_price_raw = _field(
            payload, "actualPrice", "averagePrice", default=None
        )
        actual_quantity = None
        actual_price = None
        if actual_quantity_raw is not None:
            actual_quantity = _decimal(actual_quantity_raw, "actualQty")
            if actual_quantity < ZERO:
                raise ValueError("Algo actualQty 不得为负")
            if actual_quantity > ZERO:
                if target == "OPEN":
                    raise ValueError("未触发 Algo order 不得携带成交数量")
                actual_price = _decimal(actual_price_raw, "actualPrice")
                if actual_price <= ZERO or actual_order_id is None:
                    raise ValueError("Algo 成交事实缺少 actualPrice/actualOrderId")
        observed_at = self.clock()
        changed = self.store.transition_protection_leg(
            leg.spec.entry_order_id,
            leg.spec.kind,
            leg.phase,
            target,
            observed_at,
            algo_id=None if algo_id is None else str(algo_id),
            algo_status=status,
            actual_order_id=None if actual_order_id is None else str(actual_order_id),
            cumulative_filled_quantity=actual_quantity,
            average_price=actual_price,
            last_error="",
        )
        current = self.store.get_protection_leg(leg.spec.entry_order_id, leg.spec.kind)
        if current is None:
            raise RuntimeError("保护腿在状态跃迁后消失")
        if not changed and current.phase == target:
            changed = self.store.transition_protection_leg(
                leg.spec.entry_order_id,
                leg.spec.kind,
                target,
                target,
                observed_at,
                algo_id=None if algo_id is None else str(algo_id),
                algo_status=status,
                actual_order_id=None if actual_order_id is None else str(actual_order_id),
                cumulative_filled_quantity=actual_quantity,
                average_price=actual_price,
                last_error="",
            )
            current = self.store.get_protection_leg(
                leg.spec.entry_order_id, leg.spec.kind
            )
            if current is None:
                raise RuntimeError("保护腿在并发状态收敛后消失")
        if not changed:
            raise RuntimeError("保护腿并发状态冲突")
        return current

    def _record_error(self, leg: ProtectionLegRecord, target: str, error: BaseException) -> None:
        self.store.transition_protection_leg(
            leg.spec.entry_order_id,
            leg.spec.kind,
            leg.phase,
            target,
            self.clock(),
            last_error="%s:%s" % (type(error).__name__, str(error)),
        )

    def _transition_from_cancel_ack(
        self, leg: ProtectionLegRecord, payload: Mapping[str, Any]
    ) -> ProtectionLegRecord:
        """Apply Binance's compact DELETE acknowledgement without inventing order fields."""

        if not isinstance(payload, Mapping):
            raise ValueError("Algo cancel acknowledgement 必须是 object")
        if str(_field(payload, "code", default="")) != "200":
            raise ValueError("Algo cancel acknowledgement 未明确成功")
        if self._payload_identity(payload) != leg.spec.client_algo_id:
            raise ValueError("Algo cancel acknowledgement 身份不一致")
        algo_id = _field(payload, "algoId", "algo_id")
        if algo_id in (None, "", 0, "0", -1, "-1"):
            raise ValueError("Algo cancel acknowledgement 缺少 algoId")
        changed = self.store.transition_protection_leg(
            leg.spec.entry_order_id,
            leg.spec.kind,
            leg.phase,
            "CANCELED",
            self.clock(),
            algo_id=str(algo_id),
            algo_status="CANCELED",
            last_error="",
        )
        current = self.store.get_protection_leg(
            leg.spec.entry_order_id, leg.spec.kind
        )
        if current is None:
            raise RuntimeError("保护腿在撤销确认后消失")
        if not changed and current.phase != "CANCELED":
            raise RuntimeError("保护腿撤销确认并发冲突")
        return current

    def _query_leg(self, leg: ProtectionLegRecord) -> ProtectionLegRecord:
        try:
            payload = self.client.query_algo_order(client_algo_id=leg.spec.client_algo_id)
        except Exception as exc:
            self._record_error(leg, leg.phase, exc)
            current = self.store.get_protection_leg(leg.spec.entry_order_id, leg.spec.kind)
            return current or leg
        return self._transition_from_payload(leg, payload)

    def _submit_leg(self, leg: ProtectionLegRecord) -> ProtectionLegRecord:
        if leg.phase != "PREPARED":
            raise ValueError("只有 PREPARED 保护腿可首次提交")
        if _fingerprint(_request_for_spec(leg.spec)) != leg.spec.request_fingerprint:
            raise ValueError("保护腿请求指纹不匹配")
        if not self.store.transition_protection_leg(
            leg.spec.entry_order_id,
            leg.spec.kind,
            "PREPARED",
            "SUBMIT_UNKNOWN",
            self.clock(),
        ):
            raise RuntimeError("保护腿首次提交 CAS 失败")
        current = self.store.get_protection_leg(leg.spec.entry_order_id, leg.spec.kind)
        if current is None:
            raise RuntimeError("保护腿在派发屏障后消失")
        try:
            payload = self.client.new_algo_order(_request_for_spec(current.spec))
        except BinanceSubmissionUnknown:
            return self._query_leg(current)
        except (BinanceTransportError, TimeoutError):
            return self._query_leg(current)
        except BinanceRateLimitError as exc:
            # 418/429 明确未进入撮合；保留相同 clientAlgoId 回到可重试前态。
            self._record_error(current, "PREPARED", exc)
            return self.store.get_protection_leg(
                current.spec.entry_order_id, current.spec.kind
            ) or current
        except BinanceApiError as exc:
            self._record_error(current, "FAILED", exc)
            return self.store.get_protection_leg(
                current.spec.entry_order_id, current.spec.kind
            ) or current
        return self._transition_from_payload(current, payload)

    def _arm(self, plan: PositionPlan, refreshed_ids: Set[str]) -> str:
        legs = build_protection_legs(plan)
        bundle = self.store.ensure_protection_bundle(plan, legs)
        if bundle.winner_kind is not None:
            return "protection_exit_pending:%s" % plan.symbol
        if bundle.state in ("PREPARED", "UNKNOWN"):
            self._set_state(plan.entry_order_id, "ARMING")
        for kind in ("STOP", "TARGET"):
            leg = self.store.get_protection_leg(plan.entry_order_id, kind)
            if leg is None:
                return "protection_leg_missing:%s:%s" % (plan.symbol, kind)
            if leg.phase == "PREPARED":
                leg = self._submit_leg(leg)
            elif (
                leg.phase == "SUBMIT_UNKNOWN"
                and leg.spec.client_algo_id not in refreshed_ids
            ):
                leg = self._query_leg(leg)
            if leg.phase in ("TRIGGERED", "FILLED"):
                self._claim_and_cancel(plan.entry_order_id, kind)
                return "protection_exit_pending:%s:%s" % (plan.symbol, kind)
            if leg.phase != "OPEN":
                self._set_state(plan.entry_order_id, "UNKNOWN")
                return "protection_not_armed:%s:%s:%s" % (
                    plan.symbol,
                    kind,
                    leg.phase,
                )
        self._set_state(plan.entry_order_id, "ARMED")
        return ""

    def _cancel_leg(self, leg: ProtectionLegRecord, already_observed_open: bool) -> None:
        current = leg
        if not already_observed_open:
            current = self._query_leg(leg)
        if current.phase == "OPEN":
            if not self.store.transition_protection_leg(
                current.spec.entry_order_id,
                current.spec.kind,
                "OPEN",
                "CANCEL_UNKNOWN",
                self.clock(),
            ):
                return
            current = self.store.get_protection_leg(
                current.spec.entry_order_id, current.spec.kind
            ) or current
        if current.phase != "CANCEL_UNKNOWN":
            return
        try:
            payload = self.client.cancel_algo_order(
                client_algo_id=current.spec.client_algo_id
            )
        except Exception as exc:
            self._record_error(current, "CANCEL_UNKNOWN", exc)
            return
        try:
            self._transition_from_cancel_ack(current, payload)
        except (ValueError, RuntimeError) as exc:
            self._record_error(current, "CANCEL_UNKNOWN", exc)

    def _claim_and_cancel(self, entry_order_id: str, winner: str) -> None:
        claimed = self.store.claim_protection_winner(
            entry_order_id, winner, self.clock()
        )
        if not claimed:
            return
        for leg in self.store.protection_legs(entry_order_id):
            if leg.phase == "CANCEL_UNKNOWN":
                # CAS 刚刚发生且尚未越过 DELETE 网络边界，可以直接首次撤销。
                self._cancel_leg(leg, already_observed_open=True)

    def _reconcile_winner_position(
        self, plan: PositionPlan, signed_amount: Decimal
    ) -> str:
        """Only shrink a managed plan to authoritative positionRisk after a native win."""

        bundle = self.store.get_protection_set(plan.entry_order_id)
        if bundle is None or bundle.winner_kind is None:
            return ""
        if (signed_amount > ZERO) != (plan.direction.value == "LONG"):
            return "protection_position_direction_mismatch:%s" % plan.symbol
        remaining = abs(signed_amount)
        if remaining > plan.quantity:
            return "protection_position_quantity_increase:%s" % plan.symbol
        winner = (
            self.store.get_protection_leg(plan.entry_order_id, bundle.winner_kind)
            if bundle.winner_kind in ("STOP", "TARGET")
            else next(
                (
                    leg
                    for leg in self.store.protection_legs(plan.entry_order_id)
                    if leg.cumulative_filled_quantity > ZERO
                ),
                None,
            )
        )
        if winner is None and remaining == plan.quantity:
            return ""
        if winner is None:
            return "protection_fill_evidence_missing:%s" % plan.symbol
        if winner.cumulative_filled_quantity > ZERO:
            entry = self.store.get_order(plan.entry_order_id)
            if entry is None or entry.result is None:
                return "protection_fill_evidence_missing:%s" % plan.symbol
            if winner.cumulative_filled_quantity > entry.result.executed_quantity:
                return "protection_fill_exceeds_entry:%s" % plan.symbol
        if remaining < plan.quantity:
            updated = self.store.shrink_plan_quantity(plan, remaining, self.clock())
            if updated is None:
                return "protection_plan_closed_concurrently:%s" % plan.symbol
        return ""

    def claim_local_exit(self, plan: PositionPlan) -> None:
        """Freeze LOCAL as winner before the reduce-only fallback crosses the wire."""

        bundle = self.store.get_protection_set(plan.entry_order_id)
        if bundle is None:
            return
        # 这里只做本地 CAS，并由随后账户轮询查询后撤销两腿。任何 DELETE
        # 网络等待都不能挡在本地 reduce-only MARKET 退出之前。
        self.store.claim_protection_winner(
            plan.entry_order_id, "LOCAL", self.clock()
        )

    def snapshot(self, symbol: str) -> Mapping[str, Any]:
        """Expose non-secret protection lifecycle facts for the read model."""

        normalized = str(symbol).upper()
        bundle = next(
            (item for item in self.store.protection_sets() if item.symbol == normalized),
            None,
        )
        if bundle is None:
            return {"state": "UNARMED", "winner": None, "legs": []}
        legs = self.store.protection_legs(bundle.entry_order_id)
        return {
            "state": bundle.state,
            "winner": bundle.winner_kind,
            "legs": [
                {
                    "kind": leg.spec.kind,
                    "phase": leg.phase,
                    "trigger_price": leg.spec.trigger_price,
                    "algo_status": leg.algo_status,
                }
                for leg in legs
            ],
        }

    def _resume_cancellations(
        self,
        entry_order_id: str,
        observed_open_ids: Set[str],
    ) -> None:
        for leg in self.store.protection_legs(entry_order_id):
            if leg.phase != "CANCEL_UNKNOWN":
                continue
            self._cancel_leg(
                leg,
                already_observed_open=leg.spec.client_algo_id in observed_open_ids,
            )

    def _close_flat_bundle(self, entry_order_id: str, symbol: str) -> bool:
        bundle = self.store.get_protection_set(entry_order_id)
        if bundle is None:
            return True
        if bundle.winner_kind is None:
            self._claim_and_cancel(entry_order_id, "LOCAL")
            bundle = self.store.get_protection_set(entry_order_id) or bundle
        legs = self.store.protection_legs(entry_order_id)
        for leg in legs:
            if leg.phase == "TRIGGERED":
                self.store.transition_protection_leg(
                    entry_order_id,
                    leg.spec.kind,
                    "TRIGGERED",
                    "FILLED",
                    self.clock(),
                    algo_status=leg.algo_status or "FINISHED",
                )
        legs = self.store.protection_legs(entry_order_id)
        if any(leg.phase not in PROTECTION_TERMINAL_PHASES for leg in legs):
            return False
        bundle = self.store.get_protection_set(entry_order_id)
        if bundle is None:
            return True
        if bundle.state != "CLOSED":
            self._set_state(entry_order_id, "CLOSED")
            bundle = self.store.get_protection_set(entry_order_id) or bundle
        if not self.store.delete_protection_bundle(entry_order_id, bundle.revision):
            return False
        self.store.delete_plan(symbol, entry_order_id)
        return True

    def synchronize(
        self,
        *,
        plans: Mapping[str, PositionPlan],
        position_amounts: Mapping[str, Decimal],
        managed_symbols: Set[str],
        open_algo_rows: Sequence[Mapping[str, Any]],
        pending_symbols: Optional[Set[str]] = None,
        normal_orders_clear: bool = True,
    ) -> ProtectionSyncResult:
        """Reconcile all account Algo facts, then arm only already-managed positions."""

        pending = set(pending_symbols or ())
        stored = {leg.spec.client_algo_id: leg for leg in self.store.protection_legs()}
        observed: Dict[str, Mapping[str, Any]] = {}
        first_reason = ""
        for row in open_algo_rows:
            if not isinstance(row, Mapping):
                return ProtectionSyncResult("invalid_open_algo_row", (), len(open_algo_rows))
            client_id = self._payload_identity(row)
            if client_id in observed:
                first_reason = first_reason or "duplicate_open_algo_order:%s" % (
                    client_id or "UNKNOWN"
                )
                continue
            leg = stored.get(client_id)
            if leg is None:
                first_reason = first_reason or "unmanaged_algo_order:%s" % (
                    client_id or "UNKNOWN"
                )
                continue
            if leg.phase in PROTECTION_TERMINAL_PHASES:
                first_reason = first_reason or "terminal_algo_still_open:%s" % client_id
                continue
            try:
                self._validate_payload(leg, row)
                if leg.phase == "CANCEL_UNKNOWN":
                    # openAlgoOrders 已证明撤销尚未生效；保留撤销屏障，下面
                    # 才能安全重发幂等 DELETE，不能把它降回普通 OPEN。
                    pass
                elif leg.phase != "PREPARED":
                    leg = self._transition_from_payload(leg, row)
                    stored[client_id] = leg
                else:
                    first_reason = first_reason or "algo_phase_conflict:%s" % client_id
            except (ValueError, RuntimeError):
                first_reason = first_reason or "algo_order_mismatch:%s" % client_id
            observed[client_id] = row

        observed_ids = set(observed)
        refreshed_ids: Set[str] = set()
        for leg in tuple(stored.values()):
            if (
                leg.phase in ("SUBMIT_UNKNOWN", "OPEN", "TRIGGERED")
                and leg.spec.client_algo_id not in observed_ids
            ):
                refreshed = self._query_leg(leg)
                stored[leg.spec.client_algo_id] = refreshed
                refreshed_ids.add(leg.spec.client_algo_id)
                if refreshed.last_error:
                    first_reason = first_reason or "protection_query_unknown:%s:%s" % (
                        leg.spec.symbol,
                        leg.spec.kind,
                    )

        for bundle in self.store.protection_sets():
            triggered = [
                leg
                for leg in self.store.protection_legs(bundle.entry_order_id)
                if leg.phase in ("TRIGGERED", "FILLED")
                or leg.cumulative_filled_quantity > ZERO
            ]
            if triggered and bundle.winner_kind is None:
                triggered.sort(key=lambda item: (item.updated_at, item.spec.kind))
                self._claim_and_cancel(bundle.entry_order_id, triggered[0].spec.kind)
                bundle = self.store.get_protection_set(bundle.entry_order_id) or bundle
            amount = position_amounts.get(bundle.symbol, ZERO)
            if bundle.winner_kind != "LOCAL" or amount == ZERO:
                self._resume_cancellations(bundle.entry_order_id, observed_ids)
            else:
                # 旧版本可能已经把 LOCAL 两腿推进 CANCEL_UNKNOWN。只查询恢复
                # 事实，仓位未归零前绝不能跨 DELETE 网络边界。
                for leg in self.store.protection_legs(bundle.entry_order_id):
                    if leg.phase == "CANCEL_UNKNOWN":
                        self._query_leg(leg)

        armed = []
        for symbol, plan in plans.items():
            amount = position_amounts.get(symbol, ZERO)
            if amount == ZERO:
                if symbol in pending:
                    first_reason = first_reason or "order_pending:%s" % symbol
                    continue
                if not normal_orders_clear:
                    first_reason = first_reason or "hosted_exit_order_pending:%s" % symbol
                    continue
                bundle = self.store.get_protection_set(plan.entry_order_id)
                if bundle is not None:
                    if bundle.winner_kind == "LOCAL":
                        self.store.begin_local_protection_cancellation(
                            plan.entry_order_id, self.clock()
                        )
                    self._resume_cancellations(plan.entry_order_id, observed_ids)
                    if not self._close_flat_bundle(plan.entry_order_id, symbol):
                        first_reason = first_reason or "protection_cancel_pending:%s" % symbol
                continue
            bundle = self.store.get_protection_set(plan.entry_order_id)
            if bundle is not None and bundle.winner_kind is not None:
                winner_reason = self._reconcile_winner_position(plan, amount)
                if winner_reason:
                    first_reason = first_reason or winner_reason
                else:
                    first_reason = first_reason or "protection_exit_pending:%s" % symbol
                continue
            if symbol not in managed_symbols:
                first_reason = first_reason or "protection_position_unreconciled:%s" % symbol
                continue
            reason = self._arm(plan, refreshed_ids)
            if reason:
                first_reason = first_reason or reason
            else:
                armed.append(symbol)

        plan_entries = {plan.entry_order_id for plan in plans.values()}
        for bundle in self.store.protection_sets():
            if bundle.entry_order_id in plan_entries:
                continue
            if position_amounts.get(bundle.symbol, ZERO) != ZERO:
                first_reason = first_reason or "orphan_protection_bundle:%s" % bundle.symbol
                continue
            if not normal_orders_clear:
                first_reason = first_reason or "hosted_exit_order_pending:%s" % bundle.symbol
                continue
            if bundle.winner_kind == "LOCAL":
                self.store.begin_local_protection_cancellation(
                    bundle.entry_order_id, self.clock()
                )
            self._resume_cancellations(bundle.entry_order_id, observed_ids)
            if not self._close_flat_bundle(bundle.entry_order_id, bundle.symbol):
                first_reason = first_reason or "protection_cancel_pending:%s" % bundle.symbol

        return ProtectionSyncResult(
            reason=first_reason,
            armed_symbols=tuple(sorted(armed)),
            open_algo_count=len(open_algo_rows),
        )


__all__ = [
    "ProtectionCoordinator",
    "ProtectionSyncResult",
    "build_protection_legs",
]
