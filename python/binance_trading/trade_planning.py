"""
[INPUT]: 依赖已验证 OrderIntent/ExecutionResult、PositionPlan、SymbolRules 与成交后冻结的风险几何参数
[OUTPUT]: 提供 protection_prices、build_filled_position_plan 与 build_exit_intent，统一保护几何并生成带 durable attempt 的退出意图
[POS]: binance_trading 的交易计划构造层；隔离 Decimal 几何与身份派生，使 runtime 只编排持久化和网络副作用
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from .broker import normalize_client_order_id
from .models import Direction, ExecutionResult, OrderIntent, SymbolRules, decimal_value
from .state import PositionPlan


def protection_prices(
    entry_price: Decimal,
    direction: Direction,
    distance: Decimal,
    reward_risk_ratio: Decimal,
    rules: SymbolRules,
) -> tuple[Decimal, Decimal]:
    entry = decimal_value(entry_price, "entry_price")
    risk_distance = decimal_value(distance, "risk_distance")
    ratio = decimal_value(reward_risk_ratio, "reward_risk_ratio")
    parsed_direction = Direction(direction)
    if min(entry, risk_distance, ratio) <= 0:
        raise ValueError("保护几何参数必须为正数")
    if parsed_direction is Direction.LONG:
        stop_raw, target_raw = entry - risk_distance, entry + risk_distance * ratio
    else:
        stop_raw, target_raw = entry + risk_distance, entry - risk_distance * ratio
    stop = rules.price_for_side(stop_raw, parsed_direction.exit_side)
    target = rules.price_for_side(target_raw, parsed_direction.exit_side)
    valid = (
        parsed_direction is Direction.LONG and 0 < stop < entry < target
    ) or (
        parsed_direction is Direction.SHORT and 0 < target < entry < stop
    )
    if not valid:
        raise ValueError("保护价格必须为正且符合方向几何")
    return stop, target


def build_filled_position_plan(
    *,
    intent: OrderIntent,
    result: ExecutionResult,
    distance: Decimal,
    reward_risk_ratio: Decimal,
    rules: SymbolRules,
) -> PositionPlan:
    stop_price, target_price = protection_prices(
        result.average_price, intent.direction, distance, reward_risk_ratio, rules
    )
    return PositionPlan(
        symbol=intent.symbol,
        direction=intent.direction,
        quantity=result.executed_quantity,
        entry_price=result.average_price,
        stop_price=stop_price,
        target_price=target_price,
        signal_id=intent.signal_id,
        entry_order_id=result.client_order_id,
        opened_at=result.observed_at,
        updated_at=result.observed_at,
    )


def build_exit_intent(
    plan: PositionPlan,
    reason: str,
    now: datetime,
    attempt: int,
) -> OrderIntent:
    if int(attempt) <= 0:
        raise ValueError("exit attempt 必须为正整数")
    client_id = normalize_client_order_id(
        "x-%s-%d-%s-a%d" % (
            plan.symbol,
            int(now.timestamp() * 1_000_000),
            str(reason or "EXIT")[0],
            int(attempt),
        )
    )
    return OrderIntent(
        client_order_id=client_id,
        symbol=plan.symbol,
        direction=plan.direction,
        side=plan.direction.exit_side,
        quantity=plan.quantity,
        order_type="MARKET",
        limit_price=None,
        reduce_only=True,
        created_at=now,
        signal_id=plan.signal_id,
        reason=reason,
    )


__all__ = ["build_exit_intent", "build_filled_position_plan", "protection_prices"]
