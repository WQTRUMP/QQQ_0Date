"""
[INPUT]: 依赖 paper 账本或 Binance account/positionRisk/openOrders 原始事实、保护计划、配置与 UTC 观察时钟
[OUTPUT]: 提供 ReconciledAccount、reconcile_paper 与 reconcile_testnet，将账户/持仓事实同保护计划严格对齐
[POS]: binance_trading 的只读账户防腐层；不访问网络、不写数据库，只把活动委托、裸仓、跨 symbol、cross margin 或超杠杆收敛为阻断原因
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Sequence, Set

from .config import BinanceConfig
from .ledger import LedgerPosition
from .models import AccountSnapshot, Direction, Position
from .state import PositionPlan


ZERO = Decimal("0")


def _decimal(value: Any, name: str) -> Decimal:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("%s 必须是有限十进制数" % name) from exc
    if not parsed.is_finite():
        raise ValueError("%s 必须是有限十进制数" % name)
    return parsed


def _field(value: Mapping[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in value and value[name] is not None:
            return value[name]
    return default


@dataclass(frozen=True)
class ReconciledAccount:
    account: AccountSnapshot
    positions: Dict[str, Position]
    reason: str


def reconcile_paper(
    *,
    config: BinanceConfig,
    observed_at: datetime,
    ledger_snapshot: Mapping[str, Any],
    ledger_positions: Mapping[str, LedgerPosition],
    plans: Mapping[str, PositionPlan],
    mark_prices: Mapping[str, Decimal],
    pending_symbols: Set[str],
) -> ReconciledAccount:
    positions: Dict[str, Position] = {}
    reason = ""
    for symbol, item in ledger_positions.items():
        plan = plans.get(symbol)
        direction = Direction.LONG if item.quantity > ZERO else Direction.SHORT
        if plan is None:
            reason = (
                "order_pending:%s" % symbol
                if symbol in pending_symbols
                else "unmanaged_position:%s" % symbol
            )
            continue
        if plan.direction is not direction or plan.quantity != abs(item.quantity):
            reason = "position_plan_mismatch:%s" % symbol
            continue
        mark_price = mark_prices.get(symbol, item.entry_price)
        positions[symbol] = Position(
            symbol=symbol,
            direction=direction,
            quantity=abs(item.quantity),
            entry_price=item.entry_price,
            mark_price=mark_price,
            stop_price=plan.stop_price,
            target_price=plan.target_price,
            unrealized_pnl=item.unrealized_pnl(mark_price),
            opened_at=plan.opened_at,
            signal_id=plan.signal_id,
            leverage=config.max_leverage,
            margin_type="ISOLATED",
        )
    orphan = sorted(set(plans) - set(ledger_positions))
    if orphan and not reason:
        reason = "orphan_position_plan:%s" % orphan[0]
    account = AccountSnapshot(
        wallet_balance=ledger_snapshot["wallet_balance"],
        equity=ledger_snapshot["equity"],
        available_balance=ledger_snapshot["wallet_balance"],
        unrealized_pnl=ledger_snapshot["unrealized_pnl"],
        day_start_equity=ledger_snapshot["day_start_equity"],
        observed_at=observed_at,
    )
    return ReconciledAccount(account=account, positions=positions, reason=reason)


def reconcile_testnet(
    *,
    config: BinanceConfig,
    observed_at: datetime,
    account_row: Mapping[str, Any],
    position_rows: Sequence[Mapping[str, Any]],
    open_order_rows: Sequence[Mapping[str, Any]],
    plans: Mapping[str, PositionPlan],
    pending_symbols: Set[str],
    day_start_equity: Decimal,
) -> ReconciledAccount:
    wallet = _decimal(_field(account_row, "totalWalletBalance"), "wallet balance")
    unrealized = _decimal(
        _field(account_row, "totalUnrealizedProfit", default="0"),
        "unrealized pnl",
    )
    equity = _decimal(
        _field(account_row, "totalMarginBalance", default=wallet + unrealized),
        "margin balance",
    )
    available = _decimal(_field(account_row, "availableBalance"), "available balance")
    positions: Dict[str, Position] = {}
    seen: Set[str] = set()
    reason = ""
    # 本系统只发送 IOC LIMIT 与同步 RESULT MARKET，稳态不应留下活动委托。
    # 因此任何 openOrders 事实（含其他 symbol 的人工/遗留单）都必须先人工清理。
    for row in open_order_rows:
        if not isinstance(row, Mapping):
            raise ValueError("openOrders row 必须是 object")
        symbol = str(_field(row, "symbol", default="UNKNOWN")).upper() or "UNKNOWN"
        identity = str(
            _field(row, "clientOrderId", "origClientOrderId", "orderId", default="UNKNOWN")
        )
        reason = "active_open_order:%s:%s" % (symbol, identity or "UNKNOWN")
        break
    for row in position_rows:
        if not isinstance(row, Mapping):
            raise ValueError("positionRisk row 必须是 object")
        symbol = str(_field(row, "symbol", default="")).upper()
        if str(_field(row, "positionSide", default="BOTH")).upper() != "BOTH":
            reason = "hedge_position_row:%s" % (symbol or "UNKNOWN")
            continue
        quantity_signed = _decimal(_field(row, "positionAmt", default="0"), "position amount")
        if quantity_signed == ZERO:
            continue
        if symbol not in config.symbols:
            reason = "unmanaged_symbol_position:%s" % symbol
            continue
        seen.add(symbol)
        plan = plans.get(symbol)
        if plan is None:
            reason = (
                "order_pending:%s" % symbol
                if symbol in pending_symbols
                else "unmanaged_position:%s" % symbol
            )
            continue
        direction = Direction.LONG if quantity_signed > ZERO else Direction.SHORT
        quantity = abs(quantity_signed)
        if plan.direction is not direction or plan.quantity != quantity:
            reason = "position_plan_mismatch:%s" % symbol
            continue
        margin_type = str(_field(row, "marginType", default="")).upper()
        leverage = int(_field(row, "leverage", default=0))
        if margin_type != "ISOLATED":
            reason = "cross_margin_position:%s" % symbol
            continue
        if leverage <= 0 or leverage > config.max_leverage:
            reason = "leverage_limit:%s" % symbol
            continue
        entry_price = _decimal(_field(row, "entryPrice"), "entry price")
        mark_price = _decimal(_field(row, "markPrice"), "mark price")
        liquidation = _decimal(
            _field(row, "liquidationPrice", default="0"),
            "liquidation price",
        )
        positions[symbol] = Position(
            symbol=symbol,
            direction=direction,
            quantity=quantity,
            entry_price=entry_price,
            mark_price=mark_price,
            stop_price=plan.stop_price,
            target_price=plan.target_price,
            unrealized_pnl=_decimal(
                _field(row, "unRealizedProfit", "unrealizedProfit", default="0"),
                "position unrealized pnl",
            ),
            opened_at=plan.opened_at,
            signal_id=plan.signal_id,
            leverage=leverage,
            margin_type=margin_type,
            liquidation_price=liquidation if liquidation > ZERO else None,
        )
    orphan = sorted(set(plans) - seen)
    if orphan and not reason:
        reason = "orphan_position_plan:%s" % orphan[0]
    account = AccountSnapshot(
        wallet_balance=wallet,
        equity=equity,
        available_balance=available,
        unrealized_pnl=unrealized,
        day_start_equity=day_start_equity,
        observed_at=observed_at,
    )
    return ReconciledAccount(account=account, positions=positions, reason=reason)


__all__ = ["ReconciledAccount", "reconcile_paper", "reconcile_testnet"]
