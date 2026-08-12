"""
[INPUT]: 依赖 runtime 已锁存的配置、账户、行情、持仓、托管保护、策略快照、账本与事件事实
[OUTPUT]: 提供 readiness_reason 与 build_runtime_snapshot，生成失败关闭且可 JSON 化的只读运行状态
[POS]: binance_trading 的运行状态投影层；不触碰网络/数据库，也不把 Dashboard 展示需求反向渗入交易编排
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping, Optional, Sequence

from .config import BinanceConfig
from .models import AccountSnapshot, BookTicker, MarkPrice, Position, jsonable


ZERO = Decimal("0")


def readiness_reason(
    *,
    config: BinanceConfig,
    now: datetime,
    started: bool,
    sticky_reason: str,
    bootstrapped: bool,
    bootstrap_reason: str,
    reconcile_reason: str,
    account: Optional[AccountSnapshot],
    strategies: Mapping[str, Any],
    books: Mapping[str, BookTicker],
    marks: Mapping[str, MarkPrice],
) -> str:
    if not started:
        return "runtime_not_started"
    if sticky_reason:
        return sticky_reason
    if not bootstrapped:
        return bootstrap_reason or "bootstrap_pending"
    if reconcile_reason:
        return reconcile_reason
    if account is None:
        return "account_not_hydrated"
    account_age = Decimal(str((now - account.observed_at).total_seconds()))
    if account_age < ZERO or account_age > config.account_poll_seconds * Decimal("3"):
        return "account_stale"
    for symbol in config.symbols:
        strategy = strategies.get(symbol)
        if strategy is None or not strategy.ready:
            return "strategy_not_hydrated:%s" % symbol
        book = books.get(symbol)
        if book is None:
            return "book_missing:%s" % symbol
        book_age = Decimal(str((now - book.event_time).total_seconds()))
        if book_age < Decimal("-1") or book_age > config.max_book_age_seconds:
            return "book_stale:%s" % symbol
        mark = marks.get(symbol)
        if mark is None:
            return "mark_missing:%s" % symbol
        mark_age = Decimal(str((now - mark.event_time).total_seconds()))
        if mark_age < Decimal("-1") or mark_age > config.max_book_age_seconds * Decimal("2"):
            return "mark_stale:%s" % symbol
    return ""


def build_runtime_snapshot(
    *,
    config: BinanceConfig,
    now: datetime,
    readiness: str,
    book: Optional[BookTicker],
    mark: Optional[MarkPrice],
    account: Optional[AccountSnapshot],
    positions: Sequence[Position],
    ledger: Mapping[str, Any],
    protection: Mapping[str, Any],
    strategies: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    risk_reason = readiness
    can_open = not readiness and config.trading_enabled and not positions
    if not risk_reason and not config.trading_enabled:
        risk_reason = "TRADING_DISABLED"
    elif not risk_reason and positions:
        risk_reason = "POSITION_LIMIT"
    if not risk_reason:
        risk_reason = "APPROVED"
    drawdown = account.day_drawdown_fraction if account is not None else None
    if drawdown is not None and drawdown >= config.max_daily_loss_fraction:
        can_open = False
        risk_reason = "DAILY_DRAWDOWN_LIMIT"
    account_view = {}
    if account is not None:
        account_view = {
            "wallet_balance": account.wallet_balance,
            "margin_balance": account.equity,
            "equity": account.equity,
            "available_balance": account.available_balance,
            "unrealized_pnl": account.unrealized_pnl,
            "day_start_equity": account.day_start_equity,
            "observed_at": account.observed_at,
        }
    position_view = []
    for item in positions:
        row = jsonable(item)
        row["side"] = item.direction.value
        position_view.append(row)
    return jsonable(
        {
            "ready": readiness == "",
            "mode": config.mode,
            "generated_at": now,
            "market": {
                "symbol": config.symbols[0],
                "contract_type": "PERPETUAL",
                "book": book,
                "mark": mark,
            },
            "account": account_view,
            "ledger": dict(ledger),
            "protection": dict(protection),
            "positions": position_view,
            "risk": {
                "state": "ready" if can_open else "blocked",
                "can_open": can_open,
                "reason": risk_reason,
                "daily_drawdown_pct": drawdown,
            },
            "strategies": dict(strategies),
            "events": list(events),
        }
    )


__all__ = ["build_runtime_snapshot", "readiness_reason"]
