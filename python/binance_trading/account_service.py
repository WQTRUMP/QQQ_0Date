"""
[INPUT]: 依赖 BinanceConfig、paper ledger、RuntimeStateStore 的未收敛订单、ProtectionCoordinator 与账户/持仓/活动委托 REST 事实
[OUTPUT]: 提供 AccountRefreshResult、refresh_paper_account 与 refresh_testnet_account，以持久未决屏障完成资金水合、部分成交重投影和先读后写判定
[POS]: binance_trading 的账户同步应用服务；runtime 只提交快照，本模块集中网络读取、领域对账和托管保护同步
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Mapping, Optional, Set, Tuple

from .config import BinanceConfig
from .ledger import PaperLedger
from .models import AccountSnapshot, MarkPrice, Position, decimal_value, utc_datetime
from .protection import ProtectionCoordinator
from .reconciliation import reconcile_paper, reconcile_testnet
from .state import PositionPlan, RuntimeStateStore


ZERO = Decimal("0")


@dataclass(frozen=True)
class AccountRefreshResult:
    plans: Dict[str, PositionPlan]
    positions: Dict[str, Position]
    account: Optional[AccountSnapshot]
    ledger_snapshot: Mapping[str, Any]
    reason: str
    safe_to_configure: bool = False
    protection_exit_symbols: Tuple[str, ...] = ()


def refresh_paper_account(
    *,
    config: BinanceConfig,
    ledger: PaperLedger,
    state_store: RuntimeStateStore,
    marks: Mapping[str, MarkPrice],
    pending_symbols: Set[str],
    now: Any,
) -> AccountRefreshResult:
    observed_at = utc_datetime(now, "now")
    ledger_positions = ledger.positions()
    plans = state_store.plans()
    missing_marks = sorted(set(ledger_positions) - set(marks))
    if missing_marks:
        return AccountRefreshResult(
            plans=plans,
            positions={},
            account=None,
            ledger_snapshot={},
            reason="paper_mark_missing:%s" % missing_marks[0],
        )
    mark_prices: Dict[str, Decimal] = {}
    for symbol in ledger_positions:
        mark = marks[symbol]
        age = Decimal(str((observed_at - mark.event_time).total_seconds()))
        if age < Decimal("-1") or age > config.max_book_age_seconds * Decimal("2"):
            return AccountRefreshResult(
                plans=plans,
                positions={},
                account=None,
                ledger_snapshot={},
                reason="paper_mark_stale:%s" % symbol,
            )
        mark_prices[symbol] = mark.mark_price
    ledger_snapshot = ledger.snapshot(mark_prices=mark_prices, now=observed_at)
    reconciled = reconcile_paper(
        config=config,
        observed_at=observed_at,
        ledger_snapshot=ledger_snapshot,
        ledger_positions=ledger_positions,
        plans=plans,
        mark_prices=mark_prices,
        pending_symbols=pending_symbols,
    )
    return AccountRefreshResult(
        plans=plans,
        positions=reconciled.positions,
        account=reconciled.account,
        ledger_snapshot=ledger_snapshot,
        reason=reconciled.reason,
    )


def _position_amounts(position_rows: Any) -> tuple:
    amounts: Dict[str, Decimal] = {}
    remote_exposure = False
    for row in position_rows:
        symbol = str(row.get("symbol", "")).upper()
        amount = decimal_value(row.get("positionAmt", "0"), "position amount")
        if amount != ZERO:
            remote_exposure = True
        if not symbol:
            continue
        if str(row.get("positionSide", "BOTH")).upper() != "BOTH" and amount != ZERO:
            amounts[symbol] = abs(amount)
        else:
            amounts[symbol] = amounts.get(symbol, ZERO) + amount
    return amounts, remote_exposure


async def refresh_testnet_account(
    *,
    config: BinanceConfig,
    client: Any,
    state_store: RuntimeStateStore,
    protection: ProtectionCoordinator,
    pending_symbols: Set[str],
    clock: Any,
) -> AccountRefreshResult:
    durable_pending = {
        record.intent.symbol for record in state_store.unresolved_orders()
    }
    effective_pending = set(pending_symbols) | durable_pending
    if await asyncio.to_thread(client.position_mode):
        raise RuntimeError("Binance 账户已切换为 Hedge 模式")
    account_row, position_rows, open_order_rows, open_algo_rows = await asyncio.gather(
        asyncio.to_thread(client.account),
        asyncio.to_thread(client.positions),
        asyncio.to_thread(client.open_orders),
        asyncio.to_thread(client.open_algo_orders),
    )
    now = clock()
    wallet = decimal_value(account_row.get("totalWalletBalance"), "wallet balance")
    unrealized = decimal_value(
        account_row.get("totalUnrealizedProfit", "0"), "unrealized pnl"
    )
    equity = decimal_value(
        account_row.get("totalMarginBalance", wallet + unrealized),
        "margin balance",
    )
    baseline = state_store.ensure_day_baseline(equity, now)
    plans = state_store.plans()
    position_amounts, remote_exposure = _position_amounts(position_rows)
    reconciled = reconcile_testnet(
        config=config,
        observed_at=now,
        account_row=account_row,
        position_rows=position_rows,
        open_order_rows=open_order_rows,
        plans=plans,
        pending_symbols=effective_pending,
        day_start_equity=baseline,
    )
    protection_result = await asyncio.to_thread(
        protection.synchronize,
        plans=plans,
        position_amounts=position_amounts,
        managed_symbols=set(reconciled.positions),
        open_algo_rows=open_algo_rows,
        pending_symbols=effective_pending,
        normal_orders_clear=not open_order_rows,
    )
    current_plans = state_store.plans()
    # Algo 部分成交会依据 positionRisk 只向下收敛计划数量。用同一批账户
    # 事实重新投影，避免把已经验证的新数量留在旧 mismatch 快照之外。
    reconciled = reconcile_testnet(
        config=config,
        observed_at=now,
        account_row=account_row,
        position_rows=position_rows,
        open_order_rows=open_order_rows,
        plans=current_plans,
        pending_symbols=effective_pending,
        day_start_equity=baseline,
    )
    safe_to_configure = (
        not reconciled.reason
        and not protection_result.reason
        and not remote_exposure
        and not open_order_rows
        and not open_algo_rows
        and not current_plans
        and not state_store.protection_sets()
        and not state_store.unresolved_orders()
    )
    protection_exit_symbols = tuple(
        sorted(
            bundle.symbol
            for bundle in state_store.protection_sets()
            if bundle.winner_kind is not None and bundle.symbol in reconciled.positions
        )
    )
    return AccountRefreshResult(
        plans=current_plans,
        positions=reconciled.positions,
        account=reconciled.account,
        ledger_snapshot={},
        reason=reconciled.reason or protection_result.reason,
        safe_to_configure=safe_to_configure,
        protection_exit_symbols=protection_exit_symbols,
    )


__all__ = [
    "AccountRefreshResult",
    "refresh_paper_account",
    "refresh_testnet_account",
]
