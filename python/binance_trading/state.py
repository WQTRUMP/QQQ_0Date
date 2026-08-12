"""
[INPUT]: 依赖订单意图/结果、已成交持仓计划、protection_state 的两腿 CAS 持久化、sqlite_health.require_sqlite_quick_check、UTC 基线与 SQLite 事务
[OUTPUT]: 提供 RuntimeStateStore、OrderJournalRecord 与 PositionPlan，在恢复库完整性通过后原子持久化订单结果、计划下修、保护束和每日权益基线
[POS]: binance_trading 的恢复真源；在任何 schema 写入前以 quick_check 失败关闭，然后组合订单日志与保护状态 mixin
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple

from .models import Direction, ExecutionResult, OrderIntent, decimal_value, utc_datetime
from .protection_state import ProtectionLegSpec, ProtectionStateMixin
from .sqlite_health import require_sqlite_quick_check


ORDER_PHASES = ("PREPARED", "DISPATCH_UNCERTAIN", "RESULT", "APPLIED")


@dataclass(frozen=True)
class PositionPlan:
    symbol: str
    direction: Direction
    quantity: Decimal
    entry_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    signal_id: str
    entry_order_id: str
    opened_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", str(self.symbol).upper())
        object.__setattr__(self, "direction", Direction(self.direction))
        for name in ("quantity", "entry_price", "stop_price", "target_price"):
            object.__setattr__(self, name, decimal_value(getattr(self, name), name))
        object.__setattr__(self, "opened_at", utc_datetime(self.opened_at, "opened_at"))
        object.__setattr__(self, "updated_at", utc_datetime(self.updated_at, "updated_at"))
        if self.quantity <= 0 or min(self.entry_price, self.stop_price, self.target_price) <= 0:
            raise ValueError("position plan quantity/price 必须为正数")
        geometry = (
            self.direction is Direction.LONG
            and self.stop_price < self.entry_price < self.target_price
        ) or (
            self.direction is Direction.SHORT
            and self.target_price < self.entry_price < self.stop_price
        )
        if not geometry or not self.signal_id or not self.entry_order_id:
            raise ValueError("position plan 方向几何/身份无效")


@dataclass(frozen=True)
class OrderJournalRecord:
    intent: OrderIntent
    phase: str
    result: Optional[ExecutionResult]
    updated_at: datetime

    def __post_init__(self) -> None:
        if self.phase not in ORDER_PHASES:
            raise ValueError("order journal phase 无效")
        object.__setattr__(self, "updated_at", utc_datetime(self.updated_at, "updated_at"))
        if self.result is not None and self.result.client_order_id != self.intent.client_order_id:
            raise ValueError("order journal intent/result 身份不一致")


def _iso(value: datetime) -> str:
    return utc_datetime(value).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


class RuntimeStateStore(ProtectionStateMixin):
    def __init__(self, path: Any) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            self.path,
            timeout=5,
            isolation_level=None,
            check_same_thread=False,
        )
        try:
            self._connection.row_factory = sqlite3.Row
            require_sqlite_quick_check(self._connection, "Binance 恢复状态库")
            self._connection.execute("PRAGMA busy_timeout = 5000")
            if self.path != ":memory:":
                self._connection.execute("PRAGMA journal_mode = WAL")
                self._connection.execute("PRAGMA synchronous = FULL")
            with self._transaction() as cursor:
                cursor.executescript(
                    """
                CREATE TABLE IF NOT EXISTS binance_position_plans (
                    symbol TEXT PRIMARY KEY,
                    direction TEXT NOT NULL CHECK(direction IN ('LONG', 'SHORT')),
                    quantity TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    stop_price TEXT NOT NULL,
                    target_price TEXT NOT NULL,
                    signal_id TEXT NOT NULL,
                    entry_order_id TEXT NOT NULL,
                    opened_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_daily_equity_baselines (
                    utc_date TEXT PRIMARY KEY,
                    equity TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS binance_order_journal (
                    client_order_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK(direction IN ('LONG', 'SHORT')),
                    side TEXT NOT NULL CHECK(side IN ('BUY', 'SELL')),
                    quantity TEXT NOT NULL,
                    order_type TEXT NOT NULL CHECK(order_type IN ('LIMIT', 'MARKET')),
                    limit_price TEXT,
                    reduce_only INTEGER NOT NULL CHECK(reduce_only IN (0, 1)),
                    signal_id TEXT NOT NULL,
                    stop_price TEXT,
                    target_price TEXT,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    phase TEXT NOT NULL CHECK(phase IN ('PREPARED', 'DISPATCH_UNCERTAIN', 'RESULT', 'APPLIED')),
                    result_status TEXT,
                    exchange_order_id TEXT,
                    executed_quantity TEXT,
                    average_price TEXT,
                    result_reason TEXT,
                    submission_unknown INTEGER CHECK(submission_unknown IN (0, 1)),
                    observed_at TEXT,
                    updated_at TEXT NOT NULL
                );
                    """
                )
                self._initialize_protection_tables(cursor)
        except BaseException:
            try:
                self._connection.close()
            except Exception:
                pass
            raise

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cursor = self._connection.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                yield cursor
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()
            finally:
                cursor.close()

    @staticmethod
    def _from_row(row: sqlite3.Row) -> PositionPlan:
        return PositionPlan(
            symbol=row["symbol"],
            direction=Direction(row["direction"]),
            quantity=Decimal(row["quantity"]),
            entry_price=Decimal(row["entry_price"]),
            stop_price=Decimal(row["stop_price"]),
            target_price=Decimal(row["target_price"]),
            signal_id=row["signal_id"],
            entry_order_id=row["entry_order_id"],
            opened_at=_parse_time(row["opened_at"]),
            updated_at=_parse_time(row["updated_at"]),
        )

    @staticmethod
    def _order_from_row(row: sqlite3.Row) -> OrderJournalRecord:
        intent = OrderIntent(
            client_order_id=row["client_order_id"],
            symbol=row["symbol"],
            direction=Direction(row["direction"]),
            side=row["side"],
            quantity=Decimal(row["quantity"]),
            order_type=row["order_type"],
            limit_price=Decimal(row["limit_price"]) if row["limit_price"] else None,
            reduce_only=bool(row["reduce_only"]),
            created_at=_parse_time(row["created_at"]),
            signal_id=row["signal_id"],
            stop_price=Decimal(row["stop_price"]) if row["stop_price"] else None,
            target_price=Decimal(row["target_price"]) if row["target_price"] else None,
            reason=row["reason"],
        )
        result = None
        if row["result_status"] is not None:
            result = ExecutionResult(
                client_order_id=row["client_order_id"],
                status=row["result_status"],
                order_id=row["exchange_order_id"] or "",
                executed_quantity=Decimal(row["executed_quantity"] or "0"),
                average_price=Decimal(row["average_price"] or "0"),
                observed_at=_parse_time(row["observed_at"]),
                reason=row["result_reason"] or "",
                submission_unknown=bool(row["submission_unknown"]),
            )
        return OrderJournalRecord(
            intent=intent,
            phase=row["phase"],
            result=result,
            updated_at=_parse_time(row["updated_at"]),
        )

    @staticmethod
    def _intent_identity(intent: OrderIntent) -> Tuple[Any, ...]:
        return (
            intent.symbol,
            intent.direction.value,
            intent.side,
            format(intent.quantity, "f"),
            intent.order_type,
            None if intent.limit_price is None else format(intent.limit_price, "f"),
            int(intent.reduce_only),
            intent.signal_id,
            None if intent.stop_price is None else format(intent.stop_price, "f"),
            None if intent.target_price is None else format(intent.target_price, "f"),
            intent.reason,
            _iso(intent.created_at),
        )

    def prepare_order(self, intent: OrderIntent) -> OrderJournalRecord:
        if not isinstance(intent, OrderIntent):
            raise TypeError("order journal 只接受已验证 OrderIntent")
        now = datetime.now(timezone.utc)
        with self._transaction() as cursor:
            existing = cursor.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (intent.client_order_id,),
            ).fetchone()
            if existing is not None:
                record = self._order_from_row(existing)
                if self._intent_identity(record.intent) != self._intent_identity(intent):
                    raise ValueError("client_order_id 已绑定不同订单意图")
                return record
            cursor.execute(
                """
                INSERT INTO binance_order_journal(
                    client_order_id, symbol, direction, side, quantity, order_type,
                    limit_price, reduce_only, signal_id, stop_price, target_price,
                    reason, created_at, phase, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PREPARED', ?)
                """,
                (
                    intent.client_order_id,
                    intent.symbol,
                    intent.direction.value,
                    intent.side,
                    format(intent.quantity, "f"),
                    intent.order_type,
                    None if intent.limit_price is None else format(intent.limit_price, "f"),
                    int(intent.reduce_only),
                    intent.signal_id,
                    None if intent.stop_price is None else format(intent.stop_price, "f"),
                    None if intent.target_price is None else format(intent.target_price, "f"),
                    intent.reason,
                    _iso(intent.created_at),
                    _iso(now),
                ),
            )
            row = cursor.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (intent.client_order_id,),
            ).fetchone()
            return self._order_from_row(row)

    def mark_dispatch_uncertain(
        self, client_order_id: str, observed_at: Optional[datetime] = None
    ) -> OrderJournalRecord:
        record, _claimed = self.claim_dispatch_uncertain(client_order_id, observed_at)
        return record

    def claim_dispatch_uncertain(
        self, client_order_id: str, observed_at: Optional[datetime] = None
    ) -> Tuple[OrderJournalRecord, bool]:
        """原子选出唯一派发者；非赢家只能恢复或复用已有结果。"""

        now = utc_datetime(observed_at or datetime.now(timezone.utc), "observed_at")
        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            if row is None:
                raise ValueError("订单必须先 PREPARED")
            claimed = False
            if row["phase"] == "PREPARED":
                cursor.execute(
                    """
                    UPDATE binance_order_journal SET phase='DISPATCH_UNCERTAIN', updated_at=?
                    WHERE client_order_id=? AND phase='PREPARED'
                    """,
                    (_iso(now), client_order_id),
                )
                claimed = cursor.rowcount == 1
                if not claimed:
                    raise RuntimeError("订单派发 CAS 未能选出唯一赢家")
            elif row["phase"] not in ("DISPATCH_UNCERTAIN", "RESULT", "APPLIED"):
                raise ValueError("订单阶段不能进入派发不确定态")
            updated = cursor.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            return self._order_from_row(updated), claimed

    def record_result(self, result: ExecutionResult) -> OrderJournalRecord:
        if not isinstance(result, ExecutionResult):
            raise TypeError("order journal 只接受已验证 ExecutionResult")
        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (result.client_order_id,),
            ).fetchone()
            if row is None or row["phase"] not in ("DISPATCH_UNCERTAIN", "RESULT"):
                raise ValueError("订单结果缺少可恢复的派发前态")
            phase = "DISPATCH_UNCERTAIN" if result.submission_unknown else "RESULT"
            if row["phase"] == "RESULT":
                existing = self._order_from_row(row).result
                if existing != result:
                    raise ValueError("订单终态结果发生冲突")
                return self._order_from_row(row)
            cursor.execute(
                """
                UPDATE binance_order_journal SET
                    phase=?, result_status=?, exchange_order_id=?, executed_quantity=?,
                    average_price=?, result_reason=?, submission_unknown=?, observed_at=?, updated_at=?
                WHERE client_order_id=?
                """,
                (
                    phase,
                    result.status,
                    result.order_id,
                    format(result.executed_quantity, "f"),
                    format(result.average_price, "f"),
                    result.reason,
                    int(result.submission_unknown),
                    _iso(result.observed_at),
                    _iso(result.observed_at),
                    result.client_order_id,
                ),
            )
            updated = cursor.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (result.client_order_id,),
            ).fetchone()
            return self._order_from_row(updated)

    def mark_applied(
        self, client_order_id: str, observed_at: Optional[datetime] = None
    ) -> OrderJournalRecord:
        now = utc_datetime(observed_at or datetime.now(timezone.utc), "observed_at")
        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            if row is None or row["phase"] not in ("RESULT", "APPLIED"):
                raise ValueError("只有已有明确结果的订单可标记 APPLIED")
            if row["phase"] == "RESULT":
                cursor.execute(
                    "UPDATE binance_order_journal SET phase='APPLIED', updated_at=? WHERE client_order_id=?",
                    (_iso(now), client_order_id),
                )
            updated = cursor.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            return self._order_from_row(updated)

    def abandon_prepared(
        self, client_order_id: str, observed_at: Optional[datetime] = None
    ) -> OrderJournalRecord:
        """Close a PREPARED record that provably never crossed the dispatch barrier."""
        now = utc_datetime(observed_at or datetime.now(timezone.utc), "observed_at")
        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT phase FROM binance_order_journal WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            if row is None or row["phase"] not in ("PREPARED", "APPLIED"):
                raise ValueError("只有 PREPARED 订单可安全放弃")
            if row["phase"] == "PREPARED":
                cursor.execute(
                    """
                    UPDATE binance_order_journal SET
                        phase='APPLIED', result_status='NOT_DISPATCHED', exchange_order_id='',
                        executed_quantity='0', average_price='0', result_reason='RECOVERED_PREPARED',
                        submission_unknown=0, observed_at=?, updated_at=?
                    WHERE client_order_id=?
                    """,
                    (_iso(now), _iso(now), client_order_id),
                )
            updated = cursor.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            return self._order_from_row(updated)

    def apply_entry_plan(
        self,
        client_order_id: str,
        plan: PositionPlan,
        protection_legs: Tuple[ProtectionLegSpec, ...] = (),
    ) -> None:
        """Atomically materialize entry plan, optional native legs, and APPLIED phase."""
        legs = tuple(protection_legs)
        with self._transaction() as cursor:
            order = cursor.execute(
                """
                SELECT phase, symbol, direction, reduce_only, signal_id,
                       executed_quantity, average_price, observed_at
                FROM binance_order_journal WHERE client_order_id = ?
                """,
                (client_order_id,),
            ).fetchone()
            if order is None or order["phase"] not in ("RESULT", "APPLIED"):
                raise ValueError("entry plan 缺少明确订单结果")
            if order["symbol"] != plan.symbol or bool(order["reduce_only"]):
                raise ValueError("entry plan 与订单方向不一致")
            if order["phase"] == "APPLIED":
                if legs:
                    self._assert_protection_bundle_matches(cursor, plan, legs)
                return
            if (
                order["direction"] != plan.direction.value
                or order["signal_id"] != plan.signal_id
                or client_order_id != plan.entry_order_id
                or Decimal(order["executed_quantity"] or "0") != plan.quantity
                or Decimal(order["average_price"] or "0") != plan.entry_price
                or order["observed_at"] != _iso(plan.opened_at)
            ):
                raise ValueError("entry result 与保护计划成交事实不一致")
            existing = cursor.execute(
                "SELECT signal_id, entry_order_id, opened_at, quantity FROM binance_position_plans WHERE symbol = ?",
                (plan.symbol,),
            ).fetchone()
            if existing is not None and (
                existing["signal_id"] != plan.signal_id
                or existing["entry_order_id"] != plan.entry_order_id
                or existing["opened_at"] != _iso(plan.opened_at)
            ):
                raise ValueError("已有持仓计划不得被另一入场身份覆盖")
            if existing is not None and plan.quantity > Decimal(existing["quantity"]):
                raise ValueError("同一入场计划数量只能向下收敛")
            cursor.execute(
                """
                INSERT INTO binance_position_plans(
                    symbol, direction, quantity, entry_price, stop_price, target_price,
                    signal_id, entry_order_id, opened_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    direction=excluded.direction, quantity=excluded.quantity,
                    entry_price=excluded.entry_price, stop_price=excluded.stop_price,
                    target_price=excluded.target_price, updated_at=excluded.updated_at
                """,
                (
                    plan.symbol,
                    plan.direction.value,
                    format(plan.quantity, "f"),
                    format(plan.entry_price, "f"),
                    format(plan.stop_price, "f"),
                    format(plan.target_price, "f"),
                    plan.signal_id,
                    plan.entry_order_id,
                    _iso(plan.opened_at),
                    _iso(plan.updated_at),
                ),
            )
            self._insert_protection_bundle(cursor, plan, legs)
            cursor.execute(
                "UPDATE binance_order_journal SET phase='APPLIED', updated_at=? WHERE client_order_id=?",
                (_iso(plan.updated_at), client_order_id),
            )

    def apply_exit_fill(
        self,
        client_order_id: str,
        symbol: str,
        entry_order_id: str,
        remaining_quantity: Decimal,
        observed_at: datetime,
    ) -> None:
        """Atomically reduce/delete the protection plan and consume an exit result."""
        remaining = decimal_value(remaining_quantity, "remaining_quantity")
        if remaining < 0:
            raise ValueError("remaining_quantity 不能为负")
        now = utc_datetime(observed_at, "observed_at")
        with self._transaction() as cursor:
            order = cursor.execute(
                """
                SELECT phase, symbol, reduce_only, executed_quantity
                FROM binance_order_journal WHERE client_order_id = ?
                """,
                (client_order_id,),
            ).fetchone()
            if order is None or order["phase"] not in ("RESULT", "APPLIED"):
                raise ValueError("exit apply 缺少明确订单结果")
            if order["symbol"] != str(symbol).upper() or not bool(order["reduce_only"]):
                raise ValueError("exit result 与保护计划不一致")
            if order["phase"] == "APPLIED":
                return
            plan = cursor.execute(
                "SELECT quantity FROM binance_position_plans WHERE symbol=? AND entry_order_id=?",
                (str(symbol).upper(), entry_order_id),
            ).fetchone()
            if plan is None:
                if remaining != 0:
                    raise ValueError("部分退出后保护计划缺失")
            else:
                current = Decimal(plan["quantity"])
                executed = Decimal(order["executed_quantity"] or "0")
                expected = max(current - executed, Decimal("0"))
                if remaining != expected:
                    raise ValueError("exit remaining 与订单成交数量不一致")
            if plan is not None and remaining == 0:
                cursor.execute(
                    "DELETE FROM binance_position_plans WHERE symbol=? AND entry_order_id=?",
                    (str(symbol).upper(), entry_order_id),
                )
            elif plan is not None:
                cursor.execute(
                    "UPDATE binance_position_plans SET quantity=?, updated_at=? WHERE symbol=? AND entry_order_id=?",
                    (format(remaining, "f"), _iso(now), str(symbol).upper(), entry_order_id),
                )
            cursor.execute(
                "UPDATE binance_order_journal SET phase='APPLIED', updated_at=? WHERE client_order_id=?",
                (_iso(now), client_order_id),
            )

    def unresolved_orders(self) -> Tuple[OrderJournalRecord, ...]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM binance_order_journal WHERE phase != 'APPLIED' ORDER BY created_at, client_order_id"
            ).fetchall()
        return tuple(self._order_from_row(row) for row in rows)

    def exit_attempt_count(self, symbol: str, signal_id: str) -> int:
        """Return the durable attempt ordinal source for one managed position exit."""

        with self._lock:
            row = self._connection.execute(
                """
                SELECT COUNT(*) AS total FROM binance_order_journal
                WHERE symbol=? AND signal_id=? AND reduce_only=1
                """,
                (str(symbol).upper(), str(signal_id)),
            ).fetchone()
        return int(row["total"])

    def get_order(self, client_order_id: str) -> Optional[OrderJournalRecord]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM binance_order_journal WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
        return self._order_from_row(row) if row is not None else None

    def plans(self) -> Dict[str, PositionPlan]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM binance_position_plans ORDER BY symbol"
            ).fetchall()
        return {row["symbol"]: self._from_row(row) for row in rows}

    def get_plan(self, symbol: str) -> Optional[PositionPlan]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM binance_position_plans WHERE symbol = ?",
                (str(symbol).upper(),),
            ).fetchone()
        return self._from_row(row) if row is not None else None

    def save_plan(self, plan: PositionPlan) -> None:
        with self._transaction() as cursor:
            existing = cursor.execute(
                "SELECT signal_id, entry_order_id, opened_at, quantity FROM binance_position_plans WHERE symbol = ?",
                (plan.symbol,),
            ).fetchone()
            if existing is not None and (
                existing["signal_id"] != plan.signal_id
                or existing["entry_order_id"] != plan.entry_order_id
                or existing["opened_at"] != _iso(plan.opened_at)
            ):
                raise ValueError("已有持仓计划不得被另一入场身份覆盖")
            if existing is not None and plan.quantity > Decimal(existing["quantity"]):
                raise ValueError("同一入场计划数量只能向下收敛")
            cursor.execute(
                """
                INSERT INTO binance_position_plans(
                    symbol, direction, quantity, entry_price, stop_price, target_price,
                    signal_id, entry_order_id, opened_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    direction=excluded.direction,
                    quantity=excluded.quantity,
                    entry_price=excluded.entry_price,
                    stop_price=excluded.stop_price,
                    target_price=excluded.target_price,
                    updated_at=excluded.updated_at
                """,
                (
                    plan.symbol,
                    plan.direction.value,
                    format(plan.quantity, "f"),
                    format(plan.entry_price, "f"),
                    format(plan.stop_price, "f"),
                    format(plan.target_price, "f"),
                    plan.signal_id,
                    plan.entry_order_id,
                    _iso(plan.opened_at),
                    _iso(plan.updated_at),
                ),
            )

    def shrink_plan_quantity(
        self,
        plan: PositionPlan,
        remaining_quantity: Any,
        observed_at: Optional[datetime] = None,
    ) -> Optional[PositionPlan]:
        """原子下修现有计划；并发删除一旦成功，旧快照不得将其复活。"""

        remaining = decimal_value(remaining_quantity, "remaining_quantity")
        if remaining <= 0:
            raise ValueError("下修后的计划数量必须为正")
        now = utc_datetime(observed_at or datetime.now(timezone.utc), "observed_at")
        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT * FROM binance_position_plans WHERE symbol=?",
                (plan.symbol,),
            ).fetchone()
            if row is None:
                return None
            current = self._from_row(row)
            if (
                current.entry_order_id != plan.entry_order_id
                or current.signal_id != plan.signal_id
                or current.direction is not plan.direction
                or current.opened_at != plan.opened_at
            ):
                raise ValueError("下修计划与现有 entry identity 不一致")
            if remaining > current.quantity:
                raise ValueError("持仓计划数量只能向下收敛")
            if remaining < current.quantity:
                cursor.execute(
                    """
                    UPDATE binance_position_plans SET quantity=?, updated_at=?
                    WHERE symbol=? AND entry_order_id=? AND quantity=?
                    """,
                    (
                        format(remaining, "f"),
                        _iso(now),
                        plan.symbol,
                        plan.entry_order_id,
                        format(current.quantity, "f"),
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("持仓计划下修 CAS 失败")
                row = cursor.execute(
                    "SELECT * FROM binance_position_plans WHERE symbol=?",
                    (plan.symbol,),
                ).fetchone()
            return self._from_row(row)

    def delete_plan(self, symbol: str, expected_order_id: Optional[str] = None) -> bool:
        with self._transaction() as cursor:
            if expected_order_id is None:
                cursor.execute(
                    "DELETE FROM binance_position_plans WHERE symbol = ?",
                    (str(symbol).upper(),),
                )
            else:
                cursor.execute(
                    "DELETE FROM binance_position_plans WHERE symbol = ? AND entry_order_id = ?",
                    (str(symbol).upper(), expected_order_id),
                )
            return cursor.rowcount == 1

    def ensure_day_baseline(
        self, equity: Any, observed_at: Optional[datetime] = None
    ) -> Decimal:
        parsed_equity = decimal_value(equity, "equity")
        if parsed_equity <= 0:
            raise ValueError("UTC 日基线权益必须为正")
        now = utc_datetime(observed_at or datetime.now(timezone.utc), "observed_at")
        utc_date = now.date().isoformat()
        with self._transaction() as cursor:
            existing = cursor.execute(
                "SELECT equity FROM binance_daily_equity_baselines WHERE utc_date = ?",
                (utc_date,),
            ).fetchone()
            if existing is not None:
                return Decimal(existing["equity"])
            cursor.execute(
                "INSERT INTO binance_daily_equity_baselines(utc_date, equity, observed_at) VALUES (?, ?, ?)",
                (utc_date, format(parsed_equity, "f"), _iso(now)),
            )
            return parsed_equity

    def close(self) -> None:
        with self._lock:
            self._connection.close()


__all__ = [
    "ORDER_PHASES",
    "OrderJournalRecord",
    "PositionPlan",
    "RuntimeStateStore",
]
