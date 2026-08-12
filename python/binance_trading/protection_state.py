"""
[INPUT]: 依赖 SQLite 事务钩子、Decimal 与 UTC 时间语义，接收已成交入场计划的两腿保护意图
[OUTPUT]: 提供 ProtectionLegSpec/Record、ProtectionSetRecord 与 ProtectionStateMixin，持久化 Algo 保护单的 CAS 阶段、胜者互斥、LOCAL 延后撤单屏障、部分成交与安全清理
[POS]: binance_trading 的交易所托管保护状态核心；不发网络请求，由 state.py 组合进恢复真源
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Optional, Tuple

from .models import decimal_value, utc_datetime


PROTECTION_KINDS = ("STOP", "TARGET")
PROTECTION_WINNERS = ("STOP", "TARGET", "LOCAL")
PROTECTION_SET_STATES = (
    "PREPARED",
    "ARMING",
    "ARMED",
    "EXITING",
    "CANCELING",
    "UNKNOWN",
    "CLOSED",
)
PROTECTION_LEG_PHASES = (
    "PREPARED",
    "SUBMIT_UNKNOWN",
    "OPEN",
    "TRIGGERED",
    "CANCEL_UNKNOWN",
    "CANCELED",
    "FILLED",
    "EXPIRED",
    "FAILED",
)
PROTECTION_TERMINAL_PHASES = ("CANCELED", "FILLED", "EXPIRED", "FAILED")

_CLIENT_ALGO_ID = re.compile(r"^[.A-Za-z0-9_:/-]{1,36}$")
_LEG_TRANSITIONS = {
    "PREPARED": ("SUBMIT_UNKNOWN", "CANCELED", "FAILED"),
    "SUBMIT_UNKNOWN": ("PREPARED", "OPEN", "TRIGGERED", "CANCEL_UNKNOWN", "CANCELED", "EXPIRED", "FAILED"),
    "OPEN": ("TRIGGERED", "CANCEL_UNKNOWN", "CANCELED", "EXPIRED", "FAILED"),
    "TRIGGERED": ("CANCEL_UNKNOWN", "CANCELED", "FILLED", "EXPIRED", "FAILED"),
    "CANCEL_UNKNOWN": ("OPEN", "TRIGGERED", "CANCELED", "FILLED", "EXPIRED", "FAILED"),
    "CANCELED": (),
    "FILLED": (),
    "EXPIRED": (),
    "FAILED": (),
}
_SET_TRANSITIONS = {
    "PREPARED": ("ARMING", "EXITING", "UNKNOWN", "CLOSED"),
    "ARMING": ("ARMED", "EXITING", "CANCELING", "UNKNOWN"),
    "ARMED": ("EXITING", "CANCELING", "UNKNOWN"),
    "EXITING": ("CANCELING", "UNKNOWN", "CLOSED"),
    "CANCELING": ("EXITING", "UNKNOWN", "CLOSED"),
    "UNKNOWN": ("ARMING", "ARMED", "EXITING", "CANCELING", "CLOSED"),
    "CLOSED": (),
}


def _iso(value: datetime) -> str:
    return utc_datetime(value).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _now(value: Optional[datetime]) -> datetime:
    return utc_datetime(value or datetime.now(timezone.utc), "observed_at")


@dataclass(frozen=True)
class ProtectionLegSpec:
    entry_order_id: str
    symbol: str
    kind: str
    client_algo_id: str
    order_type: str
    trigger_price: Decimal
    side: str
    request_fingerprint: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "entry_order_id", str(self.entry_order_id or "").strip())
        object.__setattr__(self, "symbol", str(self.symbol or "").strip().upper())
        object.__setattr__(self, "kind", str(self.kind or "").strip().upper())
        object.__setattr__(self, "client_algo_id", str(self.client_algo_id or "").strip())
        object.__setattr__(self, "order_type", str(self.order_type or "").strip().upper())
        object.__setattr__(self, "side", str(self.side or "").strip().upper())
        object.__setattr__(
            self, "request_fingerprint", str(self.request_fingerprint or "").strip()
        )
        object.__setattr__(
            self, "trigger_price", decimal_value(self.trigger_price, "trigger_price")
        )
        if not self.entry_order_id or not self.symbol or not self.symbol.isalnum():
            raise ValueError("保护腿入场身份/symbol 无效")
        if self.kind not in PROTECTION_KINDS:
            raise ValueError("保护腿 kind 必须是 STOP/TARGET")
        expected_type = "STOP_MARKET" if self.kind == "STOP" else "TAKE_PROFIT_MARKET"
        if self.order_type != expected_type:
            raise ValueError("保护腿 kind/order_type 不匹配")
        if self.side not in ("BUY", "SELL") or self.trigger_price <= 0:
            raise ValueError("保护腿 side/触发价无效")
        if not _CLIENT_ALGO_ID.fullmatch(self.client_algo_id):
            raise ValueError("client_algo_id 必须符合 Binance 1..36 字符约束")
        if not self.request_fingerprint or len(self.request_fingerprint) > 256:
            raise ValueError("保护腿请求指纹无效")


@dataclass(frozen=True)
class ProtectionSetRecord:
    entry_order_id: str
    symbol: str
    winner_kind: Optional[str]
    state: str
    revision: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ProtectionLegRecord:
    spec: ProtectionLegSpec
    phase: str
    algo_id: Optional[str]
    algo_status: Optional[str]
    actual_order_id: Optional[str]
    cumulative_filled_quantity: Decimal
    average_price: Optional[Decimal]
    last_error: str
    updated_at: datetime


class ProtectionStateMixin:
    """Compose crash-safe protection persistence into RuntimeStateStore."""

    def _initialize_protection_tables(self, cursor: sqlite3.Cursor) -> None:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS binance_protection_sets (
                entry_order_id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL UNIQUE,
                winner_kind TEXT CHECK(winner_kind IN ('STOP', 'TARGET', 'LOCAL')),
                state TEXT NOT NULL CHECK(state IN (
                    'PREPARED', 'ARMING', 'ARMED', 'EXITING',
                    'CANCELING', 'UNKNOWN', 'CLOSED'
                )),
                revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS binance_protection_legs (
                entry_order_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                kind TEXT NOT NULL CHECK(kind IN ('STOP', 'TARGET')),
                client_algo_id TEXT NOT NULL UNIQUE,
                algo_id TEXT,
                order_type TEXT NOT NULL CHECK(order_type IN ('STOP_MARKET', 'TAKE_PROFIT_MARKET')),
                trigger_price TEXT NOT NULL,
                side TEXT NOT NULL CHECK(side IN ('BUY', 'SELL')),
                phase TEXT NOT NULL CHECK(phase IN (
                    'PREPARED', 'SUBMIT_UNKNOWN', 'OPEN', 'TRIGGERED',
                    'CANCEL_UNKNOWN', 'CANCELED', 'FILLED', 'EXPIRED', 'FAILED'
                )),
                algo_status TEXT,
                actual_order_id TEXT,
                cumulative_filled_quantity TEXT NOT NULL DEFAULT '0',
                average_price TEXT,
                request_fingerprint TEXT NOT NULL,
                last_error TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(entry_order_id, kind),
                FOREIGN KEY(entry_order_id) REFERENCES binance_protection_sets(entry_order_id)
            )
            """
        )

    @staticmethod
    def _set_from_row(row: sqlite3.Row) -> ProtectionSetRecord:
        return ProtectionSetRecord(
            entry_order_id=row["entry_order_id"],
            symbol=row["symbol"],
            winner_kind=row["winner_kind"],
            state=row["state"],
            revision=int(row["revision"]),
            created_at=_parse_time(row["created_at"]),
            updated_at=_parse_time(row["updated_at"]),
        )

    @staticmethod
    def _leg_from_row(row: sqlite3.Row) -> ProtectionLegRecord:
        return ProtectionLegRecord(
            spec=ProtectionLegSpec(
                entry_order_id=row["entry_order_id"],
                symbol=row["symbol"],
                kind=row["kind"],
                client_algo_id=row["client_algo_id"],
                order_type=row["order_type"],
                trigger_price=Decimal(row["trigger_price"]),
                side=row["side"],
                request_fingerprint=row["request_fingerprint"],
            ),
            phase=row["phase"],
            algo_id=row["algo_id"],
            algo_status=row["algo_status"],
            actual_order_id=row["actual_order_id"],
            cumulative_filled_quantity=Decimal(row["cumulative_filled_quantity"]),
            average_price=Decimal(row["average_price"]) if row["average_price"] else None,
            last_error=row["last_error"],
            updated_at=_parse_time(row["updated_at"]),
        )

    @staticmethod
    def _validate_bundle(plan: Any, legs: Tuple[ProtectionLegSpec, ...]) -> None:
        if len(legs) != 2 or {leg.kind for leg in legs} != set(PROTECTION_KINDS):
            raise ValueError("托管保护必须同时持久化 STOP/TARGET 两腿")
        if len({leg.client_algo_id for leg in legs}) != 2:
            raise ValueError("两腿 client_algo_id 必须唯一")
        expected_side = plan.direction.exit_side
        for leg in legs:
            expected_price = plan.stop_price if leg.kind == "STOP" else plan.target_price
            if (
                leg.entry_order_id != plan.entry_order_id
                or leg.symbol != plan.symbol
                or leg.side != expected_side
                or leg.trigger_price != expected_price
            ):
                raise ValueError("保护腿与持仓计划身份/几何不一致")

    @staticmethod
    def _static_leg_identity(row: sqlite3.Row) -> Tuple[str, ...]:
        return (
            row["entry_order_id"],
            row["symbol"],
            row["kind"],
            row["client_algo_id"],
            row["order_type"],
            row["trigger_price"],
            row["side"],
            row["request_fingerprint"],
        )

    @staticmethod
    def _spec_identity(spec: ProtectionLegSpec) -> Tuple[str, ...]:
        return (
            spec.entry_order_id,
            spec.symbol,
            spec.kind,
            spec.client_algo_id,
            spec.order_type,
            format(spec.trigger_price, "f"),
            spec.side,
            spec.request_fingerprint,
        )

    def _assert_protection_bundle_matches(
        self, cursor: sqlite3.Cursor, plan: Any, legs: Tuple[ProtectionLegSpec, ...]
    ) -> None:
        self._validate_bundle(plan, legs)
        stored_set = cursor.execute(
            "SELECT entry_order_id, symbol FROM binance_protection_sets WHERE entry_order_id=?",
            (plan.entry_order_id,),
        ).fetchone()
        rows = cursor.execute(
            "SELECT * FROM binance_protection_legs WHERE entry_order_id=? ORDER BY kind",
            (plan.entry_order_id,),
        ).fetchall()
        if stored_set is None or stored_set["symbol"] != plan.symbol or len(rows) != 2:
            raise ValueError("已应用入场缺少原子保护束")
        expected = sorted((self._spec_identity(leg) for leg in legs), key=lambda item: item[2])
        actual = sorted((self._static_leg_identity(row) for row in rows), key=lambda item: item[2])
        if actual != expected:
            raise ValueError("入场身份已绑定不同保护请求")

    def _insert_protection_bundle(
        self, cursor: sqlite3.Cursor, plan: Any, legs: Iterable[ProtectionLegSpec]
    ) -> None:
        specs = tuple(legs)
        if not specs:
            return
        self._validate_bundle(plan, specs)
        existing = cursor.execute(
            "SELECT entry_order_id FROM binance_protection_sets WHERE entry_order_id=?",
            (plan.entry_order_id,),
        ).fetchone()
        if existing is not None:
            self._assert_protection_bundle_matches(cursor, plan, specs)
            return
        timestamp = _iso(plan.updated_at)
        cursor.execute(
            """
            INSERT INTO binance_protection_sets(
                entry_order_id, symbol, winner_kind, state, revision, created_at, updated_at
            ) VALUES (?, ?, NULL, 'PREPARED', 0, ?, ?)
            """,
            (plan.entry_order_id, plan.symbol, timestamp, timestamp),
        )
        for leg in specs:
            cursor.execute(
                """
                INSERT INTO binance_protection_legs(
                    entry_order_id, symbol, kind, client_algo_id, order_type,
                    trigger_price, side, phase, request_fingerprint, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'PREPARED', ?, ?)
                """,
                (
                    leg.entry_order_id,
                    leg.symbol,
                    leg.kind,
                    leg.client_algo_id,
                    leg.order_type,
                    format(leg.trigger_price, "f"),
                    leg.side,
                    leg.request_fingerprint,
                    timestamp,
                ),
            )

    def ensure_protection_bundle(
        self, plan: Any, legs: Iterable[ProtectionLegSpec]
    ) -> ProtectionSetRecord:
        specs = tuple(legs)
        with self._transaction() as cursor:
            self._insert_protection_bundle(cursor, plan, specs)
            row = cursor.execute(
                "SELECT * FROM binance_protection_sets WHERE entry_order_id=?",
                (plan.entry_order_id,),
            ).fetchone()
            if row is None:
                raise ValueError("保护束不能为空")
            return self._set_from_row(row)

    def get_protection_set(self, entry_order_id: str) -> Optional[ProtectionSetRecord]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM binance_protection_sets WHERE entry_order_id=?",
                (entry_order_id,),
            ).fetchone()
        return self._set_from_row(row) if row is not None else None

    def protection_sets(self) -> Tuple[ProtectionSetRecord, ...]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM binance_protection_sets ORDER BY created_at, entry_order_id"
            ).fetchall()
        return tuple(self._set_from_row(row) for row in rows)

    def get_protection_leg(
        self, entry_order_id: str, kind: str
    ) -> Optional[ProtectionLegRecord]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM binance_protection_legs WHERE entry_order_id=? AND kind=?",
                (entry_order_id, str(kind).upper()),
            ).fetchone()
        return self._leg_from_row(row) if row is not None else None

    def protection_legs(
        self, entry_order_id: Optional[str] = None
    ) -> Tuple[ProtectionLegRecord, ...]:
        query = "SELECT * FROM binance_protection_legs"
        parameters: Tuple[Any, ...] = ()
        if entry_order_id is not None:
            query += " WHERE entry_order_id=?"
            parameters = (entry_order_id,)
        query += " ORDER BY entry_order_id, CASE kind WHEN 'STOP' THEN 0 ELSE 1 END"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        return tuple(self._leg_from_row(row) for row in rows)

    def transition_protection_set(
        self,
        entry_order_id: str,
        expected_state: str,
        next_state: str,
        expected_revision: int,
        observed_at: Optional[datetime] = None,
    ) -> bool:
        current = str(expected_state).upper()
        target = str(next_state).upper()
        if current not in PROTECTION_SET_STATES or target not in _SET_TRANSITIONS[current]:
            raise ValueError("保护束状态跃迁无效")
        now = _now(observed_at)
        with self._transaction() as cursor:
            cursor.execute(
                """
                UPDATE binance_protection_sets
                SET state=?, revision=revision+1, updated_at=?
                WHERE entry_order_id=? AND state=? AND revision=?
                """,
                (target, _iso(now), entry_order_id, current, int(expected_revision)),
            )
            return cursor.rowcount == 1

    def transition_protection_leg(
        self,
        entry_order_id: str,
        kind: str,
        expected_phase: str,
        next_phase: str,
        observed_at: Optional[datetime] = None,
        *,
        algo_id: Optional[str] = None,
        algo_status: Optional[str] = None,
        actual_order_id: Optional[str] = None,
        cumulative_filled_quantity: Optional[Any] = None,
        average_price: Optional[Any] = None,
        last_error: Optional[str] = None,
    ) -> bool:
        current = str(expected_phase).upper()
        target = str(next_phase).upper()
        if current not in PROTECTION_LEG_PHASES or (
            target != current and target not in _LEG_TRANSITIONS[current]
        ):
            raise ValueError("保护腿阶段跃迁无效")
        parsed_quantity = None
        if cumulative_filled_quantity is not None:
            parsed_quantity = decimal_value(
                cumulative_filled_quantity, "cumulative_filled_quantity"
            )
            if parsed_quantity < 0:
                raise ValueError("累计成交量不能为负")
        parsed_average = None
        if average_price is not None:
            parsed_average = decimal_value(average_price, "average_price")
            if parsed_average <= 0:
                raise ValueError("平均成交价必须为正")
        now = _now(observed_at)
        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT * FROM binance_protection_legs WHERE entry_order_id=? AND kind=?",
                (entry_order_id, str(kind).upper()),
            ).fetchone()
            if row is None or row["phase"] != current:
                return False
            for column, supplied in (("algo_id", algo_id), ("actual_order_id", actual_order_id)):
                if supplied is not None and row[column] not in (None, str(supplied)):
                    raise ValueError("保护腿交易所身份发生冲突")
            if parsed_quantity is not None and parsed_quantity < Decimal(
                row["cumulative_filled_quantity"]
            ):
                raise ValueError("保护腿累计成交量不得回退")
            cursor.execute(
                """
                UPDATE binance_protection_legs SET
                    phase=?, algo_id=COALESCE(?, algo_id),
                    algo_status=COALESCE(?, algo_status),
                    actual_order_id=COALESCE(?, actual_order_id),
                    cumulative_filled_quantity=COALESCE(?, cumulative_filled_quantity),
                    average_price=COALESCE(?, average_price),
                    last_error=COALESCE(?, last_error), updated_at=?
                WHERE entry_order_id=? AND kind=? AND phase=?
                """,
                (
                    target,
                    None if algo_id is None else str(algo_id),
                    None if algo_status is None else str(algo_status),
                    None if actual_order_id is None else str(actual_order_id),
                    None if parsed_quantity is None else format(parsed_quantity, "f"),
                    None if parsed_average is None else format(parsed_average, "f"),
                    None if last_error is None else str(last_error),
                    _iso(now),
                    entry_order_id,
                    str(kind).upper(),
                    current,
                ),
            )
            return cursor.rowcount == 1

    def record_protection_fill(
        self,
        entry_order_id: str,
        kind: str,
        cumulative_quantity: Any,
        average_price: Any,
        observed_at: Optional[datetime] = None,
        *,
        terminal: bool = False,
        actual_order_id: Optional[str] = None,
    ) -> Decimal:
        quantity = decimal_value(cumulative_quantity, "cumulative_quantity")
        average = decimal_value(average_price, "average_price")
        if quantity < 0 or (quantity > 0 and average <= 0):
            raise ValueError("保护腿成交事实无效")
        now = _now(observed_at)
        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT * FROM binance_protection_legs WHERE entry_order_id=? AND kind=?",
                (entry_order_id, str(kind).upper()),
            ).fetchone()
            if row is None:
                raise ValueError("保护腿不存在")
            previous = Decimal(row["cumulative_filled_quantity"])
            if quantity < previous:
                raise ValueError("保护腿累计成交量不得回退")
            if actual_order_id is not None and row["actual_order_id"] not in (
                None,
                str(actual_order_id),
            ):
                raise ValueError("保护腿实际订单身份发生冲突")
            target_phase = "FILLED" if terminal else "TRIGGERED"
            if row["phase"] in PROTECTION_TERMINAL_PHASES and row["phase"] != target_phase:
                raise ValueError("终态保护腿不得重新打开")
            cursor.execute(
                """
                UPDATE binance_protection_legs SET
                    phase=?, actual_order_id=COALESCE(?, actual_order_id),
                    cumulative_filled_quantity=?, average_price=?, updated_at=?
                WHERE entry_order_id=? AND kind=?
                """,
                (
                    target_phase,
                    None if actual_order_id is None else str(actual_order_id),
                    format(quantity, "f"),
                    None if quantity == 0 else format(average, "f"),
                    _iso(now),
                    entry_order_id,
                    str(kind).upper(),
                ),
            )
            return quantity - previous

    def claim_protection_winner(
        self,
        entry_order_id: str,
        winner_kind: str,
        observed_at: Optional[datetime] = None,
        *,
        expected_revision: Optional[int] = None,
    ) -> bool:
        winner = str(winner_kind).upper()
        if winner not in PROTECTION_WINNERS:
            raise ValueError("保护胜者必须是 STOP/TARGET/LOCAL")
        now = _now(observed_at)
        with self._transaction() as cursor:
            query = """
                UPDATE binance_protection_sets
                SET winner_kind=?, state='EXITING', revision=revision+1, updated_at=?
                WHERE entry_order_id=? AND winner_kind IS NULL
            """
            parameters = [winner, _iso(now), entry_order_id]
            if expected_revision is not None:
                query += " AND revision=?"
                parameters.append(int(expected_revision))
            cursor.execute(query, tuple(parameters))
            if cursor.rowcount != 1:
                return False
            # LOCAL 只抢占退出权；原生腿必须等权威 positionAmt=0 后，
            # 再由 begin_local_protection_cancellation 处理。
            if winner == "LOCAL":
                return True
            if winner in PROTECTION_KINDS:
                cursor.execute(
                    """
                    UPDATE binance_protection_legs SET phase='TRIGGERED', updated_at=?
                    WHERE entry_order_id=? AND kind=? AND phase IN ('SUBMIT_UNKNOWN', 'OPEN')
                    """,
                    (_iso(now), entry_order_id, winner),
                )
            sibling_filter = " AND kind != ?"
            sibling_parameters = [_iso(now), entry_order_id, winner]
            cursor.execute(
                """
                UPDATE binance_protection_legs SET phase='CANCEL_UNKNOWN', updated_at=?
                WHERE entry_order_id=? AND phase IN ('SUBMIT_UNKNOWN', 'OPEN', 'TRIGGERED')
                """
                + sibling_filter,
                tuple(sibling_parameters),
            )
            cursor.execute(
                """
                UPDATE binance_protection_legs SET phase='CANCELED', updated_at=?
                WHERE entry_order_id=? AND phase='PREPARED'
                """
                + sibling_filter,
                tuple(sibling_parameters),
            )
            return True

    def begin_local_protection_cancellation(
        self,
        entry_order_id: str,
        observed_at: Optional[datetime] = None,
    ) -> bool:
        """Persist the post-flat cancellation barrier before any Algo DELETE."""
        now = _now(observed_at)
        with self._transaction() as cursor:
            row = cursor.execute(
                """
                SELECT winner_kind, state FROM binance_protection_sets
                WHERE entry_order_id=?
                """,
                (entry_order_id,),
            ).fetchone()
            if row is None:
                raise ValueError("保护束不存在")
            if row["winner_kind"] != "LOCAL":
                raise ValueError("只有 LOCAL 胜者可延后开始撤销原生保护腿")
            if row["state"] == "CANCELING":
                return False
            if row["state"] != "EXITING":
                raise ValueError("LOCAL 保护撤销只允许从 EXITING 开始")
            cursor.execute(
                """
                UPDATE binance_protection_sets
                SET state='CANCELING', revision=revision+1, updated_at=?
                WHERE entry_order_id=? AND winner_kind='LOCAL' AND state='EXITING'
                """,
                (_iso(now), entry_order_id),
            )
            if cursor.rowcount != 1:
                return False
            cursor.execute(
                """
                UPDATE binance_protection_legs SET phase='CANCEL_UNKNOWN', updated_at=?
                WHERE entry_order_id=?
                  AND phase IN ('SUBMIT_UNKNOWN', 'OPEN', 'TRIGGERED')
                """,
                (_iso(now), entry_order_id),
            )
            cursor.execute(
                """
                UPDATE binance_protection_legs SET phase='CANCELED', updated_at=?
                WHERE entry_order_id=? AND phase='PREPARED'
                """,
                (_iso(now), entry_order_id),
            )
            return True

    def delete_protection_bundle(
        self, entry_order_id: str, expected_revision: int
    ) -> bool:
        with self._transaction() as cursor:
            row = cursor.execute(
                "SELECT state, revision FROM binance_protection_sets WHERE entry_order_id=?",
                (entry_order_id,),
            ).fetchone()
            if row is None:
                return False
            if row["state"] != "CLOSED" or int(row["revision"]) != int(expected_revision):
                return False
            active = cursor.execute(
                """
                SELECT 1 FROM binance_protection_legs
                WHERE entry_order_id=? AND phase NOT IN ('CANCELED', 'FILLED', 'EXPIRED', 'FAILED')
                LIMIT 1
                """,
                (entry_order_id,),
            ).fetchone()
            if active is not None:
                raise ValueError("仍有活跃保护腿，禁止清理恢复证据")
            cursor.execute(
                "DELETE FROM binance_protection_legs WHERE entry_order_id=?",
                (entry_order_id,),
            )
            cursor.execute(
                "DELETE FROM binance_protection_sets WHERE entry_order_id=? AND revision=?",
                (entry_order_id, int(expected_revision)),
            )
            return cursor.rowcount == 1


__all__ = [
    "PROTECTION_KINDS",
    "PROTECTION_LEG_PHASES",
    "PROTECTION_SET_STATES",
    "PROTECTION_TERMINAL_PHASES",
    "PROTECTION_WINNERS",
    "ProtectionLegRecord",
    "ProtectionLegSpec",
    "ProtectionSetRecord",
    "ProtectionStateMixin",
]
