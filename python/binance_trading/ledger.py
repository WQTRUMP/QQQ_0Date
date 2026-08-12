"""
[INPUT]: 依赖已成交的单向持仓订单事实、手续费率、标记价、UTC 时钟与 sqlite_health.require_sqlite_quick_check
[OUTPUT]: 对外提供 PaperLedger、LedgerFill、LedgerPosition 及通过启动完整性门禁的可恢复资金快照
[POS]: binance_trading 的 SQLite paper 会计真源；在 schema 或资金变更前失败关闭，通过后才以单一事务同步持仓、损益、费用与 UTC 日基线
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional

from .sqlite_health import require_sqlite_quick_check


_ZERO = Decimal("0")


def _decimal(value: Any, name: str, positive: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("%s 必须是有限十进制数" % name) from exc
    if not parsed.is_finite() or (positive and parsed <= _ZERO):
        suffix = "有限正数" if positive else "有限十进制数"
        raise ValueError("%s 必须是%s" % (name, suffix))
    return parsed


def _text(value: Decimal) -> str:
    if value == _ZERO:
        return "0"
    return format(value.normalize(), "f")


def _utc(value: Optional[Any] = None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("账本时间必须显式带时区")
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float, Decimal)):
        number = Decimal(str(value))
        seconds = number / Decimal("1000") if abs(number) >= Decimal("1e11") else number
        return datetime.fromtimestamp(float(seconds), tz=timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("账本时间必须显式带时区")
        return parsed.astimezone(timezone.utc)
    raise ValueError("账本时间无效")


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class LedgerPosition:
    symbol: str
    quantity: Decimal
    entry_price: Decimal
    realized_pnl: Decimal
    updated_at: str

    @property
    def side(self) -> str:
        if self.quantity > _ZERO:
            return "LONG"
        if self.quantity < _ZERO:
            return "SHORT"
        return "FLAT"

    def unrealized_pnl(self, mark_price: Any) -> Decimal:
        mark = _decimal(mark_price, "mark_price", positive=True)
        if self.quantity == _ZERO:
            return _ZERO
        return (mark - self.entry_price) * self.quantity


@dataclass(frozen=True)
class LedgerFill:
    client_order_id: str
    exchange_order_id: str
    symbol: str
    side: str
    requested_quantity: Decimal
    quantity: Decimal
    price: Decimal
    fee: Decimal
    reduce_only: bool
    realized_pnl: Decimal
    executed_at: str
    idempotent: bool = False

    @property
    def executed_quantity(self) -> Decimal:
        return self.quantity


class PaperLedger:
    """Durable one-way futures ledger with exact Decimal arithmetic."""

    def __init__(self, path: Any, initial_balance: Any = "10000") -> None:
        self.path = str(path)
        starting_balance = _decimal(initial_balance, "initial_balance", positive=True)
        if self.path != ":memory:":
            parent = Path(self.path).expanduser().resolve().parent
            os.makedirs(str(parent), exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(
            self.path,
            timeout=5.0,
            isolation_level=None,
            check_same_thread=False,
        )
        try:
            self._connection.row_factory = sqlite3.Row
            require_sqlite_quick_check(self._connection, "Paper 账本")
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.execute("PRAGMA busy_timeout = 5000")
            if self.path != ":memory:":
                self._connection.execute("PRAGMA journal_mode = WAL")
                self._connection.execute("PRAGMA synchronous = FULL")
            self._initialize(starting_balance)
        except BaseException:
            try:
                self._connection.close()
            except Exception:
                pass
            raise

    def _initialize(self, starting_balance: Decimal) -> None:
        with self._transaction() as cursor:
            cursor.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS positions (
                    symbol TEXT PRIMARY KEY,
                    quantity TEXT NOT NULL,
                    entry_price TEXT NOT NULL,
                    realized_pnl TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS fills (
                    client_order_id TEXT PRIMARY KEY,
                    exchange_order_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
                    requested_quantity TEXT NOT NULL,
                    executed_quantity TEXT NOT NULL,
                    price TEXT NOT NULL,
                    fee_rate TEXT NOT NULL,
                    fee TEXT NOT NULL,
                    reduce_only INTEGER NOT NULL CHECK (reduce_only IN (0, 1)),
                    realized_pnl TEXT NOT NULL,
                    executed_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS fills_exchange_order_id
                    ON fills(exchange_order_id);
                CREATE TABLE IF NOT EXISTS daily_baselines (
                    utc_date TEXT PRIMARY KEY,
                    equity TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            existing = cursor.execute(
                "SELECT value FROM metadata WHERE key = 'initial_balance'"
            ).fetchone()
            if existing is None:
                values = {
                    "schema_version": "1",
                    "initial_balance": _text(starting_balance),
                    "wallet_balance": _text(starting_balance),
                    "realized_pnl": "0",
                    "fees": "0",
                }
                cursor.executemany(
                    "INSERT INTO metadata(key, value) VALUES (?, ?)", values.items()
                )
            elif Decimal(existing["value"]) != starting_balance:
                raise ValueError(
                    "重启时 initial_balance 与 SQLite 账本真源不一致"
                )

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
    def _metadata(cursor: sqlite3.Cursor, key: str) -> Decimal:
        row = cursor.execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            raise RuntimeError("账本 metadata 缺失: %s" % key)
        return Decimal(row["value"])

    @staticmethod
    def _set_metadata(cursor: sqlite3.Cursor, key: str, value: Decimal) -> None:
        cursor.execute(
            "UPDATE metadata SET value = ? WHERE key = ?", (_text(value), key)
        )
        if cursor.rowcount != 1:
            raise RuntimeError("账本 metadata 更新失败: %s" % key)

    @staticmethod
    def _position_from_row(row: sqlite3.Row) -> LedgerPosition:
        return LedgerPosition(
            symbol=row["symbol"],
            quantity=Decimal(row["quantity"]),
            entry_price=Decimal(row["entry_price"]),
            realized_pnl=Decimal(row["realized_pnl"]),
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _fill_from_row(row: sqlite3.Row) -> LedgerFill:
        return LedgerFill(
            client_order_id=row["client_order_id"],
            exchange_order_id=row["exchange_order_id"],
            symbol=row["symbol"],
            side=row["side"],
            requested_quantity=Decimal(row["requested_quantity"]),
            quantity=Decimal(row["executed_quantity"]),
            price=Decimal(row["price"]),
            fee=Decimal(row["fee"]),
            reduce_only=bool(row["reduce_only"]),
            realized_pnl=Decimal(row["realized_pnl"]),
            executed_at=row["executed_at"],
        )

    def _marks_for_rows(
        self,
        rows: Any,
        mark_prices: Optional[Mapping[str, Any]],
        default_to_entry: bool,
    ) -> Dict[str, Decimal]:
        raw = mark_prices or {}
        normalized = {str(key).upper(): value for key, value in raw.items()}
        marks = {}  # type: Dict[str, Decimal]
        for row in rows:
            quantity = Decimal(row["quantity"])
            if quantity == _ZERO:
                continue
            symbol = row["symbol"]
            value = normalized.get(symbol)
            if value is None and default_to_entry:
                value = row["entry_price"]
            if value is None:
                raise ValueError("日基线缺失 %s 的 mark price" % symbol)
            marks[symbol] = _decimal(value, "%s mark_price" % symbol, positive=True)
        return marks

    def _equity(
        self,
        cursor: sqlite3.Cursor,
        mark_prices: Optional[Mapping[str, Any]],
        default_to_entry: bool,
    ) -> tuple:
        wallet = self._metadata(cursor, "wallet_balance")
        rows = cursor.execute(
            "SELECT * FROM positions WHERE quantity != '0' ORDER BY symbol"
        ).fetchall()
        marks = self._marks_for_rows(rows, mark_prices, default_to_entry)
        unrealized = _ZERO
        for row in rows:
            quantity = Decimal(row["quantity"])
            if quantity == _ZERO:
                continue
            unrealized += (marks[row["symbol"]] - Decimal(row["entry_price"])) * quantity
        return wallet + unrealized, unrealized, rows, marks

    def _ensure_daily_baseline(
        self,
        cursor: sqlite3.Cursor,
        now: datetime,
        mark_prices: Optional[Mapping[str, Any]],
        default_to_entry: bool,
    ) -> Decimal:
        utc_date = now.date().isoformat()
        existing = cursor.execute(
            "SELECT equity FROM daily_baselines WHERE utc_date = ?", (utc_date,)
        ).fetchone()
        if existing is not None:
            return Decimal(existing["equity"])
        equity, _unrealized, _rows, _marks = self._equity(
            cursor, mark_prices, default_to_entry
        )
        cursor.execute(
            "INSERT INTO daily_baselines(utc_date, equity, created_at) VALUES (?, ?, ?)",
            (utc_date, _text(equity), _iso(now)),
        )
        return equity

    def record_fill(
        self,
        client_order_id: str,
        symbol: str,
        side: str,
        quantity: Any,
        price: Any,
        fee_rate: Any = "0",
        reduce_only: bool = False,
        executed_at: Optional[Any] = None,
        exchange_order_id: Optional[str] = None,
        mark_prices: Optional[Mapping[str, Any]] = None,
    ) -> LedgerFill:
        order_id = str(client_order_id or "").strip()
        exchange_id = str(exchange_order_id or order_id).strip()
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_side = str(side or "").strip().upper()
        requested = _decimal(quantity, "quantity", positive=True)
        fill_price = _decimal(price, "price", positive=True)
        parsed_fee_rate = _decimal(fee_rate, "fee_rate")
        if parsed_fee_rate < _ZERO:
            raise ValueError("fee_rate 不能为负")
        if not order_id or not exchange_id or not normalized_symbol:
            raise ValueError("成交身份和 symbol 不能为空")
        if normalized_side not in ("BUY", "SELL"):
            raise ValueError("side 必须是 BUY 或 SELL")
        timestamp = _utc(executed_at)

        with self._transaction() as cursor:
            existing = cursor.execute(
                "SELECT * FROM fills WHERE client_order_id = ?", (order_id,)
            ).fetchone()
            if existing is not None:
                same_request = (
                    existing["symbol"] == normalized_symbol
                    and existing["side"] == normalized_side
                    and Decimal(existing["requested_quantity"]) == requested
                    and Decimal(existing["price"]) == fill_price
                    and Decimal(existing["fee_rate"]) == parsed_fee_rate
                    and bool(existing["reduce_only"]) == bool(reduce_only)
                    and existing["exchange_order_id"] == exchange_id
                )
                if not same_request:
                    raise ValueError("幂等键对应了不同成交请求")
                return replace(self._fill_from_row(existing), idempotent=True)
            duplicate_exchange = cursor.execute(
                "SELECT client_order_id FROM fills WHERE exchange_order_id = ?",
                (exchange_id,),
            ).fetchone()
            if duplicate_exchange is not None:
                raise ValueError("交易所订单身份已绑定其他幂等键")

            position_row = cursor.execute(
                "SELECT * FROM positions WHERE symbol = ?", (normalized_symbol,)
            ).fetchone()
            old_quantity = (
                Decimal(position_row["quantity"]) if position_row is not None else _ZERO
            )
            old_entry = (
                Decimal(position_row["entry_price"])
                if position_row is not None
                else _ZERO
            )
            cumulative_realized = (
                Decimal(position_row["realized_pnl"])
                if position_row is not None
                else _ZERO
            )

            executed = requested
            delta_sign = Decimal("1") if normalized_side == "BUY" else Decimal("-1")
            if reduce_only:
                is_opposite = (old_quantity > _ZERO and normalized_side == "SELL") or (
                    old_quantity < _ZERO and normalized_side == "BUY"
                )
                if not is_opposite:
                    raise ValueError("reduce-only 订单不得建仓或加仓")
                executed = min(requested, abs(old_quantity))

            baseline_marks = dict(mark_prices or {})
            baseline_marks.setdefault(normalized_symbol, fill_price)
            self._ensure_daily_baseline(
                cursor, timestamp, baseline_marks, default_to_entry=False
            )

            signed_delta = executed * delta_sign
            realized = _ZERO
            if old_quantity == _ZERO or old_quantity * signed_delta > _ZERO:
                new_quantity = old_quantity + signed_delta
                if old_quantity == _ZERO:
                    new_entry = fill_price
                else:
                    new_entry = (
                        abs(old_quantity) * old_entry + executed * fill_price
                    ) / abs(new_quantity)
            else:
                closed_quantity = min(abs(old_quantity), executed)
                old_direction = Decimal("1") if old_quantity > _ZERO else Decimal("-1")
                realized = (fill_price - old_entry) * closed_quantity * old_direction
                new_quantity = old_quantity + signed_delta
                if new_quantity == _ZERO:
                    new_entry = _ZERO
                elif new_quantity * old_quantity > _ZERO:
                    new_entry = old_entry
                else:
                    new_entry = fill_price

            fee = executed * fill_price * parsed_fee_rate
            wallet = self._metadata(cursor, "wallet_balance") + realized - fee
            total_realized = self._metadata(cursor, "realized_pnl") + realized
            total_fees = self._metadata(cursor, "fees") + fee
            self._set_metadata(cursor, "wallet_balance", wallet)
            self._set_metadata(cursor, "realized_pnl", total_realized)
            self._set_metadata(cursor, "fees", total_fees)
            cumulative_realized += realized
            cursor.execute(
                """
                INSERT INTO positions(symbol, quantity, entry_price, realized_pnl, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    quantity = excluded.quantity,
                    entry_price = excluded.entry_price,
                    realized_pnl = excluded.realized_pnl,
                    updated_at = excluded.updated_at
                """,
                (
                    normalized_symbol,
                    _text(new_quantity),
                    _text(new_entry),
                    _text(cumulative_realized),
                    _iso(timestamp),
                ),
            )
            cursor.execute(
                """
                INSERT INTO fills(
                    client_order_id, exchange_order_id, symbol, side,
                    requested_quantity, executed_quantity, price, fee_rate, fee,
                    reduce_only, realized_pnl, executed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    exchange_id,
                    normalized_symbol,
                    normalized_side,
                    _text(requested),
                    _text(executed),
                    _text(fill_price),
                    _text(parsed_fee_rate),
                    _text(fee),
                    int(bool(reduce_only)),
                    _text(realized),
                    _iso(timestamp),
                ),
            )
            row = cursor.execute(
                "SELECT * FROM fills WHERE client_order_id = ?", (order_id,)
            ).fetchone()
            return self._fill_from_row(row)

    execute_fill = record_fill

    def get_fill(self, client_order_id: str) -> Optional[LedgerFill]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM fills WHERE client_order_id = ?",
                (str(client_order_id),),
            ).fetchone()
        return self._fill_from_row(row) if row is not None else None

    def get_position(self, symbol: str) -> LedgerPosition:
        normalized = str(symbol or "").strip().upper()
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM positions WHERE symbol = ?", (normalized,)
            ).fetchone()
        if row is None:
            return LedgerPosition(normalized, _ZERO, _ZERO, _ZERO, "")
        return self._position_from_row(row)

    def positions(self) -> Dict[str, LedgerPosition]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM positions WHERE quantity != '0' ORDER BY symbol"
            ).fetchall()
        return {row["symbol"]: self._position_from_row(row) for row in rows}

    def ensure_daily_baseline(
        self,
        mark_prices: Optional[Mapping[str, Any]] = None,
        now: Optional[Any] = None,
    ) -> Decimal:
        timestamp = _utc(now)
        with self._transaction() as cursor:
            return self._ensure_daily_baseline(
                cursor, timestamp, mark_prices, default_to_entry=True
            )

    def snapshot(
        self,
        mark_prices: Optional[Mapping[str, Any]] = None,
        now: Optional[Any] = None,
        mark_price: Optional[Any] = None,
        symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        timestamp = _utc(now)
        prices = dict(mark_prices or {})
        if mark_price is not None:
            selected_symbol = str(symbol or "").strip().upper()
            if not selected_symbol:
                open_positions = self.positions()
                if len(open_positions) != 1:
                    raise ValueError("mark_price 必须与唯一 symbol 或显式 symbol 同时提供")
                selected_symbol = next(iter(open_positions))
            prices[selected_symbol] = mark_price
        with self._transaction() as cursor:
            baseline = self._ensure_daily_baseline(
                cursor, timestamp, prices, default_to_entry=True
            )
            equity, unrealized, rows, marks = self._equity(
                cursor, prices, default_to_entry=True
            )
            initial = self._metadata(cursor, "initial_balance")
            wallet = self._metadata(cursor, "wallet_balance")
            realized = self._metadata(cursor, "realized_pnl")
            fees = self._metadata(cursor, "fees")
        positions = {}
        for row in rows:
            position = self._position_from_row(row)
            positions[position.symbol] = {
                "symbol": position.symbol,
                "side": position.side,
                "quantity": position.quantity,
                "entry_price": position.entry_price,
                "mark_price": marks[position.symbol],
                "unrealized_pnl": position.unrealized_pnl(marks[position.symbol]),
                "realized_pnl": position.realized_pnl,
                "updated_at": position.updated_at,
            }
        drawdown = _ZERO
        if baseline > _ZERO:
            drawdown = max(_ZERO, (baseline - equity) / baseline)
        first_position = next(iter(positions.values()), None)
        return {
            "schema_version": 1,
            "mode": "paper",
            "initial_balance": initial,
            "cash": wallet,
            "wallet_balance": wallet,
            "equity": equity,
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "fees": fees,
            "day_start_equity": baseline,
            "daily_drawdown_pct": drawdown,
            "positions": positions,
            "position": first_position,
            "open_position_count": len(positions),
            "utc_date": timestamp.date().isoformat(),
            "updated_at": _iso(timestamp),
        }

    account_snapshot = snapshot

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "PaperLedger":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


SQLitePaperLedger = PaperLedger


__all__ = [
    "LedgerFill",
    "LedgerPosition",
    "PaperLedger",
    "SQLitePaperLedger",
]
