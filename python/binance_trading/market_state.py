"""
[INPUT]: 依赖线性永续 BookTicker、MarkPrice、Position 与 Direction 值对象
[OUTPUT]: 提供 apply_book_update 与 apply_mark_update，以严格单调规则更新运行时行情和持仓估值
[POS]: binance_trading 的内存行情状态边界；把接收、忽略、冲突三态从交易编排器剥离，阻止旧帧驱动保护动作
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import MutableMapping

from .models import BookTicker, Direction, MarkPrice, Position


ACCEPTED = "accepted"
CONFLICT = "conflict"
IGNORED = "ignored"


def apply_book_update(
    books: MutableMapping[str, BookTicker], event: BookTicker
) -> str:
    previous = books.get(event.symbol)
    if previous is None or event.update_id > previous.update_id:
        books[event.symbol] = event
        return ACCEPTED
    if event.update_id == previous.update_id and event != previous:
        return CONFLICT
    return IGNORED


def apply_mark_update(
    marks: MutableMapping[str, MarkPrice],
    positions: MutableMapping[str, Position],
    event: MarkPrice,
) -> str:
    previous = marks.get(event.symbol)
    if previous is not None and event.event_time < previous.event_time:
        return IGNORED
    if previous is not None and event.event_time == previous.event_time:
        return IGNORED if event == previous else CONFLICT
    marks[event.symbol] = event
    position = positions.get(event.symbol)
    if position is not None:
        sign = Decimal("1") if position.direction is Direction.LONG else Decimal("-1")
        positions[event.symbol] = replace(
            position,
            mark_price=event.mark_price,
            unrealized_pnl=(event.mark_price - position.entry_price)
            * position.quantity
            * sign,
        )
    return ACCEPTED


__all__ = [
    "ACCEPTED",
    "CONFLICT",
    "IGNORED",
    "apply_book_update",
    "apply_mark_update",
]
