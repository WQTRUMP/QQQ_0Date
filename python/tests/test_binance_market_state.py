"""
[INPUT]: 依赖 market_state 单调投影与 BookTicker、MarkPrice、Position 领域值对象
[OUTPUT]: 验证旧帧忽略、同身份冲突拒绝，以及仅新 mark 更新持仓估值
[POS]: python/tests 的行情因果单元回归，补充 runtime 真实保护路径的组合测试
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from python.binance_trading.market_state import (
    ACCEPTED,
    CONFLICT,
    IGNORED,
    apply_book_update,
    apply_mark_update,
)
from python.binance_trading.models import BookTicker, Direction, MarkPrice, Position


NOW = datetime(2026, 8, 12, 8, 0, tzinfo=timezone.utc)


class BinanceMarketStateTests(unittest.TestCase):
    def test_book_update_id_is_strictly_monotonic_and_conflicts_fail_closed(self) -> None:
        books = {}
        fresh = BookTicker(
            symbol="BTCUSDT",
            bid_price="60000",
            bid_quantity="1",
            ask_price="60001",
            ask_quantity="1",
            event_time=NOW,
            transaction_time=NOW,
            update_id=10,
        )
        stale = BookTicker(
            symbol="BTCUSDT",
            bid_price="59000",
            bid_quantity="1",
            ask_price="59001",
            ask_quantity="1",
            event_time=NOW - timedelta(seconds=1),
            transaction_time=NOW - timedelta(seconds=1),
            update_id=9,
        )

        self.assertEqual(apply_book_update(books, fresh), ACCEPTED)
        self.assertEqual(apply_book_update(books, stale), IGNORED)
        self.assertEqual(apply_book_update(books, fresh), IGNORED)
        conflicting = BookTicker(
            symbol="BTCUSDT",
            bid_price="59999",
            bid_quantity="1",
            ask_price="60001",
            ask_quantity="1",
            event_time=NOW,
            transaction_time=NOW,
            update_id=10,
        )
        self.assertEqual(apply_book_update(books, conflicting), CONFLICT)
        self.assertEqual(books["BTCUSDT"], fresh)

    def test_mark_time_is_monotonic_and_only_accepted_mark_revalues_position(self) -> None:
        marks = {}
        positions = {
            "BTCUSDT": Position(
                symbol="BTCUSDT",
                direction=Direction.LONG,
                quantity="0.01",
                entry_price="59000",
                mark_price="59000",
                stop_price="58000",
                target_price="61000",
                unrealized_pnl="0",
                opened_at=NOW,
                signal_id="signal-1",
            )
        }
        fresh = MarkPrice(
            symbol="BTCUSDT",
            mark_price="60000",
            index_price="59999",
            funding_rate="0.0001",
            next_funding_time=NOW + timedelta(hours=4),
            event_time=NOW,
        )
        stale = MarkPrice(
            symbol="BTCUSDT",
            mark_price="57000",
            index_price="57000",
            funding_rate="0",
            next_funding_time=NOW + timedelta(hours=2),
            event_time=NOW - timedelta(hours=2),
        )

        self.assertEqual(apply_mark_update(marks, positions, fresh), ACCEPTED)
        self.assertEqual(positions["BTCUSDT"].unrealized_pnl, Decimal("10"))
        self.assertEqual(apply_mark_update(marks, positions, stale), IGNORED)
        self.assertEqual(positions["BTCUSDT"].mark_price, Decimal("60000"))
        conflicting = MarkPrice(
            symbol="BTCUSDT",
            mark_price="58000",
            index_price="58000",
            funding_rate=fresh.funding_rate,
            next_funding_time=fresh.next_funding_time,
            event_time=fresh.event_time,
        )
        self.assertEqual(apply_mark_update(marks, positions, conflicting), CONFLICT)
        self.assertEqual(marks["BTCUSDT"], fresh)


if __name__ == "__main__":
    unittest.main()
