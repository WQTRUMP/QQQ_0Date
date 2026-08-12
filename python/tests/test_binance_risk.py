"""
[INPUT]: 依赖 binance_trading.risk 的资金风险定额和交易所下单门
[OUTPUT]: 验证 fractional quantity、notional cap、filters 及全部失败关闭条件
[POS]: tests 的 Binance 风控回归；锁定缺失或陈旧事实绝不可退化为放行
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from python.binance_trading.risk import ExchangeFilters, RiskConfig, RiskManager


NOW = datetime(2026, 8, 11, 10, 0, tzinfo=timezone.utc)


def book(observed_at=NOW, bid="99.99", ask="100.01"):
    return SimpleNamespace(
        bid_price=Decimal(bid),
        bid_qty=Decimal("3"),
        ask_price=Decimal(ask),
        ask_qty=Decimal("4"),
        event_time=observed_at,
    )


RULES = ExchangeFilters(
    symbol="BTCUSDT",
    price_tick=Decimal("0.01"),
    quantity_step=Decimal("0.001"),
    min_quantity=Decimal("0.001"),
    max_quantity=Decimal("1000"),
    min_notional=Decimal("5"),
)


class BinanceRiskTests(unittest.TestCase):
    def setUp(self):
        self.manager = RiskManager(
            RiskConfig(
                risk_percent=Decimal("0.01"),
                max_notional_percent=Decimal("0.10"),
                max_spread_bps=Decimal("5"),
                book_ttl_seconds=Decimal("2"),
                max_daily_drawdown_percent=Decimal("0.05"),
                max_open_positions=1,
            )
        )

    def evaluate(self, **changes):
        values = dict(
            equity="10000",
            entry_price="100",
            stop_price="95",
            book=book(),
            exchange_filters=RULES,
            day_start_equity="10000",
            open_positions=0,
            now=NOW,
        )
        values.update(changes)
        return self.manager.evaluate(**values)

    def test_risk_size_is_capped_by_notional_and_keeps_fractional_step(self):
        decision = self.evaluate()

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.quantity, Decimal("10.000"))
        self.assertEqual(decision.risk_budget, Decimal("100.00"))
        self.assertEqual(decision.notional, Decimal("1000.000"))
        self.assertLessEqual(
            decision.quantity * decision.stop_distance, decision.risk_budget
        )

    def test_quantity_rounds_down_never_above_risk_budget(self):
        decision = self.evaluate(
            equity="1000",
            day_start_equity="1000",
            entry_price="17",
            stop_price="13",
            book=book(bid="16.999", ask="17.001"),
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.quantity, Decimal("2.500"))
        self.assertEqual(decision.quantity * decision.stop_distance, Decimal("10.000"))

    def test_stale_or_wide_or_incomplete_book_fails_closed(self):
        stale = self.evaluate(book=book(NOW - timedelta(seconds=3)))
        wide = self.evaluate(book=book(bid="99", ask="101"))
        incomplete = self.evaluate(
            book=SimpleNamespace(
                bid_price="99.99", ask_price="100.01", event_time=NOW
            )
        )

        self.assertEqual(stale.reason, "STALE_BOOK")
        self.assertEqual(wide.reason, "SPREAD_LIMIT")
        self.assertEqual(incomplete.reason, "INVALID_BOOK")
        self.assertFalse(any(item.allowed for item in (stale, wide, incomplete)))

    def test_drawdown_and_position_gates_fail_closed(self):
        drawdown = self.evaluate(equity="9500", day_start_equity="10000")
        position = self.evaluate(open_positions={"BTCUSDT": "0.1"})
        unknown = self.evaluate(open_positions=None)

        self.assertEqual(drawdown.reason, "DAILY_DRAWDOWN_LIMIT")
        self.assertEqual(position.reason, "POSITION_LIMIT")
        self.assertEqual(unknown.reason, "UNKNOWN_POSITION_STATE")

    def test_exchange_minimums_are_enforced_after_rounding(self):
        strict = ExchangeFilters(
            symbol="BTCUSDT",
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.1"),
            min_quantity=Decimal("0.1"),
            max_quantity=Decimal("10"),
            min_notional=Decimal("50"),
        )
        decision = self.evaluate(
            equity="100",
            day_start_equity="100",
            entry_price="100",
            stop_price="99",
            exchange_filters=strict,
        )

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "BELOW_MIN_NOTIONAL")

    def test_missing_baseline_and_filters_fail_closed(self):
        self.assertEqual(
            self.evaluate(day_start_equity=None).reason, "MISSING_DAILY_BASELINE"
        )
        self.assertEqual(
            self.evaluate(exchange_filters=None).reason, "MISSING_EXCHANGE_FILTERS"
        )

    def test_binance_config_field_names_map_without_copying_config(self):
        config = SimpleNamespace(
            risk_per_trade_fraction=Decimal("0.01"),
            max_notional_fraction=Decimal("0.10"),
            max_daily_loss_fraction=Decimal("0.05"),
            max_spread_bps=Decimal("5"),
            max_book_age_seconds=Decimal("2"),
        )

        manager = RiskManager(config)
        decision = manager.evaluate(
            equity="10000",
            entry_price="100",
            stop_price="95",
            book=book(),
            exchange_filters=RULES,
            day_start_equity="10000",
            open_positions=0,
            now=NOW,
        )

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.quantity, Decimal("10.000"))


if __name__ == "__main__":
    unittest.main()
