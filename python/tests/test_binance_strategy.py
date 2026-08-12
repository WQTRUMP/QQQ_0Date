"""
[INPUT]: 依赖固定参数 OneMinuteEmaStrategy 与已验证 BTCUSDT Kline 值对象
[OUTPUT]: 验证 1m EMA5/13 交叉、ATR14、历史静默、闭柱幂等、冲突拒绝与断档重建
[POS]: python/tests 的唯一入场策略回归；锁死分钟级产品边界，禁止现场增加指标或切换周期
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from python.binance_trading.models import Kline
from python.binance_trading.strategy import OneMinuteEmaStrategy


BASE = datetime(2026, 8, 11, 0, 0, tzinfo=timezone.utc)


def candle(
    index: int,
    close: object,
    *,
    high: object = None,
    low: object = None,
    closed: bool = True,
    interval: str = "1m",
) -> Kline:
    price = Decimal(str(close))
    opened = BASE + timedelta(minutes=index)
    return Kline(
        symbol="BTCUSDT",
        interval=interval,
        open_time=opened,
        close_time=opened + timedelta(seconds=59, milliseconds=999),
        open=price,
        high=Decimal(str(high)) if high is not None else price + Decimal("1"),
        low=Decimal(str(low)) if low is not None else price - Decimal("1"),
        close=price,
        volume=Decimal("1"),
        quote_volume=price,
        trades=1,
        closed=closed,
    )


def descending_history() -> list[Kline]:
    return [candle(index, 114 - index) for index in range(14)]


class BinanceStrategyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.strategy = OneMinuteEmaStrategy()

    def test_fixed_one_minute_crossovers_are_directional(self) -> None:
        snapshot = self.strategy.hydrate(descending_history())
        self.assertTrue(snapshot.ready)

        long_signal = self.strategy.update(candle(14, 130))
        short_signal = self.strategy.update(candle(15, 80))

        self.assertEqual(long_signal.direction.value, "LONG")
        self.assertEqual(long_signal.interval, "1m")
        self.assertGreater(long_signal.atr, 0)
        self.assertEqual(short_signal.direction.value, "SHORT")

    def test_open_and_duplicate_closed_bar_do_not_mutate_state(self) -> None:
        self.strategy.hydrate(descending_history())
        before = self.strategy.snapshot()

        self.assertIsNone(self.strategy.update(candle(14, 130, closed=False)))
        self.assertEqual(self.strategy.snapshot(), before)
        signal = self.strategy.update(candle(14, 130))
        after = self.strategy.snapshot()
        self.assertIsNotNone(signal)
        self.assertIsNone(self.strategy.update(candle(14, 130)))
        self.assertEqual(self.strategy.snapshot(), after)

    def test_history_hydration_never_replays_crossovers(self) -> None:
        bars = descending_history() + [candle(14, 130), candle(15, 80)]

        snapshot = self.strategy.hydrate(bars)

        self.assertTrue(snapshot.ready)
        self.assertEqual(snapshot.last_bar_start_time, candle(15, 80).start_time)
        self.assertIsNone(self.strategy.update(candle(16, 79)))

    def test_gap_discards_indicator_history(self) -> None:
        self.strategy.hydrate(descending_history())

        self.assertIsNone(self.strategy.update(candle(15, 100)))
        self.assertFalse(self.strategy.ready)
        for index in range(16, 29):
            self.strategy.update(candle(index, 100))

        snapshot = self.strategy.snapshot()
        self.assertTrue(snapshot.ready)
        self.assertEqual(snapshot.contiguous_bars, 14)
        self.assertEqual(snapshot.gap_resets, 1)

    def test_wilder_atr_uses_sma_seed_then_recursive_smoothing(self) -> None:
        bars = [candle(index, 100, high=101, low=99) for index in range(14)]
        self.strategy.hydrate(bars)
        self.assertEqual(self.strategy.snapshot().atr, Decimal("2"))

        self.strategy.update(candle(14, 100, high=105, low=95), historical=True)

        self.assertEqual(
            self.strategy.snapshot().atr,
            (Decimal("2") * Decimal("13") + Decimal("10")) / Decimal("14"),
        )

    def test_conflicting_duplicate_is_rejected(self) -> None:
        first = candle(0, 100)
        self.strategy.update(first)
        with self.assertRaisesRegex(ValueError, "冲突"):
            self.strategy.update(candle(0, 101))

    def test_non_one_minute_kline_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "1m"):
            self.strategy.update(candle(0, 100, interval="5m"))


if __name__ == "__main__":
    unittest.main()
