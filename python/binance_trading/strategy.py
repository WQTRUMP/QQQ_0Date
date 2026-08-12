"""
[INPUT]: 依赖 models.Kline 的 BTCUSDT 闭合 1m OHLC 与 Direction 方向契约
[OUTPUT]: 对外提供固定 EMA5/EMA13 + ATR14 的 OneMinuteEmaStrategy、TradeSignal 与 StrategySnapshot
[POS]: binance_trading 的唯一入场策略；每分钟只判断一次交叉，ATR 仅用于风险距离，不叠加其他指标
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Optional, Tuple

from .models import Direction, Kline, decimal_value


INTERVAL = "1m"
INTERVAL_MS = 60_000
FAST_EMA_PERIOD = 5
SLOW_EMA_PERIOD = 13
ATR_PERIOD = 14
STRATEGY_ID = "ema_cross_5_13_atr14_1m_v1"
ZERO = Decimal("0")


@dataclass(frozen=True)
class TradeSignal:
    symbol: str
    direction: Direction
    bar_start_time: int
    atr: Decimal
    interval: str = INTERVAL
    strategy_id: str = STRATEGY_ID

    def __post_init__(self) -> None:
        symbol = str(self.symbol or "").upper()
        if symbol != "BTCUSDT":
            raise ValueError("1m 策略只接受 BTCUSDT")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "direction", Direction(self.direction))
        object.__setattr__(self, "atr", decimal_value(self.atr, "signal atr"))
        if self.interval != INTERVAL or int(self.bar_start_time) < 0:
            raise ValueError("信号必须来自闭合 1m K 线")
        if self.atr <= ZERO or self.strategy_id != STRATEGY_ID:
            raise ValueError("1m 信号 ATR 或策略身份无效")

    @property
    def signal_id(self) -> str:
        return "%s:%s:%d:%s" % (
            self.strategy_id,
            self.symbol,
            self.bar_start_time,
            self.direction.value[0],
        )

@dataclass(frozen=True)
class StrategySnapshot:
    symbol: str
    interval: str
    last_bar_start_time: Optional[int]
    contiguous_bars: int
    fast_ema: Optional[Decimal]
    slow_ema: Optional[Decimal]
    atr: Optional[Decimal]
    ready: bool
    gap_resets: int


class _EMA:
    def __init__(self, period: int) -> None:
        self.period = period
        self.alpha = Decimal("2") / Decimal(period + 1)
        self.seed = []  # type: list[Decimal]
        self.value = None  # type: Optional[Decimal]

    def update(self, price: Decimal) -> Optional[Decimal]:
        if self.value is None:
            self.seed.append(price)
            if len(self.seed) == self.period:
                self.value = sum(self.seed, ZERO) / Decimal(self.period)
                self.seed = []
            return self.value
        self.value += self.alpha * (price - self.value)
        return self.value


class _WilderATR:
    def __init__(self) -> None:
        self.ranges = []  # type: list[Decimal]
        self.value = None  # type: Optional[Decimal]
        self.previous_close = None  # type: Optional[Decimal]

    def update(self, high: Decimal, low: Decimal, close: Decimal) -> Optional[Decimal]:
        true_range = high - low
        if self.previous_close is not None:
            true_range = max(
                true_range,
                abs(high - self.previous_close),
                abs(low - self.previous_close),
            )
        self.previous_close = close
        if self.value is None:
            self.ranges.append(true_range)
            if len(self.ranges) == ATR_PERIOD:
                self.value = sum(self.ranges, ZERO) / Decimal(ATR_PERIOD)
                self.ranges = []
            return self.value
        self.value = (
            self.value * Decimal(ATR_PERIOD - 1) + true_range
        ) / Decimal(ATR_PERIOD)
        return self.value


class OneMinuteEmaStrategy:
    """固定参数的闭合 1m EMA 交叉；每次交叉只产生一个方向信号。"""

    def __init__(self, symbol: str = "BTCUSDT") -> None:
        self.symbol = str(symbol or "").upper()
        if self.symbol != "BTCUSDT":
            raise ValueError("1m 策略只接受 BTCUSDT")
        self._gap_resets = 0
        self._reset_series()

    def _reset_series(self) -> None:
        self._fast = _EMA(FAST_EMA_PERIOD)
        self._slow = _EMA(SLOW_EMA_PERIOD)
        self._atr = _WilderATR()
        self._last_start = None  # type: Optional[int]
        self._last_fingerprint = None  # type: Optional[Tuple[object, ...]]
        self._previous_difference = None  # type: Optional[Decimal]
        self._contiguous_bars = 0

    @property
    def ready(self) -> bool:
        return (
            self._fast.value is not None
            and self._slow.value is not None
            and self._atr.value is not None
        )

    def hydrate(self, bars: Iterable[Kline]) -> StrategySnapshot:
        self._reset_series()
        for bar in bars:
            self.update(bar, historical=True)
        return self.snapshot()

    def update(self, bar: Kline, historical: bool = False) -> Optional[TradeSignal]:
        if not isinstance(bar, Kline):
            raise TypeError("策略只接受已验证 Kline")
        if not bar.closed:
            return None
        if bar.symbol != self.symbol or bar.interval != INTERVAL:
            raise ValueError("策略只接受当前 symbol 的闭合 1m K 线")

        start_time = bar.start_time
        fingerprint = (
            bar.open_time,
            bar.close_time,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.quote_volume,
            bar.trades,
        )
        if self._last_start is not None:
            if start_time == self._last_start:
                if fingerprint != self._last_fingerprint:
                    raise ValueError("同一闭柱时间出现冲突 K 线")
                return None
            if start_time < self._last_start:
                return None
            if start_time != self._last_start + INTERVAL_MS:
                self._gap_resets += 1
                self._reset_series()

        previous = self._previous_difference
        fast = self._fast.update(bar.close)
        slow = self._slow.update(bar.close)
        atr = self._atr.update(bar.high, bar.low, bar.close)
        self._last_start = start_time
        self._last_fingerprint = fingerprint
        self._contiguous_bars += 1
        if fast is None or slow is None or atr is None:
            self._previous_difference = None
            return None

        difference = fast - slow
        self._previous_difference = difference
        if historical or previous is None:
            return None
        if previous <= ZERO < difference:
            direction = Direction.LONG
        elif previous >= ZERO > difference:
            direction = Direction.SHORT
        else:
            return None
        return TradeSignal(
            symbol=self.symbol,
            direction=direction,
            bar_start_time=start_time,
            atr=atr,
        )

    def snapshot(self) -> StrategySnapshot:
        return StrategySnapshot(
            symbol=self.symbol,
            interval=INTERVAL,
            last_bar_start_time=self._last_start,
            contiguous_bars=self._contiguous_bars,
            fast_ema=self._fast.value,
            slow_ema=self._slow.value,
            atr=self._atr.value,
            ready=self.ready,
            gap_resets=self._gap_resets,
        )


__all__ = [
    "ATR_PERIOD",
    "FAST_EMA_PERIOD",
    "INTERVAL",
    "OneMinuteEmaStrategy",
    "SLOW_EMA_PERIOD",
    "STRATEGY_ID",
    "StrategySnapshot",
    "TradeSignal",
]
