"""
[INPUT]: 依赖账户权益、止损价、新鲜双边盘口、UTC 日基线与 Binance symbol filters
[OUTPUT]: 对外提供 RiskManager、RiskDecision、RiskConfig 及按步长向下取整的 fractional base quantity
[POS]: binance_trading 的开仓前失败关闭门；任一资金、盘口、回撤、持仓或交易所事实不完整都返回拒绝
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Iterable, Mapping, Optional


_ZERO = Decimal("0")
_TEN_THOUSAND = Decimal("10000")


def _decimal(value: Any, name: str, positive: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("%s 必须是有限十进制数" % name) from exc
    if not parsed.is_finite() or (positive and parsed <= _ZERO):
        suffix = "有限正数" if positive else "有限十进制数"
        raise ValueError("%s 必须是%s" % (name, suffix))
    return parsed


def _field(value: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(value, Mapping) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("盘口时间必须显式带时区")
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float, Decimal)):
        number = Decimal(str(value))
        seconds = number / Decimal("1000") if abs(number) >= Decimal("1e11") else number
        return datetime.fromtimestamp(float(seconds), tz=timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("盘口时间必须显式带时区")
        return parsed.astimezone(timezone.utc)
    raise ValueError("缺失可审计的盘口时间")


def floor_to_step(quantity: Any, step: Any) -> Decimal:
    parsed_quantity = _decimal(quantity, "quantity")
    parsed_step = _decimal(step, "quantity_step", positive=True)
    if parsed_quantity <= _ZERO:
        return _ZERO
    units = (parsed_quantity / parsed_step).to_integral_value(rounding=ROUND_DOWN)
    return units * parsed_step


@dataclass(frozen=True)
class ExchangeFilters:
    symbol: str
    price_tick: Decimal
    quantity_step: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
    min_notional: Decimal


@dataclass(frozen=True)
class RiskConfig:
    risk_percent: Decimal = Decimal("0.01")
    max_notional_percent: Decimal = Decimal("0.25")
    max_spread_bps: Decimal = Decimal("10")
    book_ttl_seconds: Decimal = Decimal("2")
    max_daily_drawdown_percent: Decimal = Decimal("0.05")
    max_open_positions: int = 1
    max_notional: Optional[Decimal] = None
    future_tolerance_seconds: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        percent_fields = (
            ("risk_percent", self.risk_percent),
            ("max_notional_percent", self.max_notional_percent),
            ("max_daily_drawdown_percent", self.max_daily_drawdown_percent),
        )
        for name, value in percent_fields:
            parsed = _decimal(value, name, positive=True)
            if parsed > Decimal("1"):
                raise ValueError("%s 必须使用 0..1 比例" % name)
            object.__setattr__(self, name, parsed)
        for name in ("max_spread_bps", "book_ttl_seconds"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name, positive=True))
        tolerance = _decimal(self.future_tolerance_seconds, "future_tolerance_seconds")
        if tolerance < _ZERO:
            raise ValueError("future_tolerance_seconds 不能为负")
        object.__setattr__(self, "future_tolerance_seconds", tolerance)
        if int(self.max_open_positions) <= 0:
            raise ValueError("max_open_positions 必须大于 0")
        object.__setattr__(self, "max_open_positions", int(self.max_open_positions))
        if self.max_notional is not None:
            object.__setattr__(
                self,
                "max_notional",
                _decimal(self.max_notional, "max_notional", positive=True),
            )


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    quantity: Decimal
    reason: str
    risk_budget: Decimal = _ZERO
    stop_distance: Decimal = _ZERO
    notional: Decimal = _ZERO
    spread_bps: Optional[Decimal] = None
    daily_drawdown_percent: Optional[Decimal] = None

    @property
    def approved(self) -> bool:
        return self.allowed


class RiskManager:
    """Size once from durable account facts; all malformed inputs reject safely."""

    def __init__(self, config: Optional[Any] = None, **overrides: Any) -> None:
        if config is None:
            self.config = RiskConfig(**overrides)
            return
        if isinstance(config, RiskConfig) and not overrides:
            self.config = config
            return
        values = {
            "risk_percent": _field(
                config,
                "risk_percent",
                "risk_per_trade",
                "risk_fraction",
                "risk_per_trade_fraction",
            ),
            "max_notional_percent": _field(
                config,
                "max_notional_percent",
                "max_notional_pct",
                "max_notional_fraction",
            ),
            "max_spread_bps": _field(config, "max_spread_bps"),
            "book_ttl_seconds": _field(
                config,
                "book_ttl_seconds",
                "book_ttl_sec",
                "quote_ttl_seconds",
                "max_book_age_seconds",
            ),
            "max_daily_drawdown_percent": _field(
                config,
                "max_daily_drawdown_percent",
                "max_daily_drawdown_pct",
                "daily_drawdown_limit",
                "max_daily_loss_fraction",
            ),
            "max_open_positions": _field(config, "max_open_positions", default=1),
            "max_notional": _field(config, "max_notional"),
            "future_tolerance_seconds": _field(
                config, "future_tolerance_seconds", default=Decimal("1")
            ),
        }
        values.update(overrides)
        required = (
            "risk_percent",
            "max_notional_percent",
            "max_spread_bps",
            "book_ttl_seconds",
            "max_daily_drawdown_percent",
        )
        missing = [name for name in required if values.get(name) is None]
        if missing:
            raise ValueError("风控配置缺失: %s" % ", ".join(missing))
        self.config = RiskConfig(**values)

    def _deny(
        self,
        reason: str,
        risk_budget: Decimal = _ZERO,
        stop_distance: Decimal = _ZERO,
        spread_bps: Optional[Decimal] = None,
        drawdown: Optional[Decimal] = None,
    ) -> RiskDecision:
        return RiskDecision(
            allowed=False,
            quantity=_ZERO,
            reason=reason,
            risk_budget=risk_budget,
            stop_distance=stop_distance,
            spread_bps=spread_bps,
            daily_drawdown_percent=drawdown,
        )

    def evaluate(
        self,
        equity: Any,
        entry_price: Any,
        stop_price: Any,
        book: Any,
        exchange_filters: Optional[Any] = None,
        day_start_equity: Any = None,
        open_positions: Any = 0,
        now: Optional[datetime] = None,
        **aliases: Any
    ) -> RiskDecision:
        """Evaluate every entry gate and return a base-asset quantity."""
        if exchange_filters is None:
            exchange_filters = aliases.get("rules") or aliases.get("filters")
        if day_start_equity is None:
            day_start_equity = aliases.get("daily_baseline")
        try:
            parsed_equity = _decimal(equity, "equity", positive=True)
            entry = _decimal(entry_price, "entry_price", positive=True)
            stop = _decimal(stop_price, "stop_price", positive=True)
        except ValueError:
            return self._deny("INVALID_ACCOUNT_OR_PRICE")
        stop_distance = abs(entry - stop)
        risk_budget = parsed_equity * self.config.risk_percent
        if stop_distance <= _ZERO:
            return self._deny("INVALID_STOP_DISTANCE", risk_budget=risk_budget)

        try:
            baseline = _decimal(day_start_equity, "day_start_equity", positive=True)
        except ValueError:
            return self._deny(
                "MISSING_DAILY_BASELINE",
                risk_budget=risk_budget,
                stop_distance=stop_distance,
            )
        drawdown = max(_ZERO, (baseline - parsed_equity) / baseline)
        if drawdown >= self.config.max_daily_drawdown_percent:
            return self._deny(
                "DAILY_DRAWDOWN_LIMIT",
                risk_budget,
                stop_distance,
                drawdown=drawdown,
            )

        try:
            if isinstance(open_positions, int):
                position_count = open_positions
            elif isinstance(open_positions, Mapping):
                position_count = sum(
                    1 for value in open_positions.values() if _decimal(value, "position") != 0
                )
            elif isinstance(open_positions, Iterable) and not isinstance(
                open_positions, (str, bytes)
            ):
                position_count = len(list(open_positions))
            else:
                position_count = int(open_positions)
        except (TypeError, ValueError):
            return self._deny("UNKNOWN_POSITION_STATE", risk_budget, stop_distance)
        if position_count < 0:
            return self._deny("UNKNOWN_POSITION_STATE", risk_budget, stop_distance)
        if position_count >= self.config.max_open_positions:
            return self._deny("POSITION_LIMIT", risk_budget, stop_distance)

        if book is None:
            return self._deny("MISSING_BOOK", risk_budget, stop_distance)
        try:
            bid = _decimal(_field(book, "bid_price", "bid"), "bid", positive=True)
            ask = _decimal(_field(book, "ask_price", "ask"), "ask", positive=True)
            bid_qty = _decimal(
                _field(book, "bid_qty", "bid_quantity"), "bid_qty", positive=True
            )
            ask_qty = _decimal(
                _field(book, "ask_qty", "ask_quantity"), "ask_qty", positive=True
            )
            del bid_qty, ask_qty
            observed_at = _utc_datetime(
                _field(book, "event_time", "observed_at", "timestamp")
            )
        except (ValueError, TypeError, OverflowError):
            return self._deny("INVALID_BOOK", risk_budget, stop_distance)
        if ask <= bid:
            return self._deny("CROSSED_BOOK", risk_budget, stop_distance)
        current_time = now or datetime.now(timezone.utc)
        if current_time.tzinfo is None:
            return self._deny("INVALID_CLOCK", risk_budget, stop_distance)
        age = Decimal(str((current_time.astimezone(timezone.utc) - observed_at).total_seconds()))
        if age > self.config.book_ttl_seconds or age < -self.config.future_tolerance_seconds:
            return self._deny("STALE_BOOK", risk_budget, stop_distance)
        midpoint = (bid + ask) / Decimal("2")
        spread_bps = (ask - bid) / midpoint * _TEN_THOUSAND
        if spread_bps > self.config.max_spread_bps:
            return self._deny(
                "SPREAD_LIMIT", risk_budget, stop_distance, spread_bps=spread_bps
            )

        if exchange_filters is None:
            return self._deny(
                "MISSING_EXCHANGE_FILTERS",
                risk_budget,
                stop_distance,
                spread_bps=spread_bps,
            )
        try:
            step = _decimal(
                _field(exchange_filters, "quantity_step", "step_size"),
                "quantity_step",
                positive=True,
            )
            minimum = _decimal(
                _field(exchange_filters, "min_quantity", "min_qty"),
                "min_quantity",
                positive=True,
            )
            maximum = _decimal(
                _field(exchange_filters, "max_quantity", "max_qty"),
                "max_quantity",
                positive=True,
            )
            min_notional = _decimal(
                _field(exchange_filters, "min_notional"),
                "min_notional",
                positive=True,
            )
            price_tick = _decimal(
                _field(exchange_filters, "price_tick", "tick_size"),
                "price_tick",
                positive=True,
            )
            del price_tick
        except ValueError:
            return self._deny(
                "INVALID_EXCHANGE_FILTERS",
                risk_budget,
                stop_distance,
                spread_bps=spread_bps,
            )
        if maximum < minimum:
            return self._deny(
                "INVALID_EXCHANGE_FILTERS",
                risk_budget,
                stop_distance,
                spread_bps=spread_bps,
            )

        notional_cap = parsed_equity * self.config.max_notional_percent
        if self.config.max_notional is not None:
            notional_cap = min(notional_cap, self.config.max_notional)
        quantity = min(risk_budget / stop_distance, notional_cap / entry, maximum)
        quantity = floor_to_step(quantity, step)
        notional = quantity * entry
        if quantity < minimum:
            return self._deny(
                "BELOW_MIN_QUANTITY",
                risk_budget,
                stop_distance,
                spread_bps,
                drawdown,
            )
        if notional < min_notional:
            return self._deny(
                "BELOW_MIN_NOTIONAL",
                risk_budget,
                stop_distance,
                spread_bps,
                drawdown,
            )
        if quantity <= _ZERO or notional > notional_cap:
            return self._deny(
                "NOTIONAL_LIMIT",
                risk_budget,
                stop_distance,
                spread_bps,
                drawdown,
            )
        return RiskDecision(
            allowed=True,
            quantity=quantity,
            reason="APPROVED",
            risk_budget=risk_budget,
            stop_distance=stop_distance,
            notional=notional,
            spread_bps=spread_bps,
            daily_drawdown_percent=drawdown,
        )

    size_position = evaluate


def calculate_order_quantity(**kwargs: Any) -> RiskDecision:
    config = kwargs.pop("config", None)
    return RiskManager(config=config).evaluate(**kwargs)


__all__ = [
    "ExchangeFilters",
    "RiskConfig",
    "RiskDecision",
    "RiskManager",
    "calculate_order_quantity",
    "floor_to_step",
]
