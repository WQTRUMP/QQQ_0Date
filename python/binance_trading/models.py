"""
[INPUT]: 依赖 dataclasses、Decimal 与 UTC datetime 的精确领域语义
[OUTPUT]: 提供行情、订单意图、持仓、资金与执行结果值对象
[POS]: binance_trading 的 BTCUSDT 线性永续合约契约层；策略信号由 strategy 独立持有
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from enum import Enum
from typing import Any, Dict, Optional


ZERO = Decimal("0")


def decimal_value(value: Any, name: str = "value") -> Decimal:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("%s 必须是有限十进制数" % name) from exc
    if not parsed.is_finite():
        raise ValueError("%s 必须是有限十进制数" % name)
    return parsed


def utc_datetime(value: datetime, name: str = "timestamp") -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("%s 必须是带时区的 UTC datetime" % name)
    normalized = value.astimezone(timezone.utc)
    if value.utcoffset() != normalized.utcoffset():
        raise ValueError("%s 必须使用 UTC" % name)
    return normalized


def datetime_from_millis(value: Any, name: str = "timestamp") -> datetime:
    try:
        millis = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("%s 必须是毫秒时间戳" % name) from exc
    if millis <= 0:
        raise ValueError("%s 必须是正毫秒时间戳" % name)
    return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)


def _symbol(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized or not normalized.isalnum() or len(normalized) > 32:
        raise ValueError("symbol 必须是 1..32 位大写字母数字")
    return normalized


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def entry_side(self) -> str:
        return "BUY" if self is Direction.LONG else "SELL"

    @property
    def exit_side(self) -> str:
        return "SELL" if self is Direction.LONG else "BUY"


@dataclass(frozen=True)
class BookTicker:
    symbol: str
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    event_time: datetime
    transaction_time: datetime
    update_id: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        for field_name in ("bid_price", "bid_quantity", "ask_price", "ask_quantity"):
            object.__setattr__(self, field_name, decimal_value(getattr(self, field_name), field_name))
        object.__setattr__(self, "event_time", utc_datetime(self.event_time, "event_time"))
        object.__setattr__(
            self, "transaction_time", utc_datetime(self.transaction_time, "transaction_time")
        )
        if self.bid_price <= ZERO or self.ask_price <= ZERO or self.bid_price >= self.ask_price:
            raise ValueError("买卖盘必须为正数且 bid < ask")
        if self.bid_quantity < ZERO or self.ask_quantity < ZERO:
            raise ValueError("买卖盘数量不能为负")
        if int(self.update_id) < 0:
            raise ValueError("update_id 不能为负")

    @property
    def midpoint(self) -> Decimal:
        return (self.bid_price + self.ask_price) / Decimal("2")

    @property
    def spread_bps(self) -> Decimal:
        return (self.ask_price - self.bid_price) / self.midpoint * Decimal("10000")


@dataclass(frozen=True)
class MarkPrice:
    symbol: str
    mark_price: Decimal
    index_price: Decimal
    funding_rate: Decimal
    next_funding_time: datetime
    event_time: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        for field_name in ("mark_price", "index_price", "funding_rate"):
            object.__setattr__(self, field_name, decimal_value(getattr(self, field_name), field_name))
        object.__setattr__(
            self, "next_funding_time", utc_datetime(self.next_funding_time, "next_funding_time")
        )
        object.__setattr__(self, "event_time", utc_datetime(self.event_time, "event_time"))
        if self.mark_price <= ZERO or self.index_price <= ZERO:
            raise ValueError("mark/index price 必须为正数")


@dataclass(frozen=True)
class Kline:
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trades: int
    closed: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        object.__setattr__(self, "interval", str(self.interval or "").strip())
        object.__setattr__(self, "open_time", utc_datetime(self.open_time, "open_time"))
        object.__setattr__(self, "close_time", utc_datetime(self.close_time, "close_time"))
        for field_name in ("open", "high", "low", "close", "volume", "quote_volume"):
            object.__setattr__(self, field_name, decimal_value(getattr(self, field_name), field_name))
        if not self.interval or self.close_time <= self.open_time:
            raise ValueError("K 线周期/时间边界无效")
        if min(self.open, self.high, self.low, self.close) <= ZERO:
            raise ValueError("K 线 OHLC 必须为正数")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("K 线 OHLC 结构无效")
        if self.volume < ZERO or self.quote_volume < ZERO or int(self.trades) < 0:
            raise ValueError("K 线成交事实不能为负")

    @property
    def start_time(self) -> int:
        return int(self.open_time.timestamp() * 1000)


@dataclass(frozen=True)
class SymbolRules:
    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
    min_notional: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        for field_name in (
            "tick_size",
            "step_size",
            "min_quantity",
            "max_quantity",
            "min_notional",
        ):
            object.__setattr__(self, field_name, decimal_value(getattr(self, field_name), field_name))
        if self.tick_size <= ZERO or self.step_size <= ZERO or self.min_quantity <= ZERO:
            raise ValueError("tick/step/min quantity 必须为正数")
        if self.max_quantity < self.min_quantity or self.min_notional < ZERO:
            raise ValueError("symbol 数量/名义价值边界无效")

    def floor_quantity(self, value: Decimal) -> Decimal:
        quantity = decimal_value(value, "quantity")
        return (quantity / self.step_size).to_integral_value(rounding=ROUND_FLOOR) * self.step_size

    def price_for_side(self, value: Decimal, side: str) -> Decimal:
        price = decimal_value(value, "price")
        rounding = ROUND_CEILING if str(side).upper() == "BUY" else ROUND_FLOOR
        return (price / self.tick_size).to_integral_value(rounding=rounding) * self.tick_size

    def accepts(self, quantity: Decimal, price: Decimal) -> bool:
        quantity = decimal_value(quantity, "quantity")
        price = decimal_value(price, "price")
        return (
            self.min_quantity <= quantity <= self.max_quantity
            and self.floor_quantity(quantity) == quantity
            and quantity * price >= self.min_notional
        )


@dataclass(frozen=True)
class AccountSnapshot:
    wallet_balance: Decimal
    equity: Decimal
    available_balance: Decimal
    unrealized_pnl: Decimal
    day_start_equity: Decimal
    observed_at: datetime
    healthy: bool = True
    reason: str = ""

    def __post_init__(self) -> None:
        for field_name in (
            "wallet_balance",
            "equity",
            "available_balance",
            "unrealized_pnl",
            "day_start_equity",
        ):
            object.__setattr__(self, field_name, decimal_value(getattr(self, field_name), field_name))
        object.__setattr__(self, "observed_at", utc_datetime(self.observed_at, "observed_at"))
        if self.equity < ZERO or self.wallet_balance < ZERO or self.day_start_equity <= ZERO:
            raise ValueError("资金快照不允许负权益/非正日初基线")

    @property
    def day_drawdown_fraction(self) -> Decimal:
        loss = max(self.day_start_equity - self.equity, ZERO)
        return loss / self.day_start_equity


@dataclass(frozen=True)
class Position:
    symbol: str
    direction: Direction
    quantity: Decimal
    entry_price: Decimal
    mark_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    unrealized_pnl: Decimal
    opened_at: datetime
    signal_id: str
    leverage: int = 1
    margin_type: str = "ISOLATED"
    liquidation_price: Optional[Decimal] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        object.__setattr__(self, "direction", Direction(self.direction))
        for field_name in (
            "quantity",
            "entry_price",
            "mark_price",
            "stop_price",
            "target_price",
            "unrealized_pnl",
        ):
            object.__setattr__(self, field_name, decimal_value(getattr(self, field_name), field_name))
        if self.liquidation_price is not None:
            object.__setattr__(
                self, "liquidation_price", decimal_value(self.liquidation_price, "liquidation_price")
            )
        object.__setattr__(self, "opened_at", utc_datetime(self.opened_at, "opened_at"))
        if self.quantity <= ZERO or min(self.entry_price, self.mark_price) <= ZERO:
            raise ValueError("position quantity/price 必须为正数")
        if int(self.leverage) <= 0 or not self.signal_id:
            raise ValueError("position leverage/signal identity 无效")


@dataclass(frozen=True)
class OrderIntent:
    client_order_id: str
    symbol: str
    direction: Direction
    side: str
    quantity: Decimal
    order_type: str
    limit_price: Optional[Decimal]
    reduce_only: bool
    created_at: datetime
    signal_id: str
    stop_price: Optional[Decimal] = None
    target_price: Optional[Decimal] = None
    reason: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        object.__setattr__(self, "direction", Direction(self.direction))
        object.__setattr__(self, "side", str(self.side).upper())
        object.__setattr__(self, "order_type", str(self.order_type).upper())
        object.__setattr__(self, "quantity", decimal_value(self.quantity, "quantity"))
        for field_name in ("limit_price", "stop_price", "target_price"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, decimal_value(value, field_name))
        object.__setattr__(self, "created_at", utc_datetime(self.created_at, "created_at"))
        if not (1 <= len(self.client_order_id) <= 36) or self.side not in {"BUY", "SELL"}:
            raise ValueError("client_order_id/side 无效")
        if self.quantity <= ZERO or self.order_type not in {"LIMIT", "MARKET"}:
            raise ValueError("order quantity/type 无效")
        if self.order_type == "LIMIT" and (self.limit_price is None or self.limit_price <= ZERO):
            raise ValueError("LIMIT 意图必须携带正限价")
        has_stop = self.stop_price is not None
        has_target = self.target_price is not None
        if has_stop != has_target:
            raise ValueError("保护价格必须成对出现")
        if self.reduce_only and (has_stop or has_target):
            raise ValueError("reduceOnly 意图不能携带入场保护价格")
        if has_stop and has_target:
            if min(self.stop_price, self.target_price) <= ZERO:
                raise ValueError("保护价格必须为正数")
            reference = self.limit_price
            valid_ordering = (
                self.direction is Direction.LONG
                and self.stop_price < (reference or self.target_price) < self.target_price
            ) or (
                self.direction is Direction.SHORT
                and self.target_price < (reference or self.stop_price) < self.stop_price
            )
            if not valid_ordering:
                raise ValueError("保护价格与交易方向不一致")
        expected_side = self.direction.exit_side if self.reduce_only else self.direction.entry_side
        if self.side != expected_side:
            raise ValueError("order side 与 direction/reduceOnly 不一致")


@dataclass(frozen=True)
class ExecutionResult:
    client_order_id: str
    status: str
    order_id: str
    executed_quantity: Decimal
    average_price: Decimal
    observed_at: datetime
    reason: str = ""
    submission_unknown: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "status", str(self.status).upper())
        object.__setattr__(
            self, "executed_quantity", decimal_value(self.executed_quantity, "executed_quantity")
        )
        object.__setattr__(self, "average_price", decimal_value(self.average_price, "average_price"))
        object.__setattr__(self, "observed_at", utc_datetime(self.observed_at, "observed_at"))
        if not self.client_order_id or self.executed_quantity < ZERO or self.average_price < ZERO:
            raise ValueError("execution result identity/quantity/price 无效")


def jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {key: jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    return value


__all__ = [
    "AccountSnapshot",
    "BookTicker",
    "Direction",
    "ExecutionResult",
    "Kline",
    "MarkPrice",
    "OrderIntent",
    "Position",
    "SymbolRules",
    "datetime_from_millis",
    "decimal_value",
    "jsonable",
    "utc_datetime",
]
