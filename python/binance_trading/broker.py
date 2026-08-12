"""
[INPUT]: 依赖 OrderIntent、PaperLedger、可注入成交价与 Binance Demo REST 客户端的最小下单/查单能力
[OUTPUT]: 对外提供 PaperBroker、BinanceTestnetBroker、clientOrderId 规范化、可安全重试的明确失败标记与 ExecutionResult
[POS]: binance_trading 的执行防腐层；统一单向持仓 reduce-only 语义，并将 -1006/-1007/408/未知 5xx/timeout 恢复限定为查单而非重投
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import hashlib
import inspect
import re
import socket
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Optional

from python.binance_trading.exchange import BinanceSubmissionUnknown, is_definitive_order_503
from python.binance_trading.ledger import LedgerFill, PaperLedger
from python.binance_trading.models import Direction, ExecutionResult, OrderIntent


_ZERO = Decimal("0")
RETRYABLE_SERVER_FAILURE = "RETRYABLE_SERVER_FAILURE"
_CLIENT_ORDER_ID = re.compile(r"[^.A-Za-z0-9_:/-]+")
_TERMINAL_ORDER_STATUSES = frozenset(
    ("FILLED", "CANCELED", "EXPIRED", "EXPIRED_IN_MATCH", "REJECTED")
)


def _decimal(value: Any, name: str, positive: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("%s 必须是有限十进制数" % name) from exc
    if not parsed.is_finite() or (positive and parsed <= _ZERO):
        raise ValueError("%s 必须是有限%s" % (name, "正数" if positive else "数"))
    return parsed


def _field(value: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(value, Mapping) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_datetime(value: Any, default: Optional[datetime] = None) -> datetime:
    if value is None:
        return default or _utc_now()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("执行时间必须显式带时区")
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float, Decimal)):
        number = Decimal(str(value))
        seconds = number / Decimal("1000") if abs(number) >= Decimal("1e11") else number
        return datetime.fromtimestamp(float(seconds), tz=timezone.utc)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("执行时间必须显式带时区")
        return parsed.astimezone(timezone.utc)
    raise ValueError("执行时间无效")


def _decimal_text(value: Decimal) -> str:
    if value == _ZERO:
        return "0"
    return format(value.normalize(), "f")


def normalize_client_order_id(value: Any, prefix: str = "bt") -> str:
    """Build a stable Binance-safe id no longer than the wire limit of 36."""
    raw = str(value or "").strip()
    if not raw:
        raw = uuid.uuid4().hex
    sanitized = _CLIENT_ORDER_ID.sub("-", raw).strip("-") or uuid.uuid4().hex
    if len(sanitized) <= 36:
        return sanitized
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:10]
    normalized_prefix = _CLIENT_ORDER_ID.sub("-", str(prefix or "bt")).strip("-") or "bt"
    room = 36 - len(normalized_prefix) - len(digest) - 2
    if room < 1:
        normalized_prefix = "bt"
        room = 36 - len(normalized_prefix) - len(digest) - 2
    return "%s-%s-%s" % (normalized_prefix, sanitized[:room], digest)


def _intent(
    *,
    client_order_id: Any,
    symbol: str,
    direction: Any,
    side: str,
    quantity: Any,
    reduce_only: bool,
    created_at: datetime,
    signal_id: str,
    order_type: str = "MARKET",
    limit_price: Optional[Any] = None,
) -> OrderIntent:
    return OrderIntent(
        client_order_id=normalize_client_order_id(client_order_id),
        symbol=symbol,
        direction=Direction(direction),
        side=side,
        quantity=_decimal(quantity, "quantity", positive=True),
        order_type=order_type,
        limit_price=(
            None
            if limit_price is None
            else _decimal(limit_price, "limit_price", positive=True)
        ),
        reduce_only=bool(reduce_only),
        created_at=created_at,
        signal_id=str(signal_id or client_order_id),
    )


class PaperBroker:
    """Immediate deterministic fills backed by the transactional paper ledger."""

    def __init__(
        self,
        ledger: PaperLedger,
        fee_bps: Any = "4",
        price_provider: Optional[Callable[[str], Any]] = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        parsed_fee_bps = _decimal(fee_bps, "fee_bps")
        if parsed_fee_bps < _ZERO:
            raise ValueError("fee_bps 不能为负")
        self.ledger = ledger
        self.fee_rate = parsed_fee_bps / Decimal("10000")
        self.price_provider = price_provider
        self.clock = clock

    def _price(self, intent: OrderIntent, fill_price: Optional[Any]) -> Decimal:
        if fill_price is not None:
            return _decimal(fill_price, "fill_price", positive=True)
        if intent.limit_price is not None:
            return _decimal(intent.limit_price, "limit_price", positive=True)
        if self.price_provider is None:
            raise ValueError("paper MARKET 订单必须提供成交价或 price_provider")
        quote = self.price_provider(intent.symbol)
        if isinstance(quote, (Decimal, int, float, str)):
            return _decimal(quote, "fill_price", positive=True)
        field_names = (
            ("ask_price", "ask") if intent.side == "BUY" else ("bid_price", "bid")
        )
        return _decimal(_field(quote, *field_names), "fill_price", positive=True)

    @staticmethod
    def _result(fill: LedgerFill) -> ExecutionResult:
        return ExecutionResult(
            client_order_id=fill.client_order_id,
            status="FILLED",
            order_id=fill.exchange_order_id,
            executed_quantity=fill.quantity,
            average_price=fill.price,
            observed_at=_utc_datetime(fill.executed_at),
            reason="IDEMPOTENT_REPLAY" if fill.idempotent else "",
        )

    def submit_order(
        self,
        intent: OrderIntent,
        fill_price: Optional[Any] = None,
        observed_at: Optional[datetime] = None,
        mark_prices: Optional[Mapping[str, Any]] = None,
    ) -> ExecutionResult:
        if not isinstance(intent, OrderIntent):
            raise TypeError("PaperBroker 只接受已验证 OrderIntent")
        timestamp = _utc_datetime(observed_at, default=self.clock())
        price = self._price(intent, fill_price)
        fill = self.ledger.record_fill(
            client_order_id=intent.client_order_id,
            exchange_order_id="paper:%s" % intent.client_order_id,
            symbol=intent.symbol,
            side=intent.side,
            quantity=intent.quantity,
            price=price,
            fee_rate=self.fee_rate,
            reduce_only=intent.reduce_only,
            executed_at=timestamp,
            mark_prices=mark_prices,
        )
        return self._result(fill)

    execute = submit_order

    def open_long(
        self,
        symbol: str,
        quantity: Any,
        price: Any,
        client_order_id: Any,
        signal_id: str = "paper-long",
        now: Optional[datetime] = None,
    ) -> ExecutionResult:
        timestamp = _utc_datetime(now, default=self.clock())
        intent = _intent(
            client_order_id=client_order_id,
            symbol=symbol,
            direction=Direction.LONG,
            side="BUY",
            quantity=quantity,
            reduce_only=False,
            created_at=timestamp,
            signal_id=signal_id,
        )
        return self.submit_order(intent, fill_price=price, observed_at=timestamp)

    def open_short(
        self,
        symbol: str,
        quantity: Any,
        price: Any,
        client_order_id: Any,
        signal_id: str = "paper-short",
        now: Optional[datetime] = None,
    ) -> ExecutionResult:
        timestamp = _utc_datetime(now, default=self.clock())
        intent = _intent(
            client_order_id=client_order_id,
            symbol=symbol,
            direction=Direction.SHORT,
            side="SELL",
            quantity=quantity,
            reduce_only=False,
            created_at=timestamp,
            signal_id=signal_id,
        )
        return self.submit_order(intent, fill_price=price, observed_at=timestamp)

    def exit_position(
        self,
        symbol: str,
        price: Any,
        client_order_id: Any,
        quantity: Optional[Any] = None,
        reason: str = "EXIT",
        now: Optional[datetime] = None,
    ) -> ExecutionResult:
        position = self.ledger.get_position(symbol)
        if position.quantity == _ZERO:
            raise ValueError("reduce-only 退出时没有可减持仓")
        direction = Direction.LONG if position.quantity > _ZERO else Direction.SHORT
        requested = abs(position.quantity) if quantity is None else quantity
        timestamp = _utc_datetime(now, default=self.clock())
        intent = _intent(
            client_order_id=client_order_id,
            symbol=symbol,
            direction=direction,
            side=direction.exit_side,
            quantity=requested,
            reduce_only=True,
            created_at=timestamp,
            signal_id=reason,
        )
        return self.submit_order(intent, fill_price=price, observed_at=timestamp)

    close_position = exit_position

    def snapshot(self, **kwargs: Any) -> Mapping[str, Any]:
        return self.ledger.snapshot(**kwargs)


class BinanceTestnetBroker:
    """One-way Demo adapter; uncertain submissions are queried exactly once."""

    def __init__(self, client: Any, clock: Callable[[], datetime] = _utc_now) -> None:
        self.client = client
        self.clock = clock

    @staticmethod
    def _params(intent: OrderIntent) -> Mapping[str, Any]:
        if not isinstance(intent, OrderIntent):
            raise TypeError("BinanceTestnetBroker 只接受已验证 OrderIntent")
        client_order_id = normalize_client_order_id(intent.client_order_id)
        params = {
            "symbol": intent.symbol,
            "side": intent.side,
            "type": intent.order_type,
            "quantity": _decimal_text(intent.quantity),
            "newClientOrderId": client_order_id,
            "positionSide": "BOTH",
            # ACK 不携带可靠 executedQty/avgPrice；运行时必须拿到终态后才能
            # 原子更新保护计划，MARKET 与 LIMIT 因而统一请求 RESULT。
            "newOrderRespType": "RESULT",
        }
        if intent.reduce_only:
            params["reduceOnly"] = True
        if intent.order_type == "LIMIT":
            params["price"] = _decimal_text(intent.limit_price or _ZERO)
            params["timeInForce"] = "IOC"
        return params

    def _submit(self, params: Mapping[str, Any]) -> Any:
        if callable(getattr(self.client, "new_order", None)):
            method = self.client.new_order
            try:
                parameters = tuple(inspect.signature(method).parameters.values())
            except (TypeError, ValueError):
                parameters = ()
            if len(parameters) == 1 and parameters[0].kind not in (
                inspect.Parameter.VAR_KEYWORD,
                inspect.Parameter.VAR_POSITIONAL,
            ):
                return method(dict(params))
            return method(**dict(params))
        if callable(getattr(self.client, "futures_create_order", None)):
            return self.client.futures_create_order(**dict(params))
        if callable(getattr(self.client, "create_order", None)):
            return self.client.create_order(**dict(params))
        if callable(getattr(self.client, "submit_order", None)):
            return self.client.submit_order(dict(params))
        raise TypeError("Binance client 缺失下单能力")

    def _query(self, symbol: str, client_order_id: str) -> Any:
        params = {"symbol": symbol, "origClientOrderId": client_order_id}
        if callable(getattr(self.client, "query_order", None)):
            method = self.client.query_order
            try:
                names = tuple(inspect.signature(method).parameters)
            except (TypeError, ValueError):
                names = ()
            if "client_order_id" in names:
                return method(symbol, client_order_id)
            return method(**params)
        if callable(getattr(self.client, "futures_get_order", None)):
            return self.client.futures_get_order(**params)
        if callable(getattr(self.client, "get_order", None)):
            return self.client.get_order(**params)
        raise TypeError("Binance client 缺失查单能力")

    @staticmethod
    def _status_code(error: BaseException) -> Optional[int]:
        for value in (
            getattr(error, "status_code", None),
            getattr(error, "status", None),
            getattr(getattr(error, "response", None), "status_code", None),
            getattr(getattr(error, "response", None), "status", None),
        ):
            try:
                if value is not None:
                    return int(value)
            except (TypeError, ValueError):
                continue
        return None

    @classmethod
    def _uncertain(cls, error: BaseException) -> bool:
        if isinstance(error, (BinanceSubmissionUnknown, TimeoutError, socket.timeout)):
            return True
        status_code = cls._status_code(error)
        code = getattr(error, "code", None)
        message = str(getattr(error, "message", "") or error)
        # Binance 明确定义 -1006/-1007 的撮合执行状态未知；HTTP 状态不能
        # 覆盖业务错误码语义，否则 4xx 包装会把已成交订单误记为拒单。
        if code in (-1006, -1007):
            return True
        if status_code is not None and is_definitive_order_503(status_code, code, message):
            return False
        if status_code == 408:
            return True
        if status_code is not None and 500 <= status_code <= 599:
            return True
        text = str(error).lower()
        return (
            "timed out" in text
            or "timeout" in text
            or re.search(r"\b5\d{2}\b", text) is not None
        )

    @classmethod
    def _retryable_server_failure(cls, error: BaseException) -> bool:
        status_code = cls._status_code(error)
        if status_code is None:
            return False
        return is_definitive_order_503(
            status_code,
            getattr(error, "code", None),
            str(getattr(error, "message", "") or error),
        )

    @classmethod
    def _not_found(cls, error: BaseException) -> bool:
        code = getattr(error, "code", None)
        return code == -2013 or cls._status_code(error) == 404

    def _execution_result(
        self,
        response: Any,
        client_order_id: str,
        reason: str = "",
    ) -> ExecutionResult:
        if isinstance(response, ExecutionResult):
            return response
        status = str(_field(response, "status", default="UNKNOWN") or "UNKNOWN")
        order_id = str(_field(response, "orderId", "order_id", default="") or "")
        executed = _decimal(
            _field(response, "executedQty", "executed_quantity", "cumQty", default="0"),
            "executed_quantity",
        )
        average = _decimal(
            _field(response, "avgPrice", "average_price", default="0"),
            "average_price",
        )
        observed = _utc_datetime(
            _field(
                response,
                "updateTime",
                "transactTime",
                "observed_at",
                "time",
                default=None,
            ),
            default=self.clock(),
        )
        returned_id = str(
            _field(
                response,
                "clientOrderId",
                "client_order_id",
                default=client_order_id,
            )
            or client_order_id
        )
        return ExecutionResult(
            client_order_id=returned_id,
            status=status,
            order_id=order_id,
            executed_quantity=executed,
            average_price=average,
            observed_at=observed,
            reason=reason,
        )

    def _unknown_result(self, client_order_id: str, reason: str) -> ExecutionResult:
        return ExecutionResult(
            client_order_id=client_order_id,
            status="UNKNOWN",
            order_id="",
            executed_quantity=_ZERO,
            average_price=_ZERO,
            observed_at=self.clock(),
            reason=reason,
            submission_unknown=True,
        )

    def _retryable_failure_result(self, client_order_id: str) -> ExecutionResult:
        return ExecutionResult(
            client_order_id=client_order_id,
            status="REJECTED",
            order_id="",
            executed_quantity=_ZERO,
            average_price=_ZERO,
            observed_at=self.clock(),
            reason=RETRYABLE_SERVER_FAILURE,
        )

    def _queried_result(
        self,
        response: Any,
        client_order_id: str,
        recovered_reason: str,
        uncertain_reason: str,
    ) -> ExecutionResult:
        result = self._execution_result(
            response, client_order_id, reason=recovered_reason
        )
        if result.status in _TERMINAL_ORDER_STATUSES:
            return result
        return replace(
            result,
            reason="%s:%s" % (uncertain_reason, result.status),
            submission_unknown=True,
        )

    def submit_order(self, intent: OrderIntent) -> ExecutionResult:
        params = self._params(intent)
        client_order_id = str(params["newClientOrderId"])
        try:
            response = self._submit(params)
        except Exception as error:
            if self._retryable_server_failure(error):
                return self._retryable_failure_result(client_order_id)
            if not self._uncertain(error):
                raise
            try:
                recovered = self._query(intent.symbol, client_order_id)
            except Exception as query_error:
                suffix = "ORDER_NOT_FOUND" if self._not_found(query_error) else "QUERY_FAILED"
                return self._unknown_result(
                    client_order_id, "SUBMISSION_UNCERTAIN_%s" % suffix
                )
            if recovered is None:
                return self._unknown_result(
                    client_order_id, "SUBMISSION_UNCERTAIN_ORDER_NOT_FOUND"
                )
            return self._queried_result(
                recovered,
                client_order_id,
                recovered_reason="RECOVERED_AFTER_UNCERTAIN_SUBMIT",
                uncertain_reason="SUBMISSION_UNCERTAIN_NON_TERMINAL",
            )
        return self._execution_result(response, client_order_id)

    def recover_order(self, symbol: str, client_order_id: str) -> ExecutionResult:
        """Query a durable uncertain identity once; never turns not-found into permission to resubmit."""
        normalized = normalize_client_order_id(client_order_id)
        try:
            response = self._query(str(symbol).upper(), normalized)
        except Exception as error:
            suffix = "ORDER_NOT_FOUND" if self._not_found(error) else "QUERY_FAILED"
            return self._unknown_result(normalized, "RECOVERY_%s" % suffix)
        if response is None:
            return self._unknown_result(normalized, "RECOVERY_ORDER_NOT_FOUND")
        return self._queried_result(
            response,
            normalized,
            recovered_reason="RECOVERED_AFTER_RESTART",
            uncertain_reason="RECOVERY_NON_TERMINAL",
        )

    execute = submit_order

    def open_long(
        self,
        symbol: str,
        quantity: Any,
        client_order_id: Any,
        signal_id: str = "testnet-long",
    ) -> ExecutionResult:
        now = self.clock()
        return self.submit_order(
            _intent(
                client_order_id=client_order_id,
                symbol=symbol,
                direction=Direction.LONG,
                side="BUY",
                quantity=quantity,
                reduce_only=False,
                created_at=now,
                signal_id=signal_id,
            )
        )

    def open_short(
        self,
        symbol: str,
        quantity: Any,
        client_order_id: Any,
        signal_id: str = "testnet-short",
    ) -> ExecutionResult:
        now = self.clock()
        return self.submit_order(
            _intent(
                client_order_id=client_order_id,
                symbol=symbol,
                direction=Direction.SHORT,
                side="SELL",
                quantity=quantity,
                reduce_only=False,
                created_at=now,
                signal_id=signal_id,
            )
        )

    def exit_position(
        self,
        symbol: str,
        direction: Any,
        quantity: Any,
        client_order_id: Any,
        reason: str = "EXIT",
    ) -> ExecutionResult:
        parsed_direction = Direction(direction)
        now = self.clock()
        return self.submit_order(
            _intent(
                client_order_id=client_order_id,
                symbol=symbol,
                direction=parsed_direction,
                side=parsed_direction.exit_side,
                quantity=quantity,
                reduce_only=True,
                created_at=now,
                signal_id=reason,
            )
        )

    close_position = exit_position

    def cancel_order(self, symbol: str, client_order_id: str) -> Any:
        params = {
            "symbol": str(symbol).upper(),
            "origClientOrderId": normalize_client_order_id(client_order_id),
        }
        if callable(getattr(self.client, "cancel_order", None)):
            method = self.client.cancel_order
            try:
                names = tuple(inspect.signature(method).parameters)
            except (TypeError, ValueError):
                names = ()
            if "client_order_id" in names:
                return method(params["symbol"], params["origClientOrderId"])
            return method(**params)
        if callable(getattr(self.client, "futures_cancel_order", None)):
            return self.client.futures_cancel_order(**params)
        raise TypeError("Binance client 缺失撤单能力")


TestnetBroker = BinanceTestnetBroker


__all__ = [
    "BinanceTestnetBroker",
    "PaperBroker",
    "RETRYABLE_SERVER_FAILURE",
    "TestnetBroker",
    "normalize_client_order_id",
]
