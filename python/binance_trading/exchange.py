"""
[INPUT]: 依赖 BinanceConfig、可注入 HTTP transport、HMAC-SHA256 与 USDⓈ-M REST/Algo JSON
[OUTPUT]: 提供 Demo REST 客户端、signed request、响应完整性保护、418/429 全局冷却闩及委托提交语义分类
[POS]: binance_trading 的交易所防腐层；把 Algo Service 身份与未知提交隔离在 REST 边界，业务层不盲重投订单
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import re
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .config import BinanceConfig
from .models import SymbolRules, decimal_value


ALGO_ORDER_TYPES = frozenset(
    {
        "STOP",
        "STOP_MARKET",
        "TAKE_PROFIT",
        "TAKE_PROFIT_MARKET",
        "TRAILING_STOP_MARKET",
    }
)
CLIENT_ALGO_ID_RE = re.compile(r"^[\.A-Z\:/a-z0-9_-]{1,36}$")
RATE_LIMIT_FALLBACK_SECONDS = {429: 60.0, 418: 120.0}


class BinanceError(RuntimeError):
    pass


class BinanceTransportError(BinanceError):
    pass


class BinanceApiError(BinanceError):
    def __init__(self, status: int, code: Optional[int], message: str) -> None:
        super().__init__("Binance API status=%s code=%s: %s" % (status, code, message))
        self.status = int(status)
        self.code = code
        self.message = message


class BinanceRateLimitError(BinanceApiError):
    """全局 REST 冷却信号；本地拦截与交易所拒绝都是明确未提交。"""

    def __init__(
        self,
        status: int,
        code: Optional[int],
        message: str,
        retry_after_seconds: float,
        *,
        locally_blocked: bool,
    ) -> None:
        super().__init__(status, code, message)
        self.retry_after_seconds = max(0.0, float(retry_after_seconds))
        self.locally_blocked = bool(locally_blocked)


class BinanceSubmissionUnknown(BinanceError):
    """The matching engine may have accepted the order; querying is mandatory."""

    def __init__(self, client_order_id: str, reason: str) -> None:
        super().__init__("order submission unknown for %s: %s" % (client_order_id, reason))
        self.client_order_id = client_order_id
        self.reason = reason


def is_definitive_order_503(status: int, code: Optional[int], message: str) -> bool:
    """仅识别 Binance 明文承诺 100% 未执行的 503 变体。"""

    if int(status) != 503:
        return False
    normalized = str(message or "").strip().casefold().rstrip(".")
    return (
        code == -1008
        or normalized == "service unavailable"
        or normalized.startswith("internal error; unable to process your request")
        or normalized.startswith("request throttled by system-level protection")
    )


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


class UrllibTransport:
    @staticmethod
    def _read_body(response: Any) -> bytes:
        try:
            return response.read()
        except (http.client.IncompleteRead, ConnectionError, OSError) as exc:
            raise BinanceTransportError(type(exc).__name__) from exc

    def request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: Optional[bytes],
        timeout: int,
    ) -> HttpResponse:
        request = urllib.request.Request(
            url=url,
            data=body,
            headers=dict(headers),
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return HttpResponse(
                    status=int(response.status),
                    headers={str(key): str(value) for key, value in response.headers.items()},
                    body=self._read_body(response),
                )
        except urllib.error.HTTPError as exc:
            return HttpResponse(
                status=int(exc.code),
                headers={str(key): str(value) for key, value in exc.headers.items()},
                body=self._read_body(exc),
            )
        except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
            raise BinanceTransportError(type(exc).__name__) from exc


def _strict_json(raw: bytes) -> Any:
    def unique_object(pairs: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Binance JSON 包含重复键: %s" % key)
            result[key] = value
        return result

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise BinanceError("Binance 返回了非法 JSON") from exc


def _params(items: Optional[Mapping[str, Any]]) -> List[Tuple[str, str]]:
    if not items:
        return []
    result: List[Tuple[str, str]] = []
    for key, value in items.items():
        if value is None:
            continue
        if isinstance(value, bool):
            encoded = "true" if value else "false"
        else:
            encoded = str(value)
        result.append((str(key), encoded))
    return result


def _retry_after_seconds(headers: Mapping[str, str]) -> Optional[float]:
    """解析 Binance 实际使用的 delta-seconds；其他形状一律交给保守回退。"""

    raw = next(
        (
            str(value).strip()
            for key, value in headers.items()
            if str(key).casefold() == "retry-after"
        ),
        "",
    )
    if not raw.isdigit():
        return None
    seconds = float(raw)
    return seconds if seconds > 0 else None


def _explicit_true(value: Any) -> bool:
    return value is True or (isinstance(value, str) and value.casefold() == "true")


def _validate_client_algo_id(value: Any) -> str:
    client_algo_id = str(value or "")
    if CLIENT_ALGO_ID_RE.fullmatch(client_algo_id) is None:
        raise ValueError("clientAlgoId 必须符合官方 1..36 字符规则")
    return client_algo_id


def _algo_identity(
    client_algo_id: Optional[str], algo_id: Optional[int]
) -> Mapping[str, Any]:
    if (client_algo_id is None) == (algo_id is None):
        raise ValueError("algoId 与 clientAlgoId 必须且只能提供一个")
    if client_algo_id is not None:
        return {"clientAlgoId": _validate_client_algo_id(client_algo_id)}
    parsed_algo_id = int(algo_id or 0)
    if parsed_algo_id <= 0:
        raise ValueError("algoId 必须为正整数")
    return {"algoId": parsed_algo_id}


class BinanceFuturesClient:
    def __init__(
        self,
        config: BinanceConfig,
        transport: Optional[Any] = None,
        clock_ms: Optional[Callable[[], int]] = None,
        monotonic: Optional[Callable[[], float]] = None,
    ) -> None:
        self.config = config
        self.transport = transport or UrllibTransport()
        self.clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.monotonic = monotonic or time.monotonic
        self._server_offset_ms = 0
        self._rate_limit_lock = threading.Lock()
        self._rate_limit_until = 0.0
        self._rate_limit_status = 429
        self._rate_limit_code: Optional[int] = None
        self._rate_limit_message = "REST cooldown active"

    @property
    def server_offset_ms(self) -> int:
        return self._server_offset_ms

    @property
    def rate_limit_remaining_seconds(self) -> float:
        with self._rate_limit_lock:
            return max(0.0, self._rate_limit_until - float(self.monotonic()))

    def _raise_if_rate_limited(self) -> None:
        with self._rate_limit_lock:
            remaining = self._rate_limit_until - float(self.monotonic())
            if remaining <= 0:
                self._rate_limit_until = 0.0
                return
            status = self._rate_limit_status
            code = self._rate_limit_code
            message = self._rate_limit_message
        raise BinanceRateLimitError(
            status,
            code,
            message,
            remaining,
            locally_blocked=True,
        )

    def _activate_rate_limit(
        self,
        status: int,
        code: Optional[int],
        message: str,
        headers: Mapping[str, str],
    ) -> BinanceRateLimitError:
        delay = _retry_after_seconds(headers)
        if delay is None:
            delay = RATE_LIMIT_FALLBACK_SECONDS[int(status)]
        now = float(self.monotonic())
        candidate = now + delay
        with self._rate_limit_lock:
            if candidate >= self._rate_limit_until:
                self._rate_limit_until = candidate
                self._rate_limit_status = int(status)
                self._rate_limit_code = code
                self._rate_limit_message = message
            remaining = self._rate_limit_until - now
            effective_status = self._rate_limit_status
            effective_code = self._rate_limit_code
            effective_message = self._rate_limit_message
        return BinanceRateLimitError(
            effective_status,
            effective_code,
            effective_message,
            remaining,
            locally_blocked=False,
        )

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Mapping[str, Any]] = None,
        signed: bool = False,
        order_client_id: Optional[str] = None,
    ) -> Any:
        if not path.startswith("/fapi/"):
            raise ValueError("Binance path 必须位于 /fapi/")
        self._raise_if_rate_limited()
        pairs = _params(params)
        headers = {"Accept": "application/json", "User-Agent": "binance-testnet-runtime/1"}
        if signed:
            if not self.config.api_key or not self.config.api_secret:
                raise BinanceError("signed request 缺少 Demo API 凭证")
            pairs.extend(
                [
                    ("recvWindow", str(self.config.recv_window_ms)),
                    ("timestamp", str(self.clock_ms() + self._server_offset_ms)),
                ]
            )
            payload = urllib.parse.urlencode(pairs)
            signature = hmac.new(
                self.config.api_secret.encode("utf-8"),
                payload.encode("ascii"),
                hashlib.sha256,
            ).hexdigest()
            pairs.append(("signature", signature))
            headers["X-MBX-APIKEY"] = self.config.api_key
        query = urllib.parse.urlencode(pairs)
        url = self.config.rest_url + path + ("?" + query if query else "")
        try:
            response = self.transport.request(
                method.upper(),
                url,
                headers,
                None,
                self.config.request_timeout_sec,
            )
        except (BinanceTransportError, urllib.error.URLError, socket.timeout, TimeoutError, OSError) as exc:
            if order_client_id:
                cause = exc.__cause__ or exc
                raise BinanceSubmissionUnknown(order_client_id, type(cause).__name__) from exc
            if isinstance(exc, BinanceTransportError):
                raise
            raise BinanceTransportError(type(exc).__name__) from exc
        if response.status in RATE_LIMIT_FALLBACK_SECONDS:
            code: Optional[int] = None
            message = "HTTP %s rate limited" % response.status
            try:
                rate_payload = _strict_json(response.body)
            except BinanceError:
                rate_payload = None
            if isinstance(rate_payload, dict):
                try:
                    code = int(rate_payload["code"]) if "code" in rate_payload else None
                except (TypeError, ValueError):
                    code = None
                message = str(
                    rate_payload.get("msg") or rate_payload.get("message") or message
                )
            raise self._activate_rate_limit(
                response.status, code, message, response.headers
            )
        try:
            payload = _strict_json(response.body)
        except BinanceError as exc:
            if order_client_id:
                raise BinanceSubmissionUnknown(
                    order_client_id, "HTTP %s with invalid JSON" % response.status
                ) from exc
            raise
        code: Optional[int] = None
        message = "HTTP %s" % response.status
        if isinstance(payload, dict):
            try:
                code = int(payload["code"]) if "code" in payload else None
            except (TypeError, ValueError):
                code = None
            message = str(payload.get("msg") or payload.get("message") or message)
        if order_client_id and code in {-1006, -1007}:
            # Binance 明确定义为 UNEXPECTED_RESP/TIMEOUT，响应无法证明未入撮合。
            raise BinanceSubmissionUnknown(order_client_id, message)
        if order_client_id and is_definitive_order_503(response.status, code, message):
            raise BinanceApiError(response.status, code, message)
        if order_client_id and (response.status == 408 or 500 <= response.status <= 599):
            # 408 与非明确失败的 5xx 无法证明订单未提交；保守地统一查单。
            raise BinanceSubmissionUnknown(order_client_id, message)
        if not 200 <= response.status < 300 or (code is not None and code < 0):
            raise BinanceApiError(response.status, code, message)
        return payload

    def public_get(self, path: str, params: Optional[Mapping[str, Any]] = None) -> Any:
        return self._request("GET", path, params=params, signed=False)

    def signed_request(
        self, method: str, path: str, params: Optional[Mapping[str, Any]] = None
    ) -> Any:
        return self._request(method, path, params=params, signed=True)

    def sync_time(self) -> int:
        sent_at = self.clock_ms()
        payload = self.public_get("/fapi/v1/time")
        received_at = self.clock_ms()
        if not isinstance(payload, dict) or "serverTime" not in payload:
            raise BinanceError("server time 载荷缺失")
        server_time = int(payload["serverTime"])
        midpoint = sent_at + (received_at - sent_at) // 2
        self._server_offset_ms = server_time - midpoint
        return self._server_offset_ms

    def exchange_info(self) -> Mapping[str, Any]:
        payload = self.public_get("/fapi/v1/exchangeInfo")
        if not isinstance(payload, dict):
            raise BinanceError("exchangeInfo 必须是 object")
        return payload

    def klines(self, symbol: str, interval: str, limit: int) -> Sequence[Any]:
        payload = self.public_get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "limit": int(limit)},
        )
        if not isinstance(payload, list):
            raise BinanceError("klines 必须是 array")
        return payload

    def account(self) -> Mapping[str, Any]:
        payload = self.signed_request("GET", "/fapi/v3/account")
        if not isinstance(payload, dict):
            raise BinanceError("account 必须是 object")
        return payload

    def positions(self, symbol: Optional[str] = None) -> Sequence[Mapping[str, Any]]:
        payload = self.signed_request(
            "GET", "/fapi/v3/positionRisk", {"symbol": symbol} if symbol else None
        )
        if not isinstance(payload, list):
            raise BinanceError("positionRisk 必须是 array")
        return payload

    def open_orders(self) -> Sequence[Mapping[str, Any]]:
        # 不带 symbol 才能发现账户中由人工或旧程序遗留的跨标的活动委托。
        payload = self.signed_request("GET", "/fapi/v1/openOrders")
        if not isinstance(payload, list) or any(not isinstance(row, Mapping) for row in payload):
            raise BinanceError("openOrders 必须是 object array")
        return payload

    def position_mode(self) -> bool:
        payload = self.signed_request("GET", "/fapi/v1/positionSide/dual")
        if not isinstance(payload, dict) or not isinstance(payload.get("dualSidePosition"), bool):
            raise BinanceError("position mode 载荷无效")
        return bool(payload["dualSidePosition"])

    def change_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> Mapping[str, Any]:
        normalized = str(margin_type or "").upper()
        if normalized != "ISOLATED":
            raise ValueError("runtime 只允许 ISOLATED margin type")
        try:
            payload = self.signed_request(
                "POST",
                "/fapi/v1/marginType",
                {"symbol": str(symbol).upper(), "marginType": normalized},
            )
        except BinanceApiError as exc:
            # -4046 表示目标值已经生效；这是幂等成功，不应阻断重启。
            if exc.code == -4046:
                return {"code": exc.code, "msg": str(exc), "already_configured": True}
            raise
        if not isinstance(payload, dict):
            raise BinanceError("margin type 载荷必须是 object")
        return payload

    def change_leverage(self, symbol: str, leverage: int) -> Mapping[str, Any]:
        parsed = int(leverage)
        if not 1 <= parsed <= 20:
            raise ValueError("runtime leverage 必须在 1..20")
        payload = self.signed_request(
            "POST",
            "/fapi/v1/leverage",
            {"symbol": str(symbol).upper(), "leverage": parsed},
        )
        if not isinstance(payload, dict) or int(payload.get("leverage", 0)) != parsed:
            raise BinanceError("leverage 配置未返回目标值")
        return payload

    def new_order(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        client_order_id = str(params.get("newClientOrderId") or "")
        if not 1 <= len(client_order_id) <= 36:
            raise ValueError("newClientOrderId 必须在 1..36 字符")
        payload = self._request(
            "POST",
            "/fapi/v1/order",
            params=params,
            signed=True,
            order_client_id=client_order_id,
        )
        if not isinstance(payload, dict):
            raise BinanceError("new order 载荷必须是 object")
        return payload

    def new_algo_order(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        """通过 Algo Service 创建可按 clientAlgoId 恢复的条件单。"""

        request = dict(params)
        client_algo_id = _validate_client_algo_id(request.get("clientAlgoId"))
        if request.get("algoType") != "CONDITIONAL":
            raise ValueError("algoType 必须显式为 CONDITIONAL")
        order_type = str(request.get("type") or "").upper()
        if order_type not in ALGO_ORDER_TYPES:
            raise ValueError("Algo type 只允许官方条件单类型")
        if str(request.get("side") or "").upper() not in {"BUY", "SELL"}:
            raise ValueError("Algo side 必须为 BUY/SELL")
        if not str(request.get("symbol") or "").strip():
            raise ValueError("Algo symbol 不能为空")

        position_side = str(request.get("positionSide") or "BOTH").upper()
        if position_side not in {"BOTH", "LONG", "SHORT"}:
            raise ValueError("Algo positionSide 无效")
        close_position = _explicit_true(request.get("closePosition"))
        if close_position:
            if order_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
                raise ValueError("closePosition 只允许 STOP_MARKET/TAKE_PROFIT_MARKET")
            if "quantity" in request or "reduceOnly" in request:
                raise ValueError("closePosition 不能与 quantity/reduceOnly 同时发送")
            side = str(request["side"]).upper()
            if (side, position_side) in {("BUY", "LONG"), ("SELL", "SHORT")}:
                raise ValueError("Hedge Mode 的 closePosition 方向与 positionSide 冲突")
        elif request.get("quantity") is None:
            raise ValueError("非 closePosition Algo 必须携带 quantity")
        if "reduceOnly" in request and position_side != "BOTH":
            raise ValueError("Hedge Mode 不允许 reduceOnly")

        if order_type == "TRAILING_STOP_MARKET":
            callback_rate = decimal_value(request.get("callbackRate"), "callbackRate")
            if not decimal_value("0.1", "minCallback") <= callback_rate <= decimal_value(
                "10", "maxCallback"
            ):
                raise ValueError("callbackRate 必须在 0.1..10")
        else:
            if request.get("triggerPrice") is None:
                raise ValueError("非 trailing 条件单必须携带 triggerPrice")
            if order_type in {"STOP", "TAKE_PROFIT"} and not (
                request.get("price") is not None or request.get("priceMatch") is not None
            ):
                raise ValueError("STOP/TAKE_PROFIT 必须携带 price 或 priceMatch")
        if "price" in request and "priceMatch" in request:
            raise ValueError("price 与 priceMatch 不能同时发送")
        if "priceMatch" in request and order_type not in {"STOP", "TAKE_PROFIT"}:
            raise ValueError("priceMatch 只允许 STOP/TAKE_PROFIT")

        payload = self._request(
            "POST",
            "/fapi/v1/algoOrder",
            params=request,
            signed=True,
            order_client_id=client_algo_id,
        )
        if not isinstance(payload, dict):
            raise BinanceSubmissionUnknown(client_algo_id, "unexpected Algo JSON shape")
        return payload

    def query_algo_order(
        self,
        client_algo_id: Optional[str] = None,
        algo_id: Optional[int] = None,
    ) -> Mapping[str, Any]:
        payload = self.signed_request(
            "GET", "/fapi/v1/algoOrder", _algo_identity(client_algo_id, algo_id)
        )
        if not isinstance(payload, dict):
            raise BinanceError("query Algo order 载荷必须是 object")
        return payload

    def cancel_algo_order(
        self,
        client_algo_id: Optional[str] = None,
        algo_id: Optional[int] = None,
    ) -> Mapping[str, Any]:
        payload = self.signed_request(
            "DELETE", "/fapi/v1/algoOrder", _algo_identity(client_algo_id, algo_id)
        )
        if not isinstance(payload, dict):
            raise BinanceError("cancel Algo order 载荷必须是 object")
        return payload

    def open_algo_orders(
        self,
        symbol: Optional[str] = None,
        algo_type: Optional[str] = None,
        algo_id: Optional[int] = None,
    ) -> Sequence[Mapping[str, Any]]:
        params: Dict[str, Any] = {}
        if symbol is not None:
            normalized_symbol = str(symbol).upper()
            if not normalized_symbol:
                raise ValueError("Algo symbol 不能为空")
            params["symbol"] = normalized_symbol
        if algo_type is not None:
            if str(algo_type).upper() != "CONDITIONAL":
                raise ValueError("algoType 只允许 CONDITIONAL")
            params["algoType"] = "CONDITIONAL"
        if algo_id is not None:
            parsed_algo_id = int(algo_id)
            if parsed_algo_id <= 0:
                raise ValueError("algoId 必须为正整数")
            params["algoId"] = parsed_algo_id
        payload = self.signed_request("GET", "/fapi/v1/openAlgoOrders", params)
        if not isinstance(payload, list) or any(not isinstance(row, Mapping) for row in payload):
            raise BinanceError("openAlgoOrders 必须是 object array")
        return payload

    def query_order(self, symbol: str, client_order_id: str) -> Mapping[str, Any]:
        payload = self.signed_request(
            "GET",
            "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_order_id},
        )
        if not isinstance(payload, dict):
            raise BinanceError("query order 载荷必须是 object")
        return payload

    def cancel_order(self, symbol: str, client_order_id: str) -> Mapping[str, Any]:
        payload = self.signed_request(
            "DELETE",
            "/fapi/v1/order",
            {"symbol": symbol, "origClientOrderId": client_order_id},
        )
        if not isinstance(payload, dict):
            raise BinanceError("cancel order 载荷必须是 object")
        return payload


def parse_symbol_rules(exchange_info: Mapping[str, Any], symbol: str) -> SymbolRules:
    normalized = str(symbol).upper()
    rows = exchange_info.get("symbols") if isinstance(exchange_info, Mapping) else None
    if not isinstance(rows, list):
        raise BinanceError("exchangeInfo.symbols 缺失")
    row = next(
        (item for item in rows if isinstance(item, Mapping) and item.get("symbol") == normalized),
        None,
    )
    if row is None:
        raise BinanceError("exchangeInfo 不包含 %s" % normalized)
    if (
        row.get("status") != "TRADING"
        or row.get("contractType") != "PERPETUAL"
        or row.get("quoteAsset") != "USDT"
    ):
        raise BinanceError("%s 不是可交易 USDT 永续合约" % normalized)
    filters = {
        str(item.get("filterType")): item
        for item in row.get("filters", [])
        if isinstance(item, Mapping) and item.get("filterType")
    }
    try:
        price = filters["PRICE_FILTER"]
        lot = filters["LOT_SIZE"]
        notional = filters["MIN_NOTIONAL"]
        min_notional = notional.get("notional", notional.get("minNotional"))
        return SymbolRules(
            symbol=normalized,
            tick_size=decimal_value(price["tickSize"], "tickSize"),
            step_size=decimal_value(lot["stepSize"], "stepSize"),
            min_quantity=decimal_value(lot["minQty"], "minQty"),
            max_quantity=decimal_value(lot["maxQty"], "maxQty"),
            min_notional=decimal_value(min_notional, "minNotional"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise BinanceError("%s exchange filters 不完整" % normalized) from exc


__all__ = [
    "BinanceApiError",
    "BinanceError",
    "BinanceFuturesClient",
    "BinanceRateLimitError",
    "BinanceSubmissionUnknown",
    "BinanceTransportError",
    "HttpResponse",
    "UrllibTransport",
    "is_definitive_order_503",
    "parse_symbol_rules",
]
