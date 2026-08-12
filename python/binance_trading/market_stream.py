"""
[INPUT]: 依赖 Binance Demo 分路由 combined WebSocket/REST kline 载荷、期望 symbol 集合与可取消异步连接
[OUTPUT]: 提供严格 bookTicker/markPrice/closed-kline 解码、历史闭柱投影、public/market 双 URL 与有界独立重连合并流
[POS]: binance_trading 的公开行情防腐层；隔离 Binance 路由迁移，下游只看 UTC/Decimal 值对象，不看单字母字段
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import asyncio
import json
import random
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, AsyncIterator, Dict, Iterable, Mapping, Optional, Sequence, Set, Tuple, Union

from .config import BinanceConfig
from .exchange import BinanceError, BinanceFuturesClient
from .models import BookTicker, Kline, MarkPrice, datetime_from_millis, decimal_value


MarketEvent = Union[BookTicker, Kline, MarkPrice]


def _strict_object(raw: Union[str, bytes, Mapping[str, Any]]) -> Mapping[str, Any]:
    if isinstance(raw, Mapping):
        return raw

    def unique_object(pairs: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("WebSocket JSON 重复键: %s" % key)
            result[key] = value
        return result

    text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    payload = json.loads(text, object_pairs_hook=unique_object)
    if not isinstance(payload, Mapping):
        raise ValueError("WebSocket payload 必须是 object")
    return payload


def parse_stream_event(
    raw: Union[str, bytes, Mapping[str, Any]], expected_symbols: Iterable[str]
) -> Optional[MarketEvent]:
    envelope = _strict_object(raw)
    payload = envelope.get("data", envelope)
    if not isinstance(payload, Mapping):
        raise ValueError("combined stream data 必须是 object")
    symbol = str(payload.get("s") or "").upper()
    expected: Set[str] = {str(item).upper() for item in expected_symbols}
    if not symbol or symbol not in expected:
        raise ValueError("stream symbol 不在授权集合")
    event_type = str(payload.get("e") or "")

    if event_type == "bookTicker":
        event_ms = payload.get("E", payload.get("T"))
        transaction_ms = payload.get("T", event_ms)
        return BookTicker(
            symbol=symbol,
            bid_price=decimal_value(payload.get("b"), "bid"),
            bid_quantity=decimal_value(payload.get("B"), "bid quantity"),
            ask_price=decimal_value(payload.get("a"), "ask"),
            ask_quantity=decimal_value(payload.get("A"), "ask quantity"),
            event_time=datetime_from_millis(event_ms, "event time"),
            transaction_time=datetime_from_millis(transaction_ms, "transaction time"),
            update_id=int(payload.get("u", 0)),
        )

    if event_type == "markPriceUpdate":
        return MarkPrice(
            symbol=symbol,
            mark_price=decimal_value(payload.get("p"), "mark price"),
            index_price=decimal_value(payload.get("i"), "index price"),
            funding_rate=decimal_value(payload.get("r", "0"), "funding rate"),
            next_funding_time=datetime_from_millis(payload.get("T"), "next funding time"),
            event_time=datetime_from_millis(payload.get("E"), "event time"),
        )

    if event_type == "kline":
        row = payload.get("k")
        if not isinstance(row, Mapping):
            raise ValueError("kline stream 缺少 k object")
        if not bool(row.get("x")):
            return None
        return Kline(
            symbol=symbol,
            interval=str(row.get("i") or ""),
            open_time=datetime_from_millis(row.get("t"), "kline open time"),
            close_time=datetime_from_millis(row.get("T"), "kline close time"),
            open=decimal_value(row.get("o"), "open"),
            high=decimal_value(row.get("h"), "high"),
            low=decimal_value(row.get("l"), "low"),
            close=decimal_value(row.get("c"), "close"),
            volume=decimal_value(row.get("v", "0"), "volume"),
            quote_volume=decimal_value(row.get("q", "0"), "quote volume"),
            trades=int(row.get("n", 0)),
            closed=True,
        )
    raise ValueError("不支持的 Binance stream event: %s" % event_type)


def parse_rest_klines(rows: Sequence[Any], symbol: str, interval: str) -> Tuple[Kline, ...]:
    bars = []
    last_open: Optional[datetime] = None
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 11:
            raise ValueError("REST kline row 结构无效")
        bar = Kline(
            symbol=symbol,
            interval=interval,
            open_time=datetime_from_millis(row[0], "kline open time"),
            close_time=datetime_from_millis(row[6], "kline close time"),
            open=decimal_value(row[1], "open"),
            high=decimal_value(row[2], "high"),
            low=decimal_value(row[3], "low"),
            close=decimal_value(row[4], "close"),
            volume=decimal_value(row[5], "volume"),
            quote_volume=decimal_value(row[7], "quote volume"),
            trades=int(row[8]),
            closed=True,
        )
        if last_open is not None and bar.open_time <= last_open:
            raise ValueError("REST kline 必须按 open time 严格递增")
        last_open = bar.open_time
        bars.append(bar)
    return tuple(bars)


def build_stream_urls(config: BinanceConfig) -> Tuple[str, str]:
    """按 Binance 2026 路由契约拆分高频 public 与常规 market 流。"""

    public_streams = []
    market_streams = []
    for symbol in config.symbols:
        key = symbol.lower()
        public_streams.append("%s@bookTicker" % key)
        market_streams.extend(
            [
                "%s@markPrice@1s" % key,
                "%s@kline_%s" % (key, config.interval),
            ]
        )
    return (
        config.ws_url + "/public/stream?streams=" + "/".join(public_streams),
        config.ws_url + "/market/stream?streams=" + "/".join(market_streams),
    )


def fetch_history(
    client: BinanceFuturesClient, config: BinanceConfig, symbol: str
) -> Tuple[Kline, ...]:
    return parse_rest_klines(
        client.klines(symbol, config.interval, config.history_limit),
        symbol,
        config.interval,
    )


class BinanceMarketStream:
    def __init__(
        self,
        config: BinanceConfig,
        connect: Optional[Any] = None,
        random_source: Optional[random.Random] = None,
    ) -> None:
        self.config = config
        self._connect = connect
        self._random = random_source or random.Random()

    async def _route_events(self, url: str) -> AsyncIterator[MarketEvent]:
        if self._connect is None:
            import websockets

            connect = websockets.connect
        else:
            connect = self._connect
        delay = 1.0
        while True:
            received = False
            try:
                async with connect(
                    url,
                    open_timeout=self.config.request_timeout_sec,
                    close_timeout=5,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1_048_576,
                ) as websocket:
                    async for raw in websocket:
                        received = True
                        delay = 1.0
                        event = parse_stream_event(raw, self.config.symbols)
                        if event is not None:
                            yield event
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            # 1000/1001 正常关闭与异常关闭都必须让出事件循环并退避；否则
            # clean EOF 会形成零 await 热循环，饿死账户恢复和 stop callback。
            jitter = self._random.random() * min(delay, 1.0)
            await asyncio.sleep(delay + jitter)
            delay = 1.0 if received else min(delay * 2, 30.0)

    async def events(self) -> AsyncIterator[MarketEvent]:
        """合并两条独立路由；任一路由重连都不会阻塞另一条行情。"""

        iterators = [self._route_events(url).__aiter__() for url in build_stream_urls(self.config)]
        pending = {
            asyncio.create_task(iterator.__anext__()): iterator
            for iterator in iterators
        }
        try:
            while pending:
                done, _ = await asyncio.wait(
                    tuple(pending), return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    iterator = pending.pop(task)
                    try:
                        event = task.result()
                    except StopAsyncIteration:
                        continue
                    pending[asyncio.create_task(iterator.__anext__())] = iterator
                    yield event
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for iterator in iterators:
                await iterator.aclose()


__all__ = [
    "BinanceMarketStream",
    "MarketEvent",
    "build_stream_urls",
    "fetch_history",
    "parse_rest_klines",
    "parse_stream_event",
]
