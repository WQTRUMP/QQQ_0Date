"""
[INPUT]: 依赖 Binance public/market routed combined stream、REST kline 样例与行情防腐解码器
[OUTPUT]: 验证双路由合并、独立买卖盘/mark/funding 时钟、仅闭柱发布、symbol 隔离和历史单调性
[POS]: Binance 公开行情边界的无网络回归
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import asyncio
import json
import unittest
from decimal import Decimal

from python.binance_trading.config import BinanceConfig
from python.binance_trading.market_stream import (
    BinanceMarketStream,
    build_stream_urls,
    parse_rest_klines,
    parse_stream_event,
)
from python.binance_trading.models import BookTicker, Kline, MarkPrice


class BinanceMarketStreamTests(unittest.TestCase):
    def test_book_and_mark_streams_keep_independent_source_times(self):
        book = parse_stream_event(
            {
                "stream": "btcusdt@bookTicker",
                "data": {
                    "e": "bookTicker",
                    "E": 1_700_000_000_100,
                    "T": 1_700_000_000_090,
                    "s": "BTCUSDT",
                    "u": 42,
                    "b": "60000.0",
                    "B": "1.25",
                    "a": "60000.1",
                    "A": "2.50",
                },
            },
            {"BTCUSDT"},
        )
        mark = parse_stream_event(
            {
                "data": {
                    "e": "markPriceUpdate",
                    "E": 1_700_000_001_000,
                    "s": "BTCUSDT",
                    "p": "60000.05",
                    "i": "60001.00",
                    "r": "0.0001",
                    "T": 1_700_001_600_000,
                }
            },
            {"BTCUSDT"},
        )

        self.assertIsInstance(book, BookTicker)
        self.assertIsInstance(mark, MarkPrice)
        self.assertEqual(book.update_id, 42)
        self.assertEqual(mark.funding_rate, Decimal("0.0001"))
        self.assertNotEqual(book.event_time, mark.event_time)

    def test_open_kline_is_ignored_and_closed_kline_is_typed(self):
        payload = {
            "data": {
                "e": "kline",
                "E": 1_700_000_060_000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1_700_000_000_000,
                    "T": 1_700_000_059_999,
                    "i": "1m",
                    "o": "100",
                    "h": "110",
                    "l": "90",
                    "c": "105",
                    "v": "10",
                    "q": "1000",
                    "n": 12,
                    "x": False,
                },
            }
        }
        self.assertIsNone(parse_stream_event(payload, {"BTCUSDT"}))
        payload["data"]["k"]["x"] = True
        event = parse_stream_event(json.dumps(payload), {"BTCUSDT"})
        self.assertIsInstance(event, Kline)
        self.assertTrue(event.closed)

    def test_unexpected_symbol_and_non_monotonic_history_are_rejected(self):
        payload = {
            "e": "bookTicker",
            "E": 1_700_000_000_100,
            "T": 1_700_000_000_090,
            "s": "ETHUSDT",
            "u": 1,
            "b": "1",
            "B": "1",
            "a": "2",
            "A": "1",
        }
        with self.assertRaisesRegex(ValueError, "symbol"):
            parse_stream_event(payload, {"BTCUSDT"})

        row = [
            1_700_000_000_000,
            "100",
            "110",
            "90",
            "105",
            "10",
            1_700_000_059_999,
            "1000",
            12,
            "5",
            "500",
        ]
        with self.assertRaisesRegex(ValueError, "严格递增"):
            parse_rest_klines([row, list(row)], "BTCUSDT", "1m")

    def test_stream_urls_split_public_and_market_routes(self):
        config = BinanceConfig.from_mapping({"BINANCE_SYMBOLS": "BTCUSDT"})
        public_url, market_url = build_stream_urls(config)
        self.assertTrue(public_url.startswith("wss://demo-fstream.binance.com/public/stream?streams="))
        self.assertTrue(market_url.startswith("wss://demo-fstream.binance.com/market/stream?streams="))
        self.assertIn("btcusdt@bookTicker", public_url)
        self.assertNotIn("markPrice", public_url)
        self.assertNotIn("kline", public_url)
        self.assertIn("btcusdt@markPrice@1s", market_url)
        self.assertIn("btcusdt@kline_1m", market_url)
        self.assertNotIn("bookTicker", market_url)


class BinanceMarketStreamAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_clean_close_backs_off_without_starving_peer_tasks(self):
        config = BinanceConfig.from_mapping({"BINANCE_SYMBOLS": "BTCUSDT"})
        connected = []

        class CleanSocket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

        def connect(url, **kwargs):
            del kwargs
            connected.append(url)
            return CleanSocket()

        stream = BinanceMarketStream(config, connect=connect)
        events = stream.events()
        waiting = asyncio.create_task(events.__anext__())
        ticks = 0
        try:
            for _ in range(5):
                await asyncio.sleep(0.005)
                ticks += 1
            self.assertEqual(ticks, 5)
            self.assertEqual(len(connected), 2)
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
            await events.aclose()

    async def test_two_routes_are_connected_and_merged(self):
        config = BinanceConfig.from_mapping({"BINANCE_SYMBOLS": "BTCUSDT"})
        public_payload = {
            "data": {
                "e": "bookTicker", "E": 1_700_000_000_100,
                "T": 1_700_000_000_090, "s": "BTCUSDT", "u": 42,
                "b": "60000", "B": "1", "a": "60001", "A": "2",
            }
        }
        market_payload = {
            "data": {
                "e": "markPriceUpdate", "E": 1_700_000_001_000,
                "s": "BTCUSDT", "p": "60000.5", "i": "60000.4",
                "r": "0.0001", "T": 1_700_001_600_000,
            }
        }

        class Socket:
            def __init__(self, payload):
                self.payload = payload

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self.payload is not None:
                    payload, self.payload = self.payload, None
                    return payload
                await asyncio.Future()

        connected = []

        def connect(url, **kwargs):
            del kwargs
            connected.append(url)
            payload = public_payload if "/public/" in url else market_payload
            return Socket(payload)

        stream = BinanceMarketStream(config, connect=connect)
        events = stream.events()
        try:
            first = await asyncio.wait_for(events.__anext__(), timeout=1)
            second = await asyncio.wait_for(events.__anext__(), timeout=1)
        finally:
            await events.aclose()

        self.assertEqual({type(first), type(second)}, {BookTicker, MarkPrice})
        self.assertEqual(set(connected), set(build_stream_urls(config)))


if __name__ == "__main__":
    unittest.main()
