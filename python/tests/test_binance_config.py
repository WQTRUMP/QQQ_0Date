"""
[INPUT]: 依赖 binance_trading 配置、进程工作目录与线性合约值对象
[OUTPUT]: 验证固定 BTCUSDT/1m、Demo/Testnet 域名硬门、显式授权、时序窗口、分模式 SQLite 与 Decimal/UTC 不变量
[POS]: Binance 单产品面的最小安全契约回归，防止现场参数扩张或配置别名污染事实
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from python.binance_trading.config import BinanceConfig
from python.binance_trading.models import BookTicker, Direction, OrderIntent, SymbolRules


class BinanceConfigTests(unittest.TestCase):
    def test_paper_defaults_to_demo_and_keeps_trading_disabled(self):
        config = BinanceConfig.from_mapping({})

        self.assertEqual(config.mode, "paper")
        self.assertEqual(config.symbols, ("BTCUSDT",))
        self.assertEqual(config.interval, "1m")
        self.assertEqual(config.account_poll_seconds, Decimal("15"))
        self.assertFalse(config.trading_enabled)
        self.assertNotIn("api_key", config.sanitized())
        self.assertNotIn("api_secret", config.sanitized())

    def test_production_or_custom_endpoints_are_rejected(self):
        for key, value in (
            ("BINANCE_REST_URL", "https://fapi.binance.com"),
            ("BINANCE_WS_URL", "wss://fstream.binance.com"),
            ("BINANCE_REST_URL", "https://example.test"),
        ):
            with self.subTest(key=key, value=value):
                with self.assertRaisesRegex(ValueError, "Demo"):
                    BinanceConfig.from_mapping({key: value})

    def test_testnet_requires_keys_trading_flag_and_exact_confirmation(self):
        base = {
            "EXECUTION_MODE": "testnet",
            "BINANCE_API_KEY": "demo-key",
            "BINANCE_API_SECRET": "demo-secret",
            "TRADING_ENABLED": "true",
            "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
        }
        config = BinanceConfig.from_mapping(base)
        self.assertEqual(config.mode, "testnet")

        for key, value in (
            ("BINANCE_API_KEY", ""),
            ("BINANCE_API_SECRET", ""),
            ("TRADING_ENABLED", "false"),
            ("BINANCE_TESTNET_TRADING_CONFIRM", "I_UNDERSTAND"),
        ):
            candidate = dict(base)
            candidate[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                BinanceConfig.from_mapping(candidate)

    def test_symbols_and_risk_bounds_fail_closed(self):
        with self.assertRaises(ValueError):
            BinanceConfig.from_mapping({"BINANCE_SYMBOLS": "BTCUSDT,ETHUSD"})
        with self.assertRaisesRegex(ValueError, "BTCUSDT"):
            BinanceConfig.from_mapping({"BINANCE_SYMBOLS": "ETHUSDT"})
        with self.assertRaisesRegex(ValueError, "1m"):
            BinanceConfig.from_mapping({"BINANCE_KLINE_INTERVAL": "5m"})
        with self.assertRaises(ValueError):
            BinanceConfig.from_mapping({"RISK_PER_TRADE_FRACTION": "1.1"})
        with self.assertRaisesRegex(ValueError, "分模式"):
            BinanceConfig.from_mapping({"BINANCE_DB_PATH": "logs/shared.db"})
        with self.assertRaises(ValueError):
            BinanceConfig.from_mapping({"BINANCE_DASHBOARD_HOST": "0.0.0.0"})
        self.assertEqual(
            BinanceConfig.from_mapping({"BINANCE_DASHBOARD_HOST": "::1"}).dashboard_host,
            "::1",
        )

    def test_runtime_timing_windows_have_conservative_closed_bounds(self):
        valid_edges = {
            "BINANCE_REQUEST_TIMEOUT_SEC": ("1", "30"),
            "ACCOUNT_POLL_SECONDS": ("1", "15"),
            "MAX_BOOK_AGE_SECONDS": ("0.25", "10"),
            "SIGNAL_MAX_AGE_SECONDS": ("1", "60"),
        }
        invalid_edges = {
            "BINANCE_REQUEST_TIMEOUT_SEC": ("0", "31", "86400"),
            "ACCOUNT_POLL_SECONDS": ("0.99", "16", "86400"),
            "MAX_BOOK_AGE_SECONDS": ("0.24", "11", "86400"),
            "SIGNAL_MAX_AGE_SECONDS": ("0.99", "61", "86400"),
        }

        for key, values in valid_edges.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    BinanceConfig.from_mapping({key: value})
        for key, values in invalid_edges.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    with self.assertRaisesRegex(ValueError, key):
                        BinanceConfig.from_mapping({key: value})

    def test_paper_and_testnet_databases_reject_same_effective_path(self):
        relative_path = "logs/binance-shared-config-test.db"
        absolute_path = str((Path.cwd() / relative_path).resolve())
        aliases = (
            (relative_path, relative_path),
            ("logs/../binance-shared-config-test.db", "binance-shared-config-test.db"),
            (relative_path, absolute_path),
        )

        for paper_path, testnet_path in aliases:
            with self.subTest(paper=paper_path, testnet=testnet_path):
                with self.assertRaisesRegex(ValueError, "有效路径必须不同"):
                    BinanceConfig.from_mapping(
                        {
                            "BINANCE_PAPER_DB_PATH": paper_path,
                            "BINANCE_TESTNET_DB_PATH": testnet_path,
                        }
                    )

    def test_from_mapping_still_selects_only_the_requested_mode_database(self):
        paper = BinanceConfig.from_mapping(
            {
                "BINANCE_PAPER_DB_PATH": "state/paper.db",
                "BINANCE_TESTNET_DB_PATH": "state/testnet.db",
            }
        )
        testnet = BinanceConfig.from_mapping(
            {
                "EXECUTION_MODE": "testnet",
                "BINANCE_API_KEY": "demo-key",
                "BINANCE_API_SECRET": "demo-secret",
                "TRADING_ENABLED": "true",
                "BINANCE_TESTNET_TRADING_CONFIRM": "TESTNET_ONLY",
                "BINANCE_PAPER_DB_PATH": "state/paper.db",
                "BINANCE_TESTNET_DB_PATH": "state/testnet.db",
            }
        )

        self.assertEqual(paper.database_path, "state/paper.db")
        self.assertEqual(testnet.database_path, "state/testnet.db")


class BinanceModelTests(unittest.TestCase):
    def test_symbol_rules_floor_fractional_quantity_and_round_price_by_side(self):
        rules = SymbolRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.10"),
            step_size=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal("100"),
            min_notional=Decimal("5"),
        )

        self.assertEqual(rules.floor_quantity(Decimal("0.1239")), Decimal("0.123"))
        self.assertEqual(rules.price_for_side(Decimal("62000.01"), "BUY"), Decimal("62000.10"))
        self.assertEqual(rules.price_for_side(Decimal("62000.09"), "SELL"), Decimal("62000.00"))

    def test_book_rejects_crossed_market_and_order_enforces_reduce_only_side(self):
        now = datetime.now(timezone.utc)
        with self.assertRaises(ValueError):
            BookTicker(
                "BTCUSDT",
                Decimal("10"),
                Decimal("1"),
                Decimal("10"),
                Decimal("1"),
                now,
                now,
                1,
            )
        with self.assertRaisesRegex(ValueError, "reduceOnly"):
            OrderIntent(
                client_order_id="exit-1",
                symbol="BTCUSDT",
                direction=Direction.LONG,
                side="BUY",
                quantity=Decimal("0.01"),
                order_type="MARKET",
                limit_price=None,
                reduce_only=True,
                created_at=now,
                signal_id="signal-1",
            )

    def test_entry_order_rejects_invalid_protection_prices(self):
        now = datetime.now(timezone.utc)
        common = dict(
            client_order_id="entry-1",
            symbol="BTCUSDT",
            direction=Direction.SHORT,
            side="SELL",
            quantity=Decimal("0.01"),
            order_type="LIMIT",
            limit_price=Decimal("60000"),
            reduce_only=False,
            created_at=now,
            signal_id="signal-1",
            stop_price=Decimal("135000"),
        )
        with self.assertRaisesRegex(ValueError, "必须为正数"):
            OrderIntent(target_price=Decimal("-90000"), **common)
        with self.assertRaisesRegex(ValueError, "交易方向"):
            OrderIntent(
                target_price=Decimal("50000"),
                **dict(common, stop_price=Decimal("55000")),
            )


if __name__ == "__main__":
    unittest.main()
