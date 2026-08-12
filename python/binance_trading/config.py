"""
[INPUT]: 依赖环境变量中的 Binance Demo 域名、凭证、分模式 SQLite 路径、风控与 Dashboard 参数
[OUTPUT]: 提供固定 BTCUSDT/1m 策略边界的 BinanceConfig，在连接前拒绝主网、隐式授权、失真时序、越界风险与账本路径别名
[POS]: binance_trading 的唯一配置防腐层；交易参数保持少而硬，paper 与 testnet 只隔离执行和本地事实库
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urlparse


DEMO_REST_URL = "https://demo-fapi.binance.com"
DEMO_WS_URL = "wss://demo-fstream.binance.com"
TESTNET_CONFIRMATION = "TESTNET_ONLY"
KLINE_INTERVAL = "1m"
SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,24}USDT$")
MIN_REQUEST_TIMEOUT_SEC = 1
MAX_REQUEST_TIMEOUT_SEC = 30
MIN_ACCOUNT_POLL_SECONDS = Decimal("1")
MAX_ACCOUNT_POLL_SECONDS = Decimal("15")
MIN_BOOK_AGE_SECONDS = Decimal("0.25")
MAX_BOOK_AGE_SECONDS = Decimal("10")
MIN_SIGNAL_AGE_SECONDS = Decimal("1")
MAX_SIGNAL_AGE_SECONDS = Decimal("60")


def _raw(values: Mapping[str, Any], key: str, default: Optional[str] = None) -> Optional[str]:
    value = values.get(key, default)
    return None if value is None else str(value).strip()


def _bool(values: Mapping[str, Any], key: str, default: bool) -> bool:
    raw = _raw(values, key)
    if raw is None or raw == "":
        return default
    normalized = raw.lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    raise ValueError("%s 必须是显式布尔值" % key)


def _int(values: Mapping[str, Any], key: str, default: int) -> int:
    raw = _raw(values, key)
    try:
        return default if raw in {None, ""} else int(raw)
    except ValueError as exc:
        raise ValueError("%s 必须是整数" % key) from exc


def _decimal(values: Mapping[str, Any], key: str, default: str) -> Decimal:
    raw = _raw(values, key, default)
    try:
        value = Decimal(raw or default)
    except InvalidOperation as exc:
        raise ValueError("%s 必须是十进制数" % key) from exc
    if not value.is_finite():
        raise ValueError("%s 必须是有限数" % key)
    return value


def _symbols(values: Mapping[str, Any]) -> Tuple[str, ...]:
    raw = _raw(values, "BINANCE_SYMBOLS", "BTCUSDT") or "BTCUSDT"
    symbols = tuple(dict.fromkeys(part.strip().upper() for part in raw.split(",") if part.strip()))
    if not symbols or any(SYMBOL_RE.fullmatch(symbol) is None for symbol in symbols):
        raise ValueError("BINANCE_SYMBOLS 只接受逗号分隔的 USDT 永续 symbol")
    if symbols != ("BTCUSDT",):
        raise ValueError("当前 Dashboard/风控产品面只允许 BINANCE_SYMBOLS=BTCUSDT")
    return symbols


def _canonical_database_path(value: str) -> Path:
    """把 SQLite 文件名投影为不要求文件已存在的绝对物理路径。"""

    try:
        return Path(value).expanduser().resolve(strict=False)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError("Binance SQLite 路径无法规范化") from exc


def _database_paths(values: Mapping[str, Any]) -> Tuple[str, str]:
    """同时解析两种模式，防止路径别名绕过账本隔离。"""

    paper = (
        _raw(values, "BINANCE_PAPER_DB_PATH", "logs/binance_trading.paper.db")
        or "logs/binance_trading.paper.db"
    )
    testnet = (
        _raw(values, "BINANCE_TESTNET_DB_PATH", "logs/binance_trading.testnet.db")
        or "logs/binance_trading.testnet.db"
    )
    paper_path = _canonical_database_path(paper)
    testnet_path = _canonical_database_path(testnet)
    same_file = paper_path == testnet_path
    if not same_file and paper_path.exists() and testnet_path.exists():
        try:
            same_file = paper_path.samefile(testnet_path)
        except OSError:
            same_file = False
    if same_file:
        raise ValueError(
            "BINANCE_PAPER_DB_PATH 与 BINANCE_TESTNET_DB_PATH 的有效路径必须不同"
        )
    return paper, testnet


@dataclass(frozen=True)
class BinanceConfig:
    mode: str
    environment: str
    rest_url: str
    ws_url: str
    api_key: str
    api_secret: str
    symbols: Tuple[str, ...]
    interval: str
    trading_enabled: bool
    recv_window_ms: int
    request_timeout_sec: int
    history_limit: int
    atr_stop_multiple: Decimal
    reward_risk_ratio: Decimal
    risk_per_trade_fraction: Decimal
    max_daily_loss_fraction: Decimal
    max_notional_fraction: Decimal
    max_leverage: int
    max_spread_bps: Decimal
    max_book_age_seconds: Decimal
    signal_max_age_seconds: Decimal
    entry_slippage_bps: Decimal
    paper_fee_bps: Decimal
    paper_starting_equity: Decimal
    account_poll_seconds: Decimal
    database_path: str
    dashboard_host: str
    dashboard_port: int
    dashboard_html: str

    def __post_init__(self) -> None:
        if self.mode not in {"paper", "testnet"}:
            raise ValueError("EXECUTION_MODE 只允许 paper/testnet")
        if self.environment != "testnet":
            raise ValueError("BINANCE_ENV 必须为 testnet")
        if self.rest_url.rstrip("/") != DEMO_REST_URL or self.ws_url.rstrip("/") != DEMO_WS_URL:
            raise ValueError("Binance runtime 只允许官方 Demo REST/WSS 域名")
        if urlparse(self.rest_url).scheme != "https" or urlparse(self.ws_url).scheme != "wss":
            raise ValueError("Binance endpoint 必须使用 TLS")
        if self.interval != KLINE_INTERVAL:
            raise ValueError("BINANCE_KLINE_INTERVAL 必须固定为 1m")
        if not self.symbols or any(SYMBOL_RE.fullmatch(item) is None for item in self.symbols):
            raise ValueError("Binance symbols 无效")
        if self.mode == "testnet" and (not self.api_key or not self.api_secret):
            raise ValueError("testnet 模式必须提供 BINANCE_API_KEY/BINANCE_API_SECRET")
        if self.mode == "testnet" and not self.trading_enabled:
            raise ValueError("testnet 模式必须显式 TRADING_ENABLED=true")
        if self.recv_window_ms <= 0 or self.recv_window_ms > 60_000:
            raise ValueError("BINANCE_RECV_WINDOW_MS 必须在 1..60000")
        if not MIN_REQUEST_TIMEOUT_SEC <= self.request_timeout_sec <= MAX_REQUEST_TIMEOUT_SEC:
            raise ValueError("BINANCE_REQUEST_TIMEOUT_SEC 必须在 1..30")
        if not 50 <= self.history_limit <= 1500:
            raise ValueError("BINANCE_HISTORY_LIMIT 必须在 50..1500")
        if self.atr_stop_multiple <= 0 or self.reward_risk_ratio <= 0:
            raise ValueError("ATR 止损倍数/盈亏比必须为正")
        for name, value in (
            ("risk_per_trade_fraction", self.risk_per_trade_fraction),
            ("max_daily_loss_fraction", self.max_daily_loss_fraction),
            ("max_notional_fraction", self.max_notional_fraction),
        ):
            if not Decimal("0") < value <= Decimal("1"):
                raise ValueError("%s 必须在 (0,1]" % name)
        if self.max_leverage <= 0 or self.max_leverage > 20:
            raise ValueError("MAX_LEVERAGE 必须在 1..20")
        if not MIN_ACCOUNT_POLL_SECONDS <= self.account_poll_seconds <= MAX_ACCOUNT_POLL_SECONDS:
            raise ValueError("ACCOUNT_POLL_SECONDS 必须在 1..15")
        if not MIN_BOOK_AGE_SECONDS <= self.max_book_age_seconds <= MAX_BOOK_AGE_SECONDS:
            raise ValueError("MAX_BOOK_AGE_SECONDS 必须在 0.25..10")
        if not MIN_SIGNAL_AGE_SECONDS <= self.signal_max_age_seconds <= MAX_SIGNAL_AGE_SECONDS:
            raise ValueError("SIGNAL_MAX_AGE_SECONDS 必须在 1..60")
        if (
            self.max_spread_bps <= 0
            or self.entry_slippage_bps < 0
            or self.paper_fee_bps < 0
            or self.paper_starting_equity <= 0
        ):
            raise ValueError("点差/费用/资金参数无效")
        if self.dashboard_host not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("Dashboard 只允许绑定 loopback")
        if not 1 <= self.dashboard_port <= 65535:
            raise ValueError("Dashboard port 无效")

    @classmethod
    def from_mapping(
        cls, values: Mapping[str, Any], mode_override: Optional[str] = None
    ) -> "BinanceConfig":
        mode = (mode_override or _raw(values, "EXECUTION_MODE", "paper") or "paper").lower()
        trading_enabled = _bool(values, "TRADING_ENABLED", False)
        confirmation = _raw(values, "BINANCE_TESTNET_TRADING_CONFIRM", "") or ""
        if mode == "testnet" and confirmation != TESTNET_CONFIRMATION:
            raise ValueError(
                "testnet 模式必须设置 BINANCE_TESTNET_TRADING_CONFIRM=TESTNET_ONLY"
            )
        if _raw(values, "BINANCE_DB_PATH") not in {None, ""}:
            raise ValueError("请使用分模式 BINANCE_PAPER_DB_PATH/BINANCE_TESTNET_DB_PATH")
        paper_db_path, testnet_db_path = _database_paths(values)
        database_path = paper_db_path if mode == "paper" else testnet_db_path
        return cls(
            mode=mode,
            environment=(_raw(values, "BINANCE_ENV", "testnet") or "testnet").lower(),
            rest_url=(_raw(values, "BINANCE_REST_URL", DEMO_REST_URL) or DEMO_REST_URL).rstrip("/"),
            ws_url=(_raw(values, "BINANCE_WS_URL", DEMO_WS_URL) or DEMO_WS_URL).rstrip("/"),
            api_key=_raw(values, "BINANCE_API_KEY", "") or "",
            api_secret=_raw(values, "BINANCE_API_SECRET", "") or "",
            symbols=_symbols(values),
            interval=_raw(values, "BINANCE_KLINE_INTERVAL", KLINE_INTERVAL)
            or KLINE_INTERVAL,
            trading_enabled=trading_enabled,
            recv_window_ms=_int(values, "BINANCE_RECV_WINDOW_MS", 5000),
            request_timeout_sec=_int(values, "BINANCE_REQUEST_TIMEOUT_SEC", 10),
            history_limit=_int(values, "BINANCE_HISTORY_LIMIT", 250),
            atr_stop_multiple=_decimal(values, "ATR_STOP_MULTIPLE", "1.5"),
            reward_risk_ratio=_decimal(values, "REWARD_RISK_RATIO", "2"),
            risk_per_trade_fraction=_decimal(values, "RISK_PER_TRADE_FRACTION", "0.005"),
            max_daily_loss_fraction=_decimal(values, "MAX_DAILY_LOSS_FRACTION", "0.02"),
            max_notional_fraction=_decimal(values, "MAX_NOTIONAL_FRACTION", "0.25"),
            max_leverage=_int(values, "MAX_LEVERAGE", 2),
            max_spread_bps=_decimal(values, "MAX_SPREAD_BPS", "8"),
            max_book_age_seconds=_decimal(values, "MAX_BOOK_AGE_SECONDS", "3"),
            signal_max_age_seconds=_decimal(values, "SIGNAL_MAX_AGE_SECONDS", "15"),
            entry_slippage_bps=_decimal(values, "ENTRY_SLIPPAGE_BPS", "2"),
            paper_fee_bps=_decimal(values, "PAPER_FEE_BPS", "4"),
            paper_starting_equity=_decimal(values, "PAPER_STARTING_EQUITY", "10000"),
            account_poll_seconds=_decimal(values, "ACCOUNT_POLL_SECONDS", "15"),
            database_path=database_path,
            dashboard_host=_raw(values, "BINANCE_DASHBOARD_HOST", "127.0.0.1") or "127.0.0.1",
            dashboard_port=_int(values, "BINANCE_DASHBOARD_PORT", 8765),
            dashboard_html=_raw(values, "BINANCE_DASHBOARD_HTML", "Binance_Dashboard.html")
            or "Binance_Dashboard.html",
        )

    @classmethod
    def from_env(cls, mode_override: Optional[str] = None) -> "BinanceConfig":
        return cls.from_mapping(os.environ, mode_override=mode_override)

    def sanitized(self) -> Dict[str, Any]:
        return {
            "mode": self.mode,
            "environment": self.environment,
            "rest_url": self.rest_url,
            "ws_url": self.ws_url,
            "symbols": list(self.symbols),
            "interval": self.interval,
            "trading_enabled": self.trading_enabled,
            "database_path": self.database_path,
            "dashboard": "%s:%d" % (self.dashboard_host, self.dashboard_port),
            "api_key_configured": bool(self.api_key),
            "api_secret_configured": bool(self.api_secret),
        }


__all__ = [
    "BinanceConfig",
    "DEMO_REST_URL",
    "DEMO_WS_URL",
    "KLINE_INTERVAL",
    "TESTNET_CONFIRMATION",
]
