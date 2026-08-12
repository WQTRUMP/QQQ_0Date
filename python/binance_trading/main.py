"""
[INPUT]: 依赖私有权限 dotenv、BinanceConfig、RuntimeInstanceLock、必备 runtime.create_runtime 与 dashboard 生命周期封装
[OUTPUT]: 对外提供 main 命令入口，在任何模式拒绝非 0600 dotenv/重复实例，并保持看板直至 worker 被证明退出
[POS]: binance_trading 的薄组合根，负责环境加载、模式所有权和进程级可恢复停机，不承载交易逻辑
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import argparse
import logging
import signal
import stat
import threading
from pathlib import Path
from types import FrameType
from typing import Any, Optional, Sequence, Tuple

from dotenv import load_dotenv

from .config import BinanceConfig
from .dashboard import create_dashboard_server
from .instance_lock import RuntimeInstanceLock


LOGGER = logging.getLogger(__name__)
ALLOWED_MODES = ("paper", "testnet")


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance USDT-M paper/testnet runtime")
    parser.add_argument("mode", nargs="?", choices=ALLOWED_MODES, default="paper")
    parser.add_argument("--env-file", default=".env.binance", help="python-dotenv file (default: .env.binance)")
    parser.add_argument("--host", default=None, help="dashboard loopback host")
    parser.add_argument("--port", type=int, default=None, help="dashboard loopback port")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    return parser.parse_args(argv)


def _load_config(mode: str, env_file: Path) -> BinanceConfig:
    if env_file.is_file():
        permissions = stat.S_IMODE(env_file.stat().st_mode)
        if permissions != 0o600:
            raise PermissionError(
                "Binance dotenv 必须 chmod 600，当前权限 %04o" % permissions
            )
        load_dotenv(dotenv_path=str(env_file), override=False)
    elif mode == "testnet":
        raise FileNotFoundError("testnet mode requires dotenv file: %s" % env_file)
    else:
        LOGGER.info("dotenv file not found; paper mode uses safe defaults: %s", env_file)

    # CLI 模式是启动意图的单一权威，不允许 dotenv 把 paper 偷换为另一模式。
    config = BinanceConfig.from_env(mode_override=mode)
    configured_mode = str(getattr(config, "mode", ""))
    if configured_mode not in ALLOWED_MODES or configured_mode != mode:
        raise ValueError("Binance mode must resolve to CLI-selected paper or testnet")
    if tuple(config.symbols) != ("BTCUSDT",):
        raise ValueError("this runtime entry requires BINANCE_SYMBOLS=BTCUSDT")
    return config


def _create_runtime(config: BinanceConfig) -> Any:
    from .runtime import create_runtime
    runtime = create_runtime(config)
    for method_name in ("start", "stop", "snapshot"):
        if not callable(getattr(runtime, method_name, None)):
            raise TypeError("create_runtime(config) result must provide %s()" % method_name)
    if not any(callable(getattr(runtime, name, None)) for name in ("ready", "is_ready")):
        raise TypeError("create_runtime(config) result must provide ready() or is_ready()")
    return runtime


def _dashboard_settings(args: argparse.Namespace, config: BinanceConfig) -> Tuple[str, int]:
    host = args.host or config.dashboard_host
    port = args.port if args.port is not None else config.dashboard_port
    return host, port


def _stop_runtime(runtime: Any) -> None:
    """反复等待有界 worker 操作结束；期间保留 Dashboard、SQLite 和实例锁。"""

    while True:
        try:
            runtime.stop()
            return
        except RuntimeError as exc:
            LOGGER.error("Binance runtime 尚未安全停止，继续等待: %s", exc)


def run(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    env_file = Path(args.env_file).expanduser().resolve()
    config = _load_config(args.mode, env_file)
    instance_lock = RuntimeInstanceLock(config.database_path)
    runtime = None
    dashboard = None
    stop_event = threading.Event()
    previous_handlers = {}

    def request_stop(signum: int, frame: Optional[FrameType]) -> None:
        del frame
        LOGGER.info("received signal %s; stopping Binance runtime", signum)
        stop_event.set()

    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, request_stop)

    runtime_started = False
    dashboard_started = False
    try:
        runtime = _create_runtime(config)
        host, port = _dashboard_settings(args, config)
        dashboard_html = Path(config.dashboard_html).expanduser()
        dashboard = create_dashboard_server(
            runtime=runtime,
            mode=args.mode,
            host=host,
            port=port,
            html_path=dashboard_html,
            unavailable_reason="runtime is not ready",
        )
        runtime.start()
        runtime_started = True
        dashboard.start()
        dashboard_started = True
        LOGGER.info("Binance %s dashboard: %s", args.mode, dashboard.base_url)
        stop_event.wait()
    finally:
        try:
            if runtime_started:
                _stop_runtime(runtime)
            if dashboard_started:
                dashboard.stop()
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
        finally:
            instance_lock.close()
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
