"""
[INPUT]: 依赖 binance_trading.main 的 dotenv 加载门与临时文件权限
[OUTPUT]: 验证任何模式的 dotenv 都必须为私有权限、启动脚本收紧 umask，且 CLI 模式不可被偷换
[POS]: python/tests 的 Binance 组合根启动安全回归，不启动行情线程或 Dashboard
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from python.binance_trading.config import BinanceConfig
from python.binance_trading.instance_lock import RuntimeInstanceLock
from python.binance_trading.main import _load_config, _stop_runtime, run


ROOT = Path(__file__).resolve().parents[2]
ENV_TEXT = """\
EXECUTION_MODE=paper
BINANCE_ENV=testnet
BINANCE_API_KEY=demo-key
BINANCE_API_SECRET=demo-secret
TRADING_ENABLED=true
BINANCE_TESTNET_TRADING_CONFIRM=TESTNET_ONLY
"""


class BinanceMainTests(unittest.TestCase):
    def test_duplicate_instance_is_rejected_before_runtime_creation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = BinanceConfig.from_mapping(
                {"BINANCE_PAPER_DB_PATH": str(Path(directory) / "paper.db")}
            )
            with RuntimeInstanceLock(config.database_path):
                with mock.patch(
                    "python.binance_trading.main._load_config", return_value=config
                ), mock.patch("python.binance_trading.main._create_runtime") as create:
                    with self.assertRaisesRegex(RuntimeError, "已有运行实例"):
                        run(["paper"])
                    create.assert_not_called()

    def test_shutdown_retries_before_monitoring_can_close(self) -> None:
        runtime = mock.Mock()
        runtime.stop.side_effect = [
            RuntimeError("worker alive"),
            RuntimeError("worker alive"),
            None,
        ]

        _stop_runtime(runtime)

        self.assertEqual(runtime.stop.call_count, 3)

    def test_dotenv_requires_private_permissions_in_every_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env.binance"
            path.write_text(ENV_TEXT, encoding="utf-8")
            for permissions in (0o400, 0o644, 0o700):
                path.chmod(permissions)
                for mode in ("paper", "testnet"):
                    with self.subTest(mode=mode, permissions=oct(permissions)):
                        with self.assertRaisesRegex(PermissionError, "chmod 600"):
                            _load_config(mode, path)

    def test_start_script_sets_private_umask_before_exec(self) -> None:
        script = (ROOT / "start.sh").read_text(encoding="utf-8")
        self.assertIn("umask 077", script)
        self.assertLess(script.index("umask 077"), script.index('exec "$PYTHON_BIN"'))

    def test_cli_testnet_mode_overrides_dotenv_mode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env.binance"
            path.write_text(ENV_TEXT, encoding="utf-8")
            path.chmod(0o600)
            with mock.patch.dict(os.environ, {}, clear=True):
                config = _load_config("testnet", path)
            self.assertEqual(config.mode, "testnet")


if __name__ == "__main__":
    unittest.main()
