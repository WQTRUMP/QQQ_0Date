"""
[INPUT]: 依赖 RuntimeInstanceLock、临时数据库路径/硬链接与 POSIX 文件权限
[OUTPUT]: 验证同一规范路径或 inode 只允许一个进程所有者，释放后可重获且双锁文件保持 0600
[POS]: python/tests 的 Binance 进程级防重复启动回归，与 dispatch 的 SQLite CAS 构成双层提交保护
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import stat
import os
import tempfile
import unittest
from pathlib import Path

from python.binance_trading.instance_lock import RuntimeInstanceLock


class RuntimeInstanceLockTests(unittest.TestCase):
    def test_same_canonical_database_has_one_owner_until_release(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "nested" / "runtime.db"
            alias = root / "nested" / ".." / "nested" / "runtime.db"

            with RuntimeInstanceLock(str(database)) as owner:
                with self.assertRaisesRegex(RuntimeError, "已有运行实例"):
                    RuntimeInstanceLock(str(alias))
                self.assertEqual(stat.S_IMODE(owner.lock_path.stat().st_mode), 0o600)
                self.assertEqual(
                    stat.S_IMODE(owner.identity_lock_path.stat().st_mode), 0o600
                )

            with RuntimeInstanceLock(str(alias)) as next_owner:
                self.assertEqual(next_owner.database_path, database.resolve())

    def test_existing_hardlink_alias_cannot_bypass_instance_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "runtime.db"
            database.touch(mode=0o600)
            alias = root / "runtime-hardlink.db"
            os.link(database, alias)

            with RuntimeInstanceLock(str(database)):
                with self.assertRaisesRegex(RuntimeError, "已有运行实例"):
                    RuntimeInstanceLock(str(alias))


if __name__ == "__main__":
    unittest.main()
