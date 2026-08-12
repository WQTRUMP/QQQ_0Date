"""
[INPUT]: 依赖已验证 Binance SQLite 路径、文件 inode、POSIX fcntl flock 与私有权限文件
[OUTPUT]: 提供 RuntimeInstanceLock，在创建网络客户端前为规范路径及其硬链接共享的恢复真源建立单实例所有权
[POS]: binance_trading 的进程级所有权边界；路径锁覆盖替换，inode 锁覆盖硬链接，SQLite 派发 CAS 仍是最终防线
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import errno
import fcntl
import os
import tempfile
from pathlib import Path
from typing import List


def _private_lock(path: Path, database: Path) -> int:
    flags = os.O_CREAT | os.O_RDWR
    for name in ("O_CLOEXEC", "O_NOFOLLOW"):
        if hasattr(os, name):
            flags |= getattr(os, name)
    fd = os.open(str(path), flags, 0o600)
    try:
        if os.fstat(fd).st_uid != os.getuid():
            raise PermissionError("锁文件所有者不是当前用户")
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except OSError as exc:
        os.close(fd)
        if exc.errno in (errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK):
            raise RuntimeError("同一 Binance 恢复库已有运行实例: %s" % database) from exc
        raise


class RuntimeInstanceLock:
    """持有规范化数据库身份对应的非阻塞 POSIX 独占锁。"""

    def __init__(self, database_path: str) -> None:
        database = Path(database_path).expanduser().resolve(strict=False)
        self.database_path = database
        self.lock_path = Path(str(database) + ".runtime.lock")
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        database_fd = os.open(str(database), os.O_CREAT | os.O_RDWR, 0o600)
        os.close(database_fd)
        identity = database.stat()
        self.identity_lock_path = Path(tempfile.gettempdir()) / (
            "binance-runtime-%d-%d-%d.lock" % (os.getuid(), identity.st_dev, identity.st_ino)
        )
        self._fds: List[int] = []
        try:
            self._fds.append(_private_lock(self.lock_path, database))
            self._fds.append(_private_lock(self.identity_lock_path, database))
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        fds, self._fds = self._fds, []
        for fd in reversed(fds):
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def __enter__(self) -> "RuntimeInstanceLock":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


__all__ = ["RuntimeInstanceLock"]
