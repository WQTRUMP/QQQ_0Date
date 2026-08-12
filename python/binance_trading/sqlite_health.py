"""
[INPUT]: 依赖已打开但尚未写入 schema/业务事实的 SQLite connection
[OUTPUT]: 提供 require_sqlite_quick_check，只接受完整单行 ok 并把损坏或执行异常转成失败关闭
[POS]: binance_trading 的共享恢复库完整性门；供 paper 账本与订单/保护状态库复用，禁止各自演化检查语义
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import sqlite3


def require_sqlite_quick_check(
    connection: sqlite3.Connection, store_name: str
) -> None:
    try:
        rows = connection.execute("PRAGMA quick_check").fetchall()
        results = tuple(str(row[0]).strip() for row in rows)
    except Exception as exc:
        raise RuntimeError("%s SQLite quick_check 执行失败" % store_name) from exc
    if results != ("ok",):
        detail = "; ".join(value[:160] for value in results[:3]) or "无结果"
        raise RuntimeError("%s SQLite quick_check 失败: %s" % (store_name, detail))


__all__ = ["require_sqlite_quick_check"]
