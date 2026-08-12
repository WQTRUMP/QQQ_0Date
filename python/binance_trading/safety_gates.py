"""
[INPUT]: 依赖 RuntimeStateStore 未决订单投影、执行模式与 REST 客户端冷却年龄
[OUTPUT]: 提供 durable_order_symbols、order_unknown_reason 与 rest_cooling_down 纯安全门
[POS]: binance_trading 的运行时授权辅助层；让编排器从持久真源重建阻断，不以内存单槽替代订单事实
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

from typing import Any, Set


def durable_order_symbols(state_store: Any) -> Set[str]:
    if state_store is None:
        return set()
    return {record.intent.symbol for record in state_store.unresolved_orders()}


def order_unknown_reason(symbols: Set[str]) -> str:
    if not symbols:
        return ""
    return "order_state_unknown:%s:DURABLE_UNRESOLVED" % ",".join(sorted(symbols))


def rest_cooling_down(mode: str, client: Any) -> bool:
    return str(mode) == "testnet" and float(
        getattr(client, "rate_limit_remaining_seconds", 0.0)
    ) > 0


__all__ = ["durable_order_symbols", "order_unknown_reason", "rest_cooling_down"]
