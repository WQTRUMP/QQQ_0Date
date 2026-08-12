"""
[INPUT]: 依赖 asyncio event-loop 单调时钟与 Binance server-time 同步调用
[OUTPUT]: 提供 ExchangeClockSync，以最多 300 秒周期管理服务端时钟偏移，失败后保持立即重试资格
[POS]: binance_trading 的时钟防腐层；为 runtime 提供不受墙钟跳变影响的周期校时调度
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Callable, Optional


MAX_SYNC_INTERVAL_SECONDS = 300.0


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _event_loop_time() -> float:
    return asyncio.get_running_loop().time()


class ExchangeClockSync:
    """Synchronize server offset on a monotonic schedule without hiding failures."""

    def __init__(
        self,
        sync_time: Callable[[], int],
        monotonic: Optional[Callable[[], float]] = None,
        interval_seconds: float = MAX_SYNC_INTERVAL_SECONDS,
    ) -> None:
        interval = float(interval_seconds)
        if interval <= 0 or interval > MAX_SYNC_INTERVAL_SECONDS:
            raise ValueError("clock sync interval 必须位于 (0, 300] 秒")
        self._sync_time = sync_time
        self._monotonic = monotonic or _event_loop_time
        self._interval_seconds = interval
        self._deadline: Optional[float] = None
        self._offset_ms = 0

    @property
    def offset_ms(self) -> int:
        return self._offset_ms

    @property
    def remaining_seconds(self) -> float:
        if self._deadline is None:
            return 0.0
        return max(0.0, self._deadline - float(self._monotonic()))

    async def synchronize(self, *, force: bool = False) -> bool:
        if not force and self.remaining_seconds > 0:
            return False
        try:
            offset_ms = int(await asyncio.to_thread(self._sync_time))
        except Exception:
            self._deadline = None
            raise
        self._offset_ms = offset_ms
        self._deadline = float(self._monotonic()) + self._interval_seconds
        return True

    async def retry_after_timestamp_error(self) -> bool:
        """Try immediately; preserve a due schedule when the retry itself fails."""
        try:
            return await self.synchronize(force=True)
        except Exception:
            return False


__all__ = ["ExchangeClockSync", "MAX_SYNC_INTERVAL_SECONDS", "utc_now"]
