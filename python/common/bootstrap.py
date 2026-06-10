import asyncio
import os

import nats


NATS_CONNECT_RETRIES = int(os.getenv("NATS_CONNECT_RETRIES", "20"))
NATS_CONNECT_DELAY_SEC = float(os.getenv("NATS_CONNECT_DELAY_SEC", "1.5"))


async def connect_nats_with_retry(
    nats_url: str,
    service_name: str,
    retries: int = NATS_CONNECT_RETRIES,
    delay_sec: float = NATS_CONNECT_DELAY_SEC,
):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            nc = await nats.connect(nats_url)
            print(f"[{service_name}] ✅ NATS 已连接: {nc.connected_url}")
            return nc
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                break
            print(
                f"[{service_name}] NATS 连接失败 ({attempt}/{retries}): {exc}. "
                f"{delay_sec:.1f}s 后重试..."
            )
            await asyncio.sleep(delay_sec)
    raise RuntimeError(
        f"{service_name} 无法连接 NATS {nats_url}，已重试 {retries} 次: {last_error}"
    )
