#!/usr/bin/env python3
"""
Longbridge Gateway — QQQ 0DTE 期权行情桥接（Docker 兼容入口）
==========================================================
只做行情转发：Longbridge → NATS。

发布到 NATS 的 subject:
  quote.option.qqqus            → QQQ 正股实时价
  quote.option.{symbol_key}     → 单腿期权实时行情（含 IV、OI）
"""

import asyncio
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from longbridge.openapi import Config, OAuthBuilder, PushQuote, QuoteContext, SubType

from python.common.bootstrap import connect_nats_with_retry


def load_env():
    env_file = Path(__file__).resolve().parents[2] / ".env.longbridge"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and val and key not in os.environ:
            os.environ[key] = val


load_env()

NATS_URL = os.getenv("NATS_URL", "nats://127.0.0.1:4222")
QQQ_SYMBOL = "QQQ.US"
QQQ_SUBJECT = "quote.option.qqqus"
MIN_DAYS = int(os.getenv("MIN_DAYS_TO_EXPIRY", "0"))
MAX_DAYS = int(os.getenv("MAX_DAYS_TO_EXPIRY", "0"))
IV_REFRESH_INTERVAL = float(os.getenv("IV_REFRESH_INTERVAL", "2.0"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def symbol_key(symbol: str) -> str:
    return "".join(ch for ch in symbol.lower() if ch.isalnum())


def to_option_quote_msg(opt) -> dict:
    ext = opt.option_extend
    msg = {
        "symbol": opt.symbol,
        "last_done": str(opt.last_done) if opt.last_done else "0",
        "prev_close": str(opt.prev_close) if opt.prev_close else "0",
        "open": str(opt.open) if opt.open else "0",
        "high": str(opt.high) if opt.high else "0",
        "low": str(opt.low) if opt.low else "0",
        "timestamp": int(opt.timestamp) if opt.timestamp else 0,
        "volume": int(opt.volume) if opt.volume else 0,
        "turnover": str(opt.turnover) if opt.turnover else "0",
        "option_extend": None,
    }
    if ext:
        msg["option_extend"] = {
            "implied_volatility": str(ext.implied_volatility) if ext.implied_volatility else "0",
            "open_interest": int(ext.open_interest) if ext.open_interest else 0,
            "expiry_date": str(ext.expiry_date) if ext.expiry_date else "",
            "strike_price": str(ext.strike_price) if ext.strike_price else "0",
            "contract_multiplier": str(ext.contract_multiplier) if ext.contract_multiplier else "100",
            "contract_type": str(ext.contract_type) if ext.contract_type else "A",
            "contract_size": str(ext.contract_size) if ext.contract_size else "100",
            "direction": str(ext.direction) if ext.direction else "",
            "historical_volatility": str(ext.historical_volatility) if ext.historical_volatility else "0",
            "underlying_symbol": str(ext.underlying_symbol) if ext.underlying_symbol else "",
        }
    return msg


def to_qqq_quote_msg(event: PushQuote) -> dict:
    return {
        "symbol": QQQ_SYMBOL,
        "last_done": str(event.last_done) if getattr(event, "last_done", None) else "0",
        "prev_close": str(event.prev_close) if getattr(event, "prev_close", None) else "0",
        "open": str(event.open) if getattr(event, "open", None) else "0",
        "high": str(event.high) if getattr(event, "high", None) else "0",
        "low": str(event.low) if getattr(event, "low", None) else "0",
        "timestamp": int(event.timestamp) if getattr(event, "timestamp", None) else 0,
        "volume": int(event.volume) if getattr(event, "volume", None) else 0,
        "turnover": str(event.turnover) if getattr(event, "turnover", None) else "0",
        "option_extend": None,
    }


class OptionChain:
    def __init__(self):
        self.expiry_date: Optional[date] = None
        self.symbols: list[str] = []
        self.is_loaded = False


async def load_option_chain(ctx: QuoteContext, chain: OptionChain):
    try:
        dates = ctx.option_chain_expiry_date_list(QQQ_SYMBOL)
        if not dates:
            print("[gateway] 未获取到到期日")
            return
        today = date.today()
        candidates = [d for d in dates if isinstance(d, date) and MIN_DAYS <= (d - today).days <= MAX_DAYS]
        chain.expiry_date = candidates[0] if candidates else dates[0]
        print(f"[gateway] 到期日: {chain.expiry_date}")

        strikes = ctx.option_chain_info_by_date(QQQ_SYMBOL, chain.expiry_date)
        for row in strikes:
            if row.call_symbol:
                chain.symbols.append(row.call_symbol)
            if row.put_symbol:
                chain.symbols.append(row.put_symbol)

        chain.is_loaded = True
        print(f"[gateway] 期权合约数: {len(chain.symbols)}")
    except Exception as exc:
        print(f"[gateway] 加载期权链失败: {exc}")


async def main():
    print(f"[gateway] 启动时间: {now_iso()}")
    nc = await connect_nats_with_retry(NATS_URL, "gateway")

    config = Config.from_env()
    client_id = os.getenv("LONGBRIDGE_OAUTH_CLIENT_ID", "")
    if client_id:
        try:
            oauth = OAuthBuilder(client_id).build(
                lambda url: print(f"[gateway] 打开浏览器授权:\n{url}")
            )
            config = Config.from_oauth(oauth)
        except Exception:
            pass

    print("[gateway] 连接 Longbridge...")
    ctx = QuoteContext(config)
    chain = OptionChain()
    await load_option_chain(ctx, chain)
    if not chain.is_loaded:
        print("[gateway] 期权链加载失败，退出")
        return

    def on_quote(symbol: str, event: PushQuote):
        try:
            px = float(event.last_done) if getattr(event, "last_done", None) else 0.0
            if px <= 0:
                return
            if symbol == QQQ_SYMBOL:
                asyncio.ensure_future(
                    nc.publish(QQQ_SUBJECT, json.dumps(to_qqq_quote_msg(event)).encode())
                )
            else:
                msg = {
                    "symbol": symbol,
                    "last_done": str(event.last_done),
                    "option_extend": None,
                }
                asyncio.ensure_future(
                    nc.publish(f"quote.option.{symbol_key(symbol)}", json.dumps(msg).encode())
                )
        except Exception:
            pass

    ctx.set_on_quote(on_quote)
    all_symbols = [QQQ_SYMBOL] + chain.symbols
    print(f"[gateway] 订阅 {len(all_symbols)} 个标的...")
    ctx.subscribe(all_symbols, [SubType.Quote], is_first_push=True)

    async def iv_refresh_loop():
        while True:
            await asyncio.sleep(IV_REFRESH_INTERVAL)
            if not chain.symbols:
                continue
            try:
                batch = ctx.option_quote(chain.symbols)
                if not batch:
                    continue
                tasks = [
                    nc.publish(
                        f"quote.option.{symbol_key(opt.symbol)}",
                        json.dumps(to_option_quote_msg(opt)).encode(),
                    )
                    for opt in batch
                ]
                if tasks:
                    await asyncio.gather(*tasks)
            except Exception as exc:
                print(f"[gateway] IV 拉取异常: {exc}")

    asyncio.create_task(iv_refresh_loop())
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
