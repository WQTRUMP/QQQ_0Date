#!/usr/bin/env python3
"""
Daily PnL Report — 每日盈亏报告
================================
收盘后从 trades.db 拉取当日交易数据，生成摘要报告。
用法: python python/daily_report/main.py
"""

from typing import Optional
import os
import sqlite3
from datetime import date, datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "logs" / "trades.db"


def report(today: Optional[str] = None) -> str:
    if today is None:
        today = date.today().isoformat()

    if not DB_PATH.exists():
        return f"⚠️ {today}: trades.db 不存在，没有交易记录"

    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row

    # ── 当日汇总 ──
    summary = db.execute(
        """SELECT
             COUNT(*)                                                AS total_trades,
             SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END)               AS wins,
             SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END)              AS losses,
             ROUND(SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END), 2)   AS gross_profit,
             ROUND(SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END), 2)   AS gross_loss,
             ROUND(SUM(pnl), 2)                                      AS net_pnl,
             ROUND(AVG(CASE WHEN pnl > 0 THEN pnl ELSE NULL END), 2) AS avg_win,
             ROUND(AVG(CASE WHEN pnl < 0 THEN pnl ELSE NULL END), 2) AS avg_loss,
             ROUND(AVG(held_minutes), 0)                             AS avg_held_min
           FROM trade_summary
           WHERE session_date = ? AND pnl IS NOT NULL""",
        (today,),
    ).fetchone()

    # ── 按策略拆分 ──
    by_strategy = db.execute(
        """SELECT
             strategy,
             COUNT(*)                                AS trades,
             SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS wins,
             ROUND(SUM(pnl), 2)                       AS net_pnl,
             ROUND(AVG(held_minutes), 0)              AS avg_held
           FROM trade_summary
           WHERE session_date = ? AND pnl IS NOT NULL
           GROUP BY strategy
           ORDER BY net_pnl DESC""",
        (today,),
    ).fetchall()

    # ── 信号统计 ──
    signal_stats = db.execute(
        """SELECT
             COUNT(*)                                  AS signals,
             SUM(CASE WHEN decision = 'Approved' THEN 1 ELSE 0 END) AS approved,
             SUM(CASE WHEN decision = 'Rejected' THEN 1 ELSE 0 END) AS rejected
           FROM trade_events
           WHERE session_date = ? AND event_type = 'risk'""",
        (today,),
    ).fetchone()

    # ── 最佳/最差单笔 ──
    best = db.execute(
        "SELECT strategy, symbol, ROUND(pnl,2) as pnl, held_minutes FROM trade_summary WHERE session_date=? AND pnl IS NOT NULL ORDER BY pnl DESC LIMIT 1",
        (today,),
    ).fetchone()
    worst = db.execute(
        "SELECT strategy, symbol, ROUND(pnl,2) as pnl, held_minutes FROM trade_summary WHERE session_date=? AND pnl IS NOT NULL ORDER BY pnl ASC LIMIT 1",
        (today,),
    ).fetchone()

    db.close()

    # ── 构建报告 ──
    lines = []
    lines.append(f"📊 QQQ 0DTE 日报 — {today}")
    lines.append("")

    if summary is None or summary["total_trades"] == 0:
        lines.append("今日无成交。")
        if signal_stats and signal_stats["signals"]:
            lines.append(f"信号: {signal_stats['signals']} 个, 批准 {signal_stats['approved']}, 拒绝 {signal_stats['rejected']}")
        return "\n".join(lines)

    s = summary
    win_rate = s["wins"] / s["total_trades"] * 100 if s["total_trades"] else 0
    profit_factor = abs(s["gross_profit"] / s["gross_loss"]) if s["gross_loss"] and s["gross_loss"] != 0 else float("inf")

    lines.append(f"**净盈亏: ${s['net_pnl']:+.2f}**")
    lines.append(f"交易: {s['total_trades']} 笔 | 胜 {s['wins']} | 负 {s['losses']} | 胜率 {win_rate:.0f}%")
    lines.append(f"毛利: ${s['gross_profit']:+.2f} | 毛亏: ${s['gross_loss']:.2f} | 盈亏比: {profit_factor:.1f}")
    lines.append(f"均盈: ${s['avg_win']:+.2f} | 均亏: ${s['avg_loss']:.2f} | 均持: {s['avg_held_min']:.0f}分钟")
    lines.append("")

    if by_strategy:
        lines.append("**按策略:**")
        for r in by_strategy:
            wr = r["wins"] / r["trades"] * 100 if r["trades"] else 0
            lines.append(f"  {r['strategy']}: {r['trades']}笔 | 胜{wr:.0f}% | ${r['net_pnl']:+.2f} | 均{r['avg_held']}min")

    if best:
        lines.append(f"\n**最佳:** {best['strategy']} {best['symbol']} ${best['pnl']:+.2f} ({best['held_minutes']}min)")

    if worst:
        lines.append(f"**最差:** {worst['strategy']} {worst['symbol']} ${worst['pnl']:+.2f} ({worst['held_minutes']}min)")

    if signal_stats:
        ss = signal_stats
        lines.append(f"\n信号: {ss['signals']} → 批准 {ss['approved']} | 拒绝 {ss['rejected']}")

    # 一句话总结
    if s["net_pnl"] >= 0:
        lines.append(f"\n🟢 今日盈利 ${s['net_pnl']:+.2f}，系统运行正常。")
    else:
        lines.append(f"\n🔴 今日亏损 ${s['net_pnl']:.2f}，需复盘。")

    return "\n".join(lines)


def cleanup():
    """清理过期日志和数据库"""
    import shutil

    log_dir = Path(__file__).parent.parent.parent / "logs"
    today = date.today()

    # 删除 30 天前的日志目录
    for d in log_dir.iterdir():
        if d.is_dir() and d.name != "." and len(d.name) == 10:  # YYYY-MM-DD 格式
            try:
                dt = date.fromisoformat(d.name)
                if (today - dt).days > 30:
                    shutil.rmtree(d)
                    print(f"[cleanup] 删除旧日志: {d.name}")
            except ValueError:
                pass

    # SQLite vacuum（回收空间）
    if DB_PATH.exists():
        db = sqlite3.connect(str(DB_PATH))
        db.execute("VACUUM")
        # 删除 90 天前的原始事件（保留汇总表）
        cutoff = today.replace(year=today.year - 1) if False else today  # 先不删事件
        db.execute(
            "DELETE FROM trade_events WHERE session_date < date('now', '-90 days')"
        )
        db.commit()
        db.close()


if __name__ == "__main__":
    # 打印报告
    print(report())
    # 清理旧日志
    cleanup()
