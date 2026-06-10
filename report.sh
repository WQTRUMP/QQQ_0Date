#!/bin/bash
# ── 每日盈亏报告（cron 触发）─────────────────────
set -euo pipefail
cd /Users/xncool/Desktop/QQQ_Single
source .venv/bin/activate 2>/dev/null || { echo "⚠️  .venv 未激活，尝试系统 Python"; }
python python/daily_report/main.py || echo "❌ 日报生成失败"
