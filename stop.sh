#!/bin/bash
# ── QQQ_Single 收盘停止 ─────────────────────────────
# 0DTE 期权下午 4:00 ET 到期 → 北京时间凌晨 4:00 关闸
set -euo pipefail
cd /Users/xncool/Desktop/QQQ_Single

echo "=== QQQ_Single 收盘停止 $(date) ==="

# 按进程名关停
for name in longbridge_gateway premarket_init market_status \
            realtime_engine greeks_engine risk_engine \
            strategy_engine dashboard_bridge trade_logger \
            signal_challenger longbridge_executor \
            position_tracker market_regime price_action; do
    pkill -f "$name" 2>/dev/null && echo "  已停止: $name" || true
done

sleep 2
echo "=== 全服务已停止 $(date) ==="
