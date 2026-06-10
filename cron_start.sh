#!/bin/bash
# ── QQQ_Single Cron 启动（实盘版 · 四策略并行）─────────
# 由 Hermes cronjob 定时触发，周一至五 21:30 北京时间
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

TODAY=$(date +%Y-%m-%d)
LOGDIR="logs/${TODAY}"
mkdir -p "$LOGDIR"

echo "=== QQQ_Single 实盘启动 $(date) ==="
echo "日志目录: $LOGDIR"

# 环境
source .venv/bin/activate
if [ -f ".env.longbridge" ]; then
    set -a; source .env.longbridge; set +a
fi

wait_for_tcp() {
    local host="$1" port="$2" name="$3"
    python3 - "$host" "$port" "$name" <<'PY'
import socket, sys, time
host, port, name = sys.argv[1], int(sys.argv[2]), sys.argv[3]
deadline = time.time() + 20
while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=1):
            print(f"{name} ready")
            raise SystemExit(0)
    except OSError:
        time.sleep(1)
raise SystemExit(f"{name} not ready on {host}:{port}")
PY
}

wait_for_http() {
    local url="$1" name="$2"
    python3 - "$url" "$name" <<'PY'
import sys, time, urllib.request
url, name = sys.argv[1], sys.argv[2]
deadline = time.time() + 20
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            if 200 <= resp.status < 300:
                print(f"{name} ready")
                raise SystemExit(0)
    except Exception:
        time.sleep(1)
raise SystemExit(f"{name} not ready at {url}")
PY
}

# ── 基础设施 ──────────────────────────
# 确保 NATS 和 Redis 在跑
pgrep -q nats-server || { echo "启动 NATS..."; brew services start nats-server 2>/dev/null || nats-server -js & }
wait_for_tcp 127.0.0.1 4222 "NATS"
pgrep -q redis-server || { echo "启动 Redis..."; brew services start redis 2>/dev/null || redis-server --daemonize yes; }
wait_for_tcp 127.0.0.1 6379 "Redis"

# 0. 交易日志收集器（最先启动，确保不丢事件）
echo "[cron] 启动 Trade Logger..."
python python/trade_logger/main.py > "${LOGDIR}/trade_logger.log" 2>&1 &
sleep 1

echo "[cron] 启动 Position Tracker..."
python python/position_tracker/main.py > "${LOGDIR}/position_tracker.log" 2>&1 &
sleep 1

# 1b. 盘前初始化
echo "[cron] 启动 Premarket Init..."
env DAILY_BARS_FILE="data/daily_bars_qqq.json" ./target/release/premarket_init > "${LOGDIR}/premarket_init.log" 2>&1 &
sleep 2

# 0c. 市场状态 (OPEN/CLOSE 信号)
echo "[cron] 启动 Market Status..."
./target/release/market_status > "${LOGDIR}/market_status.log" 2>&1 &
sleep 1

# 1. Longbridge 行情网关 (Go)
echo "[cron] 启动 Gateway..."
./target/longbridge_gateway > "${LOGDIR}/gateway.log" 2>&1 &
sleep 2

# 2. Dashboard Bridge (Python)
echo "[cron] 启动 Dashboard Bridge..."
env NATS_URL="nats://127.0.0.1:4222" python python/dashboard_bridge/main.py > "${LOGDIR}/dashboard_bridge.log" 2>&1 &
wait_for_http "http://127.0.0.1:8765/healthz" "Dashboard"

# 2b. Market Regime（体制感知 → 动态策略权重）
echo "[cron] 启动 Market Regime..."
python python/market_regime/main.py > "${LOGDIR}/market_regime.log" 2>&1 &
sleep 1

# 3. Realtime Engine (Rust)
echo "[cron] 启动 Realtime Engine..."
./target/release/realtime_engine > "${LOGDIR}/realtime_engine.log" 2>&1 &
sleep 1

# 4. Greeks Engine (Rust)
echo "[cron] 启动 Greeks Engine..."
./target/release/greeks_engine > "${LOGDIR}/greeks_engine.log" 2>&1 &
sleep 1

# 5. 信号挑战者（轿前杠精 — 过滤脏信号）
echo "[cron] 启动 Signal Challenger..."
python python/signal_challenger/main.py > "${LOGDIR}/challenger.log" 2>&1 &
sleep 1

# 6. 策略引擎 ×4
echo "[cron] 启动 Momentum 策略..."
env STRATEGY_MODE="momentum" python python/strategy_engine/main.py > "${LOGDIR}/strategy_momentum.log" 2>&1 &
sleep 1

echo "[cron] 启动 ThetaHarvest 策略 (3pt 价差)..."
env STRATEGY_MODE="theta_harvest" STRATEGY_ID="theta_harvest_v0" SPREAD_WING_WIDTH="3.0" python python/strategy_engine/main.py > "${LOGDIR}/strategy_theta_harvest.log" 2>&1 &
sleep 1

echo "[cron] 启动 GammaScalp 策略..."
env STRATEGY_MODE="gamma_scalp" python python/strategy_engine/main.py > "${LOGDIR}/strategy_gamma_scalp.log" 2>&1 &
sleep 1

echo "[cron] 启动 PriceAction 策略..."
env STRATEGY_MODE="price_action" python python/strategy_engine/main.py > "${LOGDIR}/strategy_price_action.log" 2>&1 &
sleep 1

# 7. Risk Engine (Rust) — 3pt 价差风控
echo "[cron] 启动 Risk Engine..."
env SPREAD_WING_WIDTH="3.0" ./target/release/risk_engine > "${LOGDIR}/risk_engine.log" 2>&1 &
sleep 1

# 8. Executor (Python) — 实盘
echo "[cron] 启动 Executor (LIVE)..."
env EXECUTION_MODE="live" python python/longbridge_executor/main.py > "${LOGDIR}/executor.log" 2>&1 &
sleep 1

echo "=== 实盘 + 四策略全部启动 $(date) ==="
echo ""
echo "策略: Momentum | ThetaHarvest(3pt价差) | GammaScalp | PriceAction"
echo "执行: LIVE 实盘"
echo "看板: http://localhost:8765"
echo "日志: logs/${TODAY}/"
echo "交易DB: logs/trades.db"
