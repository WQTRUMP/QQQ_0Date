#!/bin/bash
# 快速启动（跳过编译，只启动服务）
set -euo pipefail
cd "$(dirname "$0")"
EXEC_MODE="${1:-live}"

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

TODAY=$(date +%Y-%m-%d)
LOGDIR="logs/${TODAY}"
mkdir -p "$LOGDIR"

echo ">>> 清理旧进程..."
pkill -f "python/strategy_engine/main.py" 2>/dev/null || true
pkill -f "python/signal_challenger/main.py" 2>/dev/null || true
pkill -f "risk_engine" 2>/dev/null || true
pkill -f "python/longbridge_executor/main.py" 2>/dev/null || true
pkill -f "python/trade_logger/main.py" 2>/dev/null || true
pkill -f "python/position_tracker/main.py" 2>/dev/null || true
pkill -f "python/market_regime/main.py" 2>/dev/null || true
pkill -f "python/dashboard_bridge/main.py" 2>/dev/null || true
pkill -f "realtime_engine" 2>/dev/null || true
pkill -f "greeks_engine" 2>/dev/null || true
pkill -f "premarket_init" 2>/dev/null || true
pkill -f "market_status" 2>/dev/null || true
pkill -f "longbridge_gateway" 2>/dev/null || true
sleep 1
echo "  已清理 ✅"

if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
fi

if [ -f ".env.longbridge" ]; then
    set -a; source .env.longbridge; set +a
fi

# 基础设施
pgrep -q nats-server || { echo "  启动 NATS..."; brew services start nats-server 2>/dev/null || nats-server -js & }
pgrep -q redis-server || { echo "  启动 Redis..."; brew services start redis 2>/dev/null || redis-server --daemonize yes; }
wait_for_tcp 127.0.0.1 4222 "NATS"
wait_for_tcp 127.0.0.1 6379 "Redis"

PIDS=()

echo ">>> 启动 Trade Logger..."
source .venv/bin/activate
python -u python/trade_logger/main.py > "${LOGDIR}/trade_logger.log" 2>&1 &
PIDS+=($!)
echo "  trade_logger PID=$!"
sleep 1

echo ">>> 启动 Position Tracker..."
python -u python/position_tracker/main.py > "${LOGDIR}/position_tracker.log" 2>&1 &
PIDS+=($!)
echo "  position_tracker PID=$!"
sleep 1

echo ">>> 启动 Premarket Init..."
env RUST_LOG=info ./target/release/premarket_init > /dev/null 2> "${LOGDIR}/premarket_init.log" &
PIDS+=($!)
echo "  premarket_init PID=$!"
sleep 2

echo ">>> 启动 Market Status..."
env RUST_LOG=info ./target/release/market_status > /dev/null 2> "${LOGDIR}/market_status.log" &
PIDS+=($!)
echo "  market_status PID=$!"
sleep 1

echo ">>> 启动 Longbridge Gateway (Go)..."
./target/longbridge_gateway > "${LOGDIR}/gateway.log" 2>&1 &
PIDS+=($!)
echo "  gateway PID=$!"
sleep 2

echo ">>> 启动 Dashboard Bridge..."
env NATS_URL="nats://127.0.0.1:4222" python -u python/dashboard_bridge/main.py > "${LOGDIR}/dashboard_bridge.log" 2>&1 &
PIDS+=($!)
echo "  dashboard_bridge PID=$! → http://localhost:8765"
wait_for_http "http://127.0.0.1:8765/healthz" "Dashboard"

echo ">>> 启动 Market Regime..."
python -u python/market_regime/main.py > "${LOGDIR}/market_regime.log" 2>&1 &
PIDS+=($!)
echo "  market_regime PID=$!"
sleep 1

echo ">>> 启动 Realtime Engine..."
./target/release/realtime_engine > /dev/null 2> "${LOGDIR}/realtime_engine.log" &
PIDS+=($!)
echo "  realtime_engine PID=$!"
sleep 1

echo ">>> 启动 Greeks Engine..."
./target/release/greeks_engine > /dev/null 2> "${LOGDIR}/greeks_engine.log" &
PIDS+=($!)
echo "  greeks_engine PID=$!"
sleep 1

echo ">>> 启动 Signal Challenger..."
python -u python/signal_challenger/main.py > "${LOGDIR}/challenger.log" 2>&1 &
PIDS+=($!)
echo "  challenger PID=$!"
sleep 1

echo ">>> 启动 Momentum 策略..."
env STRATEGY_MODE="momentum" STRATEGY_ID="momentum_v1" python -u python/strategy_engine/main.py > "${LOGDIR}/strategy_momentum.log" 2>&1 &
PIDS+=($!)
echo "  momentum PID=$!"
sleep 1

echo ">>> 启动 ThetaHarvest 策略..."
env STRATEGY_MODE="theta_harvest" STRATEGY_ID="theta_harvest_v0" SPREAD_WING_WIDTH="3.0" SIGNAL_COOLING_SECS="120" python -u python/strategy_engine/main.py > "${LOGDIR}/strategy_theta_harvest.log" 2>&1 &
PIDS+=($!)
echo "  theta_harvest PID=$!"
sleep 1

echo ">>> 启动 GammaScalp 策略..."
env STRATEGY_MODE="gamma_scalp" STRATEGY_ID="gamma_scalp_v0" python -u python/strategy_engine/main.py > "${LOGDIR}/strategy_gamma_scalp.log" 2>&1 &
PIDS+=($!)
echo "  gamma_scalp PID=$!"
sleep 1

echo ">>> 启动 PriceAction 策略..."
env STRATEGY_MODE="price_action" STRATEGY_ID="price_action_v1" python -u python/strategy_engine/main.py > "${LOGDIR}/strategy_price_action.log" 2>&1 &
PIDS+=($!)
echo "  price_action PID=$!"
sleep 1

echo ">>> 启动 Risk Engine..."
env MAX_RISK_PER_TRADE="2000.00" SPREAD_WING_WIDTH="3.0" ./target/release/risk_engine > /dev/null 2> "${LOGDIR}/risk_engine.log" &
PIDS+=($!)
echo "  risk_engine PID=$!"
sleep 1

echo ">>> 启动 Executor (${EXEC_MODE})..."
env EXECUTION_MODE="$EXEC_MODE" python -u python/longbridge_executor/main.py > "${LOGDIR}/executor.log" 2>&1 &
PIDS+=($!)
echo "  executor PID=$!"

echo ""
echo "============================================"
echo " ✅ 全部服务已启动 (${EXEC_MODE})"
echo " 看板:    http://localhost:8765"
echo " 日志:    logs/${TODAY}/"
echo "============================================"

trap 'echo ""; echo "正在停止..."; for pid in ${PIDS[@]}; do kill $pid 2>/dev/null; done; echo "已停止"' EXIT
wait
