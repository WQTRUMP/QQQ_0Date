历史架构草图（非当前默认启动链路）

当前主链路请以 README、start.sh 和 services/longbridge_gateway 为准。

Market Data Source
        ↓
Rust Market Gateway
        ↓
NATS market.*
        ↓
Rust Realtime Engine
        ↓
Redis Global State Cache
        ↓
NATS state.* / indicator.*
        ↓
Rust Greeks Engine          ← 新: 全链 Greeks 计算
        ↓
NATS greeks.*
        ↓
Python Strategy Engine
        ↓
NATS signal.*
        ↓
Rust Risk Engine
        ↓
Rust Execution Gateway

旁路服务：
  Rust Market Status      ← 新: 开盘/收盘信号 → market.*.status
  Rust Premarket Init     ← 新: 20日日K → HV/ATR/S/R → init.*
  Go Storage Writer → QuestDB
  Go API Server → Frontend
  Config Service → SQLite / Postgres
  Alert Service → Telegram / 企业微信
  Monitor → Prometheus / Grafana

启动顺序:
  1. docker compose up -d nats redis questdb
  2. cargo run -p market_gateway
  3. cargo run -p realtime_engine
  4. cargo run -p greeks_engine
  5. cargo run -p market_status       # 发送开盘信号，定时发收盘
  6. cargo run -p premarket_init      # 跑一次: 读日K, 发 init.*, 退出
  7. .venv/bin/python python/strategy_engine/main.py
  8. cargo run -p risk_engine
  9. cargo run -p execution_gateway
