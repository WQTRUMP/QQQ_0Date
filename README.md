# QQQ 0DTE Trading System

面向 QQQ 末日期权 / 0DTE Options 的实盘交易系统原型。系统采用 NATS 事件总线，将行情、实时状态、Greeks、策略、风控、执行、持仓和看板拆分为独立服务。

> 风险提示：本项目包含实盘交易执行组件，默认不适合直接裸跑 live 模式。上传仓库不包含任何 Longbridge 凭证、交易日志或本地交易数据库。

## 架构概览

```text
Longbridge Quote API
  -> Go Market Gateway
  -> NATS quote.option.qqqus / quote.option.*
  -> Rust Realtime Engine / Greeks Engine
  -> Python Market Regime / Strategy Engine / Signal Challenger
  -> Rust Risk Engine
  -> Python Longbridge Executor
  -> Position Tracker / Trade Logger / Dashboard Bridge
```

核心链路：

```text
quote.option.* -> state.option.qqq / greeks.option.qqq
regime.option.qqq -> raw.signal.option.* -> signal.option.*
signal.option.* -> order.intent.option.* -> order.ack.option.qqq / fill.option.qqq
fill.option.qqq -> position.option.qqq
```

## 模块说明

```text
services/longbridge_gateway   Go 行情桥接，订阅 QQQ/VIX/0DTE 期权并发布 quote.option.*
services/realtime_engine      Rust 实时状态引擎，生成 Donchian、ADX、Bollinger、VIX 状态
services/greeks_engine        Rust Greeks 引擎，使用期权 IV 计算 Delta/Gamma/Theta/Vega
services/market_status        Rust 开收盘状态发布器
services/premarket_init       Rust 盘前初始化，计算 HV/ATR/趋势参考
services/risk_engine          Rust 风控引擎，处理仓位上限、信心阈值、价差风险、VIX 熔断
python/strategy_engine        Python 多策略引擎，支持 momentum/theta_harvest/gamma_scalp/price_action
python/signal_challenger      Python 信号过滤器，检查行情新鲜度和 IV 跳变
python/longbridge_executor    Python 执行网关，paper/live 模式提交订单
python/position_tracker       Python 本地持仓快照
python/trade_logger           Python 交易日志写入 SQLite
python/dashboard_bridge       Python WebSocket/HTTP 看板桥接
crates/common                 Rust 共享消息模型、品种模型和 subject 规则
```

## 环境要求

- macOS 或 Linux
- Rust toolchain，项目带 `rust-toolchain.toml`
- Go 1.22+
- Python 3.9+（仓库当前在 Python 3.11 虚拟环境完成依赖安装验证）
- Docker / Docker Compose
- NATS、Redis、QuestDB、Prometheus
- Longbridge OpenAPI 凭证，仅 live/paper 行情与实盘执行需要

## 安装依赖

基础设施：

```bash
docker compose up -d nats redis questdb prometheus
```

Python 依赖：

```bash
python3 -m venv .venv
. .venv/bin/activate
.venv/bin/pip install -r requirements.txt

# 若只启动 Python 服务，也可直接使用：
.venv/bin/pip install -r python/requirements.txt
```

说明：

- `longbridge` 的 Python SDK 来自官方文档指向的标准 PyPI 包，无需私有索引或额外 `--extra-index-url`。
- `python3 -m pip index versions longbridge` 在标准 PyPI 仅返回 `0.2.77` 及以下 `0.2.x` 版本，未提供 `4.x` 版本线。
- 当前仓库的 `requirements.txt` 与 `python/requirements.txt` 均固定为 `longbridge==0.2.77`；此前误写的 `4.x` 约束应视为错误引用了其他语言 SDK / 非 Python 版本线，不适用于 Python `pip` 安装。
- 已在标准虚拟环境中用 Python 3.11 验证 `pip install -r requirements.txt`、`pip install -r python/requirements.txt` 均可解析并可导入 `longbridge.openapi`。

Rust 服务：

```bash
cargo build --release
```

Go 行情网关：

```bash
cd services/longbridge_gateway
go mod download
go build -o ../../target/longbridge_gateway .
cd ../..
```

## 配置

创建本地配置文件：

```bash
cp configs/runtime.env.example .env.longbridge
```

然后按 Longbridge OpenAPI 账号填写 `.env.longbridge`。样例已切换为 QQQ 0DTE 主链路所需字段，包含 dashboard、盘前初始化和启动重试参数。该文件已被 `.gitignore` 忽略，禁止提交到仓库。

常用环境变量：

```text
NATS_URL=nats://127.0.0.1:4222
REDIS_URL=redis://127.0.0.1:6379
EXECUTION_MODE=paper
SPREAD_WING_WIDTH=3.0
MAX_RISK_PER_TRADE=500.00
POSITION_SIZE=3
```

## 启动方式

一键启动 paper 模式：

```bash
./start.sh paper
```

一键启动 live 模式：

```bash
./start.sh live
```

live 模式会连接 Longbridge 并提交真实订单。首次实盘运行前，建议先完成以下检查：

- 确认 `.env.longbridge` 使用正确账号
- 确认 `EXECUTION_MODE=paper` 下完整跑通信号、风控、订单、成交和持仓链路
- 确认券商真实持仓与本地 `position.option.qqq` 一致
- 确认保护腿下单、拒单处理、收盘前平仓逻辑符合预期

Dashboard：

```text
http://localhost:8765
ws://localhost:8765
http://localhost:8765/healthz
```

仓库内已包含默认看板资源 [QQQ_0DTE_Dashboard.html](/workspace/project/QQQ_0DTE_Dashboard.html)。若需替换外置 HTML，可通过 `DASHBOARD_HTML` 指向自定义文件。

## Docker 与历史链路

- `docker compose up longbridge_gateway dashboard_bridge longbridge_executor` 走 Python 兼容入口，适合最小烟雾验证。
- 本地主启动链路仍以 `./start.sh paper|live` 为准，使用 Go `services/longbridge_gateway` + Python executor。
- `services/market_gateway`、`services/execution_gateway` 和 [STRUCTURE.md](/workspace/project/STRUCTURE.md) 中的旧 Rust 链路属于历史/备用实现，不再是默认入口。

## 安全边界

仓库上传前已排除：

- `.env.longbridge` 和其他 `.env*` 凭证文件
- `logs/` 运行日志
- `logs/trades.db*` 本地交易数据库
- `.venv/`、`target/`、`__pycache__/`、`.DS_Store`
- 本地实盘下单实验脚本 `test_*_order.py`

如果需要公开仓库，请不要提交任何真实账号、订单号、成交回报、资金余额、持仓明细或交易日志。

## 当前限制

- Greeks Engine 中 Gamma Flip / Max Pain 仍偏代理化，不能当作机构级 GEX 模型。
- Longbridge 执行层当前按分腿方式处理价差，实盘存在拒单和断腿风险。
- 本地持仓簿必须与券商真实持仓对账后才适合放行新开仓。
- 0DTE 交易风险极高，任何 live 模式都应从极小仓位开始。
