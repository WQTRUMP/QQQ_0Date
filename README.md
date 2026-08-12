<!--
[INPUT]: 依赖 configs/binance.env.example、start.sh、python/binance_trading 与离线回归合同
[OUTPUT]: 对外提供 BTCUSDT 固定 1m 策略、安装、风控、启动和验证指南
[POS]: 项目根级产品说明；只描述 Binance Demo/Testnet 当前实现，不承载历史系统语义
[PROTOCOL]: 变更时更新此头部，然后检查 CLAUDE.md
-->

# Binance BTCUSDT 1m Trading System

一个只面向 Binance USDⓈ-M Demo/Testnet 的 BTCUSDT 永续合约交易系统。系统每分钟评估一次闭合 K 线，适合分钟级中高频验证；不提供主网下单入口。

## 交易逻辑

交易方向只有一条规则：

- 固定消费 `BTCUSDT` 闭合 `1m` K 线。
- EMA5 上穿 EMA13 时做多，下穿时做空。
- 交叉只在发生的那根闭柱产生一次信号。
- ATR14 不判断方向，只计算止损距离和仓位风险。
- 历史 K 线只用于水合；未闭柱、重复柱、乱序柱和断档都不能产生信号。

不叠加更多方向指标，也不开放 EMA/ATR 周期的现场调参。系统固定单标的、单仓位，每分钟最多评估一次入场。

## 风控与执行

- 入场前检查新鲜盘口、点差、账户权益、UTC 日损、可用余额、交易所过滤器及正值保护价格几何。
- 默认每笔风险为权益的 `0.5%`，名义价值不超过权益的 `25%`，最大杠杆 `2x`。
- 入场使用 `LIMIT IOC`；实际成交后重新冻结止损与目标。
- Testnet 仓位使用 Binance Algo Service 托管 STOP/TARGET，本地 mark price 继续作为 reduce-only 退出兜底。
- 普通订单和保护单在结果不确定时只查询原始订单身份，绝不盲目重投。
- Paper 与 Testnet 使用不同 SQLite 文件；恢复库损坏、账户不一致或行情陈旧时全部失败关闭。

## 安装

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
cp configs/binance.env.example .env.binance
chmod 600 .env.binance
```

Paper 默认不交易。需要产生本地模拟成交时，在 `.env.binance` 中显式设置：

```dotenv
TRADING_ENABLED=true
```

Testnet 还必须填写 Demo API key/secret，并设置：

```dotenv
BINANCE_TESTNET_TRADING_CONFIRM=TESTNET_ONLY
```

## 启动

```bash
./start.sh          # paper
./start.sh paper
./start.sh testnet
```

Dashboard 默认只监听 `http://127.0.0.1:8765/`：

- `/healthz`：进程存活。
- `/readyz`：行情、账户、策略与恢复状态全部可交易。
- `/api/snapshot`：只读运行快照。

## 目录

```text
configs/                  私有配置样例
docs/                     运行与恢复手册
python/binance_trading/   行情、1m 策略、风控、订单、账本和 Dashboard
python/tests/             Binance 离线回归
Binance_Dashboard.html    只读 BTCUSDT 操作台
start.sh                  唯一启动入口
```

## 验证

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m unittest discover \
  -s python/tests -p 'test_binance_*.py' -v
bash -n start.sh
./start.sh --help
git diff --check
```

完整配置、限频、崩溃恢复和托管保护说明见 [运行手册](docs/binance-runtime.md)。

本项目不承诺盈利。先在 paper 中验证成交成本与运行稳定性，再使用 Testnet。
