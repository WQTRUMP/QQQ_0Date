# QQQ_Single 前后端接口文档

> 最后更新: 2026-06-02  
> VIX 数据源: **VIX.US 真实 CBOE 指数**（非期权 IV proxy）

---

## 一、架构概览

```
长桥 API
  └─ Go Gateway (行情桥接)
       │  quote.option.qqqus    ← QQQ 正股行情
       │  quote.option.vix      ← VIX.US 真实指数
       │  quote.option.{key}    ← 期权单腿行情 (含 IV/OI)
       │  kline.option.qqq      ← QQQ 1分钟K线
       └─→ NATS ────────────────────────────────────────────┐
                                                            │
  ┌─ Rust 层 (数值计算) ─────────────────────────────────────┤
  │  Realtime Engine  ← quote + kline + vix                 │
  │    └─→ state.option.qqq      (ADX/Donchian/BB)          │
  │  Greeks Engine    ← quote.option.>                      │
  │    └─→ greeks.option.qqq     (GammaFlip/MaxPain/Greeks)  │
  │  Market Status    ← cron 开盘/收盘                       │
  │    └─→ market.option.qqq.status                         │
  │  Premarket Init   ← 盘前计算                             │
  │    └─→ init.option.qqq       (HV/ATR/Trend)             │
  │  Risk Engine      ← signal + fill + quote + status      │
  │    └─→ order.intent.option.{key}                        │
  │    └─→ risk.option.{key}     (风控决策)                  │
  └─────────────────────────────────────────────────────────┤
                                                            │
  ┌─ Python 层 (策略/执行) ──────────────────────────────────┤
  │  Market Regime    ← qqq + vix + state + greeks          │
  │    └─→ regime.option.qqq    (VIX/ADX→权重分配)          │
  │  Strategy Engine  ← regime + kline + state              │
  │    └─→ raw.signal.option.{key}                          │
  │  Signal Challenger ← raw.signal.option.>                │
  │    └─→ signal.option.{key}   (过滤后信号)               │
  │  Executor         ← order.intent + position             │
  │    └─→ order.ack.option.qqq                             │
  │    └─→ fill.option.qqq                                  │
  │  Position Tracker ← fill + quote                        │
  │    └─→ position.option.qqq                              │
  │  Dashboard Bridge ← 汇聚所有 subject → WebSocket 8765   │
  │    └─→ balance.option.qqq   (每15s购买力)               │
  └─────────────────────────────────────────────────────────┘
```

---

## 二、NATS 主题全集

### 2.1 行情层 (Go Gateway → NATS)

| 主题 | 发布者 | 频率 | 数据结构 |
|------|--------|------|----------|
| `quote.option.qqqus` | Go Gateway | 每 tick | `RawOptionQuote` (last_done/volume/high/low, symbol_key=`qqqus`) |
| `quote.option.vix` | Go Gateway | 每 tick | `RawOptionQuote` (last_done = VIX 指数, **无 option_extend**) |
| `quote.option.{key}` | Go Gateway | 每 tick + 10s IV 拉取 | `RawOptionQuote` + `option_extend` (IV/OI/strike/direction/expiry) |
| `kline.option.qqq` | Go Gateway + Realtime Engine | 每 1 分钟收线 | `{symbol, open, high, low, close, volume, timestamp}` |

**VIX 数据流（2026-06-02 更新）：**
```
VIX.US WebSocket tick
  → Go Gateway Publish("quote.option.vix", {symbol:"VIX.US", last_done:"14.5"})
  ├─ Market Regime  on_vix → vix_vals.push(14.5) → detect_regime()
  ├─ Dashboard Bridge on_vix → state.vix_price = 14.5 → WebSocket 推送
  └─ Realtime Engine vix_stream → vix_level 字段
```

### 2.2 Rust 引擎输出

| 主题 | 发布者 | 内容 |
|------|--------|------|
| `state.option.qqq` | Realtime Engine | `RealtimeState` (last_price, ADX, Donchian, BB, vix_level, kline_count) |
| `greeks.option.qqq` | Greeks Engine | `GreeksSnapshot` (gamma_flip, max_pain, 期权 Greeks 列表) |
| `init.option.qqq` | Premarket Init | `PremarketInit` (HV, ATR, high_20d, low_20d, avg_close_20d, trend_slope, trend_score) |
| `init.option.qqq.refresh` | Realtime Engine | `PremarketInit`（盘中同 schema 刷新，避免复用 `init.option.qqq` 裸 JSON） |
| `market.option.qqq.status` | Market Status | `MarketStatus` (Open/Close/Halted, `session_id`) |

### 2.3 体制 & 信号层

| 主题 | 发布者 | 订阅者 | 内容 |
|------|--------|--------|------|
| `regime.option.qqq` | Market Regime | Risk Engine, Strategy Engine, Dashboard | 体制判断 + 四策略权重 + VIX + ADX |
| `raw.signal.option.{key}` | Strategy Engine | Signal Challenger | 原始信号（含 spread_wing） |
| `signal.option.{key}` | Signal Challenger | Risk Engine | 过滤后信号 (symbol_key 动态) |
| `signal.option.>` | — | Risk Engine | 通配符接收所有 signal |

### 2.4 订单 & 执行层

| 主题 | 发布者 | 订阅者 | 内容 |
|------|--------|--------|------|
| `order.intent.option.{key}` | Risk Engine | Executor | `OrderIntent` (含 spread_wing 保护腿) |
| `order.ack.option.qqq` | Executor | — | `OrderAck` (按 QQQ 聚合确认) |
| `fill.option.qqq` | Executor | Risk Engine, Position Tracker | `FillEvent` (成交确认，含 `source_signal_id/is_exit/total_legs/leg`) |
| `risk.option.{key}` | Risk Engine | Market Regime | `RiskReport` (Approved/Rejected，止损/止盈) |

### 2.5 状态 & 监控

| 主题 | 发布者 | 频率 | 内容 |
|------|--------|------|------|
| `position.option.qqq` | Position Tracker | 每 30s | 持仓快照 `{positions: [...]}` |
| `balance.option.qqq` | Dashboard Bridge | 每 15s | `{buying_power, available_cash, total_assets}` |

---

## 三、Dashboard WebSocket 接口

### 端点
```
ws://localhost:8765
http://localhost:8765   (HTML 看板)
http://localhost:8765/healthz   (健康检查)
```

### 推送频率
500ms 广播一次完整快照（`PUSH_MS=500`）

### 快照数据结构

```json
{
  "ts": 1717334400.123,
  "underlying": {
    "symbol": "QQQ",
    "price": 493.50,
    "change": 2.30,
    "change_pct": 0.47,
    "volume": 12345678,
    "high": 495.00,
    "low": 491.20,
    "open": 492.00,
    "prev_close": 491.20,
    "vix": 14.50,
    "iv_rank": 0,
    "market_open": true
  },
  "chain": [
    {
      "strike": 490.0,
      "call": {"symbol": "QQQ260602C490000.US", "bid": 3.50, "ask": 3.52, "volume": 100, "oi": 5000, "iv": 0.18, "delta": 0.65, "gamma": 0.02, "theta": -0.05, "vega": 0.10},
      "put":  {"symbol": "QQQ260602P490000.US", "bid": 1.20, "ask": 1.21, "volume": 80,  "oi": 4000, "iv": 0.19, "delta": -0.35, "gamma": 0.02, "theta": -0.04, "vega": 0.10}
    }
  ],
  "quant": {
    "net_gamma": 12.5,
    "gex": 0.13,
    "cpr": 1.25,
    "gamma_flip": 493.00,
    "max_pain": 492.00,
    "gf_dist_pct": 0.10
  },
  "theta_pct": 85,
  "iv_decay_pct": 15,
  "signals": [
    {"signal_id": "...", "strategy_id": "theta_harvest", "confidence": 0.85, "reason": "..."}
  ],
  "positions": [...],
  "market_regime": {
    "regime": "low_vol_drift",
    "reason": "VIX14 沉睡日 ADX61 暗藏趋势 → 动量+PA开缝",
    "weights": {"momentum": 0.25, "theta_harvest": 0.55, "gamma_scalp": 0.00, "price_action": 0.20},
    "circuit_breaker": false,
    "confidence": 85
  },
  "account": {
    "total_assets": 944000.00,
    "total_cash": 820000.00,
    "available_cash": 121000.00,
    "market_value": 124000.00,
    "buying_power": 242000.00,
    "margin_call": 0,
    "unrealized_pnl": 0,
    "currency": "USD",
    "updated": 1717334400.0
  }
}
```

### 字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| `underlying.price` | float | QQQ 现价 |
| `underlying.vix` | float | **真实 VIX.US index 价格**（非期权 IV proxy） |
| `underlying.iv_rank` | int | IV 百分位（暂未实现，固定 0） |
| `chain[].call/put.iv` | float | 该行权价的隐含波动率（decimal 格式） |
| `chain[].call/put.delta/gamma/theta/vega` | float | BS 模型 Greeks（Dashboard Bridge 实时计算） |
| `quant.gamma_flip` | float | 做市商 Gamma 翻转点（来自 Greeks Engine） |
| `quant.max_pain` | float | 最大痛点（来自 Greeks Engine） |
| `quant.gf_dist_pct` | float | 现价距 Gamma Flip 的百分比距离 |
| `market_regime.regime` | string | 体制: `trending` / `low_vol_drift` / `high_vol` / `volatile` |
| `market_regime.weights` | object | 四策略权重分配 (momentum/theta_harvest/gamma_scalp/price_action) |
| `account.buying_power` | float | 可用购买力（含杠杆） |

---

## 四、关键数据流时序

```
VIX 更新延迟:
  VIX.US tick → Go Gateway <1ms → NATS → Market Regime <1ms → vix_vals
  → 每 10s detect_regime() → NATS regime.option.qqq
  → Risk Engine / Strategy Engine / Dashboard Bridge
  端到端: ≤10s

QQQ 价格更新延迟:
  QQQ.US tick → Go Gateway <1ms → NATS → 各组件 <1ms
  端到端: <1s

信号全链路:
  Strategy Engine 策略计算 (~50ms 批量)
  → raw.signal.option.{key}
  → Signal Challenger 过滤 (行情新鲜度检查)
  → signal.option.{key}
  → Risk Engine (购买力/冷却/去重/仓位检查)
  → order.intent.option.{key}
  → Executor 下单 (卖腿→5s确认→买腿)
  → fill.option.qqq
```

---

## 五、环境变量参考

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `NATS_URL` | `nats://127.0.0.1:4222` | NATS 连接地址 |
| `VIX_SYMBOL` | `VIX.US` | VIX 标的符号（Go Gateway） |
| `VIX_STATE_SUBJECT` | `regime.option.qqq` | Risk Engine VIX 来源（Market Regime 输出） |
| `PUBLISH_INTERVAL_SEC` | `10` | Market Regime 发布间隔 |
| `STRIKE_WINDOW` | `5` | 期权行权价筛选窗口（±$） |
| `REBALANCE_DELTA` | `2` | 扩窗触发阈值（$） |

---

## 六、变更记录

| 日期 | 变更 | 影响范围 |
|------|------|----------|
| 2026-06-02 | VIX 数据源从 QQQ 期权 IV proxy → VIX.US 真实指数 | Market Regime `on_vix` 回调、NATS 订阅拆分 |
