use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures_util::StreamExt;
use qqq_common::{
    json_bytes, subjects, AssetClass, Instrument, OrderIntent, OrderSide, OrderType,
    RiskDecision, RiskReport, SignalAction, StrategySignal, Venue,
};
use redis::AsyncCommands;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

// ── CLI 参数 ──────────────────────────────────────────

#[derive(Debug, Clone, Parser)]
#[command(name = "risk_engine")]
#[command(about = "风控引擎 v3：去重 + 信心分 + 持仓上限 + 动态仓位 + 止盈止损")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    #[arg(long, env = "SIGNAL_SUBJECT", default_value = "signal.option.>")]
    signal_subject: String,

    #[arg(long, env = "FILL_SUBJECT", default_value = "fill.option.qqq")]
    fill_subject: String,

    #[arg(long, env = "QUOTE_SUBJECT", default_value = "quote.option.>")]
    quote_subject: String,

    #[arg(long, env = "STATUS_SUBJECT", default_value = "market.option.qqq.status")]
    status_subject: String,

    #[arg(long, env = "MIN_CONFIDENCE", default_value = "0.60")]
    min_confidence: Decimal,

    #[arg(long, env = "MIN_ORDER_QUANTITY", default_value = "1")]
    min_order_quantity: Decimal,
    #[arg(long, env = "MAX_ORDER_QUANTITY", default_value = "5")]
    max_order_quantity: Decimal,

    #[arg(long, env = "POSITION_SIZE", default_value = "3")]
    position_size: u32,
    // ── 单腿止盈止损 ──
    #[arg(long, env = "SPREAD_WING_WIDTH", default_value = "3.0")]
    spread_wing_width: Decimal,
    #[arg(long, env = "SPREAD_SL_PCT", default_value = "0.60")]
    spread_sl_pct: Decimal,
    #[arg(long, env = "SPREAD_TP_PCT", default_value = "0.70")]
    spread_tp_pct: Decimal,

    // ── 止盈止损 ──
    #[arg(long, env = "STOP_LOSS_PCT", default_value = "0.20")]
    stop_loss_pct: Decimal,

    #[arg(long, env = "TRAILING_TP_PCT", default_value = "0.15")]
    trailing_tp_pct: Decimal,

    #[arg(long, env = "TRAILING_TP_ACTIVATE", default_value = "0.10")]
    trailing_tp_activate: Decimal,

    /// 固定止盈比例（到达后激活动态止盈继续跑）
    #[arg(long, env = "FIXED_TP_PCT", default_value = "0.30")]
    fixed_tp_pct: Decimal,

    #[arg(long, env = "TIME_STOP_MIN", default_value = "15")]
    time_stop_min: u32,

    #[arg(long, env = "MARKET_CLOSE_UTC", default_value = "20:00")]
    market_close_utc: String,

    // ── VIX 风控 ──
    /// VIX 数据源 subject（Market Regime 发布 regime.option.qqq，含 vix 字段）
    #[arg(long, env = "VIX_STATE_SUBJECT", default_value = "regime.option.qqq")]
    vix_state_subject: String,

    /// VIX 飙升阈值（点数），超过此变化立即全平
    #[arg(long, env = "VIX_SPIKE_THRESHOLD", default_value = "5.0")]
    vix_spike_threshold: Decimal,

    /// 账户余额推送主题（含 buying_power）
    #[arg(long, env = "BALANCE_SUBJECT", default_value = "balance.option.qqq")]
    balance_subject: String,

    // ── 信用价差风控 ──
    /// 单笔最大亏损（美元），超过此金额的价差信号将被拒绝
    #[arg(long, env = "MAX_RISK_PER_TRADE", default_value = "500.00")]
    max_risk_per_trade: Decimal,
}

// ── 持仓跟踪 ──────────────────────────────────────────

#[derive(Debug, Clone)]
struct TrackedPosition {
    order_id: String,
    symbol: String,
    side: OrderSide,
    entry_price: Decimal,
    quantity: Decimal,
    entry_time: DateTime<Utc>,
    high_water: Decimal,
    low_water: Decimal,
    trailing_active: bool,
    exit_triggered: bool,
    /// 产生此持仓的策略 ID（从信号提取）
    strategy_id: String,
    /// 原始信号 ID（用于平仓时记录盈亏到 trade_summary）
    source_signal_id: String,
    /// 信用价差配对腿的 order_id（单腿持仓为 None）
    spread_pair_id: Option<String>,
    /// 净权利金收入：卖腿权利金 - 买腿权利金（从实际成交价计算）
    spread_net_credit: Decimal,
    /// 最大可能亏损 = (价差宽度 × 100 - 净权利金 × 100) × qty
    spread_max_loss: Decimal,
}

impl TrackedPosition {
    fn new(order_id: String, symbol: String, side: OrderSide, entry_price: Decimal, quantity: Decimal, strategy_id: String, source_signal_id: String) -> Self {
        Self {
            order_id,
            symbol,
            side,
            entry_price,
            quantity,
            entry_time: Utc::now(),
            high_water: entry_price,
            low_water: entry_price,
            trailing_active: false,
            exit_triggered: false,
            strategy_id,
            source_signal_id,
            spread_pair_id: None,
            spread_net_credit: Decimal::ZERO,
            spread_max_loss: Decimal::ZERO,
        }
    }

    fn update_price(&mut self, current_price: Decimal, args: &Args) {
        if self.exit_triggered || current_price <= Decimal::ZERO {
            return;
        }
        match self.side {
            OrderSide::Buy => {
                if current_price > self.high_water {
                    self.high_water = current_price;
                }
                if !self.trailing_active {
                    let gain = (current_price - self.entry_price) / self.entry_price;
                    // 动态止盈激活：达到激活阈值 或 到达固定止盈目标
                    if gain >= args.trailing_tp_activate {
                        self.trailing_active = true;
                        info!(order_id = %self.order_id, gain_pct = %(gain * Decimal::from(100)).round_dp(1), "🎯 动态止盈激活");
                    } else if gain >= args.fixed_tp_pct {
                        self.trailing_active = true;
                        info!(order_id = %self.order_id, gain_pct = %(gain * Decimal::from(100)).round_dp(1), "🎯 固定止盈触发 → 激活动态止盈继续跑");
                    }
                }
            }
            OrderSide::Sell => {
                if current_price < self.low_water {
                    self.low_water = current_price;
                }
                if !self.trailing_active {
                    let gain = (self.entry_price - current_price) / self.entry_price;
                    // 动态止盈激活：达到激活阈值 或 到达固定止盈目标
                    if gain >= args.trailing_tp_activate {
                        self.trailing_active = true;
                        info!(order_id = %self.order_id, gain_pct = %(gain * Decimal::from(100)).round_dp(1), "🎯 动态止盈激活");
                    } else if gain >= args.fixed_tp_pct {
                        self.trailing_active = true;
                        info!(order_id = %self.order_id, gain_pct = %(gain * Decimal::from(100)).round_dp(1), "🎯 固定止盈触发 → 激活动态止盈继续跑");
                    }
                }
            }
        }
    }

    fn check_exit(&self, current_price: Decimal, args: &Args) -> (bool, bool) {
        if self.exit_triggered || current_price <= Decimal::ZERO {
            return (false, false);
        }
        let one = Decimal::ONE;
        match self.side {
            OrderSide::Buy => {
                let sl = current_price <= self.entry_price * (one - args.stop_loss_pct);
                let tp = self.trailing_active
                    && self.high_water > self.entry_price
                    && current_price <= self.high_water * (one - args.trailing_tp_pct);
                (tp, sl)
            }
            OrderSide::Sell => {
                let sl = current_price >= self.entry_price * (one + args.stop_loss_pct);
                let tp = self.trailing_active
                    && self.low_water < self.entry_price
                    && current_price >= self.low_water * (one + args.trailing_tp_pct);
                (tp, sl)
            }
        }
    }
}

// ── 全局状态 ──────────────────────────────────────────

struct RiskState {
    processed_signals: HashSet<String>,
    counted_fills: HashSet<String>,
    /// 信用价差第一条腿暂存（base_order_id → TrackedPosition），等第二条到达后配对
    pending_spreads: HashMap<String, TrackedPosition>,
    position_count: u32,
    positions: HashMap<String, TrackedPosition>,
    /// symbol → 最新报价
    latest_prices: HashMap<String, Decimal>,
    /// VIX 当前值和前值（用于检测飙升）
    vix_current: Decimal,
    vix_previous: Decimal,
    /// VIX 滚动窗口（最近 6 次更新，≈60s），检测慢涨飙升
    vix_history: VecDeque<Decimal>,
    /// 购买力（USD），来自 Dashboard Bridge 每 15s 推送
    buying_power: Decimal,
}

impl RiskState {
    fn new() -> Self {
        Self {
            processed_signals: HashSet::new(),
            counted_fills: HashSet::new(),
            pending_spreads: HashMap::new(),
            position_count: 0,
            positions: HashMap::new(),
            latest_prices: HashMap::new(),
            vix_current: Decimal::ZERO,
            vix_previous: Decimal::ZERO,
            vix_history: VecDeque::new(),
            buying_power: Decimal::MAX,  // 初始默认无限制，等 dashboard 推送后更新
        }
    }
    fn reset(&mut self) {
        self.processed_signals.clear();
        self.counted_fills.clear();
        self.pending_spreads.clear();
        self.position_count = 0;
        self.positions.clear();
        self.latest_prices.clear();
    }
}

// ── 动态仓位 ──────────────────────────────────────────

fn dynamic_quantity(confidence: Decimal, min_conf: Decimal, min_qty: Decimal, max_qty: Decimal) -> Decimal {
    if confidence >= Decimal::ONE {
        return max_qty;
    }
    let range = Decimal::ONE - min_conf;
    let ratio = ((confidence - min_conf) / range)
        .to_f64()
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);
    let qty_range = (max_qty - min_qty).to_f64().unwrap_or(0.0);
    let qty = min_qty.to_f64().unwrap_or(1.0) + qty_range * ratio;
    Decimal::from_f64_retain(qty.round()).unwrap_or(min_qty)
}

// ── 时间工具 ──────────────────────────────────────────

fn minutes_to_close(close_utc: &str) -> Option<i64> {
    let parts: Vec<&str> = close_utc.split(':').collect();
    if parts.len() != 2 { return None; }
    let h: u32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let now = Utc::now();
    let close = now.date_naive().and_hms_opt(h, m, 0)?;
    let diff = (close - now.naive_utc()).num_minutes();
    if diff < 0 { None } else { Some(diff) }
}

// ── 主入口 ────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let nats = async_nats::connect(&args.nats_url)
        .await
        .with_context(|| format!("连接 NATS 失败: {}", args.nats_url))?;

    let redis_client = redis::Client::open(args.redis_url.clone())
        .with_context(|| format!("连接 Redis 失败: {}", args.redis_url))?;
    let mut redis = redis_client
        .get_multiplexed_tokio_connection()
        .await
        .context("Redis 多路复用连接失败")?;

    let state = Arc::new(Mutex::new(RiskState::new()));

    // ── 从 Redis 恢复已处理信号（防重启重复下单）──
    let redis_signal_prefix: &str = "risk:signal:";
    {
        // 先异步查 Redis，再拿锁写 state（避免锁跨 .await 死锁）
        let keys: Vec<String> = redis.keys(format!("{}*", redis_signal_prefix)).await.unwrap_or_default();
        let mut s = state.lock().unwrap();
        for key in &keys {
            if let Some(id) = key.strip_prefix(redis_signal_prefix) {
                s.processed_signals.insert(id.to_string());
            }
        }
        info!(count = keys.len(), "从 Redis 恢复已处理信号");
    }

    let mut signal_stream = nats.subscribe(args.signal_subject.clone()).await?;
    let mut fill_stream = nats.subscribe(args.fill_subject.clone()).await?;
    let mut quote_stream = nats.subscribe(args.quote_subject.clone()).await?;
    let mut status_stream = nats.subscribe(args.status_subject.clone()).await?;
    let mut state_stream = nats.subscribe(args.vix_state_subject.clone()).await?;
    let mut balance_stream = nats.subscribe(args.balance_subject.clone()).await?;

    info!(
        signal = %args.signal_subject,
        fill = %args.fill_subject,
        quote = %args.quote_subject,
        status = %args.status_subject,
        vix_state = %args.vix_state_subject,
        balance = %args.balance_subject,
        pos_size = args.position_size,
        sl = %args.stop_loss_pct,
        ttp = %args.trailing_tp_pct,
        ttp_act = %args.trailing_tp_activate,
        time_stop = args.time_stop_min,
        vix_spike = %args.vix_spike_threshold,
        "风控引擎 v3 已启动"
    );

    // 持仓监控协程
    let monitor_nats = nats.clone();
    let monitor_args = args.clone();
    let monitor_state = state.clone();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            check_positions(&monitor_nats, &monitor_args, &monitor_state).await;
        }
    });

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("收到退出信号，风控引擎停止");
                break;
            }
            Some(msg) = signal_stream.next() => {
                if let Err(err) = handle_signal(&nats, &args, &state, &mut redis, &msg.payload).await {
                    error!(error = %err, "处理信号失败");
                }
            }
            Some(msg) = fill_stream.next() => {
                handle_fill(&state, &args, &msg.payload);
            }
            Some(msg) = quote_stream.next() => {
                handle_quote(&state, &args, &msg.payload);
            }
            Some(msg) = status_stream.next() => {
                handle_status(&state, &msg.payload);
            }
            Some(msg) = state_stream.next() => {
                handle_vix_state(&state, &args, &msg.payload);
            }
            Some(msg) = balance_stream.next() => {
                handle_balance(&state, &msg.payload);
            }
        }
    }

    Ok(())
}

// ── 行情 ──────────────────────────────────────────────

fn handle_quote(state: &Arc<Mutex<RiskState>>, args: &Args, payload: &[u8]) {
    let quote: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(_) => return,
    };
    let symbol = quote.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
    if symbol.is_empty() {
        return;
    }
    let last_done = quote
        .get("last_done")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str_exact(s).ok())
        .unwrap_or(Decimal::ZERO);
    if last_done <= Decimal::ZERO {
        return;
    }

    let mut s = state.lock().unwrap();
    s.latest_prices.insert(symbol.to_string(), last_done);

    for pos in s.positions.values_mut() {
        if pos.symbol == symbol {
            pos.update_price(last_done, args);
        }
    }
}

// ── 成交 ──────────────────────────────────────────────


/// 从 source_signal_id 提取 strategy_id
/// 格式: signal-{strategy_id}-{timestamp}-{random}
fn extract_strategy_id(source: &str) -> String {
    let parts: Vec<&str> = source.split('-').collect();
    if parts.len() >= 4 && parts[0] == "signal" {
        // 去掉 "signal" 前缀和最后两个段（timestamp + random）
        parts[1..parts.len()-2].join("-")
    } else {
        source.to_string()
    }
}

fn handle_fill(state: &Arc<Mutex<RiskState>>, args: &Args, payload: &[u8]) {
    let fill: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "解析成交失败");
            return;
        }
    };

    let order_id = fill.get("order_id").and_then(|v| v.as_str()).unwrap_or("");
    if order_id.is_empty() {
        warn!("成交缺少 order_id");
        return;
    }

    let mut s = state.lock().unwrap();
    if s.counted_fills.contains(order_id) {
        return;
    }

    // 从 source_signal_id 提取策略 ID
    let source_signal_id = fill.get("source_signal_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let strategy_id = extract_strategy_id(source_signal_id);

    // ── 检测退出订单 ──
    let is_exit = fill.get("is_exit").and_then(|v| v.as_bool()).unwrap_or(false);
    if is_exit {
        s.counted_fills.insert(order_id.to_string());
        let sym = fill.get("instrument")
            .and_then(|i| i.get("symbol"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // 按 source_signal_id（原始信号 ID）匹配持仓
        let to_remove: Vec<String> = s.positions.iter()
            .filter(|(_, pos)| pos.source_signal_id == source_signal_id)
            .map(|(k, _)| k.clone())
            .collect();
        let removed = to_remove.len();
        for key in &to_remove {
            s.positions.remove(key);
            s.position_count = s.position_count.saturating_sub(1);
        }
        if removed > 0 {
            info!(order_id = %order_id, symbol = %sym, count = removed, "📤 平仓已确认（按 source_signal_id 匹配）");
        } else {
            warn!(order_id = %order_id, source_signal_id = %source_signal_id, "⚠️ 平仓成功但本地无此持仓");
        }
        return;
    }

    let sym = fill.get("instrument")
        .and_then(|i| i.get("symbol"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let side_str = fill.get("side").and_then(|v| v.as_str()).unwrap_or("BUY");
    let side = if side_str == "SELL" { OrderSide::Sell } else { OrderSide::Buy };
    let entry = fill.get("price")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str_exact(s).ok())
        .unwrap_or(Decimal::ZERO);
    let qty = fill.get("quantity")
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str_exact(s).ok())
        .unwrap_or(Decimal::ONE);

    if entry <= Decimal::ZERO || sym.is_empty() {
        warn!(order_id = %order_id, "成交数据不完整");
        return;
    }

    s.counted_fills.insert(order_id.to_string());
    // 检查是否属于价差组合（multi-leg）
    let total_legs = fill.get("total_legs").and_then(|v| v.as_u64()).unwrap_or(1);
    if total_legs >= 2 {
        // 从 order_id 提取 base（去掉 -L0/-L1 后缀）
        let base = match order_id.rsplit_once("-L") {
            Some((base, _)) if !base.is_empty() => base.to_string(),
            _ => {
                warn!(order_id = %order_id, "⚠️ 价差订单缺少腿编号 -L0/-L1，按单腿处理");
                // 降级为单腿
                s.position_count += 1;
                let pos = TrackedPosition::new(order_id.to_string(), sym.to_string(), side, entry, qty, strategy_id.clone(), source_signal_id.to_string());
                s.positions.insert(order_id.to_string(), pos);
                info!(order_id = %order_id, symbol = %sym, strategy = %strategy_id, "📊 持仓已跟踪（降级单腿）");
                return;
            }
        };

        let pos = TrackedPosition::new(order_id.to_string(), sym.to_string(), side, entry, qty, strategy_id.clone(), source_signal_id.to_string());

        if let Some(first_leg) = s.pending_spreads.remove(&base) {
            // ── 第二条腿到达 → 配对 ──
            let (sell_leg, buy_leg) = if matches!(first_leg.side, OrderSide::Sell) {
                (first_leg, pos)
            } else {
                (pos, first_leg)
            };

            let sell_entry = sell_leg.entry_price;
            let buy_entry = buy_leg.entry_price;
            // 净权利金 = 卖腿收入 - 买腿支出
            let net_credit = sell_entry - buy_entry;

            // 从 fill instrument 解析 strike 计算宽度
            let sell_strike = fill_for_leg(&fill, &sell_leg)
                .or_else(|| fill_strike_from_pending(&base, &s, &sell_leg.order_id));
            let buy_strike = fill_for_leg(&fill, &buy_leg)
                .or_else(|| fill_strike_from_pending(&base, &s, &buy_leg.order_id));

            let width: Decimal;
            if let (Some(ss), Some(bs)) = (sell_strike, buy_strike) {
                width = if ss > bs { ss - bs } else { bs - ss };
            } else {
                // 无法解析 strike，回退到配置的 wing_width
                width = args.spread_wing_width;
                warn!(base = %base, "⚠️ 无法解析价差strike，使用配置宽度 {:.1}pt", width);
            }

            let max_loss = (width * Decimal::from(100) - net_credit * Decimal::from(100)).max(Decimal::ZERO) * qty;

            let mut sell_pos = sell_leg;
            let mut buy_pos = buy_leg;

            sell_pos.spread_pair_id = Some(buy_pos.order_id.clone());
            sell_pos.spread_net_credit = net_credit;
            sell_pos.spread_max_loss = max_loss;
            buy_pos.spread_pair_id = Some(sell_pos.order_id.clone());
            buy_pos.spread_net_credit = net_credit;
            buy_pos.spread_max_loss = max_loss;

            s.positions.insert(sell_pos.order_id.clone(), sell_pos);
            s.positions.insert(buy_pos.order_id.clone(), buy_pos);
            s.position_count += 2;
            info!(base = %base, net_credit = %net_credit, width = %width, max_loss = %max_loss, qty = %qty, "🔗 信用价差已配对 净权{:.2}×{}=${}", net_credit, qty, net_credit * Decimal::from(100) * qty);
        } else {
            // 第一条腿 → 暂存等待配对
            s.pending_spreads.insert(base.clone(), pos);
            let leg_num = fill.get("leg").and_then(|v| v.as_u64()).unwrap_or(0);
            info!(base = %base, leg = leg_num, "⏳ 等待价差第二条腿");
        }
    } else {
        // 单腿持仓
        s.position_count += 1;
        let pos = TrackedPosition::new(order_id.to_string(), sym.to_string(), side, entry, qty, strategy_id.clone(), source_signal_id.to_string());
        s.positions.insert(order_id.to_string(), pos);
        info!(order_id = %order_id, symbol = %sym, side = %side_str, entry = %entry, qty = %qty, count = s.position_count, strategy = %strategy_id, "📊 持仓已跟踪");
    }

}

/// 从某条腿的 fill 消息解析 strike
fn fill_strike(fill: &serde_json::Value) -> Option<Decimal> {
    fill.get("instrument")
        .and_then(|i| i.get("strike"))
        .and_then(|v| v.as_str())
        .and_then(|s| Decimal::from_str_exact(s).ok())
        .or_else(|| {
            fill.get("instrument")
                .and_then(|i| i.get("strike"))
                .and_then(|v| v.as_f64())
                .and_then(|f| Decimal::from_f64_retain(f))
        })
}

/// 按腿的 order_id 匹配 fill：当前 fill 匹配某条腿时用它，否则尝试从 pending 中找
fn fill_for_leg(fill: &serde_json::Value, leg: &TrackedPosition) -> Option<Decimal> {
    let fill_oid = fill.get("order_id").and_then(|v| v.as_str()).unwrap_or("");
    if fill_oid == leg.order_id {
        fill_strike(fill)
    } else {
        None
    }
}

/// 从 pending_spreads 中取某条腿的 strike（通过 symbol 反查，精度有限）
fn fill_strike_from_pending(_base: &str, _s: &std::sync::MutexGuard<'_, RiskState>, _oid: &str) -> Option<Decimal> {
    // pending_spreads 被 remove 后已不存在，只能通过 symbol 中解析 strike
    // option symbol 格式: QQQ{YYMMDD}{C/P}{STRIKE*1000}.US
    // 例如: QQQ260601C450000.US → strike = 450000 / 1000 = 450
    // 但 leg 的 symbol 已在 TrackedPosition 中，这里尝试从 fill 获取
    None // 回退到配置 SPREAD_WING_WIDTH
}

// ── 市场状态 ──────────────────────────────────────────

fn handle_status(state: &Arc<Mutex<RiskState>>, payload: &[u8]) {
    let status: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(e) => { warn!(error = %e, "解析状态失败"); return; }
    };
    let event = status.get("event").and_then(|v| v.as_str()).unwrap_or("");
    match event {
        "OPEN" => { state.lock().unwrap().reset(); info!("📈 开盘 — 状态重置"); }
        "CLOSE" => {
            let count = state.lock().unwrap().position_count;
            state.lock().unwrap().reset();
            info!(final_count = count, "📉 收盘 — 状态清理");
        }
        other => info!(event = %other, "市场状态"),
    }
}

// ── 信号处理 ──────────────────────────────────────────

async fn handle_signal(
    nats: &async_nats::Client,
    args: &Args,
    state: &Arc<Mutex<RiskState>>,
    redis: &mut redis::aio::MultiplexedConnection,
    payload: &[u8],
) -> Result<()> {
    let signal: StrategySignal = serde_json::from_slice(payload).context("解析信号失败")?;
    let signal_id = signal.signal_id.clone();

    // ── 去重：内存 + Redis 双重检查 ──
    {
        let mut s = state.lock().unwrap();
        if s.processed_signals.contains(&signal_id) {
            warn!(signal_id = %signal_id, "🔁 重复信号（内存）");
            return Ok(());
        }
        s.processed_signals.insert(signal_id.clone());
    }

    // Redis 持久化去重：重启后仍然生效
    let redis_key = format!("risk:signal:{}", signal_id);
    let already: bool = redis.exists(&redis_key).await.unwrap_or(false);
    if already {
        warn!(signal_id = %signal_id, "🔁 重复信号（Redis）");
        return Ok(());
    }
    // SETEX key 86400 value (24h TTL)
    let _: () = redis.set_ex(&redis_key, "1", 86400).await?;

    let (current_count, buying_power, vix) = {
        let s = state.lock().unwrap();
        (s.position_count, s.buying_power, s.vix_current)
    };
    let (decision, reason) = evaluate_signal(&signal, args, current_count, buying_power, vix);

    nats.publish(
        subjects::risk(&signal.instrument),
        json_bytes(&RiskReport {
            signal_id: signal_id.clone(),
            decision: decision.clone(),
            reason: reason.clone(),
            checked_at: Utc::now(),
        })?.into(),
    ).await?;

    if !matches!(decision, RiskDecision::Approved) {
        warn!(signal_id = %signal_id, reason = %reason, "信号被拒");
        return Ok(());
    }

    let quantity = dynamic_quantity_with_vix(signal.confidence, args.min_confidence, args.min_order_quantity, args.max_order_quantity, vix);
    if quantity <= Decimal::ZERO {
        warn!(signal_id = %signal_id, vix = %vix, "VIX过低暂停交易 → 信号被拒");
        return Ok(());
    }
    let side = match signal.action {
        SignalAction::Buy => OrderSide::Buy,
        SignalAction::Sell => OrderSide::Sell,
        SignalAction::Hold => return Ok(()),
    };

    let intent = OrderIntent {
        intent_id: format!("intent-{}-{}", signal.strategy_id, Utc::now().timestamp_millis()),
        source_signal_id: signal_id.clone(),
        instrument: signal.instrument,
        side,
        quantity,
        order_type: OrderType::Market,
        limit_price: None,
        reference_price: signal.reference_price,
        created_at: Utc::now(),
        spread_wing: signal.spread_wing.clone(),
        exit_reason: None,
    };
    let subject = subjects::order_intent(&intent.instrument);
    nats.publish(subject.clone(), json_bytes(&intent)?.into()).await?;

    info!(signal_id = %signal_id, conf = %signal.confidence, qty = %quantity, "✅ 订单已发布");
    Ok(())
}

fn evaluate_signal(signal: &StrategySignal, args: &Args, current_count: u32, buying_power: Decimal, vix: Decimal) -> (RiskDecision, String) {
    if matches!(signal.action, SignalAction::Hold) {
        return (RiskDecision::Rejected, "HOLD".into());
    }
    if signal.confidence < args.min_confidence {
        return (RiskDecision::Rejected, format!("信心{:.2}<{:.2}", signal.confidence, args.min_confidence));
    }
    if current_count >= args.position_size {
        return (RiskDecision::Rejected, format!("持仓满({}/{})", current_count, args.position_size));
    }

    // ── 强制信用价差：所有开仓必须带保护腿 ──
    if signal.spread_wing.is_none() && matches!(signal.action, SignalAction::Sell) {
        return (RiskDecision::Rejected, "禁止裸卖: 卖出必须带spread_wing".into());
    }

    // ── 信用价差风险检查 ──
    if let Some(ref wing) = signal.spread_wing {
        let sell_strike = signal.instrument.strike.unwrap_or(Decimal::ZERO);
        let wing_strike = wing.strike.unwrap_or(Decimal::ZERO);
        if sell_strike > Decimal::ZERO && wing_strike > Decimal::ZERO {
            let width = if sell_strike > wing_strike {
                sell_strike - wing_strike
            } else {
                wing_strike - sell_strike
            };
            // 价差宽度 × 100（每张合约乘数）
            let max_loss_per_contract = width * Decimal::from(100);
            let qty = dynamic_quantity_with_vix(signal.confidence, args.min_confidence, args.min_order_quantity, args.max_order_quantity, vix);
            let total_max_loss = max_loss_per_contract * qty;
            if total_max_loss > args.max_risk_per_trade {
                return (RiskDecision::Rejected, format!(
                    "价差风险${:.0} > 限额${:.0} (宽{}pt × {}张)",
                    total_max_loss, args.max_risk_per_trade, width, qty
                ));
            }
            // ── 购买力检查：最大亏损不能超过可用购买力 ──
            if buying_power > Decimal::ZERO && buying_power < Decimal::MAX && total_max_loss > buying_power {
                return (RiskDecision::Rejected, format!(
                    "买力不足: 风险${:.0} > 购买力${:.0}", total_max_loss, buying_power
                ));
            }
        }
    }

    let qty = dynamic_quantity_with_vix(signal.confidence, args.min_confidence, args.min_order_quantity, args.max_order_quantity, vix);
    (RiskDecision::Approved, format!("通过 {}/{} 信{:.2}→{:.0}张 VIX{:.0}", current_count, args.position_size, signal.confidence, qty, vix))
}

// ── 持仓监控（每秒巡检）───────────────────────────────

async fn check_positions(nats: &async_nats::Client, args: &Args, state: &Arc<Mutex<RiskState>>) {
    // 先收集当前报价（避免在 iter_mut 中读 latest_prices 导致借用冲突）
    let price_snapshot: HashMap<String, Decimal>;
    {
        let s = state.lock().unwrap();
        price_snapshot = s.latest_prices.clone();
    }

    let mut exits: Vec<(String, TrackedPosition, String)> = Vec::new();

    {
        let mut s = state.lock().unwrap();

        // 时间止损
        if args.time_stop_min > 0 {
            if let Some(minutes) = minutes_to_close(&args.market_close_utc) {
                if minutes <= args.time_stop_min as i64 {
                    let mut visited: HashSet<String> = HashSet::new();
                    for (oid, pos) in s.positions.iter() {
                        if pos.exit_triggered { continue; }
                        // 信用价差去重：只推一条腿，send_exit_order 会自动平 pair
                        if let Some(ref pid) = pos.spread_pair_id {
                            if visited.contains(pid) { continue; }
                        }
                        visited.insert(oid.clone());
                        exits.push((oid.clone(), pos.clone(), format!("⏰ 距收盘{}分钟", minutes)));
                    }
                    for (_, pos) in s.positions.iter_mut() {
                        pos.exit_triggered = true;
                    }
                }
            }
        }

        // VIX 飙升保护（双模态检测）
        // ① 瞬时跳变：相邻两次更新（≈10s）的点差 ≥ 阈值
        let vix_delta = (s.vix_current - s.vix_previous).abs();
        // ② 慢涨飙升：最近 60s（6 次更新）的累计变化 ≥ 阈值
        let vix_rolling_delta = if s.vix_history.len() >= 6 {
            (s.vix_history.back().copied().unwrap_or_default() 
             - s.vix_history.front().copied().unwrap_or_default()).abs()
        } else {
            Decimal::ZERO
        };
        let vix_spiking = s.vix_current > Decimal::ZERO 
            && s.vix_previous > Decimal::ZERO 
            && (vix_delta >= args.vix_spike_threshold || vix_rolling_delta >= args.vix_spike_threshold);
        if vix_spiking {
            warn!(vix_prev = %s.vix_previous, vix_now = %s.vix_current, delta = %vix_delta, rolling = %vix_rolling_delta, "⚠️ VIX 飙升，强制全平");
            let mut visited: HashSet<String> = HashSet::new();
            for (oid, pos) in s.positions.iter() {
                if pos.exit_triggered { continue; }
                // 信用价差去重：只推一条腿
                if let Some(ref pid) = pos.spread_pair_id {
                    if visited.contains(pid) { continue; }
                }
                visited.insert(oid.clone());
                exits.push((oid.clone(), pos.clone(), format!("⚠️ VIX飙升 {:.1}→{:.1}", s.vix_previous, s.vix_current)));
            }
            for (_, pos) in s.positions.iter_mut() {
                pos.exit_triggered = true;
            }
        }

        // ── 先克隆持仓快照（价差对需要读两条腿，避免借用冲突）──
        let all_positions: HashMap<String, TrackedPosition> = s.positions.clone();
        let mut visited_spreads: HashSet<String> = HashSet::new();

        // ── 单腿止盈止损（跳过价差腿）──
        for (oid, pos) in s.positions.iter_mut() {
            if pos.exit_triggered { continue; }
            if pos.spread_pair_id.is_some() { continue; } // 价差腿走下面专用逻辑

            let cp = price_snapshot.get(&pos.symbol).copied().unwrap_or(pos.entry_price);
            if cp <= Decimal::ZERO { continue; }

            pos.update_price(cp, args);
            let (tp, sl) = pos.check_exit(cp, args);

            if sl {
                pos.exit_triggered = true;
                let loss = match pos.side {
                    OrderSide::Buy  => ((Decimal::ONE - cp / pos.entry_price) * Decimal::from(100)).round_dp(1),
                    OrderSide::Sell => ((cp / pos.entry_price - Decimal::ONE) * Decimal::from(100)).round_dp(1),
                };
                exits.push((oid.clone(), pos.clone(), format!("🛑 止损 亏{}%", loss)));
            } else if tp {
                pos.exit_triggered = true;
                exits.push((oid.clone(), pos.clone(), "🎯 动态止盈".into()));
            }
        }

        // ── 信用价差止盈止损（净PnL）──
        for (oid, pos) in all_positions.iter() {
            if pos.exit_triggered { continue; }
            if pos.spread_pair_id.is_none() { continue; }
            if visited_spreads.contains(oid) { continue; }

            let pair_oid = match &pos.spread_pair_id {
                Some(id) => id.clone(),
                None => continue,
            };
            let pair_pos = match s.positions.get(&pair_oid) {
                Some(p) => p,
                None => continue,
            };
            if pair_pos.exit_triggered { continue; }

            visited_spreads.insert(oid.clone());
            visited_spreads.insert(pair_oid.clone());

            let cp = price_snapshot.get(&pos.symbol).copied().unwrap_or(pos.entry_price);
            let pair_cp = price_snapshot.get(&pair_pos.symbol).copied().unwrap_or(pair_pos.entry_price);
            if cp <= Decimal::ZERO || pair_cp <= Decimal::ZERO { continue; }

            // 净 PnL = (卖腿盈亏 + 买腿盈亏) × $100 × qty
            let multiplier = Decimal::from(100);
            let leg_pnl = match pos.side {
                OrderSide::Sell => pos.entry_price - cp,       // 卖出：降价赚
                OrderSide::Buy => cp - pos.entry_price,         // 买入：涨价赚
            };
            let pair_leg_pnl = match pair_pos.side {
                OrderSide::Sell => pair_pos.entry_price - pair_cp,
                OrderSide::Buy => pair_cp - pair_pos.entry_price,
            };
            let net_pnl = (leg_pnl + pair_leg_pnl) * multiplier * pos.quantity;

            let max_loss = pos.spread_max_loss;
            let max_profit = pos.spread_net_credit * multiplier * pos.quantity;

            let reason = if max_loss > Decimal::ZERO && net_pnl <= Decimal::ZERO - max_loss * args.spread_sl_pct {
                let loss_pct = (net_pnl.abs() / max_loss * Decimal::from(100)).round_dp(1);
                Some(format!("🛑 价差止损 {}% (net${:.0})", loss_pct, net_pnl))
            } else if max_profit > Decimal::ZERO && net_pnl >= max_profit * args.spread_tp_pct {
                let gain_pct = (net_pnl / max_profit * Decimal::from(100)).round_dp(1);
                Some(format!("🎯 价差止盈 {}% (net${:.0})", gain_pct, net_pnl))
            } else {
                None
            };

            if let Some(reason) = reason {
                if let Some(p) = s.positions.get_mut(oid) { p.exit_triggered = true; }
                if let Some(p) = s.positions.get_mut(&pair_oid) { p.exit_triggered = true; }
                exits.push((oid.clone(), pos.clone(), reason));
            }
        }
    }

    for (order_id, pos, reason) in exits {
        send_exit_order(nats, state, &order_id, &pos, &reason).await;
    }

}

async fn send_exit_order(
    nats: &async_nats::Client,
    state: &Arc<Mutex<RiskState>>,
    order_id: &str,
    pos: &TrackedPosition,
    reason: &str,
) {
    let exit_side = match pos.side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    };

    let instrument = Instrument {
        asset_class: AssetClass::Option,
        symbol: pos.symbol.clone(),
        venue: Some(Venue::Nasdaq),
        base: None,
        quote: None,
        expiry: None,
        strike: None,
        option_right: None,
    };

    // ── 信用价差：构建保护腿 Instrument 用于原子平仓 ──
    let pair_order_id = pos.spread_pair_id.clone();
    let pair_wing: Option<Instrument> = pair_order_id.as_ref().and_then(|pid| {
        let s = state.lock().unwrap();
        s.positions.get(pid).map(|pair| Instrument {
            asset_class: AssetClass::Option,
            symbol: pair.symbol.clone(),
            venue: Some(Venue::Nasdaq),
            base: None, quote: None, expiry: None,
            strike: None, option_right: None,
        })
    });

    let intent = OrderIntent {
        intent_id: format!("exit-{}-{}", order_id, Utc::now().timestamp_millis()),
        source_signal_id: pos.source_signal_id.clone(),
        instrument,
        side: exit_side,
        quantity: pos.quantity,
        order_type: OrderType::Market,
        limit_price: None,
        reference_price: pos.entry_price,
        created_at: Utc::now(),
        spread_wing: pair_wing,  // 信用价差保护腿 → Executor 原子处理双腿
        exit_reason: Some(reason.to_string()),
    };

    let subject = subjects::order_intent(&intent.instrument);

    // ── 同步发风控事件（Market Regime 用此计数连续亏损）──
    match json_bytes(&RiskReport {
        signal_id: format!("exit-{}", order_id),
        decision: RiskDecision::Approved,
        reason: reason.to_string(),
        checked_at: Utc::now(),
    }) {
        Ok(payload) => {
            if let Err(e) = nats.publish(
                subjects::risk(&intent.instrument),
                payload.into(),
            ).await {
                warn!(error = %e, "风险事件发布失败");
            }
        }
        Err(e) => warn!(error = %e, "序列化风险事件失败"),
    }

    // ── 信用价差双腿通过 spread_wing 原子平仓，由 Executor 保证顺序 ──
    let result = nats.publish(subject.clone(), json_bytes(&intent).unwrap().into()).await;
    match result {
        Ok(_) => {
            let held = Utc::now().signed_duration_since(pos.entry_time);
            if pos.spread_pair_id.is_some() {
                info!(order_id = %order_id, symbol = %pos.symbol, qty = %pos.quantity, held_min = held.num_minutes(), reason = %reason, "📤 平仓单已发送（含保护腿，Executor 原子处理）");
            } else {
                info!(order_id = %order_id, symbol = %pos.symbol, qty = %pos.quantity, held_min = held.num_minutes(), reason = %reason, "📤 平仓单已发送（等待成交确认）");
            }
        }
        Err(e) => {
            error!(error = %e, order_id = %order_id, "平仓单发送失败");
            // 回退主腿和 pair 腿（如果存在），下个 tick 重试
            let mut s = state.lock().unwrap();
            if let Some(p) = s.positions.get_mut(order_id) {
                p.exit_triggered = false;
            }
            if let Some(ref pid) = pos.spread_pair_id {
                if let Some(p) = s.positions.get_mut(pid) {
                    p.exit_triggered = false;
                }
            }
        }
    }
}

// ── VIX 状态处理 ──────────────────────────────────────

fn handle_vix_state(state: &Arc<Mutex<RiskState>>, _args: &Args, payload: &[u8]) {
    let data: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(_) => return,
    };
    // Market Regime 发布 regime.option.qqq，字段在 details.vix（ATM IV × 100）
    let vix = data.get("details")
        .and_then(|d| d.get("vix"))
        .and_then(|v| v.as_f64())
        .map(|f| Decimal::from_f64_retain(f).unwrap_or_default());

    let vix = match vix {
        Some(v) if v > Decimal::ZERO => v,
        _ => return,
    };

    let mut s = state.lock().unwrap();
    s.vix_previous = s.vix_current;
    s.vix_current = vix;
    // 滚动窗口：保留最近 6 次更新（≈60s），用于检测慢涨飙升
    s.vix_history.push_back(vix);
    if s.vix_history.len() > 6 {
        s.vix_history.pop_front();
    }
}

// ── 账户余额处理 ──────────────────────────────────────

fn handle_balance(state: &Arc<Mutex<RiskState>>, payload: &[u8]) {
    let data: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(_) => return,
    };
    let bp = data.get("buying_power")
        .and_then(|v| v.as_f64())
        .map(|f| Decimal::from_f64_retain(f).unwrap_or_default());
    if let Some(bp) = bp {
        let mut s = state.lock().unwrap();
        s.buying_power = bp;
    }
}

/// VIX 分层仓位倍率
fn vix_multiplier(vix: Decimal) -> Decimal {
    if vix < Decimal::from(15) { return Decimal::ZERO }      // 暂停交易
    if vix < Decimal::from(25) { return Decimal::ONE }       // 标准
    if vix < Decimal::from(35) { return Decimal::from_f64_retain(1.5).unwrap() }  // 加大
    Decimal::from_f64_retain(0.5).unwrap()                    // 保守
}

/// 动态仓位（含 VIX 倍率）
fn dynamic_quantity_with_vix(confidence: Decimal, min_conf: Decimal, min_qty: Decimal, max_qty: Decimal, vix: Decimal) -> Decimal {
    let mult = vix_multiplier(vix);
    // VIX < 15 → 暂停交易，返回 0 张（由调用方处理 Reject）
    if mult == Decimal::ZERO { return Decimal::ZERO; }
    let base = dynamic_quantity(confidence, min_conf, min_qty, max_qty);
    let qty = base * mult;
    if qty < Decimal::ONE { Decimal::ONE } else { qty.round_dp(0) }
}
