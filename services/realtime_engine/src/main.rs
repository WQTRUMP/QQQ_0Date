//! Realtime Engine v3
//!
//! 订阅 quote.* + kline.* → 构建 RealtimeState（含技术指标）→ Redis + NATS
//!
//! 订阅:
//!   quote.option.qqq / quote.option.* → 行情
//!   quote.option.vix                  → VIX 恐慌指数
//!   kline.option.qqq                  → QQQ 1 分钟 K 线

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use futures_util::StreamExt;
use qqq_common::{
    json_bytes, subjects, AssetClass, Instrument, OptionRight, PremarketInit, RealtimeState,
    TradeSide,
};
use redis::AsyncCommands;
use rust_decimal::Decimal;
use std::collections::VecDeque;
use tracing::{error, info};

const MAX_KLINES: usize = 200;
const DONCHIAN_PERIOD: usize = 20;
const ADX_PERIOD: usize = 14;
const BB_PERIOD: usize = 20;

#[derive(Debug, Parser)]
#[command(name = "realtime_engine")]
#[command(about = "实时状态引擎 v3：K线缓存 + 技术指标")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    redis_url: String,
    #[arg(long, env = "QUOTE_SUBJECT", default_value = "quote.option.>")]
    quote_subject: String,
    #[arg(long, env = "KLINE_SUBJECT", default_value = "kline.option.qqq")]
    kline_subject: String,
    #[arg(long, env = "VIX_SUBJECT", default_value = "quote.option.vix")]
    vix_subject: String,
}

#[derive(Debug, Clone)]
struct KlineBar {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

struct IndicatorCache {
    bars: VecDeque<KlineBar>,
    vix: Option<f64>,
    qqq_price: Option<Decimal>,
    bars_since_init: usize,
}

impl IndicatorCache {
    fn new() -> Self {
        Self {
            bars: VecDeque::with_capacity(MAX_KLINES),
            vix: None,
            qqq_price: None,
            bars_since_init: 0,
        }
    }
    fn push_kline(&mut self, bar: KlineBar) {
        self.bars.push_back(bar);
        if self.bars.len() > MAX_KLINES { self.bars.pop_front(); }
        self.bars_since_init += 1;
    }
    fn kline_count(&self) -> u32 { self.bars.len() as u32 }
    fn should_publish_init(&self) -> bool {
        self.bars.len() >= 20 && (self.bars.len() == 20 || self.bars_since_init >= 60)
    }
    fn mark_init_published(&mut self) {
        self.bars_since_init = 0;
    }

    fn donchian(&self, n: usize) -> Option<(f64, f64)> {
        if self.bars.len() < n { return None; }
        let slice: Vec<_> = self.bars.iter().rev().take(n).collect();
        let high = slice.iter().map(|b| b.high).fold(f64::MIN, f64::max);
        let low = slice.iter().map(|b| b.low).fold(f64::MAX, f64::min);
        Some((high, low))
    }
    fn sma(&self, n: usize) -> Option<f64> {
        if self.bars.len() < n { return None; }
        let sum: f64 = self.bars.iter().rev().take(n).map(|b| b.close).sum();
        Some(sum / n as f64)
    }
    fn adx(&self, n: usize) -> Option<f64> {
        if self.bars.len() < n + 1 { return None; }
        let closes: Vec<f64> = self.bars.iter().map(|b| b.close).collect();
        let highs: Vec<f64> = self.bars.iter().map(|b| b.high).collect();
        let lows: Vec<f64> = self.bars.iter().map(|b| b.low).collect();
        let mut tr_sum = 0.0f64;
        let mut pdm_sum = 0.0f64;
        let mut ndm_sum = 0.0f64;
        for i in (closes.len() - n)..closes.len() {
            let prev_close = closes[i - 1];
            let high = highs[i]; let low = lows[i];
            let tr = (high - low).max((high - prev_close).abs()).max((low - prev_close).abs());
            let up_move = high - highs[i - 1];
            let down_move = lows[i - 1] - low;
            let pdm = if up_move > down_move && up_move > 0.0 { up_move } else { 0.0 };
            let ndm = if down_move > up_move && down_move > 0.0 { down_move } else { 0.0 };
            tr_sum += tr; pdm_sum += pdm; ndm_sum += ndm;
        }
        if tr_sum == 0.0 { return Some(0.0); }
        let pdi = (pdm_sum / tr_sum) * 100.0;
        let ndi = (ndm_sum / tr_sum) * 100.0;
        if pdi + ndi == 0.0 { return Some(0.0); }
        Some(((pdi - ndi).abs() / (pdi + ndi)) * 100.0)
    }
    fn volume_avg(&self, n: usize) -> Option<f64> {
        if self.bars.len() < n { return None; }
        let sum: f64 = self.bars.iter().rev().take(n).map(|b| b.volume).sum();
        Some(sum / n as f64)
    }
    fn bollinger(&self, n: usize, k: f64) -> Option<(f64, f64, f64)> {
        if self.bars.len() < n { return None; }
        let mid = self.sma(n)?;
        let variance: f64 = self.bars.iter().rev().take(n).map(|b| (b.close - mid).powi(2)).sum::<f64>() / n as f64;
        let std = variance.sqrt();
        Some((mid + k * std, mid, mid - k * std))
    }
    fn atr(&self, n: usize) -> Option<f64> {
        if self.bars.len() < n + 1 { return None; }
        let closes: Vec<f64> = self.bars.iter().map(|b| b.close).collect();
        let highs: Vec<f64> = self.bars.iter().map(|b| b.high).collect();
        let lows: Vec<f64> = self.bars.iter().map(|b| b.low).collect();
        let start = closes.len() - n;
        let mut tr_sum = 0.0;
        for i in start..closes.len() {
            let prev_close = closes[i - 1];
            let tr = (highs[i] - lows[i]).max((highs[i] - prev_close).abs()).max((lows[i] - prev_close).abs());
            tr_sum += tr;
        }
        Some(tr_sum / n as f64)
    }
    fn historical_volatility(&self, n: usize) -> Option<f64> {
        if self.bars.len() < n + 1 { return None; }
        let closes: Vec<f64> = self.bars.iter().map(|b| b.close).collect();
        let start = closes.len() - n - 1;
        let mut returns = Vec::with_capacity(n);
        for i in (start + 1)..closes.len() {
            returns.push((closes[i] / closes[i - 1]).ln());
        }
        let mean = returns.iter().sum::<f64>() / n as f64;
        let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
        Some(variance.sqrt() * (252.0_f64 * 390.0).sqrt())
    }
    fn trend(&self, n: usize) -> Option<(f64, f64)> {
        if self.bars.len() < n { return None; }
        let closes: Vec<f64> = self.bars.iter().rev().take(n).map(|b| b.close).collect();
        let closes: Vec<f64> = closes.into_iter().rev().collect();
        let n_f = n as f64;
        let sum_x = (0..n).map(|i| i as f64).sum::<f64>();
        let sum_y: f64 = closes.iter().sum();
        let sum_xy: f64 = closes.iter().enumerate().map(|(i, &y)| i as f64 * y).sum();
        let sum_x2: f64 = (0..n).map(|i| (i as f64).powi(2)).sum();
        let mean_y = sum_y / n_f;
        let slope = (n_f * sum_xy - sum_x * sum_y) / (n_f * sum_x2 - sum_x * sum_x);
        let mean_x = sum_x / n_f;
        let intercept = mean_y - slope * mean_x;
        let ss_res: f64 = closes.iter().enumerate().map(|(i, &y)| (y - (slope * i as f64 + intercept)).powi(2)).sum();
        let ss_tot: f64 = closes.iter().map(|&y| (y - mean_y).powi(2)).sum();
        let r_squared = if ss_tot == 0.0 { 0.0 } else { (1.0 - ss_res / ss_tot).clamp(0.0, 1.0) };
        Some((slope, r_squared))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr).with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();
    let args = Args::parse();
    let nats = async_nats::connect(&args.nats_url).await?;
    let redis_client = redis::Client::open(args.redis_url.clone())?;
    let mut redis = redis_client.get_multiplexed_tokio_connection().await?;

    let mut quote_stream = nats.subscribe(args.quote_subject.clone()).await?;
    let mut kline_stream = nats.subscribe(args.kline_subject.clone()).await?;
    let mut vix_stream = nats.subscribe(args.vix_subject.clone()).await?;

    info!(quote = %args.quote_subject, kline = %args.kline_subject, vix = %args.vix_subject, "实时状态引擎 v4 启动（含冷启动恢复）");

    let cache = std::sync::Arc::new(std::sync::Mutex::new(IndicatorCache::new()));
    let init_published = std::sync::Arc::new(std::sync::Mutex::new(false));

    // ── 冷启动：从 Redis 恢复 K 线缓存 ──
    {
        let bars_json: Vec<String> = redis.lrange("kline:qqq:history", 0, 19).await.unwrap_or_default();
        if !bars_json.is_empty() {
            // 过滤过期数据：只保留今天（24h 内）的 K 线
            let now_ts = Utc::now().timestamp();
            let cutoff_ts = now_ts - 86400; // 24 小时前
            let total_count = bars_json.len();
            let fresh_bars: Vec<String> = bars_json
                .into_iter()
                .filter(|json_str| {
                    if let Ok(bar) = serde_json::from_str::<serde_json::Value>(json_str) {
                        bar["timestamp"].as_i64().map_or(false, |ts| ts >= cutoff_ts)
                    } else {
                        false
                    }
                })
                .collect();
            if fresh_bars.is_empty() {
                info!("⚠️ Redis K 线缓存均过期（>24h），丢弃");
            } else if fresh_bars.len() < total_count {
                info!(fresh = fresh_bars.len(), total = total_count, "🧹 过滤过期的 Redis K 线");
            }

            let mut c = cache.lock().unwrap();
            for json_str in fresh_bars.iter().rev() {
                if let Ok(bar) = serde_json::from_str::<serde_json::Value>(json_str) {
                    if let (Some(o), Some(h), Some(l), Some(cl), Some(v)) = (
                        bar["open"].as_f64(), bar["high"].as_f64(), bar["low"].as_f64(),
                        bar["close"].as_f64(), bar["volume"].as_i64(),
                    ) {
                        c.push_kline(KlineBar { open: o, high: h, low: l, close: cl, volume: v as f64 });
                    }
                }
            }
            let count = c.kline_count();
            if count > 0 {
                info!(count = count, "📦 从 Redis 恢复 K 线缓存");
                if count >= 20 {
                    *init_published.lock().unwrap() = true;
                    info!("✅ K 线已就绪，跳过冷启动等待");
                }
                // ── 盘中重启：把缓存的 K 线推送到 NATS，策略引擎立即可用 ──
                drop(c);
                let kline_nats_cold = nats.clone();
                // fresh_bars 已按 LPUSH 顺序（最新在前），反转发（最旧在前）
                let mut warmup_count = 0u32;
                for json_str in fresh_bars.iter().rev() {
                    let _ = kline_nats_cold.publish("kline.option.qqq", json_str.as_bytes().to_vec().into()).await;
                    warmup_count += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                }
                info!(count = warmup_count, "📡 已推送缓存 K 线到策略引擎（盘中重启恢复）");
            }
        }
    }

    // K 线处理（写缓存 + 盘前初始化 + 冷启动日志 + Redis 持久化）
    let kline_nats = nats.clone();
    let kline_cache = cache.clone();
    let kline_redis = redis_client.clone();
    let kline_init_published = init_published.clone();
    tokio::spawn(async move {
        while let Some(msg) = kline_stream.next().await {
            let bar: serde_json::Value = match serde_json::from_slice(&msg.payload) { Ok(v) => v, Err(_) => continue };
            let (Some(open), Some(high), Some(low), Some(close), Some(vol)) = (
                bar["open"].as_f64(), bar["high"].as_f64(), bar["low"].as_f64(),
                bar["close"].as_f64(), bar["volume"].as_i64(),
            ) else { continue };

            let count = {
                let mut c = kline_cache.lock().unwrap();
                c.push_kline(KlineBar { open, high, low, close, volume: vol as f64 });
                c.kline_count()
            };
            // 持久化到 Redis（冷启动恢复用）
            if let Ok(payload_str) = String::from_utf8(msg.payload.to_vec()) {
                if let Ok(mut r) = kline_redis.get_multiplexed_tokio_connection().await {
                    let _: Result<(), _> = redis::cmd("LPUSH").arg("kline:qqq:history").arg(&payload_str).query_async(&mut r).await;
                    let _: Result<(), _> = redis::cmd("LTRIM").arg("kline:qqq:history").arg(0).arg(19).query_async(&mut r).await;
                }
            }
            if count == 1 { info!("⏳ K 线冷启动中——需要 20 根 1 分钟 K 线才能计算指标（约 20 分钟）"); }
            if count == 20 && !*kline_init_published.lock().unwrap() { *kline_init_published.lock().unwrap() = true; info!("✅ K 线缓存就绪，开始计算技术指标"); }
            let should_publish = {
                let c = kline_cache.lock().unwrap();
                c.should_publish_init()
            };
            if should_publish { publish_init(&kline_nats, &kline_cache).await; }
            if count % 50 == 1 && count > 20 { info!(count = count, "K 线缓存"); }
        }
    });

    // VIX 处理
    let vix_cache = cache.clone();
    tokio::spawn(async move {
        while let Some(msg) = vix_stream.next().await {
            let quote: serde_json::Value = match serde_json::from_slice(&msg.payload) { Ok(v) => v, Err(_) => continue };
            if let Some(ld) = quote["last_done"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                vix_cache.lock().unwrap().vix = Some(ld);
            }
        }
    });

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => { info!("退出"); break; }
            Some(msg) = quote_stream.next() => {
                if let Err(e) = handle_quote(&nats, &mut redis, &cache, &msg.payload).await {
                    error!(error = %e, "处理行情失败");
                }
            }
        }
    }
    Ok(())
}

async fn handle_quote(
    nats: &async_nats::Client, redis: &mut redis::aio::MultiplexedConnection,
    cache: &std::sync::Arc<std::sync::Mutex<IndicatorCache>>, payload: &[u8],
) -> Result<()> {
    let quote: serde_json::Value = serde_json::from_slice(payload)?;
    let symbol = quote["symbol"].as_str().unwrap_or("");
    if symbol == "QQQ.US" {
        if let Some(ld) = quote["last_done"].as_str().and_then(|s| Decimal::from_str_exact(s).ok()) {
            cache.lock().unwrap().qqq_price = Some(ld);
            publish_state(nats, redis, cache, ld).await;
        }
        return Ok(());
    }
    if symbol.contains("VIX") { return Ok(()); }
    let key = symbol.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>().to_ascii_lowercase();
    let last_done = quote["last_done"].as_str().and_then(|s| Decimal::from_str_exact(s).ok()).unwrap_or_default();
    let ext = &quote["option_extend"];
    let state = RealtimeState {
        instrument: Instrument {
            asset_class: AssetClass::Option, symbol: symbol.to_string(),
            venue: None, base: None, quote: None,
            expiry: ext.get("expiry_date").and_then(|v| v.as_str()).map(|s| s.to_string()),
            strike: ext.get("strike_price").and_then(|v| v.as_str()).and_then(|s| Decimal::from_str_exact(s).ok()),
            option_right: ext.get("direction").and_then(|v| v.as_str()).map(|d| {
                if d == "C" { qqq_common::OptionRight::Call } else { qqq_common::OptionRight::Put }
            }),
        },
        last_price: last_done, last_quantity: Default::default(), last_side: TradeSide::Buy,
        source_trade_id: format!("lb-{}", quote["timestamp"].as_i64().unwrap_or(0)),
        updated_at: Utc::now(),
        donchian_high: None, donchian_low: None, adx: None,
        volume_avg_20: None, sma_20: None, bb_upper: None, bb_mid: None, bb_lower: None,
        vix_level: None, kline_count: 0,
    };
    let redis_key = format!("state:{}", key);
    let body = json_bytes(&state)?;
    redis.set::<_, _, ()>(&redis_key, String::from_utf8(body.clone())?).await?;
    nats.publish(subjects::state(&state.instrument), body.into()).await?;
    Ok(())
}

async fn publish_state(
    nats: &async_nats::Client, redis: &mut redis::aio::MultiplexedConnection,
    cache: &std::sync::Arc<std::sync::Mutex<IndicatorCache>>, qqq_price: Decimal,
) {
    let state = {
        let c = cache.lock().unwrap();
        let donchian = c.donchian(DONCHIAN_PERIOD);
        let bb = c.bollinger(BB_PERIOD, 2.0);
        RealtimeState {
            instrument: Instrument {
                asset_class: AssetClass::Option, symbol: "QQQ".to_string(),
                venue: None, base: None, quote: None, expiry: None, strike: None, option_right: None,
            },
            last_price: qqq_price, last_quantity: Default::default(), last_side: TradeSide::Buy,
            source_trade_id: String::new(), updated_at: Utc::now(),
            donchian_high: donchian.map(|(h,_)| Decimal::from_f64_retain(h).unwrap_or_default()),
            donchian_low: donchian.map(|(_,l)| Decimal::from_f64_retain(l).unwrap_or_default()),
            adx: c.adx(ADX_PERIOD).map(|a| Decimal::from_f64_retain(a).unwrap_or_default()),
            volume_avg_20: c.volume_avg(DONCHIAN_PERIOD).map(|v| Decimal::from_f64_retain(v).unwrap_or_default()),
            sma_20: c.sma(DONCHIAN_PERIOD).map(|s| Decimal::from_f64_retain(s).unwrap_or_default()),
            bb_upper: bb.map(|(u,_,_)| Decimal::from_f64_retain(u).unwrap_or_default()),
            bb_mid: bb.map(|(_,m,_)| Decimal::from_f64_retain(m).unwrap_or_default()),
            bb_lower: bb.map(|(_,_,l)| Decimal::from_f64_retain(l).unwrap_or_default()),
            vix_level: c.vix.map(|v| Decimal::from_f64_retain(v).unwrap_or_default()),
            kline_count: c.kline_count(),
        }
    };
    let body = match json_bytes(&state) { Ok(b) => b, Err(_) => return };
    let body_str = match String::from_utf8(body.clone()) { Ok(s) => s, Err(_) => return };
    let _ = redis.set::<_, _, ()>("state:qqq", &body_str).await;
    let _ = nats.publish(subjects::state(&state.instrument), body.into()).await;
}

async fn publish_init(
    nats: &async_nats::Client,
    cache: &std::sync::Arc<std::sync::Mutex<IndicatorCache>>,
) {
    let init = {
        let mut c = cache.lock().unwrap();
        let atr_val = c.atr(14);
        let hv_val = c.historical_volatility(20);
        let trend = c.trend(20);
        let recent_bars: Vec<&KlineBar> = c.bars.iter().rev().take(20).collect();
        let high_20d = recent_bars.iter().map(|bar| bar.high).fold(f64::MIN, f64::max);
        let low_20d = recent_bars.iter().map(|bar| bar.low).fold(f64::MAX, f64::min);
        let avg_close_20d = recent_bars.iter().map(|bar| bar.close).sum::<f64>() / recent_bars.len() as f64;
        c.mark_init_published();

        PremarketInit {
            instrument: Instrument {
                asset_class: AssetClass::Option,
                symbol: "QQQ".to_string(),
                venue: None,
                base: None,
                quote: None,
                expiry: None,
                strike: None,
                option_right: Some(OptionRight::Call),
            },
            historical_volatility: Decimal::from_f64_retain(hv_val.unwrap_or(0.25)).unwrap_or_default(),
            atr: Decimal::from_f64_retain(atr_val.unwrap_or(0.4)).unwrap_or_default(),
            high_20d: Decimal::from_f64_retain(high_20d).unwrap_or_default(),
            low_20d: Decimal::from_f64_retain(low_20d).unwrap_or_default(),
            avg_close_20d: Decimal::from_f64_retain(avg_close_20d).unwrap_or_default(),
            trend_slope: Decimal::from_f64_retain(trend.map(|(s, _)| s).unwrap_or(0.0)).unwrap_or_default(),
            trend_score: Decimal::from_f64_retain(trend.map(|(_, r)| r).unwrap_or(0.5)).unwrap_or_default(),
            generated_at: Utc::now(),
        }
    };
    let subject = subjects::premkt_init(&init.instrument);
    if let Err(e) = nats.publish(subject.clone(), serde_json::to_vec(&init).unwrap().into()).await {
        error!(error = %e, "发布 init 失败");
    } else {
        info!(subject = %subject, atr = %init.atr, hv = %init.historical_volatility, slope = %init.trend_slope, r2 = %init.trend_score, "📡 盘前初始化已发布");
    }
}
