//! 盘前初始化服务
//!
//! 从日 K 数据源读取最近 20 个交易日数据，
//! 计算历史波动率 (HV)、ATR、支撑/阻力位、趋势评分，
//! 通过 NATS `init.*` 发布供策略引擎在开盘前加载。

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use qqq_common::{json_bytes, subjects, Instrument, OptionRight, PremarketInit};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Parser)]
#[command(name = "premarket_init")]
#[command(about = "盘前初始化：20日日K → HV/ATR/S/R → NATS init.*")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    /// 日K JSON 文件路径，格式：[{"date":"...","open":...,"high":...,"low":...,"close":...,"volume":...}]
    #[arg(long, env = "DAILY_BARS_FILE")]
    daily_bars_file: String,

    /// 计算用的天数
    #[arg(long, env = "LOOKBACK_DAYS", default_value = "20")]
    lookback_days: usize,
}

#[derive(Debug, Deserialize)]
struct DailyBar {
    #[allow(dead_code)]
    date: Option<String>,
    #[allow(dead_code)]
    open: Option<Decimal>,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    #[allow(dead_code)]
    volume: Option<Decimal>,
}

/// 每日收益率 → 年化历史波动率
fn compute_hv(closes: &[Decimal]) -> Decimal {
    if closes.len() < 2 {
        return Decimal::ZERO;
    }

    let returns: Vec<f64> = closes
        .windows(2)
        .map(|w| {
            let prev = w[0].to_string().parse::<f64>().unwrap_or(0.0);
            let curr = w[1].to_string().parse::<f64>().unwrap_or(0.0);
            (curr / prev).ln()
        })
        .collect();

    let n = returns.len() as f64;
    let mean = returns.iter().sum::<f64>() / n;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let daily_sigma = variance.sqrt();

    // 年化 (交易日 ≈ 252)
    let hv = daily_sigma * (252.0_f64).sqrt();
    Decimal::from_str_exact(&format!("{:.6}", hv)).unwrap_or(Decimal::ZERO)
}

fn compute_trend_slope(closes: &[Decimal]) -> Decimal {
    if closes.len() < 2 {
        return Decimal::ZERO;
    }

    let n = closes.len() as f64;
    let x_mean = (n - 1.0) / 2.0;

    let closes_f64: Vec<f64> = closes
        .iter()
        .map(|c| c.to_string().parse::<f64>().unwrap_or(0.0))
        .collect();
    let y_mean = closes_f64.iter().sum::<f64>() / n;

    let mut num = 0.0_f64;
    let mut den = 0.0_f64;
    for (i, &y) in closes_f64.iter().enumerate() {
        let x = i as f64;
        num += (x - x_mean) * (y - y_mean);
        den += (x - x_mean).powi(2);
    }

    let slope = if den.abs() < 1e-12 {
        0.0
    } else {
        num / den
    };

    Decimal::from_str_exact(&format!("{:.8}", slope)).unwrap_or(Decimal::ZERO)
}

/// 趋势评分：用 R² 衡量线性拟合好坏，越接近 1 趋势越明显
fn compute_trend_score(closes: &[Decimal]) -> Decimal {
    if closes.len() < 3 {
        return Decimal::from(5) / Decimal::from(10); // 0.5
    }

    let closes_f64: Vec<f64> = closes
        .iter()
        .map(|c| c.to_string().parse::<f64>().unwrap_or(0.0))
        .collect();
    let n = closes_f64.len() as f64;
    let y_mean = closes_f64.iter().sum::<f64>() / n;
    let x_mean = (n - 1.0) / 2.0;

    let mut num = 0.0_f64;
    let mut den_x = 0.0_f64;
    for (i, &y) in closes_f64.iter().enumerate() {
        let x = i as f64;
        num += (x - x_mean) * (y - y_mean);
        den_x += (x - x_mean).powi(2);
    }

    let slope = if den_x.abs() < 1e-12 {
        0.0
    } else {
        num / den_x
    };
    let intercept = y_mean - slope * x_mean;

    // R² = 1 - SS_res / SS_tot
    let ss_tot: f64 = closes_f64.iter().map(|&y| (y - y_mean).powi(2)).sum();
    let ss_res: f64 = closes_f64
        .iter()
        .enumerate()
        .map(|(i, &y)| {
            let y_pred = slope * i as f64 + intercept;
            (y - y_pred).powi(2)
        })
        .sum();

    let r2 = if ss_tot.abs() < 1e-12 {
        0.5
    } else {
        (1.0 - ss_res / ss_tot).max(0.0)
    };

    Decimal::from_str_exact(&format!("{:.4}", r2)).unwrap_or(Decimal::from(5) / Decimal::from(10))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    // 读取日K数据
    let raw = std::fs::read_to_string(&args.daily_bars_file)
        .with_context(|| format!("读取日K文件失败: {}", args.daily_bars_file))?;
    let all_bars: Vec<DailyBar> =
        serde_json::from_str(&raw).context("解析日K JSON 失败")?;

    // 取最近 N 天
    let start = all_bars.len().saturating_sub(args.lookback_days);
    let bars = &all_bars[start..];

    if bars.len() < 2 {
        anyhow::bail!("日K数据不足，至少需要 2 条（实际 {} 条）", bars.len());
    }

    let closes: Vec<Decimal> = bars.iter().map(|b| b.close).collect();

    let hv = compute_hv(&closes);
    // 日线 ATR 不发布——日内策略使用 RealtimeEngine 的分钟级 ATR
    let trend_slope = compute_trend_slope(&closes);
    let trend_score = compute_trend_score(&closes);
    let high_20d = bars.iter().map(|b| b.high).max().unwrap_or(Decimal::ZERO);
    let low_20d = bars.iter().map(|b| b.low).min().unwrap_or(Decimal::ZERO);
    let sum_close: Decimal = closes.iter().sum();
    let avg_close_20d = sum_close / Decimal::from(closes.len());

    let init = PremarketInit {
        instrument: Instrument {
            asset_class: qqq_common::AssetClass::Option,
            symbol: "QQQ".to_string(),
            venue: None,
            base: None,
            quote: None,
            expiry: None,
            strike: None,
            option_right: Some(OptionRight::Call),
        },
        historical_volatility: hv,
        atr: Decimal::ZERO,  // 0 = 让 RealtimeEngine 的分钟 ATR 覆盖（日线 ATR ≠ 日内 ATR）
        high_20d,
        low_20d,
        avg_close_20d,
        trend_slope,
        trend_score,
        generated_at: Utc::now(),
    };

    info!(
        hv_pct = %(hv * Decimal::from(100)),
        high_20d = %high_20d,
        low_20d = %low_20d,
        trend_score = %trend_score,
        "盘前初始化计算完成 (ATR留给RealtimeEngine分钟K线覆盖)"
    );

    // 发布到 NATS
    let nats = async_nats::connect(&args.nats_url)
        .await
        .with_context(|| format!("连接 NATS 失败: {}", args.nats_url))?;

    let subject = subjects::premkt_init(&init.instrument);
    let body = json_bytes(&init).context("序列化 PremarketInit 失败")?;
    nats.publish(subject.clone(), body.into())
        .await
        .with_context(|| format!("发布盘前初始化失败: {subject}"))?;

    info!(%subject, "盘前初始化已发布");
    Ok(())
}
