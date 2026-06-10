//! Greeks Engine v2
//!
//! 订阅 quote.option.* 原始期权行情（含 Longbridge 真实 IV），
//! 收集全链数据，用 Black-Scholes 批量计算 Greeks，
//! 通过 NATS greeks.* 发布 GreeksSnapshot。
//!
//! 策略：
//!   - 缓存每条腿的最新行情（symbol → RawOptionQuote）
//!   - 缓存 QQQ 正股价
//!   - QQQ 价格变化时 → 全链重算 Greeks → 发布

use anyhow::{Context, Result};
use chrono::{Utc, NaiveTime, Timelike};
use clap::Parser;
use futures_util::StreamExt;
use qqq_common::{
    greeks::{self, GreeksRow, GreeksSnapshot},
    json_bytes, subjects, Instrument, OptionRight, RawOptionQuote,
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use tokio::time::{interval, Duration};
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(name = "greeks_engine")]
#[command(about = "Greeks 引擎 v2：订阅 quote.*，用真实 IV 算 Greeks，发布 greeks.*")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    #[arg(long, env = "QUOTE_SUBJECT_PREFIX", default_value = "quote.option")]
    quote_subject_prefix: String,

    /// 距到期剩余小时数（用于 T 计算）
    #[arg(long, env = "HOURS_TO_EXPIRY", default_value = "6.5")]
    hours_to_expiry: f64,

    /// 无风险利率
    #[arg(long, env = "RISK_FREE_RATE", default_value = "0.05")]
    risk_free_rate: Decimal,

    /// QQQ 价格变化阈值（仅变化超过此值才重算，单位美元）
    #[arg(long, env = "PRICE_CHANGE_THRESHOLD", default_value = "0.01")]
    price_change_threshold: Decimal,

    /// 期权收盘时间 UTC（格式 HH:MM），用于 T 计算
    #[arg(long, env = "MARKET_CLOSE_UTC", default_value = "20:00")]
    market_close_utc: String,
}

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

    let subject = format!("{}.>", args.quote_subject_prefix);
    let mut subscriber = nats
        .subscribe(subject.clone())
        .await
        .with_context(|| format!("订阅失败: {subject}"))?;

    info!(
        subject = %subject,
        hours_to_expiry = args.hours_to_expiry,
        market_close_utc = %args.market_close_utc,
        "Greeks 引擎 v2 已启动（T 动态计算）"
    );

    // 状态缓存
    let mut underlying_price = Decimal::ZERO;
    let mut last_published_price = Decimal::ZERO;
    let mut option_quotes: HashMap<String, RawOptionQuote> = HashMap::new();

    // 解析收盘时间
    let close_parts: Vec<&str> = args.market_close_utc.split(':').collect();
    let close_hour: u32 = close_parts.first().and_then(|s| s.parse().ok()).unwrap_or(20);
    let close_min: u32 = close_parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let close_time = NaiveTime::from_hms_opt(close_hour, close_min, 0)
        .unwrap_or_else(|| NaiveTime::from_hms_opt(20, 0, 0).unwrap());

    // 每 5 秒触发一次被动重算，确保 Greeks 在 QQQ 平稳时也不陈旧
    let mut ticker = interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("收到退出信号，Greeks 引擎停止");
                break;
            }
            _ = ticker.tick() => {
                // 被动触发重算：如果已收集到期权数据且 QQQ 价格有效，则重算
                if option_quotes.is_empty() || underlying_price <= Decimal::ZERO {
                    continue;
                }
                let t = compute_t_from_now(close_time);
                let snapshot = build_greeks_snapshot(
                    underlying_price,
                    &option_quotes,
                    t,
                    args.risk_free_rate,
                );

                let subject = subjects::greeks(&Instrument {
                    asset_class: qqq_common::AssetClass::Option,
                    symbol: "QQQ".to_string(),
                    venue: None,
                    base: None,
                    quote: None,
                    expiry: None,
                    strike: None,
                    option_right: None,
                });

                if let Err(err) = publish_snapshot(&nats, &subject, &snapshot).await {
                    error!(error = %err, subject = %subject, "被动重算发布 Greeks 失败");
                } else {
                    info!(
                        underlying = %underlying_price,
                        rows = snapshot.rows.len(),
                        gamma_flip = ?snapshot.gamma_flip,
                        "Greeks 快照已发布 (被动重算)"
                    );
                }
            }
            message = subscriber.next() => {
                let Some(message) = message else {
                    break;
                };

                match handle_quote(
                    &nats,
                    &args,
                    &message.payload,
                    &mut underlying_price,
                    &mut last_published_price,
                    &mut option_quotes,
                    &message.subject,
                    close_time,
                ).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!(error = %err, subject = %message.subject, "处理行情失败");
                    }
                }
            }
        }
    }

    Ok(())
}

async fn handle_quote(
    nats: &async_nats::Client,
    args: &Args,
    payload: &[u8],
    underlying_price: &mut Decimal,
    last_published_price: &mut Decimal,
    option_quotes: &mut HashMap<String, RawOptionQuote>,
    nats_subject: &str,
    close_time: chrono::NaiveTime,
) -> Result<()> {
    let quote: RawOptionQuote =
        serde_json::from_slice(payload).context("解析 RawOptionQuote 失败")?;

    // 识别 QQQ 正股 vs 期权（Go Gateway 发布 subject 为 quote.option.qqqus）
    let is_underlying = nats_subject.ends_with(".qqqus") || quote.symbol == "QQQ.US";

    if is_underlying {
        // QQQ 正股价格更新
        *underlying_price = quote.last_done;

        // 检查是否需要重算（价格变化超过阈值）
        let change = (*underlying_price - *last_published_price).abs();
        if change < args.price_change_threshold && *last_published_price > Decimal::ZERO {
            return Ok(());
        }

        // 只有在收集到足够期权数据后才发布
        if option_quotes.is_empty() {
            return Ok(());
        }

        // 动态计算 T：距收盘的剩余年数
        let t = compute_t_from_now(close_time);

        // 全链重算 Greeks
        let snapshot = build_greeks_snapshot(
            *underlying_price,
            option_quotes,
            t,
            args.risk_free_rate,
        );

        *last_published_price = *underlying_price;

        // 清理窗口外的过期期权缓存（保留现价 ±$10）
        let px_f64: f64 = (*underlying_price).try_into().unwrap_or(0.0);
        option_quotes.retain(|_key, q| {
            let ext = match &q.option_extend {
                Some(e) => e,
                None => return true, // QQQ 正股条目会被后面的 loop 跳过
            };
            let strike_f64: f64 = ext.strike_price.try_into().unwrap_or(0.0);
            strike_f64 <= 0.0 || (strike_f64 - px_f64).abs() <= 10.0
        });
        info!(
            option_cached = option_quotes.len(),
            underlying = %underlying_price,
            "期权缓存清理完成"
        );

        let subject = subjects::greeks(&Instrument {
            asset_class: qqq_common::AssetClass::Option,
            symbol: "QQQ".to_string(),
            venue: None,
            base: None,
            quote: None,
            expiry: None,
            strike: None,
            option_right: None,
        });

        publish_snapshot(nats, &subject, &snapshot).await?;

        info!(
            underlying = %underlying_price,
            rows = snapshot.rows.len(),
            gamma_flip = ?snapshot.gamma_flip,
            "Greeks 快照已发布"
        );
    } else {
        // 期权行情 → 缓存
        let key = quote
            .symbol
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_lowercase();
        option_quotes.insert(key, quote);
    }

    Ok(())
}

/// 序列化并发布 GreeksSnapshot 到 NATS
async fn publish_snapshot(
    nats: &async_nats::Client,
    subject: &str,
    snapshot: &GreeksSnapshot,
) -> Result<()> {
    let body = json_bytes(snapshot).context("序列化 GreeksSnapshot 失败")?;
    nats.publish(subject.to_string(), body.into())
        .await
        .with_context(|| format!("发布 Greeks 失败: {subject}"))?;
    Ok(())
}

/// 动态计算距收盘的剩余时间 → T（年）
/// `close` 为 UTC 收盘时间（如 20:00 = EDT/EST 16:00）
fn compute_t_from_now(close: NaiveTime) -> Decimal {
    let now = Utc::now().naive_utc();
    let now_secs = now.num_seconds_from_midnight() as i64;
    let close_secs = close.num_seconds_from_midnight() as i64;
    let remaining_secs = (close_secs - now_secs).max(60); // 至少 1 分钟
    let hours = remaining_secs as f64 / 3600.0;
    Decimal::from_str_exact(&format!("{:.8}", hours / (365.25 * 24.0))).unwrap_or(Decimal::ZERO)
}

fn build_greeks_snapshot(
    underlying_price: Decimal,
    option_quotes: &HashMap<String, RawOptionQuote>,
    t: Decimal,
    risk_free_rate: Decimal,
) -> GreeksSnapshot {
    let mut rows = Vec::with_capacity(option_quotes.len());

    for (_key, quote) in option_quotes {
        let ext = match &quote.option_extend {
            Some(e) => e,
            None => continue, // QQQ 正股跳过
        };

        let strike = ext.strike_price;
        let iv = ext.implied_volatility;

        if strike <= Decimal::ZERO || iv <= Decimal::ZERO {
            continue;
        }

        let right = match ext.direction.as_str() {
            "C" => OptionRight::Call,
            _ => OptionRight::Put,
        };

        let g = greeks::compute_greeks(
            underlying_price,
            strike,
            t,
            risk_free_rate,
            iv,
            &right,
        );

        rows.push(GreeksRow {
            strike,
            option_right: right,
            iv,
            delta: g.delta,
            gamma: g.gamma,
            theta: g.theta,
            vega: g.vega,
            rho: g.rho,
            theoretical_price: g.price,
        });
    }

    // gamma_flip：做市商 Gamma 翻转点的代理估值
    // 精确计算需要全链 OI 加权求和，但 OI 数据不可用。
    // 代理：设于现价下方 $1，当 QQQ 跌破此位时触发 Market Regime 的 GammaFlip 换挡
    // （做市商从 long gamma 转 short gamma → 波动放大 → GammaScalp 提权）
    let flip_strike = underlying_price - Decimal::ONE;
    let approx_strike = underlying_price.round_dp(0);

    GreeksSnapshot {
        instrument: Instrument {
            asset_class: qqq_common::AssetClass::Option,
            symbol: "QQQ".to_string(),
            venue: None,
            base: None,
            quote: None,
            expiry: None,
            strike: None,
            option_right: None,
        },
        underlying_price,
        risk_free_rate,
        hours_to_expiry: {
            let hours_per_year = Decimal::from(365) * Decimal::from(24) + Decimal::from(6); // 365.25*24 ≈ 8766
            let hours = t * hours_per_year;
            hours.to_string().parse::<f64>().unwrap_or(0.0)
        },
        rows,
        generated_at: Utc::now(),
        gamma_flip: Some(flip_strike),
        max_pain: Some(approx_strike),
    }
}
