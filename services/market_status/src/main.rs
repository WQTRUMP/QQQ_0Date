//! 市场状态服务
//!
//! 负责：
//! 1. 启动时发送 `market.*.status` OPEN 事件
//! 2. 定时（到期小时数后）发送 `market.*.status` CLOSE 事件
//!
//! 策略引擎订阅后执行 reset() 清理跨日状态。

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use qqq_common::{
    json_bytes, subjects, Instrument, MarketEvent, MarketStatus, OptionRight,
};
use tracing::info;

#[derive(Debug, Parser)]
#[command(name = "market_status")]
#[command(about = "市场状态通知：开盘/收盘信号")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    /// 收盘前剩余小时数，到期后自动发 CLOSE
    #[arg(long, env = "HOURS_TO_CLOSE", default_value = "6.5")]
    hours_to_close: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let nats = async_nats::connect(&args.nats_url)
        .await
        .with_context(|| format!("连接 NATS 失败: {}", args.nats_url))?;

    // 构造 QQQ Instrument（与策略引擎一致）
    let instrument = Instrument {
        asset_class: qqq_common::AssetClass::Option,
        symbol: "QQQ".to_string(),
        venue: None,
        base: None,
        quote: None,
        expiry: None,
        strike: None,
        option_right: Some(OptionRight::Call),
    };
    let subject = subjects::market_status(&instrument);

    // 发送开盘信号
    let open = MarketStatus {
        instrument: instrument.clone(),
        event: MarketEvent::Open,
        event_time: Utc::now(),
    };
    nats.publish(subject.clone(), json_bytes(&open)?.into())
        .await
        .with_context(|| format!("发布开盘信号失败: {subject}"))?;
    info!(?subject, "📈 开盘信号已发送");

    // 定时发送收盘信号
    let close_secs = (args.hours_to_close * 3600.0) as u64;
    let instrument_for_close = instrument.clone();
    let nats_for_close = nats.clone();
    let subject_for_close = subject.clone();

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(close_secs)).await;

        let close = MarketStatus {
            instrument: instrument_for_close,
            event: MarketEvent::Close,
            event_time: Utc::now(),
        };
        if let Err(e) = nats_for_close
            .publish(
                subject_for_close.clone(),
                json_bytes(&close).unwrap().into(),
            )
            .await
        {
            tracing::error!(error = %e, "发布收盘信号失败");
        } else {
            info!("📉 收盘信号已发送");
        }
    });

    info!(hours_to_close = args.hours_to_close, "收盘信号将在 {args_hours_to_close}h 后发送", args_hours_to_close = args.hours_to_close);

    // 保持运行直到收盘信号发完
    tokio::signal::ctrl_c().await.ok();
    Ok(())
}
