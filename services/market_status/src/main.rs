//! 市场状态服务
//!
//! 负责：
//! 1. 启动时发送 `market.*.status` OPEN 事件
//! 2. 定时（到期小时数后）发送 `market.*.status` CLOSE 事件
//!
//! 策略引擎订阅后执行 reset() 清理跨日状态。

use anyhow::{Context, Result};
use clap::Parser;
use qqq_common::{
    json_bytes,
    session_clock::{self, SessionPhase},
    subjects, Instrument, MarketEvent, MarketStatus, OptionRight,
};
use tracing::info;

#[derive(Debug, Parser)]
#[command(name = "market_status")]
#[command(about = "市场状态通知：开盘/收盘信号")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

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

    publish_status(&nats, &subject, &instrument, session_clock::current_session(chrono::Utc::now()).phase).await?;

    loop {
        let now = chrono::Utc::now();
        let (transition_at, next_phase, session_id) = session_clock::next_transition_after(now);
        let sleep_for = (transition_at - now)
            .to_std()
            .unwrap_or_else(|_| std::time::Duration::from_secs(1));

        info!(
            subject = %subject,
            next_phase = ?next_phase,
            session_id = %session_id,
            sleep_secs = sleep_for.as_secs(),
            "市场状态已对齐交易时钟，等待下一次边界"
        );

        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = tokio::time::sleep(sleep_for) => {
                publish_status(&nats, &subject, &instrument, next_phase).await?;
            }
        }
    }

    Ok(())
}

async fn publish_status(
    nats: &async_nats::Client,
    subject: &str,
    instrument: &Instrument,
    phase: SessionPhase,
) -> Result<()> {
    let snapshot = session_clock::current_session(chrono::Utc::now());
    let event = match phase {
        SessionPhase::Open => MarketEvent::Open,
        SessionPhase::Closed => MarketEvent::Close,
    };
    let status = MarketStatus {
        instrument: instrument.clone(),
        event,
        event_time: chrono::Utc::now(),
        session_id: Some(snapshot.session_id),
    };
    nats.publish(subject.to_string(), json_bytes(&status)?.into())
        .await
        .with_context(|| format!("发布市场状态失败: {subject}"))?;
    info!(subject = %subject, event = ?status.event, session_id = ?status.session_id, "市场状态已发布");
    Ok(())
}
