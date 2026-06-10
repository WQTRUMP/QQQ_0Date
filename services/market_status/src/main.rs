//! 市场状态服务
//!
//! 基于统一会话时钟发布 `market.*.status`。
//! 启动时按当前真实会话发出一次状态快照，后续仅在边界切换时发布。

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use qqq_common::{
    json_bytes, session_clock, subjects, AssetClass, Instrument, MarketEvent, MarketStatus,
};
use tracing::info;

#[derive(Debug, Parser)]
#[command(name = "market_status")]
#[command(about = "市场状态通知：基于真实会话时钟发布开盘/收盘")]
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

    let instrument = Instrument {
        asset_class: AssetClass::Option,
        symbol: "QQQ".to_string(),
        venue: None,
        base: Some("QQQ".to_string()),
        quote: None,
        expiry: None,
        strike: None,
        option_right: None,
    };
    let subject = subjects::market_status(&instrument);

    let mut last_sent: Option<(MarketEvent, String)> = None;

    loop {
        let snapshot = session_clock::current_session(Utc::now());
        let marker = (snapshot.state.clone(), snapshot.session_id.clone());
        if last_sent.as_ref() != Some(&marker) {
            let status = MarketStatus {
                instrument: instrument.clone(),
                event: snapshot.state.clone(),
                session_id: Some(snapshot.session_id.clone()),
                event_time: Utc::now(),
            };
            nats.publish(subject.clone(), json_bytes(&status)?.into())
                .await
                .with_context(|| format!("发布市场状态失败: {subject}"))?;
            info!(event = ?snapshot.state, session_id = %snapshot.session_id, next_transition_at = %snapshot.next_transition_at, "📡 市场状态已发布");
            last_sent = Some(marker);
        }

        let sleep_for = (snapshot.next_transition_at - Utc::now())
            .to_std()
            .unwrap_or_else(|_| std::time::Duration::from_secs(1));
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("收到退出信号，市场状态服务停止");
                break;
            }
            _ = tokio::time::sleep(sleep_for) => {}
        }
    }

    Ok(())
}
