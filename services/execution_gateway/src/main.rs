use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use futures_util::StreamExt;
use qqq_common::{json_bytes, subjects, FillEvent, OrderAck, OrderIntent, OrderStatus};
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(name = "execution_gateway")]
#[command(about = "执行网关：当前仅支持 paper execution")]
struct Args {
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    #[arg(
        long,
        env = "ORDER_INTENT_SUBJECT",
        default_value = "order.intent.option.>"
    )]
    order_intent_subject: String,
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
    let mut subscriber = nats
        .subscribe(args.order_intent_subject.clone())
        .await
        .with_context(|| format!("订阅订单意图失败: {}", args.order_intent_subject))?;

    info!(subject = %args.order_intent_subject, "模拟执行网关已启动");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("收到退出信号，执行网关停止");
                break;
            }
            message = subscriber.next() => {
                let Some(message) = message else {
                    break;
                };

                if let Err(err) = handle_intent(&nats, &message.payload).await {
                    error!(error = %err, "处理订单意图失败");
                }
            }
        }
    }

    Ok(())
}

async fn handle_intent(nats: &async_nats::Client, payload: &[u8]) -> Result<()> {
    let intent: OrderIntent = serde_json::from_slice(payload).context("解析 OrderIntent 失败")?;
    let order_id = format!("paper-{}", Utc::now().timestamp_millis());
    let ack = OrderAck {
        order_id: order_id.clone(),
        intent_id: intent.intent_id.clone(),
        status: OrderStatus::Accepted,
        reason: "paper execution accepted".to_string(),
        created_at: Utc::now(),
    };

    let ack_subject = subjects::order_ack(&intent.instrument);
    nats.publish(
        ack_subject.clone(),
        json_bytes(&ack).context("序列化 OrderAck 失败")?.into(),
    )
    .await
    .with_context(|| format!("发布订单确认失败: {ack_subject}"))?;

    let primary_order_id = if intent.spread_wing.is_some() {
        format!("{order_id}-L0")
    } else {
        order_id.clone()
    };

    publish_fill(
        nats,
        &intent.instrument,
        FillEvent {
            order_id: primary_order_id.clone(),
            instrument: intent.instrument.clone(),
            side: intent.side.clone(),
            quantity: intent.quantity,
            price: intent.limit_price.unwrap_or(intent.reference_price),
            source_signal_id: intent.source_signal_id.clone(),
            is_exit: intent.exit_reason.is_some(),
            total_legs: Some(if intent.spread_wing.is_some() { 2 } else { 1 }),
            leg: Some(0),
            filled_at: Utc::now(),
        },
    )
    .await?;

    if let Some(wing) = intent.spread_wing.clone() {
        let wing_side = match intent.side {
            qqq_common::OrderSide::Buy => qqq_common::OrderSide::Sell,
            qqq_common::OrderSide::Sell => qqq_common::OrderSide::Buy,
        };
        publish_fill(
            nats,
            &wing,
            FillEvent {
                order_id: format!("{order_id}-L1"),
                instrument: wing,
                side: wing_side,
                quantity: intent.quantity,
                price: intent.limit_price.unwrap_or(intent.reference_price),
                source_signal_id: intent.source_signal_id.clone(),
                is_exit: intent.exit_reason.is_some(),
                total_legs: Some(2),
                leg: Some(1),
                filled_at: Utc::now(),
            },
        )
        .await?;
    }

    info!(intent_id = %intent.intent_id, "paper 订单已确认并模拟成交");
    Ok(())
}

async fn publish_fill(
    nats: &async_nats::Client,
    instrument: &qqq_common::Instrument,
    fill: FillEvent,
) -> Result<()> {
    let fill_subject = subjects::fill(instrument);
    nats.publish(
        fill_subject.clone(),
        json_bytes(&fill).context("序列化 FillEvent 失败")?.into(),
    )
    .await
    .with_context(|| format!("发布成交事件失败: {fill_subject}"))?;
    Ok(())
}
