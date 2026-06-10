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
        default_value = "order.intent.crypto.btcusd"
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
    let fill = FillEvent {
        order_id,
        instrument: intent.instrument.clone(),
        side: intent.side,
        quantity: intent.quantity,
        price: intent.limit_price.unwrap_or(intent.reference_price),
        filled_at: Utc::now(),
    };

    let ack_subject = subjects::order_ack(&intent.instrument);
    nats.publish(
        ack_subject.clone(),
        json_bytes(&ack).context("序列化 OrderAck 失败")?.into(),
    )
    .await
    .with_context(|| format!("发布订单确认失败: {ack_subject}"))?;

    let fill_subject = subjects::fill(&intent.instrument);
    nats.publish(
        fill_subject.clone(),
        json_bytes(&fill).context("序列化 FillEvent 失败")?.into(),
    )
    .await
    .with_context(|| format!("发布成交事件失败: {fill_subject}"))?;

    info!(intent_id = %intent.intent_id, "paper 订单已确认并模拟成交");
    Ok(())
}
