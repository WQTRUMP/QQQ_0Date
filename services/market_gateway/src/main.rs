use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use qqq_common::{Instrument, MarketTrade, TradeSide, Venue};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

#[derive(Debug, Parser)]
#[command(name = "market_gateway")]
#[command(about = "实时行情网关，当前支持 BTC 成交订阅")]
struct Args {
    #[arg(long, env = "MARKET_SOURCE", default_value = "coinbase")]
    market_source: MarketSource,

    #[arg(long, env = "MARKET_WS_URL")]
    market_ws_url: Option<String>,

    #[arg(long, env = "MARKET_CONNECT_TIMEOUT_SECS", default_value_t = 10)]
    connect_timeout_secs: u64,

    #[arg(long, env = "NATS_URL")]
    nats_url: Option<String>,

    #[arg(
        long,
        env = "MARKET_SUBJECT",
        default_value = "market.crypto.btcusd.trade"
    )]
    market_subject: String,
}

#[derive(Clone, Debug, ValueEnum)]
enum MarketSource {
    Coinbase,
    Binance,
}

#[derive(Debug, Deserialize)]
struct BinanceTrade {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "t")]
    trade_id: i64,
    #[serde(rename = "p")]
    price: Decimal,
    #[serde(rename = "q")]
    quantity: Decimal,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

#[derive(Debug, Deserialize)]
struct CoinbaseMatch {
    #[serde(rename = "type")]
    message_type: String,
    trade_id: i64,
    product_id: String,
    price: Decimal,
    size: Decimal,
    side: CoinbaseSide,
    time: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CoinbaseSide {
    Buy,
    Sell,
}

impl TryFrom<BinanceTrade> for MarketTrade {
    type Error = anyhow::Error;

    fn try_from(value: BinanceTrade) -> Result<Self> {
        if value.event_type != "trade" {
            anyhow::bail!("不支持的 Binance 事件类型: {}", value.event_type);
        }

        let event_time = Utc
            .timestamp_millis_opt(value.event_time_ms)
            .single()
            .context("Binance event time 无效")?;
        let trade_time = Utc
            .timestamp_millis_opt(value.trade_time_ms)
            .single()
            .context("Binance trade time 无效")?;

        Ok(Self {
            source: "BINANCE".to_string(),
            instrument: Instrument::crypto(value.symbol, Venue::Binance, "BTC", "USDT"),
            trade_id: value.trade_id.to_string(),
            price: value.price,
            quantity: value.quantity,
            // buyer_is_maker=true 表示主动成交方是卖方。
            side: if value.buyer_is_maker {
                TradeSide::Sell
            } else {
                TradeSide::Buy
            },
            event_time,
            trade_time,
        })
    }
}

impl TryFrom<CoinbaseMatch> for MarketTrade {
    type Error = anyhow::Error;

    fn try_from(value: CoinbaseMatch) -> Result<Self> {
        if value.message_type != "match" {
            anyhow::bail!("不支持的 Coinbase 消息类型: {}", value.message_type);
        }

        Ok(Self {
            source: "COINBASE".to_string(),
            instrument: Instrument::crypto(value.product_id, Venue::Coinbase, "BTC", "USD"),
            trade_id: value.trade_id.to_string(),
            price: value.price,
            quantity: value.size,
            side: match value.side {
                CoinbaseSide::Buy => TradeSide::Buy,
                CoinbaseSide::Sell => TradeSide::Sell,
            },
            event_time: value.time,
            trade_time: value.time,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let nats_client = connect_nats(args.nats_url.as_deref()).await?;
    let market_ws_url = resolve_market_ws_url(&args);

    info!(url = %market_ws_url, source = ?args.market_source, "连接行情 WebSocket");
    let (ws_stream, _) = timeout(
        Duration::from_secs(args.connect_timeout_secs),
        connect_async(&market_ws_url),
    )
    .await
    .context("连接行情 WebSocket 超时")?
    .context("连接行情 WebSocket 失败")?;
    let (mut write, mut read) = ws_stream.split();

    if matches!(args.market_source, MarketSource::Coinbase) {
        let subscribe_message = serde_json::json!({
            "type": "subscribe",
            "product_ids": ["BTC-USD"],
            "channels": ["matches"]
        });
        write
            .send(Message::Text(subscribe_message.to_string()))
            .await
            .context("发送 Coinbase 订阅消息失败")?;
    }

    info!(
        subject = %args.market_subject,
        nats_enabled = nats_client.is_some(),
        "BTC 成交订阅已启动"
    );

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("收到退出信号，行情订阅停止");
                break;
            }
            message = read.next() => {
                let Some(message) = message else {
                    warn!("行情 WebSocket 已关闭");
                    break;
                };

                if let Err(err) = handle_message(
                    message?,
                    &args.market_source,
                    nats_client.as_ref(),
                    &args.market_subject,
                ).await {
                    error!(error = %err, "处理行情消息失败");
                }
            }
        }
    }

    Ok(())
}

fn resolve_market_ws_url(args: &Args) -> String {
    if let Some(url) = &args.market_ws_url {
        return url.clone();
    }

    match args.market_source {
        MarketSource::Coinbase => "wss://ws-feed.exchange.coinbase.com".to_string(),
        MarketSource::Binance => "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string(),
    }
}

async fn connect_nats(nats_url: Option<&str>) -> Result<Option<async_nats::Client>> {
    let Some(nats_url) = nats_url else {
        info!("未配置 NATS_URL，行情将输出到 stdout");
        return Ok(None);
    };

    let client = async_nats::connect(nats_url)
        .await
        .with_context(|| format!("连接 NATS 失败: {nats_url}"))?;
    info!(url = %nats_url, "NATS 已连接");
    Ok(Some(client))
}

async fn handle_message(
    message: Message,
    market_source: &MarketSource,
    nats_client: Option<&async_nats::Client>,
    subject: &str,
) -> Result<()> {
    match message {
        Message::Text(payload) => {
            publish_trade(&payload, market_source, nats_client, subject).await
        }
        Message::Ping(_) | Message::Pong(_) => Ok(()),
        Message::Close(frame) => {
            warn!(?frame, "收到 WebSocket close frame");
            Ok(())
        }
        other => {
            warn!(?other, "忽略非文本 WebSocket 消息");
            Ok(())
        }
    }
}

async fn publish_trade(
    payload: &str,
    market_source: &MarketSource,
    nats_client: Option<&async_nats::Client>,
    subject: &str,
) -> Result<()> {
    let market_trade = match market_source {
        MarketSource::Coinbase => {
            let message_type = serde_json::from_str::<serde_json::Value>(payload)
                .ok()
                .and_then(|value| {
                    value
                        .get("type")
                        .and_then(|message_type| message_type.as_str().map(str::to_string))
                });
            if message_type.as_deref() != Some("match") {
                return Ok(());
            }

            let coinbase_match: CoinbaseMatch =
                serde_json::from_str(payload).context("解析 Coinbase 成交消息失败")?;
            MarketTrade::try_from(coinbase_match)?
        }
        MarketSource::Binance => {
            let binance_trade: BinanceTrade =
                serde_json::from_str(payload).context("解析 Binance 成交消息失败")?;
            MarketTrade::try_from(binance_trade)?
        }
    };
    let body = serde_json::to_vec(&market_trade).context("序列化内部行情消息失败")?;

    if let Some(client) = nats_client {
        client
            .publish(subject.to_string(), body.into())
            .await
            .with_context(|| format!("发布 NATS 行情失败: {subject}"))?;
    } else {
        println!(
            "{}",
            String::from_utf8(body).context("行情 JSON UTF-8 转换失败")?
        );
    }

    Ok(())
}
