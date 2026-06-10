use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub mod greeks;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AssetClass {
    Crypto,
    Equity,
    Option,
    Future,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Venue {
    Coinbase,
    Binance,
    Nasdaq,
    Cboe,
    Paper,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OptionRight {
    Call,
    Put,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Instrument {
    pub asset_class: AssetClass,
    pub symbol: String,
    pub venue: Option<Venue>,
    pub base: Option<String>,
    pub quote: Option<String>,
    pub expiry: Option<String>,
    pub strike: Option<Decimal>,
    pub option_right: Option<OptionRight>,
}

impl Instrument {
    pub fn crypto(symbol: impl Into<String>, venue: Venue, base: &str, quote: &str) -> Self {
        Self {
            asset_class: AssetClass::Crypto,
            symbol: symbol.into(),
            venue: Some(venue),
            base: Some(base.to_string()),
            quote: Some(quote.to_string()),
            expiry: None,
            strike: None,
            option_right: None,
        }
    }

    pub fn subject_key(&self) -> String {
        self.symbol
            .chars()
            .filter(|value| value.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_lowercase()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MarketTrade {
    pub source: String,
    pub instrument: Instrument,
    pub trade_id: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub side: TradeSide,
    pub event_time: DateTime<Utc>,
    pub trade_time: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RealtimeState {
    pub instrument: Instrument,
    pub last_price: Decimal,
    pub last_quantity: Decimal,
    pub last_side: TradeSide,
    pub source_trade_id: String,
    pub updated_at: DateTime<Utc>,

    // ── 技术指标（K 线缓存够 20 根后有值）──
    /// Donchian Channel 上限（20 周期最高价）
    pub donchian_high: Option<Decimal>,
    /// Donchian Channel 下限（20 周期最低价）
    pub donchian_low: Option<Decimal>,
    /// ADX（14 周期平均趋向指数）
    pub adx: Option<Decimal>,
    /// 20 周期均量
    pub volume_avg_20: Option<Decimal>,
    /// SMA 20
    pub sma_20: Option<Decimal>,
    /// 布林带上轨
    pub bb_upper: Option<Decimal>,
    /// 布林带中轨
    pub bb_mid: Option<Decimal>,
    /// 布林带下轨
    pub bb_lower: Option<Decimal>,
    /// VIX 恐慌指数
    pub vix_level: Option<Decimal>,
    /// K 线缓存数量
    pub kline_count: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SignalAction {
    Buy,
    Sell,
    Hold,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StrategySignal {
    pub signal_id: String,
    pub strategy_id: String,
    pub instrument: Instrument,
    pub action: SignalAction,
    pub confidence: Decimal,
    pub reference_price: Decimal,
    pub reason: String,
    pub created_at: DateTime<Utc>,
    /// 信用价差保护腿（ThetaHarvest 等卖策略自动配对）
    #[serde(default)]
    pub spread_wing: Option<Instrument>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskDecision {
    Approved,
    Rejected,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RiskReport {
    pub signal_id: String,
    pub decision: RiskDecision,
    pub reason: String,
    pub checked_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OrderIntent {
    pub intent_id: String,
    pub source_signal_id: String,
    pub instrument: Instrument,
    pub side: OrderSide,
    pub quantity: Decimal,
    pub order_type: OrderType,
    pub limit_price: Option<Decimal>,
    pub reference_price: Decimal,
    pub created_at: DateTime<Utc>,
    /// 信用价差保护腿（与主腿同时下单）
    #[serde(default)]
    pub spread_wing: Option<Instrument>,
    /// 平仓原因（开仓为 None，平仓时非空）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Accepted,
    Rejected,
    Filled,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OrderAck {
    pub order_id: String,
    pub intent_id: String,
    pub status: OrderStatus,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FillEvent {
    pub order_id: String,
    pub instrument: Instrument,
    pub side: OrderSide,
    pub quantity: Decimal,
    pub price: Decimal,
    pub filled_at: DateTime<Utc>,
}

// ── Longbridge 原始行情（Python Gateway → NATS）───────

/// 单条期权扩展数据（来自 Longbridge option_extend）
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OptionQuoteExt {
    /// 隐含波动率
    pub implied_volatility: Decimal,
    /// 未平仓量
    pub open_interest: i64,
    /// 到期日 YYYYMMDD
    pub expiry_date: String,
    /// 行权价
    pub strike_price: Decimal,
    /// 合约乘数（通常 100）
    pub contract_multiplier: String,
    /// 合约类型 A=美式 U=欧式
    pub contract_type: String,
    /// 合约规模
    pub contract_size: String,
    /// C=Call P=Put
    pub direction: String,
    /// 历史波动率
    pub historical_volatility: Decimal,
    /// 正股 symbol
    pub underlying_symbol: String,
}

/// 原始期权行情（Python Longbridge Gateway 发布到 NATS quote.*）
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RawOptionQuote {
    /// 标的 symbol，如 QQQ260601C493000.US
    pub symbol: String,
    /// 最新价
    pub last_done: Decimal,
    /// 昨收价
    pub prev_close: Option<Decimal>,
    /// 开盘价
    pub open: Decimal,
    /// 最高价
    pub high: Decimal,
    /// 最低价
    pub low: Decimal,
    /// 最新成交时间戳
    pub timestamp: i64,
    /// 成交量
    pub volume: i64,
    /// 成交额
    pub turnover: Decimal,
    /// 期权扩展数据（仅期权有，正股为 None）
    pub option_extend: Option<OptionQuoteExt>,
}

// ── 市场状态 ──────────────────────────────────────────

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketEvent {
    Open,
    Close,
    Halted,
    Resumed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MarketStatus {
    pub instrument: Instrument,
    pub event: MarketEvent,
    pub event_time: DateTime<Utc>,
}

// ── 盘前初始化 ────────────────────────────────────────

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PremarketInit {
    pub instrument: Instrument,
    /// 历史波动率 (20日，年化)
    pub historical_volatility: Decimal,
    /// 平均真实波幅 ATR
    pub atr: Decimal,
    /// 20日最高价
    pub high_20d: Decimal,
    /// 20日最低价
    pub low_20d: Decimal,
    /// 20日均价
    pub avg_close_20d: Decimal,
    /// 趋势斜率（20日线性回归，正=上升趋势）
    pub trend_slope: Decimal,
    /// 震荡 vs 趋势评分 (0~1，越接近1越趋势)
    pub trend_score: Decimal,
    /// 生成时间
    pub generated_at: DateTime<Utc>,
}

pub mod subjects {
    use super::{AssetClass, Instrument};

    pub fn asset_key(asset_class: &AssetClass) -> &'static str {
        match asset_class {
            AssetClass::Crypto => "crypto",
            AssetClass::Equity => "equity",
            AssetClass::Option => "option",
            AssetClass::Future => "future",
        }
    }

    pub fn market_trade(instrument: &Instrument) -> String {
        format!(
            "market.{}.{}.trade",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }

    pub fn state(instrument: &Instrument) -> String {
        format!(
            "state.{}.{}",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }

    pub fn signal(instrument: &Instrument) -> String {
        format!(
            "signal.{}.{}",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }

    pub fn risk(instrument: &Instrument) -> String {
        format!(
            "risk.{}.{}",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }

    pub fn order_intent(instrument: &Instrument) -> String {
        format!(
            "order.intent.{}.{}",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }

    pub fn order_ack(instrument: &Instrument) -> String {
        format!(
            "order.ack.{}.{}",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }

    pub fn fill(instrument: &Instrument) -> String {
        format!(
            "fill.{}.{}",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }

    /// Greeks 快照 subject: greeks.option.qqq 等
    pub fn greeks(instrument: &Instrument) -> String {
        format!(
            "greeks.{}.{}",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }

    /// 市场状态 subject: market.option.qqq.status
    pub fn market_status(instrument: &Instrument) -> String {
        format!(
            "market.{}.{}.status",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }

    /// 盘前初始化 subject: init.option.qqq
    pub fn premkt_init(instrument: &Instrument) -> String {
        format!(
            "init.{}.{}",
            asset_key(&instrument.asset_class),
            instrument.subject_key()
        )
    }
}

pub fn json_bytes<T: Serialize>(value: &T) -> serde_json::Result<Vec<u8>> {
    serde_json::to_vec(value)
}
