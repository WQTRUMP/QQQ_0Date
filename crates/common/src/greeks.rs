//! Black-Scholes Greeks 引擎
//!
//! 为 0DTE QQQ 期权提供：
//! - 期权理论价格 (BS 定价)
//! - Δ (Delta): 价格敏感度
//! - Γ (Gamma): Delta 变化率，到期日最关键指标
//! - Θ (Theta): 时间衰减，0DTE 核心交易因子
//! - V (Vega): 波动率敏感度
//! - IV (隐含波动率): Newton‑Raphson 反推

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use statrs::distribution::{Continuous, ContinuousCDF, Normal};
use std::sync::OnceLock;

use crate::{Instrument, OptionRight};

// ── 常量 ──────────────────────────────────────────────

/// 标准正态分布（懒初始化）
fn normal() -> &'static Normal {
    static NORMAL: OnceLock<Normal> = OnceLock::new();
    NORMAL.get_or_init(|| Normal::new(0.0, 1.0).unwrap())
}

/// 一年小时数，用于 T → 0 时的精确计算
const HOURS_PER_YEAR: f64 = 365.0 * 24.0;

// ── 辅助函数 ──────────────────────────────────────────

/// Decimal → f64，高精度计算用 f64，结果再转回 Decimal
fn d_to_f64(d: Decimal) -> f64 {
    d.to_string().parse::<f64>().unwrap_or(0.0)
}

fn f64_to_d(f: f64) -> Decimal {
    Decimal::from_str_exact(&format!("{:.12}", f)).unwrap_or(Decimal::ZERO)
}

/// 标准正态 CDF
fn normal_cdf(x: f64) -> f64 {
    normal().cdf(x)
}

/// 标准正态 PDF
fn normal_pdf(x: f64) -> f64 {
    normal().pdf(x)
}

// ── Black‑Scholes 定价 ────────────────────────────────

/// 计算 European option 理论价格
///
/// # 参数
/// * `s` - 底层资产当前价格
/// * `k` - 行权价
/// * `t` - 距到期时间（年），0DTE 时可能极小（如 1/8760 = 1 小时）
/// * `r` - 无风险利率
/// * `sigma` - 隐含波动率（如 0.30 = 30%）
/// * `right` - CALL 或 PUT
pub fn black_scholes_price(
    s: Decimal,
    k: Decimal,
    t: Decimal,
    r: Decimal,
    sigma: Decimal,
    right: &OptionRight,
) -> Decimal {
    let s_f = d_to_f64(s);
    let k_f = d_to_f64(k);
    let t_f = d_to_f64(t);
    let r_f = d_to_f64(r);
    let sigma_f = d_to_f64(sigma);

    // 到期时：内在价值
    if t_f <= 0.0 || sigma_f <= 0.0 {
        return match right {
            OptionRight::Call => {
                if s > k {
                    s - k
                } else {
                    Decimal::ZERO
                }
            }
            OptionRight::Put => {
                if k > s {
                    k - s
                } else {
                    Decimal::ZERO
                }
            }
        };
    }

    let sqrt_t = t_f.sqrt();
    let d1 = ((s_f / k_f).ln() + (r_f + sigma_f * sigma_f / 2.0) * t_f) / (sigma_f * sqrt_t);
    let d2 = d1 - sigma_f * sqrt_t;

    let price = match right {
        OptionRight::Call => s_f * normal_cdf(d1) - k_f * (-r_f * t_f).exp() * normal_cdf(d2),
        OptionRight::Put => k_f * (-r_f * t_f).exp() * normal_cdf(-d2) - s_f * normal_cdf(-d1),
    };

    f64_to_d(price.max(0.0))
}

// ── Greeks 计算 ────────────────────────────────────────

/// Greeks 计算结果
#[derive(Clone, Debug)]
pub struct Greeks {
    pub price: Decimal,
    pub delta: Decimal,
    pub gamma: Decimal,
    pub theta: Decimal, // 每日 theta（非年化）
    pub vega: Decimal,  // 1% 波动率变化对应的价格变化
    pub rho: Decimal,   // 1% 利率变化对应的价格变化
    pub d1: f64,
    pub d2: f64,
}

/// 计算所有 Greeks
///
/// Theta 返回的是**每日**衰减量（除以 365），
/// Vega 返回的是 1% 波率变化（除以 100），
/// 方便 0DTE 场景直接阅读。
pub fn compute_greeks(
    s: Decimal,
    k: Decimal,
    t: Decimal,
    r: Decimal,
    sigma: Decimal,
    right: &OptionRight,
) -> Greeks {
    let s_f = d_to_f64(s);
    let k_f = d_to_f64(k);
    let t_f = d_to_f64(t);
    let r_f = d_to_f64(r);
    let sigma_f = d_to_f64(sigma);

    // 到期 / 无波动率 → 返回零 Greeks
    if t_f <= 0.0 || sigma_f <= 0.0 {
        return Greeks {
            price: Decimal::ZERO,
            delta: Decimal::ZERO,
            gamma: Decimal::ZERO,
            theta: Decimal::ZERO,
            vega: Decimal::ZERO,
            rho: Decimal::ZERO,
            d1: 0.0,
            d2: 0.0,
        };
    }

    let sqrt_t = t_f.sqrt();
    let d1 = ((s_f / k_f).ln() + (r_f + sigma_f * sigma_f / 2.0) * t_f) / (sigma_f * sqrt_t);
    let d2 = d1 - sigma_f * sqrt_t;

    let pdf_d1 = normal_pdf(d1);
    let discount = (-r_f * t_f).exp();

    let price = match right {
        OptionRight::Call => s_f * normal_cdf(d1) - k_f * discount * normal_cdf(d2),
        OptionRight::Put => k_f * discount * normal_cdf(-d2) - s_f * normal_cdf(-d1),
    };

    let delta = match right {
        OptionRight::Call => normal_cdf(d1),
        OptionRight::Put => normal_cdf(d1) - 1.0,
    };

    let gamma = pdf_d1 / (s_f * sigma_f * sqrt_t);

    // 每日 theta（BS 年化 theta / 365）
    let theta_year = match right {
        OptionRight::Call => {
            -s_f * pdf_d1 * sigma_f / (2.0 * sqrt_t)
                - r_f * k_f * discount * normal_cdf(d2)
                + r_f * s_f * normal_cdf(d1)
        }
        OptionRight::Put => {
            -s_f * pdf_d1 * sigma_f / (2.0 * sqrt_t)
                + r_f * k_f * discount * normal_cdf(-d2)
                - r_f * s_f * normal_cdf(-d1)
        }
    };
    let theta_daily = theta_year / 365.0;

    // Vega：sigma 变化 1% (0.01) 对应的价格变化
    let vega_year = s_f * pdf_d1 * sqrt_t;
    let vega = vega_year / 100.0;

    // Rho：利率变化 1% (0.01) 对应的价格变化
    let rho = match right {
        OptionRight::Call => k_f * t_f * discount * normal_cdf(d2) / 100.0,
        OptionRight::Put => -k_f * t_f * discount * normal_cdf(-d2) / 100.0,
    };

    Greeks {
        price: f64_to_d(price.max(0.0)),
        delta: f64_to_d(delta),
        gamma: f64_to_d(gamma),
        theta: f64_to_d(theta_daily),
        vega: f64_to_d(vega),
        rho: f64_to_d(rho),
        d1,
        d2,
    }
}

// ── 隐含波动率 (Newton‑Raphson) ────────────────────────

const IV_MAX_ITER: usize = 100;
const IV_TOLERANCE: f64 = 1e-8;
const IV_INITIAL_GUESS: f64 = 0.30;

/// 从市场价格反推隐含波动率
pub fn implied_volatility(
    price: Decimal,
    s: Decimal,
    k: Decimal,
    t: Decimal,
    r: Decimal,
    right: &OptionRight,
) -> Option<Decimal> {
    let price_f = d_to_f64(price);
    let s_f = d_to_f64(s);
    let k_f = d_to_f64(k);
    let t_f = d_to_f64(t);
    let r_f = d_to_f64(r);

    if price_f <= 0.0 || t_f <= 0.0 {
        return None;
    }

    let intrinsic = match right {
        OptionRight::Call => (s_f - k_f).max(0.0),
        OptionRight::Put => (k_f - s_f).max(0.0),
    };

    if price_f <= intrinsic {
        return Some(Decimal::ZERO);
    }

    let mut sigma = IV_INITIAL_GUESS;
    for _ in 0..IV_MAX_ITER {
        let sqrt_t = t_f.sqrt();
        let d1 = ((s_f / k_f).ln() + (r_f + sigma * sigma / 2.0) * t_f) / (sigma * sqrt_t);
        let d2 = d1 - sigma * sqrt_t;

        let model_price = match right {
            OptionRight::Call => {
                s_f * normal_cdf(d1) - k_f * (-r_f * t_f).exp() * normal_cdf(d2)
            }
            OptionRight::Put => {
                k_f * (-r_f * t_f).exp() * normal_cdf(-d2) - s_f * normal_cdf(-d1)
            }
        };

        let vega = s_f * normal_pdf(d1) * sqrt_t;

        if vega.abs() < 1e-12 {
            break;
        }

        let diff = model_price - price_f;
        if diff.abs() < IV_TOLERANCE {
            return Some(f64_to_d(sigma));
        }

        sigma -= diff / vega;
        if sigma <= 0.001 {
            sigma = 0.001;
        }
        if sigma > 5.0 {
            sigma = 5.0;
        }
    }

    Some(f64_to_d(sigma))
}

// ── 便捷函数：从小时计算 T ─────────────────────────────

/// 将剩余小时数转为年化时间 T
pub fn hours_to_t(hours: f64) -> Decimal {
    f64_to_d(hours / HOURS_PER_YEAR)
}

// ── 消息模型 ──────────────────────────────────────────

/// 单条期权 Greeks
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GreeksRow {
    pub strike: Decimal,
    #[serde(rename = "option_right")]
    pub option_right: OptionRight,
    /// 隐含波动率
    pub iv: Decimal,
    pub delta: Decimal,
    pub gamma: Decimal,
    /// 每日 theta
    pub theta: Decimal,
    /// 1% Vega
    pub vega: Decimal,
    /// 1% Rho（0DTE 极小，但仍提供）
    pub rho: Decimal,
    /// 理论价格
    pub theoretical_price: Decimal,
}

/// 整条期权链的 Greeks 快照，通过 NATS `greeks.*` 发布
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GreeksSnapshot {
    /// 底层品种信息
    pub instrument: Instrument,
    /// 底层当前价格
    pub underlying_price: Decimal,
    /// 计算时使用的无风险利率
    pub risk_free_rate: Decimal,
    /// 距到期剩余小时数
    pub hours_to_expiry: f64,
    /// 每条 leg 的 Greeks
    pub rows: Vec<GreeksRow>,
    /// 生成时间
    pub generated_at: DateTime<Utc>,
    /// 做市商 Gamma 翻转点（行权价，近似值）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gamma_flip: Option<Decimal>,
    /// 最大痛点（行权价，近似值）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_pain: Option<Decimal>,
}

// ── 组合 Greeks 聚合 ──────────────────────────────────

/// 对整个 option chain 或持仓组合做 Greeks 加权求和
pub fn aggregate_greeks(rows: &[GreeksRow], quantities: &[i32]) -> Greeks {
    let mut total = Greeks {
        price: Decimal::ZERO,
        delta: Decimal::ZERO,
        gamma: Decimal::ZERO,
        theta: Decimal::ZERO,
        vega: Decimal::ZERO,
        rho: Decimal::ZERO,
        d1: 0.0,
        d2: 0.0,
    };

    for (row, &qty) in rows.iter().zip(quantities.iter()) {
        let q = Decimal::from(qty);
        // 数量为负代表空头，Greeks 反向
        total.price += row.theoretical_price * q;
        total.delta += row.delta * q;
        total.gamma += row.gamma * q;
        total.theta += row.theta * q;
        total.vega += row.vega * q;
        total.rho += row.rho * q;
    }

    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atm_call_1day() {
        // ATM Call, 1 天到期, 30% IV
        let g = compute_greeks(
            Decimal::new(10000, 2), // 100.00
            Decimal::new(10000, 2), // 100.00
            hours_to_t(6.5),        // 6.5小时
            Decimal::new(5, 2),     // 5% rate
            Decimal::new(30, 2),    // 30% IV
            &OptionRight::Call,
        );

        // ATM call delta ≈ 0.5
        assert!(g.delta > Decimal::new(45, 2) && g.delta < Decimal::new(55, 2));

        // Gamma 应该存在且为正
        assert!(g.gamma > Decimal::ZERO);

        // Theta 应该为负（时间衰减）
        assert!(g.theta < Decimal::ZERO);

        // 价格应该在合理范围
        assert!(g.price > Decimal::ZERO);
    }

    #[test]
    fn test_deep_itm_put() {
        // Deep ITM Put
        let g = compute_greeks(
            Decimal::new(10000, 2),
            Decimal::new(12000, 2), // strike 120, spot 100
            hours_to_t(1.0),
            Decimal::new(5, 2),
            Decimal::new(30, 2),
            &OptionRight::Put,
        );

        // Delta 接近 -1
        assert!(g.delta < Decimal::new(-80, 2));
        // Gamma 趋于 0
        assert!(g.gamma < Decimal::new(10, 2));
    }

    #[test]
    fn test_implied_volatility_roundtrip() {
        let s = Decimal::new(49285, 2); // 492.85
        let k = Decimal::new(49300, 2); // 493.00
        let t = hours_to_t(3.0);
        let r = Decimal::new(5, 2);
        let sigma = Decimal::new(28, 2); // 28%

        let price = black_scholes_price(s, k, t, r, sigma, &OptionRight::Call);
        let iv = implied_volatility(price, s, k, t, r, &OptionRight::Call).unwrap();

        let diff = (iv - sigma).abs();
        assert!(diff < Decimal::new(1, 2)); // 误差 < 0.01
    }
}
