use rust_decimal::Decimal;
use rust_decimal::prelude::*;

#[derive(Deserialize, Clone)]
pub struct TradeUpdate {
    pub e: String,
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub trade_id: u64,
    pub p: Decimal,
    pub q: Decimal,
    #[serde(rename = "T")]
    pub trade_time: u64,
    #[serde(rename = "m")]
    pub buyer_market_maker: bool
}

pub struct TradeMetrics {
    pub imbalance: Decimal, // shouldn't this be over some period?
    pub lambda_five_micros: Decimal,
    pub lambda_one_milli: Decimal,
    pub lambda_one_second: Decimal,
    pub lambda_thirty_seconds: Decimal,
    pub lambda_one_minute: Decimal,
}

impl Default for TradeMetrics {
    fn default() -> Self {
        TradeMetrics {
            imbalance: Decimal::ZERO,
            lambda_five_micros: Decimal::ZERO,
            lambda_one_milli: Decimal::ZERO,
            lambda_one_second: Decimal::ZERO,
            lambda_thirty_seconds: Decimal::ZERO,
            lambda_one_minute: Decimal::ZERO
        }
    }
}

pub struct OrderbookMetrics {
    pub mid_price: Decimal,
    pub spread: Decimal,
    pub order_arrival_rate: Decimal,
    pub imbalance: Decimal,
    pub bid_vwap: Decimal,
    pub ask_vwap: Decimal,
}

impl Default for OrderbookMetrics {
    fn default() -> Self {
        OrderbookMetrics {
            mid_price: Decimal::ZERO,
            spread: Decimal::ZERO,
            order_arrival_rate: Decimal::ZERO,
            imbalance: Decimal::ZERO,
            bid_vwap: Decimal::ZERO,
            ask_vwap: Decimal::ZERO,
        }
    }
}