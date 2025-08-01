use rust_decimal::Decimal;
use rust_decimal::prelude::*;

use serde::Deserialize;

use std::collections::HashMap;

pub enum SubscriptionEnum {
    Binance(BinanceSubcription),
    Hyperliquid(HyperliquidSubscription),
    OxFun(OxFunSubscription)
}

pub struct BinanceSubcription {
    method: String,
    params: Vec<String>
}

pub struct OxFunSubscription {
    op: String,
    args: Vec<String>
}

pub struct HyperliquidSubscription {
    method: String,
    subscription: HashMap<String, String>
}

pub enum MetricUpdate {
    TradeUpdate(TradeMetrics),
    BookUpdate(OrderbookMetrics)
}

#[derive(Deserialize, Clone)]
pub struct TradeUpdate {
    pub e: String,
    //can actually add multiple serde(rename)s to a struct to have multiple fields convert into a single struct.
    //can use this to standardize struct creation across exchanges
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

pub struct BestBidAsk {
    best_bid_price: Decimal,
    best_bid_qty: Decimal,
    best_ask_price: Decimal,
    best_offer_qty: Decimal
}

//should rename these properties to be full names. use serde(rename)s to work around exchange variations
#[allow(dead_code)]
#[derive(Deserialize, Clone)]
pub struct DepthUpdate {
    pub e: String,
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "T")]
    pub transaction_time: u64,
    pub s: String,
    #[serde(rename = "U")]
    pub capital_u: u64,
    #[serde(rename = "u")]
    pub small_u: u64,
    pub pu: i64,
    pub b: Vec<Vec<Decimal>>,
    pub a: Vec<Vec<Decimal>>,
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