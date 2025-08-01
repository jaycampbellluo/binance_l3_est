use std::sync::mpsc::{self as std_mpsc, Receiver as StdReceiver, Sender as StdSender};
use std::thread;

use rust_decimal::Decimal;

use crate::model::*;

pub struct Strategy {
    // will be stores for metrics,
    // this is likely to be in a ringbuffer of metric updates, which would allow us to compute
    // EWMAs etc consistently
    book_metrics: OrderbookMetrics, // this should actually be a bus of 
    trade_metrics: TradeMetrics
}

impl Strategy {
    pub fn new(rx: StdReceiver<MetricUpdate>) -> Self {
        let book_metrics = OrderbookMetrics::default();
        let trade_metrics = TradeMetrics::default();

        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                self.fetch_and_stream_loop(&rx).await;
            });
        });

        Strategy {
            book_metrics,
            trade_metrics
        }
    }

    async fn fetch_and_stream_loop(&mut self, rx: StdReceiver<MetricUpdate>) {
        loop {
            while let Some(incoming) = rx.recv().await {
                match incoming {
                    MetricUpdate::BookUpdate(update) => {
                        self.book_metrics = update;
                    },
                    MetricUpdate::TradeUpdate(update) => {
                        self.trade_metrics = update;
                    },
                    _ => {} 
                };
                Self::compute_and_decide();
            };
        }
    }

    async fn compute_and_decide() {
        
    }

}