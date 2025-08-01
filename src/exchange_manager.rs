use std::thread;
use std::sync::mpsc::{self as std_mpsc, Receiver as StdReceiver, Sender as StdSender};

use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

use crate::model::*;
use crate::glass::*;

pub struct ExchangeManager {
    orderbook: Glass,
    trade_ring: String, // to be some custom ringbuffer
    tx: StdSender<MetricUpdate>
}

impl ExchangeManager {
    fn new() -> Self {
        let book = Glass::new();
        let trade_ring = String::from("wasd"); // to be some custom ringbuffer init
        let (tx, rx) = std_mpsc::channel();

        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                Self::fetch_and_stream_loop(&tx).await;
            })
        });

        Self {
            orderbook: book,

        }
    }

    async fn fetch_and_stream_loop(
        &self,
        tx: &StdSender<ExchangeUpdate>,
        mut control_rx: Receiver<Control>,
        mut symbol: String,
    ) {
        loop {
            let (mut ws_stream, response) = match connect_async(self.ws_endpoint_url).await {
                Ok(pair) => pair,
                Err(e) => {
                    println!("Error connecting {} WebSocket: {e}", self.ws_endpoint_url);
                    return;
                }
            };
            
            let tx_clone = tx.clone();
            let ws_handle = tokio::spawn(async move {
                while let Some(result) = ws_stream.next().await {
                    match result {
                        Ok(message) => match message {
                            WsMessage::Text => {
                                match serde_json::from_str::<>(&message) {
                                    Ok(json) => match json {

                                    } 
                                }
                            },
                            WsMessage::Ping(payload) => {ws_stream.send(WsMessage::Pong(payload)).await},
                            WsMessage::Pong(_) => {},
                            WsMessage::Close(_) => {
                                println!("Connection closed by server");
                                break;
                            },
                            _ => {}
                        }
                        Err(e) => {
                            println!("WebSocket message error: {e:?}");
                            break;
                        }
                    }
                }
            });
        }
    }

    async fn add_subscription(&self, subscription: SubscriptionEnum) {
        self.
    }

    fn process_update(&mut self, update: ExchangeUpdate) {
        match update {
            ExchangeUpdate::DepthUpdate => {self.handle_depth_update(update)},
            ExchangeUpdate::TradeUpdate => {self.handle_trade_update(update)}
        }
    }

    fn handle_depth_update(&mut self, update: DepthUpdate) {
        //send to strategy thread
        //log
        if update.
    }

    fn handle_trade_update(&mut self, update: TradeUpdate) {
        //send to strategy thtread
        //log
    }
}