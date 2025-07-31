use std::thread;
use std::sync::mpsc::{self as std_mpsc, Receiver as StdReceiver, Sender as StdSender};

use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

pub struct ExchangeManager {
    ws_endpoint_url: String
}

impl ExchangeManager {
    fn new() -> Self {

        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                Self::fetch_and_stream_loop().await;
            })
        });

        Self {
            ws_endpoint_url,

        }
    }

    async fn fetch_and_stream_loop(
        &self,
        exchange_data_model: ExchangeDataModel,
        tx: &StdSender<AppMessage>,
        ctx: &egui::Context,
        mut control_rx: Receiver<Control>,
        mut symbol: String,
    ) {
        loop {
            let (mut ws_stream, response) = match connect_async(self.ws_endpoint_url).await {
                Ok(pair) => pair,
                Err(e) => {
                    println!("Error connecting {} WebSocket: {e}", self.ws_endpoint_url);
                    return;
                }};
            
            let tx_clone = tx.clone();
            let ctx_clone = ctx.clone();
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

    fn process_update(&mut self, update: CustomEnum) {
        match update {
            CustomEnum::DepthUpdate => {self.handle_depth_update(update)},
            CustomEnum::TradeUpdate => {self.handle_trade_update(update)}
        }
    }

    fn handle_depth_update(&mut self, update: DepthUpdate) {

    }

    fn handle_trade_update(&mut self, update: TradeUpdate) {

    }
}