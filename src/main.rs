mod kmeans;

use eframe::egui;
use egui::{Align2, Color32};
use egui_plot::{Bar, BarChart, Plot, PlotPoint, Text};
use futures_util::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::Deserialize;
use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::sync::mpsc::{self as std_mpsc, Receiver as StdReceiver, Sender as StdSender};
use std::thread;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

#[derive(Deserialize)]
struct OrderBookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<Vec<Decimal>>,
    asks: Vec<Vec<Decimal>>,
}

#[derive(Deserialize, Clone)]
struct DepthUpdate {
    e: String,
    #[serde(rename = "E")]
    event_time: u64,
    #[serde(rename = "T")]
    transaction_time: u64,
    s: String,
    #[serde(rename = "U")]
    capital_u: u64,
    #[serde(rename = "u")]
    small_u: u64,
    pu: i64,
    b: Vec<Vec<Decimal>>,
    a: Vec<Vec<Decimal>>,
}

enum AppMessage {
    Snapshot(OrderBookSnapshot),
    Update(DepthUpdate),
}

static BID_COLORS: Lazy<Vec<Color32>> = Lazy::new(|| {
    vec![
        Color32::from_rgb(222, 235, 247), // Light Blue
        Color32::from_rgb(204, 227, 245), // Lighter Blue
        Color32::from_rgb(158, 202, 225), // Blue
        Color32::from_rgb(129, 189, 231), // Light Medium Blue
        Color32::from_rgb(107, 174, 214), // Medium Blue
        Color32::from_rgb(78, 157, 202),  // Medium Deep Blue
        Color32::from_rgb(49, 130, 189),  // Deep Blue
        Color32::from_rgb(33, 113, 181),  // Darker Deep Blue
        Color32::from_rgb(16, 96, 168),   // Dark Blue
        Color32::from_rgb(8, 81, 156),    // Darkest Blue
    ]
});

static ASK_COLORS: Lazy<Vec<Color32>> = Lazy::new(|| {
    vec![
        Color32::from_rgb(254, 230, 206), // Light Orange
        Color32::from_rgb(253, 216, 186), // Lighter Orange
        Color32::from_rgb(253, 174, 107), // Orange
        Color32::from_rgb(253, 159, 88),  // Light Deep Orange
        Color32::from_rgb(253, 141, 60),  // Deep Orange
        Color32::from_rgb(245, 126, 47),  // Medium Red-Orange
        Color32::from_rgb(230, 85, 13),   // Red-Orange
        Color32::from_rgb(204, 75, 12),   // Darker Red-Orange
        Color32::from_rgb(179, 65, 10),   // Dark Red
        Color32::from_rgb(166, 54, 3),    // Darkest Red
    ]
});

fn main() -> eframe::Result {
    // Fetch the symbol from command-line arguments or default to DOGEUSDT
    let args: Vec<String> = env::args().collect();
    let symbol: String = if args.len() > 1 {
        args[1].to_ascii_lowercase()
    } else {
        "dogeusdt".to_string()
    };

    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Order Book Visualizer",
        options,
        Box::new(move |cc| Ok(Box::new(MyApp::new(cc, symbol)))),
    )
}

struct MyApp {
    symbol: String,
    bids: BTreeMap<Decimal, VecDeque<Decimal>>,
    asks: BTreeMap<Decimal, VecDeque<Decimal>>,
    last_applied_u: u64,
    is_synced: bool,
    rx: StdReceiver<AppMessage>,
    update_buffer: VecDeque<DepthUpdate>,
    refetch_tx: Sender<()>,
    kmeans_mode: bool,
}

impl MyApp {
    fn new(cc: &eframe::CreationContext<'_>, symbol: String) -> Self {
        let (tx, rx) = std_mpsc::channel();
        let (refetch_tx, refetch_rx) = mpsc::channel(1);
        let ctx = cc.egui_ctx.clone();
        let s = symbol.clone();
        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                Self::fetch_and_stream_loop(&tx, &ctx, refetch_rx, s).await;
            });
        });

        Self {
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_applied_u: 0,
            is_synced: false,
            rx,
            update_buffer: VecDeque::new(),
            refetch_tx,
            kmeans_mode: false,
        }
    }

    async fn fetch_and_stream_loop(
        tx: &StdSender<AppMessage>,
        ctx: &egui::Context,
        mut refetch_rx: Receiver<()>,
        symbol: String, // Accept the symbol as a parameter
    ) {
        loop {
            let ws_url_str = format!("wss://fstream.binance.com/ws/{symbol}@depth@0ms"); // Use symbol
            let (mut ws_stream, response) = match connect_async(ws_url_str).await {
                Ok(pair) => pair,
                Err(e) => {
                    println!("WebSocket connection error: {e:?}");
                    return;
                }
            };
            println!("WebSocket connected: {response:?}");

            let tx_clone = tx.clone();
            let ctx_clone = ctx.clone();
            let ws_handle = tokio::spawn(async move {
                while let Some(result) = ws_stream.next().await {
                    match result {
                        Ok(message) => match message {
                            WsMessage::Text(text) => {
                                match serde_json::from_str::<DepthUpdate>(&text) {
                                    Ok(update) => {
                                        tx_clone.send(AppMessage::Update(update)).unwrap();
                                        ctx_clone.request_repaint();
                                    }
                                    Err(e) => println!("Update JSON error: {e:?}"),
                                }
                            }
                            WsMessage::Ping(payload) => {
                                if let Err(e) = ws_stream.send(WsMessage::Pong(payload)).await {
                                    println!("Pong send error: {e:?}");
                                    break;
                                }
                            }
                            WsMessage::Pong(_) => {}
                            WsMessage::Close(_) => {
                                println!("Connection closed by server.");
                                break;
                            }
                            _ => {}
                        },
                        Err(e) => {
                            println!("WebSocket error: {e:?}");
                            break;
                        }
                    }
                }
            });

            let client = reqwest::Client::new();
            let snap_url =
                format!("https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"); // Use symbol
            match client.get(snap_url).send().await {
                Ok(resp) => match resp.json::<OrderBookSnapshot>().await {
                    Ok(snap) => {
                        println!("Snapshot fetched successfully.");
                        tx.send(AppMessage::Snapshot(snap)).unwrap();
                    }
                    Err(e) => println!("Snapshot JSON error: {e:?}"),
                },
                Err(e) => println!("Snapshot request error: {e:?}"),
            }

            if refetch_rx.recv().await.is_some() {
                ws_handle.abort();
                println!("Refetch triggered, restarting connection.");
            } else {
                break;
            }
        }
    }

    fn process_update(&mut self, update: DepthUpdate) {
        if update.small_u < self.last_applied_u {
            return;
        }

        if self.is_synced {
            if (update.pu as u64) != self.last_applied_u {
                println!(
                    "Warning: Message gap detected! pu: {}, last: {}",
                    update.pu, self.last_applied_u
                );
                self.update_buffer.clear();
                let _ = self.refetch_tx.try_send(());
                return;
            }
            self.apply_update(&update);
            self.last_applied_u = update.small_u;
        } else if update.capital_u <= self.last_applied_u && self.last_applied_u <= update.small_u {
            self.apply_update(&update);
            self.last_applied_u = update.small_u;
            self.is_synced = true;
        } else {
            println!(
                "Initial gap detected! U: {}, u: {}, last: {}",
                update.capital_u, update.small_u, self.last_applied_u
            );
            self.update_buffer.clear();
            let _ = self.refetch_tx.try_send(());
        }
    }
}

impl eframe::App for MyApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                AppMessage::Snapshot(snap) => {
                    self.bids.clear();
                    self.asks.clear();
                    for bid in &snap.bids {
                        let price = bid[0];
                        let qty = bid[1];
                        if qty > Decimal::ZERO {
                            self.bids.insert(price, VecDeque::from(vec![qty]));
                        }
                    }
                    for ask in &snap.asks {
                        let price = ask[0];
                        let qty = ask[1];
                        if qty > Decimal::ZERO {
                            self.asks.insert(price, VecDeque::from(vec![qty]));
                        }
                    }
                    self.last_applied_u = snap.last_update_id;
                    self.is_synced = false;

                    while let Some(update) = self.update_buffer.pop_front() {
                        self.process_update(update);
                    }
                }
                AppMessage::Update(update) => {
                    if self.last_applied_u == 0 {
                        self.update_buffer.push_back(update);
                    } else {
                        self.process_update(update);
                    }
                }
            }
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading(format!(
                "{} Perpetual Order Book",
                self.symbol.to_uppercase()
            ));
            if ui.button("Toggle K-Means Mode").clicked() {
                self.kmeans_mode = !self.kmeans_mode;
            }

            ui.horizontal(|ui| {
                ui.vertical(|ui| {
                    egui::Grid::new("order_book_grid")
                        .striped(true)
                        .show(ui, |ui| {
                            ui.label("Asks");
                            ui.label("Price");
                            ui.label("Quantity");
                            ui.end_row();

                            for (price, qty) in self.asks.iter().take(20).rev() {
                                ui.label("");
                                ui.label(format!("{:.5}", price.to_f64().unwrap_or(0.0)));
                                ui.label(format!(
                                    "{:.0}",
                                    qty.iter().sum::<Decimal>().to_f64().unwrap_or(0.0)
                                ));
                                ui.end_row();
                            }

                            ui.label("Bids");
                            ui.label("Price");
                            ui.label("Quantity");
                            ui.end_row();

                            for (price, qty) in self.bids.iter().rev().take(20) {
                                ui.label("");
                                ui.label(format!("{:.5}", price.to_f64().unwrap_or(0.0)));
                                ui.label(format!(
                                    "{:.0}",
                                    qty.iter().sum::<Decimal>().to_f64().unwrap_or(0.0)
                                ));
                                ui.end_row();
                            }
                        });
                });

                ui.vertical(|ui| {
                    let bid_levels: Vec<(&Decimal, Decimal)> = self
                        .bids
                        .iter()
                        .rev()
                        .take(100)
                        .map(|(key, deque)| {
                            let sum = deque.iter().cloned().sum::<Decimal>(); // Sum the VecDeque<Decimal>
                            (key, sum)
                        })
                        .collect();
                    let ask_levels: Vec<(&Decimal, Decimal)> = self
                        .asks
                        .iter()
                        .take(100)
                        .map(|(key, deque)| {
                            let sum = deque.iter().cloned().sum::<Decimal>(); // Sum the VecDeque<Decimal>
                            (key, sum)
                        })
                        .collect();
                    let mut max_qty: f64 = 0.0;
                    for (_, qty) in &bid_levels {
                        max_qty = max_qty.max(qty.to_f64().unwrap_or(0.0));
                    }
                    for (_, qty) in &ask_levels {
                        max_qty = max_qty.max(qty.to_f64().unwrap_or(0.0));
                    }

                    let step = 1.0;
                    let mut bars: Vec<Bar> = Vec::new();

                    let max_bid_order: Decimal = self
                        .bids
                        .values()
                        .rev()
                        .take(100)
                        .flat_map(|dq| dq.iter())
                        .cloned()
                        .max()
                        .unwrap_or(Decimal::ZERO);
                    let max_ask_order: Decimal = self
                        .asks
                        .values()
                        .take(100)
                        .flat_map(|dq| dq.iter())
                        .cloned()
                        .max()
                        .unwrap_or(Decimal::ZERO);
                    let second_max_bid_order = {
                        let mut orders: Vec<_> = self
                            .bids
                            .values()
                            .rev()
                            .take(100)
                            .flat_map(|dq| dq.iter())
                            .cloned()
                            .collect();
                        orders.sort_by(|a, b| b.cmp(a)); // Sort in descending order
                        orders.get(1).cloned().unwrap_or(Decimal::ZERO)
                    };
                    let second_max_ask_order = {
                        let mut orders: Vec<_> = self
                            .asks
                            .values()
                            .take(100)
                            .flat_map(|dq| dq.iter())
                            .cloned()
                            .collect();
                        orders.sort_by(|a, b| b.cmp(a)); // Sort in descending order
                        orders.get(1).cloned().unwrap_or(Decimal::ZERO)
                    };

                    if !self.kmeans_mode {
                        for (i, (_, qty_deq)) in self.asks.iter().take(100).enumerate() {
                            let x = (i as f64 + 0.5) * step + 0.5;
                            let mut offset = 0.0;

                            for (j, &qty) in qty_deq.iter().enumerate() {
                                if qty <= dec!(0.0) {
                                    continue;
                                }
                                let color = if qty == max_ask_order {
                                    Color32::GOLD
                                } else if qty == second_max_ask_order {
                                    Color32::from_rgb(184, 134, 11)
                                } else {
                                    self.get_order_color(j, Color32::DARK_RED)
                                };
                                let bar = Bar::new(x, qty.to_f64().unwrap_or(0.0))
                                    .fill(color)
                                    .base_offset(offset)
                                    .width(step * 0.9);
                                bars.push(bar);
                                offset += qty.to_f64().unwrap_or(0.0);
                            }
                        }

                        // Color Mapping for Bids
                        for (i, (_, qty_deq)) in self.bids.iter().rev().take(100).enumerate() {
                            let x = -(i as f64 + 0.5) * step - 0.5;
                            let mut offset = 0.0;

                            for (j, &qty) in qty_deq.iter().enumerate() {
                                if qty <= dec!(0.0) {
                                    continue;
                                }
                                let color = if qty == max_bid_order {
                                    Color32::GOLD
                                } else if qty == second_max_bid_order {
                                    Color32::from_rgb(184, 134, 11)
                                } else {
                                    self.get_order_color(j, Color32::DARK_GREEN)
                                };
                                let bar = Bar::new(x, qty.to_f64().unwrap_or(0.0))
                                    .fill(color)
                                    .base_offset(offset)
                                    .width(step * 0.9);
                                bars.push(bar);
                                offset += qty.to_f64().unwrap_or(0.0);
                            }
                        }
                    } else {
                        let asks_for_cluster: BTreeMap<Decimal, VecDeque<Decimal>> = self
                            .asks
                            .iter()
                            .take(100)
                            .map(|(&k, v)| (k, v.clone()))
                            .collect();
                        let clustered_asks = kmeans::cluster_order_book(&asks_for_cluster, 10);

                        let bids_for_cluster: BTreeMap<Decimal, VecDeque<Decimal>> = self
                            .bids
                            .iter()
                            .rev()
                            .take(100)
                            .map(|(&k, v)| (k, v.clone()))
                            .collect();
                        let clustered_bids = kmeans::cluster_order_book(&bids_for_cluster, 10);

                        // Asks in K-Means mode
                        for (i, (_, qty_deq)) in clustered_asks.iter().enumerate() {
                            let x = (i as f64 + 0.5) * step + 0.5;
                            let mut offset = 0.0;

                            for &(qty, cluster) in qty_deq.iter() {
                                if qty <= dec!(0.0) {
                                    continue;
                                }
                                let color = if qty == max_ask_order {
                                    Color32::GOLD
                                } else {
                                    ASK_COLORS
                                        .get(cluster % ASK_COLORS.len())
                                        .cloned()
                                        .unwrap_or(Color32::GRAY)
                                };
                                let bar = Bar::new(x, qty.to_f64().unwrap_or(0.0))
                                    .fill(color)
                                    .base_offset(offset)
                                    .width(step * 0.9);
                                bars.push(bar);
                                offset += qty.to_f64().unwrap_or(0.0);
                            }
                        }

                        // Bids in K-Means mode
                        for (i, (_, qty_deq)) in clustered_bids.iter().rev().enumerate() {
                            let x = -(i as f64 + 0.5) * step - 0.5;
                            let mut offset = 0.0;

                            for &(qty, cluster) in qty_deq.iter() {
                                if qty <= dec!(0.0) {
                                    continue;
                                }
                                let color = if qty == max_bid_order {
                                    Color32::GOLD
                                } else {
                                    BID_COLORS
                                        .get(cluster % BID_COLORS.len())
                                        .cloned()
                                        .unwrap_or(Color32::GRAY)
                                };
                                let bar = Bar::new(x, qty.to_f64().unwrap_or(0.0))
                                    .fill(color)
                                    .base_offset(offset)
                                    .width(step * 0.9);
                                bars.push(bar);
                                offset += qty.to_f64().unwrap_or(0.0);
                            }
                        }
                    }

                    Plot::new("orderbook_chart")
                        .allow_drag(false)
                        .allow_scroll(false)
                        .allow_zoom(false)
                        .show_axes([true, true])
                        .show(ui, |plot_ui| {
                            plot_ui.bar_chart(BarChart::new("ob", bars));

                            for (i, (price, _)) in bid_levels.iter().enumerate() {
                                if i.is_multiple_of(20) {
                                    // Show label every 20th level
                                    let x = -(i as f64 + 0.5) * step - 0.5;
                                    plot_ui.text(
                                        Text::new(
                                            "bid",
                                            PlotPoint::new(x, -max_qty * 0.05),
                                            format!("{:.5}", price.to_f64().unwrap_or(0.0)),
                                        )
                                        .anchor(Align2::CENTER_BOTTOM),
                                    );
                                }
                            }

                            for (i, (price, _)) in ask_levels.iter().enumerate() {
                                if i.is_multiple_of(20) {
                                    // Show label every 20th level
                                    if i == 0 {
                                        continue;
                                    }
                                    let x = (i as f64 + 0.5) * step + 0.5;
                                    plot_ui.text(
                                        Text::new(
                                            "ask",
                                            PlotPoint::new(x, -max_qty * 0.05),
                                            format!("{:.5}", price.to_f64().unwrap_or(0.0)),
                                        )
                                        .anchor(Align2::CENTER_BOTTOM),
                                    );
                                }
                            }
                        });
                });
            });
        });
    }
}

impl MyApp {
    // Function to calculate color based on the order index
    fn get_order_color(&self, index: usize, base_color: Color32) -> Color32 {
        // Brighten the color by 5% for each order index
        let brightening_factor = 1.0 + 0.05 * index as f32; // 5% brighter per order
        let r = (base_color.r() as f32 * brightening_factor).min(255.0) as u8;
        let g = (base_color.g() as f32 * brightening_factor).min(255.0) as u8;
        let b = (base_color.b() as f32 * brightening_factor).min(255.0) as u8;

        Color32::from_rgb(r, g, b)
    }
}

impl MyApp {
    fn apply_update(&mut self, update: &DepthUpdate) {
        for bid in &update.b {
            let price = bid[0];
            let qty = bid[1];
            if qty == Decimal::ZERO {
                self.bids.remove(&price);
            } else {
                let price = bid[0];
                let qty = bid[1];
                if qty > Decimal::ZERO {
                    if let Some(old_qty) = self.bids.get_mut(&price) {
                        let old_sum = old_qty.iter().sum::<Decimal>();
                        if old_sum > qty {
                            let change = old_sum - qty;
                            if let Some(pos) = old_qty.iter().rposition(|&x| x == change) {
                                old_qty.remove(pos); // Removes the last occurrence of the value
                            } else {
                                let largest_order = *old_qty.iter().max().unwrap();
                                let largest_pos =
                                    old_qty.iter().position(|&x| x == largest_order).unwrap();
                                old_qty.remove(largest_pos);
                                old_qty.push_back(largest_order - change);
                            }
                        } else if old_sum < qty {
                            if old_sum < qty {
                                let change = qty - old_sum;
                                old_qty.push_back(change);
                            }
                        } else {
                            // ??
                            continue;
                        }
                    } else {
                        self.bids.insert(price, VecDeque::from(vec![qty]));
                    }
                }
            }
        }
        for ask in &update.a {
            let price = ask[0];
            let qty = ask[1];
            if qty == Decimal::ZERO {
                self.asks.remove(&price);
            } else if let Some(old_qty) = self.asks.get_mut(&price) {
                let old_sum = old_qty.iter().sum::<Decimal>();
                if old_sum > qty {
                    let change = old_sum - qty;
                    if let Some(pos) = old_qty.iter().rposition(|&x| x == change) {
                        old_qty.remove(pos); // Removes the last occurrence of the value
                    } else {
                        let largest_order = *old_qty.iter().max().unwrap();
                        let largest_pos = old_qty.iter().position(|&x| x == largest_order).unwrap();
                        old_qty.remove(largest_pos);
                        old_qty.push_back(largest_order - change);
                    }
                } else if old_sum < qty {
                    if old_sum < qty {
                        let change = qty - old_sum;
                        old_qty.push_back(change);
                    }
                } else {
                    // ??
                    continue;
                }
            } else {
                self.asks.insert(price, VecDeque::from(vec![qty]));
            }
        }
    }
}
