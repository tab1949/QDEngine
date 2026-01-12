use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::SinkExt;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, warn};

use crate::adaptor::webctp::{MarketDataClient, MarketDataEvent, TradeClient, TradeEvent, WebCtpError};

use super::message;

pub struct Client {
    pub ws: WebSocket,
    market_data: Option<Arc<Mutex<MarketDataClient>>>,
    trade: Option<Arc<Mutex<TradeClient>>>,
    outbound_tx: mpsc::UnboundedSender<String>,
    outbound_rx: mpsc::UnboundedReceiver<String>,
}

impl Client {
    pub fn new(ws: WebSocket) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        Self {
            ws,
            market_data: None,
            trade: None,
            outbound_tx,
            outbound_rx,
        }
    }

    // Send a structured report to the client; logs on serialization or send errors.
    pub async fn send_report<T: serde::Serialize>(&mut self, code: message::report::ReportCode, message: &str, data: T) {
        match message::generate_report_string(code, message, data) {
            Ok(report_string) => {
                self.ws.send(Message::Text(report_string.into())).await.unwrap_or_else(|e| {
                    error!("Error sending report: {:?}", e);
                });
            },
            Err(e) => {
                error!("Error generating report string: {:?}", e);
            }
        }
    }
}

pub async fn serve(c: &mut Client) {
    loop {
        tokio::select! {
            biased;
            outbound = c.outbound_rx.recv() => {
                match outbound {
                    Some(text) => {
                        if let Err(e) = c.ws.send(Message::Text(text.into())).await {
                            error!("Error sending outbound report: {:?}", e);
                            break;
                        }
                    },
                    None => {
                        // channel closed, but continue to serve inbound until ws closes
                    }
                }
            }
            maybe_msg = c.ws.recv() => {
                match maybe_msg {
                    Some(Ok(msg)) => {
                        if let Message::Close(_) = msg {
                            warn!("Received close frame from client. Closing connection.");
                            c.ws.close().await.unwrap_or_else(|e| {
                                error!("Error closing WebSocket: {:?}", e);
                            });
                            break;
                        }
                        else if let Message::Text(msg) = msg {
                            match message::parse_instruction(msg.as_str()) {
                                Ok(instruction) => {
                                    handle_instruction(&instruction, c).await;
                                },
                                Err(e) => {
                                    error!("Failed to parse instruction: {:?}", e);
                                    c
                                        .send_report(
                                            message::report::ReportCode::GeneralError,
                                            "failed to parse instruction",
                                            format!("{:?}", e),
                                        )
                                        .await;
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!("WebSocket error: {:?}", e);
                        break;
                    }
                    None => {
                        warn!("A WebSocket connection closed");
                        break;
                    }
                }
            }
        }
    }
}

async fn handle_instruction(instruction: &message::Instruction, client: &mut Client) {
    match instruction {
        message::Instruction::Handshake(_) => {
            client.send_report(message::report::ReportCode::GeneralError, "handshake already received", ()).await;
        },
        message::Instruction::TestRequest(_) => {},
        message::Instruction::TestCancel(_) => {},
        message::Instruction::TestQuery(_) => {},
        message::Instruction::WebCtpMarketDataConnect(instr) => {
            let mut md = MarketDataClient::new(instr.broker_id.clone(), instr.user_id.clone());
            match md.connect(&instr.addr, instr.port).await {
                Ok(_) => {
                    let md = Arc::new(Mutex::new(md));
                    spawn_market_data_listener(md.clone(), client.outbound_tx.clone());
                    client.market_data = Some(md);
                    client.send_report(
                        message::report::ReportCode::WebCtpOperationAck,
                        "webctp market data connected",
                        message::report::OperationAck { ok: true },
                    )
                    .await;
                }
                Err(e) => {
                    send_webctp_error(client, "failed to connect webctp market data", e).await;
                }
            }
        }
        message::Instruction::WebCtpMarketDataConnectFront(instr) => {
            if let Some(md) = client.market_data.as_mut() {
                let res = {
                    let mut guard = md.lock().await;
                    guard.connect_front(&instr.addr, instr.port).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "market data connect_front failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp market data connect_front sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataLogin(instr) => {
            if let Some(md) = client.market_data.as_mut() {
                let res = {
                    let mut guard = md.lock().await;
                    guard.login(&instr.password).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "market data login failed", e).await;
                } else {
                    client.send_report(
                        message::report::ReportCode::WebCtpOperationAck,
                        "webctp market data login sent",
                        message::report::OperationAck { ok: true },
                    )
                    .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataSubscribe(instr) => {
            if let Some(md) = client.market_data.as_mut() {
                let res = {
                    let mut guard = md.lock().await;
                    guard.subscribe(&instr.instruments).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "market data subscribe failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp market data subscribe sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataUnsubscribe(instr) => {
            if let Some(md) = client.market_data.as_mut() {
                let res = {
                    let mut guard = md.lock().await;
                    guard.unsubscribe(&instr.instruments).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "market data unsubscribe failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp market data unsubscribe sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataTradingDay(_) => {
            if let Some(md) = client.market_data.as_mut() {
                let res = {
                    let mut guard = md.lock().await;
                    guard.get_trading_day().await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "market data get_trading_day failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp market data get_trading_day sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataDisconnect(_) => {
            if let Some(md) = client.market_data.take() {
                let res = {
                    let mut guard = md.lock().await;
                    guard.disconnect().await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "market data disconnect failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp market data disconnected",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeConnect(instr) => {
            let mut trade = TradeClient::new(instr.broker_id.clone(), instr.investor_id.clone());
            match trade.connect(&instr.addr, instr.port).await {
                Ok(_) => {
                    let trade = Arc::new(Mutex::new(trade));
                    spawn_trade_listener(trade.clone(), client.outbound_tx.clone());
                    client.trade = Some(trade);
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade connected",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
                Err(e) => {
                    send_webctp_error(client, "failed to connect webctp trade", e).await;
                }
            }
        }
        message::Instruction::WebCtpTradeConnectFront(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard.connect_front(&instr.addr, instr.port).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade connect_front failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade connect_front sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeSet(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard.set(instr.broker_id.clone(), instr.investor_id.clone()).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade set failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade set sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeTradingDay(_) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard.get_trading_day().await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade get_trading_day failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade get_trading_day sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeAuth(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard.auth(&instr.user_id, &instr.app_id, &instr.auth_code).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade auth failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade auth sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeLogin(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard.login(&instr.user_id, &instr.password).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade login failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade login sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeLogout(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard.logout(&instr.user_id).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade logout failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade logout sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeQuerySettlementInfo(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard.query_settlement_info(&instr.trading_day).await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade query_settlement_info failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade query_settlement_info sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeConfirmSettlementInfo(_) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard.confirm_settlement_info().await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade confirm_settlement_info failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade confirm_settlement_info sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeQueryTradingAccount(_) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard.query_trading_account().await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade query_trading_account failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade query_trading_account sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeInsertOrder(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard
                        .insert_order(
                            &instr.instrument,
                            &instr.exchange,
                            &instr.reference,
                            instr.price,
                            instr.direction,
                            instr.offset,
                            instr.volume,
                            instr.price_type,
                            instr.time_condition,
                        )
                        .await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade insert_order failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade insert_order sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeQueryOrder(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard
                        .query_order(
                            instr.order_sys_id.clone(),
                            instr.exchange_id.clone(),
                            instr.from.clone(),
                            instr.to.clone(),
                        )
                        .await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade query_order failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade query_order sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeDeleteOrder(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard
                        .delete_order(
                            &instr.exchange,
                            &instr.instrument,
                            instr.delete_ref,
                            &instr.order_sys_id,
                        )
                        .await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade delete_order failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade delete_order sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeQueryInstrument(instr) => {
            if let Some(trade) = client.trade.as_mut() {
                let res = {
                    let mut guard = trade.lock().await;
                    guard
                        .query_instrument(
                            instr.exchange.clone(),
                            instr.instrument.clone(),
                            instr.exchange_inst_id.clone(),
                            instr.product_id.clone(),
                        )
                        .await
                };
                if let Err(e) = res {
                    send_webctp_error(client, "trade query_instrument failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade query_instrument sent",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeDisconnect(_) => {
            if let Some(trade) = client.trade.take() {
                if let Err(e) = trade.lock().await.disconnect().await {
                    send_webctp_error(client, "trade disconnect failed", e).await;
                } else {
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade disconnected",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
    }
}

async fn send_webctp_error(client: &mut Client, title: &str, err: WebCtpError) {
    client
        .send_report(
            message::report::ReportCode::GeneralError,
            title,
            format!("{:?}", err),
        )
        .await;
}

fn spawn_market_data_listener(md: Arc<Mutex<MarketDataClient>>, tx: mpsc::UnboundedSender<String>) {
    tokio::spawn(async move {
        loop {
            let evt = {
                let mut guard = md.lock().await;
                tokio::time::timeout(std::time::Duration::from_millis(200), guard.next_event())
                    .await
            };
            match evt {
                Ok(Ok(Some(event))) => {
                    if let Some(text) = serialize_market_event(event) {
                        let _ = tx.send(text);
                    }
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    let _ = tx.send(
                        message::generate_report_string(
                            message::report::ReportCode::GeneralError,
                            "market data event loop error",
                            format!("{:?}", e),
                        )
                        .unwrap_or_else(|ser_err| format!("{{\"code\":-1,\"message\":\"serialize error: {:?}\",\"data\":null}}", ser_err)),
                    );
                    break;
                }
                Err(_elapsed) => {
                    // timeout: allow others to acquire the lock and continue
                    continue;
                }
            }
        }
    });
}

fn spawn_trade_listener(trade: Arc<Mutex<TradeClient>>, tx: mpsc::UnboundedSender<String>) {
    tokio::spawn(async move {
        loop {
            let evt = {
                let mut guard = trade.lock().await;
                tokio::time::timeout(std::time::Duration::from_millis(200), guard.next_event())
                    .await
            };
            match evt {
                Ok(Ok(Some(event))) => {
                    if let Some(text) = serialize_trade_event(event) {
                        let _ = tx.send(text);
                    }
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    let _ = tx.send(
                        message::generate_report_string(
                            message::report::ReportCode::GeneralError,
                            "trade event loop error",
                            format!("{:?}", e),
                        )
                        .unwrap_or_else(|ser_err| format!("{{\"code\":-1,\"message\":\"serialize error: {:?}\",\"data\":null}}", ser_err)),
                    );
                    break;
                }
                Err(_elapsed) => {
                    continue;
                }
            }
        }
    });
}

fn serialize_market_event(event: MarketDataEvent) -> Option<String> {
    let (event_name, payload) = match event {
        MarketDataEvent::Ready => ("ready", serde_json::json!({})),
        MarketDataEvent::Performed(v) => ("performed", v),
        MarketDataEvent::Error(v) => ("error", v),
        MarketDataEvent::FrontConnected(v) => ("front_connected", v),
        MarketDataEvent::FrontDisconnected(v) => ("front_disconnected", v),
        MarketDataEvent::HeartbeatTimeout(v) => ("heartbeat_timeout", v),
        MarketDataEvent::Login { trading_day, raw } => (
            "login",
            serde_json::json!({"trading_day": trading_day, "raw": raw}),
        ),
        MarketDataEvent::Logout(v) => ("logout", v),
        MarketDataEvent::TradingDay { trading_day, raw } => (
            "trading_day",
            serde_json::json!({"trading_day": trading_day, "raw": raw}),
        ),
        MarketDataEvent::Subscribe(v) => ("subscribe", v),
        MarketDataEvent::Unsubscribe(v) => ("unsubscribe", v),
        MarketDataEvent::MarketData(md) => ("market_data", serde_json::json!({"debug": format!("{:?}", md)})),
        MarketDataEvent::Unknown(v) => ("unknown", v),
    };

    message::generate_report_string(
        message::report::ReportCode::WebCtpMarketDataEvent,
        "webctp market data event",
        message::report::WebCtpEventReport {
            source: "market_data".to_string(),
            event: event_name.to_string(),
            payload,
        },
    )
    .ok()
}

fn serialize_trade_event(event: TradeEvent) -> Option<String> {
    let (event_name, payload) = match event {
        TradeEvent::Ready => ("ready", serde_json::json!({})),
        TradeEvent::Performed(v) => ("performed", v),
        TradeEvent::Error(v) => ("error", v),
        TradeEvent::ErrorNull(v) => ("error_null", v),
        TradeEvent::ErrorUnknownValue(v) => ("error_unknown_value", v),
        TradeEvent::FrontConnected(v) => ("front_connected", v),
        TradeEvent::TradingDay { trading_day, raw } => (
            "trading_day",
            serde_json::json!({"trading_day": trading_day, "raw": raw}),
        ),
        TradeEvent::FrontDisconnected(v) => ("front_disconnected", v),
        TradeEvent::Authenticate(v) => ("authenticate", v),
        TradeEvent::Login { trading_day, raw } => (
            "login",
            serde_json::json!({"trading_day": trading_day, "raw": raw}),
        ),
        TradeEvent::Logout(v) => ("logout", v),
        TradeEvent::SettlementInfo(s) => ("settlement_info", serde_json::json!({"debug": format!("{:?}", s)})),
        TradeEvent::SettlementInfoConfirm(s) => ("settlement_info_confirm", serde_json::json!({"debug": format!("{:?}", s)})),
        TradeEvent::TradingAccount(a) => ("trading_account", serde_json::json!({"debug": format!("{:?}", a)})),
        TradeEvent::OrderInsertReturnError(ei) => ("order_insert_return_error", serde_json::json!({"debug": format!("{:?}", ei)})),
        TradeEvent::OrderInsertError(ei) => ("order_insert_error", serde_json::json!({"debug": format!("{:?}", ei)})),
        TradeEvent::OrderInserted(o) => ("order_inserted", serde_json::json!({"debug": format!("{:?}", o)})),
        TradeEvent::OrderTraded(o) => ("order_traded", serde_json::json!({"debug": format!("{:?}", o)})),
        TradeEvent::QueryOrder(o) => ("query_order", serde_json::json!({"debug": format!("{:?}", o)})),
        TradeEvent::QueryInstrument(i) => ("query_instrument", serde_json::json!({"debug": format!("{:?}", i)})),
        TradeEvent::OrderDeleteReturnError(ei) => ("order_delete_return_error", serde_json::json!({"debug": format!("{:?}", ei)})),
        TradeEvent::OrderDeleteError(ei) => ("order_delete_error", serde_json::json!({"debug": format!("{:?}", ei)})),
        TradeEvent::OrderDeleted(o) => ("order_deleted", serde_json::json!({"debug": format!("{:?}", o)})),
        TradeEvent::Unknown(v) => ("unknown", v),
    };

    message::generate_report_string(
        message::report::ReportCode::WebCtpTradeEvent,
        "webctp trade event",
        message::report::WebCtpEventReport {
            source: "trade".to_string(),
            event: event_name.to_string(),
            payload,
        },
    )
    .ok()
}