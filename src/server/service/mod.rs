use axum::extract::ws::{Message, WebSocket};
use futures::SinkExt;
use tokio::sync::mpsc;
use tracing::{error, warn};

use super::message;
use instructions::{MarketDataHandle, TradeHandle};
mod instructions;

pub struct Client {
    pub ws: WebSocket,
    market_data: Option<MarketDataHandle>,
    trade: Option<TradeHandle>,
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
    pub async fn send_report<T: serde::Serialize>(
        &mut self,
        code: message::report::ReportCode,
        message: &str,
        data: T,
    ) {
        match message::generate_report_string(code, message, data) {
            Ok(report_string) => {
                self.ws
                    .send(Message::Text(report_string.into()))
                    .await
                    .unwrap_or_else(|e| {
                        error!("Error sending report: {:?}", e);
                    });
            }
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
                                    instructions::handle_instruction(&instruction, c).await;
                                },
                                Err(e) => {
                                    error!("Failed to parse instruction: {} {:?}", msg.as_str(), e);
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
