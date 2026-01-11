use axum::extract::ws::{WebSocket, Message};
use futures::SinkExt;
use tracing::{info, warn, error};

use super::message;

pub struct Client {
    pub ws: WebSocket,
}

impl Client {
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
        if let Some(msg) = c.ws.recv().await {
            match msg {
                Ok(msg) => {
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
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket error: {:?}", e);
                    break;
                }
            }
        } else {
            warn!("A WebSocket connection closed");
            break;
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
        message::Instruction::ClientAuthenticate(_) => {},
        message::Instruction::Login(_) => {},
        message::Instruction::Logout(_) => {},
        message::Instruction::OrderInsert(_) => {},
        message::Instruction::OrderCancel(_) => {},
        message::Instruction::OrderQuery(_) => {},
        message::Instruction::PositionQuery(_) => {},
        message::Instruction::AccountQuery(_) => {},
        message::Instruction::InstrumentQuery(_) => {},
        message::Instruction::MarketDataSubscribe(_) => {},
        message::Instruction::MarketDataUnsubscribe(_) => {},
    }
}