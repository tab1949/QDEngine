use axum::body::Body;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::Response;
use futures::SinkExt;
use tracing::{info, error};
use rand::{distr::Alphanumeric, Rng};

use super::message;
use super::service;

/// Handle root path: upgrade WebSocket handshakes via axum, reject plain HTTP with 404.
pub async fn root(ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>) -> Response {
    match ws {
        Ok(upgrade) => {
            upgrade.on_upgrade(|socket| async move {
                info!("A WebSocket connection established.");
                tokio::spawn(handle_new_websocket(socket));
            })
        },
        Err(_) => {
            deny_common_http()
        },
    }
}

fn deny_common_http() -> Response {
    let body = 
    "<!DOCTYPE html><html><head><title>Illegal Access</title></head><body><h1>NOT FOUND</h1><p>HTTP access is not allowed.</p></body></html>";
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Type", "text/html")
        .body(Body::from(body))
        .unwrap()
}

async fn handle_new_websocket(mut ws: WebSocket) {
    let token: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .map(char::from)
        .take(16)
        .collect();
    info!("Generated session token: {}", token);
    let handshake = message::generate_report_string(
        message::report::ReportCode::Handshake, 
        "connected", 
        message::report::HandshakeData { 
            token: token.clone() 
        }).unwrap();
    ws.send(Message::Text(handshake.into())).await.unwrap();
    let mut auth_success = false;
    let mut close_reason = String::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(crate::CONFIG.handshake_timeout.unwrap()),
        async {
            if let Some(Ok(msg)) = ws.recv().await {
                if let Message::Text(text) = msg {
                    match message::parse_instruction(&text) {
                        Ok(instruction) => {
                            if let message::Instruction::Handshake(auth) = instruction {
                                if auth.token == token {
                                    info!("Client authenticated successfully. (token: {})", auth.token);
                                    auth_success = true;
                                } else {
                                    error!("Client authentication failed: invalid token. (expected: {}, got: {})", token, auth.token);
                                    auth_success = false;
                                    close_reason = "invalid token".to_string();
                                }
                            } else {
                                error!("Expected handshake instruction first.");
                                auth_success = false;
                                close_reason = "handshake expected".to_string();
                            }
                        },
                        Err(e) => {
                            error!("Failed to parse instruction: {}", e);
                            auth_success = false;
                            close_reason = "failed to parse instruction".to_string();
                        }
                    }
                }
            }
        },
    ).await;
    if close_reason.is_empty() && !auth_success {
        close_reason = "authentication timeout".to_string();
    }
    if !auth_success {
        let fail_report = message::generate_report_string(
            message::report::ReportCode::AuthenticateFailed,
            "authentication failed",
            message::report::AuthenticateFailedInfo {
                reason: close_reason,
            }
        ).unwrap();
        // Send authentication failure report
        ws.send(Message::Text(fail_report.into())).await.unwrap();
        // Close the WebSocket connection
        ws.close().await.unwrap();
        info!("Closed a WebSocket connection due to authentication failure.");
        return;
    }
    else {
        let success_report = message::generate_report_string(
            message::report::ReportCode::Success,
            "handshake successful",
            ()
        ).unwrap();
        ws.send(Message::Text(success_report.into())).await.unwrap();
        // Proceed to serve the authenticated WebSocket connection
        let mut client = service::Client::new(ws);
        service::serve(&mut client).await;
    }
}
