use axum::body::Body;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::Response;
use tracing::{info, error};

/// Handle root path: upgrade WebSocket handshakes via axum, reject plain HTTP with 404.
pub async fn root(ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>) -> Response {
    match ws {
        Ok(upgrade) => {
            upgrade.on_upgrade(|socket| async move {
                info!("A WebSocket connection established.");
                handle_websocket(socket).await;
            })
        },
        Err(_) => {
            deny_common_http()
        },
    }
}

fn deny_common_http() -> Response {
    let body = "<!DOCTYPE html><html><head><title>Illegal Access</title></head><body><h1>NOT FOUND</h1><p>HTTP access is not allowed.</p></body></html>";
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("Content-Type", "text/html")
        .body(Body::from(body))
        .unwrap()
}

async fn handle_websocket(mut ws: WebSocket) {
    while let Some(Ok(msg)) = ws.recv().await {
        if matches!(msg, Message::Close(_)) {
            break;
        }
        else {
            // Echo other messages back.
            if ws.send(msg).await.is_err() {
                break;
            }
        }
    }
    error!("A WebSocket connection closed.");
}
