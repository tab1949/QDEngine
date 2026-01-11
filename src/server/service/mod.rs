use axum::extract::ws::{WebSocket};
use tracing::{info, warn, error};

pub async fn serve(mut ws: WebSocket) {
    loop {
        if let Some(msg) = ws.recv().await {
            match msg {
                Ok(msg) => {
                    info!("Received message: {:?}", msg);
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