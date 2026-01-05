use axum::Router;
use tracing::{Level, error, info};

use crate::args;

mod controller;
mod router;
mod service;

fn get_address(config: &args::Config) -> String {
    let host = if let Some(host) = config.host.clone() {
        host
    } else {
        "0.0.0.0".to_string()
    };
    let port = if let Some(port) = config.port {
        port
    } else {
        8080
    };
    format!("{}:{}", host, port)
}

pub async fn main_process(config: &args::Config) {
    tracing_subscriber::fmt().init();
    let span = tracing::span!(Level::INFO, "main_process");
    let _enter = span.enter();

    let router: Router = router::get_router();

    let address = get_address(config);

    info!("QDEngine Server will listen {}", address);

    let server = match tokio::net::TcpListener::bind(address).await {
        Ok(server) => {
            info!("Ready to start server.");
            server
        }
        Err(e) => {
            error!(
                "Failed to bind server to {} with error: {}",
                get_address(config),
                e
            );
            return;
        }
    };
    match axum::serve(server, router).await {
        Ok(_) => info!("QDEngine Server Started."),
        Err(e) => error!("QDEngine Server exited with error: {}", e),
    }
}
