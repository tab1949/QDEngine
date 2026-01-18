use axum::Router;
use tracing::{Level, error, info};

use crate::args;

mod controller;
mod message;
mod router;
mod service;

pub struct Server<'a> {
    config: &'a args::Config,
}

impl<'a> Server<'a> {
    pub fn new(config: &'a args::Config) -> Self {
        Server { config: config }
    }

    pub async fn main_process(&self) {
        tracing_subscriber::fmt().init();
        let span = tracing::span!(Level::INFO, "main_process");
        let _enter = span.enter();

        let address = self.get_address();
        info!("QDEngine Server will listen {}", address);

        let server = match tokio::net::TcpListener::bind(address).await {
            Ok(server) => {
                info!("Ready to start server.");
                server
            }
            Err(e) => {
                error!(
                    "Failed to bind server to {} with error: {}",
                    self.get_address(),
                    e
                );
                return;
            }
        };

        let router: Router = router::get_router();

        match axum::serve(server, router).await {
            Ok(_) => info!("QDEngine Server Started."),
            Err(e) => error!("QDEngine Server exited with error: {}", e),
        }
    }

    fn get_address(&self) -> String {
        let host = if let Some(host) = self.config.host.clone() {
            host
        } else {
            "0.0.0.0".to_string()
        };
        let port = if let Some(port) = self.config.port {
            port
        } else {
            8080
        };
        format!("{}:{}", host, port)
    }
}
