pub mod market_data;
pub mod message;
pub mod trade;

use websocket_lite::Error as WsError;

pub use market_data::{MarketDataClient, MarketDataEvent};
pub use message::{MdMsgCode, TradeMsgCode};
pub use trade::{TradeClient, TradeEvent};

pub type WebCtpResult<T> = Result<T, WebCtpError>;

#[derive(Debug)]
pub enum WebCtpError {
    NotConnected,
    WebSocket(WsError),
    Json(serde_json::Error),
    Protocol(String),
}

impl From<WsError> for WebCtpError {
    fn from(value: WsError) -> Self {
        WebCtpError::WebSocket(value)
    }
}

impl From<serde_json::Error> for WebCtpError {
    fn from(value: serde_json::Error) -> Self {
        WebCtpError::Json(value)
    }
}
