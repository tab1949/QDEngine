use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use websocket_lite::{AsyncNetworkStream, ClientBuilder, Message as WsMessage};

type Socket = websocket_lite::AsyncClient<Box<dyn AsyncNetworkStream + Sync + Send + Unpin + 'static>>;

use super::message::{Envelope, MarketData, MdMsgCode};
use super::{WebCtpError, WebCtpResult};

#[derive(Debug, Clone)]
pub enum MarketDataEvent {
    Ready,
    Performed(Value),
    Error(Value),
    FrontConnected(Value),
    FrontDisconnected(Value),
    HeartbeatTimeout(Value),
    Login {
        trading_day: Option<String>,
        raw: Value,
    },
    Logout(Value),
    TradingDay {
        trading_day: Option<String>,
        raw: Value,
    },
    Subscribe(Value),
    Unsubscribe(Value),
    MarketData(MarketData),
    Unknown(Value),
}

pub struct MarketDataClient {
    broker_id: String,
    user_id: String,
    ws: Option<Socket>,
}

impl MarketDataClient {
    pub fn new(broker_id: impl Into<String>, user_id: impl Into<String>) -> Self {
        Self {
            broker_id: broker_id.into(),
            user_id: user_id.into(),
            ws: None,
        }
    }

    pub async fn connect(&mut self, addr: &str, port: u16) -> WebCtpResult<()> {
        let url = format!("ws://{}:{}/market_data", addr, port);
        let client = ClientBuilder::new(&url)
            .map_err(|e| WebCtpError::Protocol(e.to_string()))?
            .async_connect()
            .await?;
        self.ws = Some(client);
        Ok(())
    }

    pub async fn connect_front(&mut self, addr: &str, port: u16) -> WebCtpResult<()> {
        self.send_op("connect", json!({"addr": addr, "port": port.to_string()}))
            .await
    }

    pub async fn login(&mut self, password: &str) -> WebCtpResult<()> {
        self.send_op(
            "login",
            json!({"broker_id": self.broker_id, "user_id": self.user_id, "password": password}),
        )
        .await
    }

    pub async fn subscribe(&mut self, instruments: &[String]) -> WebCtpResult<()> {
        self.send_op("subscribe", json!({"instruments": instruments}))
            .await
    }

    pub async fn unsubscribe(&mut self, instruments: &[String]) -> WebCtpResult<()> {
        self.send_op("unsubscribe", json!({"instruments": instruments}))
            .await
    }

    pub async fn get_trading_day(&mut self) -> WebCtpResult<()> {
        self.send_op("get_trading_day", json!({}))
            .await
    }

    pub async fn disconnect(&mut self) -> WebCtpResult<()> {
        if let Some(ws) = self.ws.as_mut() {
            ws.send(WsMessage::close(None)).await?;
        }
        self.ws = None;
        Ok(())
    }

    async fn send_op(&mut self, op: &str, data: Value) -> WebCtpResult<()> {
        let ws = self.ws.as_mut().ok_or(WebCtpError::NotConnected)?;
        let text = serde_json::to_string(&json!({"op": op, "data": data}))?;
        ws.send(WsMessage::text(text)).await?;
        Ok(())
    }

    pub async fn next_event(&mut self) -> WebCtpResult<Option<MarketDataEvent>> {
        let ws = self.ws.as_mut().ok_or(WebCtpError::NotConnected)?;
        match ws.next().await {
            Some(Ok(msg)) => {
                if let Some(text) = msg.as_text() {
                    let env: Envelope = serde_json::from_str(text)?;
                    Ok(Some(parse_market_data(env)?))
                } else {
                    Ok(Some(parse_market_data(serde_json::from_slice(msg.data())?)?))
                }
            }
            Some(Err(e)) => Err(WebCtpError::WebSocket(e)),
            None => Ok(None),
        }
    }
}

fn parse_market_data(env: Envelope) -> WebCtpResult<MarketDataEvent> {
    match env.msg {
        Value::String(s) => match s.as_str() {
            "ready" => Ok(MarketDataEvent::Ready),
            "parse_error" | "processing_error" | "error" => {
                Ok(MarketDataEvent::Error(env.info.clone()))
            }
            _ => Ok(MarketDataEvent::Unknown(Value::String(s))),
        },
        Value::Number(num) => {
            let code: i64 = num
                .as_i64()
                .ok_or_else(|| WebCtpError::Protocol("non-integer msg code".into()))?;
            let info = env.info;
            match MdMsgCode::try_from(code) {
                Ok(MdMsgCode::Performed) => Ok(MarketDataEvent::Performed(info)),
                Ok(MdMsgCode::Error) => Ok(MarketDataEvent::Error(info)),
                Ok(MdMsgCode::Connected) => Ok(MarketDataEvent::FrontConnected(info)),
                Ok(MdMsgCode::Disconnected) => Ok(MarketDataEvent::FrontDisconnected(info)),
                Ok(MdMsgCode::HeartbeatTimeout) => Ok(MarketDataEvent::HeartbeatTimeout(info)),
                Ok(MdMsgCode::Login) => Ok(MarketDataEvent::Login {
                    trading_day: parse_trading_day(&info),
                    raw: info,
                }),
                Ok(MdMsgCode::Logout) => Ok(MarketDataEvent::Logout(info)),
                Ok(MdMsgCode::TradingDay) => Ok(MarketDataEvent::TradingDay {
                    trading_day: parse_trading_day(&info),
                    raw: info,
                }),
                Ok(MdMsgCode::Subscribe) => Ok(MarketDataEvent::Subscribe(info)),
                Ok(MdMsgCode::Unsubscribe) => Ok(MarketDataEvent::Unsubscribe(info)),
                Ok(MdMsgCode::MarketData) => {
                    let data: MarketData = serde_json::from_value(info)?;
                    Ok(MarketDataEvent::MarketData(data))
                }
                Err(_) => Ok(MarketDataEvent::Unknown(json!({"msg_code": code}))),
            }
        }
        other => Ok(MarketDataEvent::Unknown(other)),
    }
}

fn parse_trading_day(info: &Value) -> Option<String> {
    match info.get("trading_day") {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}
