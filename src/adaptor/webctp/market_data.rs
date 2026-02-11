use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use websocket_lite::{AsyncNetworkStream, ClientBuilder, Message as WsMessage};

type Socket =
    websocket_lite::AsyncClient<Box<dyn AsyncNetworkStream + Sync + Send + Unpin + 'static>>;

use super::message::{Envelope, MarketData, MdMsgCode, MdLogin, MdLogout, MdSubscribe, MdUnsubscribe, MdTradingDay};
use super::{WebCtpError, WebCtpResult};

#[derive(Debug, Clone)]
pub enum MarketDataEvent {
    Ready { err: Value, info: Value },
    Performed { err: Value, info: Value },
    Error { err: Value },
    FrontConnected { err: Value, info: Value },
    FrontDisconnected { err: Value, info: Value },
    HeartbeatTimeout { err: Value, info: Value },
    Login { err: Value, info: MdLogin },
    Logout { err: Value, info: MdLogout },
    TradingDay { err: Value, info: MdTradingDay },
    Subscribe { err: Value, info: MdSubscribe },
    Unsubscribe { err: Value, info: MdUnsubscribe },
    MarketData { err: Value, info: MarketData },
    ErrorSize { err: Value },
    Unknown { err: Value, raw: Value },
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

    pub async fn connect(&mut self, url: &str) -> WebCtpResult<()> {
        let client = ClientBuilder::new(url)
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
        self.send_op("get_trading_day", json!({})).await
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
                    Ok(Some(parse_market_data(serde_json::from_slice(
                        msg.data(),
                    )?)?))
                }
            }
            Some(Err(e)) => Err(WebCtpError::WebSocket(e)),
            None => Ok(None),
        }
    }
}

fn parse_market_data(env: Envelope) -> WebCtpResult<MarketDataEvent> {
    let Envelope { msg, err, info } = env;

    match msg {
        Value::String(s) => match s.as_str() {
            "ready" => Ok(MarketDataEvent::Ready { err, info }),
            "parse_error" | "processing_error" | "error" => Ok(MarketDataEvent::Error { err }),
            _ => Ok(MarketDataEvent::Unknown {
                err,
                raw: Value::String(s),
            }),
        },
        Value::Number(num) => {
            let code: i64 = num
                .as_i64()
                .ok_or_else(|| WebCtpError::Protocol("non-integer msg code".into()))?;
            match MdMsgCode::try_from(code) {
                Ok(MdMsgCode::Performed) => Ok(MarketDataEvent::Performed { err, info }),
                Ok(MdMsgCode::Error) => Ok(MarketDataEvent::Error { err }),
                Ok(MdMsgCode::Connected) => Ok(MarketDataEvent::FrontConnected { err, info }),
                Ok(MdMsgCode::Disconnected) => Ok(MarketDataEvent::FrontDisconnected { err, info }),
                Ok(MdMsgCode::HeartbeatTimeout) => {
                    Ok(MarketDataEvent::HeartbeatTimeout { err, info })
                }
                Ok(MdMsgCode::Login) => {
                    let info: MdLogin = serde_json::from_value(info)?;
                    Ok(MarketDataEvent::Login { err, info })
                }
                Ok(MdMsgCode::Logout) => {
                    let info: MdLogout = serde_json::from_value(info)?;
                    Ok(MarketDataEvent::Logout { err, info })
                }
                Ok(MdMsgCode::TradingDay) => {
                    let info: MdTradingDay = serde_json::from_value(info)?;
                    Ok(MarketDataEvent::TradingDay { err, info })
                }
                Ok(MdMsgCode::Subscribe) => {
                    let info: MdSubscribe = serde_json::from_value(info)?;
                    Ok(MarketDataEvent::Subscribe { err, info })
                }
                Ok(MdMsgCode::Unsubscribe) => {
                    let info: MdUnsubscribe = serde_json::from_value(info)?;
                    Ok(MarketDataEvent::Unsubscribe { err, info })
                }
                Ok(MdMsgCode::MarketData) => {
                    let info: MarketData = serde_json::from_value(info)?;
                    Ok(MarketDataEvent::MarketData { err, info })
                }
                Ok(MdMsgCode::ErrorSize) => Ok(MarketDataEvent::ErrorSize { err }),
                Err(_) => Ok(MarketDataEvent::Unknown {
                    err,
                    raw: json!({"msg_code": code}),
                }),
            }
        }
        other => Ok(MarketDataEvent::Unknown { err, raw: other }),
    }
}
