use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use websocket_lite::{AsyncNetworkStream, ClientBuilder, Message as WsMessage};

type Socket = websocket_lite::AsyncClient<Box<dyn AsyncNetworkStream + Sync + Send + Unpin + 'static>>;

use super::message::{
    Envelope, Instrument, OrderDeleteError, OrderDeleteReturnError, OrderDeleted, OrderInsertError,
    OrderInsertReturnError, OrderInserted, OrderTraded, QueryOrder, SettlementInfo, SettlementInfoConfirm,
    TradeMsgCode, TradingAccount,
};
use super::{WebCtpError, WebCtpResult};

#[derive(Debug, Clone)]
pub enum TradeEvent {
    Ready,
    Performed(Value),
    Error(Value),
    ErrorNull(Value),
    ErrorUnknownValue(Value),
    FrontConnected(Value),
    TradingDay {
        trading_day: Option<String>,
        raw: Value,
    },
    FrontDisconnected(Value),
    Authenticate(Value),
    Login {
        trading_day: Option<String>,
        raw: Value,
    },
    Logout(Value),
    SettlementInfo(SettlementInfo),
    SettlementInfoConfirm(SettlementInfoConfirm),
    TradingAccount(TradingAccount),
    OrderInsertReturnError(OrderInsertReturnError),
    OrderInsertError(OrderInsertError),
    OrderInserted(OrderInserted),
    OrderTraded(OrderTraded),
    QueryOrder(QueryOrder),
    QueryInstrument(Instrument),
    OrderDeleteReturnError(OrderDeleteReturnError),
    OrderDeleteError(OrderDeleteError),
    OrderDeleted(OrderDeleted),
    Unknown(Value),
}

pub struct TradeClient {
    broker_id: String,
    investor_id: String,
    ws: Option<Socket>,
}

impl TradeClient {
    pub fn new(broker_id: impl Into<String>, investor_id: impl Into<String>) -> Self {
        Self {
            broker_id: broker_id.into(),
            investor_id: investor_id.into(),
            ws: None,
        }
    }

    pub async fn connect(&mut self, addr: &str, port: u16) -> WebCtpResult<()> {
        let url = format!("ws://{}:{}/trade", addr, port);
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

    pub async fn set(&mut self, broker_id: Option<String>, investor_id: Option<String>) -> WebCtpResult<()> {
        if let Some(b) = broker_id.clone() {
            self.broker_id = b;
        }
        if let Some(i) = investor_id.clone() {
            self.investor_id = i;
        }
        let mut data = serde_json::Map::new();
        if let Some(b) = broker_id {
            data.insert("broker_id".into(), json!(b));
        }
        if let Some(i) = investor_id {
            data.insert("investor_id".into(), json!(i));
        }
        self.send_op("set", Value::Object(data)).await
    }

    pub async fn get_trading_day(&mut self) -> WebCtpResult<()> {
        self.send_op("get_trading_day", json!({}))
            .await
    }

    pub async fn auth(&mut self, user_id: &str, app_id: &str, auth_code: &str) -> WebCtpResult<()> {
        self.send_op(
            "auth",
            json!({"user_id": user_id, "app_id": app_id, "auth_code": auth_code}),
        )
        .await
    }

    pub async fn login(&mut self, user_id: &str, password: &str) -> WebCtpResult<()> {
        self.send_op("login", json!({"user_id": user_id, "password": password}))
            .await
    }

    pub async fn logout(&mut self, user_id: &str) -> WebCtpResult<()> {
        self.send_op("logout", json!({"user_id": user_id}))
            .await
    }

    pub async fn query_settlement_info(&mut self, trading_day: &str) -> WebCtpResult<()> {
        self.send_op("query_settlement_info", json!({"trading_day": trading_day}))
            .await
    }

    pub async fn confirm_settlement_info(&mut self) -> WebCtpResult<()> {
        self.send_op("confirm_settlement_info", json!({}))
            .await
    }

    pub async fn query_trading_account(&mut self) -> WebCtpResult<()> {
        self.send_op("query_trading_account", json!({}))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_order(
        &mut self,
        instrument: &str,
        exchange: &str,
        reference: &str,
        price: f64,
        direction: i64,
        offset: i64,
        volume: i64,
        price_type: i64,
        time_condition: i64,
    ) -> WebCtpResult<()> {
        self.send_op(
            "insert_order",
            json!({
                "instrument": instrument,
                "exchange": exchange,
                "ref": reference,
                "price": price,
                "direction": direction,
                "offset": offset,
                "volume": volume,
                "price_type": price_type,
                "time_condition": time_condition,
            }),
        )
        .await
    }

    pub async fn query_order(
        &mut self,
        order_sys_id: Option<String>,
        exchange_id: Option<String>,
        from: Option<String>,
        to: Option<String>,
    ) -> WebCtpResult<()> {
        let mut data = serde_json::Map::new();
        if let Some(v) = order_sys_id {
            data.insert("order_sys_id".into(), json!(v));
        }
        if let Some(v) = exchange_id {
            data.insert("exchange_id".into(), json!(v));
        }
        if let Some(v) = from {
            data.insert("from".into(), json!(v));
        }
        if let Some(v) = to {
            data.insert("to".into(), json!(v));
        }
        self.send_op("query_order", Value::Object(data)).await
    }

    pub async fn delete_order(&mut self, exchange: &str, instrument: &str, delete_ref: i64, order_sys_id: &str) -> WebCtpResult<()> {
        self.send_op(
            "delete_order",
            json!({
                "exchange": exchange,
                "instrument": instrument,
                "delete_ref": delete_ref,
                "order_sys_id": order_sys_id,
            }),
        )
        .await
    }

    pub async fn query_instrument(
        &mut self,
        exchange: Option<String>,
        instrument: Option<String>,
        exchange_inst_id: Option<String>,
        product_id: Option<String>,
    ) -> WebCtpResult<()> {
        let mut data = serde_json::Map::new();
        if let Some(v) = exchange {
            data.insert("exchange".into(), json!(v));
        }
        if let Some(v) = instrument {
            data.insert("instrument".into(), json!(v));
        }
        if let Some(v) = exchange_inst_id {
            data.insert("exchange_inst_id".into(), json!(v));
        }
        if let Some(v) = product_id {
            data.insert("product_id".into(), json!(v));
        }
        self.send_op("query_instrument", Value::Object(data)).await
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

    pub async fn next_event(&mut self) -> WebCtpResult<Option<TradeEvent>> {
        let ws = self.ws.as_mut().ok_or(WebCtpError::NotConnected)?;
        match ws.next().await {
            Some(Ok(msg)) => {
                if let Some(text) = msg.as_text() {
                    let env: Envelope = serde_json::from_str(text)?;
                    Ok(Some(parse_trade(env)?))
                } else {
                    Ok(Some(TradeEvent::Unknown(json!({"binary": true}))))
                }
            }
            Some(Err(e)) => Err(WebCtpError::WebSocket(e)),
            None => Ok(None),
        }
    }
}

fn parse_trade(env: Envelope) -> WebCtpResult<TradeEvent> {
    match env.msg {
        Value::String(s) => match s.as_str() {
            "ready" => Ok(TradeEvent::Ready),
            "parse_error" | "processing_error" | "error" => {
                Ok(TradeEvent::Error(env.info.clone()))
            }
            _ => Ok(TradeEvent::Unknown(Value::String(s))),
        },
        Value::Number(num) => {
            let code: i64 = num
                .as_i64()
                .ok_or_else(|| WebCtpError::Protocol("non-integer msg code".into()))?;
            let info = env.info;
            match TradeMsgCode::try_from(code) {
                Ok(TradeMsgCode::Performed) => Ok(TradeEvent::Performed(info)),
                Ok(TradeMsgCode::Error) => Ok(TradeEvent::Error(info.clone())),
                Ok(TradeMsgCode::ErrorNull) => Ok(TradeEvent::ErrorNull(info.clone())),
                Ok(TradeMsgCode::ErrorUnknownValue) => Ok(TradeEvent::ErrorUnknownValue(info.clone())),
                Ok(TradeMsgCode::Connected) => Ok(TradeEvent::FrontConnected(info)),
                Ok(TradeMsgCode::TradingDay) => Ok(TradeEvent::TradingDay {
                    trading_day: parse_trading_day(&info),
                    raw: info,
                }),
                Ok(TradeMsgCode::Disconnected) => Ok(TradeEvent::FrontDisconnected(info)),
                Ok(TradeMsgCode::Authenticate) => Ok(TradeEvent::Authenticate(info)),
                Ok(TradeMsgCode::Login) => Ok(TradeEvent::Login {
                    trading_day: parse_trading_day(&info),
                    raw: info,
                }),
                Ok(TradeMsgCode::Logout) => Ok(TradeEvent::Logout(info)),
                Ok(TradeMsgCode::SettlementInfo) => {
                    let data: SettlementInfo = serde_json::from_value(info)?;
                    Ok(TradeEvent::SettlementInfo(data))
                }
                Ok(TradeMsgCode::SettlementInfoConfirm) => {
                    let data: SettlementInfoConfirm = serde_json::from_value(info)?;
                    Ok(TradeEvent::SettlementInfoConfirm(data))
                }
                Ok(TradeMsgCode::TradingAccount) => {
                    let data: TradingAccount = serde_json::from_value(info)?;
                    Ok(TradeEvent::TradingAccount(data))
                }
                Ok(TradeMsgCode::OrderInsertError) => {
                    let data: OrderInsertError = serde_json::from_value(info)?;
                    Ok(TradeEvent::OrderInsertError(data))
                }
                Ok(TradeMsgCode::OrderInsertReturnError) => {
                    let data: OrderInsertReturnError = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderInsertReturnError(data))
                }
                Ok(TradeMsgCode::OrderInserted) => {
                    let data: OrderInserted = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderInserted(data))
                }
                Ok(TradeMsgCode::OrderTraded) => {
                    let data: OrderTraded = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderTraded(data))
                }
                Ok(TradeMsgCode::QueryOrder) => {
                    let data: QueryOrder = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::QueryOrder(data))
                }
                Ok(TradeMsgCode::OrderDeleteError) => {
                    let data: OrderDeleteError = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderDeleteError(data))
                }
                Ok(TradeMsgCode::OrderDeleteReturnError) => {
                    let data: OrderDeleteReturnError = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderDeleteReturnError(data))
                }
                Ok(TradeMsgCode::OrderDeleted) => {
                    let data: OrderDeleted = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderDeleted(data))
                }
                Ok(TradeMsgCode::QueryInstrument) => {
                    let data: Instrument = serde_json::from_value(info)?;
                    Ok(TradeEvent::QueryInstrument(data))
                }
                Err(_) => Ok(TradeEvent::Unknown(json!({"msg_code": code}))),
            }
        }
        other => Ok(TradeEvent::Unknown(other)),
    }
}

fn parse_trading_day(info: &Value) -> Option<String> {
    match info.get("trading_day") {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}
