use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use websocket_lite::{AsyncNetworkStream, ClientBuilder, Message as WsMessage};

type Socket =
    websocket_lite::AsyncClient<Box<dyn AsyncNetworkStream + Sync + Send + Unpin + 'static>>;

use super::message::{
    Envelope, Instrument, OrderDeleteError, OrderDeleteReturnError, OrderDeleted, OrderInsertError,
    OrderInsertReturnError, OrderInserted, OrderTraded, QueryOrder, SettlementInfo,
    SettlementInfoConfirm, TradeMsgCode, TradingAccount, TradeLogin, TradeLogout, TradeAuthenticate, TradeTradingDay,
};
use super::{WebCtpError, WebCtpResult};

#[derive(Debug, Clone)]
pub enum TradeEvent {
    Ready {
        err: Value,
        info: Value,
    },
    Performed {
        err: Value,
        info: Value,
    },
    Error {
        err: Value,
    },
    ErrorNull {
        err: Value,
    },
    ErrorUnknownValue {
        err: Value,
        info: Value,
    },
    FrontConnected {
        err: Value,
        info: Value,
    },
    TradingDay {
        err: Value,
        info: TradeTradingDay,
    },
    FrontDisconnected {
        err: Value,
        info: Value,
    },
    Authenticate {
        err: Value,
        info: TradeAuthenticate,
    },
    Login {
        err: Value,
        info: TradeLogin,
    },
    Logout {
        err: Value,
        info: TradeLogout,
    },
    SettlementInfo {
        err: Value,
        info: SettlementInfo,
    },
    SettlementInfoConfirm {
        err: Value,
        info: SettlementInfoConfirm,
    },
    TradingAccount {
        err: Value,
        info: TradingAccount,
    },
    OrderInsertReturnError {
        err: Value,
        info: OrderInsertReturnError,
    },
    OrderInsertError {
        err: Value,
        info: OrderInsertError,
    },
    OrderInserted {
        err: Value,
        info: OrderInserted,
    },
    OrderTraded {
        err: Value,
        info: OrderTraded,
    },
    QueryOrder {
        err: Value,
        info: QueryOrder,
    },
    QueryInstrument {
        err: Value,
        info: Instrument,
    },
    OrderDeleteReturnError {
        err: Value,
        info: OrderDeleteReturnError,
    },
    OrderDeleteError {
        err: Value,
        info: OrderDeleteError,
    },
    OrderDeleted {
        err: Value,
        info: OrderDeleted,
    },
    Unknown {
        err: Value,
        raw: Value,
    },
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

    pub async fn connect(&mut self, url: &str) -> WebCtpResult<()> {
        let client = ClientBuilder::new(url)
            .map_err(|e| WebCtpError::Protocol(e.to_string()))?
            .async_connect()
            .await?;
        self.ws = Some(client);
        Ok(())
    }

    pub async fn connect_front(&mut self, op_ref: &str, addr: &str, port: u16) -> WebCtpResult<()> {
        self.send_op(
            "connect", 
            json!({
                "op_ref": op_ref, 
                "addr": addr, 
                "port": port.to_string()
            })
        )
        .await
    }

    pub async fn set(
        &mut self,
        op_ref: &str,
        broker_id: Option<String>,
        investor_id: Option<String>,
    ) -> WebCtpResult<()> {
        if let Some(b) = broker_id.clone() {
            self.broker_id = b;
        }
        if let Some(i) = investor_id.clone() {
            self.investor_id = i;
        }
        let mut data = serde_json::Map::new();
        data.insert("op_ref".into(), json!(op_ref));
        if let Some(b) = broker_id {
            data.insert("broker_id".into(), json!(b));
        }
        if let Some(i) = investor_id {
            data.insert("investor_id".into(), json!(i));
        }
        self.send_op("set", Value::Object(data)).await
    }

    pub async fn get_trading_day(&mut self, op_ref: &str) -> WebCtpResult<()> {
        self.send_op(
            "get_trading_day", 
            json!({
                "op_ref": op_ref
            })
        )
        .await
    }

    pub async fn auth(&mut self, op_ref: &str, user_id: &str, app_id: &str, auth_code: &str) -> WebCtpResult<()> {
        self.send_op(
            "auth",
            json!({
                "op_ref": op_ref,
                "user_id": user_id, 
                "app_id": app_id, 
                "auth_code": auth_code
            }),
        )
        .await
    }

    pub async fn login(&mut self, op_ref: &str, user_id: &str, password: &str) -> WebCtpResult<()> {
        self.send_op(
            "login", 
            json!({
                "op_ref": op_ref, 
                "user_id": user_id, 
                "password": password
            })
        )
        .await
    }

    pub async fn logout(&mut self, op_ref: &str, user_id: &str) -> WebCtpResult<()> {
        self.send_op(
            "logout", 
            json!({
                "op_ref": op_ref, 
                "user_id": user_id
            })
        )
        .await
    }

    pub async fn query_settlement_info(&mut self, op_ref: &str, trading_day: &str) -> WebCtpResult<()> {
        self.send_op(
            "query_settlement_info", 
            json!({
                "op_ref": op_ref, 
                "trading_day": trading_day
            })
        )
        .await
    }

    pub async fn confirm_settlement_info(&mut self, op_ref: &str) -> WebCtpResult<()> {
        self.send_op(
            "confirm_settlement_info", 
            json!({
                "op_ref": op_ref
            })
        )
        .await
    }

    pub async fn query_trading_account(&mut self, op_ref: &str) -> WebCtpResult<()> {
        self.send_op(
            "query_trading_account", 
            json!({
                "op_ref": op_ref
            })
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn insert_order(
        &mut self,
        op_ref: &str,
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
                "op_ref": op_ref,
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
        op_ref: &str,
        order_sys_id: Option<String>,
        exchange_id: Option<String>,
        from: Option<String>,
        to: Option<String>,
    ) -> WebCtpResult<()> {
        let mut data = serde_json::Map::new();
        data.insert("op_ref".into(), json!(op_ref));
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

    pub async fn delete_order(
        &mut self,
        op_ref: &str,
        exchange: &str,
        instrument: &str,
        delete_ref: i64,
        order_sys_id: &str,
    ) -> WebCtpResult<()> {
        self.send_op(
            "delete_order",
            json!({
                "op_ref": op_ref,
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
        op_ref: &str,
        exchange: Option<String>,
        instrument: Option<String>,
        exchange_inst_id: Option<String>,
        product_id: Option<String>,
    ) -> WebCtpResult<()> {
        let mut data = serde_json::Map::new();
        data.insert("op_ref".into(), json!(op_ref));
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
                    Ok(Some(TradeEvent::Unknown {
                        err: json!({"reason": "binary_message"}),
                        raw: json!({"binary": true}),
                    }))
                }
            }
            Some(Err(e)) => Err(WebCtpError::WebSocket(e)),
            None => Ok(None),
        }
    }
}

fn parse_trade(env: Envelope) -> WebCtpResult<TradeEvent> {
    let Envelope { msg, err, info } = env;

    match msg {
        Value::String(s) => match s.as_str() {
            "ready" => Ok(TradeEvent::Ready { err, info }),
            "parse_error" | "processing_error" | "error" => Ok(TradeEvent::Error { err }),
            _ => Ok(TradeEvent::Unknown {
                err,
                raw: Value::String(s),
            }),
        },
        Value::Number(num) => {
            let code: i64 = num
                .as_i64()
                .ok_or_else(|| WebCtpError::Protocol("non-integer msg code".into()))?;
            match TradeMsgCode::try_from(code) {
                Ok(TradeMsgCode::Performed) => Ok(TradeEvent::Performed { err, info }),
                Ok(TradeMsgCode::Error) => Ok(TradeEvent::Error { err }),
                Ok(TradeMsgCode::ErrorNull) => Ok(TradeEvent::ErrorNull { err }),
                Ok(TradeMsgCode::ErrorUnknownValue) => Ok(TradeEvent::ErrorUnknownValue { err, info }),
                Ok(TradeMsgCode::Connected) => Ok(TradeEvent::FrontConnected { err, info }),
                Ok(TradeMsgCode::TradingDay) => {
                    let info: TradeTradingDay = serde_json::from_value(info)?;
                    Ok(TradeEvent::TradingDay { err, info })
                }
                Ok(TradeMsgCode::Disconnected) => Ok(TradeEvent::FrontDisconnected { err, info }),
                Ok(TradeMsgCode::Authenticate) => {
                    let info: TradeAuthenticate = serde_json::from_value(info)?;
                    Ok(TradeEvent::Authenticate { err, info })
                }
                Ok(TradeMsgCode::Login) => {
                    let info: TradeLogin = serde_json::from_value(info)?;
                    Ok(TradeEvent::Login { err, info })
                }
                Ok(TradeMsgCode::Logout) => {
                    let info: TradeLogout = serde_json::from_value(info)?;
                    Ok(TradeEvent::Logout { err, info })
                }
                Ok(TradeMsgCode::SettlementInfo) => {
                    let info: SettlementInfo = serde_json::from_value(info)?;
                    Ok(TradeEvent::SettlementInfo { err, info })
                }
                Ok(TradeMsgCode::SettlementInfoConfirm) => {
                    let info: SettlementInfoConfirm = serde_json::from_value(info)?;
                    Ok(TradeEvent::SettlementInfoConfirm { err, info })
                }
                Ok(TradeMsgCode::TradingAccount) => {
                    let info: TradingAccount = serde_json::from_value(info)?;
                    Ok(TradeEvent::TradingAccount { err, info })
                }
                Ok(TradeMsgCode::OrderInsertError) => {
                    let info: OrderInsertError = serde_json::from_value(info)?;
                    Ok(TradeEvent::OrderInsertError { err, info })
                }
                Ok(TradeMsgCode::OrderInsertReturnError) => {
                    let info: OrderInsertReturnError = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderInsertReturnError { err, info })
                }
                Ok(TradeMsgCode::OrderInserted) => {
                    let info: OrderInserted = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderInserted { err, info })
                }
                Ok(TradeMsgCode::OrderTraded) => {
                    let info: OrderTraded = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderTraded { err, info })
                }
                Ok(TradeMsgCode::QueryOrder) => {
                    let info: QueryOrder = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::QueryOrder { err, info })
                }
                Ok(TradeMsgCode::OrderDeleteError) => {
                    let info: OrderDeleteError = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderDeleteError { err, info })
                }
                Ok(TradeMsgCode::OrderDeleteReturnError) => {
                    let info: OrderDeleteReturnError = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderDeleteReturnError { err, info })
                }
                Ok(TradeMsgCode::OrderDeleted) => {
                    let info: OrderDeleted = serde_json::from_value(info.clone())?;
                    Ok(TradeEvent::OrderDeleted { err, info })
                }
                Ok(TradeMsgCode::QueryInstrument) => {
                    let info: Instrument = serde_json::from_value(info)?;
                    Ok(TradeEvent::QueryInstrument { err, info })
                }
                Err(_) => Ok(TradeEvent::Unknown {
                    err,
                    raw: json!({"msg_code": code}),
                }),
            }
        }
        other => Ok(TradeEvent::Unknown { err, raw: other }),
    }
}
