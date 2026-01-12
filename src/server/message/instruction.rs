use std::str;

use serde::{
    Deserialize, 
};

#[derive(Deserialize)]
pub struct HandshakeInstruction {
    pub token: String,
}

#[derive(Deserialize)]
enum MarketType {
    Futures, 
    FuturesOptions, 
    StockCN,
    StockHK,
    StockUS
}

#[derive(Deserialize)]
pub struct TestRequestInstruction {
    market:     MarketType,
    time_begin: String,
    time_end:   String,
}

#[derive(Deserialize)]
pub struct TestCancelInstruction {
    reference: u64,
}

#[derive(Deserialize)]
pub struct TestQueryInstruction {
    reference: u64,
}

// WebCTP market data instructions
#[derive(Deserialize)]
pub struct WebCtpMarketDataConnectInstruction {
    pub addr: String,
    pub port: u16,
    pub broker_id: String,
    pub user_id: String,
}

#[derive(Deserialize)]
pub struct WebCtpMarketDataConnectFrontInstruction {
    pub addr: String,
    pub port: u16,
}

#[derive(Deserialize)]
pub struct WebCtpMarketDataLoginInstruction {
    pub password: String,
}

#[derive(Deserialize)]
pub struct WebCtpMarketDataSubscribeInstruction {
    pub instruments: Vec<String>,
}

#[derive(Deserialize)]
pub struct WebCtpMarketDataUnsubscribeInstruction {
    pub instruments: Vec<String>,
}

#[derive(Deserialize)]
pub struct WebCtpMarketDataTradingDayInstruction {}

#[derive(Deserialize)]
pub struct WebCtpMarketDataDisconnectInstruction {}

// WebCTP trade instructions
#[derive(Deserialize)]
pub struct WebCtpTradeConnectInstruction {
    pub addr: String,
    pub port: u16,
    pub broker_id: String,
    pub investor_id: String,
}

#[derive(Deserialize)]
pub struct WebCtpTradeConnectFrontInstruction {
    pub addr: String,
    pub port: u16,
}

#[derive(Deserialize)]
pub struct WebCtpTradeSetInstruction {
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
}

#[derive(Deserialize)]
pub struct WebCtpTradeTradingDayInstruction {}

#[derive(Deserialize)]
pub struct WebCtpTradeAuthInstruction {
    pub user_id: String,
    pub app_id: String,
    pub auth_code: String,
}

#[derive(Deserialize)]
pub struct WebCtpTradeLoginInstruction {
    pub user_id: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct WebCtpTradeLogoutInstruction {
    pub user_id: String,
}

#[derive(Deserialize)]
pub struct WebCtpTradeQuerySettlementInfoInstruction {
    pub trading_day: String,
}

#[derive(Deserialize)]
pub struct WebCtpTradeConfirmSettlementInfoInstruction {}

#[derive(Deserialize)]
pub struct WebCtpTradeQueryTradingAccountInstruction {}

#[derive(Deserialize)]
pub struct WebCtpTradeInsertOrderInstruction {
    pub instrument: String,
    pub exchange: String,
    pub reference: String,
    pub price: f64,
    pub direction: i64,
    pub offset: i64,
    pub volume: i64,
    pub price_type: i64,
    pub time_condition: i64,
}

#[derive(Deserialize)]
pub struct WebCtpTradeQueryOrderInstruction {
    pub order_sys_id: Option<String>,
    pub exchange_id: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Deserialize)]
pub struct WebCtpTradeDeleteOrderInstruction {
    pub exchange: String,
    pub instrument: String,
    pub delete_ref: i64,
    pub order_sys_id: String,
}

#[derive(Deserialize)]
pub struct WebCtpTradeQueryInstrumentInstruction {
    pub exchange: Option<String>,
    pub instrument: Option<String>,
    pub exchange_inst_id: Option<String>,
    pub product_id: Option<String>,
}

#[derive(Deserialize)]
pub struct WebCtpTradeDisconnectInstruction {}