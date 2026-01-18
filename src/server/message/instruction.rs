use std::str;

use serde::{
    Deserialize, 
};

#[derive(Deserialize, Clone)]
pub struct HandshakeInstruction {
    pub token: String,
}

#[derive(Deserialize, Clone, Copy)]
enum MarketType {
    Futures, 
    FuturesOptions, 
    StockCN,
    StockHK,
    StockUS
}

#[derive(Deserialize, Clone)]
pub struct TestRequestInstruction {
    market:     MarketType,
    time_begin: String,
    time_end:   String,
}

#[derive(Deserialize, Clone)]
pub struct TestCancelInstruction {
    reference: u64,
}

#[derive(Deserialize, Clone)]
pub struct TestQueryInstruction {
    reference: u64,
}

// WebCTP market data instructions
#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataConnectInstruction {
    pub addr: String,
    pub port: u16,
    pub broker_id: String,
    pub user_id: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataConnectFrontInstruction {
    pub addr: String,
    pub port: u16,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataLoginInstruction {
    pub password: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataSubscribeInstruction {
    pub instruments: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataUnsubscribeInstruction {
    pub instruments: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataTradingDayInstruction {}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataDisconnectInstruction {}

// WebCTP trade instructions
#[derive(Deserialize, Clone)]
pub struct WebCtpTradeConnectInstruction {
    pub addr: String,
    pub port: u16,
    pub broker_id: String,
    pub investor_id: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeConnectFrontInstruction {
    pub addr: String,
    pub port: u16,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeSetInstruction {
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeTradingDayInstruction {}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeAuthInstruction {
    pub user_id: String,
    pub app_id: String,
    pub auth_code: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeLoginInstruction {
    pub user_id: String,
    pub password: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeLogoutInstruction {
    pub user_id: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeQuerySettlementInfoInstruction {
    pub trading_day: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeConfirmSettlementInfoInstruction {}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeQueryTradingAccountInstruction {}

#[derive(Deserialize, Clone)]
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

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeQueryOrderInstruction {
    pub order_sys_id: Option<String>,
    pub exchange_id: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeDeleteOrderInstruction {
    pub exchange: String,
    pub instrument: String,
    pub delete_ref: i64,
    pub order_sys_id: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeQueryInstrumentInstruction {
    pub exchange: Option<String>,
    pub instrument: Option<String>,
    pub exchange_inst_id: Option<String>,
    pub product_id: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeDisconnectInstruction {}
