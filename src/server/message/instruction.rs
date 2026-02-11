use std::str;

use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct HandshakeInstruction {
    pub token: String,
}

#[derive(Deserialize, Clone, Copy)]
pub enum MarketType {
    Futures,
    FuturesOptions,
    StockCN,
    StockHK,
    StockUS,
}

#[derive(Deserialize, Clone)]
pub struct BacktestRequestInstruction {
    pub market: MarketType,
    pub subject: String,
    pub time_begin: String,
    pub time_end: String,
}

#[derive(Deserialize, Clone)]
pub struct BacktestCancelInstruction {
    pub reference: String,
}

#[derive(Deserialize, Clone)]
pub struct BacktestQueryInstruction {
    pub reference: String,
}

// WebCTP market data instructions
#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataConnectInstruction {
    pub url: String,
    pub broker_id: String,
    pub user_id: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataConnectFrontInstruction {
    pub op_ref: String,
    pub addr: String,
    pub port: u16,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataLoginInstruction {
    pub op_ref: String,
    pub password: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataSubscribeInstruction {
    pub op_ref: String,
    pub instruments: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataUnsubscribeInstruction {
    pub op_ref: String,
    pub instruments: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataTradingDayInstruction {
    pub op_ref: String
}

#[derive(Deserialize, Clone)]
pub struct WebCtpMarketDataDisconnectInstruction {}

// WebCTP trade instructions
#[derive(Deserialize, Clone)]
pub struct WebCtpTradeConnectInstruction {
    pub url: String,
    pub broker_id: String,
    pub investor_id: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeConnectFrontInstruction {
    pub op_ref: String,
    pub addr: String,
    pub port: u16,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeSetInstruction {
    pub op_ref: String,
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeTradingDayInstruction {
    pub op_ref: String
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeAuthInstruction {
    pub op_ref: String,
    pub user_id: String,
    pub app_id: String,
    pub auth_code: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeLoginInstruction {
    pub op_ref: String,
    pub user_id: String,
    pub password: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeLogoutInstruction {
    pub op_ref: String,
    pub user_id: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeQuerySettlementInfoInstruction {
    pub op_ref: String,
    pub trading_day: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeConfirmSettlementInfoInstruction {
    pub op_ref: String
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeQueryTradingAccountInstruction {
    pub op_ref: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeInsertOrderInstruction {
    pub op_ref: String,
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
    pub op_ref: String,
    pub order_sys_id: Option<String>,
    pub exchange_id: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeDeleteOrderInstruction {
    pub op_ref: String,
    pub exchange: String,
    pub instrument: String,
    pub delete_ref: i64,
    pub order_sys_id: String,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeQueryInstrumentInstruction {
    pub op_ref: String,
    pub exchange: Option<String>,
    pub instrument: Option<String>,
    pub exchange_inst_id: Option<String>,
    pub product_id: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct WebCtpTradeDisconnectInstruction {}
