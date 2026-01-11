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

#[derive(Deserialize)]
pub struct ClientAuthenticateInstruction {
    app_id: String,
    auth_code: String,
}

/**
 * This login instruction will cause both market data and trade sessions to login.
 */
#[derive(Deserialize)]
pub struct LoginInstruction {
    account: String,
    password: String,
}

#[derive(Deserialize)]
pub struct LogoutInstruction {
    account: String
}

#[derive(Deserialize)]
pub struct OrderInsertInstruction {
}

#[derive(Deserialize)]
pub struct OrderCancelInstruction {
}

#[derive(Deserialize)]
pub struct OrderQueryInstruction {
}

#[derive(Deserialize)]
pub struct PositionQueryInstruction {
}

#[derive(Deserialize)]
pub struct AccountQueryInstruction {
}

#[derive(Deserialize)]
pub struct InstrumentQueryInstruction {
}

#[derive(Deserialize)]
pub struct MarketDataSubscribeInstruction {
}

#[derive(Deserialize)]
pub struct MarketDataUnsubscribeInstruction {
}