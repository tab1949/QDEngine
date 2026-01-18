use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::convert::TryFrom;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i64)]
pub enum MdMsgCode {
    Performed = 0,
    Error = 1,
    Connected = 2,
    Disconnected = 3,
    HeartbeatTimeout = 4,
    Login = 5,
    Logout = 6,
    TradingDay = 7,
    Subscribe = 8,
    Unsubscribe = 9,
    MarketData = 10,
}

impl TryFrom<i64> for MdMsgCode {
    type Error = ();

    fn try_from(value: i64) -> Result<Self, ()> {
        match value {
            0 => Ok(MdMsgCode::Performed),
            1 => Ok(MdMsgCode::Error),
            2 => Ok(MdMsgCode::Connected),
            3 => Ok(MdMsgCode::Disconnected),
            4 => Ok(MdMsgCode::HeartbeatTimeout),
            5 => Ok(MdMsgCode::Login),
            6 => Ok(MdMsgCode::Logout),
            7 => Ok(MdMsgCode::TradingDay),
            8 => Ok(MdMsgCode::Subscribe),
            9 => Ok(MdMsgCode::Unsubscribe),
            10 => Ok(MdMsgCode::MarketData),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i64)]
pub enum TradeMsgCode {
    Performed = 0,
    Error = 1,
    ErrorNull = 2,
    ErrorUnknownValue = 3,
    Connected = 4,
    TradingDay = 5,
    Disconnected = 6,
    Authenticate = 7,
    Login = 8,
    Logout = 9,
    SettlementInfo = 10,
    SettlementInfoConfirm = 11,
    TradingAccount = 12,
    OrderInsertError = 13,
    OrderInsertReturnError = 14,
    OrderInserted = 15,
    OrderTraded = 16,
    QueryOrder = 17,
    OrderDeleteError = 18,
    OrderDeleteReturnError = 19,
    OrderDeleted = 20,
    QueryInstrument = 21,
}

impl TryFrom<i64> for TradeMsgCode {
    type Error = ();

    fn try_from(value: i64) -> Result<Self, ()> {
        match value {
            0 => Ok(TradeMsgCode::Performed),
            1 => Ok(TradeMsgCode::Error),
            2 => Ok(TradeMsgCode::ErrorNull),
            3 => Ok(TradeMsgCode::ErrorUnknownValue),
            4 => Ok(TradeMsgCode::Connected),
            5 => Ok(TradeMsgCode::TradingDay),
            6 => Ok(TradeMsgCode::Disconnected),
            7 => Ok(TradeMsgCode::Authenticate),
            8 => Ok(TradeMsgCode::Login),
            9 => Ok(TradeMsgCode::Logout),
            10 => Ok(TradeMsgCode::SettlementInfo),
            11 => Ok(TradeMsgCode::SettlementInfoConfirm),
            12 => Ok(TradeMsgCode::TradingAccount),
            13 => Ok(TradeMsgCode::OrderInsertError),
            14 => Ok(TradeMsgCode::OrderInsertReturnError),
            15 => Ok(TradeMsgCode::OrderInserted),
            16 => Ok(TradeMsgCode::OrderTraded),
            17 => Ok(TradeMsgCode::QueryOrder),
            18 => Ok(TradeMsgCode::OrderDeleteError),
            19 => Ok(TradeMsgCode::OrderDeleteReturnError),
            20 => Ok(TradeMsgCode::OrderDeleted),
            21 => Ok(TradeMsgCode::QueryInstrument),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct MarketData {
    pub trading_day: String,
    pub instrument_id: String,
    pub exchange_id: String,
    pub exchange_inst_id: String,
    pub last_price: f64,
    pub pre_settlement_price: f64,
    pub pre_close_price: f64,
    pub pre_open_interest: f64,
    pub open_price: f64,
    pub highest_price: f64,
    pub lowest_price: f64,
    pub volume: i64,
    pub turnover: f64,
    pub open_interest: f64,
    pub close_price: f64,
    pub settlement_price: f64,
    pub upper_limit_price: f64,
    pub lower_limit_price: f64,
    pub pre_delta: f64,
    pub curr_delta: f64,
    pub update_time: String,
    pub update_millisec: i64,
    pub bp1: f64,
    pub bv1: i64,
    pub ap1: f64,
    pub av1: i64,
    pub bp2: f64,
    pub bv2: i64,
    pub ap2: f64,
    pub av2: i64,
    pub bp3: f64,
    pub bv3: i64,
    pub ap3: f64,
    pub av3: i64,
    pub bp4: f64,
    pub bv4: i64,
    pub ap4: f64,
    pub av4: i64,
    pub bp5: f64,
    pub bv5: i64,
    pub ap5: f64,
    pub av5: i64,
    pub average_price: f64,
    pub action_day: String,
    pub banding_upper_price: f64,
    pub banding_lower_price: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct TradingAccount {
    pub broker_id: Option<String>,
    pub account_id: Option<String>,
    pub pre_mortgage: Option<f64>,
    pub pre_credit: Option<f64>,
    pub pre_deposit: Option<f64>,
    pub pre_balance: Option<f64>,
    pub pre_margin: Option<f64>,
    pub interest_base: Option<f64>,
    pub interest: Option<f64>,
    pub deposit: Option<f64>,
    pub withdraw: Option<f64>,
    pub frozen_margin: Option<f64>,
    pub frozen_cash: Option<f64>,
    pub frozen_commission: Option<f64>,
    pub current_margin: Option<f64>,
    pub cash_in: Option<f64>,
    pub commission: Option<f64>,
    pub close_profit: Option<f64>,
    pub position_profit: Option<f64>,
    pub available: Option<f64>,
    pub withdraw_quota: Option<f64>,
    pub trading_day: Option<String>,
    pub settlement_id: Option<String>,
    pub credit: Option<f64>,
    pub mortgage: Option<f64>,
    pub exchange_margin: Option<f64>,
    pub delivery_margin: Option<f64>,
    pub exchange_delivery_margin: Option<f64>,
    pub reserve_balance: Option<f64>,
    pub currency_id: Option<String>,
    pub pre_fund_mortgage_in: Option<f64>,
    pub pre_fund_mortgage_out: Option<f64>,
    pub fund_mortgage_in: Option<f64>,
    pub fund_mortgage_out: Option<f64>,
    pub fund_mortgage_available: Option<f64>,
    pub mortgageable_fund: Option<f64>,
    pub spec_product_margin: Option<f64>,
    pub spec_product_frozen_margin: Option<f64>,
    pub spec_product_commission: Option<f64>,
    pub spec_product_frozen_commission: Option<f64>,
    pub spec_product_position_profit: Option<f64>,
    pub spec_product_close_profit: Option<f64>,
    pub spec_product_position_profit_by_alg: Option<f64>,
    pub spec_product_exchange_margin: Option<f64>,
    pub biz_type: Option<i64>,
    pub frozen_swap: Option<f64>,
    pub remain_swap: Option<f64>,
    pub option_value: Option<f64>,
    pub req_id: i64,
    pub is_last: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct SettlementInfo {
    pub trading_day: Option<String>,
    pub settlement_id: Option<String>,
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
    pub sequence_no: Option<i64>,
    pub content: Option<String>,
    pub account_id: Option<String>,
    pub currency_id: Option<String>,
    pub req_id: i64,
    pub is_last: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct SettlementInfoConfirm {
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
    pub confirm_date: Option<String>,
    pub confirm_time: Option<String>,
    pub settlement_id: Option<i64>,
    pub account_id: Option<String>,
    pub currency_id: Option<String>,
    pub req_id: i64,
    pub is_last: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct OrderInserted {
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
    pub user_id: Option<String>,
    pub exchange_id: Option<String>,
    pub req_id: Option<i64>,
    pub r#ref: Option<String>,
    pub order_local_id: Option<String>,
    pub order_sys_id: Option<String>,
    pub sequence_no: Option<i64>,
    pub instrument_id: Option<String>,
    pub insert_date: Option<String>,
    pub insert_time: Option<String>,
    pub active_time: Option<String>,
    pub suspend_time: Option<String>,
    pub update_time: Option<String>,
    pub cancel_time: Option<String>,
    pub order_submit_status: Option<i64>,
    pub order_status: Option<i64>,
    pub volume_traded: Option<i64>,
    pub volume_total: Option<i64>,
    pub status_msg: Option<String>,
    pub direction: Option<i64>,
    pub offset: Option<i64>,
    pub price_type: Option<i64>,
    pub hedge: Option<i64>,
    pub time_condition: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct OrderTraded {
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
    pub user_id: Option<String>,
    pub exchange_id: Option<String>,
    pub r#ref: Option<String>,
    pub trade_id: Option<String>,
    pub order_sys_id: Option<String>,
    pub order_local_id: Option<String>,
    pub broker_order_seq: Option<String>,
    pub settlement_id: Option<String>,
    pub volume: Option<i64>,
    pub direction: Option<i64>,
    pub offset: Option<i64>,
    pub hedge: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct QueryOrder {
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
    pub user_id: Option<String>,
    pub exchange_id: Option<String>,
    pub request_id: Option<i64>,
    pub r#ref: Option<String>,
    pub order_local_id: Option<String>,
    pub order_sys_id: Option<String>,
    pub sequence_no: Option<i64>,
    pub instrument_id: Option<String>,
    pub insert_date: Option<String>,
    pub insert_time: Option<String>,
    pub active_time: Option<String>,
    pub suspend_time: Option<String>,
    pub update_time: Option<String>,
    pub cancel_time: Option<String>,
    pub order_submit_status: Option<i64>,
    pub order_status: Option<i64>,
    pub order_memo: Option<String>,
    pub volume_traded: Option<i64>,
    pub volume_total: Option<i64>,
    pub status_msg: Option<String>,
    pub direction: Option<i64>,
    pub offset: Option<i64>,
    pub price_type: Option<i64>,
    pub hedge: Option<i64>,
    pub time_condition: Option<i64>,
    pub req_id: i64,
    pub is_last: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct OrderInsertError {
    pub account_id: Option<String>,
    pub user_id: Option<String>,
    pub investor_id: Option<String>,
    pub broker_id: Option<String>,
    pub client_id: Option<String>,
    pub currency_id: Option<String>,
    pub exchange_id: Option<String>,
    pub gtd_date: Option<String>,
    pub instrument_id: Option<String>,
    pub is_auto_suspend: Option<bool>,
    pub is_swap_order: Option<bool>,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub volume_total_original: Option<i64>,
    pub min_volume: Option<i64>,
    pub order_memo: Option<String>,
    pub order_ref: Option<String>,
    pub request_id: Option<i64>,
    pub session_req_seq: Option<i64>,
    pub direction: Option<i64>,
    pub offset: Option<i64>,
    pub price_type: Option<i64>,
    pub hedge: Option<i64>,
    pub time_condition: Option<i64>,
    pub req_id: Option<i64>,
    pub is_last: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct OrderInsertReturnError {
    pub account_id: Option<String>,
    pub user_id: Option<String>,
    pub investor_id: Option<String>,
    pub broker_id: Option<String>,
    pub client_id: Option<String>,
    pub currency_id: Option<String>,
    pub exchange_id: Option<String>,
    pub gtd_date: Option<String>,
    pub instrument_id: Option<String>,
    pub is_auto_suspend: Option<bool>,
    pub is_swap_order: Option<bool>,
    pub limit_price: Option<f64>,
    pub stop_price: Option<f64>,
    pub volume_total_original: Option<i64>,
    pub min_volume: Option<i64>,
    pub order_memo: Option<String>,
    pub order_ref: Option<String>,
    pub request_id: Option<i64>,
    pub session_req_seq: Option<i64>,
    pub direction: Option<i64>,
    pub offset: Option<i64>,
    pub price_type: Option<i64>,
    pub hedge: Option<i64>,
    pub time_condition: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct OrderDeleteError {
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
    pub order_action_ref: Option<i64>,
    pub order_ref: Option<String>,
    pub request_id: Option<i64>,
    pub front_id: Option<i64>,
    pub session_id: Option<i64>,
    pub exchange_id: Option<String>,
    pub order_sys_id: Option<String>,
    pub action_flag: Option<String>,
    pub limit_price: Option<f64>,
    pub volume_change: Option<i64>,
    pub user_id: Option<String>,
    pub invest_unit_id: Option<String>,
    pub mac_address: Option<String>,
    pub instrument_id: Option<String>,
    pub ip_address: Option<String>,
    pub order_memo: Option<String>,
    pub session_req_seq: Option<i64>,
    pub req_id: Option<i64>,
    pub is_last: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct OrderDeleteReturnError {
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
    pub order_action_ref: Option<i64>,
    pub order_ref: Option<String>,
    pub request_id: Option<i64>,
    pub front_id: Option<i64>,
    pub session_id: Option<i64>,
    pub exchange_id: Option<String>,
    pub order_sys_id: Option<String>,
    pub action_flag: Option<String>,
    pub limit_price: Option<f64>,
    pub volume_change: Option<i64>,
    pub action_date: Option<String>,
    pub action_time: Option<String>,
    pub trader_id: Option<String>,
    pub install_id: Option<i64>,
    pub order_local_id: Option<String>,
    pub action_local_id: Option<String>,
    pub participant_id: Option<String>,
    pub client_id: Option<String>,
    pub business_unit: Option<String>,
    pub order_action_status: Option<String>,
    pub user_id: Option<String>,
    pub status_msg: Option<String>,
    pub branch_id: Option<String>,
    pub invest_unit_id: Option<String>,
    pub mac_address: Option<String>,
    pub instrument_id: Option<String>,
    pub ip_address: Option<String>,
    pub order_memo: Option<String>,
    pub session_req_seq: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(default)]
pub struct OrderDeleted {
    pub broker_id: Option<String>,
    pub investor_id: Option<String>,
    pub user_id: Option<String>,
    pub exchange_id: Option<String>,
    pub req_id: Option<i64>,
    pub r#ref: Option<String>,
    pub order_local_id: Option<String>,
    pub order_sys_id: Option<String>,
    pub sequence_no: Option<i64>,
    pub instrument_id: Option<String>,
    pub insert_date: Option<String>,
    pub insert_time: Option<String>,
    pub active_time: Option<String>,
    pub suspend_time: Option<String>,
    pub update_time: Option<String>,
    pub cancel_time: Option<String>,
    pub order_submit_status: Option<i64>,
    pub order_status: Option<i64>,
    pub volume_traded: Option<i64>,
    pub volume_total: Option<i64>,
    pub status_msg: Option<String>,
    pub direction: Option<i64>,
    pub offset: Option<i64>,
    pub price_type: Option<i64>,
    pub hedge: Option<i64>,
    pub time_condition: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct Instrument {
    pub req_id: i64,
    pub is_last: bool,
    pub exchange_id: String,
    pub instrument_name: String,
    pub product_class: String,
    pub delivery_year: i64,
    pub delivery_month: i64,
    pub max_market_order_volume: i64,
    pub min_market_order_volume: i64,
    pub max_limit_order_volume: i64,
    pub min_limit_order_volume: i64,
    pub volume_multiple: i64,
    pub price_tick: f64,
    pub create_date: String,
    pub open_date: String,
    pub expire_date: String,
    pub start_deliv_date: String,
    pub end_deliv_date: String,
    pub inst_life_phase: String,
    pub is_trading: i64,
    pub position_type: String,
    pub position_date_type: String,
    pub long_margin_ratio: f64,
    pub short_margin_ratio: f64,
    pub max_margin_side_algorithm: String,
    pub strike_price: f64,
    pub options_type: String,
    pub underlying_multiple: f64,
    pub combination_type: String,
    pub instrument_id: String,
    pub exchange_inst_id: String,
    pub product_id: String,
    pub underlying_instr_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Envelope {
    pub msg: Value,
    pub err: Value,
    pub info: Value,
}
