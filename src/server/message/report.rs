use serde::{self, Serialize};
use serde_json::Value;
use serde_repr::{self, Serialize_repr};

#[derive(Serialize_repr)]
#[repr(i64)]
pub enum ReportCode {
    AuthenticateFailed = -2,
    GeneralError = -1,
    Success = 0,
    Handshake = 1,
    WebCtpMarketDataEvent = 2,
    WebCtpTradeEvent = 3,
    WebCtpOperationAck = 4,
}

#[derive(Serialize)]
pub struct Report<T> {
    pub code: ReportCode,
    pub message: String,
    pub data: T,
}

#[derive(Serialize)]
pub struct HandshakeData {
    pub token: String,
}

#[derive(Serialize)]
pub struct AuthenticateFailedInfo {
    pub reason: String,
}

#[derive(Serialize)]
pub struct OperationAck {
    pub ok: bool,
}

#[derive(Serialize)]
pub struct WebCtpEventReportData {
    pub source: String,
    pub event: String,
    pub payload: Value,
}

#[derive(Serialize)]
pub struct ErrorReport {
    pub r#type: String,
    pub info: String,
}