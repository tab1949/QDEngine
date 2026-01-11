use serde::{
    self,
    Serialize
};
use serde_repr::{
    self,
    Serialize_repr
};

#[derive(Serialize_repr)]
#[repr(i64)]
pub enum ReportCode {
    AuthenticateFailed = -2,
    GeneralError = -1,
    Success = 0,
    Handshake = 1,
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