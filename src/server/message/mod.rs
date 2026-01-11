use serde::{
    self, 
    Deserialize, Serialize
};
pub mod instruction;
pub mod report;

#[derive(Deserialize)]
#[serde(tag = "action", content = "data")]
pub enum Instruction {
    Handshake(instruction::HandshakeInstruction),
    TestRequest(instruction::TestRequestInstruction),
    TestCancel(instruction::TestCancelInstruction),
    TestQuery(instruction::TestQueryInstruction),
    ClientAuthenticate(instruction::ClientAuthenticateInstruction),
    Login(instruction::LoginInstruction),
    Logout(instruction::LogoutInstruction),
    OrderInsert(instruction::OrderInsertInstruction),
    OrderCancel(instruction::OrderCancelInstruction),
    OrderQuery(instruction::OrderQueryInstruction),
    PositionQuery(instruction::PositionQueryInstruction),
    AccountQuery(instruction::AccountQueryInstruction),
    InstrumentQuery(instruction::InstrumentQueryInstruction),
    MarketDataSubscribe(instruction::MarketDataSubscribeInstruction),
    MarketDataUnsubscribe(instruction::MarketDataUnsubscribeInstruction),
}

pub fn parse_instruction(raw: &str) -> Result<Instruction, serde_json::Error> {
    serde_json::from_str(raw)
}

pub fn generate_report_string<T: Serialize>(code: report::ReportCode, message: &str, data: T) -> Result<String, serde_json::Error> {
    let report = report::Report {
        code: code,
        message: message.to_string(),
        data: data,
    };
    serde_json::to_string(&report)
}