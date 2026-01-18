use serde::{self, Deserialize, Serialize};
pub mod instruction;
pub mod report;

#[derive(Deserialize)]
#[serde(tag = "action", content = "data")]
pub enum Instruction {
    Handshake(instruction::HandshakeInstruction),
    TestRequest(instruction::TestRequestInstruction),
    TestCancel(instruction::TestCancelInstruction),
    TestQuery(instruction::TestQueryInstruction),
    WebCtpMarketDataConnect(instruction::WebCtpMarketDataConnectInstruction),
    WebCtpMarketDataConnectFront(instruction::WebCtpMarketDataConnectFrontInstruction),
    WebCtpMarketDataLogin(instruction::WebCtpMarketDataLoginInstruction),
    WebCtpMarketDataSubscribe(instruction::WebCtpMarketDataSubscribeInstruction),
    WebCtpMarketDataUnsubscribe(instruction::WebCtpMarketDataUnsubscribeInstruction),
    WebCtpMarketDataTradingDay(instruction::WebCtpMarketDataTradingDayInstruction),
    WebCtpMarketDataDisconnect(instruction::WebCtpMarketDataDisconnectInstruction),
    WebCtpTradeConnect(instruction::WebCtpTradeConnectInstruction),
    WebCtpTradeConnectFront(instruction::WebCtpTradeConnectFrontInstruction),
    WebCtpTradeSet(instruction::WebCtpTradeSetInstruction),
    WebCtpTradeTradingDay(instruction::WebCtpTradeTradingDayInstruction),
    WebCtpTradeAuth(instruction::WebCtpTradeAuthInstruction),
    WebCtpTradeLogin(instruction::WebCtpTradeLoginInstruction),
    WebCtpTradeLogout(instruction::WebCtpTradeLogoutInstruction),
    WebCtpTradeQuerySettlementInfo(instruction::WebCtpTradeQuerySettlementInfoInstruction),
    WebCtpTradeConfirmSettlementInfo(instruction::WebCtpTradeConfirmSettlementInfoInstruction),
    WebCtpTradeQueryTradingAccount(instruction::WebCtpTradeQueryTradingAccountInstruction),
    WebCtpTradeInsertOrder(instruction::WebCtpTradeInsertOrderInstruction),
    WebCtpTradeQueryOrder(instruction::WebCtpTradeQueryOrderInstruction),
    WebCtpTradeDeleteOrder(instruction::WebCtpTradeDeleteOrderInstruction),
    WebCtpTradeQueryInstrument(instruction::WebCtpTradeQueryInstrumentInstruction),
    WebCtpTradeDisconnect(instruction::WebCtpTradeDisconnectInstruction),
}

pub fn parse_instruction(raw: &str) -> Result<Instruction, serde_json::Error> {
    serde_json::from_str(raw)
}

pub fn generate_report_string<T: Serialize>(
    code: report::ReportCode,
    message: &str,
    data: T,
) -> Result<String, serde_json::Error> {
    let report = report::Report {
        code: code,
        message: message.to_string(),
        data: data,
    };
    serde_json::to_string(&report)
}
