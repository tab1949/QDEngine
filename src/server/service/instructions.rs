use tokio::sync::{mpsc, oneshot};

use crate::adaptor::webctp;
use crate::server::{message, service};

#[derive(Clone)]
pub(crate) struct MarketDataHandle {
    cmd_tx: mpsc::UnboundedSender<MarketDataCmd>,
}

#[derive(Clone)]
pub(crate) struct TradeHandle {
    cmd_tx: mpsc::UnboundedSender<TradeCmd>,
}

enum MarketDataCmd {
    ConnectFront {
        instr: message::instruction::WebCtpMarketDataConnectFrontInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    Login {
        instr: message::instruction::WebCtpMarketDataLoginInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    Subscribe {
        instr: message::instruction::WebCtpMarketDataSubscribeInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    Unsubscribe {
        instr: message::instruction::WebCtpMarketDataUnsubscribeInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    TradingDay {
        instr: message::instruction::WebCtpMarketDataTradingDayInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    Disconnect {
        instr: message::instruction::WebCtpMarketDataDisconnectInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
}

enum TradeCmd {
    ConnectFront {
        instr: message::instruction::WebCtpTradeConnectFrontInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    Set {
        instr: message::instruction::WebCtpTradeSetInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    TradingDay {
        instr: message::instruction::WebCtpTradeTradingDayInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    Auth {
        instr: message::instruction::WebCtpTradeAuthInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    Login {
        instr: message::instruction::WebCtpTradeLoginInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    Logout {
        instr: message::instruction::WebCtpTradeLogoutInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    QuerySettlementInfo {
        instr: message::instruction::WebCtpTradeQuerySettlementInfoInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    ConfirmSettlementInfo {
        instr: message::instruction::WebCtpTradeConfirmSettlementInfoInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    QueryTradingAccount {
        instr: message::instruction::WebCtpTradeQueryTradingAccountInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    InsertOrder {
        instr: message::instruction::WebCtpTradeInsertOrderInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    QueryOrder {
        instr: message::instruction::WebCtpTradeQueryOrderInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    DeleteOrder {
        instr: message::instruction::WebCtpTradeDeleteOrderInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    QueryInstrument {
        instr: message::instruction::WebCtpTradeQueryInstrumentInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
    Disconnect {
        instr: message::instruction::WebCtpTradeDisconnectInstruction,
        resp: oneshot::Sender<webctp::WebCtpResult<()>>,
    },
}

async fn send_webctp_error(client: &mut service::Client, title: &str, err: webctp::WebCtpError) {
    client
        .send_report(
            message::report::ReportCode::GeneralError,
            title,
            format!("{:?}", err),
        )
        .await;
}

fn spawn_market_data_listener(
    md: webctp::MarketDataClient,
    tx: mpsc::UnboundedSender<String>,
) -> MarketDataHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut md = md;
        loop {
            tokio::select! {
                evt = md.next_event() => {
                    match evt {
                        Ok(Some(event)) => {
                            if let Some(text) = serialize_market_event(event) {
                                let _ = tx.send(text);
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            let _ = tx.send(
                                message::generate_report_string(
                                    message::report::ReportCode::GeneralError,
                                    "market data event loop error",
                                    format!("{:?}", e),
                                )
                                .unwrap_or_else(|ser_err| format!("{{\"code\":-1,\"message\":\"serialize error: {:?}\",\"data\":null}}", ser_err)),
                            );
                            break;
                        }
                    }
                }
                maybe_cmd = cmd_rx.recv() => {
                    match maybe_cmd {
                        Some(MarketDataCmd::ConnectFront { instr, resp }) => {
                            let _ = resp.send(md.connect_front(&instr.addr, instr.port).await);
                        }
                        Some(MarketDataCmd::Login { instr, resp }) => {
                            let _ = resp.send(md.login(&instr.password).await);
                        }
                        Some(MarketDataCmd::Subscribe { instr, resp }) => {
                            let _ = resp.send(md.subscribe(&instr.instruments).await);
                        }
                        Some(MarketDataCmd::Unsubscribe { instr, resp }) => {
                            let _ = resp.send(md.unsubscribe(&instr.instruments).await);
                        }
                        Some(MarketDataCmd::TradingDay { resp, .. }) => {
                            let _ = resp.send(md.get_trading_day().await);
                        }
                        Some(MarketDataCmd::Disconnect { resp, .. }) => {
                            let res = md.disconnect().await;
                            let _ = resp.send(res);
                            break;
                        }
                        None => break,
                    }
                }
            }
        }
    });

    MarketDataHandle { cmd_tx }
}

fn spawn_trade_listener(
    trade: webctp::TradeClient,
    tx: mpsc::UnboundedSender<String>,
) -> TradeHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut trade = trade;
        loop {
            tokio::select! {
                evt = trade.next_event() => {
                    match evt {
                        Ok(Some(event)) => {
                            if let Some(text) = serialize_trade_event(event) {
                                let _ = tx.send(text);
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            let _ = tx.send(
                                message::generate_report_string(
                                    message::report::ReportCode::GeneralError,
                                    "trade event loop error",
                                    format!("{:?}", e),
                                )
                                .unwrap_or_else(|ser_err| format!("{{\"code\":-1,\"message\":\"serialize error: {:?}\",\"data\":null}}", ser_err)),
                            );
                            break;
                        }
                    }
                }
                maybe_cmd = cmd_rx.recv() => {
                    match maybe_cmd {
                        Some(TradeCmd::ConnectFront { instr, resp }) => {
                            let _ = resp.send(trade.connect_front(&instr.addr, instr.port).await);
                        }
                        Some(TradeCmd::Set { instr, resp }) => {
                            let _ = resp.send(trade.set(instr.broker_id.clone(), instr.investor_id.clone()).await);
                        }
                        Some(TradeCmd::TradingDay { resp, .. }) => {
                            let _ = resp.send(trade.get_trading_day().await);
                        }
                        Some(TradeCmd::Auth { instr, resp }) => {
                            let _ = resp.send(trade.auth(&instr.user_id, &instr.app_id, &instr.auth_code).await);
                        }
                        Some(TradeCmd::Login { instr, resp }) => {
                            let _ = resp.send(trade.login(&instr.user_id, &instr.password).await);
                        }
                        Some(TradeCmd::Logout { instr, resp }) => {
                            let _ = resp.send(trade.logout(&instr.user_id).await);
                        }
                        Some(TradeCmd::QuerySettlementInfo { instr, resp }) => {
                            let _ = resp.send(trade.query_settlement_info(&instr.trading_day).await);
                        }
                        Some(TradeCmd::ConfirmSettlementInfo { resp, .. }) => {
                            let _ = resp.send(trade.confirm_settlement_info().await);
                        }
                        Some(TradeCmd::QueryTradingAccount { resp, .. }) => {
                            let _ = resp.send(trade.query_trading_account().await);
                        }
                        Some(TradeCmd::InsertOrder { instr, resp }) => {
                            let _ = resp.send(trade.insert_order(&instr.instrument, &instr.exchange, &instr.reference, instr.price, instr.direction, instr.offset, instr.volume, instr.price_type, instr.time_condition).await);
                        }
                        Some(TradeCmd::QueryOrder { instr, resp }) => {
                            let _ = resp.send(trade.query_order(instr.order_sys_id.clone(), instr.exchange_id.clone(), instr.from.clone(), instr.to.clone()).await);
                        }
                        Some(TradeCmd::DeleteOrder { instr, resp }) => {
                            let _ = resp.send(trade.delete_order(&instr.exchange, &instr.instrument, instr.delete_ref, &instr.order_sys_id).await);
                        }
                        Some(TradeCmd::QueryInstrument { instr, resp }) => {
                            let _ = resp.send(trade.query_instrument(instr.exchange.clone(), instr.instrument.clone(), instr.exchange_inst_id.clone(), instr.product_id.clone()).await);
                        }
                        Some(TradeCmd::Disconnect { resp, .. }) => {
                            let res = trade.disconnect().await;
                            let _ = resp.send(res);
                            break;
                        }
                        None => break,
                    }
                }
            }
        }
    });

    TradeHandle { cmd_tx }
}

fn serialize_market_event(event: webctp::MarketDataEvent) -> Option<String> {
    let (event_name, payload) = match event {
        webctp::MarketDataEvent::Ready => ("ready", serde_json::json!({})),
        webctp::MarketDataEvent::Performed(v) => ("performed", v),
        webctp::MarketDataEvent::Error(v) => ("error", v),
        webctp::MarketDataEvent::FrontConnected(v) => ("front_connected", v),
        webctp::MarketDataEvent::FrontDisconnected(v) => ("front_disconnected", v),
        webctp::MarketDataEvent::HeartbeatTimeout(v) => ("heartbeat_timeout", v),
        webctp::MarketDataEvent::Login { trading_day, raw } => (
            "login",
            serde_json::json!({"trading_day": trading_day, "raw": raw}),
        ),
        webctp::MarketDataEvent::Logout(v) => ("logout", v),
        webctp::MarketDataEvent::TradingDay { trading_day, raw } => (
            "trading_day",
            serde_json::json!({"trading_day": trading_day, "raw": raw}),
        ),
        webctp::MarketDataEvent::Subscribe(v) => ("subscribe", v),
        webctp::MarketDataEvent::Unsubscribe(v) => ("unsubscribe", v),
        webctp::MarketDataEvent::MarketData(md) => (
            "market_data",
            serde_json::json!({"debug": format!("{:?}", md)}),
        ),
        webctp::MarketDataEvent::Unknown(v) => ("unknown", v),
    };

    message::generate_report_string(
        message::report::ReportCode::WebCtpMarketDataEvent,
        "webctp market data event",
        message::report::WebCtpEventReport {
            source: "market_data".to_string(),
            event: event_name.to_string(),
            payload,
        },
    )
    .ok()
}

fn serialize_trade_event(event: webctp::TradeEvent) -> Option<String> {
    let (event_name, payload) = match event {
        webctp::TradeEvent::Ready => ("ready", serde_json::json!({})),
        webctp::TradeEvent::Performed(v) => ("performed", v),
        webctp::TradeEvent::Error(v) => ("error", v),
        webctp::TradeEvent::ErrorNull(v) => ("error_null", v),
        webctp::TradeEvent::ErrorUnknownValue(v) => ("error_unknown_value", v),
        webctp::TradeEvent::FrontConnected(v) => ("front_connected", v),
        webctp::TradeEvent::TradingDay { trading_day, raw } => (
            "trading_day",
            serde_json::json!({"trading_day": trading_day, "raw": raw}),
        ),
        webctp::TradeEvent::FrontDisconnected(v) => ("front_disconnected", v),
        webctp::TradeEvent::Authenticate(v) => ("authenticate", v),
        webctp::TradeEvent::Login { trading_day, raw } => (
            "login",
            serde_json::json!({"trading_day": trading_day, "raw": raw}),
        ),
        webctp::TradeEvent::Logout(v) => ("logout", v),
        webctp::TradeEvent::SettlementInfo(s) => (
            "settlement_info",
            serde_json::json!({"debug": format!("{:?}", s)}),
        ),
        webctp::TradeEvent::SettlementInfoConfirm(s) => (
            "settlement_info_confirm",
            serde_json::json!({"debug": format!("{:?}", s)}),
        ),
        webctp::TradeEvent::TradingAccount(a) => (
            "trading_account",
            serde_json::json!({"debug": format!("{:?}", a)}),
        ),
        webctp::TradeEvent::OrderInsertReturnError(ei) => (
            "order_insert_return_error",
            serde_json::json!({"debug": format!("{:?}", ei)}),
        ),
        webctp::TradeEvent::OrderInsertError(ei) => (
            "order_insert_error",
            serde_json::json!({"debug": format!("{:?}", ei)}),
        ),
        webctp::TradeEvent::OrderInserted(o) => (
            "order_inserted",
            serde_json::json!({"debug": format!("{:?}", o)}),
        ),
        webctp::TradeEvent::OrderTraded(o) => (
            "order_traded",
            serde_json::json!({"debug": format!("{:?}", o)}),
        ),
        webctp::TradeEvent::QueryOrder(o) => (
            "query_order",
            serde_json::json!({"debug": format!("{:?}", o)}),
        ),
        webctp::TradeEvent::QueryInstrument(i) => (
            "query_instrument",
            serde_json::json!({"debug": format!("{:?}", i)}),
        ),
        webctp::TradeEvent::OrderDeleteReturnError(ei) => (
            "order_delete_return_error",
            serde_json::json!({"debug": format!("{:?}", ei)}),
        ),
        webctp::TradeEvent::OrderDeleteError(ei) => (
            "order_delete_error",
            serde_json::json!({"debug": format!("{:?}", ei)}),
        ),
        webctp::TradeEvent::OrderDeleted(o) => (
            "order_deleted",
            serde_json::json!({"debug": format!("{:?}", o)}),
        ),
        webctp::TradeEvent::Unknown(v) => ("unknown", v),
    };

    message::generate_report_string(
        message::report::ReportCode::WebCtpTradeEvent,
        "webctp trade event",
        message::report::WebCtpEventReport {
            source: "trade".to_string(),
            event: event_name.to_string(),
            payload,
        },
    )
    .ok()
}

async fn market_data_task_stopped(client: &mut service::Client) {
    client.market_data = None;
    client
        .send_report(
            message::report::ReportCode::GeneralError,
            "market data task stopped",
            (),
        )
        .await;
}

async fn trade_task_stopped(client: &mut service::Client) {
    client.trade = None;
    client
        .send_report(
            message::report::ReportCode::GeneralError,
            "trade task stopped",
            (),
        )
        .await;
}

pub async fn handle_instruction(instruction: &message::Instruction, client: &mut service::Client) {
    match instruction {
        message::Instruction::Handshake(_) => {
            client
                .send_report(
                    message::report::ReportCode::GeneralError,
                    "handshake already received",
                    (),
                )
                .await;
        }
        message::Instruction::TestRequest(_) => {}
        message::Instruction::TestCancel(_) => {}
        message::Instruction::TestQuery(_) => {}
        message::Instruction::WebCtpMarketDataConnect(instr) => {
            let mut md =
                webctp::MarketDataClient::new(instr.broker_id.clone(), instr.user_id.clone());
            match md.connect(&instr.addr, instr.port).await {
                Ok(_) => {
                    let handle = spawn_market_data_listener(md, client.outbound_tx.clone());
                    client.market_data = Some(handle);
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp market data connected",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
                Err(e) => {
                    send_webctp_error(client, "failed to connect webctp market data", e).await;
                }
            }
        }
        message::Instruction::WebCtpMarketDataConnectFront(instr) => {
            if let Some(handle) = client.market_data.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(MarketDataCmd::ConnectFront {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    market_data_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp market data connect_front sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "market data connect_front failed", e).await;
                    }
                    Err(_) => {
                        market_data_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataLogin(instr) => {
            if let Some(handle) = client.market_data.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(MarketDataCmd::Login {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    market_data_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp market data login sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "market data login failed", e).await;
                    }
                    Err(_) => {
                        market_data_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataSubscribe(instr) => {
            if let Some(handle) = client.market_data.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(MarketDataCmd::Subscribe {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    market_data_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp market data subscribe sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "market data subscribe failed", e).await;
                    }
                    Err(_) => {
                        market_data_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataUnsubscribe(instr) => {
            if let Some(handle) = client.market_data.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(MarketDataCmd::Unsubscribe {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    market_data_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp market data unsubscribe sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "market data unsubscribe failed", e).await;
                    }
                    Err(_) => {
                        market_data_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataTradingDay(instr) => {
            if let Some(handle) = client.market_data.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(MarketDataCmd::TradingDay {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    market_data_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp market data get_trading_day sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "market data get_trading_day failed", e).await;
                    }
                    Err(_) => {
                        market_data_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpMarketDataDisconnect(instr) => {
            if let Some(handle) = client.market_data.take() {
                let (resp_tx, resp_rx) = oneshot::channel();
                let _ = handle.cmd_tx.send(MarketDataCmd::Disconnect {
                    instr: instr.clone(),
                    resp: resp_tx,
                });

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp market data disconnected",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "market data disconnect failed", e).await;
                    }
                    Err(_) => {
                        market_data_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "market data client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeConnect(instr) => {
            let mut trade =
                webctp::TradeClient::new(instr.broker_id.clone(), instr.investor_id.clone());
            match trade.connect(&instr.addr, instr.port).await {
                Ok(_) => {
                    let handle = spawn_trade_listener(trade, client.outbound_tx.clone());
                    client.trade = Some(handle);
                    client
                        .send_report(
                            message::report::ReportCode::WebCtpOperationAck,
                            "webctp trade connected",
                            message::report::OperationAck { ok: true },
                        )
                        .await;
                }
                Err(e) => {
                    send_webctp_error(client, "failed to connect webctp trade", e).await;
                }
            }
        }
        message::Instruction::WebCtpTradeConnectFront(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::ConnectFront {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade connect_front sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade connect_front failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeSet(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::Set {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade set sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade set failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeTradingDay(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::TradingDay {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade get_trading_day sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade get_trading_day failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeAuth(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::Auth {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade auth sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade auth failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeLogin(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::Login {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade login sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade login failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeLogout(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::Logout {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade logout sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade logout failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeQuerySettlementInfo(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::QuerySettlementInfo {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade query_settlement_info sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade query_settlement_info failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeConfirmSettlementInfo(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::ConfirmSettlementInfo {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade confirm_settlement_info sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade confirm_settlement_info failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeQueryTradingAccount(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::QueryTradingAccount {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade query_trading_account sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade query_trading_account failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeInsertOrder(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::InsertOrder {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade insert_order sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade insert_order failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeQueryOrder(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::QueryOrder {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade query_order sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade query_order failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeDeleteOrder(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::DeleteOrder {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade delete_order sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade delete_order failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeQueryInstrument(instr) => {
            if let Some(handle) = client.trade.as_ref() {
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(TradeCmd::QueryInstrument {
                        instr: instr.clone(),
                        resp: resp_tx,
                    })
                    .is_err()
                {
                    trade_task_stopped(client).await;
                    return;
                }

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade query_instrument sent",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade query_instrument failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
        message::Instruction::WebCtpTradeDisconnect(instr) => {
            if let Some(handle) = client.trade.take() {
                let (resp_tx, resp_rx) = oneshot::channel();
                let _ = handle.cmd_tx.send(TradeCmd::Disconnect {
                    instr: instr.clone(),
                    resp: resp_tx,
                });

                match resp_rx.await {
                    Ok(Ok(_)) => {
                        client
                            .send_report(
                                message::report::ReportCode::WebCtpOperationAck,
                                "webctp trade disconnected",
                                message::report::OperationAck { ok: true },
                            )
                            .await;
                    }
                    Ok(Err(e)) => {
                        send_webctp_error(client, "trade disconnect failed", e).await;
                    }
                    Err(_) => {
                        trade_task_stopped(client).await;
                    }
                }
            } else {
                client
                    .send_report(
                        message::report::ReportCode::GeneralError,
                        "trade client not connected",
                        (),
                    )
                    .await;
            }
        }
    }
}
