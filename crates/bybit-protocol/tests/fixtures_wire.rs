//! The recorded and documented answers of the exchange are read by the models, with the digits intact.

use brokapi_bybit_protocol::http::models::{
    CancelAllResult, Envelope, ExecutionRecord, InstrumentInfo, Kline, KlineResult,
    OrderBookResult, OrderRecord, OrderRef, Page, PositionRecord, PublicTradeRecord, ServerTime,
    Ticker, WalletRecord,
};
use brokapi_bybit_protocol::ws::models::{DataKind, WsIncoming, parse_incoming};

fn rest<T: serde::de::DeserializeOwned>(name: &str) -> Envelope<T> {
    let text = std::fs::read_to_string(format!(
        "{}/tests/fixtures/rest/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap();
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{name}: {e}"))
}

fn ws(name: &str) -> String {
    std::fs::read_to_string(format!(
        "{}/tests/fixtures/ws/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

#[test]
fn the_server_time_is_read() {
    let t: ServerTime = rest::<ServerTime>("server_time.json").result.unwrap();
    assert!(t.time_second > 1_700_000_000);
    assert_eq!(t.time_nano / 1_000_000_000, t.time_second);
}

#[test]
fn an_instrument_keeps_its_steps_as_written() {
    let page: Page<InstrumentInfo> = rest("instrument_btcusdt.json").result.unwrap();
    let i = &page.list[0];
    assert_eq!(i.symbol, "BTCUSDT");
    assert_eq!(i.contract_type, "LinearPerpetual");
    assert_eq!(i.price_filter.tick_size.to_string(), "0.10");
    assert_eq!(i.lot_size_filter.qty_step.to_string(), "0.001");
    assert_eq!(i.lot_size_filter.min_order_qty.to_string(), "0.001");
    assert_eq!(
        i.lot_size_filter.min_notional_value.unwrap().to_string(),
        "5"
    );
    assert_eq!(i.settle_coin, "USDT");
    let many: Page<InstrumentInfo> = rest("instruments_page.json").result.unwrap();
    assert_eq!(many.list.len(), 2);
}

#[test]
fn an_unknown_symbol_is_a_refusal_with_a_code() {
    let e: Envelope<serde_json::Value> = rest("instrument_unknown.json");
    assert_eq!(e.ret_code, 10001);
    assert!(e.ret_msg.contains("symbol invalid"));
    let e: Envelope<serde_json::Value> = rest("order_book_unknown.json");
    assert_eq!(e.ret_code, 10001);
}

#[test]
fn a_book_ticker_trades_and_candles_are_read() {
    let b: OrderBookResult = rest("order_book.json").result.unwrap();
    assert_eq!((b.bids.len(), b.asks.len()), (5, 5));
    assert_eq!(b.bids[0].0.to_string(), "81695.20");
    assert_eq!(b.asks[0].1.to_string(), "7.638");
    assert!(
        b.bids[0].0 < b.asks[0].0,
        "the best bid is below the best ask"
    );
    assert!(
        b.bids[0].0 > b.bids[1].0 && b.asks[0].0 < b.asks[1].0,
        "best first on both sides"
    );
    let t: Page<Ticker> = rest("ticker.json").result.unwrap();
    assert_eq!(t.list[0].bid1_price.unwrap().to_string(), "81695.20");
    assert_eq!(t.list[0].ask1_size.unwrap().to_string(), "6.499");
    let trades: Page<PublicTradeRecord> = rest("recent_trade.json").result.unwrap();
    assert_eq!(trades.list[0].side, "Buy");
    assert_eq!(trades.list[0].price.to_string(), "81695.30");
    let k: KlineResult = rest("kline.json").result.unwrap();
    let rows: Vec<Kline> = k.list.iter().filter_map(|r| Kline::from_row(r)).collect();
    assert_eq!(rows.len(), 3);
    assert!(
        rows[0].start_ms > rows[1].start_ms,
        "the exchange lists the newest first"
    );
}

#[test]
fn the_documented_private_answers_are_read() {
    let orders: Page<OrderRecord> = rest("doc_open_orders.json").result.unwrap();
    let o = &orders.list[0];
    assert_eq!(
        (o.order_status.as_str(), o.order_link_id.as_str()),
        ("New", "test-000005")
    );
    assert_eq!(o.price.unwrap().to_string(), "1600.00");
    assert_eq!(o.avg_price.unwrap().to_string(), "0");
    assert_eq!(o.trigger_price.unwrap().to_string(), "0.00");
    let hist: Page<OrderRecord> = rest("doc_order_history.json").result.unwrap();
    assert!(!hist.list.is_empty());
    let ex: Page<ExecutionRecord> = rest("doc_executions.json").result.unwrap();
    assert_eq!(ex.list[0].exec_price.to_string(), "1190.15");
    assert_eq!(ex.list[0].exec_fee.to_string(), "0.071409");
    assert_eq!(ex.list[0].exec_type, "Trade");
    let pos: Page<PositionRecord> = rest("doc_positions.json").result.unwrap();
    assert_eq!(
        (pos.list[0].side.as_str(), pos.list[0].size.to_string()),
        ("Sell", "300".to_string())
    );
    assert_eq!(pos.list[0].avg_price.unwrap().to_string(), "27464.50441675");
    #[derive(serde::Deserialize)]
    struct W {
        list: Vec<WalletRecord>,
    }
    let w: W = rest("doc_wallet_balance.json").result.unwrap();
    assert_eq!(w.list[0].coin[0].coin, "BTC");
    let created: OrderRef = rest("doc_create_order.json").result.unwrap();
    assert!(!created.order_id.is_empty());
    let all: CancelAllResult = rest("doc_cancel_all.json").result.unwrap();
    assert_eq!(all.list.len(), 2);
}

#[test]
fn the_public_frames_are_told_apart() {
    let snap = parse_incoming(&ws("orderbook50_snapshot.json"));
    match snap {
        WsIncoming::Data {
            topic,
            kind,
            data,
            cts,
            ..
        } => {
            assert_eq!(topic, "orderbook.50.BTCUSDT");
            assert_eq!(kind, DataKind::Snapshot);
            assert!(cts > 0);
            assert_eq!(data["b"].as_array().unwrap().len(), 10);
        }
        other => panic!("{other:?}"),
    }
    let deltas: Vec<serde_json::Value> =
        serde_json::from_str(&ws("orderbook50_deltas.json")).unwrap();
    let us: Vec<i64> = deltas
        .iter()
        .map(|d| d["data"]["u"].as_i64().unwrap())
        .collect();
    assert!(
        us.windows(2).all(|w| w[1] == w[0] + 1),
        "the update ids of the deltas follow one another: {us:?}"
    );
    assert!(matches!(
        parse_incoming(&deltas[0].to_string()),
        WsIncoming::Data {
            kind: DataKind::Delta,
            ..
        }
    ));
    assert!(matches!(
        parse_incoming(&ws("public_trade.json")),
        WsIncoming::Data { .. }
    ));
    assert!(matches!(
        parse_incoming(&ws("kline.json")),
        WsIncoming::Data { .. }
    ));
    assert!(matches!(
        parse_incoming(&ws("orderbook1_snapshot.json")),
        WsIncoming::Data {
            kind: DataKind::Snapshot,
            ..
        }
    ));
}

#[test]
fn the_answers_to_requests_are_told_apart() {
    assert!(matches!(
        parse_incoming(&ws("ack_subscribe_ok.json")),
        WsIncoming::Ack {
            success: true,
            req_id: Some(_),
            ..
        }
    ));
    match parse_incoming(&ws("ack_subscribe_failed.json")) {
        WsIncoming::Ack {
            success: false,
            message,
            op,
            ..
        } => {
            assert_eq!(op, "subscribe");
            assert!(message.contains("handler not found"));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(parse_incoming(&ws("pong.json")), WsIncoming::Pong);
    // The login of a key the exchange does not know (recorded on both contours with a made-up key).
    match parse_incoming(&ws("auth_failed.json")) {
        WsIncoming::Ack {
            op,
            success: false,
            message,
            req_id: None,
        } => {
            assert_eq!(
                (op.as_str(), message.as_str()),
                ("auth", "API key is invalid.")
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(
        matches!(parse_incoming(&ws("ack_unsubscribe.json")), WsIncoming::Ack { op, .. } if op == "unsubscribe")
    );
}

#[test]
fn the_documented_private_frames_are_events_the_models_read() {
    for (file, topic) in [
        ("doc_private_order.json", "order"),
        ("doc_private_execution.json", "execution"),
        ("doc_private_position.json", "position"),
        ("doc_private_wallet.json", "wallet"),
    ] {
        let WsIncoming::Data {
            topic: t,
            kind,
            data,
            ts,
            ..
        } = parse_incoming(&ws(file))
        else {
            panic!("{file} is not a data frame");
        };
        assert_eq!((t.as_str(), kind), (topic, DataKind::Event));
        assert!(ts > 0, "creationTime is the time of the frame");
        match topic {
            "order" => assert!(
                !serde_json::from_value::<Vec<OrderRecord>>(data)
                    .unwrap()
                    .is_empty()
            ),
            "execution" => {
                let r: Vec<ExecutionRecord> = serde_json::from_value(data).unwrap();
                assert_eq!(r[0].exec_price.to_string(), "95900.1");
            }
            "position" => {
                let r: Vec<PositionRecord> = serde_json::from_value(data).unwrap();
                assert_eq!(
                    r[0].avg_price.unwrap().to_string(),
                    "0",
                    "the stream calls the entry price entryPrice"
                );
            }
            _ => {
                let r: Vec<WalletRecord> = serde_json::from_value(data).unwrap();
                assert_eq!(r[0].coin[0].wallet_balance.to_string(), "0.00102964");
            }
        }
    }
}
