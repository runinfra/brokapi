#![cfg(feature = "integration-live")]

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::models::enums::OrderStatus;
use brokapi_bcs_protocol::models::instrument::InstrumentKey;
use brokapi_bcs_protocol::models::messages::IncomingMessage;
use brokapi_bcs_protocol::ws::client::BcsWsClient;
use brokapi_bcs_protocol::ws::models::MarketDataMessage;
#[allow(unused_imports)]
use brokapi_bcs_protocol::ws::session::WsSession;
use kanal;
use tokio::time::Duration;

fn setup() -> BcsConfig {
    dotenvy::dotenv().ok();
    BcsConfig::from_env().expect("Config error")
}

// ===== LIMITS ✅ =====
#[tokio::test]
async fn test_ws_limits() {
    let config = setup();
    let ws_client = BcsWsClient::new(config).expect("WsClient error");
    let (event_tx, event_rx) = kanal::bounded_async::<IncomingMessage>(128);

    let _session = ws_client.connect_limits(event_tx);

    println!("╔══════════════════════════════════════╗");
    println!("║      WS LIMITS                       ║");
    println!("╠══════════════════════════════════════╣");

    let timeout = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(timeout);
    let mut received = 0;

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                match &event {
                    IncomingMessage::Limits(data) => {
                        received += 1;

                        // Депозитарные позиции
                        if data.depo_limit.as_ref().map_or(false, |v| !v.is_empty()) {
                            println!("║ Депо позиции: {} шт", data.depo_limit.as_ref().map_or(0, |v| v.len()));
                            for pos in data.depo_limit.as_ref().unwrap().iter().take(3) {
                                println!("  • {} ({}) | {} шт @ {:.2} ₽",
                                    pos.ticker.as_deref().unwrap_or("?"),
                                    pos.class_code.as_deref().unwrap_or("?"),
                                    pos.quantity.as_ref().map(|q| q.value).unwrap_or_default(),
                                    pos.average_price.unwrap_or_default()
                                );
                            }
                        }

                        // Денежные лимиты
                        if data.money_limits.as_ref().map_or(false, |v| !v.is_empty()) {
                            println!("║ Денежные лимиты: {} валют", data.money_limits.as_ref().map_or(0, |v| v.len()));
                            for money in data.money_limits.as_ref().unwrap() {
                                println!(
                                    "  • {} {}: qty={:.2} locked={:.2}",
                                    money.exchange.as_deref().unwrap_or("?"),
                                    money.currency.as_deref().unwrap_or("?"),
                                    money.quantity.as_ref().map(|q| q.value).unwrap_or_default(),
                                    money.locked.unwrap_or_default()
                                );
                            }
                        }

                        // Фьючерсные лимиты
                        if data.futures_limits.as_ref().map_or(false, |v| !v.is_empty()) {
                            println!("║ Фьючерсы: {} записей", data.futures_limits.as_ref().map_or(0, |v| v.len()));
                        }

                        if received >= 2 {
                            println!("╚══════════════════════════════════════╝");
                            break;
                        }
                    }
                    IncomingMessage::Error(e) => {
                        println!("  ❌ WS Error: {} - {}", e.code, e.message);
                        break;
                    }
                    IncomingMessage::Unknown(raw) => {
                        println!("  [unknown] {}", raw);
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                println!("  Таймаут 5с, получено сообщений: {}", received);
                println!("╚══════════════════════════════════════╝");
                break;
            }
        }
    }

    assert!(received > 0, "Should receive limits data");
}

// ===== PORTFOLIO ✅ =====
#[tokio::test]
async fn test_ws_portfolio() {
    let config = setup();
    let ws_client = BcsWsClient::new(config).expect("WsClient error");
    let (event_tx, event_rx) = kanal::bounded_async::<IncomingMessage>(128);

    let _session = ws_client.connect_portfolio(event_tx);

    println!("╔══════════════════════════════════════╗");
    println!("║      WS PORTFOLIO                    ║");
    println!("╠══════════════════════════════════════╣");

    let timeout = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(timeout);
    let mut received = 0;

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {

                match &event {
                    IncomingMessage::Portfolio(positions) => {
                        received += 1;

                        // Группировка по типам
                        let money_positions: Vec<_> = positions.iter()
                            .filter(|p| p.position_type == "moneyLimit")
                            .collect();
                        let depo_positions: Vec<_> = positions.iter()
                            .filter(|p| p.position_type == "depoLimit")
                            .collect();

                        // Денежные позиции
                        if !money_positions.is_empty() {
                            println!("║ Денежные позиции: {} валют", money_positions.len());
                            for p in money_positions.iter().take(5) {
                                let blocked = if p.is_blocked.unwrap_or(false) { " [BLOCKED]" } else { "" };
                                println!("  • {} | qty={:.2} locked={:.2} | balance={:.2} rub={:.2}{}",
                                    p.ticker.as_deref().unwrap_or("?"),
                                    p.quantity.unwrap_or_default(),
                                    p.locked.unwrap_or_default(),
                                    p.balance_value.unwrap_or_default(),
                                    p.balance_value_rub.unwrap_or_default(),
                                    blocked
                                );
                            }
                        }

                        // Депозитарные позиции
                        if !depo_positions.is_empty() {
                            println!("║ Депо позиции: {} инструментов", depo_positions.len());
                            for p in depo_positions.iter().take(5) {
                                let name = p.display_name.as_deref().unwrap_or("?");
                                let pl = p.unrealized_pl.unwrap_or_default();
                                let pl_pct = p.unrealized_percent_pl.unwrap_or_default();
                                let pl_sign = if !pl.is_sign_negative() { "+" } else { "" };
                                let share = p.portfolio_share.unwrap_or_default();

                                println!("  • {} ({}) | qty={:.0} @ {:.2} ₽",
                                    p.ticker.as_deref().unwrap_or("?"),
                                    name,
                                    p.quantity.unwrap_or_default(),
                                    p.current_price.unwrap_or_default()
                                );
                                println!("    └ P/L: {}{:.2} ({}{:.2}%) | Share: {:.2}%",
                                    pl_sign, pl, pl_sign, pl_pct, share);
                            }
                        }

                        if received >= 2 {
                            println!("╚══════════════════════════════════════╝");
                            break;
                        }
                    }
                    IncomingMessage::Error(e) => {
                        println!("  ❌ WS Error: {} - {}", e.code, e.message);
                        break;
                    }
                    IncomingMessage::Unknown(raw) => {
                        println!("  [unknown] {}", raw);
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                println!("  Таймаут 5с, получено сообщений: {}", received);
                println!("╚══════════════════════════════════════╝");
                break;
            }
        }
    }

    assert!(received > 0, "Should receive portfolio data");
}

// ===== MARKET DATA: QUOTES ✅ =====
#[tokio::test]
async fn test_ws_quotes() {
    let config = setup();
    let ws_client = BcsWsClient::new(config).expect("WsClient error");
    let (event_tx, event_rx) = kanal::bounded_async::<IncomingMessage>(128);

    let session = ws_client.connect_market_data(event_tx);

    let instruments = vec![InstrumentKey::new("MTLR", "TQBR")];
    ws_client
        .subscribe_quotes(&session, instruments)
        .await
        .expect("Subscribe failed");

    println!("╔══════════════════════════════════════╗");
    println!("║      WS QUOTES (MTLR TQBR)           ║");
    println!("╠══════════════════════════════════════╣");

    let timeout = tokio::time::sleep(Duration::from_secs(480));
    tokio::pin!(timeout);
    let mut received = 0;

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                match &event {
                    IncomingMessage::MarketData(md) => {
                        received += 1;

                        if let MarketDataMessage::Undefined = md {
                            println!("  [init] handshake OK");
                            continue;
                        }

                        // Распаковываем Quotes данные
                        if let MarketDataMessage::Quotes(q) = md {
                            let change = q.change.unwrap_or_default();
                            let change_pct = q.change_rate.unwrap_or_default();
                            let sign = if !change.is_sign_negative() { "+" } else { "" };

                            println!("║ Обновление #{}", received);
                            println!("  • {}:{} | {}",
                                q.ticker, q.class_code,
                                q.date_time.as_deref().unwrap_or("?")
                            );
                            println!("  ├ Цены: bid={:.2} ₽ / last={:.2} ₽ / ask={:.2} ₽",
                                q.bid.unwrap_or_default(),
                                q.last_price.unwrap_or_default(),
                                q.ask.unwrap_or_default()
                            );
                            println!("  ├ Изменение: {}{:.2} ₽ ({}{:.2}%)",
                                sign, change, sign, change_pct
                            );
                            println!("  ├ Диапазон: {:.2} - {:.2} ₽ (O:{:.2} C:{:.2})",
                                q.low.unwrap_or_default(),
                                q.high.unwrap_or_default(),
                                q.open.unwrap_or_default(),
                                q.close.unwrap_or_default()
                            );
                            println!("  └ Объем: {:.0} шт ({} сделок)",
                                q.volume.unwrap_or_default(),
                                q.trade_num.unwrap_or(0)
                            );
                        } else {
                            println!("  [other] {:?}", md);
                        }

                        if received >= 3 {
                            println!("╚══════════════════════════════════════╝");
                        }


                    }
                    IncomingMessage::Error(e) => {
                        println!("  ❌ WS Error: {} - {}", e.code, e.message);
                        break;
                    }
                    IncomingMessage::Unknown(raw) => {
                        println!("  [unknown] {}", raw);
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                println!("  Таймаут 5с, получено сообщений: {}", received);
                println!("╚══════════════════════════════════════╝");
                break;
            }
        }
    }

    assert!(received > 0, "Should receive at least one message");
}

// ===== MARKET DATA: ORDER BOOK ✅ =====
#[tokio::test]
async fn test_ws_order_book() {
    let config = setup();
    let ws_client = BcsWsClient::new(config).expect("WsClient error");
    let (event_tx, event_rx) = kanal::bounded_async::<IncomingMessage>(128);

    let session = ws_client.connect_market_data(event_tx);

    let instruments = vec![
        InstrumentKey::new("SBER", "TQBR"),
        InstrumentKey::new("GAZP", "TQBR"),
        InstrumentKey::new("LKOH", "TQBR"),
    ];
    ws_client
        .subscribe_order_book(&session, instruments, 20)
        .await
        .expect("Subscribe order book failed");

    println!("╔══════════════════════════════════════╗");
    println!("║      WS ORDER BOOK (20 levels)       ║");
    println!("╠══════════════════════════════════════╣");

    let timeout = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(timeout);
    let mut received = 0;

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                match &event {
                    IncomingMessage::MarketData(md) => {
                        received += 1;

                        // Пропускаем handshake
                        if let MarketDataMessage::Undefined = md {
                            println!("  [init] handshake OK");
                            continue;
                        }

                        // Обрабатываем OrderBook
                        if let MarketDataMessage::OrderBook(ob) = md {
                            println!("║ Обновление #{} | {}:{} | depth={}",
                                received,
                                ob.ticker,
                                ob.class_code,
                                ob.depth.unwrap_or(0)
                            );

                            // Топ-3 уровня стакана
                            println!("  ├ BIDS (покупка):");
                            for (i, bid) in ob.bids.iter().take(3).enumerate() {
                                println!("  │  #{}: {:.2} ₽ × {:.0} шт",
                                    i + 1, bid.price, bid.quantity);
                            }

                            println!("  ├ ASKS (продажа):");
                            for (i, ask) in ob.asks.iter().take(3).enumerate() {
                                println!("  │  #{}: {:.2} ₽ × {:.0} шт",
                                    i + 1, ask.price, ask.quantity);
                            }

                            let spread = if !ob.bids.is_empty() && !ob.asks.is_empty() {
                                ob.asks[0].price - ob.bids[0].price
                            } else {
                                rust_decimal::Decimal::ZERO
                            };
                            println!("  └ Спред: {:.2} ₽", spread);
                        } else {
                            println!("  [other] {:?}", md);
                        }

                        // Держать поток 60 секунд, получая все сообщения
                        if received >= 100 {  // или убрать break полностью
                            break;
                        }
                    }
                    IncomingMessage::Error(e) => {
                        println!("  ❌ WS Error: {} - {}", e.code, e.message);
                        break;
                    }
                    IncomingMessage::Unknown(raw) => {
                        println!("  [unknown] {}", raw);
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                println!("  Таймаут 5с, получено сообщений: {}", received);
                println!("╚══════════════════════════════════════╝");
                break;
            }
        }
    }

    assert!(received > 0, "Should receive order book data");
}

// ===== MARKET DATA: LAST TRADES ✅ =====
#[tokio::test]
async fn test_ws_last_trades() {
    let config = setup();
    let ws_client = BcsWsClient::new(config).expect("WsClient error");
    let (event_tx, event_rx) = kanal::bounded_async::<IncomingMessage>(128);

    let session = ws_client.connect_market_data(event_tx);

    let instruments = vec![
        InstrumentKey::new("SBER", "TQBR"),
        InstrumentKey::new("GAZP", "TQBR"),
        InstrumentKey::new("LKOH", "TQBR"),
    ];
    ws_client
        .subscribe_last_trades(&session, instruments)
        .await
        .expect("Subscribe last trades failed");

    println!("╔══════════════════════════════════════╗");
    println!("║      WS LAST TRADES (лента)          ║");
    println!("╠══════════════════════════════════════╣");

    let timeout = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(timeout);
    let mut received = 0;

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                match &event {
                    IncomingMessage::MarketData(md) => {
                        received += 1;

                        // Пропускаем handshake
                        if let MarketDataMessage::Undefined = md {
                            println!("  [init] handshake OK");
                            continue;
                        }

                        // Обрабатываем сделки
                        if let MarketDataMessage::LastTrades(trades_data) = md {
                            let side = trades_data.side.as_deref().unwrap_or("?");
                            let side_emoji = match side {
                                "BUY" => "🟢 BUY",
                                "SELL" => "🔴 SELL",
                                _ => "⚪ ?",
                            };

                            println!("║ Сделка #{} | {}:{}",
                                received,
                                trades_data.ticker,
                                trades_data.class_code
                            );
                            println!("  ├ {} @ {:.2} ₽ | qty: {:.0} | vol: {:.2}",
                                side_emoji,
                                trades_data.price,
                                trades_data.quantity,
                                trades_data.volume.unwrap_or_default()
                            );
                            println!("  └ Время: {}",
                                trades_data.date_time.as_deref().unwrap_or("?")
                            );
                        } else {
                            println!("  [other] {:?}", md);
                        }

                        if received >= 100 {  // или убрать break полностью
                            break;
                        }
                    }
                    IncomingMessage::Error(e) => {
                        println!("  ❌ WS Error: {} - {}", e.code, e.message);
                        break;
                    }
                    IncomingMessage::Unknown(raw) => {
                        println!("  [unknown] {}", raw);
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                println!("  Таймаут 5с, получено сообщений: {}", received);
                println!("╚══════════════════════════════════════╝");
                break;
            }
        }
    }

    assert!(received > 0, "Should receive last trades data");
}

// ===== MARKET DATA: LAST CANDLE ✅ =====
#[tokio::test]
async fn test_ws_last_candle() {
    let config = setup();
    let ws_client = BcsWsClient::new(config).expect("WsClient error");
    let (event_tx, event_rx) = kanal::bounded_async::<IncomingMessage>(128);

    let session = ws_client.connect_market_data(event_tx);

    let instruments = vec![
        InstrumentKey::new("SBER", "TQBR"),
        InstrumentKey::new("GAZP", "TQBR"),
        InstrumentKey::new("LKOH", "TQBR"),
    ];
    ws_client
        .subscribe_last_candle(&session, instruments, "M1")
        .await
        .expect("Subscribe last candle failed");

    println!("╔══════════════════════════════════════╗");
    println!("║      WS CANDLES (M1)                 ║");
    println!("╠══════════════════════════════════════╣");

    let timeout = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(timeout);
    let mut received = 0;

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                match &event {
                    IncomingMessage::MarketData(md) => {
                        if let MarketDataMessage::LastCandleSuccess(s) = md {
                            println!("  [ack] CandleStickSuccess tf={:?}", s.time_frame);
                            continue;
                        }

                        if let MarketDataMessage::Undefined = md {
                            println!("  [init] Undefined/control message");
                            continue;
                        }

                        // Обрабатываем свечи
                        if let MarketDataMessage::LastCandle(candle) = md {
                            received += 1;

                            let open = candle.open.unwrap_or_default();
                            let close = candle.close.unwrap_or_default();
                            let change = close - open;
                            let sign = if !change.is_sign_negative() { "+" } else { "" };

                            println!(
                                "║ Свеча #{} | {}:{} | {}",
                                received,
                                candle.ticker.as_deref().unwrap_or("?"),
                                candle.class_code.as_deref().unwrap_or("?"),
                                candle.time_frame.as_deref().unwrap_or("M1")
                            );
                            println!(
                                    "  ├ OHLC: O:{:.2} H:{:.2} L:{:.2} C:{:.2}",
                                    open,
                                    candle.high.unwrap_or_default(),
                                    candle.low.unwrap_or_default(),
                                    close
                                );
                            println!(
                                    "  ├ Изменение: {}{:.2} ({}{:.2}%)",
                                    sign,
                                    change,
                                    sign,
                                    if open.is_sign_positive() && !open.is_zero() { change / open * rust_decimal::Decimal::from(100) } else { rust_decimal::Decimal::ZERO }
                                                                                );
                            println!(
                                    "  └ Объем: {:.0} | {}",
                                    candle.volume.unwrap_or_default(),
                                    candle.timestamp.as_deref().unwrap_or("?")
                                );

                            if received >= 5 {
                                println!("╚══════════════════════════════════════╝");
                                break;
                            }
                        } else {
                            println!("  [other] {:?}", md);
                        }

                        if received >= 100 {
                            println!("╚══════════════════════════════════════╝");
                            break;
                        }
                    }
                    IncomingMessage::Error(e) => {
                        println!("  ❌ WS Error: {} - {}", e.code, e.message);
                        break;
                    }
                    IncomingMessage::Unknown(raw) => {
                        println!("  [unknown] {}", raw);
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                println!("  Таймаут 10с, получено сообщений: {}", received);
                println!("╚══════════════════════════════════════╝");
                break;
            }
        }
    }

    assert!(received > 0, "Should receive candle data");
}

// ===== MARGINAL INDICATORS ✅ =====
#[tokio::test]
async fn test_ws_marginal_indicators() {
    let config = setup();
    let ws_client = BcsWsClient::new(config).expect("WsClient error");
    let (event_tx, event_rx) = kanal::bounded_async::<IncomingMessage>(128);

    let _session = ws_client.connect_marginal_indicators(event_tx);

    println!("╔══════════════════════════════════════╗");
    println!("║      WS MARGINAL INDICATORS          ║");
    println!("╠══════════════════════════════════════╣");

    let timeout = tokio::time::sleep(Duration::from_secs(60));
    tokio::pin!(timeout);
    let mut received = 0;

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                match &event {
                    IncomingMessage::MarginalIndicators(mi) => {
                        received += 1;
                        println!("║ Обновление #{}", received);

                        // Данные счёта
                        if let Some(agreement) = &mi.agreement_data {
                            println!("  ├ Счёт: {} ({})",
                                agreement.number.as_deref().unwrap_or("?"),
                                agreement.display_name.as_deref().unwrap_or("?")
                            );
                        }

                        // Стоимость портфеля
                        if let Some(portfolio) = &mi.portfolio_current_value {
                            let change = portfolio.daily_portfolio_change_rub.unwrap_or_default();
                            let sign = if !change.is_sign_negative() { "+" } else { "" };
                            println!("  ├ Портфель: {:.2} ₽ | Изм: {}{:.2} ₽ ({}{:.2}%)",
                                portfolio.current_value_rub.unwrap_or_default(),
                                sign, change, sign,
                                portfolio.daily_portfolio_change_perc.unwrap_or_default()
                            );
                        }

                        // Устойчивость
                        if let Some(stability) = &mi.portfolio_stability {
                            println!("  ├ Устойчивость: {} | Маржа: {}",
                                stability.stability_status_name.as_deref().unwrap_or("?"),
                                if stability.is_margin_on.unwrap_or(false) { "ON" } else { "OFF" }
                            );
                        }

                        // Деньги по валютам
                        if !mi.cash_by_currency.is_empty() {
                            println!("  ├ Деньги:");
                            for cash in &mi.cash_by_currency {
                                println!("  │  • {}: {:.2}",
                                    cash.currency.as_deref().unwrap_or("?"),
                                    cash.sum.unwrap_or_default()
                                );
                            }
                        }

                        // Задолженности
                        if let Some(debt) = &mi.money_debt {
                            println!("  ├ Задолженность: {:.2}", debt.total_money_debt.unwrap_or_default());
                        }

                        // Маржинальные позиции
                        if let Some(securities) = &mi.margin_securities {
                            println!("  └ Позиции: {} шт", securities.margin_by_security.len());
                            for pos in securities.margin_by_security.iter().take(3) {
                                println!("    • {}: qty={:.0} value={:.2}",
                                    pos.ticker.as_deref().unwrap_or("?"),
                                    pos.quantity.unwrap_or_default(),
                                    pos.current_value.unwrap_or_default()
                                );
                            }
                        }

                        if received >= 2 {
                            println!("╚══════════════════════════════════════╝");
                            break;
                        }
                    }
                    IncomingMessage::Error(e) => {
                        println!("  ❌ WS Error: {} - {}", e.code, e.message);
                        break;
                    }
                    IncomingMessage::Unknown(raw) => {
                        println!("  [unknown] {}", raw);
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                println!("  Таймаут 10с, получено: {}", received);
                println!("╚══════════════════════════════════════╝");
                break;
            }
        }
    }

    assert!(received > 0, "Should receive marginal indicators data");
}

// ===== ORDER EXECUTION =====
#[tokio::test]
async fn test_ws_order_execution() {
    let config = setup();
    let ws_client = BcsWsClient::new(config).expect("WsClient error");
    let (event_tx, event_rx) = kanal::bounded_async::<IncomingMessage>(128);

    let _session = ws_client.connect_order_execution(event_tx);

    println!("╔════════════════════════════════════════════════════╗");
    println!("║            ORDER EXECUTION WS CONNECTED           ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ stream: /orders/execution/ws");
    println!("╚════════════════════════════════════════════════════╝");

    let timeout = tokio::time::sleep(Duration::from_secs(480));
    tokio::pin!(timeout);

    let mut received = 0usize;
    let valid_status_seen = false;

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                match event {
                    IncomingMessage::OrderExecution(exec) => {
                        received += 1;
                        let data = exec.data.as_ref();

                        let status = data.map(|d| d.order_status);
                        let exec_type = data.and_then(|d| d.execution_type);

                        println!("╔════════════════════════════════════════════════════╗");
                        println!("║                 ORDER EXECUTION EVT               ║");
                        println!("╠════════════════════════════════════════════════════╣");
                        println!("║ originalClientOrderId: {:?}", exec.original_client_order_id);
                        println!("║ clientOrderId:         {:?}", exec.client_order_id);
                        println!("║ status:                {:?}", status);
                        println!("║ executionType:         {:?}", exec_type);
                        println!("║ instrument:            {:?} / {:?}", data.map(|d| d.ticker.as_str()), data.map(|d| d.class_code.as_str()));
                        println!("║ side/orderType:        {:?} / {:?}", data.map(|d| d.side), data.and_then(|d| d.order_type));
                        println!("║ qty:                   {:?} (exec={:?}, last={:?}, left={:?})",
                            data.and_then(|d| d.order_quantity),
                            data.and_then(|d| d.executed_quantity),
                            data.and_then(|d| Some(d.last_quantity)),
                            data.and_then(|d| Some(d.remained_quantity)),
                        );
                        println!("║ price/avg:             {:?} / {:?}", data.map(|d| d.price), data.and_then(|d| d.average_price));
                        println!("║ orderId/executionId:   {:?} / {:?}", data.and_then(|d| d.order_id.as_deref()), data.and_then(|d| d.execution_id.as_deref()));
                        println!("║ orderNumber:           {:?}", data.and_then(|d| d.order_number.as_deref()));
                        println!("║ transactionTime:       {:?}", data.and_then(|d| d.transaction_time));
                        println!("║ tradeDate:             {:?}", data.and_then(|d| d.trade_date.as_deref()));
                        println!("║ currency:              {:?}", data.and_then(|d| d.currency.as_deref()));
                        println!("║ value/commission:      {:?} / {:?}", data.and_then(|d| d.execution_value), data.and_then(|d| d.commission));
                        println!("║ accruedCoupon:         {:?}", data.and_then(|d| d.accrued_coupon));
                        println!("╚════════════════════════════════════════════════════╝");

                        if received == 1 {
                            println!("  [first-event] received, keep listening until timeout...");
                        }
                    }
                    IncomingMessage::Error(e) => {
                        println!("  [ws-error] code={} message={}", e.code, e.message);
                        break;
                    }
                    IncomingMessage::Unknown(raw) => {
                        println!("  [unknown] {}", raw);
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                println!("  [timeout] 5s, received={}", received);
                break;
            }
        }
    }

    assert!(
        received > 0,
        "Should receive at least one OrderExecution message"
    );
    assert!(
        valid_status_seen,
        "OrderExecution message received, but orderStatus is unknown"
    );
}

// ===== ORDER TRANSACTION =====
#[tokio::test]
async fn test_ws_order_transaction() {
    let config = setup();
    let ws_client = BcsWsClient::new(config).expect("WsClient error");
    let (event_tx, event_rx) = kanal::bounded_async::<IncomingMessage>(128);

    let _session = ws_client.connect_order_transaction(event_tx);

    println!("╔════════════════════════════════════════════════════╗");
    println!("║           ORDER TRANSACTION WS CONNECTED          ║");
    println!("╠════════════════════════════════════════════════════╣");
    println!("║ stream: /orders/transaction/ws");
    println!("╚════════════════════════════════════════════════════╝");

    let timeout = tokio::time::sleep(Duration::from_secs(480));
    tokio::pin!(timeout);

    let mut received = 0usize;
    let mut valid_status_seen = false;

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                match event {
                    IncomingMessage::OrderTransaction(tx) => {
                        received += 1;
                        let data = tx.data.as_ref();

                        let status = data.map(|d| d.order_status);
                        let exec_type = data.and_then(|d| d.execution_type);

                        if !matches!(status, Some(OrderStatus::Unknown) | None) {
                            valid_status_seen = true;
                        }

                        println!("╔════════════════════════════════════════════════════╗");
                        println!("║                ORDER TRANSACTION EVT              ║");
                        println!("╠════════════════════════════════════════════════════╣");
                        println!("║ originalClientOrderId: {:?}", tx.original_client_order_id);
                        println!("║ clientOrderId:         {:?}", tx.client_order_id);
                        println!("║ status:                {:?}", status);
                        println!("║ executionType:         {:?}", exec_type);
                        println!("║ instrument:            {:?} / {:?}", data.map(|d| d.ticker.as_str()), data.map(|d| d.class_code.as_str()));
                        println!("║ side/orderType:        {:?} / {:?}", data.map(|d| d.side), data.and_then(|d| d.order_type));
                        println!("║ qty:                   {:?} (exec={:?}, last={:?}, left={:?})",
                            data.and_then(|d| d.order_quantity),
                            data.and_then(|d| d.executed_quantity),
                            data.and_then(|d| Some(d.last_quantity)),
                            data.and_then(|d| Some(d.remained_quantity)),
                        );
                        println!("║ price/avg:             {:?} / {:?}", data.map(|d| d.price), data.and_then(|d| d.average_price));
                        println!("║ orderId/executionId:   {:?} / {:?}", data.and_then(|d| d.order_id.as_deref()), data.and_then(|d| d.execution_id.as_deref()));
                        println!("║ orderNumber:           {:?}", data.and_then(|d| d.order_number.as_deref()));
                        println!("║ transactionTime:       {:?}", data.and_then(|d| d.transaction_time));
                        println!("║ tradeDate:             {:?}", data.and_then(|d| d.trade_date.as_deref()));
                        println!("║ currency:              {:?}", data.and_then(|d| d.currency.as_deref()));
                        println!("║ value/commission:      {:?} / {:?}", data.and_then(|d| d.execution_value), data.and_then(|d| d.commission));
                        println!("║ accruedCoupon:         {:?}", data.and_then(|d| d.accrued_coupon));
                        println!("╚════════════════════════════════════════════════════╝");

                        if received == 1 {
                            println!("  [first-event] received, keep listening until timeout...");
                        }
                    }
                    IncomingMessage::Error(e) => {
                        println!("  [ws-error] code={} message={}", e.code, e.message);
                        break;
                    }
                    IncomingMessage::Unknown(raw) => {
                        println!("  [unknown] {}", raw);
                    }
                    _ => {}
                }
            }
            _ = &mut timeout => {
                println!("  [timeout] 5s, received={}", received);
                break;
            }
        }
    }

    assert!(
        received > 0,
        "Should receive at least one OrderTransaction message"
    );
    assert!(
        valid_status_seen,
        "OrderTransaction message received, but orderStatus is unknown"
    );
}
