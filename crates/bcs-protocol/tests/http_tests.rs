#![cfg(feature = "integration-live")]

use brokapi_bcs_protocol::config::BcsConfig;
use brokapi_bcs_protocol::http::client::BcsHttpClient;
use brokapi_bcs_protocol::http::models::CandlesRequest;
use brokapi_bcs_protocol::http::models::TradeSearchRequest;
use brokapi_bcs_protocol::http::models::{
    DailyScheduleRequest, InstrumentsByIsinsRequest, InstrumentsByTickersRequest,
    InstrumentsByTypeRequest, TradingStatusRequest,
};
use chrono::Utc;

fn setup() -> (BcsConfig, BcsHttpClient) {
    dotenv::dotenv().ok();
    let config = BcsConfig::from_env().expect("Config error");
    let client = BcsHttpClient::new(config.clone()).expect("Client error");
    (config, client)
}

// ===== Авторизация ✅ =====
#[tokio::test]
async fn test_auth() {
    let (_, client) = setup();
    let resp = client.authenticate().await.expect("Auth failed");
    assert_eq!(resp.token_type, "Bearer");

    // Подробный вывод токена (частично скрыты для безопасности)
    let access_preview = &resp.access_token[..resp.access_token.len().min(20)];
    let refresh_preview = &resp.refresh_token.as_ref().unwrap()
        [..resp.refresh_token.as_ref().unwrap().chars().count().min(10)];

    println!("╔══════════════════════════════════════╗");
    println!("║         AUTH SUCCESS                 ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ token_type:      {}", resp.token_type);
    println!(
        "║ expires_in:       {}s ({}h)",
        resp.expires_in,
        resp.expires_in / 3600
    );
    println!("║ refresh_expires: {}s", resp.refresh_expires_in);
    println!("║ access_token:     {}...", access_preview);
    println!("║ refresh_token:    {}...", refresh_preview);
    println!(
        "║ scope:            {}",
        resp.scope.as_deref().unwrap_or("N/A")
    );
    println!("╚══════════════════════════════════════╝");
}

// ===== Портфолио ✅ =====
#[tokio::test]
async fn test_portfolio() {
    let (_, client) = setup();
    let positions = client.get_portfolio().await.expect("Portfolio failed");

    println!("╔══════════════════════════════════════╗");
    println!("║         PORTFOLIO                    ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ Всего позиций: {}", positions.len());
    println!("╚══════════════════════════════════════╝");

    for (i, p) in positions.iter().enumerate() {
        let ticker = p.ticker.as_deref().unwrap_or("N/A");
        let name = p.display_name.as_deref().unwrap_or("");
        let account = p.account.as_deref().unwrap_or("N/A");
        let exchange = p.exchange.as_deref().unwrap_or("N/A");
        let qty = p
            .quantity
            .map(|q| format!("{:.2}", q))
            .unwrap_or_else(|| "N/A".to_string());
        let price = p
            .current_price
            .map(|pr| format!("{:.2}", pr))
            .unwrap_or_else(|| "N/A".to_string());
        let buy_price = p
            .balance_price
            .map(|pr| format!("{:.2}", pr))
            .unwrap_or_else(|| "N/A".to_string());
        let buy_value = p
            .balance_value
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| "N/A".to_string());
        let pl = p
            .unrealized_pl
            .map(|pl| format!("{:.2}", pl))
            .unwrap_or_else(|| "N/A".to_string());

        let is_blocked = p.is_blocked.unwrap_or(false);
        let is_blocked_trade_account = p.is_blocked_trade_account.unwrap_or(false);

        let term = p.term.as_deref().unwrap_or("N/A");
        let board = p.board.as_deref().unwrap_or("N/A");

        println!("  {}. {} ({})", i + 1, ticker, name);
        println!(
            "     Счёт: {} | Биржа: {} | Режим: {}",
            account, exchange, board
        );
        println!(
            "     Тип: {} | Срок: {} | Кол-во: {} | Цена: {}",
            p.position_type, term, qty, price
        );
        println!(
            "     Куплено по: {} (Всего: {}) | Текущая цена: {} | P&L: {}",
            buy_price, buy_value, price, pl
        );
        println!(
            "     Блокировка: class={} | account={}",
            is_blocked, is_blocked_trade_account
        );
    }
}

// ===== Лимиты ✅ =====
#[tokio::test]
async fn test_limits() {
    let (_, client) = setup();
    let limits = client.get_limits().await.expect("Limits failed");

    println!("╔══════════════════════════════════════╗");
    println!("║         LIMITS                       ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ Депо лимитов:    {}", limits.depo_limit.len());
    println!("║ Денежных лимитов: {}", limits.money_limits.len());
    println!("║ Фьючерс лимитов:  {}", limits.futures_limits.len());
    println!("╚══════════════════════════════════════╝");

    // Депозитарные лимиты
    if !limits.depo_limit.is_empty() {
        println!("  Депо лимиты:");
        for (i, l) in limits.depo_limit.iter().take(3).enumerate() {
            let ticker = l.ticker.as_deref().unwrap_or("N/A");
            let qty = l
                .quantity
                .as_ref()
                .map(|q| format!("{:.2}", q.value))
                .unwrap_or_else(|| "N/A".to_string());
            println!("    {}. {}: {}", i + 1, ticker, qty);
        }
    }

    // Денежные лимиты
    if !limits.money_limits.is_empty() {
        println!("  Денежные лимиты:");
        for (i, l) in limits.money_limits.iter().take(3).enumerate() {
            let currency = l.currency.as_deref().unwrap_or("N/A");
            let exchange = l.exchange.as_deref().unwrap_or("N/A");
            let qty = l
                .quantity
                .as_ref()
                .map(|q| format!("{:.2}", q.value))
                .unwrap_or_else(|| "N/A".to_string());
            let locked = l
                .locked
                .map(|v| format!("{:.2}", v))
                .unwrap_or_else(|| "0.00".to_string());

            println!(
                "    {}. {} ({}) | Кол-во: {} | Заблокировано: {}",
                i + 1,
                currency,
                exchange,
                qty,
                locked
            );
        }
    }

    // Фьючерсные лимиты
    if !limits.futures_limits.is_empty() {
        println!("  Фьючерсные лимиты:");
        for (i, l) in limits.futures_limits.iter().take(5).enumerate() {
            let currency = l.currency.as_deref().unwrap_or("N/A");
            let exchange = l.exchange.as_deref().unwrap_or("N/A");
            let cbp_limit = l.cbp_limit.unwrap_or(0.0);
            let cbpl_used = l.cbpl_used.unwrap_or(0.0);
            let cbpl_planned = l.cbpl_planned.unwrap_or(0.0);
            let var_margin = l.var_margin.unwrap_or(0.0);

            println!(
                "    {}. {} ({}) | Лимит: {:.2} | Использовано: {:.2} | План: {:.2} | Вар.маржа: {:.2}",
                i + 1,
                currency,
                exchange,
                cbp_limit,
                cbpl_used,
                cbpl_planned,
                var_margin
            );
        }
    } else {
        println!("  ⚠️ Фьючерсные лимиты отсутствуют — доступ к FORTS не подключен");
    }
}

// ===== Получить расписание на день ✅ =====
#[tokio::test]
async fn test_schedule() {
    let (_, client) = setup();
    let schedule = client
        .get_daily_schedule(&DailyScheduleRequest {
            class_code: "TQOB".to_string(),
            ticker: "SU26238RMFS4".to_string(),
        })
        .await
        .expect("Schedule failed");

    println!("╔══════════════════════════════════════╗");
    println!("║         SCHEDULE (SU26238RMFS4, TQOB)        ║");
    println!("╠══════════════════════════════════════╣");
    println!(
        "║ Рабочий день: {}",
        if schedule.is_work_day {
            "Да ✅"
        } else {
            "Нет ❌"
        }
    );
    println!("║ Сессий:        {}", schedule.daily_schedule.len());
    println!("╚══════════════════════════════════════╝");

    if !schedule.daily_schedule.is_empty() {
        println!("  Торговые сессии:");
        for (i, s) in schedule.daily_schedule.iter().enumerate() {
            println!(
                "    {}. {} ({})",
                i + 1,
                s.trading_session_type,
                s.trading_session_status
            );
            println!("       {} - {}", s.start_date, s.end_date);
        }
    } else {
        println!("  Нет активных торговых сессий");
    }
}

// ===== Получить инструменты по тикеру ✅ =====
#[tokio::test]
async fn test_instruments_by_tickers() {
    let (_, client) = setup();
    let instruments = client
        .get_instruments_by_tickers(
            &InstrumentsByTickersRequest {
                tickers: vec!["MTLR".to_string()],
            },
            None,
            None,
        )
        .await
        .expect("Instruments failed");

    println!("╔══════════════════════════════════════╗");
    println!("║         INSTRUMENTS (MTLR)           ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ Найдено: {}", instruments.len());
    println!("╚══════════════════════════════════════╝");

    for (i, inst) in instruments.iter().enumerate() {
        println!("  {}. {}", i + 1, inst.ticker);
        println!(
            "     Название: {}",
            inst.display_name.as_deref().unwrap_or("N/A")
        );
        println!("     Лот: {}", inst.lot_size.unwrap_or(0.0));
        println!("     ISIN: {}", inst.isin.as_deref().unwrap_or("N/A"));
        println!(
            "     Валюта: {}",
            inst.trading_currency.as_deref().unwrap_or("N/A")
        );
    }

    // Сохранение в JSON файл
    let json = serde_json::to_string_pretty(&instruments).expect("JSON serialize failed");
    let filename = "sber_instruments.json";
    std::fs::write(filename, json).expect("Failed to write file");
    println!("\n💾 Сохранено в файл: {}", filename);
}

// ===== Получить инструменты по ISIN ✅ =====
#[tokio::test]
async fn test_instruments_by_isins() {
    let (_, client) = setup();
    let isin = "RU000A0DKXV5"; // GAZP
    let instruments = client
        .get_instruments_by_isins(
            &InstrumentsByIsinsRequest {
                isins: vec![isin.to_string()],
            },
            None,
            None,
        )
        .await
        .expect("Instruments by ISINs failed");

    println!("╔══════════════════════════════════════╗");
    println!("║      INSTRUMENTS BY ISIN             ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ ISIN запрос: {}", isin);
    println!("║ Найдено:      {}", instruments.len());
    println!("╚══════════════════════════════════════╝");

    for (i, inst) in instruments.iter().enumerate() {
        println!("  {}. {}", i + 1, inst.ticker);
        println!(
            "     Название: {}",
            inst.display_name.as_deref().unwrap_or("N/A")
        );
        println!("     ISIN:     {}", inst.isin.as_deref().unwrap_or("N/A"));
        println!(
            "     Тип:      {}",
            inst.instrument_type.as_deref().unwrap_or("N/A")
        );
        println!("     Лот:      {}", inst.lot_size.unwrap_or(0.0));

        // Площадки
        if !inst.boards.is_empty() {
            let boards: Vec<String> = inst
                .boards
                .iter()
                .map(|b| format!("{} ({})", b.class_code, b.exchange))
                .collect();
            println!("     Площадки: {}", boards.join(", "));
        }
    }

    // Сохранение в JSON
    let json = serde_json::to_string_pretty(&instruments).expect("JSON serialize failed");
    let filename = "gazp_by_isin.json";
    std::fs::write(filename, json).expect("Failed to write file");
    println!("\n💾 Сохранено в файл: {}", filename);
}

// ===== Получить инструменты по типу ✅ =====
#[tokio::test]
async fn test_instruments_by_type() {
    let (_, client) = setup();

    let mut all_futures = Vec::new();
    let mut page = 0;
    let size = 50;

    println!("╔══════════════════════════════════════╗");
    println!("║      ALL FUTURES (пагинация)           ║");
    println!("╠══════════════════════════════════════╣");

    loop {
        let instruments = client
            .get_instruments_by_type(&InstrumentsByTypeRequest {
                instrument_type: "BONDS".to_string(), // ✅ множественное число
                base_asset_ticker: None,
                size: Some(size),
                page: Some(page),
            })
            .await
            .expect("Instruments by type failed");

        let count = instruments.len();
        println!("  Страница {}: +{} облигаций", page, count);

        if instruments.is_empty() {
            break;
        }

        all_futures.extend(instruments);

        // Если получили меньше чем size — это последняя страница
        if count < size as usize {
            break;
        }

        page += 1;

        // Защита от бесконечного цикла
        if page > 50 {
            println!("  ⚠️ Достигнут лимит 50 страниц");
            break;
        }
    }

    println!("╠══════════════════════════════════════╣");
    println!("║ Всего облигаций: {}", all_futures.len());
    println!("╚══════════════════════════════════════╝");

    // Показать первые 10
    if !all_futures.is_empty() {
        println!("  Первые 10 облигаций:");
        for (i, inst) in all_futures.iter().take(10).enumerate() {
            println!(
                "    {}. {} ({}) | ISIN: {}",
                i + 1,
                inst.ticker,
                inst.display_name.as_deref().unwrap_or("N/A"),
                inst.isin.as_deref().unwrap_or("N/A")
            );
        }
    }

    // Сохранение в JSON
    let json = serde_json::to_string_pretty(&all_futures).expect("JSON serialize failed");
    let filename = "all_bonds.json";
    std::fs::write(filename, json).expect("Failed to write file");
    println!(
        "\n💾 Сохранено в файл: {} ({} инструментов)",
        filename,
        all_futures.len()
    );
}

// ===== Получить статус торгов ✅ =====
#[tokio::test]
async fn test_trading_statuses() {
    let (_, client) = setup();
    let request = TradingStatusRequest {
        class_code: "TQOB".to_string(),
    };

    let status = client
        .get_trading_status(&request)
        .await
        .expect("Trading status failed");

    println!("╔══════════════════════════════════════╗");
    println!("║      TRADING STATUS (TQOB)           ║");
    println!("╠══════════════════════════════════════╣");
    println!(
        "║ Тип сессии:   {} ({})",
        status.trading_session_type, status.trading_session_type_id
    );
    println!("║ Статус:       {}", status.trading_session_status);
    println!(
        "║ Следующая:     {}",
        status.next_session_date.format("%Y-%m-%d %H:%M")
    );
    println!("╚══════════════════════════════════════╝");
}

// ===== Получение дисконтов по инструментам ✅ =====
#[tokio::test]
async fn test_discounts() {
    let (_, client) = setup();
    let discounts = client
        .get_instruments_discounts()
        .await
        .expect("Discounts failed");

    println!("╔══════════════════════════════════════╗");
    println!("║      DISCOUNTS                       ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ Всего записей: {}", discounts.len());
    println!("╚══════════════════════════════════════╝");

    for (i, d) in discounts.iter().take(5).enumerate() {
        println!("  {}. {}", i + 1, d.ticker);
        println!("     Дисконт лонг:  {}%", d.discount_long);
        println!("     Дисконт шорт: {}%", d.discount_short);
    }

    if discounts.len() > 5 {
        println!("  ... и еще {} записей", discounts.len() - 5);
    }
}

// ===== CANDLES ✅ =====
#[tokio::test]
async fn test_candles() {
    let (_, client) = setup();

    let end_date = Utc::now();
    let start_date = end_date - chrono::Duration::hours(24);

    let request = CandlesRequest {
        ticker: "SBER".to_string(),
        class_code: "TQBR".to_string(),
        start_date,
        end_date,
        time_frame: "H1".to_string(),
    };

    let candles = client.get_candles(&request).await.expect("Candles failed");

    println!("╔══════════════════════════════════════╗");
    println!("║      CANDLES (SBER / TQBR)           ║");
    println!("╠══════════════════════════════════════╣");
    println!(
        "║ Период:      {} — {}",
        candles.start_date.format("%Y-%m-%d %H:%M"),
        candles.end_date.format("%H:%M")
    );
    println!("║ Таймфрейм:   {}", candles.time_frame);
    println!("║ Всего баров:  {}", candles.bars.len());
    println!("╚══════════════════════════════════════╝");

    if !candles.bars.is_empty() {
        let first = candles.bars.first().unwrap();
        let last = candles.bars.last().unwrap();

        println!("  Первый бар ({})", first.time.format("%H:%M"));
        println!(
            "    O: {:.2}  H: {:.2}  L: {:.2}  C: {:.2}  V: {:.0}",
            first.open, first.high, first.low, first.close, first.volume
        );

        println!("  Последний бар ({})", last.time.format("%H:%M"));
        println!(
            "    O: {:.2}  H: {:.2}  L: {:.2}  C: {:.2}  V: {:.0}",
            last.open, last.high, last.low, last.close, last.volume
        );

        // Показать изменение цены
        let change = ((last.close - first.open) / first.open) * 100.0;
        let emoji = if change >= 0.0 { "📈" } else { "📉" };
        println!("  Изменение: {:.2}% {}", change, emoji);
    }
}

// ===== TRADES ✅ =====
#[tokio::test]
async fn test_trades() {
    let (_, client) = setup();
    let end_date = Utc::now();
    let start_date = end_date - chrono::Duration::days(7);

    let start_date_time = start_date.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let end_date_time = end_date.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

    let request = TradeSearchRequest {
        start_date_time,
        end_date_time,
        side: None,
        tickers: Some(vec!["SBER".to_string()]),
    };

    let response = client
        .search_trades(&request, Some(50), Some(0))
        .await
        .expect("Trades search failed");

    println!("╔══════════════════════════════════════╗");
    println!("║      TRADES (SBER, last 7 days)      ║");
    println!("╠══════════════════════════════════════╣");
    println!("║ Всего сделок:  {}", response.total_records);
    println!("║ Страниц:       {}", response.total_pages);
    println!("║ Показано:      {}", response.records.len());
    println!("╚══════════════════════════════════════╝");

    for (i, trade) in response.records.iter().take(5).enumerate() {
        let data = match &trade.data {
            Some(d) => d,
            None => continue,
        };

        let side_str = match data.side.as_deref() {
            Some("1") => "BUY 📗",
            Some("2") => "SELL 📕",
            _ => "?",
        };

        println!(
            "  {}. {} | {}",
            i + 1,
            data.ticker.as_deref().unwrap_or("?"),
            data.transaction_time.as_deref().unwrap_or("?")
        );
        println!("     Направление: {}", side_str);
        println!(
            "     Цена:        {:.2} {}",
            data.price.unwrap_or(0.0),
            data.currency.as_deref().unwrap_or("RUB")
        );
        println!(
            "     Объём:       {:.0}",
            data.executed_quantity.unwrap_or(0.0)
        );
        println!(
            "     Сумма:       {:.2}",
            data.execution_value.unwrap_or(0.0)
        );
    }

    if response.records.len() > 5 {
        println!("  ... и еще {} сделок", response.records.len() - 5);
    }
}
