# Готовность: что умеет библиотека по трем брокерам

Дата: 2026-10-19 (обновлено 20.10: деньги, цены и количества - точные числа, цена проверяется по шагу до отправки. Вечером 19.10 БКС выровнен с Т-Банком и Финамом по `Overflow`, `Resync`, `cid` и квоте). Строка - возможность общего интерфейса, колонка - брокер. В каждой клетке три отметки:

**код / mock / live**

- **код** - возможность написана в коннекторе.
- **mock** - есть тест на mock-сервере, который ее проверяет. Тест указан в сноске. Нет теста - пишу "нет".
- **live** - прогон на настоящем API. **Везде "не проверено"**: токенов не было, живой прогон коннекторов не делался.

Значения: `да`, `нет у брокера` (в API брокера этого нет), `нет` и причина в сноске. Таблица заполнена по тестам и коду, без
оптимизма: "mock да" стоит, только если тест действительно эту возможность проверяет.

Короткие названия тестов расшифрованы в конце. Общие сценарии (`brokapi-conformance`) гоняет каждый коннектор против
своего mock, это `[К]` в сносках.

| Возможность | БКС | Т-Банк | Финам |
|---|---|---|---|
| Стакан снимком | да / да [К1] / не проверено | да / да [К1] / не проверено | да / да [К1, Ф1] / не проверено |
| Стакан потоком | да / да [К2, Б1] / не проверено | да / да [К2] / не проверено | да / да [К2, Ф2] / не проверено |
| Сделки рынка (история и поток) | да / да [К8, К9, Б2] / не проверено | да / да [К8, К9, Т1] / не проверено | да / да [К8, К9, Ф3] / не проверено |
| Котировки потоком | да / да [К9, Б2] / не проверено | да / да [К9, Т1] / не проверено | да / да [К9, Ф3] / не проверено |
| Свечи (история и поток) | да / да [К8, К9, Б2] / не проверено | да / да [К8, К9, Т1] / не проверено | да / да [К8, К9, Ф3, Ф4] / не проверено |
| Справочник (поиск, карточка) | да / да [К3, Б3] / не проверено | да / да [К3, Т1] / не проверено | да / да [К3, Ф5] / не проверено |
| Торговые параметры (лот, шаг цены) | да / да [К3, Б3] / не проверено | да / да [К3, Т1] / не проверено | да / да [К3, Ф5] / не проверено |
| Статус торгов | да / да [К8, Б2] / не проверено | да / да [К8, Т1] / не проверено | да / да [К8, Ф5] / не проверено |
| Расписание | да / да [К8, Б2] / не проверено | да / да [К8, Т1] / не проверено | да / да [К8, Ф5] / не проверено |
| Постановка заявки | да / да [К4, Б4] / не проверено | да / да [К4] / не проверено | да / да [К4] / не проверено |
| Отмена заявки | да / да [К4, Б4] / не проверено | да / да [К4] / не проверено | да / да [К4] / не проверено |
| Изменение заявки | да / да [К11, Б5] / не проверено | да / да [К11, Т2] / не проверено | нет у брокера / да, отказ проверен [К11, Ф6] / нет у брокера |
| Статус заявки по cid | да / да [К4, Б4] / не проверено | да / да [К4] / не проверено | да / да [К4] / не проверено |
| Активные заявки | да / да [К10, Б6] / не проверено | да / да [К10, Т3] / не проверено | да / да [К10, Ф7] / не проверено |
| Поток заявок | да / да [К12, Б7] / не проверено | да / да [К12, Т3] / не проверено | да / да [К12, Ф7] / не проверено |
| Сверка после рестарта | да / да [К5, Б6] / не проверено | да / да [К5, Т3] / не проверено | да / да [К5, Ф7] / не проверено |
| Свои сделки, история | да / да [К13, Б8] / не проверено | да / да [К13, Т4] / не проверено | да / да [К13, Ф8] / не проверено |
| Свои сделки, поток | да / да [К13, Б7] / не проверено | да / да [К13, Т4] / не проверено | да / да [К13, Ф8] / не проверено |
| Портфель снимком | да / да [К6] / не проверено | да / да [К6, Т5] / не проверено | да / да [К6, Ф9] / не проверено |
| Портфель потоком | да / да [К6] / не проверено | да / да [К6, Т6] / не проверено | да / да [К6, Ф9] / не проверено |
| Стоп-заявки | да / да [К7, Б9] / не проверено | да / да [К7, Т7] / не проверено | да / да [К7, Ф10] / не проверено |
| Переполнение буфера (`Overflow`, `Degraded` до сверки) | да / да [Б11] / не проверено | да / да [Т9] / не проверено | да / да [Ф12] / не проверено |
| `Resync` стакана после обрыва потока | да / да [Б12] / не проверено | да / да [Т10] / не проверено | да / да [Ф2] / не проверено |
| `RateLimited` на постановке не тратит `cid` | да / да [Б13] / не проверено | да / да [Т11] / не проверено | да / да [Ф13] / не проверено |
| Доступ ко всему API (`raw()`) | да / да [Р] / не проверено | да / да [Р] / не проверено | да / да [Р] / не проверено |
| Несколько счетов | да, токен на счет / да [Б10] / не проверено | да / да [Т8] / не проверено | да / да [Ф11] / не проверено |
| Деньги, цены и количества точными числами (`Price`, `Qty`, `Money`) | да / да [Ч1, Ч2] / не проверено | да / да [Ч1, Ч3] / не проверено | да / да [Ч1, Ч4] / не проверено |
| Проверка цены по шагу до отправки (`price_grid`, `price_off_step`) | да / да [Ш1] / не проверено | да / да [Ш1] / не проверено | да / да [Ш1] / не проверено |

**Клеток "mock нет" не осталось.** Там, где у брокера возможности нет (замена заявки у Финама), в клетке "нет у брокера" и
проверка, что коннектор честно отвечает `Unsupported`.

## Что стоит помнить

- **Живое подтверждение есть только у протокола БКС** (`brokapi-bcs-protocol`): авторизация, справочник,
  свечи, WS стакана, котировок и сделок, портфель, лимиты, создание, статус и отмена заявки (по старым формам), WS исполнения.
  Актуальные методы заказов, `orders/events/ws` и условные заявки вживую не пробовались. Коннекторы вживую не запускались ни
  у кого. Живые тесты БКС написаны (`crates/bcs/tests/live.rs`, в том числе `live_stop_order_roundtrip`) и только собираются.
- **БКС переведен на актуальные методы (19.10).** Отмена, изменение и создание идут синхронными `POST /orders/cancel/sync`,
  `/orders/edit/sync`, `/orders/sync`, статус заявки - `GET /orders`, список - `POST /orders/search`, поток статусов и
  исполнений - один канал `orders/events/ws`. Deprecated-формы коннектор не вызывает (mock отвечает на них 404 и считает
  обращения, тест это проверяет), в `brokapi-bcs-protocol` и через `raw()` они остались. Как они ведут себя вживую, не проверялось:
  `live_checklist.md`, БКС, вопросы 10 и 11.
- **Стоп-заявки у БКС теперь есть в коннекторе** (условная заявка `orderType = 5`, 19.10). Документация молчит про срок жизни,
  направление срабатывания, смысл `price` и отмену пары TP+SL. Выбрано безопасное толкование (разобрано в `crates/bcs/src/map/stop.rs`):
  принимается только срок "до отмены" и поле срока не шлется, направление решает брокер, пары TP+SL не создаются, потока
  стоп-заявок нет. Все это вопросы на живой прогон ([Живые прогоны](live.md), БКС, вопросы 12-14).
- **Mock БКС теперь по документации, а не по догадкам.** Страницы заявок, ленты сделок, свечей и расписания на сайте БКС
  перешли на отдельные JSON-схемы, которые лежат только в JS сайта (мы достаем их из JS сайта и сверяем с ними mock). Сверка с ними нашла расхождения коннектора и исправила: лента сделок (запрос без `limit`, ответ
  `{records}`), таймфреймы `D` и `W` (не `D1`, `W1`.`M10` у БКС нет), расписание (нужен `ticker`, времена суток), поиск по виду
  (`STOCK`, `BONDS`), история своих сделок (плоская запись), статусы списка заявок (1 снята, 2 исполнена, 3 активна). Mock проверяет
  запросы по этим схемам. Это не заменяет живой прогон: схемы сайта тоже могут расходиться с сервером.
- **Т-Банк: cid стоп-заявки не возвращается брокером.** Связь cid и номера хранится в памяти процесса, после рестарта в списке
  `cid` пустой и снимать стоп надо по номеру брокера.
- **Финам: тейк-профит без цены после срабатывания**, срок "до даты" только у тейк-профита, стоп-заявки в `active_orders` и
  `reconcile` не показываются (для них свой список).
- **Что вообще ни разу не проверено на настоящем API**, любой брокер: поведение при повторе `cid` (идемпотентность), эхо `cid`
  у Финама, лимиты, срок жизни токенов и потоков. Вопросы по каждому брокеру: `live_checklist.md`.

## Расшифровка сносок

**Общие сценарии, `[К]`.** Живут в `crates/conformance/src/lib.rs`, запускаются из `crates/broker-<брокер>/tests/conformance.rs`.

- К1 `order_book_snapshot`: стакан REST, глубина, сортировка, стакан не пересечен.
- К2 `order_book_stream`: событие стакана приходит с меткой коннектора и подпиской, отписка (`unsubscribe`).
- К3 `instrument_lookup`: поиск по тикеру и параметры торговли (лот, шаг).
- К4 `place_and_cancel`, `rejected_order_does_not_block_the_instrument`: постановка, `order_state` по cid, отмена, отказанная заявка не блокирует бумагу.
- К5 `reconcile`: пустой список, неизвестный cid попадает в `closed_offline`.
- К6 `portfolio`: снимок и событие портфеля после подписки.
- К7 `stop_order_far_from_market` (у всех трех: поставить, увидеть в списке с параметрами, снять по cid и по номеру брокера, стоп-лимит без цены - отказ). Сценарий `stop_orders_unsupported` остался в `brokapi-conformance` для будущего брокера без стопов, сейчас его никто не гоняет.
- К8 `market_data_history`: лента сделок (есть, не больше `limit`, цены и количества положительные), свечи (OHLC согласованы, свеча закрывается не раньше открытия), статус торгов (не `Unknown`), расписание (есть сессии, начало раньше конца).
- К9 `market_data_streams`: после подписки приходят сделка рынка, котировка и свеча, для нужного инструмента, с источником и номером подписки.
- К10 `active_orders_lifecycle`: поставленная заявка в `active_orders` под своим cid с инструментом, стороной, количеством и ценой, после отмены ее там нет.
- К11 `replace_order`: где у брокера есть замена - заявка уходит под новый cid с новыми количеством и ценой, старая не активна. Где нет (Финам) - `Unsupported` и заявка не тронута.
- К12 `order_state_events`: после `subscribe_order_state` постановка дает событие с нашим cid (не отказ), отмена дает `Cancelled` с тем же cid, события на своей подписке.
- К13 `executions`: история на пустом счете читается, после исполнения заявки приходит `Event::Execution` с нашим cid, ценой, количеством и номером сделки, история не пуста. Как mock исполняет заявку, передает адаптер.

**Точные числа, `[Ч]` и `[Ш]` (20.10).**

- Ч1 `crates/core/src/types/number.rs` (`tests`): примеры, где `f64` расходится с сеткой (`244.3 % 0.01`, `109.32 / 0.01`, `101.05 / 0.05`, `0.1 + 0.2`), округление вниз, вверх и к ближайшему, текст и JSON хранят запись числа (`244.30`), случайные проверки: число лежит на сетке и в вилке, текст и JSON туда и обратно, суммы точные.
- Ч2 БКС: `crates/bcs-protocol/tests/numbers.rs` (число JSON сохраняет знаки, `244.30` и `0.0001`, 19 знаков, число уходит в заявке JSON-числом, случайные числа туда и обратно), `http/models.rs::last_trades_tests::the_untagged_wrapper_keeps_the_digits` (число внутри `untagged`), `utils/precision.rs` (округление и случайная проверка вилки).
- Ч3 Т-Банк: `crates/tbank-protocol/src/money.rs` (`units` и `nano` туда и обратно на случайных числах, границы, больше 9 знаков и переполнение - отказ), `crates/tbank/src/ids.rs` (примеры), `map/execution.rs` (комиссия делится между сделками, части дают сумму точно).
- Ч4 Финам: `crates/finam/src/util.rs` (строка Decimal туда и обратно на случайных числах, грамматика, запись сохраняется, `Money` туда и обратно), `connector_tests.rs::directory_trading_params_status_and_schedule` (шаг из `min_step` и `decimals` целыми: `0.01`).
- Ш1 `a_price_off_the_step_is_refused_before_anything_is_sent` в `orders_api.rs` (БКС), `connector_tests.rs` (Т-Банк и Финам): цена не на шаге - `Rejected { price_off_step }`, на брокера ничего не ушло, тот же `cid` потом проходит. `crates/runtime/src/price_grid.rs` - таблица шагов.

**БКС** (`crates/bcs/tests/`).

Mock БКС (`tests/mock_server/server.rs`) с состоянием: заявки и сделки по аккаунтам (аккаунт - токен доступа), события на `orders/events/ws`,
условные заявки. Deprecated-пути отвечают 404 и считаются. Запросы рынка проверяются по схемам сайта (таймфреймы, обязательный `ticker`
расписания, типы инструментов, нет `limit` у ленты сделок).

- Б1 `mock_integration.rs::full_stack_happy_path`, `ws_reconnect_after_controlled_disconnect`.
- Б2 `md_api.rs` (6 тестов): лента сделок (новейшие `limit`, старые первыми, инструмент из запроса), свечи (метки `D`/`W`, `M10` - `Unsupported` без отправки), статус и расписание (интервалы суток в московском времени, закрытый перерыв не сессия, `"MOEX"` - ошибка), поиск по тикеру, ISIN и виду, `get`, потоки сделок, котировок и свечей со своими номерами подписок.
- Б3 `md_api.rs::directory_lookups_by_ticker_isin_and_kind` (карточка `get`, торговые параметры), `find_by_kind_reads_every_page` (250 записей читаются тремя страницами, ровно 100 - двумя, 7 - одной).
- Б4 `orders_api.rs::commands_use_the_current_methods_and_never_the_deprecated_ones` (постановка через `/orders/sync`, отмена через `/orders/cancel/sync`, изменение через `/orders/edit/sync`, статус через `GET /orders`, ни одного обращения к deprecated), `a_refusal_is_rejected_whether_it_is_http_400_or_status_8_in_a_200_answer`, `cancel_tells_the_truth_about_what_happened`.
- Б5 `orders_api.rs::replace_moves_the_order_and_fills_in_what_the_caller_left_out` (количество и цена из текущей заявки, у частично исполненной нужно количество, отказ цены оставляет оригинал).
- Б6 `orders_api.rs::active_orders_name_orders_this_process_did_not_place_by_their_real_cid` (cid списка восстанавливается одним запросом по биржевому номеру и запоминается, `reconcile`: подтвержденные, чужие, закрытые).
- Б7 `orders_api.rs::order_events_stream_follows_place_fill_and_cancel_with_our_cid`, `mock_integration.rs::executions_stream_from_the_order_events_socket`, `sansio_tests.rs` (сообщение канала без полей исполнения, частичное исполнение).
- Б8 `orders_api.rs::own_trades_history_reads_the_flat_records_of_the_docs` (плоская запись, `cid` пуст: в записи его нет).
- Б9 `orders_api.rs::stop_orders_roundtrip_with_the_wire_the_docs_describe`, `stop_order_input_is_refused_before_anything_is_sent`, `map/stop.rs` (тесты тела запроса), `live.rs::live_stop_order_roundtrip` (только сборка).
- Б11 `mock_integration.rs::lossless_overflow_is_visible_and_degrades_until_reconcile` (буфер на два события, канал без чтения: один `Overflow`, `place` и `cancel` дают `Degraded`, после `reconcile` торговля открыта).
- Б14 `mock_integration.rs::reconnect_pause_is_a_setting_of_the_connector` (короткая пауза в настройке возвращает стакан за доли секунды), `brokapi-bcs-protocol` `ws/session.rs` (значения по умолчанию), `brokapi-tbank` `stream.rs::backoff_grows_and_caps`.
- Б12 `mock_integration.rs::reconnect_sends_resync_before_the_new_order_book` (порядок: снимок, `Resync` с нашей подпиской и инструментом, снимок), `core/md.rs` (первое подключение `Resync` не дает).
- Б13 `orders_api.rs::a_refusal_before_the_wire_leaves_the_cid_usable` (отказ входа и `RateLimited` на `place` и `place_stop`: на брокера ничего не ушло, тот же `cid` потом проходит, после отправки второй раз - `duplicate_place`).
- Б10 `orders_api.rs::two_accounts_with_two_tokens_see_only_their_own_orders_and_events`: два коннектора с разными токенами, у каждого свои заявки, стопы, события и сверка.

**Т-Банк** (`crates/tbank/tests/`).

- Т1 `connector_tests.rs`: `reference_and_history_calls`, `trades_and_candles_stream`, `quotes_and_book_of_one_instrument_use_one_wire_subscription`.
- Т2 `connector_tests.rs::replace_moves_the_order_to_the_new_cid`.
- Т3 `connector_tests.rs::order_state_events_follow_place_cancel_and_fill`, `reconcile_sorts_confirmed_closed_and_foreign`.
- Т4 `connector_tests.rs::own_trades_stream_and_history`.
- Т5 `connector_tests.rs::portfolio_reads`.
- Т6 `multi_account.rs` (подписка на портфель), `sansio_tests.rs::portfolio_subscription_needs_both_streams_and_emits_cash_and_positions`.
- Т7 `stop_orders.rs` (14 тестов: сверка до начала, параметры туда и обратно, чужой счет, неизвестный cid, повтор cid, неясный ответ, локальные проверки, потока нет, длинный список читается по периодам, короткий одним запросом, неделимый список дает понятный отказ, 80007 это `Rejected`, а 80002 это `RateLimited`). Разбор кодов `RESOURCE_EXHAUSTED` - тесты в `error.rs`.
- Т9 `connector_tests.rs::overflow_of_a_lossless_subscription_degrades_until_reconcile`.
- Т10 `connector_tests.rs::broken_stream_is_reopened_and_subscriptions_restored`, `sansio_tests.rs` (`SubscriptionReset`).
- Т11 `connector_tests.rs::rate_limited_place_leaves_the_cid_usable`.
- Т8 `multi_account.rs::two_connectors_one_token_see_only_their_own_account`, `connector_tests.rs::one_limiter_for_both_connectors_of_a_user`.

**Финам** (`crates/finam/tests/`).

- Ф1 `connector_tests.rs::rest_book_is_best_first_and_depth_limited`.
- Ф2 `connector_tests.rs::dropped_book_stream_is_reopened_with_resync`, `increment_without_a_base_forces_a_fresh_stream`.
- Ф3 `connector_tests.rs::quotes_trades_and_candles`.
- Ф4 `connector_tests.rs::m10_candles_are_built_from_m5`.
- Ф5 `connector_tests.rs::directory_trading_params_status_and_schedule`, лот для торговли (`trade_lot_size`) - `trade_lot_size_replaces_the_lot_of_the_directory_only_when_it_is_above_zero` и `a_refused_params_call_keeps_the_lot_of_the_directory_and_a_quota_error_is_returned`, конец периода в запросах истории - `every_history_request_has_both_ends_of_the_period`.
- Ф6 `connector_tests.rs::commands_wait_for_reconcile_and_replace_is_unsupported` (метода нет в API Финама, коннектор отвечает `Unsupported`).
- Ф7 `connector_tests.rs::order_state_stream_delivers_updates_with_our_cid`, `reconcile_sorts_confirmed_closed_offline_and_foreign`.
- Ф8 `connector_tests.rs::executions_history_and_stream`.
- Ф9 `connector_tests.rs::portfolio_stream_pushes_updates_and_positions_balances_come_from_one_call`.
- Ф10 `stop_orders.rs` (9 тестов: сверка до начала, `PlaceOrder` с нужным условием срабатывания, тейк-профит через `PlaceSLTPOrder`, чужой стоп без cid, обычные списки стопов не показывают, повтор cid, неясный ответ, потока нет). Процент вместо штук в чужой SL/TP-заявке - тест `map/stop.rs`.
- Ф12 `connector_tests.rs::lossless_overflow_emits_overflow_degrades_and_reconcile_recovers`.
- Ф13 `connector_tests.rs::rate_limited_place_leaves_the_cid_usable`.
- Ф11 `connector_tests.rs::two_accounts_share_one_jwt_and_see_only_their_own_data`.

**`raw()`, `[Р]`.** `crates/bcs/tests/raw_access.rs`, `crates/tbank/tests/raw_access.rs`, `crates/finam/tests/raw_access.rs`. Проверяют:
вызов проходит авторизацию, тратит квоту по имени метода, отказ по квоте не доходит до брокера, счетчик общий с соседним коннектором,
метод без маршрута идет по маршруту по умолчанию. У Финама еще обновление токена при чтении и отсутствие повтора у команды, у Т-Банка и БКС
неясный ответ команды - `Unknown`. У БКС еще открытие канала WebSocket через `raw()`.
