# Готовность: что умеет библиотека по брокерам

Дата: 2026-10-19 (обновлено 20.10: деньги, цены и количества - точные числа, цена проверяется по шагу до отправки. Вечером 19.10 БКС выровнен с Т-Банком и Финамом по `Overflow`, `Resync`, `cid` и квоте). Строка - возможность общего интерфейса, колонка - брокер. В каждой клетке три отметки:

**код / mock / live**

- **код** - возможность написана в коннекторе.
- **mock** - есть тест на mock-сервере, который ее проверяет. Тест указан в сноске. Нет теста - пишу "нет".
- **live** - прогон на настоящем API. **Везде "не проверено"**: токенов не было, живой прогон коннекторов не делался.

Значения: `да`, `нет у брокера` (в API брокера этого нет), `нет` и причина в сноске. Таблица заполнена по тестам и коду, без
оптимизма: "mock да" стоит, только если тест действительно эту возможность проверяет.

Короткие названия тестов расшифрованы в конце. Общие сценарии (`brokapi-conformance`) гоняет каждый коннектор против
своего mock, это `[К]` в сносках.

| Возможность | БКС | Т-Банк | Финам | АЛОР |
|---|---|---|---|---|
| Стакан снимком | да / да [К1] / не проверено | да / да [К1] / не проверено | да / да [К1, Ф1] / не проверено | да / да [К1] / не проверено |
| Стакан потоком | да / да [К2, Б1] / не проверено | да / да [К2] / не проверено | да / да [К2, Ф2] / не проверено | да / да [К2, А1] / не проверено |
| Сделки рынка (история и поток) | да / да [К8, К9, Б2] / не проверено | да / да [К8, К9, Т1] / не проверено | да / да [К8, К9, Ф3] / не проверено | да / да [К8, К9, А2] / не проверено |
| Котировки потоком | да / да [К9, Б2] / не проверено | да / да [К9, Т1] / не проверено | да / да [К9, Ф3] / не проверено | да / да [К9, А2] / не проверено |
| Свечи (история и поток) | да / да [К8, К9, Б2] / не проверено | да / да [К8, К9, Т1] / не проверено | да / да [К8, К9, Ф3, Ф4] / не проверено | да / да [К8, К9, А2] / не проверено |
| Справочник (поиск, карточка) | да / да [К3, Б3] / не проверено | да / да [К3, Т1] / не проверено | да / да [К3, Ф5] / не проверено | да / да [К3, А3] / не проверено |
| Торговые параметры (лот, шаг цены) | да / да [К3, Б3] / не проверено | да / да [К3, Т1] / не проверено | да / да [К3, Ф5] / не проверено | да / да [К3, А3] / не проверено |
| Статус торгов | да / да [К8, Б2] / не проверено | да / да [К8, Т1] / не проверено | да / да [К8, Ф5] / не проверено | да / да [К8, А3] / не проверено |
| Расписание | да / да [К8, Б2] / не проверено | да / да [К8, Т1] / не проверено | да / да [К8, Ф5] / не проверено | нет у брокера / да, отказ проверен [А4] / нет у брокера |
| Постановка заявки | да / да [К4, Б4] / не проверено | да / да [К4] / не проверено | да / да [К4] / не проверено | да / да [К4, А5] / не проверено |
| Отмена заявки | да / да [К4, Б4] / не проверено | да / да [К4] / не проверено | да / да [К4] / не проверено | да / да [К4, А5] / не проверено |
| Изменение заявки | да / да [К11, Б5] / не проверено | да / да [К11, Т2] / не проверено | нет у брокера / да, отказ проверен [К11, Ф6] / нет у брокера | да / да [К11, А6] / не проверено |
| Статус заявки по cid | да / да [К4, Б4] / не проверено | да / да [К4] / не проверено | да / да [К4] / не проверено | да / да [К4, А5] / не проверено |
| Активные заявки | да / да [К10, Б6] / не проверено | да / да [К10, Т3] / не проверено | да / да [К10, Ф7] / не проверено | да / да [К10, А7] / не проверено |
| Поток заявок | да / да [К12, Б7] / не проверено | да / да [К12, Т3] / не проверено | да / да [К12, Ф7] / не проверено | да / да [К12, А8] / не проверено |
| Сверка после рестарта | да / да [К5, Б6] / не проверено | да / да [К5, Т3] / не проверено | да / да [К5, Ф7] / не проверено | да / да [К5, А7] / не проверено |
| Свои сделки, история | да / да [К13, Б8] / не проверено | да / да [К13, Т4] / не проверено | да / да [К13, Ф8] / не проверено | да / да [К13, А9] / не проверено |
| Свои сделки, поток | да / да [К13, Б7] / не проверено | да / да [К13, Т4] / не проверено | да / да [К13, Ф8] / не проверено | да / да [К13, А8] / не проверено |
| Портфель снимком | да / да [К6] / не проверено | да / да [К6, Т5] / не проверено | да / да [К6, Ф9] / не проверено | да / да [К6, А10] / не проверено |
| Портфель потоком | да / да [К6] / не проверено | да / да [К6, Т6] / не проверено | да / да [К6, Ф9] / не проверено | да / да [К6, А10] / не проверено |
| Стоп-заявки | да / да [К7, Б9] / не проверено | да / да [К7, Т7] / не проверено | да / да [К7, Ф10] / не проверено | да / да [К7, А11] / не проверено |
| Переполнение буфера (`Overflow`, `Degraded` до сверки) | да / да [Б11] / не проверено | да / да [Т9] / не проверено | да / да [Ф12] / не проверено | да / да [А12] / не проверено |
| `Resync` стакана после обрыва потока | да / да [Б12] / не проверено | да / да [Т10] / не проверено | да / да [Ф2] / не проверено | да / да [А1] / не проверено |
| `RateLimited` на постановке не тратит `cid` | да / да [Б13] / не проверено | да / да [Т11] / не проверено | да / да [Ф13] / не проверено | да / да [А13] / не проверено |
| Количество в единицах инструмента, `invalid_qty` до отправки, без округления | да / да [Н1] / не проверено | да / да [Н1, Н2] / не проверено | да / да [Н1] / не проверено | да / да [Н1, А5] / не проверено |
| Повтор `cid` не уходит в сеть (`duplicate_place`) | да / да [Н3] / не проверено | да / да [Н3] / не проверено | да / да [Н3] / не проверено | да / да [Н3] / не проверено |
| `Unknown` на постановке без ответа, заявка находится через `order_state` | да / да [Н4] / не проверено | да / да [Н4] / не проверено | да / да [Н4] / не проверено | да / да [Н4] / не проверено |
| Шлюз проверяется еще раз после очереди по бумаге | да / да [Н5] / не проверено | да / да [Н5] / не проверено | да / да [Н5] / не проверено | да / да [Н5] / не проверено |
| Обрыв потоков заявок и исполнений: `Event::Reconnected`, `Degraded` до сверки | да / да [Н6] / не проверено | да / да [Н6] / не проверено | да / да [Н6] / не проверено | да / да [Н6] / не проверено |
| Идентификатор инструмента другого брокера: `Config` без сети | да / да [Н7] / не проверено | да / да [Н7] / не проверено | да / да [Н7] / не проверено | да / да [Н7] / не проверено |
| Неизвестный инструмент и заявка: `Rejected` с кодом `not_found` | да / да [Н8] / не проверено | да / да [Н8] / не проверено | да / да [Н8] / не проверено | да / да [Н8] / не проверено |
| История своих сделок постранично или `range_too_long`, без молчаливой обрезки | да / да [Н9] / не проверено | да, 20 страниц / да [Т4] / не проверено | да / да [Н9] / не проверено | да / да [Н9, А9] / не проверено |
| Цена исполнения по приросту накопленных итогов; статус 6 «отменяется» не отмена; сверка по `GET` | да / да [Н10] / не проверено | нет у брокера / нет у брокера | нет у брокера / нет у брокера | нет у брокера |
| Доступ ко всему API (`raw()`) | да / да [Р] / не проверено | да / да [Р] / не проверено | да / да [Р] / не проверено | да / да [Р] / не проверено |
| Несколько счетов | да, токен на счет / да [Б10] / не проверено | да / да [Т8] / не проверено | да / да [Ф11] / не проверено | да / да [А14] / не проверено |
| Деньги, цены и количества точными числами (`Price`, `Qty`, `Money`) | да / да [Ч1, Ч2] / не проверено | да / да [Ч1, Ч3] / не проверено | да / да [Ч1, Ч4] / не проверено | да / да [Ч1, А15] / не проверено |
| Проверка цены по шагу до отправки (`price_grid`, `price_off_step`) | да / да [Ш1] / не проверено | да / да [Ш1] / не проверено | да / да [Ш1] / не проверено | да / да [Ш1, А16] / не проверено |

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
- **АЛОР.** Коннекторы написаны по описанию API брокера и проверены на имитации. Объем уровней стакана, ленты и свечей, повтор `X-REQID`, регистр значений в стоп-заявках и срок стоп-заявки без времени окончания документация не называет или называет неоднозначно: вопросы на живой прогон ([Живые прогоны](live.md), раздел АЛОР).
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
- К7 `stop_order_far_from_market` (у всех: поставить, увидеть в списке с параметрами, снять по cid и по номеру брокера, стоп-лимит без цены - отказ). Сценарий `stop_orders_unsupported` остался в `brokapi-conformance` для будущего брокера без стопов, сейчас его никто не гоняет.
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

**АЛОР** (`crates/alor/tests/`, `crates/alor-protocol/tests/`).

Имитация АЛОРа (`brokapi-mocks`, модуль `alor`) с состоянием: заявки, стоп-заявки и сделки по аккаунтам (аккаунт - токен доступа), события на подписках WebSocket (`/ws`),
канал `/cws`, повтор `X-REQID` получает копию первого ответа. Она написана по описанию API брокера (OpenAPI и AsyncAPI) и сверки с записанными ответами брокера не имеет.

- А1 `conformance.rs::market_data`, `the_order_book_is_resynced_after_a_cut`, `core/md.rs` (после второго подключения у каждого стакана `Resync`, у котировок нет).
- А2 `conformance.rs::market_data_history_and_streams`, `connector_tests.rs::the_last_trades_are_oldest_first_and_cut_to_the_limit`, `core/md.rs::trades_of_the_history_and_repeats_are_dropped`.
- А3 `conformance.rs::instruments`, `map/instrument.rs` (лот, шаг, вид, коды статуса торгов).
- А4 `conformance.rs::market_data_history_and_streams`: `schedule` отвечает `Unsupported`, без сети.
- А5 `conformance.rs::orders`, `connector_tests.rs::the_cid_goes_to_the_broker_as_the_request_id_and_as_the_comment_and_pieces_become_lots` (штуки в лоты и обратно, `cid` в комментарии и в `X-REQID`), `a_fraction_is_refused_even_when_the_lot_is_not_known`.
- А6 `conformance.rs::own_orders_events_active_replace_and_executions` (замена под новым `cid`), `connector_tests.rs::a_market_order_cannot_be_replaced`.
- А7 `conformance.rs::orders`, `connector_tests.rs::an_order_of_another_program_is_listed_with_a_made_up_cid_and_can_be_cancelled_by_it`.
- А8 `conformance.rs::own_orders_events_active_replace_and_executions`, `connector_tests.rs::a_fill_comes_as_an_order_event_and_an_execution_with_the_cid`, `core/ud.rs`.
- А9 `conformance.rs::the_history_of_trades_is_read_to_the_last_page` (250 сделок одним ответом), `a_history_that_fills_a_page_is_an_error_not_a_silent_cut` (1200).
- А10 `conformance.rs::portfolio`, `connector_tests.rs::positions_carry_the_main_board_and_money_is_a_balance`.
- А11 `conformance.rs::stop_orders`, `map/stop.rs` (условие срабатывания, срок, отказы до отправки).
- А12 `wave2.rs::lossless_overflow_is_visible_and_degrades_until_reconcile`.
- А13 `wave2.rs::a_refusal_before_the_wire_leaves_the_cid_usable` (для `place` и `place_stop`).
- А14 `wave2.rs::two_accounts_with_two_tokens_see_only_their_own_orders`, `one_limiter_for_both_connectors_of_a_login`.
- А15 `crates/alor-protocol/tests/numbers.rs` (число JSON сохраняет знаки, 19 знаков, случайные числа туда и обратно, заявка уходит JSON-числом).
- А16 `connector_tests.rs::a_price_off_the_step_is_refused_before_anything_is_sent_and_the_cid_stays_usable`.
- Токены и WebSocket: `crates/alor-protocol/tests/http_auth.rs` (токен берется один раз, чтение повторяется один раз после 401 с новым токеном, команда после 401 не повторяется, отказанный refresh-токен это `Auth`, токен не в `Debug`), `tests/endpoints_wire.rs` (каждый из методов клиента шлет нужный глагол, путь, запрос и заголовок `X-REQID`).

**`raw()`, `[Р]`.** `crates/bcs/tests/raw_access.rs`, `crates/tbank/tests/raw_access.rs`, `crates/finam/tests/raw_access.rs`, `crates/alor/tests/raw_access.rs`. Проверяют:
вызов проходит авторизацию, тратит квоту по имени метода, отказ по квоте не доходит до брокера, счетчик общий с соседним коннектором,
метод без маршрута идет по маршруту по умолчанию. У Финама еще обновление токена при чтении и отсутствие повтора у команды, у Т-Банка и БКС
неясный ответ команды - `Unknown`. У БКС и АЛОРа еще открытие канала WebSocket через `raw()` (у АЛОРа и торгового канала `/cws`).

## Проверки по разбору кода 21.10

Общие сценарии `brokapi-conformance` (в каждом коннекторе их запускает `tests/conformance.rs`), отдельные тесты - в `tests/fixes.rs` крейта.

- Н1 `invalid_qty`: ноль, отрицательное, дробное, не кратное лоту отказаны, `cid` свободен, на брокера ушла одна заявка. БКС: `0.4` и `2.5` не округляются (`fixes`/`conformance`), Т-Банк: `an_order_in_units_is_sent_in_lots_and_comes_back_in_units`, `a_quantity_that_is_not_whole_lots_never_reaches_the_broker`, Финам: `the_lot_is_checked_only_when_it_is_known`.
- Н2 Т-Банк в штуках: стакан, лента, свечи, котировки, исполнения, стоп-заявки, замена, чужая заявка (`the_book_trades_and_candles_are_in_units`, `a_fill_is_in_units_in_the_stream_and_in_the_history`, `a_replace_takes_units_and_keeps_the_quantity_by_default`, `an_order_event_whose_lot_is_unknown_is_a_loss_the_bot_is_told_about`).
- Н3 `duplicate_place`. Н4 `place_without_answer_is_unknown`. Н5 `gate_checked_after_the_queue`. Н6 `stream_cut_degrades_until_reconcile` (плюс модульные тесты ядер: `a_reopened_stream_of_orders_or_fills_is_announced_once`, `the_core_announces_a_cut_from_the_second_connect_on`, `a_reconnected_notice_degrades_and_comes_in_order_with_the_events`).
- Н7 `foreign_instrument_id`. Н8 `unknown_instrument`, `order_state_unknown_cid`. Н9 `executions_history_is_complete` и тесты на 250 сделок (БКС 3 страницы по 100, Финам 3 запроса по 100) и на предел.
- Н10 БКС (`fixes.rs`): `two_fills_at_different_prices_give_two_prices`, `fills_at_different_prices_arrive_with_their_own_prices`, `a_cancel_that_is_only_started_is_not_a_cancel`, `an_order_that_fills_while_it_is_cancelling_is_filled`, `an_order_that_is_alive_by_get_but_missing_from_the_list_is_confirmed`.
- `Resync` стакана: `order_book_resync` у всех.

## Проверки второй волны 21.10

Отдельные тесты лежат в `tests/wave2.rs` крейтов `runtime`, `bcs`, `tbank`, `finam`, в `bcs-protocol/tests/security.rs` и в модулях рядом с кодом. Все на имитациях.

- В1 Токены не в `Debug`: `debug_output_hides_every_token`, `the_configs_of_the_connectors_do_not_print_the_token` (БКС), `the_jwt_header_is_sensitive_and_is_sent_bare_or_with_bearer` (Финам), токен Т-Банка скрыт и в `TbankConfig`.
- В2 Команда БКС не повторяется после 401, чтение повторяется, токен обновляется заранее: `a_command_is_sent_once_even_after_a_401`, `a_read_is_repeated_once_after_a_401`, `a_token_that_is_about_to_expire_is_renewed_before_it_is_used`.
- В3 `.env` только из текущего каталога или по пути, адреса из окружения только `https://`: `the_env_file_is_read_from_the_given_path_only`, `a_plain_text_endpoint_in_the_environment_is_refused_...` (Т-Банк), `the_api_url_of_the_environment_must_be_https` (Финам).
- В4 Лимиты БКС по таблице брокера: `the_limits_follow_the_table_of_the_broker`, `a_burst_of_orders_is_stopped_before_the_broker_would_stop_it`.
- В5 Финам: `a_rejected_token_on_a_command_starts_the_renewal_but_does_not_repeat_the_command`, `an_auth_call_that_never_answers_ends_with_a_deadline`, `the_lifetime_is_the_shorter_of_the_details_and_the_claims`, `stop_and_sltp_orders_do_not_show_up_in_the_stream_of_ordinary_orders`, `a_fill_of_a_foreign_order_keeps_no_cid_even_after_the_order_was_seen`, `a_resync_does_not_overtake_the_data_that_is_already_buffered`.
- В6 Т-Банк: `the_history_of_trades_asks_for_margin_and_card_trades_too`, `a_trade_before_the_answer_gets_its_cid_when_the_answer_comes` (и три соседних), `a_rejected_token_on_a_stream_fails_the_subscription_at_once`, `the_limit_of_open_streams_fails_the_subscription_and_a_later_one_can_try_again`, `an_extra_root_certificate_is_accepted_only_when_it_is_a_pem`, книга стопов (`stop_book_tests` рядом с кодом).
- В7 Ядро: `conflate_keeps_the_latest_event_of_every_kind`, автомат заявки в `sansio` (`news_that_is_faster_than_the_answer_is_taken`, `a_late_answer_changes_nothing`, `an_older_snapshot_does_not_take_a_fill_back`, `contradicting_news_is_an_error_and_leaves_the_state`), окно памяти (`finished_orders_are_forgotten_after_the_window_and_the_cid_is_free_again`, `the_count_limit_drops_the_oldest_finished_orders_first`), `place` без ожидающего (`a_place_whose_caller_gives_up_still_ends_and_leaves_nothing_in_flight`, Т-Банк и Финам), `a_command_is_retried_only_after_a_rate_limit`.
- В8 БКС: `the_cid_is_the_order_not_the_request_that_caused_the_message`, `a_corrected_report_is_not_a_new_fill`, `an_error_that_names_a_ticker_fails_that_subscription_only` (и три соседних), `the_ids_of_orders_that_finished_long_ago_are_forgotten`.
- В9 Пакет: `the_book_names_the_rust_version_of_the_crate` (версия Rust в книге равна `rust-version` крейта).

## Python

Пакет `brokapi` для Python - тонкая обертка над тем же ядром ([раздел Python](python/index.md)). Правила, коннекторы и сверка у него те
же, поэтому таблица выше верна и для него. Свои проверки у обертки:

| Что | Состояние |
|---|---|
| Подключение к БКС, Т-Банку, Финаму, АЛОРу, чтение (стакан, свечи, портфель, справочник), заявки (`reconcile`, `place`, `cancel`, `order_state`, `active_orders`, `replace` где умеет брокер), события, исполнения | есть, проверено тестами `pytest` против имитаций всех брокеров |
| Точные числа: `Decimal` в обе стороны, `float` в вызове отвергается, время с поясом | проверено тестами |
| Ошибки: текст исключения побуквенно равен тексту `Error` в Rust | проверено тестом на примерах каждого варианта |
| Общий счетчик лимитов у клиентов одного брокера, счетчик чужого брокера отвергается | проверено тестами |
| Команда доходит до конца, если ожидание отменили | проверено тестом (отмена задачи, заявка все равно видна) |
| История свечей по окнам, склейка без дублей, продолжение после сбоя | проверено тестами на имитации БКС |
| Примеры книги (быстрый старт, бот, несколько брокеров, история, правила) | каждый запускается тестом |
| Сборка колеса под Linux и установка в чистое окружение | проверено |
| Колеса под macOS и Windows | настройка сборки есть, не запускалась |
| **Проверка на настоящем брокере через Python** | **не было** |
| Стоп-заявки в Python (`place_stop`, `cancel_stop`, `active_stop_orders`) | проверено тестами на имитациях всех брокеров. Срабатывание стопа проверено на БКС и Финаме, имитация Т-Банка его не умеет |
| `raw()` в Python (любой метод Т-Банка и Финама по имени, БКС по таблице или по пути, АЛОР по глаголу и пути) | проверено тестами на имитациях: чтение, неизвестный метод, лишнее поле, квота общая с интерфейсом. Методы с потоком не отдаются |
| Запись и проигрыш потока событий в Python (`brokapi.record`) | проверено тестами: записанное, прочитанное и проигранное совпадают на имитациях трех брокеров, формат файла как у Rust |

## Покрытие тестами

Измеряется `tools/coverage` (нужны `cargo-llvm-cov` и компонент `llvm-tools-preview`): запускает все тесты рабочей области и
печатает таблицу по крейтам. Считается код библиотеки (`crates/*/src`). Тесты, имитации, общий набор проверок и примеры в число
не входят. Тесты документации (doctest) в число не входят: llvm-cov считает их только на nightly. Строки это самое грубое число,
области (ветки `match`) строже.

| Крейт | Строки до | Строки после | Области до | Области после |
|---|---|---|---|---|
| bcs | 87,23% | 90,15% | 86,79% | 89,18% |
| bcs-protocol | 71,60% | 85,85% | 70,91% | 85,11% |
| core | 85,65% | 89,18% | 89,02% | 90,33% |
| finam | 91,87% | 92,49% | 92,73% | 93,31% |
| finam-protocol | 91,97% | 93,99% | 90,96% | 93,04% |
| runtime | 88,83% | 92,87% | 89,95% | 93,53% |
| sansio | 85,18% | 91,51% | 86,79% | 91,92% |
| tbank | 93,75% | 95,09% | 92,82% | 94,38% |
| tbank-protocol | 81,95% | 89,49% | 80,05% | 88,89% |
| **всего** | **88,77%** | **92,13%** | **88,92%** | **92,04%** |

«До» это конец первой волны исправлений 21.10 (до разбора кода строки были 87,46%), «после» это конец второй. **Цель 95% по строкам
не достигнута**: 92,13% (непокрытых строк 1251 из примерно 15 900). Выше 95% только `tbank`. Тесты второй волны добавлены на поведение
(безопасность БКС, лимиты, автомат заявки, окна памяти, потоки Т-Банка, отмена `place`, чужие исполнения Финама, сообщения канала заявок БКС,
запросы протокола БКС против записывающего сервера, WebSocket-сессия БКС против сервера из теста), не ради цифры.

**Что стоит закрыть в следующей волне.** Файлы с наибольшим числом непокрытых строк. Что именно в них не покрыто, не разбиралось:
это список для чтения отчета `tools/coverage --html`, а не вывод.

| Файл | Непокрытых строк |
|---|---|
| `bcs/src/core/ud.rs` | 64 |
| `bcs-protocol/src/ws/session.rs` | 57 (было 87): сторож молчания сокета на 60 секунд идет по настенным часам, без управления временем его не проверить |
| `bcs-protocol/src/ws/client.rs` | 55 |
| `tbank/src/ud.rs` | 53 |
| `finam/src/ud.rs` | 51 |
| `tbank/src/core/ud.rs` | 36 (было 76) |
| `bcs/src/md.rs` | 36 |
| `tbank/src/md.rs`, `runtime/src/record.rs` | 30 каждый |
| `finam/src/stream.rs`, `bcs/src/ud.rs`, `bcs-protocol/src/http/models.rs` | 27 каждый |
| `bcs/src/map/order.rs` | 26 (было 57) |
