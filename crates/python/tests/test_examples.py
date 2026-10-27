"""Every example of the book is a test: the programs of `examples/` run against the imitations of the brokers."""

from __future__ import annotations

import asyncio
import sys
import types
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid
from pathlib import Path

import pytest

import brokapi
from brokapi import testing

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
sys.path.insert(0, str(EXAMPLES))

import gateway_bot  # noqa: E402
import history  # noqa: E402
import infrastructure  # noqa: E402
import quickstart_bcs  # noqa: E402
import trading_bot  # noqa: E402


async def test_quickstart_reads_then_places_and_cancels(capsys: pytest.CaptureFixture[str]) -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        await quickstart_bcs.run(client, can_trade=True)
    out = capsys.readouterr().out
    assert "order book SBER: best bid" in out
    assert "accepted: broker order id" in out
    assert "order_state: Cancelled" in out


async def test_quickstart_with_a_read_only_token_stops_before_the_order(capsys: pytest.CaptureFixture[str]) -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        await quickstart_bcs.run(client, can_trade=False)
    out = capsys.readouterr().out
    assert "the order step is skipped" in out
    assert "accepted" not in out


async def test_the_main_function_connects_by_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    async with await testing.mock_bcs() as mock:
        real_connect = brokapi.connect
        endpoint = mock.settings["endpoint"]

        async def connect(broker: str, **kw: object) -> brokapi.Client:
            return await real_connect(broker, endpoint=endpoint, **kw)  # type: ignore[arg-type]

        monkeypatch.setenv("BCS_REFRESH_TOKEN", "mock-refresh-token")
        monkeypatch.setenv("BCS_IS_WRITE", "true")
        monkeypatch.setattr(quickstart_bcs.brokapi, "connect", connect)
        await quickstart_bcs.main()


async def test_trading_bot_buys_once_and_stops_when_filled(tmp_path: Path) -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        bot = trading_bot.TradingBot(client, "SBER", trading_bot.Journal(tmp_path / "j.jsonl"), Decimal(1))
        task = asyncio.create_task(trading_bot.run(bot))
        await asyncio.wait_for(bot.placed.wait(), 10)
        cid = bot.working
        assert cid is not None
        mock.fill(cid, qty=bot.params.min_qty, price=Decimal("1"))
        await asyncio.wait_for(task, 10)
        assert bot.done and bot.filled == bot.params.min_qty
        assert bot.journal.orders[cid][0] == "done"
        # The journal is a file: a new Journal over it knows the same.
        assert trading_bot.Journal(tmp_path / "j.jsonl").orders[cid] == bot.journal.orders[cid]
        assert trading_bot.Journal(tmp_path / "j.jsonl").alive() == []


async def test_trading_bot_asks_the_broker_after_unknown_and_never_sends_again() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        real_place = client.place
        sent: list[object] = []

        async def lost_answer(**kwargs: object) -> brokapi.OrderAck:
            sent.append(kwargs["cid"])
            await real_place(**kwargs)  # type: ignore[arg-type]
            raise brokapi.Unknown()  # the order is at the broker, the answer is lost

        client.place = lost_answer  # type: ignore[method-assign]
        bot = trading_bot.TradingBot(client, "SBER", trading_bot.Journal(), Decimal(1))
        task = asyncio.create_task(trading_bot.run(bot))
        await asyncio.wait_for(bot.placed.wait(), 10)
        assert len(sent) == 1
        assert bot.working == sent[0]
        assert bot.journal.orders[bot.working][0] == "working"
        mock.fill(bot.working, qty=bot.params.min_qty, price=Decimal("1"))
        await asyncio.wait_for(task, 10)
        assert len(sent) == 1


async def test_trading_bot_counts_an_order_filled_while_it_was_away_and_buys_no_more(tmp_path: Path) -> None:
    journal = tmp_path / "j.jsonl"
    async with await testing.mock_bcs() as mock:
        # The first life: the order stands, then the bot stops without cancelling it (a crash, a restart).
        first = await mock.connect()
        bot = trading_bot.TradingBot(first, "SBER", trading_bot.Journal(journal), Decimal(1))
        await bot.start()
        bot.top_bid = Decimal("200.00")
        await bot.try_buy()
        cid = bot.working
        assert cid is not None
        await first.close()
        # While nobody watches, the order is filled.
        mock.fill(cid, qty=bot.params.min_qty, price=Decimal("198.00"))

        # The second life over the same journal: the reconcile says closed_offline, the bot asks how it ended.
        second = await mock.connect()
        sent: list[object] = []
        real_place = second.place

        async def counted(**kwargs: object) -> brokapi.OrderAck:
            sent.append(kwargs["cid"])
            return await real_place(**kwargs)  # type: ignore[arg-type]

        second.place = counted  # type: ignore[method-assign]
        again = trading_bot.TradingBot(second, "SBER", trading_bot.Journal(journal), Decimal(1))
        await again.start()
        assert again.filled == again.params.min_qty
        assert again.done
        assert again.journal.orders[cid] == ("done", "Filled")
        again.top_bid = Decimal("200.00")
        await again.try_buy()
        assert sent == []  # the budget is bought: no second order
        await second.close()
        # A third life knows it from the journal alone.
        assert trading_bot.Journal(journal).bought() == again.params.min_qty


async def test_trading_bot_treats_transport_on_place_as_unknown() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        bot = trading_bot.TradingBot(client, "SBER", trading_bot.Journal(), Decimal(1))
        await bot.start()
        bot.top_bid = Decimal("200.00")
        real_place = client.place
        sent: list[object] = []

        async def cut(**kwargs: object) -> brokapi.OrderAck:
            sent.append(kwargs["cid"])
            raise brokapi.Transport("request timed out after 30s")  # nothing says whether the order stands

        client.place = cut  # type: ignore[method-assign]
        await bot.try_buy()
        first = sent[0]
        # Not a crash, and not "sending" for ever: the bot asked the broker, which has no such order.
        assert bot.journal.orders[first] == ("done", "unknown, not found at the broker")
        client.place = real_place  # type: ignore[method-assign]
        await bot.try_buy()  # the next signal uses a new cid
        assert bot.working is not None and bot.working != first
        await client.cancel(bot.working)


async def test_trading_bot_handles_a_refusal_and_a_closed_gate() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        bot = trading_bot.TradingBot(client, "SBER", trading_bot.Journal(), Decimal(1))
        await bot.start()
        bot.top_bid = Decimal("200.00")

        real_place = client.place
        outcomes = [brokapi.Rejected("validation", "no money"), brokapi.NotReconciled()]

        async def refusing(**kwargs: object) -> brokapi.OrderAck:
            raise outcomes.pop(0)

        client.place = refusing  # type: ignore[method-assign]
        await bot.try_buy()
        assert bot.working is None and bot.journal.alive() == []
        await bot.try_buy()
        assert bot.need_reconcile and bot.working is None
        client.place = real_place  # type: ignore[method-assign]
        await bot.try_buy()  # reconciles first, then places under a new cid
        assert not bot.need_reconcile and bot.working is not None
        await client.cancel(bot.working)


async def test_a_reconnected_order_stream_makes_the_bot_reconcile() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        bot = trading_bot.TradingBot(client, "SBER", trading_bot.Journal(), Decimal(1))
        await bot.start()
        moment = datetime(2026, 10, 21, tzinfo=timezone.utc)
        envelope = brokapi.Envelope(
            source="bcs-ud", sub=None, ts=brokapi.Timestamps(rx_ns=0), payload=brokapi.Reconnected(sub=7, gap_from=moment)
        )
        await bot.on_event(envelope)
        assert bot.need_reconcile


async def test_an_unknown_event_makes_the_bot_reconcile() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        bot = trading_bot.TradingBot(client, "SBER", trading_bot.Journal(), Decimal(1))
        await bot.start()
        envelope = brokapi.Envelope(
            source="bcs-ud", sub=None, ts=brokapi.Timestamps(rx_ns=0), payload=brokapi.UnknownEvent("BrandNew", {})
        )
        await bot.on_event(envelope)
        assert bot.need_reconcile


async def test_infrastructure_two_lives_of_the_process(tmp_path: Path) -> None:
    journal = tmp_path / "orders.journal"
    async with (
        await testing.mock_bcs() as bcs,
        await testing.mock_tbank() as tbank,
        await testing.mock_finam() as finam,
        await testing.mock_alor() as alor,
    ):
        settings = {
            "bcs": bcs.settings,
            "tbank-a": {**tbank.settings, "account_id": "acc-a"},
            "tbank-b": {**tbank.settings, "account_id": "acc-b"},
            "finam": finam.settings,
            "alor": alor.settings,
        }
        # The first life: an order on every account, then the process goes away without cancelling.
        infra = await infrastructure.Infra.start(settings, journal)
        placed: dict[str, uuid.UUID] = {}
        for name, client in infra.clients.items():
            sber = await client.instrument("SBER")
            params = await client.trading_params(sber)
            cid, error = await infra.place(name, sber, params.min_qty, Decimal("100.00"))
            assert error is None
            placed[name] = cid
        # An order that was never sent: the journal line is there, the order is not.
        ghost = brokapi.new_cid()
        infra.journal.set("bcs", ghost, "sending")
        await infra.close()

        # While the process is down, one order is filled.
        tbank.fill(placed["tbank-a"], account="acc-a")

        # The second life: every account is reconciled with the journal.
        again = await infrastructure.Infra.start(settings, journal)
        try:
            assert again.journal.orders[ghost][1] == "done"  # closed offline: it was never sent
            assert again.journal.orders[placed["bcs"]][1] == "working"
            assert again.journal.orders[placed["finam"]][1] == "working"
            assert again.journal.orders[placed["alor"]][1] == "working"
            assert again.journal.orders[placed["tbank-b"]][1] == "working"
            assert again.clients["tbank-a"].limiter.broker == "tbank"
            filled = await again.clients["tbank-a"].order_state(placed["tbank-a"])
            assert filled.state.kind is brokapi.OrderStateKind.FILLED
            # The reconcile asked how the orders closed offline ended, and the journal says it.
            lines = journal.read_text().splitlines()
            assert f"tbank-a\t{placed['tbank-a']}\tdone\tFilled" in lines
            assert f"bcs\t{ghost}\tdone\tnever reached the broker" in lines
        finally:
            await again.close()


async def test_history_is_downloaded_and_goes_on_where_the_file_ends(tmp_path: Path) -> None:
    async with await testing.mock_bcs(synthetic_candles=True) as mock:
        client = await mock.connect()
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 4, 11, tzinfo=timezone.utc)
        written = await history.download(client, "SBER", brokapi.Timeframe.H1, start, end, tmp_path)
        assert written == 2400
        lines = (tmp_path / "SBER.csv").read_text().splitlines()
        assert len(lines) == 2400
        # A second run has nothing to do.
        assert await history.download(client, "SBER", brokapi.Timeframe.H1, start, end, tmp_path) == 0
        # A crash cut the file in half and left a broken line: the run goes on from the last complete line.
        (tmp_path / "SBER.csv").write_text("\n".join(lines[:1000]) + "\n2026-02-1")
        again = await history.download(client, "SBER", brokapi.Timeframe.H1, start, end, tmp_path)
        assert again == 1400
        text = (tmp_path / "SBER.csv").read_text().splitlines()
        stamps = [datetime.fromisoformat(line.split(",")[0]) for line in text if line.count(",") == 5]
        assert len(stamps) == 2400
        assert stamps == sorted(set(stamps))
        assert stamps[1] - stamps[0] == timedelta(hours=1)





# ---------------------------------------------------------------------------------------------------- stops, raw, record

import raw as raw_example  # noqa: E402
import record as record_example  # noqa: E402
import stops  # noqa: E402


@pytest.mark.parametrize("mock_of", [testing.mock_bcs, testing.mock_tbank, testing.mock_finam, testing.mock_alor])
async def test_stops_example_protects_lists_and_releases(mock_of: object, capsys: pytest.CaptureFixture[str]) -> None:
    async with await mock_of() as mock:  # type: ignore[operator]
        client = await mock.connect()
        await stops.run(client)
    out = capsys.readouterr().out
    assert "stop StopLoss Active at 100" in out
    assert "cancelled, 0 waiting" in out


@pytest.mark.parametrize("mock_of", [testing.mock_bcs, testing.mock_tbank, testing.mock_finam, testing.mock_alor])
async def test_a_lost_answer_of_place_stop_is_looked_up_not_repeated(mock_of: object) -> None:
    """The broker took the stop and the answer was lost: the recipe finds it by its cid and does not place a second one."""
    async with await mock_of() as mock:  # type: ignore[operator]
        client = await mock.connect()
        await client.reconcile()
        sber = await client.instrument("SBER")
        real = client.place_stop
        sent: list[uuid.UUID] = []

        async def counted(**kw: object) -> brokapi.OrderAck:
            sent.append(kw["cid"])  # type: ignore[arg-type]
            return await real(**kw)  # type: ignore[arg-type]

        client.place_stop = counted  # type: ignore[method-assign]
        mock.lose_stop_answers()
        found = await stops.protect_once(client, sber, Decimal("100"), wait=0)
        assert len(sent) == 1  # sent once, never again
        assert found.stop is not None and found.stop.cid == sent[0]  # and found at the broker by its cid
        assert found.unclear == ()
        assert len(await client.active_stop_orders()) == 1  # one stop at the broker, not two
        await stops.release(client, found.stop)
        assert await client.active_stop_orders() == []


async def test_a_stop_that_may_be_ours_is_reported_not_called_nothing() -> None:
    """At T-Bank a stop of another run has no cid. Next to a lost answer it cannot be told from ours: no second stop."""
    async with await testing.mock_tbank() as mock:
        mine = await mock.connect()
        other = await mock.connect(name="other-run")
        for client in (mine, other):
            await client.reconcile()
        sber = await mine.instrument("SBER")
        await stops.protect_once(other, sber, Decimal("100"), wait=0)  # an equal stop, placed by another program

        mock.lose_stop_answers()
        found = await stops.protect_once(mine, sber, Decimal("100"), wait=0)
        assert found.stop is None
        assert len(found.unclear) == 2  # theirs and (probably) ours: no cid on either
        assert all(stop.cid is None for stop in found.unclear)
        assert len(await mine.active_stop_orders()) == 2  # and no third one was placed


async def test_when_nothing_is_at_the_broker_the_recipe_says_so_and_may_be_called_again() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        await client.reconcile()
        sber = await client.instrument("SBER")
        real = client.place_stop

        async def lost_on_the_way(**kw: object) -> brokapi.OrderAck:
            raise brokapi.Unknown()  # the request never reached the broker

        client.place_stop = lost_on_the_way  # type: ignore[method-assign]
        assert await stops.protect_once(client, sber, Decimal("100"), wait=0) == stops.Placement()
        assert await client.active_stop_orders() == []

        client.place_stop = real  # type: ignore[method-assign]
        found = await stops.protect_once(client, sber, Decimal("100"), wait=0)  # a new cid, nothing is doubled
        assert found.stop is not None
        assert len(await client.active_stop_orders()) == 1


async def test_a_refused_stop_leaves_nothing_behind() -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        await client.reconcile()
        sber = await client.instrument("SBER")
        # BCS has no lifetime but "until cancelled": this stays a plain refusal and the helper reports "no stop".
        real = client.place_stop

        async def refused(**kw: object) -> brokapi.OrderAck:
            return await real(**{**kw, "expiry": brokapi.StopExpiry.end_of_day()})  # type: ignore[arg-type]

        client.place_stop = refused  # type: ignore[method-assign]
        assert await stops.protect_once(client, sber, Decimal("100"), wait=0) == stops.Placement()
        assert await client.active_stop_orders() == []


async def test_raw_examples_read_each_broker(
    bcs: tuple[testing.MockBroker, brokapi.Client],
    tbank: tuple[testing.MockBroker, brokapi.Client],
    finam: tuple[testing.MockBroker, brokapi.Client],
    alor: tuple[testing.MockBroker, brokapi.Client],
) -> None:
    assert (await raw_example.bcs_quotes(bcs[1]))["SBER"] > 0
    assert Decimal(str(await raw_example.alor_summary(alor[1]))) > 0
    prices = await raw_example.tbank_prices(tbank[1])
    assert prices["SBER"] == "274.050000000"
    assert Decimal(await raw_example.finam_quote(finam[1])) > 0
    assert await raw_example.raw_read(bcs[1], "GetPortfolio", {}) == await bcs[1].raw("GetPortfolio")


async def test_raw_command_example_does_not_hide_an_unknown_outcome() -> None:
    async with await testing.mock_tbank() as mock:
        client = await mock.connect()

        async def lost(*a: object, **kw: object) -> object:
            raise brokapi.Unknown()

        client.raw = lost  # type: ignore[method-assign]
        with pytest.raises(brokapi.Unknown):
            await raw_example.raw_command(client, "CancelOrder", {"account_id": "acc-a", "order_id": "x"})


async def test_record_example_one_analysis_for_the_live_stream_and_for_the_file(tmp_path: Path) -> None:
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        sber = await client.instrument("SBER")
        qty = (await client.trading_params(sber)).min_qty
        await client.subscribe_order_state()
        await client.subscribe_executions()
        await client.reconcile()
        cid = brokapi.new_cid()
        await client.place(cid=cid, instrument=sber, side=brokapi.Side.BUY, qty=qty, price=Decimal("250.25"))
        mock.fill(cid, qty=qty, price=Decimal("250.25"))

        path = tmp_path / "run.jsonl"
        live = await asyncio.wait_for(record_example.record_run(client, path, count=4), 10)

    assert live.traded == qty * Decimal("250.25")
    assert await record_example.analyze_file(path) == live  # the file says the same as the live run
    assert record_example.sources(path) == {"bcs-ud"}


async def test_the_gateway_bot_survives_a_new_gateway_process() -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway("bcs-main")
        client = await gateway.connect(bot="my-bot")
        bot = gateway_bot.Bot(client)
        task = asyncio.create_task(gateway_bot.run(bot))
        await asyncio.wait_for(bot.placed.wait(), 10)
        cid = bot.cid
        assert cid is not None

        # The gateway dies and a new process takes its place. The bot is told (Gap), subscribes again and reconciles.
        await gateway.restart()
        await asyncio.wait_for(bot.mended.wait(), 10)
        assert bot.ledger == {cid} and bot.ended is None

        # The order is filled at the broker: the bot hears it through the new process.
        mock.fill(cid, qty=Decimal(1), price=Decimal("100.00"))
        await asyncio.wait_for(task, 10)
        assert bot.ended is brokapi.OrderStateKind.FILLED and bot.ledger == set()


async def test_the_gateway_bot_learns_how_an_order_closed_while_it_was_away_ended() -> None:
    async with await testing.mock_bcs() as mock:
        first = await mock.connect()
        bot = gateway_bot.Bot(first)
        await bot.setup()
        await bot.place()
        assert bot.cid is not None
        await first.close()
        mock.fill(bot.cid, qty=Decimal(1), price=Decimal("100.00"))

        # A new life with the same ledger: closed_offline, and order_state tells it was filled, not cancelled.
        second = await mock.connect()
        again = gateway_bot.Bot(second)
        again.ledger = {bot.cid}
        await again.setup()
        assert again.ended is brokapi.OrderStateKind.FILLED
        assert again.ledger == set()
        await second.close()


async def test_the_gateway_bot_runs_unchanged_at_the_broker() -> None:
    """Only the line that makes the client differs: the bot is the same."""
    async with await testing.mock_bcs() as mock:
        client = await mock.connect()
        bot = gateway_bot.Bot(client)
        task = asyncio.create_task(gateway_bot.run(bot))
        await asyncio.wait_for(bot.placed.wait(), 10)
        assert bot.cid is not None
        mock.fill(bot.cid, qty=Decimal(1), price=Decimal("100.00"))
        await asyncio.wait_for(task, 10)
        assert bot.ended is brokapi.OrderStateKind.FILLED


async def test_the_gateway_bot_connects_by_the_name_of_the_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    async with await testing.mock_bcs() as mock:
        gateway = await mock.start_gateway("bcs-main")
        # `brokapi.state/gateways/bcs-main` is where the service keeps the gateway; here it is a temporary directory.
        real = brokapi.connect_gateway

        async def connect_gateway(name: str, *, bot: str) -> brokapi.Client:
            assert name == "bcs-main"
            return await real(dir=gateway.directory, bot=bot)

        monkeypatch.setattr(gateway_bot, "brokapi", types.SimpleNamespace(connect_gateway=connect_gateway))
        client = await gateway_bot.connect()
        assert (client.gateway, client.name) == ("bcs-main", "my-bot")
