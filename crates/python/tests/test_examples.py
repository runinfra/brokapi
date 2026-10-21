"""Every example of the book is a test: the programs of `examples/` run against the imitations of the brokers."""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid
from pathlib import Path

import pytest

import brokapi
from brokapi import testing

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
sys.path.insert(0, str(EXAMPLES))

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
    ):
        settings = {
            "bcs": bcs.settings,
            "tbank-a": {**tbank.settings, "account_id": "acc-a"},
            "tbank-b": {**tbank.settings, "account_id": "acc-b"},
            "finam": finam.settings,
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
            assert again.journal.orders[placed["tbank-b"]][1] == "working"
            assert again.clients["tbank-a"].limiter.broker == "tbank"
            filled = await again.clients["tbank-a"].order_state(placed["tbank-a"])
            assert filled.state.kind is brokapi.OrderStateKind.FILLED
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



