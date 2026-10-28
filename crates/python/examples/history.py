"""A long candle history into files, within the rate limit, that goes on where it stopped.

The code of the page "Как скачать историю свечей" of the Python section of the book. One file per instrument: CSV with a
header line, one line per bar. The file is its own checkpoint: where it ends, the download goes on. The tests run it
against the imitations.
"""

import asyncio
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import brokapi


# ANCHOR: files
HEADER = "ts,open,high,low,close,volume\n"  # pandas.read_csv(path, parse_dates=["ts"]) reads the file as it is


def last_bar(path: Path) -> datetime | None:
    """The open time of the last COMPLETE line of the file, or None. A line cut by a crash is ignored."""
    if not path.exists():
        return None
    last = None
    for line in path.read_text().splitlines():
        parts = line.split(",")
        if len(parts) == 6 and parts[0] != "ts":
            last = datetime.fromisoformat(parts[0])
    return last


def cut_broken_tail(path: Path) -> None:
    """A crash can leave a half line at the end. Cut it off, or the next page would be glued to it."""
    if path.exists():
        text = path.read_text()
        keep = text[: text.rfind("\n") + 1]
        if keep != text:
            path.write_text(keep)


def write_page(path: Path, bars: list[brokapi.Candle]) -> None:
    """A page goes to the file with one write and is flushed before the next page is asked for."""
    text = "".join(
        f"{b.open_ts.isoformat()},{b.open},{b.high},{b.low},{b.close},{b.volume}\n" for b in bars
    )
    if not path.exists() or path.stat().st_size == 0:
        text = HEADER + text
    with path.open("a") as file:
        file.write(text)
        file.flush()
# ANCHOR_END: files


# ANCHOR: read
def read_bars(path: Path) -> list[dict[str, object]]:
    """The file back, with exact numbers: Decimal prices, aware times. For pandas: `pandas.DataFrame(read_bars(path))`
    keeps the Decimals (dtype object), `pandas.read_csv(path, parse_dates=["ts"])` reads floats."""
    with path.open(newline="") as file:
        return [
            {"ts": datetime.fromisoformat(row["ts"]), **{k: Decimal(row[k]) for k in ("open", "high", "low", "close", "volume")}}
            for row in csv.DictReader(file)
            if row["volume"]  # a line cut by a crash has fewer fields
        ]
# ANCHOR_END: read


# ANCHOR: download
async def download(
    client: brokapi.Client,
    ticker: str,
    timeframe: brokapi.Timeframe,
    start: datetime,
    end: datetime,
    folder: Path,
    *,
    attempts: int = 5,
) -> int:
    """Downloads [start, end) into `<folder>/<ticker>.csv` and returns the bars written in this run."""
    instrument = await client.instrument(ticker)
    path = folder / f"{ticker}.csv"
    written = 0
    stuck = 0
    while True:
        cut_broken_tail(path)
        # Go on after the last bar in the file (nothing is read twice), or from `start` for a new file.
        done = last_bar(path)
        begin = start if done is None else done + timedelta(seconds=timeframe.seconds)
        if begin >= end:
            return written
        pages = client.candle_pages(instrument, timeframe, begin, end)
        try:
            async for page in pages:
                write_page(path, page)
                written += len(page)
                stuck = 0
            return written
        except brokapi.BrokapiError as error:
            if error.error_class is not brokapi.ErrorClass.RETRYABLE:
                raise  # the same read fails again: check the instrument, the timeframe and the period
            stuck += 1
            if stuck >= attempts:
                raise
            # RateLimited or Transport after the reader's own patience: wait and go on from the end of the file.
            wait = error.retry_after.total_seconds() if isinstance(error, brokapi.RateLimited) else 0.5
            await asyncio.sleep(wait)
# ANCHOR_END: download


async def main() -> None:
    client = await brokapi.connect("bcs")
    try:
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        folder = Path("bars")
        folder.mkdir(exist_ok=True)
        for ticker in ("SBER", "GAZP"):
            count = await download(client, ticker, brokapi.Timeframe.H1, end - timedelta(days=365), end, folder)
            print(f"{ticker}: {count} bars")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
