"""Read the prepared `live/` Parquet folder in deterministic event-time order.

Only one day partition is held in memory at a time, so the full dataset never has to be
sorted at once.
"""

from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

Position = tuple[datetime, str]  # (event_time, event_id): a total order over the stream
Row = dict[str, Any]


def position(row: Row) -> Position:
    return (row["event_time"], row["event_id"])


def _day_partitions(live_dir: Path) -> list[tuple[date, Path]]:
    days = [
        (date.fromisoformat(p.name.removeprefix("event_date=")), p)
        for p in live_dir.iterdir()
        if p.is_dir() and p.name.startswith("event_date=")
    ]
    return sorted(days)


def read_events(live_dir: Path, after: Position | None = None) -> Iterator[Row]:
    """Yield rows ordered by (event_time, event_id), skipping everything <= `after`."""
    for day, folder in _day_partitions(live_dir):
        if after is not None and day < after[0].date():
            continue  # whole day already replayed; don't even read it
        table = pq.read_table(folder).sort_by(
            [("event_time", "ascending"), ("event_id", "ascending")]
        )
        for row in table.to_pylist():
            if row["event_time"].tzinfo is None:
                raise ValueError(
                    f"{folder}: event_time has no time zone; was the Parquet written as INT96?"
                )
            if after is not None and position(row) <= after:
                continue
            yield row
