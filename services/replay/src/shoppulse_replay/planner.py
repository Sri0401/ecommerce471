"""Decide what to send and when: deterministic faults, emission order and the safe checkpoint.

Everything here is pure (no I/O, no clock), so every fault and the checkpoint logic are
unit-tested without Kafka.
"""

import hashlib
import heapq
from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from shoppulse_replay.scenario import Scenario
from shoppulse_replay.source import Position, Row, position

Fault = Literal["duplicate", "late", "malformed"]


@dataclass(frozen=True, order=True)
class Emission:
    """One message to send. Ordered by (emit_time, seq) so the heap is deterministic."""

    emit_time: datetime
    seq: int
    row: Row = field(compare=False)
    row_index: int = field(compare=False)
    faults: tuple[Fault, ...] = field(compare=False, default=())


@dataclass
class _OpenRow:
    position: Position
    remaining: int  # messages for this row not yet emitted


def _chance(seed: int, kind: str, event_id: str) -> float:
    """Deterministic number in [0, 1) per (seed, fault kind, event).

    Same seed -> same faults on the same events, on every run and after every restart,
    independent of how far the replay got before.
    """
    digest = hashlib.sha256(f"{seed}|{kind}|{event_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


class Planner:
    def __init__(self, scenario: Scenario) -> None:
        self._scenario = scenario
        self._pending: list[Emission] = []
        self._seq = 0
        # Rows in read order (keyed by read index, because exact source duplicates share a
        # position). The checkpoint only moves past a row once all its messages are emitted.
        self._open_rows: deque[int] = deque()
        self._rows: dict[int, _OpenRow] = {}
        self._next_row_index = 0
        self._done_upto: Position | None = None

    @property
    def checkpoint(self) -> Position | None:
        """Every row at or before this position has been fully emitted.

        A late event still waiting in the buffer holds the checkpoint back, so a crash never
        loses it; on resume, rows after the checkpoint are re-sent and deduped by event_id.
        """
        return self._done_upto

    def plan(self, rows: Iterable[Row]) -> Iterator[Emission]:
        for row in rows:
            self._schedule(row)
            # Release everything due by now; later rows cannot be due earlier than this row.
            while self._pending and self._pending[0].emit_time <= row["event_time"]:
                yield self._release()
        while self._pending:
            yield self._release()

    def _schedule(self, row: Row) -> None:
        s, event_id = self._scenario, row["event_id"]
        emit_time = row["event_time"]
        faults: list[Fault] = []
        if _chance(s.seed, "late", event_id) < s.late_rate:
            emit_time += s.late_delay
            faults.append("late")
        emit_time = self._after_outages(emit_time)

        messages: list[tuple[Fault, ...]] = [tuple(faults)]
        if _chance(s.seed, "duplicate", event_id) < s.duplicate_rate:
            messages.append((*faults, "duplicate"))
        if _chance(s.seed, "malformed", event_id) < s.malformed_rate:
            # An extra corrupt message: the real event still arrives, the DLQ gets the garbage.
            messages.append((*faults, "malformed"))

        index = self._next_row_index
        self._next_row_index += 1
        self._open_rows.append(index)
        self._rows[index] = _OpenRow(position(row), remaining=len(messages))
        for message_faults in messages:
            heapq.heappush(
                self._pending, Emission(emit_time, self._seq, row, index, message_faults)
            )
            self._seq += 1

    def _after_outages(self, emit_time: datetime) -> datetime:
        for outage in self._scenario.outages:
            if outage.start <= emit_time < outage.end:
                return outage.end
        return emit_time

    def _release(self) -> Emission:
        emission = heapq.heappop(self._pending)
        self._rows[emission.row_index].remaining -= 1
        while self._open_rows and self._rows[self._open_rows[0]].remaining == 0:
            self._done_upto = self._rows.pop(self._open_rows.popleft()).position
        return emission
