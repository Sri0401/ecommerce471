"""Drive a replay: pace emissions in wall-clock time, send them, and checkpoint safely."""

import time
from collections.abc import Iterable
from datetime import datetime
from typing import Protocol

from shoppulse_replay.checkpoint import CheckpointStore
from shoppulse_replay.planner import Emission, Planner
from shoppulse_replay.source import Row


class Sink(Protocol):
    def send(self, emission: Emission) -> None: ...

    def flush(self) -> None:
        """Block until everything sent so far is acknowledged."""


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


def run(
    rows: Iterable[Row],
    planner: Planner,
    sink: Sink,
    checkpoints: CheckpointStore,
    speed: float | None,
    checkpoint_every: int = 10_000,
    clock: Clock | None = None,
) -> int:
    """Replay `rows`; returns the number of messages sent.

    The checkpoint is saved only after `sink.flush()`, so it never claims more than Kafka has
    acknowledged. With `speed=60`, one hour of event time takes one minute.
    """
    clock = clock or SystemClock()
    origin: tuple[datetime, float] | None = None
    sent = 0
    for emission in planner.plan(rows):
        if speed is not None:
            if origin is None:
                origin = (emission.emit_time, clock.monotonic())
            due = origin[1] + (emission.emit_time - origin[0]).total_seconds() / speed
            delay = due - clock.monotonic()
            if delay > 0:
                clock.sleep(delay)
        sink.send(emission)
        sent += 1
        if sent % checkpoint_every == 0:
            _commit(sink, planner, checkpoints)
    _commit(sink, planner, checkpoints)
    return sent


def _commit(sink: Sink, planner: Planner, checkpoints: CheckpointStore) -> None:
    sink.flush()
    if planner.checkpoint is not None:
        checkpoints.save(planner.checkpoint)
