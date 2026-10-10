"""Fault-injection scenarios, loaded from TOML (stdlib `tomllib`, no extra dependency)."""

import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Outage:
    """The source stops sending in [start, end); everything due in the gap arrives at `end`."""

    start: datetime
    end: datetime


@dataclass(frozen=True)
class Scenario:
    name: str
    seed: int = 0
    speed: float | None = None  # event-time seconds per wall second; None = as fast as possible
    duplicate_rate: float = 0.0
    late_rate: float = 0.0
    late_delay: timedelta = timedelta(minutes=30)
    malformed_rate: float = 0.0
    outages: tuple[Outage, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        for name in ("duplicate_rate", "late_rate", "malformed_rate"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1, got {value}")
        if self.speed is not None and self.speed <= 0:
            raise ValueError(f"speed must be positive, got {self.speed}")
        if self.late_delay <= timedelta(0):
            raise ValueError("late_delay must be positive")
        for outage in self.outages:
            if outage.start.tzinfo is None or outage.end <= outage.start:
                raise ValueError(f"invalid outage {outage}: needs UTC offset and end > start")


def load_scenario(path: Path) -> Scenario:
    """Load a scenario file. Unknown keys are an error, so typos cannot silently do nothing."""
    with path.open("rb") as f:  # tomllib reads bytes: TOML is always UTF-8
        data: dict[str, Any] = tomllib.load(f)
    outages = tuple(
        Outage(start=o["start"], end=o["start"] + timedelta(minutes=o["duration_minutes"]))
        for o in data.pop("outage", [])
    )
    late_delay_minutes = data.pop("late_delay_minutes", None)
    known = {"name", "seed", "speed", "duplicate_rate", "late_rate", "malformed_rate"}
    unknown = set(data) - known
    if unknown:
        raise ValueError(f"unknown scenario keys: {sorted(unknown)}")
    if late_delay_minutes is not None:
        data["late_delay"] = timedelta(minutes=late_delay_minutes)
    return Scenario(outages=outages, **data)
