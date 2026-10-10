"""Persist the replay checkpoint so a restarted replay resumes instead of starting over."""

import json
import os
from datetime import datetime
from pathlib import Path

from shoppulse_replay.source import Position


class CheckpointStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self) -> Position | None:
        if not self._path.exists():
            return None
        data = json.loads(self._path.read_text(encoding="utf-8"))
        return (datetime.fromisoformat(data["event_time"]), data["event_id"])

    def save(self, checkpoint: Position) -> None:
        """Atomic: write a temp file, then rename over the old one.

        A crash mid-write leaves the previous checkpoint intact, never a half-written file.
        """
        event_time, event_id = checkpoint
        tmp = self._path.with_suffix(".tmp")
        payload = {"event_time": event_time.isoformat(), "event_id": event_id}
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, self._path)
