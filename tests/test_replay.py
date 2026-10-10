from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from shoppulse_contracts import events
from shoppulse_replay import runner
from shoppulse_replay.checkpoint import CheckpointStore
from shoppulse_replay.planner import Emission, Planner
from shoppulse_replay.scenario import Outage, Scenario, load_scenario
from shoppulse_replay.source import Row, read_events

T0 = datetime(2026, 10, 11, tzinfo=UTC)
SCENARIOS = Path(__file__).parents[1] / "services" / "replay" / "scenarios"


def _rows(n: int, step: timedelta = timedelta(seconds=10)) -> list[Row]:
    return [
        {
            "event_id": f"{i:064x}",
            "store_id": "rees46",
            "event_time": T0 + i * step,
            "event_type": "view",
            "product_id": 1000 + i % 7,
            "category_id": 2053013555631882655,
            "category_code": None,
            "brand": None,
            "price": Decimal("9.99"),
            "user_id": 500 + i % 13,
            "user_session": None,
        }
        for i in range(n)
    ]


def _plan(scenario: Scenario, rows: list[Row]) -> list[Emission]:
    return list(Planner(scenario).plan(rows))


class ListSink:
    def __init__(self) -> None:
        self.sent: list[Emission] = []
        self.flushes = 0

    def send(self, emission: Emission) -> None:
        self.sent.append(emission)

    def flush(self) -> None:
        self.flushes += 1


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        self.slept += seconds


# --- scenarios ----------------------------------------------------------------------------


def test_shipped_scenarios_load() -> None:
    chaos = load_scenario(SCENARIOS / "chaos.toml")
    assert chaos.late_delay == timedelta(minutes=90)
    assert chaos.outages[0].end - chaos.outages[0].start == timedelta(minutes=20)
    assert load_scenario(SCENARIOS / "clean.toml").duplicate_rate == 0


def test_unknown_scenario_key_is_an_error(tmp_path: Path) -> None:
    path = tmp_path / "typo.toml"
    path.write_text('name = "x"\nduplicate_rat = 0.5\n')
    with pytest.raises(ValueError, match="duplicate_rat"):
        load_scenario(path)


@pytest.mark.parametrize("kwargs", [{"duplicate_rate": 1.5}, {"speed": 0}, {"late_rate": -0.1}])
def test_invalid_scenario_values_are_rejected(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        Scenario(name="bad", **kwargs)


# --- source -------------------------------------------------------------------------------


def _write_live(tmp_path: Path, rows: list[Row]) -> Path:
    live = tmp_path / "live"
    by_day: dict[str, list[Row]] = {}
    for row in rows:
        by_day.setdefault(row["event_time"].date().isoformat(), []).append(row)
    for day, day_rows in by_day.items():
        folder = live / f"event_date={day}"
        folder.mkdir(parents=True)
        # Write in reverse so the reader has to sort.
        table = pa.Table.from_pylist(list(reversed(day_rows)))
        table = table.cast(table.schema.set(2, pa.field("event_time", pa.timestamp("us", "UTC"))))
        pq.write_table(table, folder / "part-0.parquet")
    (live / "_SUCCESS").write_text("")  # Spark marker files must be ignored
    return live


def test_source_reads_days_in_event_time_order(tmp_path: Path) -> None:
    rows = _rows(50, step=timedelta(hours=1))  # spans 3 days
    got = list(read_events(_write_live(tmp_path, rows)))
    assert [r["event_id"] for r in got] == [r["event_id"] for r in rows]
    assert got[0]["event_time"].tzinfo is not None


def test_source_resumes_after_checkpoint(tmp_path: Path) -> None:
    rows = _rows(50, step=timedelta(hours=1))
    after = (rows[29]["event_time"], rows[29]["event_id"])
    got = list(read_events(_write_live(tmp_path, rows), after=after))
    assert [r["event_id"] for r in got] == [r["event_id"] for r in rows[30:]]


def test_source_rejects_naive_timestamps(tmp_path: Path) -> None:
    folder = tmp_path / "live" / "event_date=2026-10-11"
    folder.mkdir(parents=True)
    row = _rows(1)[0] | {"event_time": datetime(2026, 10, 11)}
    pq.write_table(pa.Table.from_pylist([row]), folder / "part-0.parquet")
    with pytest.raises(ValueError, match="INT96"):
        list(read_events(tmp_path / "live"))


def test_source_rows_satisfy_the_contract(tmp_path: Path) -> None:
    for row in read_events(_write_live(tmp_path, _rows(5))):
        events.to_record(row)  # raises if the replay would produce an invalid event


# --- planner ------------------------------------------------------------------------------


def test_clean_scenario_sends_every_event_once_in_order() -> None:
    rows = _rows(500)
    out = _plan(Scenario(name="clean"), rows)
    assert [e.row["event_id"] for e in out] == [r["event_id"] for r in rows]
    assert all(e.faults == () for e in out)


def test_faults_are_deterministic_per_seed() -> None:
    rows = _rows(2000)
    scenario = Scenario(name="chaos", seed=7, duplicate_rate=0.05, late_rate=0.05)

    def faulted(s: Scenario) -> list[tuple[str, tuple[str, ...]]]:
        return [(e.row["event_id"], e.faults) for e in _plan(s, rows) if e.faults]

    assert faulted(scenario) == faulted(scenario)
    assert faulted(scenario) != faulted(Scenario(name="other", seed=8, duplicate_rate=0.05))


def test_duplicate_rate_is_roughly_honoured() -> None:
    out = _plan(Scenario(name="dups", duplicate_rate=0.05), _rows(4000))
    duplicates = sum("duplicate" in e.faults for e in out)
    assert 120 < duplicates < 280  # 5% of 4000 = 200
    assert len(out) == 4000 + duplicates


def test_late_events_arrive_after_their_delay_and_order_holds() -> None:
    delay = timedelta(minutes=30)
    out = _plan(Scenario(name="late", late_rate=0.1, late_delay=delay), _rows(1000))
    late = [e for e in out if "late" in e.faults]
    assert late
    assert all(e.emit_time == e.row["event_time"] + delay for e in late)
    emit_times = [e.emit_time for e in out]
    assert emit_times == sorted(emit_times)  # the stream itself stays in emit order


def test_malformed_is_an_extra_message_not_a_lost_event() -> None:
    rows = _rows(2000)
    out = _plan(Scenario(name="bad", malformed_rate=0.05), rows)
    assert any("malformed" in e.faults for e in out)
    good = {e.row["event_id"] for e in out if "malformed" not in e.faults}
    assert good == {r["event_id"] for r in rows}


def test_outage_holds_messages_until_it_ends() -> None:
    outage = Outage(start=T0 + timedelta(minutes=10), end=T0 + timedelta(minutes=30))
    out = _plan(Scenario(name="outage", outages=(outage,)), _rows(600))  # 100 minutes
    assert not [e for e in out if outage.start <= e.emit_time < outage.end]
    assert sum(e.emit_time == outage.end for e in out) > 100  # the backlog burst


# --- checkpoint and resume ----------------------------------------------------------------


def test_checkpoint_never_passes_a_late_event_still_buffered() -> None:
    rows = _rows(1000)
    scenario = Scenario(name="late", late_rate=0.05, late_delay=timedelta(minutes=30))
    planner = Planner(scenario)
    released: set[str] = set()
    for emission in planner.plan(rows):
        released.add(emission.row["event_id"])
        checkpoint = planner.checkpoint
        if checkpoint is not None:
            done = [r for r in rows if (r["event_time"], r["event_id"]) <= checkpoint]
            assert all(r["event_id"] in released for r in done)


def test_crash_and_resume_loses_nothing(tmp_path: Path) -> None:
    rows = _rows(1000)
    scenario = Scenario(name="chaos", seed=3, duplicate_rate=0.02, late_rate=0.05)
    store = CheckpointStore(tmp_path / "checkpoint.json")

    # First run "crashes" after 400 messages; the checkpoint was saved after the last flush.
    first = Planner(scenario)
    sent: set[str] = set()
    for i, emission in enumerate(first.plan(rows)):
        sent.add(emission.row["event_id"])
        if i == 399:
            assert first.checkpoint is not None
            store.save(first.checkpoint)
            break

    resume_from = store.load()
    assert resume_from is not None
    remaining = [r for r in rows if (r["event_time"], r["event_id"]) > resume_from]
    for emission in Planner(scenario).plan(remaining):
        sent.add(emission.row["event_id"])

    assert sent == {r["event_id"] for r in rows}  # some re-sent; silver dedups them


def test_checkpoint_store_round_trip(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "checkpoint.json")
    assert store.load() is None
    store.save((T0, "abc"))
    assert store.load() == (T0, "abc")
    assert not (tmp_path / "checkpoint.tmp").exists()


# --- runner -------------------------------------------------------------------------------


def test_runner_paces_by_speed_and_commits_after_flush(tmp_path: Path) -> None:
    rows = _rows(361)  # 360 gaps of 10 s = 1 hour of event time
    sink, clock = ListSink(), FakeClock()
    store = CheckpointStore(tmp_path / "checkpoint.json")
    sent = runner.run(
        rows,
        Planner(Scenario(name="clean")),
        sink,
        store,
        speed=60.0,
        checkpoint_every=100,
        clock=clock,
    )
    assert sent == 361
    assert clock.slept == pytest.approx(60.0)  # 1 hour at 60x = 1 minute
    assert sink.flushes == 4  # after 100, 200, 300 and at the end
    assert store.load() == (rows[-1]["event_time"], rows[-1]["event_id"])


def test_runner_without_speed_never_sleeps(tmp_path: Path) -> None:
    clock = FakeClock()
    runner.run(
        _rows(50),
        Planner(Scenario(name="fast")),
        ListSink(),
        CheckpointStore(tmp_path / "c.json"),
        speed=None,
        clock=clock,
    )
    assert clock.slept == 0
