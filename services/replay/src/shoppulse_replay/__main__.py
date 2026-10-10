"""CLI: replay the prepared "live" period into Kafka.

    python -m shoppulse_replay --scenario services/replay/scenarios/clean.toml \
        --live-dir data/prep/rees46/split/live

Resumes from the scenario's checkpoint if one exists; `--reset` starts from the beginning.
"""

import argparse
import json
import logging
from pathlib import Path

from confluent_kafka import Producer

from shoppulse_contracts import events
from shoppulse_contracts.registry import RegistryClient, RegistryError
from shoppulse_replay import runner
from shoppulse_replay.checkpoint import CheckpointStore
from shoppulse_replay.kafka_sink import DeliveryError, KafkaSink, producer_config
from shoppulse_replay.planner import Planner
from shoppulse_replay.scenario import load_scenario
from shoppulse_replay.source import read_events

log = logging.getLogger("shoppulse_replay")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m shoppulse_replay")
    parser.add_argument("--scenario", type=Path, required=True)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--registry", default="http://localhost:8081")
    parser.add_argument("--checkpoint-dir", type=Path, default=Path("data/replay"))
    parser.add_argument("--speed", type=float, help="override the scenario's speed")
    parser.add_argument("--max-speed", action="store_true", help="no pacing at all")
    parser.add_argument("--reset", action="store_true", help="ignore the existing checkpoint")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    scenario = load_scenario(args.scenario)
    speed = None if args.max_speed else (args.speed or scenario.speed)
    args.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    store = CheckpointStore(args.checkpoint_dir / f"{scenario.name}.checkpoint.json")
    resume_from = None if args.reset else store.load()

    try:
        # Fail fast if the contract is not registered: producers never register schemas.
        schema_id = RegistryClient(args.registry).latest_id(events.SUBJECT)
        sink = KafkaSink(Producer(producer_config(args.bootstrap)), schema_id)
        log.info(
            "scenario=%s speed=%s schema_id=%s resume_from=%s",
            scenario.name,
            speed,
            schema_id,
            resume_from,
        )
        sent = runner.run(
            read_events(args.live_dir, after=resume_from),
            Planner(scenario),
            sink,
            store,
            speed=speed,
        )
    except (RegistryError, DeliveryError) as e:
        # Non-zero exit: the orchestrator restarts us and we resume from the checkpoint.
        log.error("replay stopped: %s (checkpoint: %s)", e, store.load())
        return 1
    print(
        json.dumps(
            {
                "scenario": scenario.name,
                "messages_sent": sent,
                "resumed_from": str(resume_from),
                "checkpoint": str(store.load()),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
