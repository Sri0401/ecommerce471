"""Replay service: turns the prepared "live" period into a reproducible event stream (ADR-001).

Pipeline: `source` (ordered rows) -> `planner` (faults, emission times) -> `runner` (pacing,
checkpoints) -> a sink (Kafka in production, a list in tests).
"""
