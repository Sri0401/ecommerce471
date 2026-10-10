# ShopPulse

A multi-store e-commerce data and ML platform: streaming and batch ingestion, a Delta Lake
lakehouse, a dbt star schema, real-time purchase-intent predictions, demand forecasting,
and human-in-the-loop approvals, built to keep working when components fail.

> **Status:** Week 1, foundations. Nothing runs yet.

## Important: the "live" data is replayed history

No free, live public e-commerce event stream exists. ShopPulse replays **real historical
behaviour data** (REES46, 2019-2020) in event-time order, with timestamps shifted forward by a
multiple of 7 days. The engineering is real; the "live" stream is a replay. See
[ADR-001](docs/adr/ADR-001-scope-datasets-architecture.md).

## Documentation

- [CLAUDE.md](CLAUDE.md): scope, stack, working rules
- [Architecture decisions](docs/adr/)
- [Failure modes](docs/failure-modes.md)

## Data attribution

- REES46 eCommerce Behavior Data: courtesy of [REES46 Marketing Platform](https://rees46.com/).
