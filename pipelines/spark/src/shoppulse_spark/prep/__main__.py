"""CLI: `python -m shoppulse_spark.prep {sample,split} ...`"""

import argparse
import json
from datetime import datetime

from shoppulse_spark.prep import jobs
from shoppulse_spark.session import local_session

# Last two weeks of the Oct 2019 - Feb 2020 range are replayed as "live" (ADR-001).
DEFAULT_SPLIT_AT = "2020-02-16T00:00:00+00:00"


def _aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("include a UTC offset, e.g. 2020-02-16T00:00:00+00:00")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m shoppulse_spark.prep")
    sub = parser.add_subparsers(dest="command", required=True)

    sample = sub.add_parser("sample", help="validate raw CSVs and keep the user sample")
    sample.add_argument("--input", nargs="+", required=True, help="raw .csv or .csv.gz files")
    sample.add_argument("--output", required=True)
    sample.add_argument("--sample-pct", type=int, default=5)

    split = sub.add_parser("split", help="shift by whole weeks and split into history/live")
    split.add_argument("--input", required=True, help="output folder of the sample step")
    split.add_argument("--output", required=True)
    split.add_argument("--split-at", type=_aware_datetime, default=DEFAULT_SPLIT_AT)
    split.add_argument("--shift-weeks", type=int, required=True)

    args = parser.parse_args(argv)
    spark = local_session(f"prep-{args.command}")
    try:
        if args.command == "sample":
            manifest = jobs.run_sample(spark, args.input, args.output, args.sample_pct)
        else:
            manifest = jobs.run_split(
                spark, args.input, args.output, args.split_at, args.shift_weeks
            )
    finally:
        spark.stop()
    print(json.dumps(manifest, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
