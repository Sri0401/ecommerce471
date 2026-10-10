"""CLI: `python -m shoppulse_contracts register --registry http://localhost:8081`"""

import argparse
import sys

from shoppulse_contracts import events
from shoppulse_contracts.registry import RegistryClient, RegistryError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m shoppulse_contracts")
    sub = parser.add_subparsers(dest="command", required=True)
    register = sub.add_parser("register", help="set BACKWARD compatibility and register v1")
    register.add_argument("--registry", default="http://localhost:8081")
    args = parser.parse_args(argv)

    client = RegistryClient(args.registry)
    try:
        # Compatibility first, so even the very first version is registered under the rule.
        client.set_compatibility(events.SUBJECT, "BACKWARD")
        schema_id = client.register(events.SUBJECT, events.schema_text())
    except RegistryError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"{events.SUBJECT}: schema id {schema_id} (compatibility BACKWARD)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
