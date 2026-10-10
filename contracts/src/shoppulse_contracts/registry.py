"""Minimal client for the Confluent Schema Registry REST API (served by Karapace, ADR-003).

Standard library only. Schemas are registered explicitly (contract-first); producers only
look up the ID of the latest registered version and never register on their own.
"""

import json
import urllib.error
import urllib.request
from typing import Any, cast
from urllib.parse import quote, urlparse

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class RegistryError(RuntimeError):
    """The registry is unreachable or rejected the request (e.g. incompatible schema)."""


class RegistryClient:
    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        if urlparse(base_url).scheme not in ("http", "https"):
            raise ValueError(f"registry URL must be http(s), got {base_url!r}")
        self._base = base_url.rstrip("/")
        self._timeout = timeout

    def set_compatibility(self, subject: str, level: str) -> None:
        self._request("PUT", f"/config/{quote(subject)}", {"compatibility": level})

    def register(self, subject: str, schema_text: str) -> int:
        """Register a schema (idempotent: an identical schema returns its existing ID)."""
        body = self._request(
            "POST", f"/subjects/{quote(subject)}/versions", {"schema": schema_text}
        )
        return int(body["id"])

    def latest_id(self, subject: str) -> int:
        return int(self._request("GET", f"/subjects/{quote(subject)}/versions/latest")["id"])

    def _request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(  # noqa: S310 - scheme checked in __init__
            self._base + path, data=data, method=method, headers={"Content-Type": CONTENT_TYPE}
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:  # noqa: S310
                return cast(dict[str, Any], json.loads(response.read()))
        except urllib.error.HTTPError as e:
            raise RegistryError(f"{method} {path} -> {e.code}: {e.read().decode()}") from e
        except (urllib.error.URLError, TimeoutError) as e:
            raise RegistryError(f"{method} {path}: registry unreachable ({e})") from e
