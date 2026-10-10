import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

from shoppulse_contracts import wire
from shoppulse_contracts.registry import RegistryClient, RegistryError

# --- wire format --------------------------------------------------------------------------


def test_frame_round_trip() -> None:
    framed = wire.frame(42, b"avro-body")
    assert framed[:5] == b"\x00\x00\x00\x00\x2a"
    assert wire.unframe(framed) == (42, b"avro-body")


@pytest.mark.parametrize("data", [b"", b"\x00\x00", b"malformed:abc", b"\x01\x00\x00\x00\x01x"])
def test_unframe_rejects_non_confluent_bytes(data: bytes) -> None:
    with pytest.raises(wire.WireFormatError):
        wire.unframe(data)


# --- registry client (against a stub HTTP server) -----------------------------------------


class _StubRegistry(BaseHTTPRequestHandler):
    calls: list[tuple[str, str, Any]] = []

    def _reply(self, status: int, body: dict[str, Any]) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802 - http.server naming
        self.calls.append(("GET", self.path, None))
        if self.path.endswith("/versions/latest"):
            self._reply(200, {"id": 7, "version": 1})
        else:
            self._reply(404, {"error_code": 40401, "message": "Subject not found"})

    def do_POST(self) -> None:  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.calls.append(("POST", self.path, body))
        self._reply(200, {"id": 7})

    def do_PUT(self) -> None:  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.calls.append(("PUT", self.path, body))
        self._reply(200, body)

    def log_message(self, *args: Any) -> None:  # keep test output quiet
        pass


@pytest.fixture
def stub_registry() -> Iterator[str]:
    _StubRegistry.calls = []
    server = HTTPServer(("127.0.0.1", 0), _StubRegistry)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def test_register_sets_compatibility_and_returns_id(stub_registry: str) -> None:
    client = RegistryClient(stub_registry)
    client.set_compatibility("shoppulse.events.v1-value", "BACKWARD")
    assert client.register("shoppulse.events.v1-value", '{"type": "string"}') == 7
    assert _StubRegistry.calls[0] == (
        "PUT",
        "/config/shoppulse.events.v1-value",
        {"compatibility": "BACKWARD"},
    )
    assert _StubRegistry.calls[1][2] == {"schema": '{"type": "string"}'}


def test_latest_id(stub_registry: str) -> None:
    assert RegistryClient(stub_registry).latest_id("shoppulse.events.v1-value") == 7


def test_http_error_becomes_registry_error(stub_registry: str) -> None:
    with pytest.raises(RegistryError, match="404"):
        RegistryClient(stub_registry)._request("GET", "/subjects/unknown/versions")


def test_unreachable_registry_fails_fast() -> None:
    # Failure mode #21: nothing listens on port 9 (discard) -> clear error, not a hang.
    with pytest.raises(RegistryError, match="unreachable"):
        RegistryClient("http://127.0.0.1:9", timeout=2).latest_id("x")


def test_non_http_registry_url_is_rejected() -> None:
    with pytest.raises(ValueError):
        RegistryClient("file:///etc/passwd")
