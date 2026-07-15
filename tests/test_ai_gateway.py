from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from scripts.ai import gateway_proxy
from scripts.common.operational_controls import OperationalPolicy, policy_environment


class _FakeResponse:
    status = 200

    def __init__(self) -> None:
        self._chunks = [b'{"ok":true}', b""]

    def getheaders(self):
        return [("Content-Type", "application/json"), ("Set-Cookie", "secret")]

    def read(self, _size):
        return self._chunks.pop(0)


class _FakeConnection:
    requests = []

    def __init__(self, host, port, **_kwargs):
        assert host == "gateway.example"
        assert port == 443

    def request(self, method, path, *, body, headers):
        type(self).requests.append((method, path, body, headers))

    def getresponse(self):
        if self.requests[-1][1] == "/v1/controls/admit":
            return _AdmissionResponse()
        return _FakeResponse()

    def close(self):
        return None


class _AdmissionResponse:
    status = 200

    def read(self, _size):
        return b'{"decision":"admit","reservation_id":"reserve-1"}'


@contextmanager
def _server():
    policy = gateway_proxy.ControlPolicy(
        repository="valkey-io/valkey",
        operation="test:gateway",
        policy_sha256="a" * 64,
        daily_request_limit=24,
        daily_input_token_limit=2_000_000,
        daily_output_token_limit=500_000,
        daily_cost_microusd=20_000_000,
        max_queue_depth=4,
        failure_threshold=3,
        circuit_cooldown_seconds=60,
        max_publications_per_day=20,
        run_request_limit=8,
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), gateway_proxy.GatewayHandler)
    server.config = SimpleNamespace(
        host="gateway.example",
        port=443,
        token="gateway-secret",
        policy=policy,
        controller=gateway_proxy.GatewayController(policy),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_gateway_forwards_only_messages_with_gateway_credential(monkeypatch):
    _FakeConnection.requests = []
    monkeypatch.setattr(gateway_proxy.http.client, "HTTPSConnection", _FakeConnection)
    with _server() as base_url:
        request = urllib.request.Request(
            f"{base_url}/v1/messages",
            data=json.dumps({"model": "opus", "max_tokens": 1024}).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": "attacker-token",
                "X-Untrusted": "drop-me",
            },
            method="POST",
        )
        with urllib.request.urlopen(request) as response:
            assert response.read() == b'{"ok":true}'
            assert response.headers.get("Set-Cookie") is None

    assert len(_FakeConnection.requests) == 2
    admission = _FakeConnection.requests[0]
    assert admission[1] == "/v1/controls/admit"
    admission_body = json.loads(admission[2])
    assert admission_body["policy"]["repository"] == "valkey-io/valkey"
    assert admission_body["policy"]["daily_cost_microusd"] == 20_000_000

    method, path, body, headers = _FakeConnection.requests[1]
    assert (method, path) == ("POST", "/v1/messages")
    assert json.loads(body) == {"model": "opus", "max_tokens": 1024}
    assert headers["Authorization"] == "Bearer gateway-secret"
    assert headers["X-Valkey-Control-Reservation"] == "reserve-1"
    assert "X-Untrusted" not in headers


def test_gateway_denies_arbitrary_endpoint(monkeypatch):
    monkeypatch.setattr(
        gateway_proxy.http.client,
        "HTTPSConnection",
        lambda *_a, **_k: pytest.fail("upstream must not be contacted"),
    )
    with _server() as base_url:
        request = urllib.request.Request(
            f"{base_url}/v1/files",
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(request)
    assert exc.value.code == 404


def test_gateway_denies_before_upstream_when_run_budget_is_exhausted(monkeypatch):
    _FakeConnection.requests = []
    monkeypatch.setattr(gateway_proxy.http.client, "HTTPSConnection", _FakeConnection)
    with _server() as base_url:
        for expected in (200, 200, 200, 200, 200, 200, 200, 200, 429):
            request = urllib.request.Request(
                f"{base_url}/v1/messages",
                data=b'{"model":"opus","max_tokens":1}',
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            if expected == 200:
                with urllib.request.urlopen(request) as response:
                    assert response.status == 200
            else:
                with pytest.raises(urllib.error.HTTPError) as exc:
                    urllib.request.urlopen(request)
                assert exc.value.code == expected
    assert len(_FakeConnection.requests) == 16


def test_gateway_rejects_missing_max_tokens_without_admission(monkeypatch):
    monkeypatch.setattr(
        gateway_proxy.http.client,
        "HTTPSConnection",
        lambda *_a, **_k: pytest.fail("upstream must not be contacted"),
    )
    with _server() as base_url:
        request = urllib.request.Request(
            f"{base_url}/v1/messages",
            data=b'{"model":"opus"}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(request)
    assert exc.value.code == 400


def test_controller_enforces_in_flight_queue_depth() -> None:
    policy = gateway_proxy.ControlPolicy(
        repository="valkey-io/valkey",
        operation="test:queue",
        policy_sha256="a" * 64,
        daily_request_limit=10,
        daily_input_token_limit=10_000,
        daily_output_token_limit=10_000,
        daily_cost_microusd=1_000_000,
        max_queue_depth=1,
        failure_threshold=3,
        circuit_cooldown_seconds=60,
        max_publications_per_day=10,
        run_request_limit=10,
    )
    controller = gateway_proxy.GatewayController(policy)
    first = controller.reserve("/v1/messages", b'{"max_tokens":1}')
    with pytest.raises(gateway_proxy.GatewayRejected, match="queue depth"):
        controller.reserve("/v1/messages", b'{"max_tokens":1}')
    controller.finish()
    second = controller.reserve("/v1/messages", b'{"max_tokens":1}')
    controller.finish()
    assert first.reserved_output_tokens == second.reserved_output_tokens == 1


def test_controller_opens_and_recovers_failure_circuit(monkeypatch) -> None:
    now = [100.0]
    monkeypatch.setattr(gateway_proxy.time, "monotonic", lambda: now[0])
    policy = gateway_proxy.ControlPolicy(
        repository="valkey-io/valkey",
        operation="test:circuit",
        policy_sha256="a" * 64,
        daily_request_limit=10,
        daily_input_token_limit=10_000,
        daily_output_token_limit=10_000,
        daily_cost_microusd=1_000_000,
        max_queue_depth=2,
        failure_threshold=2,
        circuit_cooldown_seconds=60,
        max_publications_per_day=10,
        run_request_limit=10,
    )
    controller = gateway_proxy.GatewayController(policy)
    controller.record_upstream_failure()
    controller.record_upstream_status(503)
    with pytest.raises(gateway_proxy.GatewayRejected, match="circuit breaker"):
        controller.reserve("/v1/messages", b'{"max_tokens":1}')

    now[0] = 161.0
    reservation = controller.reserve("/v1/messages", b'{"max_tokens":1}')
    controller.finish()
    assert reservation.reserved_output_tokens == 1


@pytest.mark.parametrize(
    "url",
    [
        "http://gateway.example",
        "https://user@gateway.example",
        "https://gateway.example/path",
        "https://gateway.example?redirect=evil",
    ],
)
def test_gateway_config_requires_https_origin(monkeypatch, url):
    monkeypatch.setenv("AI_GATEWAY_UPSTREAM_URL", url)
    monkeypatch.setenv("AI_GATEWAY_TOKEN", "token")
    with pytest.raises(ValueError):
        gateway_proxy.GatewayConfig()


def test_gateway_config_requires_complete_control_policy(monkeypatch):
    monkeypatch.setenv("AI_GATEWAY_UPSTREAM_URL", "https://gateway.example")
    monkeypatch.setenv("AI_GATEWAY_TOKEN", "token")
    for name, value in policy_environment(
        "valkey-io/valkey",
        "test:gateway",
        OperationalPolicy(),
    ).items():
        monkeypatch.setenv(name, value)
    config = gateway_proxy.GatewayConfig()
    assert config.policy.repository == "valkey-io/valkey"
    assert config.policy.daily_cost_microusd == 20_000_000

    monkeypatch.delenv("AI_CONTROL_MAX_QUEUE_DEPTH")
    with pytest.raises(ValueError, match="MAX_QUEUE_DEPTH"):
        gateway_proxy.GatewayConfig()
