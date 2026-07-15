"""Narrow Anthropic-compatible model gateway proxy.

The proxy runs in a container separate from the tool-using model runtime. It
holds the upstream credential, permits only the messages APIs, and never mounts
the repository checkout.
"""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import ssl
import threading
import time
import urllib.parse
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_MAX_REQUEST_BYTES = 8 * 1024 * 1024
_MAX_RESPONSE_BYTES = 32 * 1024 * 1024
_MAX_ADMISSION_RESPONSE_BYTES = 64 * 1024
_ALLOWED_PATHS = {"/v1/messages", "/v1/messages/count_tokens"}
_ADMISSION_PATH = "/v1/controls/admit"
_FORWARDED_REQUEST_HEADERS = {
    "accept",
    "anthropic-beta",
    "anthropic-version",
    "content-type",
}
_FORWARDED_RESPONSE_HEADERS = {
    "content-type",
    "request-id",
    "retry-after",
}
_REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_OPERATION_RE = re.compile(r"^[A-Za-z0-9_.:/-]{1,200}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RESERVATION_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")


@dataclass(frozen=True)
class ControlPolicy:
    repository: str
    operation: str
    policy_sha256: str
    daily_request_limit: int
    daily_input_token_limit: int
    daily_output_token_limit: int
    daily_cost_microusd: int
    max_queue_depth: int
    failure_threshold: int
    circuit_cooldown_seconds: int
    max_publications_per_day: int
    run_request_limit: int


@dataclass(frozen=True)
class LocalReservation:
    estimated_input_tokens: int
    reserved_output_tokens: int


class GatewayRejected(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class GatewayController:
    """Per-run limits and an automatic upstream-failure circuit breaker."""

    def __init__(self, policy: ControlPolicy) -> None:
        self.policy = policy
        self._lock = threading.Lock()
        self._requests = 0
        self._estimated_input_tokens = 0
        self._reserved_output_tokens = 0
        self._in_flight = 0
        self._consecutive_failures = 0
        self._circuit_open_until = 0.0

    def reserve(self, path: str, body: bytes) -> LocalReservation:
        estimated_input = max(1, (len(body) + 3) // 4)
        reserved_output = 0
        if path == "/v1/messages":
            try:
                payload = json.loads(body)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise GatewayRejected(400, "request body must be valid JSON") from exc
            if not isinstance(payload, dict):
                raise GatewayRejected(400, "request body must be a JSON object")
            max_tokens = payload.get("max_tokens")
            if (
                not isinstance(max_tokens, int)
                or isinstance(max_tokens, bool)
                or max_tokens <= 0
            ):
                raise GatewayRejected(
                    400,
                    "messages request requires a positive max_tokens",
                )
            reserved_output = max_tokens

        with self._lock:
            now = time.monotonic()
            if now < self._circuit_open_until:
                raise GatewayRejected(503, "AI gateway circuit breaker is open")
            if self._in_flight >= self.policy.max_queue_depth:
                raise GatewayRejected(429, "AI gateway queue depth is exhausted")
            if self._requests >= self.policy.run_request_limit:
                raise GatewayRejected(429, "AI request budget for this run is exhausted")
            if (
                self._estimated_input_tokens + estimated_input
                > self.policy.daily_input_token_limit
            ):
                raise GatewayRejected(429, "AI input-token budget is exhausted")
            if (
                self._reserved_output_tokens + reserved_output
                > self.policy.daily_output_token_limit
            ):
                raise GatewayRejected(429, "AI output-token budget is exhausted")
            self._requests += 1
            self._estimated_input_tokens += estimated_input
            self._reserved_output_tokens += reserved_output
            self._in_flight += 1
        return LocalReservation(estimated_input, reserved_output)

    def release(self, reservation: LocalReservation) -> None:
        """Release a local reservation when central admission rejects it."""
        with self._lock:
            self._requests -= 1
            self._estimated_input_tokens -= reservation.estimated_input_tokens
            self._reserved_output_tokens -= reservation.reserved_output_tokens
            self._in_flight -= 1

    def finish(self) -> None:
        """Release one in-flight slot after an admitted request completes."""
        with self._lock:
            if self._in_flight <= 0:
                raise RuntimeError("AI gateway in-flight accounting underflow")
            self._in_flight -= 1

    def record_upstream_status(self, status: int) -> None:
        with self._lock:
            if status < 500:
                self._consecutive_failures = 0
                return
            self._record_failure_locked()

    def record_upstream_failure(self) -> None:
        with self._lock:
            self._record_failure_locked()

    def _record_failure_locked(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.policy.failure_threshold:
            self._circuit_open_until = (
                time.monotonic() + self.policy.circuit_cooldown_seconds
            )


class GatewayConfig:
    def __init__(self) -> None:
        upstream = os.environ.get("AI_GATEWAY_UPSTREAM_URL", "").strip().rstrip("/")
        parsed = urllib.parse.urlsplit(upstream)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username:
            raise ValueError("AI_GATEWAY_UPSTREAM_URL must be an HTTPS origin")
        if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError("AI_GATEWAY_UPSTREAM_URL must not contain a path, query, or fragment")
        token = os.environ.get("AI_GATEWAY_TOKEN", "")
        if not token or "\r" in token or "\n" in token:
            raise ValueError("AI_GATEWAY_TOKEN must be a non-empty single-line value")
        self.host = parsed.hostname
        self.port = parsed.port or 443
        self.token = token
        enabled = os.environ.get("AI_CONTROL_ENABLED", "").strip().lower()
        if enabled != "true":
            raise ValueError("AI controls must be explicitly enabled")
        repository = os.environ.get("AI_CONTROL_REPOSITORY", "")
        operation = os.environ.get("AI_CONTROL_OPERATION", "")
        policy_sha256 = os.environ.get("AI_CONTROL_POLICY_SHA256", "")
        if not _REPOSITORY_RE.fullmatch(repository):
            raise ValueError("AI_CONTROL_REPOSITORY must be an owner/name slug")
        if not _OPERATION_RE.fullmatch(operation):
            raise ValueError("AI_CONTROL_OPERATION is invalid")
        if not _SHA256_RE.fullmatch(policy_sha256):
            raise ValueError("AI_CONTROL_POLICY_SHA256 is invalid")
        self.policy = ControlPolicy(
            repository=repository,
            operation=operation,
            policy_sha256=policy_sha256,
            daily_request_limit=_control_int(
                "AI_CONTROL_DAILY_REQUEST_LIMIT",
                1,
                10_000,
            ),
            daily_input_token_limit=_control_int(
                "AI_CONTROL_DAILY_INPUT_TOKEN_LIMIT",
                1,
                1_000_000_000,
            ),
            daily_output_token_limit=_control_int(
                "AI_CONTROL_DAILY_OUTPUT_TOKEN_LIMIT",
                1,
                100_000_000,
            ),
            daily_cost_microusd=_control_int(
                "AI_CONTROL_DAILY_COST_MICROUSD",
                1,
                10_000_000_000,
            ),
            max_queue_depth=_control_int(
                "AI_CONTROL_MAX_QUEUE_DEPTH",
                1,
                32,
            ),
            failure_threshold=_control_int(
                "AI_CONTROL_FAILURE_THRESHOLD",
                1,
                100,
            ),
            circuit_cooldown_seconds=_control_int(
                "AI_CONTROL_CIRCUIT_COOLDOWN_SECONDS",
                60,
                604_800,
            ),
            max_publications_per_day=_control_int(
                "AI_CONTROL_MAX_PUBLICATIONS_PER_DAY",
                1,
                10_000,
            ),
            run_request_limit=_control_int(
                "AI_CONTROL_RUN_REQUEST_LIMIT",
                1,
                100,
            ),
        )
        if self.policy.run_request_limit > self.policy.daily_request_limit:
            raise ValueError("run request limit exceeds daily request limit")
        self.controller = GatewayController(self.policy)


class GatewayHandler(BaseHTTPRequestHandler):
    server_version = "ValkeyAIGateway/1"
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        if self.path != "/health":
            self._error(404, "not found")
            return
        self._send_bytes(200, b"ok\n", "text/plain")

    def do_POST(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path not in _ALLOWED_PATHS or parsed.query or parsed.fragment:
            self._error(404, "endpoint not allowed")
            return
        if self.headers.get("Transfer-Encoding"):
            self._error(400, "transfer encoding is not supported")
            return
        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length or "")
        except ValueError:
            self._error(411, "valid content length required")
            return
        if length < 0 or length > _MAX_REQUEST_BYTES:
            self._error(413, "request too large")
            return
        body = self.rfile.read(length)
        if len(body) != length:
            self._error(400, "incomplete request")
            return

        config: GatewayConfig = self.server.config  # type: ignore[attr-defined]
        try:
            local_reservation = config.controller.reserve(parsed.path, body)
            reservation_id = _request_central_admission(
                config,
                path=parsed.path,
                body=body,
                reservation=local_reservation,
            )
        except GatewayRejected as exc:
            if "local_reservation" in locals():
                config.controller.release(local_reservation)
            self._error(exc.status, exc.message)
            return
        headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower() in _FORWARDED_REQUEST_HEADERS
        }
        headers["Authorization"] = f"Bearer {config.token}"
        headers["X-Valkey-Control-Reservation"] = reservation_id
        headers["X-Valkey-Control-Policy-SHA256"] = config.policy.policy_sha256
        headers["Content-Length"] = str(len(body))
        connection: http.client.HTTPSConnection | None = None
        try:
            connection = http.client.HTTPSConnection(
                config.host,
                config.port,
                timeout=90,
                context=ssl.create_default_context(),
            )
            connection.request("POST", parsed.path, body=body, headers=headers)
            response = connection.getresponse()
            config.controller.record_upstream_status(response.status)
            self.send_response(response.status)
            for name, value in response.getheaders():
                if name.lower() in _FORWARDED_RESPONSE_HEADERS:
                    self.send_header(name, value)
            self.send_header("Connection", "close")
            self.end_headers()
            transferred = 0
            while True:
                chunk = response.read(64 * 1024)
                if not chunk:
                    break
                transferred += len(chunk)
                if transferred > _MAX_RESPONSE_BYTES:
                    self.close_connection = True
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        except (OSError, http.client.HTTPException):
            config.controller.record_upstream_failure()
            if not self.wfile.closed:
                self.close_connection = True
        finally:
            if connection is not None:
                connection.close()
            config.controller.finish()

    def do_PUT(self) -> None:
        self._error(405, "method not allowed")

    def do_DELETE(self) -> None:
        self._error(405, "method not allowed")

    def log_message(self, format: str, *args: object) -> None:
        # Do not log request content or authorization-adjacent metadata.
        return

    def _error(self, status: int, message: str) -> None:
        self._send_bytes(status, (message + "\n").encode("ascii"), "text/plain")

    def _send_bytes(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)


def _request_central_admission(
    config: GatewayConfig,
    *,
    path: str,
    body: bytes,
    reservation: LocalReservation,
) -> str:
    policy = asdict(config.policy)
    payload = json.dumps(
        {
            "version": 1,
            "path": path,
            "request_sha256": hashlib.sha256(body).hexdigest(),
            "estimated_input_tokens": reservation.estimated_input_tokens,
            "reserved_output_tokens": reservation.reserved_output_tokens,
            "policy": policy,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    connection = http.client.HTTPSConnection(
        config.host,
        config.port,
        timeout=30,
        context=ssl.create_default_context(),
    )
    try:
        connection.request(
            "POST",
            _ADMISSION_PATH,
            body=payload,
            headers={
                "Authorization": f"Bearer {config.token}",
                "Content-Type": "application/json",
                "Content-Length": str(len(payload)),
            },
        )
        response = connection.getresponse()
        raw = response.read(_MAX_ADMISSION_RESPONSE_BYTES + 1)
        if len(raw) > _MAX_ADMISSION_RESPONSE_BYTES:
            raise GatewayRejected(503, "AI control response is oversized")
        if response.status != 200:
            config.controller.record_upstream_status(response.status)
            status = 429 if response.status in {402, 409, 429} else 503
            raise GatewayRejected(status, "central AI admission denied")
        try:
            result = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise GatewayRejected(503, "AI control response is invalid") from exc
        if (
            not isinstance(result, dict)
            or set(result) != {"decision", "reservation_id"}
            or result.get("decision") != "admit"
            or not isinstance(result.get("reservation_id"), str)
            or not _RESERVATION_RE.fullmatch(result["reservation_id"])
        ):
            raise GatewayRejected(503, "AI control response is invalid")
        return result["reservation_id"]
    except GatewayRejected:
        raise
    except (OSError, http.client.HTTPException) as exc:
        config.controller.record_upstream_failure()
        raise GatewayRejected(503, "central AI admission unavailable") from exc
    finally:
        connection.close()


def _control_int(name: str, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, "")
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if str(value) != raw or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def main() -> None:
    config = GatewayConfig()
    server = ThreadingHTTPServer(("0.0.0.0", 8080), GatewayHandler)
    server.config = config  # type: ignore[attr-defined]
    server.serve_forever()


if __name__ == "__main__":
    main()
