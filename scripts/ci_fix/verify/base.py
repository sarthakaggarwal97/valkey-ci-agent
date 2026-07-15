"""Code-owned CI job environment classification."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class VerifyEnv(str, Enum):
    """The environment a failed job runs in, as classification supports it."""

    LOCAL = "local"          # plain Linux job: use the pinned verifier image
    DOCKER = "docker"        # Linux job: isolate the declared container image
    MACOS = "macos"          # macOS job: use the Seatbelt/UID-isolated worker
    UNSUPPORTED = "unsupported"  # cannot be classified/verified safely


@dataclass(frozen=True)
class FailedJob:
    """A job that did not succeed in the linked CI run (owned by code)."""

    name: str
    conclusion: str


def backend_label(env: VerifyEnv, image: str = "") -> str:
    """A short human label for the verifier used, for the PR comment.

    Derived from the env (and image) so the comment string and the routing
    enum cannot drift apart.
    """
    if env is VerifyEnv.DOCKER:
        return f"docker:{image}" if image else "docker"
    return env.value
