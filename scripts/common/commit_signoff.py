"""Helpers for DCO-friendly commit identities and signoff handling."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class CommitSigner:
    """Configured commit signer used for automated PRs and backports."""

    name: str = ""
    email: str = ""

    @property
    def configured(self) -> bool:
        return bool(self.name.strip() and self.email.strip())

    @property
    def signoff_line(self) -> str:
        return f"Signed-off-by: {self.name.strip()} <{self.email.strip()}>"


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name, "")
    if not value:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_signer_from_env() -> CommitSigner:
    return CommitSigner(
        name=os.environ.get("CI_BOT_COMMIT_NAME", "").strip(),
        email=os.environ.get("CI_BOT_COMMIT_EMAIL", "").strip(),
    )


def require_dco_signoff_from_env() -> bool:
    return _env_flag("CI_BOT_REQUIRE_DCO_SIGNOFF", default=False)

