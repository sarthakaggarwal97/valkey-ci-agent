"""Backport controller credential loading."""

from __future__ import annotations

import argparse
import os

BACKPORT_GITHUB_TOKEN_ENV = "BACKPORT_GITHUB_TOKEN"


def consume_github_token(parser: argparse.ArgumentParser) -> str:
    """Return the dedicated backport token and remove it from ambient env."""
    token = os.environ.pop(BACKPORT_GITHUB_TOKEN_ENV, "").strip()
    if not token:
        parser.error(f"GitHub token is required via {BACKPORT_GITHUB_TOKEN_ENV}.")
    return token
