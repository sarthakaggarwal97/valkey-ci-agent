"""Shared logging setup for workflow entry points."""

from __future__ import annotations

import logging

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def configure_logging(*, verbose: bool = False) -> None:
    """Configure the one log format every workflow entry point uses.

    Timestamps and logger names let an operator line a record up with the
    Actions step timing and tell which module made a decision.
    """
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format=LOG_FORMAT)
