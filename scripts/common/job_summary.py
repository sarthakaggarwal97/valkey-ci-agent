"""Write to the files GitHub Actions reads back: job summaries and step outputs."""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def emit_job_summary(text: str) -> None:
    """Write *text* to the GitHub Actions job summary file.

    The path is read from the ``GITHUB_STEP_SUMMARY`` environment variable.
    If the variable is unset or the write fails, the error is logged but
    does not raise.
    """
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        logger.debug("GITHUB_STEP_SUMMARY not set; skipping job summary.")
        return
    try:
        with open(summary_path, "a", encoding="utf-8") as fh:
            fh.write(text + "\n")
        logger.debug("Wrote job summary to %s.", summary_path)
    except OSError as exc:
        logger.warning("Failed to write job summary: %s", exc)


def write_outputs(values: dict[str, str]) -> None:
    """Append *values* to the step's ``GITHUB_OUTPUT`` file, one ``key=value`` per line.

    A no-op outside Actions. A value with a line break is refused rather than
    written: GitHub would read the text after the break as a second,
    forged output.
    """
    path = os.environ.get("GITHUB_OUTPUT", "")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as output:
        for key, value in values.items():
            if "\n" in value or "\r" in value:
                raise ValueError(f"multiline workflow output refused for {key}")
            output.write(f"{key}={value}\n")
