"""Log parser router and protocol for CI failure log parsing."""

from __future__ import annotations

import logging
import re
from typing import Protocol

from scripts.models import ParsedFailure

logger = logging.getLogger(__name__)

RAW_EXCERPT_LINES = 500

_ERROR_MARKERS = re.compile(
    r"(?:error:|Error:|FAILED|fatal:|FATAL|assertion failed|Traceback)",
    re.IGNORECASE,
)


def _extract_marker_excerpt(lines: list[str], limit: int) -> str | None:
    """Scan *lines* for error markers and return a context window.

    Returns ``None`` when no markers are found so the caller can fall
    back to the plain tail excerpt.
    """
    marker_indices: list[int] = []
    for idx, line in enumerate(lines):
        if _ERROR_MARKERS.search(line):
            marker_indices.append(idx)

    if not marker_indices:
        return None

    # Find the first cluster of markers (consecutive markers within a
    # 20-line window).
    cluster_start = marker_indices[0]
    cluster_end = marker_indices[0]
    for mi in marker_indices[1:]:
        if mi - cluster_end <= 20:
            cluster_end = mi
        else:
            break

    # Extract a context window around the cluster
    context_padding = 30
    region_start = max(0, cluster_start - context_padding)
    region_end = min(len(lines), cluster_end + context_padding + 1)
    marker_region = lines[region_start:region_end]

    # Combine marker region with log tail, up to the configured limit
    tail_budget = limit - len(marker_region)
    if tail_budget > 0:
        tail_lines = lines[-tail_budget:]
        # Avoid duplicating lines that appear in both regions
        combined = list(marker_region)
        marker_region_set = set(range(region_start, region_end))
        tail_start = len(lines) - tail_budget
        for i, line in enumerate(tail_lines):
            abs_idx = tail_start + i
            if abs_idx not in marker_region_set:
                combined.append(line)
        return "\n".join(combined[:limit])
    else:
        return "\n".join(marker_region[:limit])


class LogParser(Protocol):
    """Protocol for individual log parsers."""

    def can_parse(self, log_content: str) -> bool: ...
    def parse(self, log_content: str) -> list[ParsedFailure]: ...


class LogParserRouter:
    """Tries registered parsers in order; falls back to raw excerpt."""

    def __init__(self, parsers: list[LogParser] | None = None) -> None:
        self._parsers: list[LogParser] = parsers or []

    def register(self, parser: LogParser) -> None:
        self._parsers.append(parser)

    def parse(
        self,
        log_content: str,
        *,
        raw_excerpt_lines: int | None = None,
    ) -> tuple[list[ParsedFailure], str | None, bool]:
        """Parse log content.

        Returns:
            (parsed_failures, raw_excerpt_or_none, is_unparseable)
        """
        for parser in self._parsers:
            try:
                if parser.can_parse(log_content):
                    failures = parser.parse(log_content)
                    if failures:
                        logger.info(
                            "Parsing complete: %s matched, %d failure(s) extracted.",
                            type(parser).__name__, len(failures),
                        )
                        return failures, None, False
            except Exception as exc:
                logger.warning("Parser %s raised: %s", type(parser).__name__, exc)
                continue

        # No parser matched — return raw excerpt
        limit = raw_excerpt_lines if raw_excerpt_lines is not None else RAW_EXCERPT_LINES
        lines = log_content.splitlines()

        # Try error-marker extraction first
        marker_excerpt = _extract_marker_excerpt(lines, limit)
        if marker_excerpt is not None:
            excerpt = marker_excerpt
        else:
            excerpt = "\n".join(lines[-limit:])

        logger.warning(
            "Parsing complete: no parser matched, flagging as unparseable "
            "(returning up to %d lines).",
            limit,
        )
        return [], excerpt, True
