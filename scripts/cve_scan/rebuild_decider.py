"""Select findings with published fixes for downstream candidate builds."""

from __future__ import annotations

from scripts.cve_scan.models import Classification, Finding


def classify(finding: Finding) -> Classification:
    """Select published-fix findings; the rebuilt artifact remains untrusted."""
    if not finding.fixed_version:
        return Classification(
            finding=finding,
            fixable=False,
            rationale="No published package fix yet.",
        )

    return Classification(
        finding=finding,
        fixable=True,
        rationale=(
            f"Fix {finding.fixed_version} published; downstream must verify "
            f"the exact rebuilt digest before promotion."
        ),
    )


def classify_all(findings: list[Finding]) -> list[Classification]:
    """Classify a list of findings. Returns one Classification per Finding."""
    return [classify(f) for f in findings]
