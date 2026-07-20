"""Canonical bot identity, shared by every workflow.

Two GitHub accounts drive the agent and both are defined here so no workflow
hardcodes its own copy:

- ``valkeyrie-bot``: the manual-dispatch bot. Its ``[bot]`` login and noreply
  email author every agent-made commit.
- ``valkeyrie-ops``: the GitHub App that opens PRs (sweeps, release cuts) and
  drives the comment poller.
"""

from __future__ import annotations

# Mention logins (what appears after ``@`` in comments).
BOT_LOGIN = "valkeyrie-bot"
APP_LOGIN = "valkeyrie-ops"

# Commit author identity for agent-authored commits.
BOT_NAME = f"{BOT_LOGIN}[bot]"
BOT_EMAIL = f"3692572+{BOT_LOGIN}[bot]@users.noreply.github.com"
