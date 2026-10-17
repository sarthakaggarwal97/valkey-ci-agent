"""Canonical bot identity and the Valkey project facts every workflow shares.

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

# The Valkey server repository.
VALKEY_REPO = "valkey-io/valkey"

# Members of this team may command the bots.
MAINTAINER_ORG = "valkey-io"
MAINTAINER_TEAM = "contributors"

# Branches the agent pushes. Other workflows recognize the agent's PRs by them.
# A sweep branch is ``<prefix>/<target branch>``. A release-cut branch is a
# throwaway ``<prefix>/<version>-<stage>`` that PRs into the M.m release line.
SWEEP_BRANCH_PREFIX = "agent/backport/sweep"
RELEASE_CUT_BRANCH_PREFIX = "agent/release-cut"

# The project-board field and option that queue a PR for backporting.
BACKPORT_STATUS_FIELD = "Status"
BACKPORT_STATUS_OPTION = "To be backported"
