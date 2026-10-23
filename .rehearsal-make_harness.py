#!/usr/bin/env python3
"""Turn an upstream valkey-ci-agent tree into a fork rehearsal harness.

Only identity substitutions: valkey-io -> sarthakaggarwal97, the App token
steps -> the rehearsal PAT (recording the permissions production requests),
and the bot/team identity checks -> the fork owner. Everything else runs as
merged upstream.
"""
from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(sys.argv[1])
OWNER = "sarthakaggarwal97"
USER_ID = "25262500"
changes: list[str] = []


def edit(path: str, old: str, new: str, *, count: int | None = None, regex: bool = False) -> None:
    p = ROOT / path
    text = p.read_text()
    if regex:
        updated, n = re.subn(old, new, text)
    else:
        n = text.count(old)
        updated = text.replace(old, new)
    if n == 0 or (count is not None and n != count):
        raise SystemExit(f"{path}: expected {count or '>=1'} of {old!r}, found {n}")
    p.write_text(updated)
    changes.append(f"{path}: {n}x {old[:60]!r}")


TOKEN_STEP = re.compile(
    r"(?P<indent>[ ]+)uses: actions/create-github-app-token@[0-9a-f]+ # v[\d.]+\n"
    r"(?P=indent)with:\n(?P<with>(?:(?P=indent)  .*\n)+)"
)


def pat_steps(text: str) -> tuple[str, int]:
    def repl(m: re.Match[str]) -> str:
        ind = m.group("indent")
        lines = [ln.strip() for ln in m.group("with").splitlines()]
        requested = [ln for ln in lines if ln.startswith(("permission-", "repositories:", "owner:"))]
        # repositories may be a block list; collect continuation entries too
        record = "; ".join(ln for ln in lines if not ln.startswith(("app-id", "private-key", "#")))
        out = [
            f"{ind}# REHEARSAL: the PAT stands in for the App token. Production requests:",
            f"{ind}#   {record}",
            f"{ind}shell: bash",
            f"{ind}env:",
            f"{ind}  PAT: ${{{{ secrets.REHEARSAL_PAT }}}}",
            f"{ind}run: |",
            f"{ind}  echo '::notice title=App token scope (production)::{record}'",
            f'{ind}  echo "token=${{PAT}}" >> "$GITHUB_OUTPUT"',
            f'{ind}  echo "app-slug={OWNER}" >> "$GITHUB_OUTPUT"',
        ]
        del requested
        return "\n".join(out) + "\n"

    return TOKEN_STEP.subn(repl, text)


wf = ROOT / ".github/workflows"
for path in sorted(wf.glob("release*.yml")):
    rel = str(path.relative_to(ROOT))
    text = path.read_text()
    text, n = pat_steps(text)
    if n:
        changes.append(f"{rel}: {n} App token step(s) -> PAT")
    n2 = text.count("github.repository == 'valkey-io/valkey-ci-agent'")
    text = text.replace(
        "github.repository == 'valkey-io/valkey-ci-agent'",
        f"github.repository == '{OWNER}/valkey-ci-agent'",
    )
    if n2:
        changes.append(f"{rel}: {n2} repository gate(s)")
    path.write_text(text)

edit(".github/workflows/release-notes-cut.yml",
     'export RELEASE_NOTES_REPO="valkey-io/${RELEASE_NOTES_TARGET_REPO_NAME}"',
     f'export RELEASE_NOTES_REPO="{OWNER}/${{RELEASE_NOTES_TARGET_REPO_NAME}}"', count=1)
edit(".github/workflows/release-notes-cut.yml",
     "HAS_APP_CREDS: ${{ secrets.VALKEYRIE_BOT_APP_ID != '' && secrets.VALKEYRIE_BOT_PRIVATE_KEY != '' }}",
     "HAS_APP_CREDS: ${{ secrets.REHEARSAL_PAT != '' }}", count=1)
edit(".github/workflows/release-prepare.yml", "--repo valkey-io/valkey ", f"--repo {OWNER}/valkey ", count=1)
edit(".github/workflows/release-publish.yml",
     "uses: valkey-io/valkey-release-automation/.github/workflows/qualify-release.yml@main",
     f"uses: {OWNER}/valkey-release-automation/.github/workflows/qualify-release.yml@main", count=1)
edit(".github/workflows/release-notes-review.yml",
     "github.event.sender.id == 284117741", f"github.event.sender.id == {USER_ID}", count=1)
# The team lookups cannot run against a personal account: authorize the fork
# owner through the gate's existing allowlist, which production leaves empty.
for name in ("release-notes-review.yml", "release-notes-review-watch.yml"):
    edit(f".github/workflows/{name}",
         "          RELEASE_NOTES_REVIEW_TOKEN:",
         f"          CI_FIX_AUTH_ALLOWLIST: {OWNER}\n          RELEASE_NOTES_REVIEW_TOKEN:")

edit("release_policy.yml", "repo: valkey-io/valkey\n", f"repo: {OWNER}/valkey\n", count=1)
edit("scripts/release/authorize.py",
     '    lookup_failures: list[str] = []\n',
     f'    # REHEARSAL: a personal account has no teams; only the fork owner may act.\n'
     f'    if policy.repo == "{OWNER}/valkey":\n'
     f'        if actor.casefold() != "{OWNER}":\n'
     f'            raise NotAuthorizedError(f"@{{actor}} is not the rehearsal fork owner")\n'
     f'        return\n\n'
     '    lookup_failures: list[str] = []\n', count=1)
edit("scripts/release/tracker.py", '    return login.endswith("[bot]")',
     f'    return login.endswith("[bot]") or login == "{OWNER}"  # REHEARSAL', count=1)
for name in ("valkey", "valkey-ci-agent", "valkey-release-automation"):
    edit("scripts/release/tracker.py", f'default="valkey-io/{name}"', f'default="{OWNER}/{name}"', count=1)

edit("scripts/release_notes/review.py",
     'REPOSITORIES = tuple(f"valkey-io/{name}"', f'REPOSITORIES = tuple(f"{OWNER}/{{name}}"', count=1)
edit("scripts/release_notes/review.py",
     'repo_full_name=f"valkey-io/{repo_name}"', f'repo_full_name=f"{OWNER}/{{repo_name}}"', count=1)
edit("scripts/release_notes/review.py",
     '!= f"{APP_LOGIN}[bot]":', f'!= "{OWNER}":  # REHEARSAL', count=1)
# The PAT posts the bot's "Addressed in" replies as the same user who
# reviews; keep those replies out of the human comments, as [bot] does.
edit("scripts/release_notes/review.py",
     '            and not self.author_login.endswith("[bot]")\n        )',
     '            and not self.author_login.endswith("[bot]")\n'
     f'            and not (self.author_login == "{OWNER}" and self.body.startswith("Addressed in ["))  # REHEARSAL\n'
     '        )', count=1)
edit("scripts/release_notes/review_poll.py",
     'AGENT_REPO = "valkey-io/valkey-ci-agent"', f'AGENT_REPO = "{OWNER}/valkey-ci-agent"', count=1)
edit("scripts/release_notes/review_poll.py",
     'else f"valkey-io/{args.repo}"', f'else f"{OWNER}/{{args.repo}}"', count=1)
for path in ("scripts/release_notes/review_poll.py", "scripts/release_notes/review_handler.py"):
    edit(path, "f\"{os.environ['RELEASE_NOTES_REVIEW_APP_SLUG']}[bot]\"",
         "os.environ['RELEASE_NOTES_REVIEW_APP_SLUG']  # REHEARSAL: PAT user, no [bot]")

(ROOT / "REHEARSAL.md").write_text(
    "# Fork rehearsal harness (do not merge)\n\n"
    "Generated by make_harness.py from upstream main. Changes:\n\n"
    + "\n".join(f"- {c}" for c in changes) + "\n"
)
print("\n".join(changes))
leftover = [
    f"{p.relative_to(ROOT)}:{i}: {ln.strip()}"
    for p in wf.glob("release*.yml")
    for i, ln in enumerate(p.read_text().splitlines(), 1)
    if not ln.strip().startswith("#") and ("uses: actions/create-github-app-token" in ln) or "valkey-io/valkey-ci-agent'" in ln
]
if leftover:
    raise SystemExit("leftover:\n" + "\n".join(leftover))
