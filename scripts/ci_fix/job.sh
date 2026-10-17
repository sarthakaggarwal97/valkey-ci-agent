#!/usr/bin/env bash
# Prepare one planned fix inside the job that hosts it.
#
# Run from the workspace the agent repository was checked out into
# (.ci-fix-agent) by the steps scripts/valkey_workflows/normalize.py's
# fix_steps() generates:
#
# - in a copied Valkey job, the workspace is Valkey's own
#   checkout, already set up and built by the job's steps. The fix is
#   prepared there, rebuilt and tested with the job's own commands
#   (CI_FIX_BUILD, CI_FIX_RUNTEST, CI_FIX_RUNTEST_MODULEAPI). The job runs
#   on an Ubuntu or macOS runner, or as root in a container, where a
#   checkout without git in it is an archive of CI_FIX_COMMIT;
# - in ci-fix-run.yml's fix-local job, for a failure no copied job can
#   host, the workspace holds nothing else, and the fix is prepared on a
#   fresh clone with the bot's own approximation of the job.
#
# The steps before this one installed the agent and its dependencies
# (.github/actions/setup-agent) and the model's AWS credentials.
#
# The agent's code moves out of the workspace first: the agent's account owns
# the checkout while it works, and must not be able to change the code that
# judges and exports its work.
set -euo pipefail

# A container job runs as root, and has no sudo.
as_root() {
  if (( EUID == 0 )); then "$@"; else sudo "$@"; fi
}

agent="${RUNNER_TEMP}/ci-fix-agent"
home="${GITHUB_WORKSPACE}/.ci-fix-agent"

# The job's own cleanup runs the post phase of every action it used, and the
# agent's setup is one of this checkout's own composite actions: the runner
# reads it from ${home} again then. In a copied job the agent's account owns
# the workspace while it works, so whatever is at ${home} by now may be the
# account's: it goes first (a link is removed, never followed), or a move
# would land the real code inside it and the runner would read the account's
# action instead.
restore_agent() {
  [[ -d "${agent}" ]] || return 0
  as_root rm -rf -- "${home}"
  mv -- "${agent}" "${home}"
}

mv -- "${home}" "${agent}"
trap 'restore_agent || echo "::warning::could not put the CI-fix agent back for the job cleanup"' EXIT

# The agent and its commands run as this account, which cannot read the
# job's credentials (scripts/common/isolation.py).
if [[ "$(uname -s)" == Darwin ]]; then
  # macOS has no useradd: the account gets a group of its own, not staff,
  # which can read the runner's files.
  id=$(( $( (dscl . -list /Users UniqueID; dscl . -list /Groups PrimaryGroupID) \
    | awk '{print $2}' | sort -n | tail -1) + 1 ))
  sudo dscl . -create /Groups/ci-fix-agent PrimaryGroupID "${id}"
  sudo dscl . -create /Users/ci-fix-agent
  sudo dscl . -create /Users/ci-fix-agent UniqueID "${id}"
  sudo dscl . -create /Users/ci-fix-agent PrimaryGroupID "${id}"
  sudo dscl . -create /Users/ci-fix-agent UserShell /bin/bash
  sudo dscl . -create /Users/ci-fix-agent NFSHomeDirectory /Users/ci-fix-agent
  sudo mkdir -p /Users/ci-fix-agent
  sudo chown "${id}:${id}" /Users/ci-fix-agent
else
  # Above the runner's own account, which in a container is just a number
  # that owns the workspace and the runner's files there.
  above=$(( $(stat -c %u "${RUNNER_TEMP}") + 1 ))
  as_root useradd --create-home --shell /bin/bash -K UID_MIN="${above}" -K GID_MIN="${above}" ci-fix-agent
fi
export CI_FIX_AGENT_USER=ci-fix-agent
export CI_AGENT_EVIDENCE_DIR="${RUNNER_TEMP}/ci-fix-out/agent-evidence"

args=()
repo="${GITHUB_WORKSPACE}"
if [[ -n "${CI_FIX_BUILD:-}" ]] && (( EUID == 0 )); then
  # The runner made the workspace as its own account; git trusts only a
  # checkout the account running it owns.
  chown 0:0 "${repo}"
fi
if [[ -n "${CI_FIX_BUILD:-}" && ! -d "${repo}/.git" ]]; then
  # Without git in its container, the job checked its commit out as an
  # archive: make it the shallow clone a checkout with git would have made,
  # keeping the files the job built.
  git init -q "${repo}"
  git -C "${repo}" remote add origin "https://github.com/${CI_FIX_REPOSITORY}"
  git -C "${repo}" fetch -q --depth=1 origin "${CI_FIX_COMMIT}"
  git -C "${repo}" reset -q FETCH_HEAD
fi
if [[ -d "${repo}/.git" ]]; then
  # The account must reach the checkout, git must not send the job's token,
  # and the search for an existing fix needs the history.
  dir="${repo}"
  while [[ "${dir}" != / ]]; do
    dir="$(dirname "${dir}")"
    as_root chmod o+x "${dir}"
  done
  git -C "${repo}" config --local --unset-all http.https://github.com/.extraheader || true
  # actions/checkout v6 keeps the token in a file it includes instead.
  for key in $(git -C "${repo}" config --local --name-only --get-regexp '^includeif\.' || true); do
    git -C "${repo}" config --local --unset-all "${key}"
  done
  if [[ -f "${repo}/.git/shallow" ]]; then
    git -C "${repo}" fetch -q --unshallow --filter=blob:none origin
  fi
  git -C "${repo}" remote set-head origin --auto >/dev/null
  args+=(--repo-dir "${repo}")
fi

cd "${agent}"
python -m scripts.ci_fix.run fix \
  --plan "${RUNNER_TEMP}/ci-fix-plan" --out "${RUNNER_TEMP}/ci-fix-out" "${args[@]}"
