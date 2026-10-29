#!/usr/bin/env bash
# Replay the target job's sanitized setup as a disposable account.
#
# Trusted code has already cloned the exact target and dependency commits into
# one physical checkout directly below /tmp (or macOS's physical
# /private/tmp). The setup, command, and model identities all use that same
# pathname, so no runner-workspace ACL is needed and generated absolute paths
# stay valid. The real GITHUB_WORKSPACE remains trusted action state.
set -euo pipefail

unset BASH_ENV ENV PYTHONPATH PYTHONHOME NODE_OPTIONS
unset LD_PRELOAD LD_LIBRARY_PATH DYLD_INSERT_LIBRARIES DYLD_LIBRARY_PATH

SETUP_USER=ci-fix-setup

as_root() {
  if (( EUID == 0 )); then "$@"; else /usr/bin/sudo -n "$@"; fi
}

fail() {
  echo "::error::$1"
  exit 1
}

runner_temp="$(cd -- "${RUNNER_TEMP}" && pwd -P)"
if [[ "${RUNNER_TEMP}" != /* || -L "${RUNNER_TEMP}" \
      || "${runner_temp}" == / || "${runner_temp}" != "${RUNNER_TEMP}" ]]; then
  fail "The runner temp directory is not a safe physical directory"
fi
as_root chmod go-rwx "${runner_temp}"

workspace_state="${runner_temp}/ci-fix-sandbox-state"
account_state="${runner_temp}/ci-fix-accounts-state"
if [[ ! -f "${workspace_state}" || -L "${workspace_state}" ]]; then
  fail "The trusted CI-fix sandbox state is missing or redirected"
fi
exec 3< "${workspace_state}"
IFS= read -r state_header <&3 || fail "The CI-fix sandbox state is incomplete"
IFS= read -r repo <&3 || fail "The CI-fix sandbox state is incomplete"
IFS= read -r repository <&3 || fail "The CI-fix sandbox state is incomplete"
IFS= read -r commit <&3 || fail "The CI-fix sandbox state is incomplete"
IFS= read -r mode <&3 || fail "The CI-fix sandbox state is incomplete"
IFS= read -r pull_number <&3 || fail "The CI-fix sandbox state is incomplete"
IFS= read -r anchor_device <&3 || fail "The CI-fix sandbox state is incomplete"
IFS= read -r anchor_inode <&3 || fail "The CI-fix sandbox state is incomplete"
if IFS= read -r _unexpected <&3; then
  fail "The CI-fix sandbox state has unexpected data"
fi
exec 3<&-

temp_root="$(cd -- /tmp && pwd -P)"
repo_physical="$(cd -- "${repo}" && pwd -P)"
anchor="${repo%/*}"
anchor_physical="$(cd -- "${anchor}" && pwd -P)"
if [[ "${state_header}" != ci-fix-sandbox-v2 \
      || ! "${repository}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ \
      || ! "${commit}" =~ ^[0-9A-Fa-f]{40}$ \
      || ! "${pull_number}" =~ ^(0|[1-9][0-9]*)$ \
      || ! "${anchor_device}" =~ ^[0-9]+$ \
      || ! "${anchor_inode}" =~ ^[1-9][0-9]*$ \
      || "${mode}" != job \
      || ( "${temp_root}" != /tmp && "${temp_root}" != /private/tmp ) \
      || "${repo}" != "${repo_physical}" || -L "${repo}" \
      || "${repo##*/}" != repo \
      || "${anchor}" != "${anchor_physical}" || -L "${anchor}" \
      || "${anchor%/*}" != "${temp_root}" \
      || "${anchor##*/}" != valkey-ci-fix-* ]]; then
  fail "The trusted CI-fix sandbox state is invalid"
fi

plan_name="${runner_temp}/ci-fix-plan"
plan="$(cd -- "${plan_name}" && pwd -P)"
if [[ "${plan}" != "${plan_name}" || ! -f "${plan}/setup.sh" || -L "${plan}/setup.sh" ]]; then
  fail "The authoritative CI-fix setup plan is missing or redirected"
fi
as_root chmod -R go-rwx "${plan}"
as_root rm -rf -- "${runner_temp}/ci-fix-out"

agent_name="${GITHUB_WORKSPACE}/.ci-fix-agent"
if [[ ! -d "${agent_name}" || -L "${agent_name}" ]]; then
  fail "The trusted CI-fix agent checkout is missing or redirected"
fi
agent="$(cd -- "${agent_name}" && pwd -P)"
python_bin="$(command -v python)"
if [[ "${python_bin}" != /* || "${python_bin}" == "${repo}/"* ]]; then
  fail "The trusted CI-fix Python did not resolve outside the target checkout"
fi
case "${repo}${runner_temp}${plan}${agent}${python_bin}" in
  *$'\n'*) fail "CI-fix control paths may not contain a newline" ;;
esac

preserve_user=""

account_command() {
  (
    cd "${agent}"
    "${python_bin}" -m scripts.ci_fix.accounts "$@"
  )
}

cleanup() {
  local status=$?
  if (( status != 0 )) || [[ -z "${preserve_user}" ]]; then
    set +e
    bash "${agent}/scripts/ci_fix/job.sh" cleanup || status=1
  fi
  exit "${status}"
}
trap cleanup EXIT

account_command create --state-file "${account_state}" --user "${SETUP_USER}"

(
  cd "${agent}"
  "${python_bin}" -m scripts.ci_fix.setup_job \
    --repo-dir "${repo}" \
    --setup "${plan}/setup.sh" \
    --runner-temp "${runner_temp}" \
    --user "${SETUP_USER}"
)

# Keep the now-quiescent numeric identity allocated until job.sh has created
# distinct command and model identities. Cleanup above has already removed
# cron/at persistence before any AWS credential exists.
preserve_user=1
