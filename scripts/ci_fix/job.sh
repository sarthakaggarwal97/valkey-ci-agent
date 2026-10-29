#!/usr/bin/env bash
# Prepare, run, and clean one CI fix in its isolated stable checkout.
#
# Trusted workflow code creates the checkout directly below physical /tmp (or
# /private/tmp on macOS) before credentials exist. Target setup, test commands,
# and model processes reuse that one pathname. The runner's GITHUB_WORKSPACE
# contains only the trusted agent/action checkout and is never handed to a
# disposable identity, so parent-directory ACL changes are unnecessary.
set -euo pipefail

unset BASH_ENV ENV PYTHONPATH PYTHONHOME NODE_OPTIONS
unset LD_PRELOAD LD_LIBRARY_PATH DYLD_INSERT_LIBRARIES DYLD_LIBRARY_PATH

SETUP_USER=ci-fix-setup
COMMAND_USER=ci-fix-command
AGENT_USER=ci-fix-agent

as_root() {
  if (( EUID == 0 )); then "$@"; else /usr/bin/sudo -n "$@"; fi
}

fail() {
  echo "::error::$1"
  exit 1
}

paths() {
  runner_temp="$(cd -- "${RUNNER_TEMP}" && pwd -P)"
  if [[ "${RUNNER_TEMP}" != /* || -L "${RUNNER_TEMP}" \
        || "${runner_temp}" == / || "${runner_temp}" != "${RUNNER_TEMP}" ]]; then
    fail "The runner temp directory is not a safe physical directory"
  fi

  plan_name="${runner_temp}/ci-fix-plan"
  plan="$(cd -- "${plan_name}" && pwd -P)"
  if [[ "${plan}" != "${plan_name}" || ! -f "${plan}/plan.json" || -L "${plan}/plan.json" ]]; then
    fail "The authoritative CI-fix plan is missing or redirected"
  fi

  workspace_state="${runner_temp}/ci-fix-sandbox-state"
  account_state="${runner_temp}/ci-fix-accounts-state"
  if [[ ! -f "${workspace_state}" || -L "${workspace_state}" ]]; then
    fail "The trusted CI-fix sandbox state is missing or redirected"
  fi
  exec 3< "${workspace_state}"
  IFS= read -r workspace_header <&3 || fail "The CI-fix sandbox state is incomplete"
  IFS= read -r repo <&3 || fail "The CI-fix sandbox state is incomplete"
  IFS= read -r workspace_repository <&3 || fail "The CI-fix sandbox state is incomplete"
  IFS= read -r workspace_commit <&3 || fail "The CI-fix sandbox state is incomplete"
  IFS= read -r workspace_mode <&3 || fail "The CI-fix sandbox state is incomplete"
  IFS= read -r workspace_pull_number <&3 || fail "The CI-fix sandbox state is incomplete"
  IFS= read -r workspace_anchor_device <&3 || fail "The CI-fix sandbox state is incomplete"
  IFS= read -r workspace_anchor_inode <&3 || fail "The CI-fix sandbox state is incomplete"
  if IFS= read -r _unexpected <&3; then
    fail "The CI-fix sandbox state has unexpected data"
  fi
  exec 3<&-

  temp_root="$(cd -- /tmp && pwd -P)"
  repo_physical="$(cd -- "${repo}" && pwd -P)"
  anchor="${repo%/*}"
  anchor_physical="$(cd -- "${anchor}" && pwd -P)"
  if [[ "${workspace_header}" != ci-fix-sandbox-v2 \
        || ! "${workspace_repository}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ \
        || ! "${workspace_commit}" =~ ^[0-9A-Fa-f]{40}$ \
        || ! "${workspace_pull_number}" =~ ^(0|[1-9][0-9]*)$ \
        || ! "${workspace_anchor_device}" =~ ^[0-9]+$ \
        || ! "${workspace_anchor_inode}" =~ ^[1-9][0-9]*$ \
        || ( "${workspace_mode}" != job && "${workspace_mode}" != local ) \
        || ( "${temp_root}" != /tmp && "${temp_root}" != /private/tmp ) \
        || "${repo}" != "${repo_physical}" || -L "${repo}" \
        || "${repo##*/}" != repo \
        || "${anchor}" != "${anchor_physical}" || -L "${anchor}" \
        || "${anchor%/*}" != "${temp_root}" \
        || "${anchor##*/}" != valkey-ci-fix-* ]]; then
    fail "The trusted CI-fix sandbox state is invalid"
  fi

  agent_name="${GITHUB_WORKSPACE}/.ci-fix-agent"
  if [[ ! -d "${agent_name}" || -L "${agent_name}" ]]; then
    fail "The trusted CI-fix agent checkout is missing or redirected"
  fi
  agent="$(cd -- "${agent_name}" && pwd -P)"
  out="${runner_temp}/ci-fix-out"
  control="${runner_temp}/ci-fix-control"
  state="${control}/ready"
}

create_account() {
  account_command create --state-file "${account_state}" --user "$1"
}

account_command() {
  (
    cd "${agent}"
    "${python_bin}" -m scripts.ci_fix.accounts "$@"
  )
}

cleanup_paths() {
  local agent_name
  runner_temp="$(cd -- "${RUNNER_TEMP}" && pwd -P)"
  if [[ "${RUNNER_TEMP}" != /* || -L "${RUNNER_TEMP}" \
        || "${runner_temp}" == / || "${runner_temp}" != "${RUNNER_TEMP}" ]]; then
    echo "::error::The runner temp directory is not a safe physical directory"
    return 1
  fi
  workspace_state="${runner_temp}/ci-fix-sandbox-state"
  account_state="${runner_temp}/ci-fix-accounts-state"
  control="${runner_temp}/ci-fix-control"
  agent_name="${GITHUB_WORKSPACE}/.ci-fix-agent"
  if [[ ! -d "${agent_name}" || -L "${agent_name}" ]]; then
    echo "::error::The trusted CI-fix agent checkout is missing or redirected"
    return 1
  fi
  agent="$(cd -- "${agent_name}" && pwd -P)"
  python_bin="$(command -v python)"
  if [[ "${python_bin}" != /* || "${python_bin}" == "${runner_temp}/"* ]]; then
    echo "::error::A trusted Python is unavailable for mount-safe sandbox cleanup"
    return 1
  fi
}

remove_accounts() {
  if [[ ! -e "${account_state}" && ! -L "${account_state}" ]]; then
    if [[ -e "${workspace_state}" || -L "${workspace_state}" ]]; then
      echo "::error::The CI-fix account ownership state is missing"
      return 1
    fi
    return 0
  fi
  if [[ ! -f "${account_state}" || -L "${account_state}" ]]; then
    echo "::error::The CI-fix account ownership state is redirected"
    return 1
  fi
  (
    cd "${agent}"
    as_root "${python_bin}" -m scripts.ci_fix.accounts \
      cleanup --state-file "${account_state}"
  )
}

remove_workspace() {
  [[ -e "${workspace_state}" || -L "${workspace_state}" ]] || return 0
  if [[ ! -f "${workspace_state}" || -L "${workspace_state}" ]]; then
    echo "::error::The trusted CI-fix sandbox state is missing or redirected"
    return 1
  fi
  (
    cd "${agent}"
    as_root "${python_bin}" -m scripts.ci_fix.prepare_job_workspace \
      cleanup --state-file "${workspace_state}"
  )
}

prepare() {
  paths
  as_root chmod go-rwx "${runner_temp}"
  as_root chmod -R go-rwx "${plan}"
  python_bin="$(command -v python)"
  if [[ "${python_bin}" != /* || "${python_bin}" == "${repo}/"* ]]; then
    fail "The trusted CI-fix Python did not resolve outside the target checkout"
  fi
  case "${repo}${runner_temp}${plan}${out}${agent}${python_bin}" in
    *$'\n'*) fail "CI-fix control paths may not contain a newline" ;;
  esac
  if [[ "$(uname -s)" == Linux && "$(command -v setfacl || true)" != /* ]]; then
    fail "The trusted ACL scrubber is unavailable"
  fi

  if [[ "${workspace_mode}" == job ]]; then
    if ! id "${SETUP_USER}" >/dev/null 2>&1; then
      fail "The isolated setup account was not preserved for preflight"
    fi
    account_command quiesce --state-file "${account_state}" --user "${SETUP_USER}"
  fi

  as_root rm -rf -- "${out}" "${control}"
  mkdir -m 700 "${out}" "${control}"
  install -m 700 "$0" "${control}/job.sh"

  prepared=""
  rollback() {
    local status=$?
    if [[ -z "${prepared}" ]]; then
      if remove_accounts; then
        remove_workspace || status=1
      else
        status=1
      fi
      as_root rm -rf -- "${out}" "${control}"
    fi
    exit "${status}"
  }
  trap rollback EXIT

  create_account "${COMMAND_USER}"
  create_account "${AGENT_USER}"
  setup_uid="$(id -u "${SETUP_USER}" 2>/dev/null || true)"
  command_uid="$(id -u "${COMMAND_USER}")"
  agent_uid="$(id -u "${AGENT_USER}")"
  if [[ "${command_uid}" == "${agent_uid}" \
        || ( -n "${setup_uid}" && ( "${setup_uid}" == "${command_uid}" \
                                    || "${setup_uid}" == "${agent_uid}" ) ) ]]; then
    fail "CI-fix isolation accounts did not receive distinct identities"
  fi

  {
    printf '%s\n' "${repo}"
    printf '%s\n' "${runner_temp}"
    printf '%s\n' "${plan}"
    printf '%s\n' "${out}"
    printf '%s\n' "${agent}"
    printf '%s\n' "${python_bin}"
    printf '%s\n' "${workspace_mode}"
    printf '%s\n' "${setup_uid}"
    printf '%s\n' "${command_uid}"
    printf '%s\n' "${agent_uid}"
  } > "${state}"
  chmod 600 "${state}"
  prepared=1
  trap - EXIT
}

read_state() {
  if [[ ! -f "${state}" || -L "${state}" ]]; then
    fail "The credential-free CI-fix preflight did not complete"
  fi
  exec 3< "${state}"
  IFS= read -r state_repo <&3
  IFS= read -r state_temp <&3
  IFS= read -r state_plan <&3
  IFS= read -r state_out <&3
  IFS= read -r state_agent <&3
  IFS= read -r python_bin <&3
  IFS= read -r state_mode <&3
  IFS= read -r state_setup_uid <&3
  IFS= read -r state_command_uid <&3
  IFS= read -r state_agent_uid <&3
  if IFS= read -r _unexpected <&3; then
    fail "The sealed CI-fix preflight state has unexpected data"
  fi
  exec 3<&-
  if [[ "${state_repo}" != "${repo}" || "${state_temp}" != "${runner_temp}" \
        || "${state_plan}" != "${plan}" || "${state_out}" != "${out}" \
        || "${state_agent}" != "${agent}" \
        || "${state_mode}" != "${workspace_mode}" \
        || "${state_setup_uid}" != "$(id -u "${SETUP_USER}" 2>/dev/null || true)" \
        || "${state_command_uid}" != "$(id -u "${COMMAND_USER}" 2>/dev/null || true)" \
        || "${state_agent_uid}" != "$(id -u "${AGENT_USER}" 2>/dev/null || true)" \
        || "${state_command_uid}" == "${state_agent_uid}" \
        || ( -n "${state_setup_uid}" && ( "${state_setup_uid}" == "${state_command_uid}" \
                                          || "${state_setup_uid}" == "${state_agent_uid}" ) ) \
        || ! -x "${python_bin}" || ! -d "${agent}" || -L "${agent}" \
        || ! -d "${out}" || -L "${out}" ]]; then
    fail "The sealed CI-fix preflight state no longer matches this job"
  fi
}

run_fix() {
  paths
  read_state
  export CI_FIX_AGENT_USER="${AGENT_USER}"
  export CI_FIX_COMMAND_USER="${COMMAND_USER}"
  export CI_AGENT_EVIDENCE_DIR="${out}/agent-evidence"
  cd "${agent}"
  "${python_bin}" -m scripts.ci_fix.run fix \
    --plan "${plan}" --out "${out}" --repo-dir "${repo}"
}

cleanup() {
  local status=0
  if cleanup_paths; then
    if remove_accounts; then
      remove_workspace || status=1
    else
      status=1
    fi
  else
    status=1
  fi
  if [[ -n "${control:-}" && -d "${control}" && ! -L "${control}" ]]; then
    as_root rm -rf -- "${control}" || status=1
  fi
  return "${status}"
}

case "${1:-}" in
  prepare) prepare ;;
  run) run_fix ;;
  cleanup) cleanup ;;
  *)
    echo "usage: $0 prepare|run|cleanup" >&2
    exit 2
    ;;
esac
