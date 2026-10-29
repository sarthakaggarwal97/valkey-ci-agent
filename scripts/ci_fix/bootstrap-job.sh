#!/usr/bin/env bash
# Install the fixed dependency set used by supported Ubuntu-hosted Valkey jobs.
#
# This script is trusted agent code. It runs before target setup and before
# AWS credentials, with an empty environment and fixed package arguments.
# Target workflow text never becomes a privileged command. macOS images need
# no system packages for the currently supported setup profile.
set -euo pipefail

unset BASH_ENV ENV PYTHONPATH PYTHONHOME NODE_OPTIONS
unset LD_PRELOAD LD_LIBRARY_PATH DYLD_INSERT_LIBRARIES DYLD_LIBRARY_PATH

if [[ "$(uname -s)" == Darwin ]]; then
  exit 0
fi
if [[ "$(uname -s)" != Linux || ! -x /usr/bin/apt-get || ! -x /usr/bin/env \
      || ! -x /usr/bin/timeout ]]; then
  echo "::error::The exact CI-fix dependency profile supports Ubuntu and macOS hosts only"
  exit 1
fi

as_root() {
  if (( EUID == 0 )); then "$@"; else /usr/bin/sudo -n "$@"; fi
}

clean_env=(
  /usr/bin/env -i
  HOME=/tmp
  PATH=/usr/bin:/bin:/usr/sbin:/sbin
  DEBIAN_FRONTEND=noninteractive
)
packages=(
  build-essential
  pkg-config
  libssl-dev
  libzstd-dev
  libgtest-dev
  libgmock-dev
  tcl8.6
  tclx
  tcl-tls
  valgrind
  acl
)

as_root "${clean_env[@]}" /usr/bin/timeout --kill-after=5s 300s \
  /usr/bin/apt-get update -q
as_root "${clean_env[@]}" /usr/bin/timeout --kill-after=5s 300s \
  /usr/bin/apt-get install -y -q --no-install-recommends "${packages[@]}"
