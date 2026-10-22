#!/usr/bin/env bash
# Runs the test suite in WSL against the local Postgres.
#
# The integration suites skip themselves unless POSTGRES_HOST is set, so the unit tests
# stay runnable on a machine with nothing running. This script points them at the
# database from scripts/wsl-services.sh.
set -euo pipefail

export PATH="$HOME/.local/share/swiftly/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
WIN_ROOT="${LAUNCHOS_WIN_ROOT:-/mnt/a/Projects/LaunchOs}"
WSL_ROOT="${LAUNCHOS_WSL_ROOT:-$HOME/launchos}"

export POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_USER="${POSTGRES_USER:-launchos}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-launchos}"
export POSTGRES_DB="${POSTGRES_DB:-launchos}"
# Each test makes its own launch with a unique sku, so they can share a database. The
# pool is kept small because the suite runs several harnesses at once.
export POSTGRES_POOL="${POSTGRES_POOL:-8}"

# The expiry suite sweeps reservations and asserts how many it reclaimed, so it has to
# be the only sweeper running. A live allocator node reclaims them first through its own
# background sweeper, and the assertions then fail for a reason that has nothing to do
# with the code under test.
HERE="$(cd "$(dirname "$0")" && pwd)"

# Stopping the cluster under a measurement run would silently ruin it: the simulator
# keeps going against a node that is no longer there and the results are garbage. Refuse
# instead, since that mistake is invisible until the numbers look strange.
if pgrep -f "[l]aunch-sim" >/dev/null 2>&1; then
  echo "a launch-sim run is in progress; this script stops the allocator nodes and would" >&2
  echo "ruin it. Wait for the run to finish, or stop it first." >&2
  exit 1
fi

bash "$HERE/wsl-cluster.sh" stop >/dev/null 2>&1 || true

rsync -a --delete \
  "$WIN_ROOT/Package.swift" "$WIN_ROOT/Sources" "$WIN_ROOT/Tests" "$WSL_ROOT/"
cd "$WSL_ROOT"
exec swift test "$@"
