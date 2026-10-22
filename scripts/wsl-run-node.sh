#!/usr/bin/env bash
# Runs one allocator node in WSL against the services from scripts/wsl-services.sh.
#
#   scripts/wsl-run-node.sh                     # single node on :8080
#   NODE_INDEX=1 PORT=8081 NODE_COUNT=2 scripts/wsl-run-node.sh
set -euo pipefail

export PATH="$HOME/.local/share/swiftly/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
WSL_ROOT="${LAUNCHOS_WSL_ROOT:-$HOME/launchos}"
CONFIG="${CONFIG:-release}"

export POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_USER="${POSTGRES_USER:-launchos}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-launchos}"
export POSTGRES_DB="${POSTGRES_DB:-launchos}"
export REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
export REDIS_PORT="${REDIS_PORT:-6379}"
export NODE_INDEX="${NODE_INDEX:-0}"
export NODE_COUNT="${NODE_COUNT:-1}"
export NODE_ID="${NODE_ID:-node-$NODE_INDEX}"
export PORT="${PORT:-8080}"
export MULTI_NODE_MODE="${MULTI_NODE_MODE:-postgresArbitrated}"
export SWEEP_STRATEGY="${SWEEP_STRATEGY:-bucketed}"
export LOG_LEVEL="${LOG_LEVEL:-notice}"
# The connection pool is per node. Several nodes plus the simulator have to fit inside
# Postgres's max_connections, so this is deliberately modest rather than generous.
export POSTGRES_POOL="${POSTGRES_POOL:-32}"

BIN="$WSL_ROOT/.build/$CONFIG/launch-server"
if ! [[ -x "$BIN" ]]; then
  echo "no binary at $BIN; run scripts/wsl-build.sh -c $CONFIG first" >&2
  exit 1
fi

echo "==> node $NODE_ID on :$PORT ($MULTI_NODE_MODE, sweep=$SWEEP_STRATEGY)"
exec "$BIN" serve --hostname 0.0.0.0 --port "$PORT"
