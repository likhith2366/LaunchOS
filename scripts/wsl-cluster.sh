#!/usr/bin/env bash
# Starts or stops a cluster of allocator nodes in WSL.
#
#   scripts/wsl-cluster.sh start            # one node on :8080
#   NODES=3 scripts/wsl-cluster.sh start    # three nodes on :8080-:8082
#   MULTI_NODE_MODE=leasedShards NODES=3 scripts/wsl-cluster.sh start
#   scripts/wsl-cluster.sh stop
#   scripts/wsl-cluster.sh kill 1           # kill node 1 only, for recovery tests
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
HERE="$(cd "$(dirname "$0")" && pwd)"
NODES="${NODES:-1}"
BASE_PORT="${BASE_PORT:-8080}"
LOGDIR="${LOGDIR:-/tmp/launchos}"
CONFIG="${CONFIG:-release}"

start() {
  mkdir -p "$LOGDIR"
  for ((i = 0; i < NODES; i++)); do
    port=$((BASE_PORT + i))
    if curl -s -m 1 "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
      echo "node $i already up on :$port"
      continue
    fi
    NODE_INDEX="$i" NODE_COUNT="$NODES" NODE_ID="node-$i" PORT="$port" \
      CONFIG="$CONFIG" \
      MULTI_NODE_MODE="${MULTI_NODE_MODE:-postgresArbitrated}" \
      SWEEP_STRATEGY="${SWEEP_STRATEGY:-bucketed}" \
      nohup bash "$HERE/wsl-run-node.sh" >"$LOGDIR/node-$i.log" 2>&1 &
  done

  for ((i = 0; i < NODES; i++)); do
    port=$((BASE_PORT + i))
    for _ in $(seq 60); do
      if curl -s -m 1 "http://127.0.0.1:$port/health" >/dev/null 2>&1; then break; fi
      sleep 0.5
    done
    if curl -s -m 1 "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
      echo "node $i up on :$port"
    else
      echo "node $i FAILED to start; see $LOGDIR/node-$i.log" >&2
      tail -5 "$LOGDIR/node-$i.log" >&2 || true
      exit 1
    fi
  done
}

stop() {
  # The bracket keeps the pattern from matching this script's own command line.
  pkill -f "[l]aunch-server" || true
  sleep 1
  echo "stopped"
}

kill_one() {
  local idx="$1"
  # SIGKILL, not SIGTERM: the point of a recovery test is a node that dies without
  # getting the chance to release its leases or finish its in-flight checkouts.
  pkill -9 -f "[l]aunch-server.*--port $((BASE_PORT + idx))" \
    || pkill -9 -f "NODE_ID=node-$idx" \
    || true
  echo "killed node $idx"
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  kill) kill_one "${2:?node index required}" ;;
  status)
    for ((i = 0; i < NODES; i++)); do
      port=$((BASE_PORT + i))
      if curl -s -m 1 "http://127.0.0.1:$port/health" >/dev/null 2>&1; then
        echo "node $i: up on :$port"
      else
        echo "node $i: down"
      fi
    done
    ;;
  *) echo "usage: $0 start|stop|status|kill <index>" >&2; exit 2 ;;
esac
