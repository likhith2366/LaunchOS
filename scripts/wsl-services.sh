#!/usr/bin/env bash
# Starts Postgres and Redis inside WSL without root, as a fallback for when Docker
# Desktop is not available.
#
# Docker Compose (deploy/docker-compose.yml) is the documented way to run LaunchOS,
# because it is also how several allocator nodes, Prometheus and Grafana come up. This
# script exists because Docker Desktop on Windows is a single point of failure for the
# whole inner loop, and a measurement run should not be blocked on it.
#
# Both daemons run as the ordinary user from unpacked .deb files under ~/.local.
# Postgres in particular refuses to run as root, so there is nothing unusual about this.
#
#   scripts/wsl-services.sh start|stop|status
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

PREFIX="$HOME/.local/launchos-services"
PGBIN="$PREFIX/usr/lib/postgresql/16/bin"
PGDATA="$PREFIX/pgdata"
PGLOG="$PREFIX/postgres.log"
REDISLOG="$PREFIX/redis.log"
PGPORT="${PGPORT:-5432}"
REDISPORT="${REDISPORT:-6379}"
export LD_LIBRARY_PATH="$PREFIX/usr/lib/x86_64-linux-gnu:$PREFIX/usr/lib/postgresql/16/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

fetch() {
  mkdir -p "$PREFIX/debs"
  cd "$PREFIX/debs"
  # postgresql-16 needs its client and common packages plus libpq; the rest are
  # shared libraries Ubuntu's build links against.
  apt-get download \
    postgresql-16 postgresql-client-16 postgresql-common postgresql-client-common \
    libpq5 libllvm18 libxslt1.1 redis-server redis-tools libjemalloc2 liblzf1 \
    libicu74 liblz4-1 libzstd1 libssl3t64 libsystemd0 libselinux1 libpam0g \
    libgssapi-krb5-2 libldap2 libxml2 2>&1 | tail -2
  for deb in ./*.deb; do dpkg -x "$deb" "$PREFIX"; done
}

init_db() {
  if ! [[ -f "$PGDATA/PG_VERSION" ]]; then
    echo "==> initdb"
    mkdir -p "$PGDATA"
    chmod 700 "$PGDATA"
    "$PGBIN/initdb" -D "$PGDATA" -U launchos --auth=trust --encoding=UTF8 >/dev/null
  fi
  # Appended idempotently, since initdb only runs once but these may need changing.
  if ! grep -q "^# launchos" "$PGDATA/postgresql.conf"; then
    mkdir -p "$PREFIX/sockets"
    # Ubuntu's build defaults its socket directory to /var/run/postgresql, which only
    # root can create. Everything else here is sizing for this workload: enough
    # connections for several allocator nodes plus the simulator, and a shared_buffers
    # that is not the 128 kB default.
    cat >>"$PGDATA/postgresql.conf" <<CONF
# launchos
listen_addresses = '127.0.0.1'
unix_socket_directories = '$PREFIX/sockets'
max_connections = 300

# fsync and synchronous_commit stay on. Every durability claim this project makes rests
# on commits being real, and the cost of that is itself one of the measurements.
fsync = on
synchronous_commit = on

# Group commit, measured and rejected.
#
# commit_delay makes a committing transaction pause this many microseconds when others
# are in flight, so they share one WAL flush. It is the textbook answer when the slowest
# query in a saturated run is waiting on IO, which this one was.
#
# It made things worse. Five isolated runs each: 969 allocations/s without it against
# 730 with it plus wal_compression. The fsyncs were evidently already being batched
# effectively, so the delay was pure added latency on the critical path of every sale -
# and a sale commits two or three times, so it paid the delay twice or three times over.
#
# Left here set to zero, with the reasoning, because "we should enable group commit" is
# the first thing anyone will suggest.
commit_delay = 0

# wal_compression was measured at the same time and is off for the same reason: it trades
# CPU for write bandwidth, and on a box where Postgres, Redis, the node and the load
# generator share eight cores, CPU is not the thing in surplus.
wal_compression = off

shared_buffers = 1GB
effective_cache_size = 4GB
work_mem = 8MB

# Fewer, better-spread checkpoints. At max_wal_size = 1GB a heavy run checkpoints
# repeatedly and each one stalls writes; raising it trades disk for steadier latency, and
# completion_target spreads the flush rather than dumping it.
max_wal_size = 2GB
min_wal_size = 512MB
checkpoint_completion_target = 0.9
wal_buffers = 64MB
log_min_duration_statement = 2000
CONF
  fi
}

start() {
  if ! [[ -x "$PGBIN/postgres" ]] || ! [[ -x "$PREFIX/usr/bin/redis-server" ]]; then
    echo "==> fetching packages into $PREFIX (no root needed)"
    fetch
  fi
  init_db

  if "$PGBIN/pg_isready" -h 127.0.0.1 -p "$PGPORT" -q 2>/dev/null; then
    echo "postgres already up on $PGPORT"
  else
    echo "==> starting postgres on $PGPORT"
    "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGLOG" -o "-p $PGPORT" -w start
    "$PGBIN/createdb" -h 127.0.0.1 -p "$PGPORT" -U launchos launchos 2>/dev/null || true
  fi

  if "$PREFIX/usr/bin/redis-cli" -p "$REDISPORT" ping >/dev/null 2>&1; then
    echo "redis already up on $REDISPORT"
  else
    echo "==> starting redis on $REDISPORT"
    # appendonly so the durability gap Redis would introduce is at least bounded;
    # LaunchOS never treats Redis as authoritative either way.
    "$PREFIX/usr/bin/redis-server" \
      --port "$REDISPORT" --daemonize yes --appendonly yes \
      --dir "$PREFIX" --logfile "$REDISLOG" --save ''
  fi
  status
}

stop() {
  if [[ -f "$PGDATA/postmaster.pid" ]]; then
    "$PGBIN/pg_ctl" -D "$PGDATA" -m fast stop || true
  fi
  "$PREFIX/usr/bin/redis-cli" -p "$REDISPORT" shutdown nosave 2>/dev/null || true
  echo "stopped"
}

status() {
  if "$PGBIN/pg_isready" -h 127.0.0.1 -p "$PGPORT" -q 2>/dev/null; then
    echo "postgres: up on $PGPORT ($("$PGBIN/postgres" --version))"
  else
    echo "postgres: down"
  fi
  if "$PREFIX/usr/bin/redis-cli" -p "$REDISPORT" ping >/dev/null 2>&1; then
    echo "redis: up on $REDISPORT"
  else
    echo "redis: down"
  fi
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
