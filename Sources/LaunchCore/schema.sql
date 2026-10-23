-- LaunchOS schema. Postgres is the source of truth for everything that must survive
-- a restart: inventory, reservations, orders, idempotency keys and queue order.
-- Redis holds only derived state (positions, counters) and is never authoritative.

CREATE TABLE IF NOT EXISTS launches (
    id                 UUID PRIMARY KEY,
    sku                TEXT NOT NULL UNIQUE,
    total_units        INTEGER NOT NULL CHECK (total_units >= 0),
    shard_count        INTEGER NOT NULL CHECK (shard_count >= 1),
    collapse_threshold INTEGER NOT NULL DEFAULT 0 CHECK (collapse_threshold >= 0),
    collapsed          BOOLEAN NOT NULL DEFAULT FALSE,
    -- Whether this launch has a waiting room. Durable rather than per-process, so every
    -- node agrees on whether checkout requires admission even though the admission
    -- controller itself is in-memory and per-node.
    requires_admission BOOLEAN NOT NULL DEFAULT FALSE,
    -- Name of this launch's own arrival sequence. A queue position has to be answerable
    -- in constant time, which needs a dense arrival index per launch -- and the shared
    -- queue_seq below cannot give one, because every launch draws from it and their
    -- numbers interleave. A sequence per launch is dense by construction, and sequences
    -- are non-transactional, so drawing from one costs no row lock.
    queue_sequence     TEXT,
    reservation_ttl_ms BIGINT NOT NULL DEFAULT 90000,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The row every request in the system contends on. The whole point of shard_count > 1
-- is that there are shard_count of these per launch instead of one.
CREATE TABLE IF NOT EXISTS inventory_shards (
    launch_id  UUID NOT NULL REFERENCES launches(id) ON DELETE CASCADE,
    shard_id   INTEGER NOT NULL,
    available  INTEGER NOT NULL CHECK (available >= 0),
    reserved   INTEGER NOT NULL CHECK (reserved  >= 0),
    sold       INTEGER NOT NULL CHECK (sold      >= 0),
    PRIMARY KEY (launch_id, shard_id)
);

CREATE TABLE IF NOT EXISTS reservations (
    id             UUID PRIMARY KEY,
    launch_id      UUID NOT NULL REFERENCES launches(id) ON DELETE CASCADE,
    shard_id       INTEGER NOT NULL,
    customer_id    TEXT NOT NULL,
    units          INTEGER NOT NULL CHECK (units > 0),
    state          TEXT NOT NULL CHECK (state IN ('held','committed','cancelled','expired')),
    owner_node     TEXT NOT NULL,
    probes         INTEGER NOT NULL DEFAULT 1,
    expires_at     TIMESTAMPTZ NOT NULL,
    -- Minute bucket of expires_at. Lets the sweeper read one narrow partition per tick
    -- instead of scanning the whole table for expired rows.
    expiry_bucket  BIGINT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at    TIMESTAMPTZ
);

-- Supports the index-scan sweep strategy.
CREATE INDEX IF NOT EXISTS reservations_expiry_idx
    ON reservations (expires_at) WHERE state = 'held';
-- Supports the bucketed sweep strategy.
CREATE INDEX IF NOT EXISTS reservations_bucket_idx
    ON reservations (expiry_bucket) WHERE state = 'held';
CREATE INDEX IF NOT EXISTS reservations_launch_idx
    ON reservations (launch_id, state);

CREATE TABLE IF NOT EXISTS orders (
    id              UUID PRIMARY KEY,
    launch_id       UUID NOT NULL REFERENCES launches(id) ON DELETE CASCADE,
    customer_id     TEXT NOT NULL,
    reservation_id  UUID NOT NULL UNIQUE,
    units           INTEGER NOT NULL CHECK (units > 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_launch_idx ON orders (launch_id);

-- Written in the same transaction as the order it describes. That shared commit is the
-- entire idempotency guarantee: there is no window where an order exists without its key.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key          TEXT PRIMARY KEY,
    launch_id    UUID NOT NULL,
    customer_id  TEXT NOT NULL,
    fingerprint  TEXT NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('in_flight','completed','failed')),
    response     JSONB,
    owner_node   TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idempotency_inflight_idx
    ON idempotency_keys (updated_at) WHERE state = 'in_flight';

-- The durable queue log. seq comes from a Postgres sequence, so arrival order is
-- globally ordered and survives both a service restart and a wiped Redis.
-- The (launch_id, customer_id) primary key is what makes a retry keep its position:
-- the second join is a no-op that returns the row the first one created.
CREATE SEQUENCE IF NOT EXISTS queue_seq;

CREATE TABLE IF NOT EXISTS queue_entries (
    launch_id    UUID NOT NULL REFERENCES launches(id) ON DELETE CASCADE,
    customer_id  TEXT NOT NULL,
    seq          BIGINT NOT NULL DEFAULT nextval('queue_seq'),
    -- Dense 1-based arrival index within this launch. seq still decides ordering and
    -- durability; this exists so "how many are ahead of me" is arithmetic rather than a
    -- count over everyone ahead. Zero means no per-launch sequence was available, and
    -- positions fall back to counting.
    arrival_index BIGINT NOT NULL DEFAULT 0,
    bucket       BIGINT NOT NULL,
    arrival_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    state        TEXT NOT NULL CHECK (state IN ('waiting','admitted','expired')),
    admitted_seq BIGINT,
    admitted_at  TIMESTAMPTZ,
    retries      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (launch_id, customer_id)
);

CREATE INDEX IF NOT EXISTS queue_waiting_idx
    ON queue_entries (launch_id, seq) WHERE state = 'waiting';
CREATE INDEX IF NOT EXISTS queue_bucket_idx
    ON queue_entries (launch_id, bucket, seq) WHERE state = 'waiting';
-- Supports reconciling the departure counter against the table.
CREATE INDEX IF NOT EXISTS queue_departed_idx
    ON queue_entries (launch_id) WHERE state <> 'waiting';

-- Monotonic counter for admission order, so fairness can be measured as the
-- correlation between arrival seq and admission seq.
CREATE SEQUENCE IF NOT EXISTS admission_seq;

-- Lease-based shard ownership for the multi-node allocator. fence_token comes from a
-- sequence and is only re-issued on a change of owner, so a node that wakes up after a
-- pause can be told its lease is stale.
CREATE SEQUENCE IF NOT EXISTS fence_seq;

CREATE TABLE IF NOT EXISTS shard_leases (
    launch_id   UUID NOT NULL REFERENCES launches(id) ON DELETE CASCADE,
    shard_id    INTEGER NOT NULL,
    owner_node  TEXT NOT NULL,
    fence_token BIGINT NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (launch_id, shard_id)
);

CREATE INDEX IF NOT EXISTS shard_leases_owner_idx ON shard_leases (launch_id, owner_node);

-- Migrations for databases created by an earlier version.
--
-- Everything above is CREATE ... IF NOT EXISTS, which creates a missing table but will
-- not add a column to one that already exists -- so on an existing database the new
-- columns simply never appeared and every query referencing them failed. These are
-- idempotent, so migrate() stays a loop that can run on every boot.
ALTER TABLE launches ADD COLUMN IF NOT EXISTS queue_sequence TEXT;
ALTER TABLE queue_entries ADD COLUMN IF NOT EXISTS arrival_index BIGINT NOT NULL DEFAULT 0;

-- Supports the periodic duplicate-order reconciliation, which joins orders against
-- idempotency keys on (launch, customer). Without these it is two sequential scans and
-- a hash join over whole tables; the check is deliberately infrequent, but infrequent
-- and unindexed still means a multi-hundred-megabyte read competing with a launch.
CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (launch_id, customer_id);
CREATE INDEX IF NOT EXISTS idempotency_launch_customer_idx
    ON idempotency_keys (launch_id, customer_id);
