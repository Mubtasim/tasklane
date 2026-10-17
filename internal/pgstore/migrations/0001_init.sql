CREATE TABLE queues (
    name                  text PRIMARY KEY,
    max_depth             bigint  NOT NULL CHECK (max_depth > 0),
    visibility_timeout_ms bigint  NOT NULL CHECK (visibility_timeout_ms > 0),
    max_attempts          int     NOT NULL CHECK (max_attempts > 0),
    paused                boolean NOT NULL DEFAULT false,
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TYPE job_state AS ENUM ('available', 'leased', 'completed', 'dead', 'cancelled');

-- fillfactor leaves room on each page so state updates can be HOT updates.
CREATE TABLE jobs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue            text      NOT NULL REFERENCES queues(name),
    kind             text      NOT NULL,
    payload          jsonb     NOT NULL DEFAULT '{}',
    priority         smallint  NOT NULL DEFAULT 2 CHECK (priority BETWEEN 0 AND 3),
    state            job_state NOT NULL DEFAULT 'available',
    run_at           timestamptz NOT NULL DEFAULT now(),
    attempts         int       NOT NULL DEFAULT 0,
    max_attempts     int       NOT NULL CHECK (max_attempts > 0),
    last_error       text,
    lease_token      uuid,
    lease_expires_at timestamptz,
    idempotency_key  text,
    trace_context    text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
) WITH (fillfactor = 70);

CREATE INDEX jobs_available ON jobs (queue, priority, run_at, id) WHERE state = 'available';
CREATE INDEX jobs_leased ON jobs (lease_expires_at) WHERE state = 'leased';
CREATE UNIQUE INDEX jobs_idempotency ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_finished ON jobs (finished_at) WHERE state IN ('completed', 'cancelled');
CREATE INDEX jobs_dead ON jobs (queue, finished_at) WHERE state = 'dead';
