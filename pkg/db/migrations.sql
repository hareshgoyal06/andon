CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS events (
    ts              TIMESTAMPTZ NOT NULL,
    station_id      TEXT        NOT NULL,
    type            TEXT        NOT NULL,
    state           TEXT,
    prev_state      TEXT,
    cycle_ms        INT,
    target_cycle_ms INT,
    part_count      INT,
    scrap_count     INT,
    temp_c          DOUBLE PRECISION,
    fault_code      INT,
    error           TEXT,
    PRIMARY KEY (station_id, ts, type)
);
SELECT create_hypertable('events', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS station_state (
    station_id      TEXT PRIMARY KEY,
    state           TEXT NOT NULL,
    online          BOOLEAN NOT NULL DEFAULT TRUE,
    cycle_ms        INT,
    target_cycle_ms INT,
    part_count      INT,
    scrap_count     INT,
    temp_c          DOUBLE PRECISION,
    fault_code      INT,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS alerts (
    id          TEXT PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    station_id  TEXT NOT NULL,
    severity    TEXT NOT NULL,
    kind        TEXT NOT NULL,
    message     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS alerts_ts_idx ON alerts (ts DESC);

CREATE TABLE IF NOT EXISTS oee_snapshots (
    ts            TIMESTAMPTZ NOT NULL,
    station_id    TEXT NOT NULL,
    availability  DOUBLE PRECISION NOT NULL,
    performance   DOUBLE PRECISION NOT NULL,
    quality       DOUBLE PRECISION NOT NULL,
    oee           DOUBLE PRECISION NOT NULL,
    avg_cycle_ms  DOUBLE PRECISION,
    parts         INT,
    scrap         INT,
    faults        INT,
    is_bottleneck BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (station_id, ts)
);
SELECT create_hypertable('oee_snapshots', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS commands (
    id          TEXT PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    station_id  TEXT NOT NULL,
    type        TEXT NOT NULL,
    value       INT,
    actor       TEXT,
    status      TEXT NOT NULL DEFAULT 'queued',
    error       TEXT,
    applied_at  TIMESTAMPTZ
);
