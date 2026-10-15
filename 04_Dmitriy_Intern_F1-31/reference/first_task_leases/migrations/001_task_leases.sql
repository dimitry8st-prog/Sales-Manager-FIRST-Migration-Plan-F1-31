-- F1-16 task leases. One row per task_key; the row is never deleted, so the
-- generation counter only grows. A lease is valid while the PostgreSQL
-- clock_timestamp() of the deciding transaction is strictly before expires_at.
CREATE SCHEMA IF NOT EXISTS {{schema}};
CREATE TABLE IF NOT EXISTS {{schema}}.task_leases (
  task_key text PRIMARY KEY CHECK (char_length(task_key) BETWEEN 1 AND 128),
  owner_token uuid NOT NULL,
  generation bigint NOT NULL CHECK (generation >= 1),
  acquired_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL
);
