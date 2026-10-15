CREATE SCHEMA IF NOT EXISTS {{schema}};
CREATE TABLE IF NOT EXISTS {{schema}}.inbox_events (
  inbox_id uuid PRIMARY KEY,
  client_id text NOT NULL CHECK (octet_length(client_id) BETWEEN 1 AND 512),
  mailbox_ref text NOT NULL CHECK (octet_length(mailbox_ref) BETWEEN 1 AND 512),
  provider text NOT NULL CHECK (octet_length(provider) BETWEEN 1 AND 512),
  message_id text NOT NULL CHECK (octet_length(message_id) BETWEEN 1 AND 512),
  event_key text NOT NULL CHECK (event_key ~ '^[0-9a-f]{64}$'),
  payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
  normalized_event jsonb NOT NULL CHECK (jsonb_typeof(normalized_event) = 'object'),
  stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (client_id, mailbox_ref, provider, message_id),
  UNIQUE (client_id, mailbox_ref, event_key)
);
