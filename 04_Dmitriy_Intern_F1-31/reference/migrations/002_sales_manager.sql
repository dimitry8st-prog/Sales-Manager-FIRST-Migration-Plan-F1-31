ALTER TABLE runtime_next.tasks
    ADD COLUMN IF NOT EXISTS dedupe_key TEXT,
    ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS result JSONB,
    ADD COLUMN IF NOT EXISTS priority SMALLINT NOT NULL DEFAULT 0;

UPDATE runtime_next.tasks SET available_at=COALESCE(available_at, created_at, NOW());
ALTER TABLE runtime_next.tasks ALTER COLUMN available_at SET DEFAULT NOW();
ALTER TABLE runtime_next.tasks ALTER COLUMN available_at SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS tasks_dedupe_key_uidx
    ON runtime_next.tasks(dedupe_key)
    WHERE dedupe_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS tasks_claim_idx
    ON runtime_next.tasks(status, available_at, priority DESC, created_at);

CREATE TABLE IF NOT EXISTS runtime_next.sales_control (
    id SMALLINT PRIMARY KEY CHECK (id=1),
    emergency_stop BOOLEAN NOT NULL DEFAULT FALSE,
    daily_send_limit INTEGER NOT NULL DEFAULT 20 CHECK (daily_send_limit BETWEEN 1 AND 1000),
    sent_today INTEGER NOT NULL DEFAULT 0 CHECK (sent_today >= 0),
    counter_date DATE NOT NULL DEFAULT CURRENT_DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO runtime_next.sales_control(id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS runtime_next.sales_campaigns (
    campaign_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    direction TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'RUNNING'
        CHECK (status IN ('RUNNING', 'PAUSED', 'EXHAUSTED')),
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS runtime_next.sales_leads (
    lead_id UUID PRIMARY KEY,
    campaign_id UUID NOT NULL REFERENCES runtime_next.sales_campaigns(campaign_id),
    company_name TEXT NOT NULL,
    domain TEXT,
    contact_name TEXT,
    contact_role TEXT,
    email TEXT,
    phone TEXT,
    source_url TEXT NOT NULL,
    source_observed_at TIMESTAMPTZ NOT NULL,
    aliases JSONB NOT NULL DEFAULT '[]'::jsonb,
    stage TEXT NOT NULL CHECK (stage IN (
        'DISCOVERED', 'DUPLICATE_CLEARED', 'LIVE_VERIFIED', 'GOLD_AUDITED',
        'MATERIALS_READY', 'SEND_QUEUED', 'SENT', 'SENT_VERIFIED',
        'RESPONSE_RECEIVED', 'REJECTED'
    )),
    live_evidence JSONB,
    gold_audit JSONB,
    materials JSONB,
    policy_decision JSONB,
    send_receipt JSONB,
    reply JSONB,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    rejection_reason TEXT,
    send_reserved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (email IS NOT NULL OR phone IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS sales_leads_campaign_stage_idx
    ON runtime_next.sales_leads(campaign_id, stage, created_at);
CREATE INDEX IF NOT EXISTS sales_leads_email_idx
    ON runtime_next.sales_leads((lower(email)))
    WHERE email IS NOT NULL;

CREATE TABLE IF NOT EXISTS runtime_next.sales_processed_registry (
    registry_id BIGSERIAL PRIMARY KEY,
    fingerprint_type TEXT NOT NULL,
    fingerprint_value TEXT NOT NULL,
    lead_id UUID NOT NULL REFERENCES runtime_next.sales_leads(lead_id),
    status TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (fingerprint_type, fingerprint_value)
);

CREATE INDEX IF NOT EXISTS sales_registry_lead_idx
    ON runtime_next.sales_processed_registry(lead_id);

CREATE TABLE IF NOT EXISTS runtime_next.sales_events (
    event_id BIGSERIAL PRIMARY KEY,
    lead_id UUID NOT NULL REFERENCES runtime_next.sales_leads(lead_id),
    event_type TEXT NOT NULL,
    from_stage TEXT,
    to_stage TEXT,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS sales_events_lead_idx
    ON runtime_next.sales_events(lead_id, event_id DESC);
