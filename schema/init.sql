CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS benchmark_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow TEXT NOT NULL CHECK (workflow IN ('approval', 'enrichment', 'synchronization')),
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'completed', 'completed_with_errors', 'failed')),
    total_items INTEGER NOT NULL CHECK (total_items > 0),
    completed_items INTEGER NOT NULL DEFAULT 0 CHECK (completed_items >= 0),
    failed_items INTEGER NOT NULL DEFAULT 0 CHECK (failed_items >= 0),
    external_latency_ms INTEGER NOT NULL DEFAULT 0 CHECK (external_latency_ms BETWEEN 0 AND 200),
    failure_every INTEGER NOT NULL DEFAULT 0 CHECK (failure_every >= 0),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS benchmark_products (
    id BIGSERIAL PRIMARY KEY,
    external_id TEXT NOT NULL UNIQUE,
    sku TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    category TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    approval_status TEXT NOT NULL DEFAULT 'pending' CHECK (approval_status IN ('pending', 'approved', 'rejected')),
    enrichment JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS benchmark_job_items (
    job_id UUID NOT NULL REFERENCES benchmark_jobs(id) ON DELETE CASCADE,
    item_index INTEGER NOT NULL CHECK (item_index > 0),
    external_id TEXT NOT NULL,
    product_id BIGINT REFERENCES benchmark_products(id) ON DELETE SET NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    source_payload JSONB,
    result JSONB,
    error_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, item_index)
);

CREATE TABLE IF NOT EXISTS benchmark_outbox (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL UNIQUE REFERENCES benchmark_jobs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL DEFAULT 'benchmark.job.created',
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    dispatched_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_benchmark_job_items_pending
    ON benchmark_job_items (job_id, item_index) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_benchmark_outbox_pending
    ON benchmark_outbox (id) WHERE dispatched_at IS NULL;
