-- Автоматически создаёт таблицы для SQLBackend
CREATE TABLE IF NOT EXISTS jobkit_jobs (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    attempts INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_jobkit_pending ON jobkit_jobs(status, lease_expires_at)
WHERE status = 'pending' AND (lease_expires_at IS NULL OR lease_expires_at < now());
