-- ============================================================================
-- FS3 ETL Pipeline — Mission Control Database Schema
-- ============================================================================
-- Run this file ONCE on the PostgreSQL server to set up all tables required
-- by both the ETL pipeline and the Mission Control Dashboard.
--
-- Tables created here:
--   1. fs3.etl_pipeline_log   — Per-file pipeline state (PENDING → COMPLETED/FAILED)
--   2. fs3.etl_audit_log      — Human-readable event log (INFO / WARNING / CRITICAL)
--   3. fs3.etl_dlq            — Dead Letter Queue for failed files (with retry support)
--   4. fs3.etl_machine_registry — Machine heartbeat and health tracking
--
-- Usage:
--   psql -h YOUR_HOST -U YOUR_USER -d YOUR_DB -f schema_mission_control.sql
-- ============================================================================


-- ── 1. PIPELINE STATE TABLE ──────────────────────────────────────────────────
-- Tracks every file from detection to final outcome.
-- This is the source of truth for the Live File Queue pillar in the dashboard.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fs3.etl_pipeline_log (
    id              BIGSERIAL       PRIMARY KEY,
    file_name       VARCHAR(500)    NOT NULL,
    machine_id      VARCHAR(50),
    vehicle_model   VARCHAR(10),
    parts_code      VARCHAR(20),
    op_no           VARCHAR(30),
    file_size       BIGINT,

    -- Status transitions: PENDING → PROCESSING → COMPLETED or FAILED
    status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING'
                                    CHECK (status IN ('PENDING','PROCESSING','COMPLETED','FAILED')),

    -- Tracks which step is currently active (COPY, TRANSFORM, LOAD, FINALIZE)
    current_step    VARCHAR(100),

    -- Full error detail stored here on failure — shown in DLQ "Reason" button
    error_message   TEXT,

    -- Timing columns for performance analytics (avg processing time KPI)
    started_at      TIMESTAMPTZ     DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,

    -- Processing time in seconds (computed after completion for fast KPI queries)
    processing_sec  NUMERIC(10,2),

    -- PID of the worker process that handled this file (helps debug zombie locks)
    pid             INTEGER,

    -- Row version for optimistic lock detection
    row_version     INTEGER         DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_pipeline_status
    ON fs3.etl_pipeline_log(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_filename
    ON fs3.etl_pipeline_log(file_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_model_op
    ON fs3.etl_pipeline_log(vehicle_model, op_no);
CREATE INDEX IF NOT EXISTS idx_pipeline_started
    ON fs3.etl_pipeline_log(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_machine
    ON fs3.etl_pipeline_log(machine_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_updated
    ON fs3.etl_pipeline_log(updated_at);

-- ── CRITICAL: Partial Unique Index for Atomic File Claim ─────────────────────
-- Required by StatusManager.register() which uses:
--   INSERT ... ON CONFLICT (file_name) WHERE (status IN ('PENDING','PROCESSING')) DO NOTHING
--
-- This makes the "claim" operation fully ATOMIC at the database level.
-- Two threads racing to claim the same file: only ONE gets a log_id back.
-- The other gets NULL and skips immediately — no TOCTOU race, no duplicates.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_active_file
    ON fs3.etl_pipeline_log(file_name)
    WHERE (status IN ('PENDING', 'PROCESSING'));


COMMENT ON TABLE fs3.etl_pipeline_log IS
    'Per-file pipeline execution state. Primary source for the Live File Queue and KPI panels in the Mission Control Dashboard.';


-- ── 2. AUDIT LOG TABLE ───────────────────────────────────────────────────────
-- Stores every significant event emitted by the pipeline.
-- This is the data source for the "Audit Logs" pillar in the dashboard.
-- Intentionally separate from etl_pipeline_log so we can log system-level
-- events (startup, shutdown, config changes) that are not file-specific.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fs3.etl_audit_log (
    id              BIGSERIAL       PRIMARY KEY,

    -- Severity level matching Python logging levels
    level           VARCHAR(10)     NOT NULL DEFAULT 'INFO'
                                    CHECK (level IN ('INFO','WARNING','ERROR','CRITICAL')),

    -- Optional link back to the pipeline run that generated this log entry
    pipeline_log_id BIGINT          REFERENCES fs3.etl_pipeline_log(id) ON DELETE SET NULL,

    -- Context fields (duplicated here for fast dashboard filtering without joins)
    machine_id      VARCHAR(50),
    vehicle_model   VARCHAR(10),
    parts_code      VARCHAR(20),
    op_no           VARCHAR(30),
    current_step    VARCHAR(100),

    -- The actual log message
    message         TEXT            NOT NULL,

    -- Full Python traceback stored here for CRITICAL/ERROR entries
    traceback       TEXT,

    logged_at       TIMESTAMPTZ     DEFAULT NOW(),
    pid             INTEGER
);

CREATE INDEX IF NOT EXISTS idx_audit_level
    ON fs3.etl_audit_log(level);
CREATE INDEX IF NOT EXISTS idx_audit_logged_at
    ON fs3.etl_audit_log(logged_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_model
    ON fs3.etl_audit_log(vehicle_model, op_no);
CREATE INDEX IF NOT EXISTS idx_audit_machine
    ON fs3.etl_audit_log(machine_id);
CREATE INDEX IF NOT EXISTS idx_audit_pipeline_id
    ON fs3.etl_audit_log(pipeline_log_id);

COMMENT ON TABLE fs3.etl_audit_log IS
    'Structured event log for all pipeline activity. Searchable by vehicle model, operation, level, and machine in the Mission Control Dashboard.';


-- ── 3. DEAD LETTER QUEUE TABLE ───────────────────────────────────────────────
-- Stores files that have failed processing and are awaiting manual review or retry.
-- When the "Retry" button is clicked in the dashboard, the row is deleted from here
-- and the file_path is re-queued for processing by the Job Runner.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fs3.etl_dlq (
    id              BIGSERIAL       PRIMARY KEY,

    -- Link back to the pipeline run that failed
    pipeline_log_id BIGINT          REFERENCES fs3.etl_pipeline_log(id) ON DELETE CASCADE,

    file_name       VARCHAR(500)    NOT NULL,
    file_path       TEXT            NOT NULL,   -- Full path in _NG folder
    machine_id      VARCHAR(50),
    vehicle_model   VARCHAR(10),
    parts_code      VARCHAR(20),
    op_no           VARCHAR(30),
    failed_step     VARCHAR(100),

    -- Categorized error type for the DLQ filter in the dashboard
    error_category  VARCHAR(50)     DEFAULT 'UNKNOWN'
                                    CHECK (error_category IN (
                                        'SCHEMA_DRIFT',
                                        'DB_TIMEOUT',
                                        'VALIDATION_FAILED',
                                        'FILE_NOT_FOUND',
                                        'PARSE_ERROR',
                                        'UNKNOWN'
                                    )),

    -- Full error message and traceback shown in the "Reason" button popup
    error_message   TEXT,
    traceback       TEXT,

    -- Retry tracking — limits infinite retry loops
    retry_count     INTEGER         DEFAULT 0,
    max_retries     INTEGER         DEFAULT 3,
    last_retry_at   TIMESTAMPTZ,

    -- DLQ entry lifecycle
    queued_at       TIMESTAMPTZ     DEFAULT NOW(),

    -- When retried, status changes to RETRYING; on success it's removed from DLQ
    dlq_status      VARCHAR(20)     DEFAULT 'AWAITING_REVIEW'
                                    CHECK (dlq_status IN ('AWAITING_REVIEW','RETRYING','RESOLVED'))
);

CREATE INDEX IF NOT EXISTS idx_dlq_status
    ON fs3.etl_dlq(dlq_status);
CREATE INDEX IF NOT EXISTS idx_dlq_category
    ON fs3.etl_dlq(error_category);
CREATE INDEX IF NOT EXISTS idx_dlq_machine
    ON fs3.etl_dlq(machine_id);

COMMENT ON TABLE fs3.etl_dlq IS
    'Dead Letter Queue: holds files that failed processing. The Mission Control Dashboard Retry button updates dlq_status to RETRYING and triggers re-processing.';


-- ── 4. MACHINE REGISTRY TABLE ────────────────────────────────────────────────
-- Tracks the health and last-seen timestamp of every configured machine.
-- The Job Runner updates this on every successful file detection.
-- The dashboard shows ONLINE / IDLE / WARNING indicators from this table.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fs3.etl_machine_registry (
    machine_id          VARCHAR(50)     PRIMARY KEY,
    machine_description VARCHAR(500),
    pipeline_type       VARCHAR(20),
    source_path         TEXT,

    -- Updated every time the watcher successfully scans the folder
    last_seen_at        TIMESTAMPTZ,

    -- Updated when a file is successfully committed
    last_file_at        TIMESTAMPTZ,

    -- Total cumulative counts (for the dashboard machine detail view)
    total_processed     BIGINT          DEFAULT 0,
    total_failed        BIGINT          DEFAULT 0,

    -- Health flags set by the Job Runner
    schema_drift_flag   BOOLEAN         DEFAULT FALSE,
    is_enabled          BOOLEAN         DEFAULT TRUE,

    -- When the watcher thread started for this machine
    watcher_started_at  TIMESTAMPTZ,

    registered_at       TIMESTAMPTZ     DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     DEFAULT NOW()
);

COMMENT ON TABLE fs3.etl_machine_registry IS
    'Machine health registry. ONLINE = last_seen_at within 10 minutes. IDLE = last_seen_at older than 10 minutes. WARNING = schema_drift_flag = TRUE or total_failed > threshold.';


-- ── HELPFUL VIEWS ────────────────────────────────────────────────────────────
-- Pre-built views the dashboard uses for fast reads without complex SQL in Python.
-- ─────────────────────────────────────────────────────────────────────────────

-- Overview KPIs view
CREATE OR REPLACE VIEW fs3.v_etl_today_kpis AS
SELECT
    COUNT(*) FILTER (WHERE status = 'COMPLETED')                        AS total_processed_today,
    COUNT(*) FILTER (WHERE status = 'FAILED')                           AS total_failed_today,
    COUNT(*) FILTER (WHERE status IN ('PENDING','PROCESSING'))           AS queue_depth,
    ROUND(AVG(processing_sec) FILTER (WHERE status = 'COMPLETED'), 2)   AS avg_processing_sec,
    (SELECT COUNT(*) FROM fs3.etl_dlq WHERE dlq_status = 'AWAITING_REVIEW') AS dlq_depth
FROM fs3.etl_pipeline_log
WHERE started_at >= CURRENT_DATE;

-- 12-hour throughput view (one row per hour)
CREATE OR REPLACE VIEW fs3.v_etl_hourly_throughput AS
SELECT
    DATE_TRUNC('hour', completed_at)    AS hour_bucket,
    COUNT(*)                            AS files_completed,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS files_failed
FROM fs3.etl_pipeline_log
WHERE completed_at >= NOW() - INTERVAL '12 hours'
GROUP BY 1
ORDER BY 1;

-- Machine health view
CREATE OR REPLACE VIEW fs3.v_machine_health AS
SELECT
    machine_id,
    machine_description,
    pipeline_type,
    last_seen_at,
    last_file_at,
    total_processed,
    total_failed,
    schema_drift_flag,
    is_enabled,
    CASE
        WHEN last_seen_at >= NOW() - INTERVAL '10 minutes' THEN 'ONLINE'
        WHEN last_seen_at >= NOW() - INTERVAL '60 minutes' THEN 'IDLE'
        ELSE 'WARNING'
    END AS health_status
FROM fs3.etl_machine_registry;


-- ── IDEMPOTENT MIGRATION (run-once for existing databases) ───────────────────
-- If you already ran an older version of this schema file, run the block below
-- to upgrade your existing etl_pipeline_log table with the new file_size column
-- and the atomic claim index. Safe to re-run — uses IF NOT EXISTS / IF EXISTS.
-- ─────────────────────────────────────────────────────────────────────────────
DO $$
BEGIN
    -- Add file_size column if not already present
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'fs3'
          AND table_name   = 'etl_pipeline_log'
          AND column_name  = 'file_size'
    ) THEN
        ALTER TABLE fs3.etl_pipeline_log ADD COLUMN file_size BIGINT;
        RAISE NOTICE 'MIGRATION: file_size column added to fs3.etl_pipeline_log.';
    ELSE
        RAISE NOTICE 'MIGRATION: file_size column already exists — skipped.';
    END IF;
END;
$$;

-- Add the updated_at index (for the 30-min stale-PROCESSING timeout query)
CREATE INDEX IF NOT EXISTS idx_pipeline_updated
    ON fs3.etl_pipeline_log(updated_at);

-- Add the partial unique index (for atomic ON CONFLICT file claim)
-- NOTE: If this index already exists without the WHERE clause, drop and recreate it.
CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_active_file
    ON fs3.etl_pipeline_log(file_name)
    WHERE (status IN ('PENDING', 'PROCESSING'));

-- Add the DLQ pipeline_log_id column and retry tracking columns if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'fs3'
          AND table_name   = 'etl_dlq'
          AND column_name  = 'pipeline_log_id'
    ) THEN
        ALTER TABLE fs3.etl_dlq ADD COLUMN pipeline_log_id BIGINT
            REFERENCES fs3.etl_pipeline_log(id) ON DELETE SET NULL;
        RAISE NOTICE 'MIGRATION: pipeline_log_id column added to fs3.etl_dlq.';
    ELSE
        RAISE NOTICE 'MIGRATION: pipeline_log_id column already exists — skipped.';
    END IF;
END;
$$;

