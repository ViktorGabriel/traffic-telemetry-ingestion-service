-- 001_init_telemetry_partitions.sql
-- High-throughput partitioned telemetry storage for traffic monitoring

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Telemetry Events Table Partitioned by Range of recorded_at (Monthly or Daily)
CREATE TABLE IF NOT EXISTS telemetry_events (
    id UUID DEFAULT gen_random_uuid(),
    device_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    speed_kmh NUMERIC(5, 2) NOT NULL,
    latitude NUMERIC(10, 7),
    longitude NUMERIC(10, 7),
    heading_deg NUMERIC(5, 2),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (recorded_at, id)
) PARTITION BY RANGE (recorded_at);

-- Partitions for November & December 2026 (Extendable by cron / pg_partman)
CREATE TABLE IF NOT EXISTS telemetry_events_2026_11 PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

CREATE TABLE IF NOT EXISTS telemetry_events_2026_12 PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');

-- Default partition to safely capture any outlier timestamps
CREATE TABLE IF NOT EXISTS telemetry_events_default PARTITION OF telemetry_events DEFAULT;

-- Time-Series Query Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_telemetry_device_time 
    ON telemetry_events (device_id, recorded_at DESC);

-- BRIN (Block Range Index) for massive compaction and fast time-series scans
CREATE INDEX IF NOT EXISTS idx_telemetry_recorded_brin 
    ON telemetry_events USING BRIN (recorded_at);

-- Incidents Table for Operation Center Tracking
CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY,
    device_id VARCHAR(64) NOT NULL,
    incident_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    detected_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_incidents_device_detected 
    ON incidents (device_id, detected_at DESC);
