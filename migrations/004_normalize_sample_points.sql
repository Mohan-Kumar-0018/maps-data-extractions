-- Migration 004: Normalize sample_points with junction table
--
-- Before: sample_points had (lat, lng, zoom, category, status, kml_file) — N×M rows
-- After:  sample_points stores pure geography (N rows),
--         category_sample_point_mappings stores the (category, point, status) work items (N×M rows)

-- 1. Drop old sample_points table (stale data from old schema)
DROP TABLE IF EXISTS sample_points CASCADE;

-- 2. Recreate sample_points — pure geography, no category/status
CREATE TABLE sample_points (
    id          BIGSERIAL PRIMARY KEY,
    lat         DOUBLE PRECISION NOT NULL,
    lng         DOUBLE PRECISION NOT NULL,
    zoom        INTEGER NOT NULL DEFAULT 16,
    kml_file    TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (lat, lng, zoom, kml_file)
);

-- 3. Junction table — the "work item" for extraction
CREATE TABLE category_sample_point_mappings (
    id              BIGSERIAL PRIMARY KEY,
    category_id     BIGINT NOT NULL REFERENCES categories(id),
    sample_point_id BIGINT NOT NULL REFERENCES sample_points(id),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'in_progress', 'done', 'failed')),
    total_results   INTEGER NOT NULL DEFAULT 0,
    new_count       INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    filtered_count  INTEGER NOT NULL DEFAULT 0,
    search_url      TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (category_id, sample_point_id)
);

CREATE INDEX idx_mappings_status ON category_sample_point_mappings (status);
CREATE INDEX idx_mappings_category ON category_sample_point_mappings (category_id);

-- 4. Add mapping_id FK to places_info
ALTER TABLE places_info
    ADD COLUMN IF NOT EXISTS mapping_id BIGINT REFERENCES category_sample_point_mappings(id);
