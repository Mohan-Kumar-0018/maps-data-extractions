-- Migration 004: Normalize grid_points with junction table
--
-- Before: grid_points had (lat, lng, zoom, category, status, kml_file) — N×M rows
-- After:  grid_points stores pure geography (N rows),
--         search_tasks stores the (category, point, status) work items (N×M rows)

-- 1. Drop old grid_points table (stale data from old schema)
DROP TABLE IF EXISTS grid_points CASCADE;

-- 2. Recreate grid_points — pure geography, no category/status
CREATE TABLE grid_points (
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
CREATE TABLE search_tasks (
    id              BIGSERIAL PRIMARY KEY,
    category_id     BIGINT NOT NULL REFERENCES categories(id),
    grid_point_id   BIGINT NOT NULL REFERENCES grid_points(id),
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'in_progress', 'done', 'failed')),
    total_results   INTEGER NOT NULL DEFAULT 0,
    new_count       INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    filtered_count  INTEGER NOT NULL DEFAULT 0,
    search_url      TEXT NOT NULL DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (category_id, grid_point_id)
);

CREATE INDEX idx_search_tasks_status ON search_tasks (status);
CREATE INDEX idx_search_tasks_category ON search_tasks (category_id);

-- 4. Add search_task_id FK to listings
ALTER TABLE listings
    ADD COLUMN IF NOT EXISTS search_task_id BIGINT REFERENCES search_tasks(id);
