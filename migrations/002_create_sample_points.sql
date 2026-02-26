CREATE TABLE IF NOT EXISTS grid_points (
    id          BIGSERIAL PRIMARY KEY,
    lat         DOUBLE PRECISION NOT NULL,
    lng         DOUBLE PRECISION NOT NULL,
    zoom        INTEGER NOT NULL DEFAULT 16,
    category    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'in_progress', 'done', 'failed')),
    kml_file    TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_grid_points_status ON grid_points (status);
CREATE INDEX IF NOT EXISTS idx_grid_points_category ON grid_points (category);
