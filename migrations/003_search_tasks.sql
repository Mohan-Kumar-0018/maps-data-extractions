CREATE TABLE IF NOT EXISTS search_tasks (
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

CREATE INDEX IF NOT EXISTS idx_search_tasks_status ON search_tasks (status);
CREATE INDEX IF NOT EXISTS idx_search_tasks_category ON search_tasks (category_id);
