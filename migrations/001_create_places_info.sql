CREATE TABLE IF NOT EXISTS places_info (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL DEFAULT '',
    rating          DOUBLE PRECISION,
    total_reviews   INTEGER,
    address         TEXT NOT NULL DEFAULT '',
    phone           TEXT NOT NULL DEFAULT '',
    website         TEXT NOT NULL DEFAULT '',
    opening_hours   TEXT NOT NULL DEFAULT '',
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    google_maps_url TEXT NOT NULL DEFAULT '',
    place_id        TEXT NOT NULL UNIQUE,
    category        TEXT NOT NULL DEFAULT '',
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    info_status     TEXT NOT NULL DEFAULT 'pending',
    website_email   TEXT NOT NULL DEFAULT '',
    website_phone   TEXT NOT NULL DEFAULT '',
    social_media    TEXT NOT NULL DEFAULT '',
    contact_status  TEXT NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_places_info_category ON places_info (category);
CREATE INDEX IF NOT EXISTS idx_places_info_coords ON places_info (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_places_info_status ON places_info (info_status);
CREATE INDEX IF NOT EXISTS idx_places_contact_status ON places_info (contact_status);
