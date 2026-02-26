CREATE TABLE IF NOT EXISTS listings (
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

CREATE INDEX IF NOT EXISTS idx_listings_category ON listings (category);
CREATE INDEX IF NOT EXISTS idx_listings_coords ON listings (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings (info_status);
CREATE INDEX IF NOT EXISTS idx_listings_contact_status ON listings (contact_status);
