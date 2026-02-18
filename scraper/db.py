"""PostgreSQL storage for scraped business data."""

import logging
import os
from pathlib import Path

import psycopg2
import yaml

from scraper.models import Business

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yml"

_UPSERT_SQL = """
INSERT INTO places_info
    (name, rating, total_reviews, address, phone, website,
     opening_hours, latitude, longitude, google_maps_url, place_id, category)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (place_id) DO UPDATE SET
    duplicate_count = places_info.duplicate_count + 1,
    updated_at = NOW();
"""


def _load_config() -> dict | None:
    """Load database config from config.yml if it exists."""
    if _CONFIG_PATH.is_file():
        with open(_CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        return cfg.get("database") if cfg else None
    return None


def _get_connection_params() -> dict:
    """Build psycopg2 connection params.

    Priority: DATABASE_URL env var > config.yml > individual DB_* env vars.
    """
    url = os.environ.get("DATABASE_URL")
    if url:
        return {"dsn": url}

    cfg = _load_config()
    if cfg:
        return {
            "host": cfg.get("host", "localhost"),
            "port": int(cfg.get("port", 5432)),
            "dbname": cfg.get("name", "maps"),
            "user": cfg.get("user", "postgres"),
            "password": cfg.get("password", ""),
        }

    return {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "maps"),
        "user": os.environ.get("DB_USER", "postgres"),
        "password": os.environ.get("DB_PASSWORD", ""),
    }


class PlacesDB:
    """Thin wrapper around a psycopg2 connection for the places_info table."""

    def __init__(self) -> None:
        params = _get_connection_params()
        self._conn = psycopg2.connect(**params)
        self._conn.autocommit = True
        logger.info("Connected to PostgreSQL")

    def insert_business(self, biz: Business) -> None:
        if not biz.place_id:
            return
        with self._conn.cursor() as cur:
            cur.execute(_UPSERT_SQL, (
                biz.name,
                biz.rating,
                biz.total_reviews,
                biz.address,
                biz.phone,
                biz.website,
                biz.opening_hours,
                biz.latitude,
                biz.longitude,
                biz.google_maps_url,
                biz.place_id,
                biz.category,
            ))

    def close(self) -> None:
        self._conn.close()
        logger.info("PostgreSQL connection closed")
