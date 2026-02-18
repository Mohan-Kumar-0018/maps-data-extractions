"""PostgreSQL storage for scraped business data."""

import logging
import os
from pathlib import Path

import psycopg2
import yaml

from typing import List, Tuple

from scraper.models import Business

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.yml"

_UPSERT_SQL = """
INSERT INTO places_info
    (name, rating, total_reviews, address, phone, website,
     opening_hours, latitude, longitude, google_maps_url, place_id, category, mapping_id)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

    def insert_business(self, biz: Business, mapping_id: int | None = None) -> None:
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
                mapping_id,
            ))

    # ── sample_points table ──────────────────────────────────────────

    def insert_sample_points(
        self,
        points: List[Tuple[float, float]],
        zoom: int,
        kml_file: str,
    ) -> List[int]:
        """Bulk-insert geographic sample points (ON CONFLICT DO NOTHING).

        Returns list of all point IDs matching the input coordinates
        (both newly inserted and already existing).
        """
        ids = []
        sql = """
            INSERT INTO sample_points (lat, lng, zoom, kml_file)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (lat, lng, zoom, kml_file) DO NOTHING
        """
        select_sql = """
            SELECT id FROM sample_points
            WHERE lat = %s AND lng = %s AND zoom = %s AND kml_file = %s
        """
        with self._conn.cursor() as cur:
            for lat, lng in points:
                cur.execute(sql, (lat, lng, zoom, kml_file))
                cur.execute(select_sql, (lat, lng, zoom, kml_file))
                row = cur.fetchone()
                if row:
                    ids.append(row[0])
        return ids

    # ── category_sample_point_mappings table ─────────────────────────

    def create_mappings(self, category_id: int, sample_point_ids: List[int]) -> int:
        """Bulk-insert mappings for a category and list of sample point IDs.

        Uses ON CONFLICT DO NOTHING to skip already-existing mappings.
        Returns number of new rows inserted.
        """
        sql = """
            INSERT INTO category_sample_point_mappings (category_id, sample_point_id)
            VALUES (%s, %s)
            ON CONFLICT (category_id, sample_point_id) DO NOTHING
        """
        inserted = 0
        with self._conn.cursor() as cur:
            for sp_id in sample_point_ids:
                cur.execute(sql, (category_id, sp_id))
                inserted += cur.rowcount
        return inserted

    def fetch_pending_mappings(self, category: str | None = None) -> List[dict]:
        """Return all pending mappings, joined with sample_points and categories.

        Each dict has: mapping_id, lat, lng, zoom, category.
        """
        if category:
            sql = """
                SELECT m.id, sp.lat, sp.lng, sp.zoom, c.name
                FROM category_sample_point_mappings m
                JOIN sample_points sp ON sp.id = m.sample_point_id
                JOIN categories c ON c.id = m.category_id
                WHERE c.name = %s AND m.status = 'pending'
                ORDER BY m.id
            """
            params: tuple = (category,)
        else:
            sql = """
                SELECT m.id, sp.lat, sp.lng, sp.zoom, c.name
                FROM category_sample_point_mappings m
                JOIN sample_points sp ON sp.id = m.sample_point_id
                JOIN categories c ON c.id = m.category_id
                WHERE m.status = 'pending'
                ORDER BY m.id
            """
            params = ()

        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return [
                {
                    "mapping_id": row[0],
                    "lat": row[1],
                    "lng": row[2],
                    "zoom": row[3],
                    "category": row[4],
                }
                for row in cur.fetchall()
            ]

    def claim_mapping(self, mapping_id: int) -> bool:
        """Atomically set a pending mapping to in_progress. Returns True if claimed."""
        sql = """
            UPDATE category_sample_point_mappings
            SET status = 'in_progress', updated_at = NOW()
            WHERE id = %s AND status = 'pending'
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (mapping_id,))
            return cur.rowcount == 1

    def mark_mapping_done(self, mapping_id: int, total_results: int = 0) -> None:
        sql = """
            UPDATE category_sample_point_mappings
            SET status = 'done', total_results = %s, updated_at = NOW()
            WHERE id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (total_results, mapping_id))

    def mark_mapping_failed(self, mapping_id: int) -> None:
        sql = """
            UPDATE category_sample_point_mappings
            SET status = 'failed', updated_at = NOW()
            WHERE id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (mapping_id,))

    def reset_in_progress_mappings(self, category: str | None = None) -> int:
        """Reset any in_progress mappings back to pending (cleanup from interrupted runs).

        If category is None, resets across all categories.
        """
        if category:
            sql = """
                UPDATE category_sample_point_mappings m
                SET status = 'pending', updated_at = NOW()
                FROM categories c
                WHERE m.category_id = c.id AND c.name = %s AND m.status = 'in_progress'
            """
            with self._conn.cursor() as cur:
                cur.execute(sql, (category,))
                return cur.rowcount
        else:
            sql = """
                UPDATE category_sample_point_mappings
                SET status = 'pending', updated_at = NOW()
                WHERE status = 'in_progress'
            """
            with self._conn.cursor() as cur:
                cur.execute(sql)
                return cur.rowcount

    # ── categories table ─────────────────────────────────────────────

    def insert_category(self, name: str) -> None:
        """Insert a category (ignores duplicates)."""
        sql = """
            INSERT INTO categories (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (name,))

    def get_or_create_category(self, name: str) -> int:
        """Insert a category if it doesn't exist, return its ID."""
        sql = """
            INSERT INTO categories (name)
            VALUES (%s)
            ON CONFLICT (name) DO NOTHING
        """
        select_sql = "SELECT id FROM categories WHERE name = %s"
        with self._conn.cursor() as cur:
            cur.execute(sql, (name,))
            cur.execute(select_sql, (name,))
            return cur.fetchone()[0]

    def list_categories(self) -> List[dict]:
        """Return all categories."""
        sql = "SELECT id, name, created_at FROM categories ORDER BY id"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return [
                {"id": row[0], "name": row[1], "created_at": row[2]}
                for row in cur.fetchall()
            ]

    def close(self) -> None:
        self._conn.close()
        logger.info("PostgreSQL connection closed")
