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
    updated_at = NOW()
RETURNING (xmax = 0) AS is_new;
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

    def insert_business(self, biz: Business, mapping_id: int | None = None) -> bool:
        """Insert or update a business. Returns True if new, False if duplicate."""
        if not biz.place_id:
            return False
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
            row = cur.fetchone()
            return bool(row and row[0])

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

    def insert_subdivision_points(
        self,
        sub_points: List[Tuple[float, float]],
        zoom: int,
        kml_file: str,
        category_id: int,
    ) -> int:
        """Insert sub-points and create mappings for a single category.

        Returns number of new mappings created.
        """
        point_ids = self.insert_sample_points(sub_points, zoom, kml_file)
        return self.create_mappings(category_id, point_ids)

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

    def mark_mapping_done(
        self,
        mapping_id: int,
        total_results: int = 0,
        new_count: int = 0,
        duplicate_count: int = 0,
        filtered_count: int = 0,
        search_url: str = "",
    ) -> None:
        sql = """
            UPDATE category_sample_point_mappings
            SET status = 'done', total_results = %s,
                new_count = %s, duplicate_count = %s, filtered_count = %s,
                search_url = %s, updated_at = NOW()
            WHERE id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (total_results, new_count, duplicate_count, filtered_count, search_url, mapping_id))

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

    # ── enrichment (places_info.info_status) ────────────────────────

    def fetch_pending_enrichments(self, limit: int | None = None) -> List[dict]:
        """Return places_info rows needing enrichment."""
        sql = "SELECT id, google_maps_url FROM places_info WHERE info_status = 'pending' ORDER BY id"
        if limit:
            sql += f" LIMIT {int(limit)}"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return [{"id": row[0], "google_maps_url": row[1]} for row in cur.fetchall()]

    def claim_enrichment(self, row_id: int) -> bool:
        """Atomically set a pending enrichment to in_progress. Returns True if claimed."""
        sql = """
            UPDATE places_info
            SET info_status = 'in_progress', updated_at = NOW()
            WHERE id = %s AND info_status = 'pending'
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (row_id,))
            return cur.rowcount == 1

    def update_enrichment(self, row_id: int, total_reviews: int | None, phone: str, website: str, address: str = "") -> None:
        """Write enriched fields and mark done."""
        sql = """
            UPDATE places_info
            SET total_reviews = COALESCE(%s, total_reviews),
                phone = CASE WHEN %s = '' THEN phone ELSE %s END,
                website = CASE WHEN %s = '' THEN website ELSE %s END,
                address = CASE WHEN %s = '' THEN address ELSE %s END,
                info_status = 'done',
                updated_at = NOW()
            WHERE id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (total_reviews, phone, phone, website, website, address, address, row_id))

    def mark_enrichment_failed(self, row_id: int) -> None:
        """Mark an enrichment as failed."""
        sql = """
            UPDATE places_info
            SET info_status = 'failed', updated_at = NOW()
            WHERE id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (row_id,))

    def reset_in_progress_enrichments(self) -> int:
        """Reset stale in_progress enrichments back to pending."""
        sql = """
            UPDATE places_info
            SET info_status = 'pending', updated_at = NOW()
            WHERE info_status = 'in_progress'
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount

    # ── contact extraction (places_info.contact_status) ──────────────

    def fetch_pending_contacts(self, limit: int | None = None) -> List[dict]:
        """Return places_info rows needing contact extraction (must have a website)."""
        sql = "SELECT id, website FROM places_info WHERE contact_status = 'pending' AND website != '' ORDER BY id"
        if limit:
            sql += f" LIMIT {int(limit)}"
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return [{"id": row[0], "website": row[1]} for row in cur.fetchall()]

    def claim_contact(self, row_id: int) -> bool:
        """Atomically set a pending contact to in_progress. Returns True if claimed."""
        sql = """
            UPDATE places_info
            SET contact_status = 'in_progress', updated_at = NOW()
            WHERE id = %s AND contact_status = 'pending'
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (row_id,))
            return cur.rowcount == 1

    def update_contact(self, row_id: int, emails: str, phones: str, social_media: str) -> None:
        """Write extracted contact fields and mark done."""
        sql = """
            UPDATE places_info
            SET website_email = %s,
                website_phone = %s,
                social_media = %s,
                contact_status = 'done',
                updated_at = NOW()
            WHERE id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (emails, phones, social_media, row_id))

    def mark_contact_failed(self, row_id: int) -> None:
        """Mark a contact extraction as failed."""
        sql = """
            UPDATE places_info
            SET contact_status = 'failed', updated_at = NOW()
            WHERE id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (row_id,))

    def reset_in_progress_contacts(self) -> int:
        """Reset stale in_progress contacts back to pending."""
        sql = """
            UPDATE places_info
            SET contact_status = 'pending', updated_at = NOW()
            WHERE contact_status = 'in_progress'
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

    # ── dashboard queries (read-only) ──────────────────────────────

    def dashboard_overall_stats(self) -> dict:
        """Top-level counts for the dashboard."""
        sql = """
            SELECT
                (SELECT COUNT(*) FROM sample_points) AS total_sample_points,
                (SELECT COUNT(*) FROM categories) AS total_categories,
                (SELECT COUNT(*) FROM category_sample_point_mappings WHERE status = 'done') AS mappings_done,
                (SELECT COUNT(*) FROM category_sample_point_mappings WHERE status = 'pending') AS mappings_pending,
                (SELECT COUNT(*) FROM category_sample_point_mappings WHERE status = 'failed') AS mappings_failed,
                (SELECT COUNT(*) FROM places_info) AS total_businesses,
                (SELECT COALESCE(SUM(duplicate_count), 0) FROM places_info) AS total_duplicate_hits,
                (SELECT COUNT(*) FROM places_info WHERE info_status = 'done') AS enriched_done,
                (SELECT COUNT(*) FROM places_info WHERE info_status = 'failed') AS enriched_failed,
                (SELECT COUNT(*) FROM places_info WHERE contact_status = 'done') AS contacts_done,
                (SELECT COUNT(*) FROM places_info WHERE contact_status = 'failed') AS contacts_failed,
                (SELECT COUNT(*) FROM places_info WHERE website != '') AS businesses_with_websites,
                (SELECT COUNT(*) FROM places_info WHERE contact_status = 'done'
                    AND (website_email != '' OR website_phone != '' OR social_media != '')) AS contacts_with_data
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {
                "total_sample_points": row[0],
                "total_categories": row[1],
                "mappings_done": row[2],
                "mappings_pending": row[3],
                "mappings_failed": row[4],
                "total_businesses": row[5],
                "total_duplicate_hits": row[6],
                "enriched_done": row[7],
                "enriched_failed": row[8],
                "contacts_done": row[9],
                "contacts_failed": row[10],
                "businesses_with_websites": row[11],
                "contacts_with_data": row[12],
            }

    def dashboard_category_breakdown(self) -> List[dict]:
        """Per-category stats for the dashboard."""
        sql = """
            SELECT
                c.name AS category,
                COUNT(DISTINCT m.id) FILTER (WHERE m.status = 'done') AS mappings_done,
                COUNT(DISTINCT m.id) FILTER (WHERE m.status = 'failed') AS mappings_failed,
                COALESCE(SUM(m.total_results), 0) AS total_raw_results,
                COUNT(DISTINCT p.id) AS unique_businesses,
                COUNT(DISTINCT p.id) FILTER (WHERE p.info_status = 'done') AS enriched_count,
                COUNT(DISTINCT p.id) FILTER (WHERE p.contact_status = 'done') AS contacts_done
            FROM categories c
            LEFT JOIN category_sample_point_mappings m ON m.category_id = c.id
            LEFT JOIN places_info p ON p.mapping_id = m.id
            GROUP BY c.id, c.name
            ORDER BY c.name
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return [
                {
                    "category": row[0],
                    "mappings_done": row[1],
                    "mappings_failed": row[2],
                    "total_raw_results": row[3],
                    "unique_businesses": row[4],
                    "enriched_count": row[5],
                    "contacts_done": row[6],
                }
                for row in cur.fetchall()
            ]

    def dashboard_sample_point_stats(self) -> List[dict]:
        """Per-point data for map visualization."""
        sql = """
            SELECT
                sp.id,
                sp.lat,
                sp.lng,
                COUNT(DISTINCT m.id) AS total_mappings,
                COUNT(DISTINCT m.id) FILTER (WHERE m.status = 'done') AS mappings_done,
                COALESCE(SUM(m.total_results), 0) AS total_raw_results,
                COUNT(DISTINCT p.id) AS unique_businesses,
                COALESCE(SUM(p.duplicate_count), 0) AS duplicate_hits
            FROM sample_points sp
            LEFT JOIN category_sample_point_mappings m ON m.sample_point_id = sp.id
            LEFT JOIN places_info p ON p.mapping_id = m.id
            GROUP BY sp.id, sp.lat, sp.lng
            ORDER BY sp.id
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return [
                {
                    "id": row[0],
                    "lat": float(row[1]),
                    "lng": float(row[2]),
                    "total_mappings": row[3],
                    "mappings_done": row[4],
                    "total_raw_results": row[5],
                    "unique_businesses": row[6],
                    "duplicate_hits": row[7],
                }
                for row in cur.fetchall()
            ]

    def dashboard_zero_result_points(self) -> List[dict]:
        """Points where all mappings are done but zero results."""
        sql = """
            SELECT
                sp.id,
                sp.lat,
                sp.lng,
                COUNT(m.id) AS total_mappings
            FROM sample_points sp
            JOIN category_sample_point_mappings m ON m.sample_point_id = sp.id
            WHERE m.status = 'done'
            GROUP BY sp.id, sp.lat, sp.lng
            HAVING SUM(m.total_results) = 0
            ORDER BY sp.id
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return [
                {
                    "id": row[0],
                    "lat": float(row[1]),
                    "lng": float(row[2]),
                    "total_mappings": row[3],
                }
                for row in cur.fetchall()
            ]

    def dashboard_duplicate_hotspots(self, limit: int = 50) -> List[dict]:
        """Top businesses by duplicate_count."""
        sql = """
            SELECT name, category, latitude, longitude, duplicate_count
            FROM places_info
            WHERE duplicate_count > 0
            ORDER BY duplicate_count DESC
            LIMIT %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (limit,))
            return [
                {
                    "name": row[0],
                    "category": row[1],
                    "lat": float(row[2]) if row[2] else None,
                    "lng": float(row[3]) if row[3] else None,
                    "duplicate_count": row[4],
                }
                for row in cur.fetchall()
            ]

    def dashboard_point_category_breakdown(self) -> dict:
        """Per-point per-category stats. Returns {point_id: {category: {total_results, unique_businesses}}}."""
        sql = """
            SELECT
                m.sample_point_id,
                c.name AS category,
                m.total_results,
                COUNT(p.id) AS unique_businesses
            FROM category_sample_point_mappings m
            JOIN categories c ON c.id = m.category_id
            LEFT JOIN places_info p ON p.mapping_id = m.id
            GROUP BY m.sample_point_id, c.name, m.total_results
            ORDER BY m.sample_point_id, c.name
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            result: dict = {}
            for row in cur.fetchall():
                point_id = row[0]
                if point_id not in result:
                    result[point_id] = {}
                result[point_id][row[1]] = {
                    "total_results": row[2],
                    "unique_businesses": row[3],
                }
            return result

    def dashboard_duplicate_distribution(self) -> List[dict]:
        """Histogram of duplicate counts."""
        sql = """
            SELECT duplicate_count, COUNT(*) AS business_count
            FROM places_info
            GROUP BY duplicate_count
            ORDER BY duplicate_count
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return [
                {"duplicate_count": row[0], "business_count": row[1]}
                for row in cur.fetchall()
            ]

    def close(self) -> None:
        self._conn.close()
        logger.info("PostgreSQL connection closed")
