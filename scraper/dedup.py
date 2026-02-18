"""Thread-safe deduplication and polygon boundary filtering."""

import threading
import logging
from typing import List, Tuple

from shapely.geometry import Polygon, Point

from scraper.models import Business

logger = logging.getLogger(__name__)


class Deduplicator:
    """Thread-safe business deduplicator with polygon boundary check."""

    def __init__(self, polygon_coords: List[Tuple[float, float]]):
        self._lock = threading.Lock()
        self._seen_place_ids: set[str] = set()
        self._polygon = Polygon([(lng, lat) for lat, lng in polygon_coords])

    def filter_businesses(self, businesses: List[Business]) -> List[Business]:
        """
        Filter a batch of businesses: remove duplicates and those outside the polygon.

        Thread-safe — multiple workers can call this concurrently.
        """
        kept: List[Business] = []
        stats = {"new": 0, "outside": 0, "duplicate": 0, "no_coords": 0}

        for biz in businesses:
            if biz.latitude is None or biz.longitude is None:
                stats["no_coords"] += 1
                continue

            if not self._polygon.contains(Point(biz.longitude, biz.latitude)):
                stats["outside"] += 1
                continue

            with self._lock:
                if biz.place_id and biz.place_id in self._seen_place_ids:
                    stats["duplicate"] += 1
                    continue

                if biz.place_id:
                    self._seen_place_ids.add(biz.place_id)
                kept.append(biz)
                stats["new"] += 1

        logger.info(
            f"Filter results: {stats['new']} new, "
            f"{stats['outside']} outside, "
            f"{stats['duplicate']} duplicates, "
            f"{stats['no_coords']} no coords"
        )
        return kept

    @property
    def total_seen(self) -> int:
        with self._lock:
            return len(self._seen_place_ids)
