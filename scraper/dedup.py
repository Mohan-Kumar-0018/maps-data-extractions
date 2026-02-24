"""Polygon boundary filtering."""

import logging
from typing import List, Tuple

from shapely.geometry import Polygon, Point

from scraper.models import Business

logger = logging.getLogger(__name__)


class PolygonFilter:
    """Filter businesses to those inside the polygon boundary."""

    def __init__(self, polygon_coords: List[Tuple[float, float]]):
        self._polygon = Polygon([(lng, lat) for lat, lng in polygon_coords])

    def is_inside(self, biz: Business) -> bool:
        """Return True if the business has coords and is inside the polygon."""
        if biz.latitude is None or biz.longitude is None:
            return False
        return self._polygon.contains(Point(biz.longitude, biz.latitude))

    def is_inside_coords(self, lat: float, lng: float) -> bool:
        """Return True if the raw (lat, lng) coordinate is inside the polygon."""
        return self._polygon.contains(Point(lng, lat))
