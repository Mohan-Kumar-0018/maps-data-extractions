"""Generate search sample points within a polygon."""

import math
import logging
from functools import partial
from typing import List, Tuple

from shapely.geometry import Polygon, Point
from shapely.ops import transform
import pyproj

logger = logging.getLogger(__name__)


def calculate_area_km2(polygon_coords: List[Tuple[float, float]]) -> float:
    """
    Calculate polygon area in km² using UTM projection.

    Args:
        polygon_coords: List of (lat, lng) tuples.
    """
    try:
        polygon = Polygon([(lng, lat) for lat, lng in polygon_coords])
        bounds = polygon.bounds  # (min_lng, min_lat, max_lng, max_lat)
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lng = (bounds[0] + bounds[2]) / 2

        wgs84 = pyproj.CRS("EPSG:4326")
        utm_zone = int((center_lng + 180) / 6) + 1
        hemisphere = "326" if center_lat >= 0 else "327"
        utm_crs = pyproj.CRS(f"EPSG:{hemisphere}{utm_zone}")

        transformer = pyproj.Transformer.from_crs(wgs84, utm_crs, always_xy=True)
        polygon_utm = transform(transformer.transform, polygon)

        return polygon_utm.area / 1_000_000
    except Exception as e:
        logger.warning(f"Could not calculate exact area: {e}, defaulting to 1.0 km²")
        return 1.0


def generate_sample_points(
    polygon_coords: List[Tuple[float, float]],
) -> Tuple[List[Tuple[float, float]], int]:
    """
    Generate evenly distributed sample points within a polygon.

    Auto-scales point density: ~1 search point per 3 km² at fixed zoom 16.
    This ensures larger polygons get proportionally more coverage.

    Args:
        polygon_coords: List of (lat, lng) tuples.

    Returns:
        Tuple of (sample_points, zoom_level).
    """
    area_km2 = calculate_area_km2(polygon_coords)

    zoom_level = 16
    num_points = max(1, round(area_km2 / 3))

    logger.info(f"Area: {area_km2:.2f} km² -> {num_points} sample points, zoom {zoom_level}")

    polygon = Polygon([(lng, lat) for lat, lng in polygon_coords])
    min_lng, min_lat, max_lng, max_lat = polygon.bounds

    grid_size = int(math.ceil(math.sqrt(num_points)))
    lat_step = (max_lat - min_lat) / (grid_size + 1)
    lng_step = (max_lng - min_lng) / (grid_size + 1)

    sample_points: List[Tuple[float, float]] = []
    for i in range(1, grid_size + 1):
        for j in range(1, grid_size + 1):
            lat = min_lat + i * lat_step
            lng = min_lng + j * lng_step
            if polygon.contains(Point(lng, lat)):
                sample_points.append((lat, lng))
            if len(sample_points) >= num_points:
                break
        if len(sample_points) >= num_points:
            break

    # Fallback: use centroid if no grid points fell inside the polygon
    if not sample_points:
        centroid = polygon.centroid
        sample_points.append((centroid.y, centroid.x))

    logger.info(f"Generated {len(sample_points)} search points within polygon")
    return sample_points, zoom_level


def generate_sub_points(
    lat: float,
    lng: float,
    current_zoom: int,
    max_zoom: int = 18,
) -> Tuple[List[Tuple[float, float]], int] | Tuple[None, None]:
    """
    Generate a 2x2 grid of sub-points around (lat, lng) at zoom + 1.

    Returns (sub_points, new_zoom) or (None, None) if already at max zoom.
    """
    if current_zoom >= max_zoom:
        return None, None

    new_zoom = current_zoom + 1
    offset = 360 / (2 ** new_zoom) * 1.25

    sub_points = [
        (lat + offset, lng + offset),
        (lat + offset, lng - offset),
        (lat - offset, lng + offset),
        (lat - offset, lng - offset),
    ]

    return sub_points, new_zoom
