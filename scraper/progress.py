"""Thread-safe progress tracker for live dashboard."""

import json
import threading
import time
from typing import List, Tuple


class ProgressTracker:
    """Tracks scraping progress across all sample points."""

    def __init__(
        self,
        polygon_coords: List[Tuple[float, float]],
        grid_points: List[Tuple[float, float]],
        area_km2: float,
    ):
        self._lock = threading.Lock()
        self._start_time = time.time()
        self.polygon = polygon_coords
        self.area_km2 = area_km2
        self.points = [
            {"lat": lat, "lng": lng, "status": "pending", "businesses": 0}
            for lat, lng in grid_points
        ]
        self.total_businesses = 0

    def mark_active(self, idx: int) -> None:
        with self._lock:
            self.points[idx]["status"] = "active"

    def mark_done(self, idx: int) -> None:
        with self._lock:
            self.points[idx]["status"] = "done"

    def add_business(self, idx: int) -> None:
        with self._lock:
            self.points[idx]["businesses"] += 1
            self.total_businesses += 1

    def to_json(self) -> str:
        with self._lock:
            done = sum(1 for p in self.points if p["status"] == "done")
            active = sum(1 for p in self.points if p["status"] == "active")
            data = {
                "polygon": [[lat, lng] for lat, lng in self.polygon],
                "area_km2": round(self.area_km2, 2),
                "total_points": len(self.points),
                "done_points": done,
                "active_points": active,
                "points": list(self.points),
                "total_businesses": self.total_businesses,
                "elapsed_seconds": round(time.time() - self._start_time, 1),
            }
            return json.dumps(data)
