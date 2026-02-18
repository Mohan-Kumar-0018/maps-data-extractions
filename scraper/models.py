"""Business data model and CSV helpers."""

from dataclasses import dataclass, fields, asdict
from typing import Optional


@dataclass
class Business:
    """Represents a business extracted from Google Maps."""

    name: str = ""
    rating: Optional[float] = None
    total_reviews: Optional[int] = None
    address: str = ""
    phone: str = ""
    website: str = ""
    opening_hours: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    google_maps_url: str = ""
    place_id: str = ""
    category: str = ""

    @staticmethod
    def csv_headers() -> list[str]:
        return [f.name for f in fields(Business)]

    def to_csv_row(self) -> list[str]:
        d = asdict(self)
        return [str(d[h]) if d[h] is not None else "" for h in self.csv_headers()]
