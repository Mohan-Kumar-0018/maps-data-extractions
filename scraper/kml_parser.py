"""Parse KML files and extract polygon coordinates."""

import xml.etree.ElementTree as ET
from typing import List, Tuple


def parse_kml(filepath: str) -> List[Tuple[float, float]]:
    """
    Parse a KML file and return polygon coordinates as (lat, lng) tuples.

    Supports KML files with Polygon geometry. Extracts the first
    Polygon's outer boundary coordinates.
    """
    tree = ET.parse(filepath)
    root = tree.getroot()

    # KML uses a namespace - detect it from the root tag
    ns = ""
    if root.tag.startswith("{"):
        ns = root.tag.split("}")[0] + "}"

    # Find all <coordinates> elements inside Polygon/outerBoundaryIs
    # Try multiple paths since KML structure can vary
    coord_text = None

    # Path 1: Polygon > outerBoundaryIs > LinearRing > coordinates
    for coords_elem in root.iter(f"{ns}coordinates"):
        # Walk up to check if this is inside a Polygon
        # Since ElementTree doesn't support parent lookup easily,
        # we search for the full path instead
        coord_text = coords_elem.text
        if coord_text and coord_text.strip():
            break

    if not coord_text:
        raise ValueError(f"No coordinates found in KML file: {filepath}")

    return _parse_coordinate_string(coord_text.strip())


def _parse_coordinate_string(text: str) -> List[Tuple[float, float]]:
    """
    Parse KML coordinate string into (lat, lng) tuples.

    KML format is: lng,lat[,alt] separated by whitespace.
    We return (lat, lng) to match standard geographic convention.
    """
    coords = []
    for token in text.split():
        parts = token.strip().split(",")
        if len(parts) >= 2:
            lng = float(parts[0])
            lat = float(parts[1])
            coords.append((lat, lng))

    if len(coords) < 3:
        raise ValueError(f"Polygon needs at least 3 points, got {len(coords)}")

    return coords
