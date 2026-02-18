#!/usr/bin/env python3
"""Visualize polygon boundary and sample search points on an interactive map."""

import argparse
import webbrowser

import folium

from scraper.kml_parser import parse_kml
from scraper.sampler import generate_sample_points, calculate_area_km2


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview search grid on a map.")
    parser.add_argument("--kml", required=True, help="Path to KML file with polygon boundary")
    parser.add_argument("--output", default="output/preview.html", help="Output HTML path (default: output/preview.html)")
    parser.add_argument("--no-open", action="store_true", help="Don't auto-open the map in a browser")
    args = parser.parse_args()

    polygon_coords = parse_kml(args.kml)
    area_km2 = calculate_area_km2(polygon_coords)
    sample_points, zoom = generate_sample_points(polygon_coords)

    # Center map on polygon centroid
    center_lat = sum(lat for lat, _ in polygon_coords) / len(polygon_coords)
    center_lng = sum(lng for _, lng in polygon_coords) / len(polygon_coords)

    m = folium.Map(location=[center_lat, center_lng], zoom_start=13)

    # Draw polygon boundary
    folium.Polygon(
        locations=[(lat, lng) for lat, lng in polygon_coords],
        color="blue",
        weight=2,
        fill=True,
        fill_opacity=0.1,
        tooltip=f"Area: {area_km2:.2f} km²",
    ).add_to(m)

    # Draw sample points
    for i, (lat, lng) in enumerate(sample_points):
        folium.CircleMarker(
            location=[lat, lng],
            radius=6,
            color="red",
            fill=True,
            fill_color="red",
            fill_opacity=0.7,
            tooltip=f"Point {i+1}: ({lat:.6f}, {lng:.6f})",
        ).add_to(m)

    import os
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    m.save(args.output)

    print(f"Area: {area_km2:.2f} km²")
    print(f"Sample points: {len(sample_points)} at zoom {zoom}")
    print(f"Map saved to: {args.output}")

    if not args.no_open:
        webbrowser.open(f"file://{os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
