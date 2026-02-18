#!/usr/bin/env python3
"""CLI entry point for the Google Maps polygon scraper."""

import argparse
import csv
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from scraper.kml_parser import parse_kml
from scraper.sampler import generate_sample_points, calculate_area_km2
from scraper.browser import search_and_extract
from scraper.dedup import Deduplicator
from scraper.models import Business
from scraper.progress import ProgressTracker
from scraper.live_server import start_live_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape Google Maps business data within a KML polygon boundary."
    )
    parser.add_argument("--kml", required=True, help="Path to KML file with polygon boundary")
    parser.add_argument("--category", required=True, help="Search category (e.g. 'restaurants')")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel browser workers (default: 4)")
    parser.add_argument("--output", default="output/results.csv", help="Output CSV path (default: output/results.csv)")
    parser.add_argument("--max-results", type=int, default=10, help="Max results per search point (default: 10)")
    parser.add_argument("--db", action="store_true", help="Also store results in PostgreSQL (requires DATABASE_URL or DB_* env vars)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # 0. Optional DB connection
    db = None
    if args.db:
        try:
            from scraper.db import PlacesDB
            db = PlacesDB()
        except Exception as e:
            logger.warning(f"Could not connect to PostgreSQL, continuing CSV-only: {e}")

    # 1. Parse KML
    logger.info(f"Parsing KML file: {args.kml}")
    polygon_coords = parse_kml(args.kml)
    logger.info(f"Polygon has {len(polygon_coords)} vertices")

    # 2. Generate sample points
    sample_points, zoom = generate_sample_points(polygon_coords)
    logger.info(f"Will search {len(sample_points)} points at zoom {zoom}")

    # 3. Set up deduplicator and progress tracker
    dedup = Deduplicator(polygon_coords)
    area_km2 = calculate_area_km2(polygon_coords)
    tracker = ProgressTracker(polygon_coords, sample_points, area_km2)
    server = start_live_server(tracker)

    # 4. Open CSV and write header immediately so partial results survive interrupts
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    csv_file = open(args.output, "w", newline="", encoding="utf-8")
    writer = csv.writer(csv_file)
    writer.writerow(Business.csv_headers())
    csv_file.flush()

    total_written = 0
    write_lock = threading.Lock()

    def _on_extract(biz: Business, point_idx: int) -> None:
        """Called per-business from browser thread. Dedup + write to CSV immediately."""
        nonlocal total_written
        kept = dedup.filter_businesses([biz])
        if kept:
            with write_lock:
                writer.writerow(kept[0].to_csv_row())
                csv_file.flush()
                total_written += 1
                tracker.add_business(point_idx)
            if db:
                try:
                    db.insert_business(kept[0])
                except Exception as e:
                    logger.warning(f"DB insert failed for {kept[0].place_id}: {e}")

    try:
        if args.workers <= 1:
            # Single-threaded
            for idx, (lat, lng) in enumerate(sample_points):
                logger.info(f"Point {idx+1}/{len(sample_points)}: ({lat:.6f}, {lng:.6f})")
                tracker.mark_active(idx)
                callback = lambda biz, _idx=idx: _on_extract(biz, _idx)
                search_and_extract(lat, lng, args.category, zoom, args.max_results, on_extract=callback)
                tracker.mark_done(idx)
                logger.info(f"Running total: {total_written} unique businesses")
        else:
            # Multi-threaded
            logger.info(f"Starting {args.workers} workers for {len(sample_points)} points")

            def _worker(idx: int, lat: float, lng: float) -> list[Business]:
                # Stagger starts to avoid detection
                time.sleep(idx * 2.0)
                logger.info(f"[Worker {idx+1}] Point ({lat:.6f}, {lng:.6f})")
                tracker.mark_active(idx)
                callback = lambda biz, _idx=idx: _on_extract(biz, _idx)
                return search_and_extract(lat, lng, args.category, zoom, args.max_results, on_extract=callback)

            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {
                    executor.submit(_worker, idx, lat, lng): idx
                    for idx, (lat, lng) in enumerate(sample_points)
                }
                for future in as_completed(futures):
                    idx = futures[future]
                    try:
                        future.result()
                        tracker.mark_done(idx)
                        logger.info(
                            f"[Worker {idx+1}] done — "
                            f"running total: {total_written} unique businesses"
                        )
                    except Exception as e:
                        tracker.mark_done(idx)
                        logger.error(f"[Worker {idx+1}] failed: {e}")

        logger.info(f"Scraping complete: {total_written} unique businesses inside polygon")
    except KeyboardInterrupt:
        logger.info(f"Interrupted — saved {total_written} businesses to {args.output}")
    finally:
        csv_file.close()
        if db:
            db.close()
        server.shutdown()

    logger.info(f"Output: {args.output}")


if __name__ == "__main__":
    main()
