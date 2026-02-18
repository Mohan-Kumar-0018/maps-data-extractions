#!/usr/bin/env python3
"""Quick test: scrape a single location at zoom 16."""

import argparse
import csv
import logging
import os

from scraper.browser import search_and_extract
from scraper.models import Business

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Test scrape at a single location.")
    parser.add_argument("--lat", type=float, default=24.7521, help="Latitude (default: 24.7521)")
    parser.add_argument("--lng", type=float, default=46.6748, help="Longitude (default: 46.6748)")
    parser.add_argument("--category", default="restaurants", help="Search category (default: restaurants)")
    parser.add_argument("--max-results", type=int, default=10, help="Max results (default: 10)")
    parser.add_argument("--output", default="output/test_single.csv", help="Output CSV (default: output/test_single.csv)")
    parser.add_argument("--db", action="store_true", help="Also store results in PostgreSQL (requires DATABASE_URL or DB_* env vars)")
    args = parser.parse_args()

    db = None
    if args.db:
        try:
            from scraper.db import PlacesDB
            db = PlacesDB()
        except Exception as e:
            logger.warning(f"Could not connect to PostgreSQL, continuing CSV-only: {e}")

    logger.info(f"Searching '{args.category}' at ({args.lat}, {args.lng}) zoom 16")

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    csv_file = open(args.output, "w", newline="", encoding="utf-8")
    writer = csv.writer(csv_file)
    writer.writerow(Business.csv_headers())
    csv_file.flush()

    count = 0

    def _on_extract(biz):
        nonlocal count
        writer.writerow(biz.to_csv_row())
        csv_file.flush()
        count += 1
        if db:
            try:
                db.insert_business(biz)
            except Exception as e:
                logger.warning(f"DB insert failed for {biz.place_id}: {e}")

    try:
        search_and_extract(args.lat, args.lng, args.category, 16, args.max_results, on_extract=_on_extract)
    except KeyboardInterrupt:
        logger.info(f"Interrupted — saved {count} businesses")
    finally:
        csv_file.close()
        if db:
            db.close()

    logger.info(f"Done: {count} businesses saved to {args.output}")


if __name__ == "__main__":
    main()
