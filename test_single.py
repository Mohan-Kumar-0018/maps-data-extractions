#!/usr/bin/env python3
"""Quick test: scrape a single location at zoom 16."""

import argparse
import logging

from scraper.browser import search_and_extract
from scraper.db import PlacesDB

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
    args = parser.parse_args()

    db = PlacesDB()

    logger.info(f"Searching '{args.category}' at ({args.lat}, {args.lng}) zoom 16")

    count = 0

    def _on_extract(biz):
        nonlocal count
        try:
            db.insert_business(biz)
            count += 1
        except Exception as e:
            logger.warning(f"DB insert failed for {biz.place_id}: {e}")

    try:
        search_and_extract(args.lat, args.lng, args.category, 16, args.max_results, on_extract=_on_extract)
    except KeyboardInterrupt:
        logger.info(f"Interrupted — inserted {count} businesses")
    finally:
        db.close()

    logger.info(f"Done: {count} businesses inserted to DB")


if __name__ == "__main__":
    main()
