#!/usr/bin/env python3
"""CLI entry point for the Google Maps polygon scraper.

Three-step resumable pipeline:
  python main.py add-category "restaurants" "hospitals" "schools"
  python main.py sample  --kml boundary.kml
  python main.py extract --workers 4 --max-results 10
"""

import argparse
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from scraper.kml_parser import parse_kml
from scraper.sampler import generate_sample_points, calculate_area_km2
from scraper.browser import search_and_extract, extract_place_details
from scraper.db import PlacesDB
from scraper.dedup import PolygonFilter
from scraper.models import Business
from scraper.progress import ProgressTracker
from scraper.live_server import start_live_server
from scraper.website import extract_website_contacts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape Google Maps business data within a KML polygon boundary."
    )
    sub = parser.add_subparsers(dest="command")

    # ── add-category ────────────────────────────────────────────────
    ac = sub.add_parser("add-category", help="Add one or more search categories to the DB")
    ac.add_argument("names", nargs="+", help="Category names (e.g. 'restaurants' 'hospitals')")

    # ── list-categories ─────────────────────────────────────────────
    sub.add_parser("list-categories", help="List all categories in the DB")

    # ── sample ──────────────────────────────────────────────────────
    sp = sub.add_parser("sample", help="Parse KML and store sample points in DB")
    sp.add_argument("--kml", required=True, help="Path to KML file with polygon boundary")
    sp.add_argument("--category", default=None, help="Single category (default: all categories in DB)")

    # ── extract ─────────────────────────────────────────────────────
    ep = sub.add_parser("extract", help="Extract businesses from pending sample points (resumable)")
    ep.add_argument("--category", default=None, help="Single category (default: all categories)")
    ep.add_argument("--kml", default=None, help="Optional KML file for polygon filtering and live map")
    ep.add_argument("--workers", type=int, default=4, help="Number of parallel browser workers (default: 4)")
    ep.add_argument("--max-results", type=int, default=10, help="Max results per search point (default: 10)")
    ep.add_argument("--live", action="store_true", help="Start live progress dashboard (requires --kml)")

    # ── enrich ───────────────────────────────────────────────────────
    nr = sub.add_parser("enrich", help="Enrich places_info by visiting detail pages (resumable)")
    nr.add_argument("--workers", type=int, default=4, help="Number of parallel browser workers (default: 4)")
    nr.add_argument("--limit", type=int, default=None, help="Max number of places to enrich")

    # ── contact ──────────────────────────────────────────────────────
    ct = sub.add_parser("contact", help="Extract emails, phones, social links from business websites (resumable)")
    ct.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    ct.add_argument("--limit", type=int, default=None, help="Max number of places to process")

    return parser


# ── add-category ────────────────────────────────────────────────────

def cmd_add_category(args: argparse.Namespace) -> None:
    db = PlacesDB()
    try:
        for name in args.names:
            db.insert_category(name)
            logger.info(f"Added category: {name}")
    finally:
        db.close()


# ── list-categories ─────────────────────────────────────────────────

def cmd_list_categories(args: argparse.Namespace) -> None:
    db = PlacesDB()
    try:
        categories = db.list_categories()
        if not categories:
            logger.info("No categories found. Use 'add-category' to add some.")
            return
        logger.info(f"Found {len(categories)} categories:")
        for cat in categories:
            print(f"  [{cat['id']}] {cat['name']}")
    finally:
        db.close()


# ── Step 1: sample ──────────────────────────────────────────────────

def cmd_sample(args: argparse.Namespace) -> None:
    """Parse KML, generate sample points, and store them in the DB."""
    logger.info(f"Parsing KML file: {args.kml}")
    polygon_coords = parse_kml(args.kml)
    logger.info(f"Polygon has {len(polygon_coords)} vertices")

    sample_points, zoom = generate_sample_points(polygon_coords)
    logger.info(f"Generated {len(sample_points)} sample points at zoom {zoom}")

    db = PlacesDB()
    try:
        # Step 1: Insert geographic points (ON CONFLICT DO NOTHING)
        point_ids = db.insert_sample_points(sample_points, zoom, args.kml)
        logger.info(f"Sample points in DB: {len(point_ids)} (new + existing)")

        # Step 2: Determine which categories to create mappings for
        if args.category:
            categories = [args.category]
        else:
            cats = db.list_categories()
            if not cats:
                logger.error("No categories in DB. Use 'add-category' first, or pass --category.")
                return
            categories = [c["name"] for c in cats]

        # Step 3: Create mappings for each category (ON CONFLICT DO NOTHING)
        total_new = 0
        for cat_name in categories:
            cat_id = db.get_or_create_category(cat_name)
            new_count = db.create_mappings(cat_id, point_ids)
            total_new += new_count
            logger.info(f"Category '{cat_name}': {new_count} new mappings created")

        logger.info(
            f"Done: {len(point_ids)} geographic points x {len(categories)} categories. "
            f"{total_new} new mappings created (duplicates skipped)."
        )
    finally:
        db.close()


# ── Step 2: extract ─────────────────────────────────────────────────

def cmd_extract(args: argparse.Namespace) -> None:
    """Fetch pending mappings and run browser extraction. Resumable."""
    db = PlacesDB()

    # Reset any mappings left as in_progress from a previous interrupted run
    reset_count = db.reset_in_progress_mappings(args.category)
    if reset_count:
        logger.info(f"Reset {reset_count} interrupted in_progress mappings back to pending")

    # Fetch pending mappings — filtered by category if provided, else all
    pending = db.fetch_pending_mappings(args.category)

    if not pending:
        label = f"category '{args.category}'" if args.category else "any category"
        logger.info(f"No pending mappings for {label}. Nothing to do.")
        db.close()
        return

    # Summarise what we're about to process
    categories_in_batch = sorted(set(p["category"] for p in pending))
    logger.info(
        f"Found {len(pending)} pending mappings "
        f"across {len(categories_in_batch)} categories: {', '.join(categories_in_batch)}"
    )

    # Optional polygon filter + live server
    poly_filter = None
    server = None
    tracker = None
    if args.kml:
        polygon_coords = parse_kml(args.kml)
        poly_filter = PolygonFilter(polygon_coords)
        if args.live:
            area_km2 = calculate_area_km2(polygon_coords)
            sample_coords = [(p["lat"], p["lng"]) for p in pending]
            tracker = ProgressTracker(polygon_coords, sample_coords, area_km2)
            server = start_live_server(tracker)

    total_written = 0
    write_lock = threading.Lock()

    def _on_extract(biz: Business, point_idx: int, mapping_id: int) -> None:
        nonlocal total_written
        if poly_filter and not poly_filter.is_inside(biz):
            return
        with write_lock:
            total_written += 1
            if tracker:
                tracker.add_business(point_idx)
        try:
            db.insert_business(biz, mapping_id=mapping_id)
        except Exception as e:
            logger.warning(f"DB insert failed for {biz.place_id}: {e}")

    def _process_mapping(idx: int, mapping: dict) -> None:
        mapping_id = mapping["mapping_id"]
        lat, lng, zoom = mapping["lat"], mapping["lng"], mapping["zoom"]
        category = mapping["category"]

        if not db.claim_mapping(mapping_id):
            return  # Already claimed by another process

        if tracker:
            tracker.mark_active(idx)

        logger.info(f"Mapping {idx+1}/{len(pending)}: ({lat:.6f}, {lng:.6f}) [{category}] [id={mapping_id}]")
        try:
            callback = lambda biz, _idx=idx, _mid=mapping_id: _on_extract(biz, _idx, _mid)
            results = search_and_extract(lat, lng, category, zoom, args.max_results, on_extract=callback)
            db.mark_mapping_done(mapping_id, total_results=len(results))
            if tracker:
                tracker.mark_done(idx)
            logger.info(f"Mapping {mapping_id} done ({len(results)} results) — running total: {total_written} businesses")
        except Exception as e:
            db.mark_mapping_failed(mapping_id)
            if tracker:
                tracker.mark_done(idx)
            logger.error(f"Mapping {mapping_id} failed: {e}")

    try:
        if args.workers <= 1:
            for idx, mapping in enumerate(pending):
                _process_mapping(idx, mapping)
        else:
            logger.info(f"Starting {args.workers} workers")
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {}
                for idx, mapping in enumerate(pending):
                    time.sleep(idx * 2.0 if idx < args.workers else 0)
                    futures[executor.submit(_process_mapping, idx, mapping)] = idx
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Worker error: {e}")

        logger.info(f"Extraction complete: {total_written} businesses inserted")
    except KeyboardInterrupt:
        logger.info(f"Interrupted — {total_written} businesses inserted so far. Re-run to resume.")
    finally:
        db.close()
        if server:
            server.shutdown()


# ── Step 3: enrich ─────────────────────────────────────────────────

def cmd_enrich(args: argparse.Namespace) -> None:
    """Visit each business detail page to extract phone, website, and review count."""
    db = PlacesDB()

    # Reset any in_progress enrichments from a previous interrupted run
    reset_count = db.reset_in_progress_enrichments()
    if reset_count:
        logger.info(f"Reset {reset_count} interrupted in_progress enrichments back to pending")

    pending = db.fetch_pending_enrichments(limit=args.limit)
    if not pending:
        logger.info("No pending enrichments. Nothing to do.")
        db.close()
        return

    logger.info(f"Found {len(pending)} places to enrich")

    done_count = 0
    failed_count = 0
    count_lock = threading.Lock()

    def _enrich_one(idx: int, row: dict) -> None:
        nonlocal done_count, failed_count
        row_id = row["id"]
        url = row["google_maps_url"]

        if not db.claim_enrichment(row_id):
            return

        logger.info(f"Enriching {idx+1}/{len(pending)}: id={row_id}")
        try:
            details = extract_place_details(url)
            db.update_enrichment(
                row_id,
                total_reviews=details["total_reviews"],
                phone=details["phone"],
                website=details["website"],
            )
            with count_lock:
                done_count += 1
            logger.info(f"Enriched id={row_id}: reviews={details['total_reviews']}, phone={details['phone']!r}, website={details['website']!r}")
        except Exception as e:
            db.mark_enrichment_failed(row_id)
            with count_lock:
                failed_count += 1
            logger.error(f"Enrichment failed for id={row_id}: {e}")

    try:
        if args.workers <= 1:
            for idx, row in enumerate(pending):
                _enrich_one(idx, row)
        else:
            logger.info(f"Starting {args.workers} workers")
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {}
                for idx, row in enumerate(pending):
                    if idx < args.workers:
                        time.sleep(idx * 2.0)
                    futures[executor.submit(_enrich_one, idx, row)] = idx
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Worker error: {e}")

        logger.info(f"Enrichment complete: {done_count} done, {failed_count} failed")
    except KeyboardInterrupt:
        logger.info(f"Interrupted — {done_count} done, {failed_count} failed so far. Re-run to resume.")
    finally:
        db.close()


# ── Step 4: contact ────────────────────────────────────────────────

def cmd_contact(args: argparse.Namespace) -> None:
    """Visit business websites to extract emails, phones, and social media links."""
    db = PlacesDB()

    # Reset any in_progress contacts from a previous interrupted run
    reset_count = db.reset_in_progress_contacts()
    if reset_count:
        logger.info(f"Reset {reset_count} interrupted in_progress contacts back to pending")

    pending = db.fetch_pending_contacts(limit=args.limit)
    if not pending:
        logger.info("No pending contacts (or no rows with websites). Nothing to do.")
        db.close()
        return

    logger.info(f"Found {len(pending)} places to extract contacts from")

    done_count = 0
    failed_count = 0
    count_lock = threading.Lock()

    def _contact_one(idx: int, row: dict) -> None:
        nonlocal done_count, failed_count
        row_id = row["id"]
        website = row["website"]

        if not db.claim_contact(row_id):
            return

        logger.info(f"Contact {idx+1}/{len(pending)}: id={row_id} url={website}")
        try:
            result = extract_website_contacts(website)
            emails = ", ".join(result["emails"])
            phones = ", ".join(result["phones"])
            social = ", ".join(result["social_media"])
            db.update_contact(row_id, emails=emails, phones=phones, social_media=social)
            with count_lock:
                done_count += 1
            logger.info(
                f"Contact id={row_id}: emails={emails!r}, phones={phones!r}, social={social!r}"
            )
        except Exception as e:
            db.mark_contact_failed(row_id)
            with count_lock:
                failed_count += 1
            logger.error(f"Contact failed for id={row_id}: {e}")

    try:
        if args.workers <= 1:
            for idx, row in enumerate(pending):
                _contact_one(idx, row)
        else:
            logger.info(f"Starting {args.workers} workers")
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {}
                for idx, row in enumerate(pending):
                    if idx < args.workers:
                        time.sleep(idx * 2.0)
                    futures[executor.submit(_contact_one, idx, row)] = idx
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Worker error: {e}")

        logger.info(f"Contact extraction complete: {done_count} done, {failed_count} failed")
    except KeyboardInterrupt:
        logger.info(f"Interrupted — {done_count} done, {failed_count} failed so far. Re-run to resume.")
    finally:
        db.close()


# ── main ────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "add-category":
        cmd_add_category(args)
    elif args.command == "list-categories":
        cmd_list_categories(args)
    elif args.command == "sample":
        cmd_sample(args)
    elif args.command == "extract":
        cmd_extract(args)
    elif args.command == "enrich":
        cmd_enrich(args)
    elif args.command == "contact":
        cmd_contact(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
