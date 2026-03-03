#!/usr/bin/env python3
"""CLI entry point for the Google Maps polygon scraper.

Resumable pipeline:
  python main.py sample  --kml sample_map.kml
  python main.py extract --kml sample_map.kml --workers 4
  python main.py enrich  --workers 4
  python main.py contact --workers 4
"""

import argparse
import csv
import itertools
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from scraper.kml_parser import parse_kml
from scraper.sampler import generate_grid_points, generate_sub_points, calculate_area_km2
from scraper.browser import search_and_extract, extract_place_details, DEFAULT_USER_AGENT
from scraper.db import ListingsDB, load_config
from scraper.dedup import PolygonFilter
from scraper.models import Business
from scraper.progress import ProgressTracker
from scraper.live_server import start_live_server
from scraper.dashboard_server import start_dashboard_server
from scraper.website import extract_website_contacts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

KML_FILE_PATH = "final_file_path.kml"


def _build_proxy_rotator(config: dict):
    """Return a thread-safe next_proxy() function or None if no proxies configured."""
    proxies = config.get("proxies")
    if not proxies:
        return None
    cycle = itertools.cycle(proxies)
    lock = threading.Lock()
    def next_proxy():
        with lock:
            return next(cycle)
    return next_proxy


def _build_ua_rotator(config: dict):
    """Return a thread-safe next_ua() function, falling back to DEFAULT_USER_AGENT."""
    agents = config.get("user_agents") or [DEFAULT_USER_AGENT]
    cycle = itertools.cycle(agents)
    lock = threading.Lock()
    def next_ua():
        with lock:
            return next(cycle)
    return next_ua


def _load_kml(kml_path: str = KML_FILE_PATH):
    """Parse a KML file. Raises FileNotFoundError if missing."""
    if not os.path.isfile(kml_path):
        raise FileNotFoundError(f"KML file not found: {kml_path}")
    return parse_kml(kml_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scrape Google Maps business data within a KML polygon boundary."
    )
    sub = parser.add_subparsers(dest="command")

    # ── sample ──────────────────────────────────────────────────────
    sp = sub.add_parser("sample", help="Parse KML and store grid points in DB")
    sp.add_argument("--kml", required=True, help="KML polygon file (required)")

    # ── extract ─────────────────────────────────────────────────────
    ep = sub.add_parser("extract", help="Extract businesses from pending search tasks (resumable)")
    ep.add_argument("--kml", required=True, help="KML polygon file (required)")
    ep.add_argument("--workers", type=int, default=4, help="Number of parallel browser workers (default: 4)")
    ep.add_argument("--max-results", type=int, default=20, help="Max results per search point (default: 20)")
    ep.add_argument("--live", action="store_true", help="Start live progress dashboard")
    ep.add_argument("--subdivide-threshold", type=int, default=None,
                    help="Subdivide when new_count >= N (default: max_results - 2)")
    ep.add_argument("--max-zoom", type=int, default=18, help="Stop subdividing past this zoom (default: 18)")
    ep.add_argument("--no-subdivide", action="store_true", help="Disable adaptive subdivision")
    ep.add_argument("--retry-failed", action="store_true", help="Reset failed tasks to pending before extraction")

    # ── enrich ───────────────────────────────────────────────────────
    nr = sub.add_parser("enrich", help="Enrich listings by visiting detail pages (resumable)")
    nr.add_argument("--workers", type=int, default=4, help="Number of parallel browser workers (default: 4)")
    nr.add_argument("--limit", type=int, default=None, help="Max number of places to enrich")
    nr.add_argument("--retry-failed", action="store_true", help="Reset failed enrichments to pending before processing")

    # ── contact ──────────────────────────────────────────────────────
    ct = sub.add_parser("contact", help="Extract emails, phones, social links from business websites (resumable)")
    ct.add_argument("--workers", type=int, default=4, help="Number of parallel workers (default: 4)")
    ct.add_argument("--limit", type=int, default=None, help="Max number of places to process")
    ct.add_argument("--retry-failed", action="store_true", help="Reset failed contacts to pending before processing")

    # ── export ─────────────────────────────────────────────────────────
    ex = sub.add_parser("export", help="Export listings to CSV or JSON")
    ex.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format (default: csv)")
    ex.add_argument("--output", "-o", default=None, help="Output file path (default: auto-generated in output/)")
    ex.add_argument("--category", default=None, help="Single category (default: all)")

    # ── dashboard ────────────────────────────────────────────────────
    db = sub.add_parser("dashboard", help="Start interactive data summary dashboard")
    db.add_argument("--kml", default=KML_FILE_PATH, help=f"KML polygon file (default: {KML_FILE_PATH})")
    db.add_argument("--port", type=int, default=8090, help="Dashboard server port (default: 8090)")

    return parser


# ── Step 1: sample ──────────────────────────────────────────────────

def cmd_sample(args: argparse.Namespace) -> None:
    """Parse KML, generate grid points, and store them in the DB."""
    kml_path = args.kml
    polygon_coords = _load_kml(kml_path)
    logger.info(f"Polygon has {len(polygon_coords)} vertices")

    grid_points, zoom = generate_grid_points(polygon_coords)
    logger.info(f"Generated {len(grid_points)} grid points at zoom {zoom}")

    db = ListingsDB()
    try:
        # Step 1: Insert geographic points (ON CONFLICT DO NOTHING)
        point_ids = db.insert_grid_points(grid_points, zoom, kml_path)
        logger.info(f"Grid points in DB: {len(point_ids)} (new + existing)")

        # Step 2: Get all categories from DB
        cats = db.list_categories()
        if not cats:
            logger.error("No categories in DB.")
            return
        categories = [c["name"] for c in cats]

        # Step 3: Create search tasks for each category (ON CONFLICT DO NOTHING)
        total_new = 0
        for cat_name in categories:
            cat_id = db.get_or_create_category(cat_name)
            new_count = db.create_search_tasks(cat_id, point_ids)
            total_new += new_count
            logger.info(f"Category '{cat_name}': {new_count} new search tasks created")

        logger.info(
            f"Done: {len(point_ids)} geographic points x {len(categories)} categories. "
            f"{total_new} new search tasks created (duplicates skipped)."
        )
    finally:
        db.close()


# ── Step 2: extract ─────────────────────────────────────────────────

def cmd_extract(args: argparse.Namespace) -> None:
    """Fetch pending search tasks and run browser extraction. Resumable."""
    config = load_config()
    screenshots_enabled = config.get("screenshots", False)
    kml_path = args.kml
    db = ListingsDB()

    # Reset any tasks left as in_progress from a previous interrupted run
    reset_count = db.reset_in_progress_tasks()
    if reset_count:
        logger.info(f"Reset {reset_count} interrupted in_progress tasks back to pending")

    # Retry failed tasks if requested
    if args.retry_failed:
        retry_count = db.reset_failed_tasks()
        if retry_count:
            logger.info(f"Reset {retry_count} failed tasks back to pending for retry")

    # Fetch all pending tasks
    pending = db.fetch_pending_tasks()

    if not pending:
        logger.info("No pending tasks. Nothing to do.")
        db.close()
        return

    # Summarise what we're about to process
    categories_in_batch = sorted(set(p["category"] for p in pending))
    logger.info(
        f"Found {len(pending)} pending tasks "
        f"across {len(categories_in_batch)} categories: {', '.join(categories_in_batch)}"
    )

    # Polygon filter (always active) + optional live server
    polygon_coords = _load_kml(kml_path)
    poly_filter = PolygonFilter(polygon_coords)
    server = None
    tracker = None
    if args.live:
        area_km2 = calculate_area_km2(polygon_coords)
        sample_coords = [(p["lat"], p["lng"]) for p in pending]
        tracker = ProgressTracker(polygon_coords, sample_coords, area_km2)
        server = start_live_server(tracker)

    # Subdivision config
    subdivide_enabled = not args.no_subdivide
    subdivide_threshold = args.subdivide_threshold if args.subdivide_threshold is not None else (args.max_results - 2)
    max_zoom = args.max_zoom
    total_subdivisions = 0

    # Proxy / user-agent rotation
    next_proxy = _build_proxy_rotator(config)
    next_ua = _build_ua_rotator(config)

    total_written = 0
    write_lock = threading.Lock()
    # Per-task counters: {search_task_id: {"new": N, "dup": N, "filtered": N}}
    task_counts: dict = {}

    def _on_extract(biz: Business, point_idx: int, search_task_id: int) -> None:
        nonlocal total_written
        if poly_filter and not poly_filter.is_inside(biz):
            with write_lock:
                task_counts[search_task_id]["out_of_bounds"] += 1
            logger.debug(f"  Out of bounds (outside polygon): {biz.name!r} ({biz.latitude}, {biz.longitude})")
            return
        with write_lock:
            total_written += 1
            if tracker:
                tracker.add_business(point_idx)
        try:
            is_new = db.insert_business(biz, search_task_id=search_task_id)
            with write_lock:
                if is_new:
                    task_counts[search_task_id]["new"] += 1
                else:
                    task_counts[search_task_id]["dup"] += 1
        except Exception as e:
            logger.warning(f"DB insert failed for {biz.place_id}: {e}")

    def _process_task(idx: int, task: dict) -> None:
        nonlocal total_subdivisions
        search_task_id = task["search_task_id"]
        lat, lng, zoom = task["lat"], task["lng"], task["zoom"]
        category = task["category"]

        if not db.claim_task(search_task_id):
            return  # Already claimed by another process

        with write_lock:
            task_counts[search_task_id] = {"new": 0, "dup": 0, "out_of_bounds": 0}

        if tracker:
            tracker.mark_active(idx)

        logger.info(f"Task {idx+1}/{len(pending)}: ({lat:.6f}, {lng:.6f}) [{category}] [id={search_task_id}]")
        try:
            callback = lambda biz, _idx=idx, _tid=search_task_id: _on_extract(biz, _idx, _tid)
            screenshot_path = None
            if screenshots_enabled:
                screenshot_dir = os.path.join("output", "screenshots")
                os.makedirs(screenshot_dir, exist_ok=True)
                screenshot_path = os.path.join(screenshot_dir, f"{search_task_id}.png")
            proxy = next_proxy() if next_proxy else None
            ua = next_ua()
            results, used_url = search_and_extract(lat, lng, category, zoom, args.max_results, on_extract=callback, screenshot_path=screenshot_path, proxy=proxy, user_agent=ua)
            tc = task_counts[search_task_id]
            db.mark_task_done(
                search_task_id,
                total_results=len(results),
                new_count=tc["new"],
                duplicate_count=tc["dup"],
                out_of_bounds_count=tc["out_of_bounds"],
                search_url=used_url,
            )
            if tracker:
                tracker.mark_done(idx)
            logger.info(
                f"Task {search_task_id} done: {len(results)} raw → "
                f"{tc['new']} new, {tc['dup']} duplicates, {tc['out_of_bounds']} out of bounds "
                f"— running total: {total_written}"
            )

            # Adaptive subdivision: create sub-points for high-yield tasks
            if subdivide_enabled and tc["new"] >= subdivide_threshold and zoom < max_zoom:
                sub_pts, new_zoom = generate_sub_points(lat, lng, zoom, max_zoom)
                if sub_pts:
                    sub_pts_inside = [p for p in sub_pts if poly_filter.is_inside_coords(*p)]
                    if sub_pts_inside:
                        cat_id = db.get_or_create_category(category)
                        new_tasks = db.insert_subdivision_points(sub_pts_inside, new_zoom, kml_path, cat_id)
                        if new_tasks:
                            with write_lock:
                                total_subdivisions += new_tasks
                            logger.info(
                                f"Subdivided task {search_task_id}: {new_tasks} new pending "
                                f"tasks at zoom {new_zoom} ({len(sub_pts_inside)} sub-points)"
                            )
        except Exception as e:
            db.mark_task_failed(search_task_id)
            if tracker:
                tracker.mark_done(idx)
            logger.error(f"Task {search_task_id} failed: {e}")

    try:
        if args.workers <= 1:
            for idx, task in enumerate(pending):
                _process_task(idx, task)
        else:
            logger.info(f"Starting {args.workers} workers")
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = {}
                for idx, task in enumerate(pending):
                    time.sleep(idx * 2.0 if idx < args.workers else 0)
                    futures[executor.submit(_process_task, idx, task)] = idx
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Worker error: {e}")

        summary = f"Extraction complete: {total_written} businesses inserted"
        if total_subdivisions:
            summary += f", {total_subdivisions} new subdivision tasks queued (re-run to process)"
        logger.info(summary)
    except KeyboardInterrupt:
        logger.info(f"Interrupted — {total_written} businesses inserted so far. Re-run to resume.")
    finally:
        db.close()
        if server:
            server.shutdown()


# ── Step 3: enrich ─────────────────────────────────────────────────

def cmd_enrich(args: argparse.Namespace) -> None:
    """Visit each business detail page to extract phone, website, and review count."""
    config = load_config()
    db = ListingsDB()

    # Reset any in_progress enrichments from a previous interrupted run
    reset_count = db.reset_in_progress_enrichments()
    if reset_count:
        logger.info(f"Reset {reset_count} interrupted in_progress enrichments back to pending")

    # Retry failed enrichments if requested
    if args.retry_failed:
        retry_count = db.reset_failed_enrichments()
        if retry_count:
            logger.info(f"Reset {retry_count} failed enrichments back to pending for retry")

    pending = db.fetch_pending_enrichments(limit=args.limit)
    if not pending:
        logger.info("No pending enrichments. Nothing to do.")
        db.close()
        return

    logger.info(f"Found {len(pending)} places to enrich")

    # Proxy / user-agent rotation
    next_proxy = _build_proxy_rotator(config)
    next_ua = _build_ua_rotator(config)

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
            proxy = next_proxy() if next_proxy else None
            ua = next_ua()
            details = extract_place_details(url, proxy=proxy, user_agent=ua)
            db.update_enrichment(
                row_id,
                total_reviews=details["total_reviews"],
                phone=details["phone"],
                website=details["website"],
                address=details["address"],
            )
            with count_lock:
                done_count += 1
            logger.info(f"Enriched id={row_id}: reviews={details['total_reviews']}, phone={details['phone']!r}, website={details['website']!r}, address={details['address']!r}")
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
    db = ListingsDB()

    # Mark listings without a website as done (nothing to crawl)
    skipped = db.skip_contacts_without_website()
    if skipped:
        logger.info(f"Skipped {skipped} listings with no website")

    # Reset any in_progress contacts from a previous interrupted run
    reset_count = db.reset_in_progress_contacts()
    if reset_count:
        logger.info(f"Reset {reset_count} interrupted in_progress contacts back to pending")

    # Retry failed contacts if requested
    if args.retry_failed:
        retry_count = db.reset_failed_contacts()
        if retry_count:
            logger.info(f"Reset {retry_count} failed contacts back to pending for retry")

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


# ── export ────────────────────────────────────────────────────────

def _serialize_value(val):
    """Convert datetime objects to ISO format strings for JSON/CSV."""
    if isinstance(val, datetime):
        return val.isoformat()
    return val


def cmd_export(args: argparse.Namespace) -> None:
    """Export listings to CSV or JSON."""
    db = ListingsDB()
    try:
        rows = db.export_listings(category=args.category)
    finally:
        db.close()

    if not rows:
        logger.info("No listings to export.")
        return

    # Determine output path
    os.makedirs("output", exist_ok=True)
    if args.output:
        out_path = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join("output", f"listings_{timestamp}.{args.format}")

    # Serialize datetime values
    for row in rows:
        for key in row:
            row[key] = _serialize_value(row[key])

    if args.format == "csv":
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    else:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, default=str)

    logger.info(f"Exported {len(rows)} listings to {out_path}")


# ── dashboard ──────────────────────────────────────────────────────

def cmd_dashboard(args: argparse.Namespace) -> None:
    """Start an interactive data summary dashboard."""
    polygon_coords = _load_kml(args.kml)
    start_dashboard_server(port=args.port, polygon_coords=polygon_coords)


# ── main ────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "sample":
        cmd_sample(args)
    elif args.command == "extract":
        cmd_extract(args)
    elif args.command == "enrich":
        cmd_enrich(args)
    elif args.command == "contact":
        cmd_contact(args)
    elif args.command == "export":
        cmd_export(args)
    elif args.command == "dashboard":
        cmd_dashboard(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
