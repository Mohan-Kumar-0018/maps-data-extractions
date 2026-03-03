# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Google Maps web scraper that extracts business data within user-defined polygon areas. Takes a KML file (via `--kml` flag) and search categories, then runs a 5-step resumable pipeline: add categories, generate grid points, extract listings, enrich details, extract contacts. Stores everything in PostgreSQL.

## Commands

**Always use the Makefile** — never call `venv/bin/python` directly.

```bash
# Setup
python -m venv venv && source venv/bin/activate
make install                    # pip install + playwright install chromium

# Database
make setup-db                   # Run all migrations
make reset-db                   # Drop all tables and recreate (dev only)

# Pipeline (all via make run ARGS="...")
make run ARGS='sample --kml boundary.kml'
make run ARGS='extract --kml boundary.kml --workers 4 --max-results 10'
make run ARGS='extract --kml boundary.kml --workers 4 --live'
make run ARGS='extract --kml boundary.kml --retry-failed --workers 4'
make run ARGS="enrich --workers 4"
make run ARGS='enrich --retry-failed --workers 4'
make run ARGS="contact --workers 4 --limit 100"
make run ARGS='contact --retry-failed --workers 4'
make run ARGS='export --format csv'
make run ARGS='export --format json -o out.json --category "restaurants"'
make export ARGS='--format csv'  # Shorthand for export
make dashboard                  # Interactive summary at http://localhost:8090

# Testing individual records
make test-extract ID=291        # Test extraction for a specific search task ID
make test-enrich ID=42          # Test enrichment for a specific listings row
make test-contact ID=42         # Test contact extraction for a specific row
make test ARGS="..."            # Run test_single.py
make preview ARGS="..."         # Run preview.py

make clean                      # Remove CSV output and __pycache__
```

## Configuration

Database credentials in `config.yml` (priority: `DATABASE_URL` env > `config.yml` > `DB_*` env vars):

```yaml
database:
  host: localhost
  port: 5432
  name: maps_data
  user: postgres
  password: postgres

screenshots: false   # set true to save screenshots during extraction

# Proxy rotation — uncomment and add your proxies
# proxies:
#   - "http://user:pass@proxy1:8080"

# Custom user agents — uncomment to override the default
# user_agents:
#   - "Mozilla/5.0 ..."
```

- `screenshots` (default `false`): when `true`, extraction saves a PNG per search task to `output/screenshots/`.
- `proxies` (optional): list of proxy URLs for round-robin rotation during extraction/enrichment.
- `user_agents` (optional): list of user-agent strings for round-robin rotation (defaults to built-in Chrome UA).

KML polygon boundary file: passed via `--kml` flag (required for `sample`, defaults to `final_file_path.kml` for `extract` and `dashboard`).

## Architecture

```
main.py                  # CLI: sample, extract, enrich, contact, export, dashboard
scraper/
  browser.py             # Playwright automation: search_and_extract(), extract_place_details()
  db.py                  # ListingsDB class — all PostgreSQL operations
  sampler.py             # Adaptive grid sampling + subdivision within polygon
  kml_parser.py          # Parse KML → list of (lat, lng) tuples
  dedup.py               # Thread-safe deduplication + polygon boundary filter
  models.py              # Business dataclass
  website.py             # HTTP-based contact extraction (requests + BeautifulSoup)
  progress.py            # Thread-safe progress tracker for live dashboard
  live_server.py         # Leaflet.js live progress map (during extraction)
  dashboard_server.py    # Post-run interactive summary dashboard
migrations/              # SQL files (002 superseded by 004)
```

### Database Schema

```
categories (id, name)
grid_points (id, lat, lng, zoom, kml_file)      — UNIQUE(lat,lng,zoom,kml_file)
search_tasks                                     — junction table (work items)
  (id, category_id, grid_point_id, status, source, total_results,
   new_count, duplicate_count, out_of_bounds_count, search_url)
listings                                         — extracted businesses
  (..., search_task_id FK, place_id UNIQUE, duplicate_count,
   info_status, contact_status, website_email, website_phone, social_media)
```

### Pipeline & Data Flow

```
1. sample       → KML → grid_points + search_tasks (both idempotent)
2. extract      → pending search tasks → browser search → listings (resumable, adaptive subdivision)
3. enrich       → pending info_status → visit detail pages → phone, website, address, reviews
4. contact      → pending contact_status + has website → crawl site → emails, phones, social links
5. export       → listings → CSV or JSON file in output/
```

Each step is resumable: uses `claim_*()` (atomic `UPDATE WHERE status='pending'`) then `mark_*_done/failed()`. Interrupted runs reset `in_progress` back to `pending` on restart. Use `--retry-failed` on extract/enrich/contact to also reset `failed` back to `pending`.

### Key Design Decisions

- **Junction table**: Geographic points stored once; `search_tasks` tracks (category, point, status) — prevents duplicates and supports adding new categories without re-sampling
- **Claim pattern**: All pipeline steps use atomic status transitions (`pending → in_progress → done/failed`) for thread-safe concurrent workers
- **Adaptive subdivision**: During extraction, high-yield points (new_count >= threshold) spawn sub-points at higher zoom, queued as new pending search tasks
- **One browser per worker**: Each thread launches its own Playwright Chromium instance (browser.py)
- **Contact extraction uses requests**: `website.py` crawls homepage + up to 2 contact/about pages via HTTP (no browser needed), unlike extraction/enrichment which use Playwright
- **Staggered starts**: 2-second delay per worker to reduce detection risk
- **Multiple CSS selector fallbacks**: Each browser extraction field tries several selectors
- **Bulk DB inserts**: `insert_grid_points()` and `create_search_tasks()` use `psycopg2.extras.execute_values` for single-round-trip bulk operations
- **Proxy rotation**: Optional round-robin proxy and user-agent rotation via `config.yml`, thread-safe with locks
- **Data quality dashboard**: Field completeness bars and per-category completeness table in the dashboard

## Dev Workflow

- **Migrations**: No incremental migration files during dev — edit the original migration SQL and run `make reset-db`
