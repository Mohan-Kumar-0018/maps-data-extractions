# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Google Maps web scraper that extracts business data within user-defined polygon areas. Takes a KML file (polygon boundary) and search categories, then stores business listings in PostgreSQL. Uses Playwright (Chromium) for browser automation, Shapely for polygon geometry, and pyproj for coordinate transforms.

## Usage

```bash
pip install -r requirements.txt
playwright install chromium

# Define categories to search
python main.py add-category "restaurants" "hospitals" "schools"
python main.py list-categories

# Generate sample points for all categories (or one with --category)
python main.py sample --kml boundary.kml
python main.py sample --kml boundary.kml --category "restaurants"

# Extract businesses — all categories or one (resumable)
python main.py extract --workers 4 --max-results 10
python main.py extract --category "restaurants" --workers 4
python main.py extract --live --kml boundary.kml

# Re-run after interruption — picks up where it left off
python main.py extract --workers 4
```

Defaults: `--workers 4`, `--max-results 10`

## Dependencies

- `playwright` — browser automation (Chromium, headless)
- `shapely` — polygon geometry and point-in-polygon checks
- `pyproj` — coordinate transformations for area calculation
- `psycopg2` — PostgreSQL driver
- `pyyaml` — config file parsing

## Architecture

```
main.py                  # CLI entry point: add-category, list-categories, sample, extract
scraper/
  models.py              # Business dataclass, CSV header/row helpers
  kml_parser.py          # Parse KML → list of (lat, lng) tuples
  sampler.py             # Adaptive grid sampling within polygon
  browser.py             # Playwright automation: search, scroll, extract
  dedup.py               # Thread-safe deduplication + polygon boundary filter
  db.py                  # PostgreSQL storage (places_info, sample_points, mappings, categories)
  progress.py            # Thread-safe progress tracker for live dashboard
  live_server.py         # Background HTTP server serving Leaflet.js dashboard
migrations/
  001_create_places_info.sql
  002_create_sample_points.sql   # (superseded by 004)
  003_create_categories.sql
  004_normalize_sample_points.sql  # Junction table schema
output/                  # Default CSV output directory
```

### Database Schema

```
categories (id, name)                           — search categories
sample_points (id, lat, lng, zoom, kml_file)    — pure geography, UNIQUE(lat,lng,zoom,kml_file)
category_sample_point_mappings                  — junction table (work items)
  (id, category_id FK, sample_point_id FK, status)
  UNIQUE(category_id, sample_point_id)
places_info (..., mapping_id FK)                — extracted businesses
```

### Data Flow

```
Setup:
  add-category "restaurants" "hospitals" → rows in categories table

Step 1 — sample (idempotent):
  KML file → kml_parser.parse() → polygon coords
    → sampler.generate_sample_points() → search points + zoom level
    → db.insert_sample_points() → N rows in sample_points (ON CONFLICT DO NOTHING)
    → for each category: db.create_mappings() → N mapping rows (ON CONFLICT DO NOTHING)
  Re-running is safe — no duplicates created.

Step 2 — extract (resumable):
  db.fetch_pending_mappings(category?)
    → for each mapping: db.claim_mapping() → search_and_extract(category) → db.mark_mapping_done()
    → businesses filtered by polygon → db.insert_business(biz, mapping_id)
  If interrupted, re-run picks up remaining pending mappings.
```

### Key Design Decisions

- **Multi-category**: Categories stored in DB table; sample/extract operate on all categories by default
- **Junction table**: Geographic points stored once; `category_sample_point_mappings` tracks (category, point, status) work items — prevents duplicates on re-run and supports adding new categories without re-sampling
- **Two-step pipeline**: Sample and extract are separate commands; extraction is resumable via mapping status tracking
- **One browser per worker**: Each thread launches its own Playwright Chromium instance
- **Adaptive density**: Sample point count scales with polygon area (1–16 points)
- **Staggered starts**: 2-second delay per worker to reduce detection risk
- **Multiple CSS selector fallbacks**: Each extraction field tries several selectors
- **Thread-safe dedup**: `threading.Lock` around place ID set for concurrent workers
