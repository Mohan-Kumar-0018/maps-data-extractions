# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Google Maps web scraper that extracts business data within user-defined polygon areas. Takes a KML file (polygon boundary) and a search category, then outputs a CSV of business listings. Uses Playwright (Chromium) for browser automation, Shapely for polygon geometry, and pyproj for coordinate transforms.

## Usage

```bash
pip install -r requirements.txt
playwright install chromium

python main.py --kml boundary.kml --category "restaurants" --workers 4 --output results.csv
python main.py --kml area.kml --category "hospitals" --max-results 500
```

Defaults: `--workers 4`, `--output output/results.csv`, `--max-results 200`

## Dependencies

- `playwright` — browser automation (Chromium, headless)
- `shapely` — polygon geometry and point-in-polygon checks
- `pyproj` — coordinate transformations for area calculation

## Architecture

```
main.py                  # CLI entry point (argparse), orchestration pipeline
scraper/
  models.py              # Business dataclass, CSV header/row helpers
  kml_parser.py          # Parse KML → list of (lat, lng) tuples
  sampler.py             # Adaptive grid sampling within polygon
  browser.py             # Playwright automation: search, scroll, extract
  dedup.py               # Thread-safe deduplication + polygon boundary filter
output/                  # Default CSV output directory
```

### Data Flow

```
KML file → kml_parser.parse() → polygon coords
  → sampler.generate_sample_points() → search points + zoom level
  → ThreadPoolExecutor:
      browser.search_and_extract(point, category) → List[Business]
  → dedup.filter_businesses() → deduplicated + in-polygon only
  → CSV writer → output file
```

### Key Design Decisions

- **One browser per worker**: Each thread launches its own Playwright Chromium instance
- **Adaptive density**: Sample point count scales with polygon area (1–16 points)
- **Staggered starts**: 2-second delay per worker to reduce detection risk
- **Multiple CSS selector fallbacks**: Each extraction field tries several selectors
- **Thread-safe dedup**: `threading.Lock` around place ID set for concurrent workers
