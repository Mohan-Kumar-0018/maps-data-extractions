# Google Maps Data Extraction

Scrapes Google Maps business data within a polygon boundary defined by a KML file. Uses Playwright (headless Chromium) for browser automation, with a multi-step resumable pipeline that stores everything in PostgreSQL.

## Setup

```bash
python -m venv venv
source venv/bin/activate
make install
```

Configure database credentials in `config.yml`:

```yaml
database:
  host: localhost
  port: 5432
  name: maps
  user: postgres
  password: ""
```

Create the database tables:

```bash
make setup-db
```

## Pipeline

Each step is resumable — if interrupted, re-run the same command to pick up where it left off.

### 1. Sample

Generate search points within the KML polygon boundary. Creates grid points and search tasks for all categories in the DB:

```bash
make run ARGS='sample --kml riyadh-final-map.kml'
```

### 2. Extract

Search Google Maps at each grid point and extract business listings. Results are filtered by the polygon boundary and deduplicated by `place_id`:

```bash
# Extract all categories (4 parallel browsers, max 10 results per point)
make run ARGS='extract --kml riyadh-final-map.kml --workers 4 --max-results 10'

# With live progress map at http://localhost:8080
make run ARGS='extract --kml riyadh-final-map.kml --workers 4 --live'
```

Each task logs a breakdown: `20 raw → 3 new, 12 duplicates, 5 filtered out`

Screenshots are saved to `output/screenshots/{search_task_id}.png`.

### 3. Enrich

Visit each business's Google Maps detail page to extract phone, website, address, and review count:

```bash
make run ARGS="enrich --workers 4"

# Limit to N places
make run ARGS="enrich --workers 4 --limit 100"
```

### 4. Contact

Extract emails, phone numbers, and social media links from business websites:

```bash
make run ARGS="contact --workers 4"

# Limit to N places
make run ARGS="contact --workers 4 --limit 100"
```

## Dashboard

View a summary of all pipeline data with an interactive map:

```bash
make dashboard
```

Opens a browser at http://localhost:8090 with:
- Stat cards (grid points, businesses, enriched, contacts)
- Pipeline funnel chart
- Category breakdown table
- Interactive map with color-coded grid points (filterable by category)
- Duplicate analysis
- Zero-result points

## Testing Individual Records

```bash
# Test extraction for a specific search task ID
make test-extract ID=291

# Test enrichment for a specific listings row
make test-enrich ID=42

# Test contact extraction for a specific listings row
make test-contact ID=42
```

## Other Commands

```bash
make reset-db    # Drop and recreate all tables
make clean       # Remove CSV output and __pycache__
```
