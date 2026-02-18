VENV := venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: install run preview test test-enrich test-contact clean setup-db reset-db

install:
	$(PIP) install -r requirements.txt
	$(VENV)/bin/playwright install chromium

run: install
	$(PYTHON) main.py $(ARGS)

preview: install
	$(PYTHON) preview.py $(ARGS)

test: install
	$(PYTHON) test_single.py $(ARGS)

DB_URL := $(shell $(PYTHON) -c "import yaml; c=yaml.safe_load(open('config.yml'))['database']; print(f\"postgresql://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['name']}\")")

setup-db:
	psql "$(DB_URL)" -f migrations/001_create_places_info.sql
	psql "$(DB_URL)" -f migrations/002_create_sample_points.sql
	psql "$(DB_URL)" -f migrations/003_create_categories.sql
	psql "$(DB_URL)" -f migrations/004_normalize_sample_points.sql

reset-db:
	psql "$(DB_URL)" -c "DROP TABLE IF EXISTS category_sample_point_mappings CASCADE;"
	psql "$(DB_URL)" -c "DROP TABLE IF EXISTS sample_points CASCADE;"
	psql "$(DB_URL)" -c "DROP TABLE IF EXISTS places_info CASCADE;"
	psql "$(DB_URL)" -c "DROP TABLE IF EXISTS categories CASCADE;"
	psql "$(DB_URL)" -f migrations/001_create_places_info.sql
	psql "$(DB_URL)" -f migrations/003_create_categories.sql
	psql "$(DB_URL)" -f migrations/004_normalize_sample_points.sql

test-enrich: install
	@$(PYTHON) -c "\
import sys; \
from scraper.db import PlacesDB; \
from scraper.browser import extract_place_details; \
row_id = int('$(ID)'); \
db = PlacesDB(); \
cur = db._conn.cursor(); \
cur.execute('SELECT google_maps_url FROM places_info WHERE id = %s', (row_id,)); \
row = cur.fetchone(); \
assert row, f'No places_info row with id={row_id}'; \
url = row[0]; \
print(f'URL: {url}'); \
details = extract_place_details(url); \
print(f'total_reviews: {details[\"total_reviews\"]}'); \
print(f'phone: {details[\"phone\"]}'); \
print(f'website: {details[\"website\"]}'); \
db.close()"

test-contact: install
	@$(PYTHON) -c "\
import sys; \
from scraper.db import PlacesDB; \
from scraper.website import extract_website_contacts; \
row_id = int('$(ID)'); \
db = PlacesDB(); \
cur = db._conn.cursor(); \
cur.execute('SELECT website FROM places_info WHERE id = %s', (row_id,)); \
row = cur.fetchone(); \
assert row, f'No places_info row with id={row_id}'; \
url = row[0]; \
assert url, f'Row id={row_id} has no website'; \
print(f'Website: {url}'); \
result = extract_website_contacts(url); \
print(f'Emails: {result[\"emails\"]}'); \
print(f'Phones: {result[\"phones\"]}'); \
print(f'Social: {result[\"social_media\"]}'); \
db.close()"

clean:
	rm -rf output/*.csv
	find . -type d -name __pycache__ -exec rm -rf {} +
