VENV := venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: install run preview test test-extract test-enrich test-contact clean setup-db reset-db dashboard

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
	psql "$(DB_URL)" -c "DROP TABLE IF EXISTS search_tasks CASCADE;"
	psql "$(DB_URL)" -c "DROP TABLE IF EXISTS grid_points CASCADE;"
	psql "$(DB_URL)" -c "DROP TABLE IF EXISTS listings CASCADE;"
	psql "$(DB_URL)" -c "DROP TABLE IF EXISTS categories CASCADE;"
	psql "$(DB_URL)" -f migrations/001_create_places_info.sql
	psql "$(DB_URL)" -f migrations/003_create_categories.sql
	psql "$(DB_URL)" -f migrations/004_normalize_sample_points.sql

test-extract: install
	@$(PYTHON) -c "\
import sys, os; \
from scraper.db import ListingsDB; \
from scraper.browser import search_and_extract; \
search_task_id = int('$(ID)'); \
db = ListingsDB(); \
cur = db._conn.cursor(); \
cur.execute('SELECT t.id, gp.lat, gp.lng, gp.zoom, c.name FROM search_tasks t JOIN grid_points gp ON gp.id = t.grid_point_id JOIN categories c ON c.id = t.category_id WHERE t.id = %s', (search_task_id,)); \
row = cur.fetchone(); \
assert row, f'No task with id={search_task_id}'; \
tid, lat, lng, zoom, category = row; \
print(f'Task {tid}: ({lat}, {lng}) zoom={zoom} [{category}]'); \
print(f'Searching...'); \
ss_dir = os.path.join('output', 'screenshots'); \
os.makedirs(ss_dir, exist_ok=True); \
ss_path = os.path.join(ss_dir, f'{search_task_id}.png'); \
results, url = search_and_extract(lat, lng, category, zoom, max_results=10, on_extract=lambda biz: print(f'  {biz.name} | {biz.place_id} | ({biz.latitude}, {biz.longitude})'), screenshot_path=ss_path); \
print(f'\nTotal results: {len(results)}'); \
print(f'Search URL: {url}'); \
print(f'Screenshot: {ss_path}'); \
db.close()"

test-enrich: install
	@$(PYTHON) -c "\
import sys; \
from scraper.db import ListingsDB; \
from scraper.browser import extract_place_details; \
row_id = int('$(ID)'); \
db = ListingsDB(); \
cur = db._conn.cursor(); \
cur.execute('SELECT google_maps_url FROM listings WHERE id = %s', (row_id,)); \
row = cur.fetchone(); \
assert row, f'No listings row with id={row_id}'; \
url = row[0]; \
print(f'URL: {url}'); \
details = extract_place_details(url); \
print(f'total_reviews: {details[\"total_reviews\"]}'); \
print(f'phone: {details[\"phone\"]}'); \
print(f'website: {details[\"website\"]}'); \
print(f'address: {details[\"address\"]}'); \
db.close()"

test-contact: install
	@$(PYTHON) -c "\
import sys; \
from scraper.db import ListingsDB; \
from scraper.website import extract_website_contacts; \
row_id = int('$(ID)'); \
db = ListingsDB(); \
cur = db._conn.cursor(); \
cur.execute('SELECT website FROM listings WHERE id = %s', (row_id,)); \
row = cur.fetchone(); \
assert row, f'No listings row with id={row_id}'; \
url = row[0]; \
assert url, f'Row id={row_id} has no website'; \
print(f'Website: {url}'); \
result = extract_website_contacts(url); \
print(f'Emails: {result[\"emails\"]}'); \
print(f'Phones: {result[\"phones\"]}'); \
print(f'Social: {result[\"social_media\"]}'); \
db.close()"

dashboard: install
	$(PYTHON) main.py dashboard $(ARGS)

clean:
	rm -rf output/*.csv
	find . -type d -name __pycache__ -exec rm -rf {} +
