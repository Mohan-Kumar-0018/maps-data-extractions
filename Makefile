VENV := venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: install run preview test clean setup-db reset-db

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

reset-db:
	psql "$(DB_URL)" -c "DROP TABLE IF EXISTS places_info CASCADE;"
	psql "$(DB_URL)" -f migrations/001_create_places_info.sql

clean:
	rm -rf output/*.csv
	find . -type d -name __pycache__ -exec rm -rf {} +
