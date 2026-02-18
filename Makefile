VENV := venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: install run preview test clean setup-db

install:
	$(PIP) install -r requirements.txt
	$(VENV)/bin/playwright install chromium

run: install
	$(PYTHON) main.py $(ARGS)

preview: install
	$(PYTHON) preview.py $(ARGS)

test: install
	$(PYTHON) test_single.py $(ARGS)

setup-db:
	psql "$(DATABASE_URL)" -f migrations/001_create_places_info.sql

clean:
	rm -rf output/*.csv
	find . -type d -name __pycache__ -exec rm -rf {} +
