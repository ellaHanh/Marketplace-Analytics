.PHONY: install run test clean reset-db

PYTHON := python3
PIP    := pip3
DB_URL ?= $(shell grep DATABASE_URL .env 2>/dev/null | cut -d= -f2-)

install:
	$(PIP) install -r requirements.txt

run:
	$(PYTHON) main.py

reset-db:
	$(PYTHON) -c "from src.db import reset_database; reset_database()"

test:
	pytest tests/ -v --tb=short

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.log" -delete
