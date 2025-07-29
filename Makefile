# Define the base directory for your Python modules
PYTHONPATH := src
PYTHON := python

# Default target
.DEFAULT_GOAL := help

# Setup virtualenv (optional)
venv:
	python -m venv .venv
	source .venv/bin/activate && pip install -r requirements.txt

# Run data ingestion
ingest:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) src/db_utils/run_data_ingestion.py

# Reset the database (drop + re-create schema and views)
reset-db:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) src/db_utils/reset_database.py

# Run tests (if any)
test:
	PYTHONPATH=$(PYTHONPATH) pytest tests/

# Show help
help:
	@echo "🛠️  Available commands:"
	@echo "  make ingest       Run the full data ingestion pipeline"
	@echo "  make reset-db     Reset the PostgreSQL database and rebuild views"
	@echo "  make test         Run tests"
	@echo "  make venv         Create virtual environment and install requirements"
