.PHONY: test demo gen-sql check-drift install help

help:
	@echo "Available commands:"
	@echo "  make test        - Run automated pytest suite across unit, integration, and SQL contract tests"
	@echo "  make demo        - Execute SkyPoints pipeline live demonstration"
	@echo "  make gen-sql     - Generate static Snowflake country merge SQL from Python script"
	@echo "  make check-drift - Verify generated Snowflake SQL matches generate_country_merges.py"
	@echo "  make install     - Install package and development dependencies in editable mode"

install:
	pip install -e ".[dev]"

test:
	python -m pytest -q

demo:
	python -m skypoints.cli

gen-sql:
	python scripts/generate_country_merges.py

check-drift:
	python scripts/generate_country_merges.py
	git diff --exit-code sql/snowflake/04_country_merges.generated.sql
