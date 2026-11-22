# SkyPoints Airline Loyalty ETL Pipeline

[![Test Suite](https://img.shields.io/badge/pytest-66%20passed-brightgreen.svg)]()
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)]()
[![Snowflake Smoke Tests](https://img.shields.io/badge/Snowflake%20Smoke%20Tests-34%2F34%20Passed-brightgreen.svg)]()
[![Architecture](https://img.shields.io/badge/architecture-Modular%20Layered-purple.svg)]()

> **Incubyte Data Craftsperson Assessment Solution**  
> Production-grade loyalty data engineering pipeline designed with Software Craftsmanship, Test-Driven Development (TDD), and strict parity between Python/DuckDB local execution and Snowflake cloud data warehouse SQL.

---

## Quickstart & Execution

```bash
# 1. Install dependencies in editable mode
make install

# 2. Run test suite (66 unit, integration, and SQL contract tests)
make test

# 3. Run live pipeline demonstration
make demo

# 4. Generate Snowflake country merges & check SQL drift
make gen-sql
make check-drift
```

---

## Repository Structure

```text
skypoints-pipeline/
├── README.md                     # Project overview and quickstart
├── pyproject.toml                # Build configuration and pytest settings
├── Makefile                      # make test | demo | gen-sql | check-drift
├── .github/workflows/ci.yml      # CI workflow for multi-python testing and SQL drift
├── docs/
│   ├── assessment/DE_Assessment_file.pdf   # Original assessment specification
│   ├── architecture.md           # End-to-end design, flow diagrams, and scale design
│   ├── assumptions.md            # Documented assumptions, edge cases, and limits
│   └── snowflake-runbook.md      # SnowSQL runbook and smoke test verification matrix
├── data/
│   ├── raw/                      # Company originals: IND.csv, USA.csv, AUS.xlsx
│   ├── samples/                  # sample_member_profile.dat, sample_redemptions.json
│   └── derived/                  # Derived CSV fallbacks (e.g. AUS.csv from AUS.xlsx)
├── sql/snowflake/
│   ├── 01_ddl.sql                # DDL for landing, staging, country tables, DLQ, and views
│   ├── 02_ingestion.sql          # Ingestion template (stages, formats, COPY INTO, streams)
│   ├── 03_transformations.sql    # Validation, batch claiming, and deduplication (Steps 1-4)
│   ├── 04_country_merges.generated.sql # Generated incremental country table merges
│   ├── 05_mart_and_finalize.sql  # Loyalty 360 mart, ambiguity handling, and finalization
│   ├── run_all.sql               # Sourced orchestration script for SnowSQL
│   └── tests/smoke_test.sql      # Comprehensive Snowflake smoke test suite
├── scripts/
│   ├── generate_country_merges.py # Deterministic SQL merge code generator
│   └── run_snowflake_smoke_test.py # Live Snowflake smoke test automated runner
├── src/skypoints/
│   ├── models.py                 # Strongly typed dataclasses
│   ├── pipeline.py               # End-to-end orchestrator
│   ├── cli.py                    # Interactive demo runner (python -m skypoints.cli)
│   ├── ingestion/                # Flat file, CSV, and XLSX readers
│   ├── validation/               # Validation rules, domain checks, and DQ warnings
│   ├── transform/                # Staging engine and country router
│   └── redemptions/              # JSON redemption parser and analytical join engine
└── tests/
    ├── unit/                     # TDD unit tests for validation, router, and redemptions
    ├── integration/              # End-to-end pipeline execution tests
    ├── sql_contract/             # Automated Snowflake SQL contract verification
    └── fixtures/                 # Shared test fixtures and sample data
```

---

## Core Architecture & Engineering Highlights

- **Modular Python Package**: Organized cleanly into `src/skypoints/` with subpackages for ingestion, validation, transformation, and redemptions, plus backward-compatibility shims.
- **Idempotent Batch Processing**: Uses session-scoped `$run_batch_id` with atomic row claiming. In-flight records and mid-run interruptions are automatically reclaimed without data loss or duplicate DLQ entries.
- **Extract Date Recency**: Dedupes records deterministically using feed `EXTRACT_DATE DESC`, then `COALESCE(flight_date, enrollment_date) DESC`, then `SOURCE_FILE_NAME DESC`, then `LINE_NO DESC`.
- **Non-Destructive Ambiguity**: Redemptions for members spanning multiple source systems are flagged `IS_AMBIGUOUS = TRUE` and quarantined rather than deleted; when ambiguity clears, transactions rejoin the analytical mart.
- **Loyalty 360 Mart**: Incremental MERGE updates only touched members, aggregating completed miles separately from pending miles, and identifying top partners with alphabetical tie-breaking.

---

## Documentation Links

- [Assessment Specification](docs/assessment/DE_Assessment_file.pdf)
- [Architecture & Scale Design](docs/architecture.md)
- [Assumptions & Known Limitations](docs/assumptions.md)
- [Snowflake Runbook & Smoke Test Results](docs/snowflake-runbook.md)
