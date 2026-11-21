# SkyPoints Global Airline Loyalty Data Pipeline

[![Test Suite](https://img.shields.io/badge/pytest-passing-brightgreen.svg)]()
[![Code Architecture](https://img.shields.io/badge/architecture-Snowflake%20%2B%20DuckDB-blue.svg)]()
[![Methodology](https://img.shields.io/badge/methodology-TDD%20%26%20Clean%20Code-orange.svg)]()

> **Incubyte Data Craftsperson Assessment Solution**  
> Designed and built following Software Craftsmanship, Test-Driven Development (TDD), and scalable engineering practices (Python/DuckDB verified by the pytest suite; Snowflake SQL has not yet been executed; run smoke_test.sql and paste results below).

---

## 1. System Architecture & High-Scale Design

The pipeline ingests millions to billions of daily records across two primary feeds:
1. **Member Profile Data**: Pipe-delimited flat file (`|H|` Header, `|D|` Detail records).
2. **Partner Redemption Transactions**: Semi-structured nested JSON feeds.

```mermaid
flowchart TD
    subgraph Ingestion ["1. Scalable Ingestion"]
        FF["Flat Files\n(Pipe-Delimited)"] --> IngestReader["Vectorized Chunk Reader\n(PySpark / DuckDB / Snowpipe)"]
        JF["Partner JSON Feeds\n(Redemptions)"] --> JSONParser["Batch JSON Flattener"]
    end

    subgraph Validation ["2. Quality & Quarantine (DLQ)"]
        IngestReader --> Val{"Validator Engine"}
        Val -->|Valid| Raw["Staging Ingestion"]
        Val -->|Corrupt / Missing Mandatory / Invalid Domain| DLQ["Dead Letter Queue (DLQ)\n(Quarantined with Reason)"]
    end

    subgraph Staging ["3. Staging & Derived Fields"]
        Raw --> StagingTable["stg_member_profile\n+ Age (Exact Calendar)\n+ Stale_Member (Flight_Date > 90d)"]
    end

    subgraph Deduplication ["4. Deduplication & Country Router"]
        StagingTable --> Dedup["Window Function (Latest Event Wins)\nROW_NUMBER() OVER (PARTITION BY source_system, member_id\nORDER BY COALESCE(last_flight_date, enrollment_date) DESC, ingested_at DESC)"]
        Dedup --> Router{"Country Router"}
        Router --> T_IND["table_india"]
        Router --> T_USA["table_usa"]
        Router --> T_CAN["table_canada"]
        Router --> T_PHIL["table_philippines"]
        Router --> T_AU["table_australia"]
    end

    subgraph Analytics ["5. Loyalty 360 Mart"]
        JSONParser --> StgRedemp["stg_redemptions_flattened"]
        StgRedemp --> JoinView["member_loyalty_360\n(Profile + Aggregated Redemption Stats)"]
        Router -.->|members_all view| JoinView
    end
```

---

## 2. Answers to Assessment Deliverables

### Deliverable 1: DDL Queries
Production Snowflake DDLs and local DuckDB schemas are implemented in:
- [`sql/snowflake/ddl.sql`](sql/snowflake/ddl.sql):
  - `RAW_MEMBER_LANDING`: Raw VARCHAR landing table ensuring 100% ingestion reliability.
  - `STG_MEMBER_PROFILE`: Strongly typed staging schema clustered on `(COUNTRY, ENROLLMENT_DATE)`.
  - `TABLE_INDIA`, `TABLE_USA`, `TABLE_CAN`, etc.: Country target tables clustered on `MEMBER_ID`.
  - `RAW_REDEMPTIONS_LANDING`: Semi-structured VARIANT landing table.
  - `STG_REDEMPTIONS_FLATTENED`: Flattened relational transactions table.
  - `MEMBER_LOYALTY_360`: Analytical dimension and fact mart.

### Deliverable 2: Staging Load with Derived Columns (`Age` & `Stale_Member`)
- **`Age`**: Computed dynamically from `DOB` using exact calendar differences (accounting for leap years and month/day progression). Handles 7-digit dropped leading zeros (`3051985` $\rightarrow$ `03051985`).
- **`Stale_Member`**: Computed as `'Y'` when `DATEDIFF(day, Last_Flight_Date, CURRENT_DATE) > 90` or when `Last_Flight_Date` is NULL; otherwise `'N'`.
- **Query-Time Recomputation**: Physical country tables store ingestion-time calculations, while the dynamic view `MEMBERS_ALL_CURRENT` recomputes `Age` and `Stale_Member` dynamically at query time relative to `CURRENT_DATE()`.
- Code: [`src/staging.py`](src/staging.py) | SQL: [`sql/snowflake/transformations.sql`](sql/snowflake/transformations.sql) | DDL: [`sql/snowflake/ddl.sql`](sql/snowflake/ddl.sql)

### Deliverable 3: Country Split & "Latest Record Wins" Rule
- When members move countries, historical records may have older country designations.
- **Resolution**:
  ```sql
  ROW_NUMBER() OVER (
      PARTITION BY source_system, member_id 
      ORDER BY EXTRACT_DATE DESC, COALESCE(last_flight_date, enrollment_date) DESC, ingested_at DESC, line_no DESC
  ) AS rn
  ```
- Only `rn = 1` survives per batch. Staging is incremental and per-batch (`TRUNCATE TABLE STG_MEMBER_PROFILE`).
- MERGE and cross-country DELETE operations are recency-guarded: incoming records only update or migrate members if the incoming extract/event date is strictly newer than the target country row. Older late-arriving files never overwrite or delete newer records.
- Static, drift-free MERGE and cross-country DELETE statements are generated from `CANONICAL_TABLE_MAP` via `scripts/generate_country_merges.py` into `sql/snowflake/generated_country_merges.sql`.
- Code: [`src/country_router.py`](src/country_router.py) | Generator: [`scripts/generate_country_merges.py`](scripts/generate_country_merges.py)

### Deliverable 4: Semi-Structured JSON Redemption Feed & 360 Join
- The nested JSON array of redemptions is parsed and unnested into `stg_redemptions_flattened` (or using Snowflake `LATERAL FLATTEN(input => payload:redemptions)`).
- Joined back to member profile data (`MEMBERS_ALL_CURRENT` unified view) to produce `member_loyalty_360`:
  - `total_redemptions`: Total count of redemption transactions.
  - `total_miles_redeemed`: Sum of only `COMPLETED` miles redeemed.
  - `pending_miles_redeemed`: Sum of unsettled `PENDING` miles tracked separately.
  - `last_redemption_date`: Most recent redemption activity.
  - `top_partner`: Member's most frequently used partner airline (ties broken deterministically by highest count then alphabetical order).
  - Incremental refresh: refreshes redemptions and aggregates only for members touched by the current batch.
- Code: [`src/redemptions.py`](src/redemptions.py)

### Deliverable 5: Data Validations & Dead Letter Queue (DLQ)
Catches dirty data issues visible across the assessment brief and sample datasets:
- **Mandatory Field Checks**: Null checks for `Member_Name`, `Member_Id`, `Enrollment_Date`, and `Country`.
- **Date Sanitation & Hints**: Strict date parsing with source-specific format hints (e.g. USA MMDDYYYY vs AU DDMMYYYY), handling invalid calendar values (e.g. month `13` in `2021-13-13`), text strings `'NULL'`, and dropped leading zeros.
- **Domain & Length Checks**: Validates `Tier_Code` in `('PLT', 'GLD', 'SLV')`, `Is_Active` in `('A', 'I')`, numeric `Post_Code`, and max length limits (18 chars for `Member_Id`, 5 chars for `Country`/`State`/`Tier`). Empty/whitespace strings are sanitized to NULL.
- **Key Collisions & Non-Blocking DQ Warnings**: Resolves name collisions on reused member IDs by keeping latest record and quarantining superseded ones. Emits non-blocking warnings for anomalous ID lengths and high DOB concentration.
- **Dead Letter Queue (DLQ)**: Invalid records are quarantined into a dedicated structure with explicit `error_reason` and timestamps without halting the batch.
- Code: [`src/validators.py`](src/validators.py)

### Deliverable 6: Live Demonstration
The repository includes a ready-to-run live demonstration CLI script:
```bash
python -m src.main
```

---

## 3. Billion-Record Scale Engineering Considerations

| Challenge | Solution at Billions Scale |
| :--- | :--- |
| **Data Ingestion Bottleneck** | Auto-ingest via Snowflake Snowpipe or distributed PySpark chunked readers. Raw landing tables store unparsed strings to avoid load aborts. |
| **Storage & Clustering Strategy** | Target tables are physically split per country (e.g. TABLE_INDIA, TABLE_USA) and clustered on MEMBER_ID. Snowflake micro-partition clustering ensures minimal data scanning during point lookups and analytical aggregations. |
| **Deduplication at Scale** | Micro-batch deduping using micro-partition clustering on `MEMBER_ID` (and `(COUNTRY, ENROLLMENT_DATE)` in staging). Incremental deduplication is constrained to newly ingested micro-batches via watermark filters rather than scanning entire tables. |
| **Fault Tolerance & DLQ** | Non-blocking quarantine pattern (DLQ). Malformed rows never crash daily ingestion; alerted via metrics. |
| **Idempotency** | Target tables populated via deterministic `MERGE INTO` operations keyed on `(SOURCE_SYSTEM, MEMBER_ID)`, making reruns safe without duplicating data. |

---

## 3B. Production Ingestion Architecture (`ingestion.sql`)
The file [`sql/snowflake/ingestion.sql`](sql/snowflake/ingestion.sql) provides production Snowflake DDL templates for staging and loading landing data:
- **Pipe-Delimited Feeds**: Loyalty member files start each record with `|` (making `$1` empty); the `COPY INTO RAW_MEMBER_LANDING` query maps `$2` as `RECORD_TYPE`, `$3` as `MEMBER_NAME`, etc., while capturing `METADATA$FILENAME` into `SOURCE_FILE_NAME` and `METADATA$FILE_ROW_NUMBER` into `LINE_NO`.
- **JSON Redemption Feeds**: Partner redemption files use file format `FF_JSON` with `STRIP_OUTER_ARRAY = TRUE` to ingest both JSON arrays and standalone objects into `RAW_REDEMPTIONS_LANDING`.
- **Continuous Orchestration**: Includes documented templates for serverless Snowpipe `AUTO_INGEST` and Snowflake Change Data Capture (`STREAM` + `TASK`) to trigger transformation batches sequentially upon new file arrival.
*(Note: `ingestion.sql` is a deployment template and has not been executed against a live warehouse).*

---

## 4. Quickstart: Running Tests & Demonstration

### Prerequisites
- Python 3.10+
- Dependencies: `pip install duckdb pytest pandas`

### 1. Run Comprehensive Local Test Suite (TDD + Contracts)
```bash
python -m pytest tests/ -v
```
All **53 unit and integration tests** execute locally against in-memory DuckDB, validating:
- Ingestion parsers (pipe-delimited and CSV with format hints).
- Validation rules, length/domain checks, sanitization, and DLQ quarantine.
- Key collision resolution and deterministic tie-breaking.
- Non-blocking data quality warnings.
- Age (exact calendar differences) and Stale_Member (> 90 days).
- Country routing with latest-record-wins and migration deletes.
- Semi-structured JSON redemption feed parsing without invented data.
- Analytical mart aggregation from country tables without side effects.
- Static generation zero-drift and Snowflake SQL structural contracts.

### 2. Snowflake Warehouse Production Pipeline & Smoke Test
#### Running the Production Pipeline via SnowSQL
The pipeline execution is orchestrated in a unified SnowSQL session via [`sql/snowflake/run_all.sql`](sql/snowflake/run_all.sql):
```sql
!source sql/snowflake/run_all.sql
```
This executes the four production components in strict sequence:
1. [`sql/snowflake/ddl.sql`](sql/snowflake/ddl.sql): Tables, views, and country mapping reference tables.
2. [`sql/snowflake/transformations.sql`](sql/snowflake/transformations.sql): Validation DLQ, staging load, deduplication, and JSON flattening.
3. [`sql/snowflake/generated_country_merges.sql`](sql/snowflake/generated_country_merges.sql): Routing MERGEs and cross-country migration DELETEs.
4. [`sql/snowflake/03_mart_and_finalize.sql`](sql/snowflake/03_mart_and_finalize.sql): Member Loyalty 360 analytical mart aggregation and landing batch finalization.

*Note*: Because `$run_batch_id` is a Snowflake session variable, all four scripts must run within the same session.

#### Behavioral Verification via Smoke Test
To verify the SQL behaviorally against a live Snowflake warehouse (without mock engines):
1. Log into your Snowflake account and open a **Worksheet**.
2. Open [`sql/snowflake/smoke_test.sql`](sql/snowflake/smoke_test.sql) and copy its entire contents into the worksheet.
3. Click **Run All** (execution takes ~1-2 minutes).
4. Review the final assertion result table. It validates all assertions before and after rerun via `run_all.sql`.

#### Smoke Test Results
> **Verification status**: Snowflake SQL has not yet been executed; run sql/snowflake/smoke_test.sql on a Snowflake trial account and paste results below (Date: <DATE>).

| Test ID | Scenario | Expected | Actual | Status |
| :--- | :--- | :--- | :--- | :--- |
| 1 | Member moving country: deleted from old TABLE_USA | 0 | | |
| 2 | Newer extract date move: inserted into new TABLE_INDIA | 1 | | |
| 3 | Composite key: Same ID in IND_SYS and USA_SYS both survive | 2 | | |
| 4 | Rename collision: older name quarantined in DLQ | 1 | | |
| 5 | Rename collision: newer name kept in target table | Alice Green | | |
| 6 | Empty-string tier code: treated as null, not quarantined | 1 | | |
| 7 | Unmapped country: quarantined in DLQ at staging | 1 | | |
| 8 | Null country: quarantined in DLQ at staging | 1 | | |
| 9 | Bad redemption feed_date: quarantined into DLQ | 1 | | |
| 10 | Mart before rerun: MEMBER_LOYALTY_360 total member rows | 5 | | |
| 11 | Mart before rerun: MOVE_101 country updated to IND | 1 | | |
| 12 | Mart before rerun: MOVE_101 COMPLETED miles summed correctly | 5000 | | |
| 13 | Mart before rerun: MOVE_101 PENDING miles tracked separately | 0 | | |
| 14 | Ambiguous member_id redemption: quarantined into DLQ | 1 | | |
| 15 | Rerun delta: STG_MEMBER_PROFILE row count unchanged | 0 | | |
| 16 | Rerun delta: RAW_DLQ_QUARANTINE row count unchanged | 0 | | |
| 17 | Rerun delta: STG_REDEMPTIONS_FLATTENED row count unchanged | 0 | | |
| 18 | Rerun delta: MEMBER_LOYALTY_360 row count unchanged | 0 | | |
| 19 | DLQ reject preservation: two rows with missing member_id both quarantined | 2 | | |
| 20 | DLQ reject preservation: same ID in two source systems both bad both quarantined | 2 | | |
| 21 | DLQ reject preservation: same bad row in two batches yields 2 DLQ rows | 2 | | |
| 22 | Rename across two batches: no DLQ row created | 0 | | |
| 23 | Rename across two batches: newest name wins in target table | Updated Name | | |
| 24 | Ambiguity round trip: redemptions return to mart when ambiguity disappears | 3000 | | |
| 25 | Invalid DOB: quarantined into DLQ | 1 | | |
| 26 | Non-numeric miles: quarantined into DLQ without abort | 1 | | |
| 27 | POST_CODE present in country target table | 40001 | | |
| 28 | Out-of-order older record audited in DLQ as superseded | 1 | | |
| 29 | In-flight landing record processed successfully on subsequent run | 1 | | |

> [!NOTE]
> Automated pytest tests verify SQL contract and syntax structure locally. True behavioral verification (merges, deletes, deduplication, quarantine) is performed directly on a Snowflake warehouse via `sql/snowflake/smoke_test.sql`.

### 3. Run the Live Demonstration Pipeline
```bash
python -m src.main
```
Outputs live formatted tables showing ingestion metrics, DLQ quarantine items, country target tables, and the Member Loyalty 360 mart view.

---

## 5. Known Limitations & Design Assumptions

1. **Member Name vs Member ID as Primary Key**:
   - The assessment brief table marks `Member Name` as `Key Column: Y` and `Member ID` as `Key Column: N`.
   - In airline domain architecture, passenger names are non-unique (homonyms). `Member ID` is the true unique surrogate key. Our architecture treats `Member ID` as the primary key while strictly enforcing `Member Name` as mandatory non-null.
2. **Missing `Is_Active` Defaults to `'A'`**:
   - Feeds with missing or empty active flags default to active status (`'A'`), in alignment with loyalty industry enrollment standards where newly registered members are active by default. Out-of-domain values (other than `'A'` or `'I'`) are quarantined.
3. **Null `Last_Flight_Date` is Flagged Stale (`'Y'`)**:
   - Members with no recorded flight history (`LAST_FLIGHT_DATE IS NULL`) are treated as stale (`> 90 days`), ensuring inactive accounts are highlighted for marketing reactivation.
4. **Mandatory Country & DLQ Routing**:
   - `Country` is treated as mandatory. Records with NULL or unmapped country cannot be routed to any country-specific table and are quarantined to the DLQ (`RAW_DLQ_QUARANTINE`) with explicit error reasons rather than dropping them silently or inventing fallback values.
5. **`EXTRACT_DATE` Derivation & Deterministic Ranking**:
   - `EXTRACT_DATE` is derived from the first 8 consecutive digits (`YYYYMMDD`) in `SOURCE_FILE_NAME`, falling back to `INGESTED_AT::DATE`.
   - Record recency and tie-breaking follow a strict deterministic order: `(EXTRACT_DATE DESC, COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE) DESC, LINE_NO DESC)`. This guarantees reproducible deduplication without relying on random UUIDs or statement-constant ingested timestamps.
6. **Post Code Specification & `NUMBER(5,0)` Representation**:
   - The assessment PDF lists `Post Code (Position 9, INT, N)`, but the sample flat file feed contains 11 pipe-delimited values and omits `Post Code`. Both Snowflake DDL and Python validation support `Post_Code` when present while tolerating feeds where it is omitted.
   - `POST_CODE` is defined as `NUMBER(5,0)` in country target tables; numeric representation strips leading zeros (e.g., Australian or US postcodes such as `0300` are stored as `300`).
7. **Cross-Source System Move Limitation**:
   - Member IDs are scoped by `source_system` (`FLAT_FILE`, `IND`, `USA`, `AUS`) to avoid data loss from ID reuse across different feeds (e.g., IDs `1, 2, 3` appearing in both `IND.csv` and `USA.csv`).
   - Cross-source system moves cannot be tracked automatically without a centralized enterprise master data registry; migration deletes are therefore scoped to within the same `SOURCE_SYSTEM`.
8. **Separate Python and SQL Implementations**:
   - Python/DuckDB and Snowflake SQL are independent implementations of the identical business logic, not a transpiled or unified engine. Schema changes, sanitization rules, and routing conditions are maintained consistently across both.
9. **DuckDB Local Stand-In & Pytest SQL Contract Tests**:
   - For local development, testing, and CI/CD demonstration, DuckDB is utilized as an in-memory embedded OLAP engine.
   - Dialect differences (e.g., Snowflake's `LATERAL FLATTEN` vs DuckDB's `unnest`, session variables, and `!source` directives) mean Snowflake SQL cannot be fully executed by DuckDB locally. Pytest performs comprehensive contract and syntax tests, while full behavioral validation is conducted by running `sql/snowflake/smoke_test.sql` directly on a Snowflake warehouse.
10. **COMPLETED vs PENDING Redemption Miles**:
    - `total_miles_redeemed` reflects cleared mileage balance (`status = 'COMPLETED'`), while `pending_miles_redeemed` tracks unsettled transactions separately to ensure financial accuracy.
11. **`data/AUS.xlsx` Workbook Preservation**:
    - The repository retains `data/AUS.xlsx` as the raw, original assessment spreadsheet artifact, while `data/AUS.csv` serves as the standardized, UTF-8 encoded comma-delimited feed consumed by the automated pipeline.
12. **Generated Country Merges & Zero-Drift SQL**:
    - Static MERGE and DELETE blocks are generated by [`scripts/generate_country_merges.py`](scripts/generate_country_merges.py) into [`sql/snowflake/generated_country_merges.sql`](sql/snowflake/generated_country_merges.sql) to eliminate runtime dynamic SQL / scripting risks while guaranteeing zero drift between Python and Snowflake mappings.
13. **In-Flight Landing Row Claiming (`BATCH_ID`)**:
    - To prevent race conditions from concurrent Snowpipe file ingestion, landing rows are claimed at batch start (`UPDATE RAW_*_LANDING SET BATCH_ID = $run_batch_id WHERE BATCH_ID IS NULL AND PROCESSED_AT IS NULL`). Finalize only marks rows where `BATCH_ID = $run_batch_id`, ensuring in-flight rows arriving mid-run are never accidentally stamped and are safely processed in the next batch.
14. **Dynamic Query-Time Aging & `MEMBER_LOYALTY_360_CURRENT`**:
    - `MEMBER_LOYALTY_360` stores static attributes merged for touched members. To ensure untouched members never experience stale `AGE` or `STALE_MEMBER` flags, the `MEMBER_LOYALTY_360_CURRENT` dynamic view recalculates age and stale status relative to `CURRENT_DATE()` at query time.
15. **Idempotent Schema Evolution for Pre-Existing Schemas**:
    - `sql/snowflake/ddl.sql` includes safe `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migration statements so existing environments can be upgraded seamlessly without dropping tables.

---

## 6. Repository Structure
```text
incubyte-data-craftsperson-assessment/
├── data/
│   ├── sample_member_profile.dat   # Pipe-delimited flat file feed (|H| and |D|)
│   ├── sample_redemptions.json     # Partner airline JSON redemption feed
│   ├── AUS.csv / AUS.xlsx          # Country sample datasets
│   ├── USA.csv                     # Country sample datasets
│   ├── IND.csv                     # Country sample datasets
│   └── DE_Assessment_file.pdf      # Original assessment specification
├── scripts/
│   └── generate_country_merges.py  # Generates static country MERGE & DELETE SQL
├── sql/
│   └── snowflake/
│       ├── ddl.sql                 # Production Snowflake table & view DDLs
│       ├── ingestion.sql           # Snowpipe ingestion DDL, stages & file formats
│       ├── transformations.sql     # Staging derivations, deduplication & JSON flattening
│       ├── generated_country_merges.sql # Generated static country MERGE statements
│       ├── 03_mart_and_finalize.sql# Mart refresh & landing batch finalization
│       ├── run_all.sql             # Unified SnowSQL runner for production pipeline
│       └── smoke_test.sql          # Self-contained live Snowflake verification script
├── src/
│   ├── models.py                   # Dataclasses & schema models
│   ├── validators.py               # Validation rules & DLQ routing
│   ├── flat_file_reader.py         # Pipe-delimited & CSV feed parser
│   ├── staging.py                  # Staging engine (Age & Stale Member)
│   ├── country_router.py           # Country routing & latest record wins
│   ├── redemptions.py              # JSON redemption flattener & 360 mart
│   ├── pipeline.py                 # End-to-end orchestrator
│   └── main.py                     # CLI & live demonstration runner
└── tests/
    ├── test_validation.py          # TDD tests for validation rules & DLQ
    ├── test_staging.py             # TDD tests for Age & Stale Member
    ├── test_country_split.py       # TDD tests for country routing
    ├── test_redemptions.py         # TDD tests for JSON parsing & joins
    ├── test_pipeline.py            # End-to-end integration tests
    └── test_snowflake_sql.py       # SQL contract & structural syntax tests
```
