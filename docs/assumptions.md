# SkyPoints Pipeline Assumptions & Known Limitations

This document details the domain assumptions, data sanitization heuristics, and operational boundaries adopted throughout the SkyPoints pipeline.

---

## 1. Documented Assumptions

### 1.1 Ingestion & Extract Date Resolution
- **Feed Date vs Flight Date**: "Latest wins" recency ranking prioritizes feed extraction date (`EXTRACT_DATE`), not flight date or enrollment date.
- **Filename Convention**: `EXTRACT_DATE` is extracted by parsing the first 8-digit sequence (`YYYYMMDD`) from `SOURCE_FILE_NAME`. If no date is present in the filename, it falls back to the calendar date of `INGESTED_AT` (UTC).
- **Line Ordering**: When multiple records share the exact same `EXTRACT_DATE`, the pipeline tie-breaks deterministically using `COALESCE(last_flight_date, enrollment_date) DESC`, then `SOURCE_FILE_NAME DESC`, and finally `LINE_NO DESC`.

### 1.2 Country Code Canonicalization
The pipeline maps legacy and regional country representations to 5 canonical target codes:
- `IND` (Aliases: `INDIA`) $\rightarrow$ `TABLE_INDIA`
- `USA` $\rightarrow$ `TABLE_USA`
- `CAN` (Aliases: `CANADA`) $\rightarrow$ `TABLE_CAN`
- `PHIL` (Aliases: `PH`) $\rightarrow$ `TABLE_PHIL`
- `AU` (Aliases: `AUS`) $\rightarrow$ `TABLE_AU`

**Policy on Unmapped/Missing Countries**:
Any record with an empty, null, or unmapped country code (e.g., `GBR`, `DEU`, `UNKNOWN`) is rejected during staging and routed to `RAW_DLQ_QUARANTINE` under reason `'Unmapped country: Country is required and cannot be empty'`.

### 1.3 Key Collisions vs Cross-Batch Updates
- **Intra-Batch Collision**: When a batch contains two or more records sharing `(source_system, member_id)` with different names, this represents conflicting identities in one delivery. The newest record is admitted to staging, and earlier conflicting records are quarantined in `RAW_DLQ_QUARANTINE` (`'Key collision: superseded by newer record'`).
- **Cross-Batch Updates**: A name change across different batches (e.g. marriage or legal name update) is treated as a valid update to core tables. No DLQ reject is generated.

### 1.4 Ambiguous Member Redemptions
- **Scope of Ambiguity**: A `member_id` is classified as ambiguous when the same identifier exists across two or more distinct `SOURCE_SYSTEM` values (e.g. `101` in `FLAT_FILE` and `101` in `USA_LEGACY`).
- **Non-Destructive Flagging**: Ambiguous transactions are flagged with `IS_AMBIGUOUS = TRUE` in `STG_REDEMPTIONS_FLATTENED` and logged to DLQ. They are **never deleted**. When a subsequent batch clarifies identity (or eliminates cross-source conflicts), `IS_AMBIGUOUS` is reset to `FALSE` and the transactions rejoin analytical calculations.

### 1.5 Analytical Calculations
- **Member Age**: Computed using standard calendar age calculation (`year_diff - ((cur_month, cur_day) < (dob_month, dob_day))`). Future birthdates return `NULL`.
- **Stale Member**: Flagged `'Y'` if the member has not flown in more than 90 days (`reference_date - last_flight_date > 90`), or if `last_flight_date` is `NULL`. Otherwise flagged `'N'`.
- **Top Partner**: Identified by highest count of redemption transactions. Ties are broken alphabetically by partner name.
- **Miles Aggregation**: Only redemptions with status `'COMPLETED'` are aggregated into `total_miles_redeemed`. Status `'PENDING'` transactions are tracked separately under `pending_miles_redeemed` and not credited toward redeemed balances.

---

## 2. Known Limitations & Operational Boundaries

1. **Snowflake Execution Boundary**: The Python / DuckDB implementation is comprehensively verified by automated test suites. The Snowflake implementation (`sql/snowflake/`) contains production-ready DDL and SQL scripts configured for live SnowSQL execution; Snowflake warehouse execution is verified via `smoke_test.sql` directly by the warehouse administrator.
2. **Date Timezone Standardization**: All timestamps (`INGESTED_AT`, `PROCESSED_AT`, `LAST_UPDATED_AT`) are stored in `TIMESTAMP_NTZ` / UTC. Daylight saving time adjustments are assumed to be handled upstream by feed providers.
3. **Out-of-Order Feed Ingestion**: In rare scenarios where older feeds are re-delivered after newer feeds have already updated core tables, the pipeline's MERGE conditions (`src.EXTRACT_DATE > target.EXTRACT_DATE`) safely prevent regressions while auditing older records to DLQ.
