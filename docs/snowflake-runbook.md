# Snowflake Runbook & Smoke Test Verification

This runbook provides the operational instructions for deploying and running the SkyPoints loyalty pipeline in Snowflake via SnowSQL.

---

## 1. Script Architecture & Execution Order

All Snowflake SQL scripts reside in [`sql/snowflake/`](../sql/snowflake/). Because the pipeline uses session variables (notably `$run_batch_id`), all transformation scripts must be executed in a single, contiguous session.

| Step | Script File | Purpose & Action |
| :---: | :--- | :--- |
| **01** | [`01_ddl.sql`](../sql/snowflake/01_ddl.sql) | Creates landing tables, staging tables, country tables, DLQ quarantine, views, and reference mappings. |
| **02** | [`02_ingestion.sql`](../sql/snowflake/02_ingestion.sql) | Production template declaring external stages, file formats, and `COPY INTO` streams (template only; not executed in smoke test). |
| **03** | [`03_transformations.sql`](../sql/snowflake/03_transformations.sql) | Initializes session `$run_batch_id`, claims landing rows, runs member/redemption validation, DLQ routing, and deduplication. |
| **04** | [`04_country_merges.generated.sql`](../sql/snowflake/04_country_merges.generated.sql) | Deterministically generated incremental MERGEs for `TABLE_INDIA`, `TABLE_USA`, `TABLE_AU`. |
| **05** | [`05_mart_and_finalize.sql`](../sql/snowflake/05_mart_and_finalize.sql) | Flags ambiguous redemptions, builds `MEMBER_LOYALTY_360` mart, and marks claimed landing rows as processed. |

To execute the entire pipeline end-to-end in one command:
```bash
snowsql -c <your_connection> -f sql/snowflake/run_all.sql
```

---

## 2. Smoke Test Execution Guide

The smoke test suite validates end-to-end data integrity, boundary handling, and idempotent rerun guarantees in an isolated schema `SKYPOINTS_SMOKE_TEST`.

### Running the Smoke Test

#### Method A: Via Automated Python Runner (Recommended & Zero Setup)
The automated Python runner connects directly to Snowflake using credentials from environment variables (or `.env`), resolves all SQL dependencies in memory, executes all 4 pipeline test stages, and outputs the full assertion table:
```bash
python scripts/run_snowflake_smoke_test.py
```

#### Method B: Via SnowSQL CLI
Execute the official smoke test file directly using SnowSQL (which natively executes `!source` directives):
```bash
snowsql -c <your_connection> -f sql/snowflake/tests/smoke_test.sql
```

The smoke test executes the following automated workflow:
1. Provisions an isolated schema `SKYPOINTS_SMOKE_TEST`.
2. Populates synthetic edge-case data across members and redemptions.
3. Sources `sql/snowflake/run_all.sql` for **Run 1**.
4. Asserts data accuracy in core country tables, DLQ quarantine, and `MEMBER_LOYALTY_360`.
5. Takes a snapshot of table counts (`PRE_RERUN_SNAPSHOT`).
6. Sources `sql/snowflake/run_all.sql` for **Run 2 (Rerun Idempotency)**.
7. Asserts zero row count drift across staging, DLQ, and mart tables.
8. Produces a tabular test results summary.

---

## 3. Smoke Test Results Verification Matrix

> [!NOTE]
> **Live Warehouse Execution Verified (34/34 Passed)**: All 34 test scenarios below were executed and verified against a live Snowflake warehouse (`ACCOUNT: HWEAQUL-UO73138`, `WAREHOUSE: COMPUTE_WH`, `SCHEMA: SKYPOINTS_SMOKE_TEST`) via `scripts/run_snowflake_smoke_test.py` and `sql/snowflake/tests/smoke_test.sql`.

| Test ID | Scenario Name | Category | Expected Result | Actual Result | Verified Status |
| :---: | :--- | :--- | :---: | :---: | :---: |
| 1 | Member moving country: deleted from old TABLE_USA | Country Migration | 0 | 0 | ✅ PASS |
| 2 | Newer extract date move: inserted into new TABLE_INDIA | Country Migration | 1 | 1 | ✅ PASS |
| 3 | Composite key: Same ID in IND_SYS and USA_SYS both survive | Multi-Source Key | 2 | 2 | ✅ PASS |
| 4 | Rename collision: older name quarantined in DLQ | Intra-Batch Collision | 1 | 1 | ✅ PASS |
| 5 | Rename collision: newer name kept in target table | Intra-Batch Collision | Alice Green | Alice Green | ✅ PASS |
| 6 | Empty-string tier code: treated as null, not quarantined | Data Cleansing | 1 | 1 | ✅ PASS |
| 7 | Unmapped country: quarantined in DLQ at staging | Data Quality Gate | 1 | 1 | ✅ PASS |
| 8 | Null country: quarantined in DLQ at staging | Data Quality Gate | 1 | 1 | ✅ PASS |
| 9 | Bad redemption feed_date: quarantined into DLQ | Redemptions Gate | 1 | 1 | ✅ PASS |
| 10 | Mart before rerun: MEMBER_LOYALTY_360 total member rows | Analytical Mart | 10 | 10 | ✅ PASS |
| 11 | Mart before rerun: MOVE_101 country updated to IND | Analytical Mart | 1 | 1 | ✅ PASS |
| 12 | Mart before rerun: MOVE_101 COMPLETED miles summed correctly | Miles Calculation | 5000 | 5000 | ✅ PASS |
| 13 | Mart before rerun: MOVE_101 PENDING miles tracked separately | Miles Calculation | 0 | 0 | ✅ PASS |
| 14 | Ambiguous member_id redemption: quarantined into DLQ | Redemptions Ambiguity | 1 | 1 | ✅ PASS |
| 15 | Rerun delta: STG_MEMBER_PROFILE row count unchanged | Idempotent Rerun | 0 | 0 | ✅ PASS |
| 16 | Rerun delta: RAW_DLQ_QUARANTINE row count unchanged | Idempotent Rerun | 0 | 0 | ✅ PASS |
| 17 | Rerun delta: STG_REDEMPTIONS_FLATTENED row count unchanged | Idempotent Rerun | 0 | 0 | ✅ PASS |
| 18 | Rerun delta: MEMBER_LOYALTY_360 row count unchanged | Idempotent Rerun | 0 | 0 | ✅ PASS |
| 19 | DLQ reject preservation: two rows with missing member_id both quarantined | DLQ Uniqueness | 2 | 2 | ✅ PASS |
| 20 | DLQ reject preservation: same ID in two source systems both bad both quarantined | DLQ Uniqueness | 2 | 2 | ✅ PASS |
| 21 | DLQ reject preservation: same bad row in two batches yields 2 DLQ rows | DLQ Uniqueness | 2 | 2 | ✅ PASS |
| 22 | Rename across two batches: no DLQ row created | Cross-Batch Rename | 0 | 0 | ✅ PASS |
| 23 | Rename across two batches: newest name wins in target table | Cross-Batch Rename | Updated Name | Updated Name | ✅ PASS |
| 24 | Ambiguity round trip: redemptions return to mart when ambiguity disappears | Redemptions Lifecycle | 3000 | 3000 | ✅ PASS |
| 25 | Invalid DOB: quarantined into DLQ | Data Quality Gate | 1 | 1 | ✅ PASS |
| 26 | Non-numeric miles: quarantined into DLQ without abort | Data Quality Gate | 1 | 1 | ✅ PASS |
| 27 | POST_CODE present in country target table | Schema Completeness | 40001 | 40001 | ✅ PASS |
| 28 | Out-of-order older record audited in DLQ as superseded | Recency Audit | 1 | 1 | ✅ PASS |
| 29 | In-flight landing record processed successfully on subsequent run | Batch Claiming | 1 | 1 | ✅ PASS |
| 30 | Interrupted batch: unprocessed landing record with old batch ID is re-claimed and processed | Fault Tolerance | 1 | 1 | ✅ PASS |
| 31 | Interrupted batch: landing row stamped with PROCESSED_AT | Fault Tolerance | 1 | 1 | ✅ PASS |
| 32 | Same-day correction with equal line numbers: deleted from old TABLE_USA | Tie-Break Accuracy | 0 | 0 | ✅ PASS |
| 33 | Same-day correction with equal line numbers: inserted into new TABLE_INDIA | Tie-Break Accuracy | 1 | 1 | ✅ PASS |
| 34 | Same-day correction in earlier-sorting file name wins on time and later load wins on tie | Tie-Break Accuracy | Tie Winner Later Load | Tie Winner Later Load | ✅ PASS |
