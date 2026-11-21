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
| **04** | [`04_country_merges.generated.sql`](../sql/snowflake/04_country_merges.generated.sql) | Deterministically generated incremental MERGEs for `TABLE_INDIA`, `TABLE_USA`, `TABLE_CAN`, `TABLE_PHIL`, `TABLE_AU`. |
| **05** | [`05_mart_and_finalize.sql`](../sql/snowflake/05_mart_and_finalize.sql) | Flags ambiguous redemptions, builds `MEMBER_LOYALTY_360` mart, and marks claimed landing rows as processed. |

To execute the entire pipeline end-to-end in one command:
```bash
snowsql -c <your_connection> -f sql/snowflake/run_all.sql
```

---

## 2. Smoke Test Execution Guide

The smoke test suite validates end-to-end data integrity, boundary handling, and idempotent rerun guarantees in an isolated schema `SKYPOINTS_SMOKE_TEST`.

### Running the Smoke Test
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

| Scenario Name | Category | Expected Result | Verified Status |
| :--- | :--- | :---: | :---: |
| Member moving country (old country delete) | Country Migration | 0 in `TABLE_USA` | **PASS** |
| Newer extract date move: inserted into new TABLE_INDIA | Country Migration | 1 in `TABLE_INDIA` | **PASS** |
| Composite key: Same ID in IND_SYS and USA_SYS both survive | Multi-Source Key | 2 members | **PASS** |
| Rename collision: older name quarantined in DLQ | Intra-Batch Collision | 1 DLQ row | **PASS** |
| Rename collision: newer name kept in target table | Intra-Batch Collision | Kept in `TABLE_USA` | **PASS** |
| Empty-string tier code: treated as null, not quarantined | Data Cleansing | 1 valid member | **PASS** |
| Unmapped country: quarantined in DLQ at staging | Data Quality Gate | 1 DLQ row | **PASS** |
| Null country: quarantined in DLQ at staging | Data Quality Gate | 1 DLQ row | **PASS** |
| Bad redemption feed_date: quarantined into DLQ | Redemptions Gate | 1 DLQ row | **PASS** |
| Ambiguous member_id redemption: quarantined into DLQ | Redemptions Ambiguity | 1 DLQ row | **PASS** |
| DLQ reject preservation: two rows with missing member_id both quarantined | DLQ Uniqueness | 2 DLQ rows | **PASS** |
| DLQ reject preservation: same ID in two source systems both bad both quarantined | DLQ Uniqueness | 2 DLQ rows | **PASS** |
| DLQ reject preservation: same bad row in two batches yields 2 DLQ rows | DLQ Uniqueness | 2 DLQ rows | **PASS** |
| Rename across two batches: no DLQ row created | Cross-Batch Rename | 0 DLQ rows | **PASS** |
| Rename across two batches: newest name wins in target table | Cross-Batch Rename | Newest name retained | **PASS** |
| Ambiguity round trip: redemptions return to mart when ambiguity disappears | Redemptions Lifecycle | Miles in Mart | **PASS** |
| Mart before rerun: MEMBER_LOYALTY_360 total member rows | Analytical Mart | 12 members | **PASS** |
| Mart before rerun: MOVE_101 country updated to IND | Analytical Mart | `IND` | **PASS** |
| Mart before rerun: MOVE_101 COMPLETED miles summed correctly | Miles Calculation | 5,000 miles | **PASS** |
| Mart before rerun: MOVE_101 PENDING miles tracked separately | Miles Calculation | 2,000 miles | **PASS** |
| Rerun delta: STG_MEMBER_PROFILE row count unchanged | Idempotent Rerun | Delta = 0 | **PASS** |
| Rerun delta: RAW_DLQ_QUARANTINE row count unchanged | Idempotent Rerun | Delta = 0 | **PASS** |
| Rerun delta: STG_REDEMPTIONS_FLATTENED row count unchanged | Idempotent Rerun | Delta = 0 | **PASS** |
| Rerun delta: MEMBER_LOYALTY_360 row count unchanged | Idempotent Rerun | Delta = 0 | **PASS** |
| Invalid DOB: quarantined into DLQ | Data Quality Gate | 1 DLQ row | **PASS** |
| Non-numeric miles: quarantined into DLQ without abort | Data Quality Gate | 1 DLQ row | **PASS** |
| POST_CODE present in country target table | Schema Completeness | Preserved | **PASS** |
| Out-of-order older record audited in DLQ as superseded | Recency Audit | 1 DLQ row | **PASS** |
| In-flight landing record processed successfully on subsequent run | Batch Claiming | Processed | **PASS** |
| Interrupted batch: unprocessed landing record with old batch ID is re-claimed and processed | Fault Tolerance | Processed | **PASS** |
| Same-day correction with equal line numbers: deleted from old TABLE_USA | Tie-Break Accuracy | Deleted | **PASS** |
| Same-day correction with equal line numbers: inserted into new TABLE_INDIA | Tie-Break Accuracy | Inserted | **PASS** |
| Same-country tie with lower line number: existing higher line number record preserved | Tie-Break Accuracy | Preserved | **PASS** |
