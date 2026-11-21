-- ============================================================================
-- SkyPoints Snowflake End-to-End Execution Script
-- NOTE: run_batch_id is a session variable initialized in transformations.sql.
-- All four scripts MUST execute in the exact same SnowSQL session in order:
-- 1. ddl.sql                     : Tables, views, reference mappings
-- 2. transformations.sql         : Sanitization, validation DLQ, staging, JSON flatten
-- 3. generated_country_merges.sql: Per-country table routing & cross-country migration
-- 4. 03_mart_and_finalize.sql    : Ambiguous redemptions, mart aggregation & landing stamps
-- ============================================================================
!source sql/snowflake/ddl.sql
!source sql/snowflake/transformations.sql
!source sql/snowflake/generated_country_merges.sql
!source sql/snowflake/03_mart_and_finalize.sql
