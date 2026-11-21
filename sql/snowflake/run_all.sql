-- ============================================================================
-- SkyPoints Snowflake End-to-End Execution Script
-- NOTE: run_batch_id is a session variable initialized in 03_transformations.sql.
-- All four scripts MUST execute in the exact same SnowSQL session in order:
-- 1. 01_ddl.sql                       : Tables, views, reference mappings
-- 2. 03_transformations.sql           : Sanitization, validation DLQ, staging, JSON flatten
-- 3. 04_country_merges.generated.sql  : Per-country table routing & cross-country migration
-- 4. 05_mart_and_finalize.sql         : Ambiguous redemptions, mart aggregation & landing stamps
-- ============================================================================
!source sql/snowflake/01_ddl.sql
!source sql/snowflake/03_transformations.sql
!source sql/snowflake/04_country_merges.generated.sql
!source sql/snowflake/05_mart_and_finalize.sql
