import os
from pathlib import Path

def build_combined():
    repo_root = Path(__file__).resolve().parent.parent
    sf_dir = repo_root / "sql" / "snowflake"
    smoke_file = sf_dir / "tests" / "smoke_test.sql"
    combined_file = sf_dir / "tests" / "smoke_test_combined.sql"
    
    ddl_sql = (sf_dir / "01_ddl.sql").read_text(encoding="utf-8")
    trans_sql = (sf_dir / "03_transformations.sql").read_text(encoding="utf-8")
    merges_sql = (sf_dir / "04_country_merges.generated.sql").read_text(encoding="utf-8")
    mart_sql = (sf_dir / "05_mart_and_finalize.sql").read_text(encoding="utf-8")
    
    inlined_run_all = "\n\n".join([
        "-- ==================== INLINED 01_DDL.SQL ====================",
        ddl_sql,
        "-- ==================== INLINED 03_TRANSFORMATIONS.SQL ====================",
        trans_sql,
        "-- ==================== INLINED 04_COUNTRY_MERGES.SQL ====================",
        merges_sql,
        "-- ==================== INLINED 05_MART_AND_FINALIZE.SQL ====================",
        mart_sql
    ])
    
    smoke_sql = smoke_file.read_text(encoding="utf-8")
    combined_sql = smoke_sql.replace("!source sql/snowflake/01_ddl.sql", ddl_sql)
    combined_sql = combined_sql.replace("!source sql/snowflake/run_all.sql", inlined_run_all)
    
    combined_file.write_text(combined_sql, encoding="utf-8")
    print(f"Generated {combined_file} ({len(combined_sql)} chars)")

if __name__ == "__main__":
    build_combined()
