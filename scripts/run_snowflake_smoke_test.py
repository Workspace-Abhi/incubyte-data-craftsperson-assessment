"""
Run Snowflake smoke test against live Snowflake warehouse.
Usage:
    python scripts/run_snowflake_smoke_test.py
"""
import sys
import os
import io
from pathlib import Path
import snowflake.connector

ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT", "HWEAQUL-UO73138")
USER = os.environ.get("SNOWFLAKE_USER", "ABHISHEKKUMAR.STUDENT1@GMAIL.COM")
PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", "TempPassword123!")
ROLE = os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

REPO_ROOT = Path(__file__).resolve().parent.parent

def load_sql_recursive(sql_path: Path) -> str:
    """Recursively resolve SnowSQL !source directives in memory."""
    content = sql_path.read_text(encoding="utf-8")
    lines = []
    for line in content.splitlines():
        trimmed = line.strip()
        if trimmed.startswith("!source "):
            ref_path = trimmed.split("!source ", 1)[1].strip()
            full_ref = REPO_ROOT / ref_path
            lines.append(f"-- >>> INLINED {ref_path}")
            lines.append(load_sql_recursive(full_ref))
            lines.append(f"-- <<< END {ref_path}")
        else:
            lines.append(line)
    return "\n".join(lines)

def main():
    print(f"Connecting to Snowflake account {ACCOUNT} as {USER}...")
    
    try:
        conn = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            role=ROLE,
        )
        print(" Connected successfully to Snowflake!")
        cur = conn.cursor()
        
        # Ensure warehouse exists and is active
        print(f"Ensuring warehouse {WAREHOUSE} is ready...")
        cur.execute(f"CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;")
        cur.execute(f"USE WAREHOUSE {WAREHOUSE};")
        cur.execute("CREATE DATABASE IF NOT EXISTS SKYPOINTS_DB;")
        cur.execute("USE DATABASE SKYPOINTS_DB;")
        
        # Read and recursively resolve smoke test SQL in memory
        smoke_sql_path = REPO_ROOT / "sql" / "snowflake" / "tests" / "smoke_test.sql"
        print(f"Reading and resolving {smoke_sql_path}...")
        full_sql = load_sql_recursive(smoke_sql_path)

        print("Executing smoke test suite on Snowflake (this takes ~1-2 minutes)...")
        from snowflake.connector.util_text import split_statements
        results = []
        stmt_idx = 0
        for stmt, is_empty in split_statements(io.StringIO(full_sql)):
            clean = "\n".join([l for l in stmt.splitlines() if not l.strip().startswith("--")]).strip()
            if not clean:
                continue
            stmt_idx += 1
            cur.execute(stmt)
            if cur.description and len(cur.description) >= 3:
                cols = [c[0].lower() for c in cur.description]
                if "scenario" in cols and "status" in cols:
                    results = cur.fetchall()
        
        print(f"Executed {stmt_idx} statements.")
        print("\n" + "=" * 80)
        print("SNOWFLAKE SMOKE TEST RESULTS")
        print("=" * 80)
        
        passed = 0
        failed = 0
        for row in results:
            scenario, expected, actual, status = row[0], row[1], row[2], row[3]
            mark = "PASS" if status == "PASS" else "FAIL"
            if status == "PASS":
                passed += 1
            else:
                failed += 1
            print(f"[{mark:4s}] {scenario} | Expected: {expected} | Actual: {actual}")

        print("=" * 80)
        print(f"TOTAL: {len(results)} | PASSED: {passed} | FAILED: {failed}")
        print("=" * 80)

        cur.close()
        conn.close()
        
        if failed > 0:
            sys.exit(1)
        else:
            print("\n ALL SNOWFLAKE SMOKE TESTS PASSED ON LIVE WAREHOUSE!")
            
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
