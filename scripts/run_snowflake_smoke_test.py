"""
Run Snowflake smoke test using browser SSO (externalbrowser).
Usage:
    python scripts/run_snowflake_smoke_test.py
"""
import sys
import os
import snowflake.connector

ACCOUNT = "HWEAQUL-UO73138"
USER = "ABHISHEKKUMAR.STUDENT1@GMAIL.COM"
ROLE = "ACCOUNTADMIN"
WAREHOUSE = "COMPUTE_WH"

def main():
    print(f"Connecting to Snowflake account {ACCOUNT} as {USER} using browser SSO...")
    print("Your web browser will open for Google Sign-In. Please complete the login in your browser.")
    
    try:
        conn = snowflake.connector.connect(
            user=USER,
            account=ACCOUNT,
            authenticator="externalbrowser",
            role=ROLE,
        )
        print(" Connected successfully to Snowflake!")
        cur = conn.cursor()
        
        # Ensure warehouse exists and is active
        print(f"Ensuring warehouse {WAREHOUSE} is ready...")
        cur.execute(f"CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE} WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;")
        cur.execute(f"USE WAREHOUSE {WAREHOUSE};")
        
        # Read combined SQL file
        sql_file = os.path.join(os.path.dirname(__file__), "..", "sql", "snowflake", "tests", "smoke_test_combined.sql")
        print(f"Reading {sql_file}...")
        with open(sql_file, "r", encoding="utf-8") as f:
            full_sql = f.read()

        print("Executing smoke test suite on Snowflake (this takes ~1-2 minutes)...")
        # execute_stream executes all statements sequentially
        import io
        results = []
        for cur_res in conn.execute_stream(io.StringIO(full_sql)):
            # Capture the last query result (AssertionResults)
            if cur_res.description and len(cur_res.description) >= 3:
                cols = [c[0].lower() for c in cur_res.description]
                if "scenario" in cols and "status" in cols:
                    results = cur_res.fetchall()
        
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
            print("\n ALL SNOWFLAKE SMOKE TESTS PASSED!")
            
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
