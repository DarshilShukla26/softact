"""
run_setup.py — Executes setup.sql against your Snowflake account.

Usage:
    source .env
    python3 snowflake/run_setup.py
"""

import os
import sys
import pathlib
import snowflake.connector

def main():
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing  = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"ERROR: missing environment variables: {missing}")
        print("Run:  source .env")
        sys.exit(1)

    sql_path = pathlib.Path(__file__).parent / "setup.sql"
    sql      = sql_path.read_text()

    # Strip inline -- comments from each line, then split on semicolons
    clean_lines = []
    for line in sql.splitlines():
        stripped = line.split("--")[0]   # drop everything after --
        clean_lines.append(stripped)
    clean_sql = "\n".join(clean_lines)

    statements = [
        s.strip() for s in clean_sql.split(";")
        if s.strip()
    ]

    print(f"Connecting to Snowflake account: {os.environ['SNOWFLAKE_ACCOUNT']}")
    conn = snowflake.connector.connect(
        account  = os.environ["SNOWFLAKE_ACCOUNT"],
        user     = os.environ["SNOWFLAKE_USER"],
        password = os.environ["SNOWFLAKE_PASSWORD"],
        role     = "ACCOUNTADMIN",   # free-trial accounts default to ACCOUNTADMIN
    )
    cur = conn.cursor()

    print(f"Running {len(statements)} statements from setup.sql ...\n")
    for i, stmt in enumerate(statements, 1):
        preview = stmt[:60].replace("\n", " ")
        try:
            cur.execute(stmt)
            rows = cur.fetchall()
            print(f"  [{i:02d}] OK  {preview}...")
            # Print SHOW results so we can see what was created
            if stmt.strip().upper().startswith("SHOW"):
                for row in rows:
                    print(f"         {row}")
        except Exception as e:
            print(f"  [{i:02d}] ERROR  {preview}...")
            print(f"         {e}")
            # Non-fatal: IF NOT EXISTS makes most statements idempotent

    cur.close()
    conn.close()
    print("\nSetup complete.")

if __name__ == "__main__":
    main()
