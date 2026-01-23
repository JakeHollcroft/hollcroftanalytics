import duckdb
import pandas as pd
from pathlib import Path
import os

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

def describe(conn, table):
    try:
        return conn.execute(f"DESCRIBE {table}").df()
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})

def table_exists(conn, table):
    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        return True
    except Exception:
        return False

conn = duckdb.connect(DB_FILE, read_only=True)

print("\n=== EMPLOYEE JOIN AUDIT ===")

tables = ["employees", "job_emps", "job_employees", "job_appointments", "estimate_options", "estimates"]
for t in tables:
    print(f"\n--- {t} exists? {table_exists(conn, t)}")
    if table_exists(conn, t):
        print(describe(conn, t))

# Use a YTD window if you want
# DuckDB: current_date is available
ytd_filter = "DATE_TRUNC('year', CURRENT_DATE)"

# 1) What employee identifiers are present in estimate_options?
if table_exists(conn, "estimate_options"):
    print("\n--- DISTINCT employee-like columns in estimate_options (sample) ---")
    # This tries common column names; adjust if your schema differs
    cols = conn.execute("DESCRIBE estimate_options").df()["column_name"].tolist()
    candidate_cols = [c for c in cols if any(k in c.lower() for k in ["employee", "user", "created", "tech", "assignee", "owner", "sales"])]
    print("Candidate cols:", candidate_cols)

    # Pick a few likely ones to inspect
    for c in candidate_cols[:10]:
        try:
            df = conn.execute(f"""
                SELECT {c} AS val, COUNT(*) AS n
                FROM estimate_options
                WHERE created_at::DATE >= {ytd_filter}
                GROUP BY 1
                ORDER BY n DESC
                LIMIT 15
            """).df()
            print(f"\nTop values for {c}:")
            print(df)
        except Exception as e:
            print(f"\nCould not query {c}: {e}")

# 2) What keys exist in employees?
if table_exists(conn, "employees"):
    print("\n--- employees sample (look for id/user_id/name fields) ---")
    try:
        print(conn.execute("SELECT * FROM employees LIMIT 5").df())
    except Exception as e:
        print("employees sample error:", e)

# 3) Join coverage tests
# You need to select the actual employee identifier field from estimate rollup.
# Below assumes estimate_options has employee_id OR user_id. Replace est_key accordingly after you see schema.
est_key = None
if table_exists(conn, "estimate_options"):
    eo_cols = conn.execute("DESCRIBE estimate_options").df()["column_name"].tolist()
    # choose a likely key
    for k in ["employee_id", "user_id", "created_by_id", "created_by", "salesperson_id", "technician_id"]:
        if k in eo_cols:
            est_key = k
            break

if est_key and table_exists(conn, "employees"):
    emp_cols = conn.execute("DESCRIBE employees").df()["column_name"].tolist()
    # try matching against common employee keys
    for emp_key in ["employee_id", "id", "user_id"]:
        if emp_key in emp_cols:
            print(f"\n--- JOIN COVERAGE: estimate_options.{est_key} -> employees.{emp_key} ---")
            try:
                df_cov = conn.execute(f"""
                    WITH est AS (
                        SELECT CAST({est_key} AS VARCHAR) AS est_emp_key
                        FROM estimate_options
                        WHERE created_at::DATE >= {ytd_filter}
                          AND {est_key} IS NOT NULL
                    ),
                    emp AS (
                        SELECT CAST({emp_key} AS VARCHAR) AS emp_key
                        FROM employees
                        WHERE {emp_key} IS NOT NULL
                    )
                    SELECT
                      COUNT(*) AS rows,
                      COUNT(DISTINCT est_emp_key) AS distinct_est_keys,
                      SUM(CASE WHEN emp.emp_key IS NOT NULL THEN 1 ELSE 0 END) AS matched_rows
                    FROM est
                    LEFT JOIN emp ON est.est_emp_key = emp.emp_key
                """).df()
                print(df_cov)
            except Exception as e:
                print("join coverage error:", e)

conn.close()
