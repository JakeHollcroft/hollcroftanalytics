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


def col_exists(conn, table, col_name):
    try:
        cols = conn.execute(f"DESCRIBE {table}").df()["column_name"].tolist()
        return col_name in cols
    except Exception:
        return False


conn = duckdb.connect(DB_FILE, read_only=True)

print("\n=== EMPLOYEE JOIN AUDIT ===")

tables = [
    "employees",
    "job_emps",
    "job_employees",
    "job_appointments",
    "estimate_options",
    "estimates",
]
for t in tables:
    exists = table_exists(conn, t)
    print(f"\n--- {t} exists? {exists}")
    if exists:
        print(describe(conn, t))

# Use a YTD window if you want
ytd_filter = "DATE_TRUNC('year', CURRENT_DATE)"

# ----------------------------
# NEW: Status enumeration
# ----------------------------
if table_exists(conn, "estimate_options"):
    print("\n=== ESTIMATE OPTION STATUS ENUMERATION ===")

    eo_cols = conn.execute("DESCRIBE estimate_options").df()["column_name"].tolist()
    has_approval = "approval_status" in eo_cols
    has_status = "status" in eo_cols

    if has_approval:
        print("\n--- DISTINCT approval_status (ALL TIME) ---")
        df = conn.execute(
            """
            SELECT
              lower(trim(approval_status)) AS approval_status,
              COUNT(*) AS cnt
            FROM estimate_options
            GROUP BY 1
            ORDER BY cnt DESC
            """
        ).df()
        print(df)

        print("\n--- DISTINCT approval_status (YTD) ---")
        df = conn.execute(
            f"""
            SELECT
              lower(trim(approval_status)) AS approval_status,
              COUNT(*) AS cnt
            FROM estimate_options
            WHERE created_at::DATE >= {ytd_filter}
            GROUP BY 1
            ORDER BY cnt DESC
            """
        ).df()
        print(df)
    else:
        print("\n--- approval_status column not found in estimate_options ---")

    if has_status:
        print("\n--- DISTINCT status (ALL TIME) ---")
        df = conn.execute(
            """
            SELECT
              lower(trim(status)) AS status,
              COUNT(*) AS cnt
            FROM estimate_options
            GROUP BY 1
            ORDER BY cnt DESC
            """
        ).df()
        print(df)

        print("\n--- DISTINCT status (YTD) ---")
        df = conn.execute(
            f"""
            SELECT
              lower(trim(status)) AS status,
              COUNT(*) AS cnt
            FROM estimate_options
            WHERE created_at::DATE >= {ytd_filter}
            GROUP BY 1
            ORDER BY cnt DESC
            """
        ).df()
        print(df)
    else:
        print("\n--- status column not found in estimate_options ---")

    if has_approval and has_status:
        print("\n--- DISTINCT (approval_status, status) COMBOS (ALL TIME) ---")
        df = conn.execute(
            """
            SELECT
              lower(coalesce(trim(approval_status), '')) AS approval_status,
              lower(coalesce(trim(status), '')) AS status,
              COUNT(*) AS cnt
            FROM estimate_options
            GROUP BY 1,2
            ORDER BY cnt DESC
            """
        ).df()
        print(df)

        print("\n--- DISTINCT (approval_status, status) COMBOS (YTD) ---")
        df = conn.execute(
            f"""
            SELECT
              lower(coalesce(trim(approval_status), '')) AS approval_status,
              lower(coalesce(trim(status), '')) AS status,
              COUNT(*) AS cnt
            FROM estimate_options
            WHERE created_at::DATE >= {ytd_filter}
            GROUP BY 1,2
            ORDER BY cnt DESC
            """
        ).df()
        print(df)

# 1) What employee identifiers are present in estimate_options?
if table_exists(conn, "estimate_options"):
    print("\n--- DISTINCT employee-like columns in estimate_options (sample) ---")
    cols = conn.execute("DESCRIBE estimate_options").df()["column_name"].tolist()
    candidate_cols = [
        c
        for c in cols
        if any(
            k in c.lower()
            for k in ["employee", "user", "created", "tech", "assignee", "owner", "sales"]
        )
    ]
    print("Candidate cols:", candidate_cols)

    for c in candidate_cols[:10]:
        try:
            df = conn.execute(
                f"""
                SELECT {c} AS val, COUNT(*) AS n
                FROM estimate_options
                WHERE created_at::DATE >= {ytd_filter}
                GROUP BY 1
                ORDER BY n DESC
                LIMIT 15
                """
            ).df()
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
est_key = None
if table_exists(conn, "estimate_options"):
    eo_cols = conn.execute("DESCRIBE estimate_options").df()["column_name"].tolist()
    for k in [
        "employee_id",
        "user_id",
        "created_by_id",
        "created_by",
        "salesperson_id",
        "technician_id",
    ]:
        if k in eo_cols:
            est_key = k
            break

if est_key and table_exists(conn, "employees"):
    emp_cols = conn.execute("DESCRIBE employees").df()["column_name"].tolist()
    for emp_key in ["employee_id", "id", "user_id"]:
        if emp_key in emp_cols:
            print(f"\n--- JOIN COVERAGE: estimate_options.{est_key} -> employees.{emp_key} ---")
            try:
                df_cov = conn.execute(
                    f"""
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
                    """
                ).df()
                print(df_cov)
            except Exception as e:
                print("join coverage error:", e)

conn.close()
