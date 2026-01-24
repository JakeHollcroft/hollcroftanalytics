# query_service_efficiency_audit.py
#
# Goal: pull + validate every field we need for Service Efficiency
# (Billed Hours vs Paid Hours), so we don’t have to refactor later.
#
# What we’ll validate:
# 1) Table presence + schemas
# 2) Row counts + key uniqueness/duplication risk
# 3) Date coverage (YTD + all-time) for invoices + appointments
# 4) Appointment duration quality (nulls, negatives, extreme values)
# 5) Tech mapping coverage:
#       job_appointments.dispatched_employee_id -> employees.employee_id
#       jobs assigned employees (job_employees) vs dispatched employees
# 6) Invoice -> job linkage coverage
# 7) Invoice items shapes (types, qty/unit_cost/unit_price) for billed hours proxy
# 8) Pricebook services coverage (duration/cost/price) if we use it later
#
# Notes:
# - HousecallPro values are often stored in cents upstream; your KPI layer converts.
#   Here we’re just inspecting what’s in DuckDB and checking magnitude patterns.

import os
from pathlib import Path
import duckdb
import pandas as pd

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

# ---------- helpers ----------
def table_exists(conn, table: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        return True
    except Exception:
        return False

def describe(conn, table: str) -> pd.DataFrame:
    try:
        return conn.execute(f"DESCRIBE {table}").df()
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})

def q(conn, sql: str, params=None) -> pd.DataFrame:
    try:
        if params:
            return conn.execute(sql, params).df()
        return conn.execute(sql).df()
    except Exception as e:
        return pd.DataFrame({"error": [str(e)], "sql": [sql[:500]]})

def print_df(title: str, df: pd.DataFrame, max_rows: int = 50):
    print(f"\n=== {title} ===")
    if df is None:
        print("(None)")
        return
    with pd.option_context("display.max_rows", max_rows, "display.max_columns", 200, "display.width", 200):
        print(df)

def column_list(conn, table: str) -> list[str]:
    try:
        return conn.execute(f"DESCRIBE {table}").df()["column_name"].tolist()
    except Exception:
        return []

def has_cols(conn, table: str, cols: list[str]) -> dict[str, bool]:
    existing = set([c.lower() for c in column_list(conn, table)])
    return {c: (c.lower() in existing) for c in cols}

def safe_cast_date_expr(col: str) -> str:
    # created_at and invoice_date are stored as TEXT in your schema.
    # DuckDB can parse ISO timestamps. Use try_cast to avoid hard failures.
    return f"try_cast({col} AS TIMESTAMP)"

# ---------- main ----------
def main():
    conn = duckdb.connect(DB_FILE, read_only=True)

    print("\nSERVICE EFFICIENCY DATA AUDIT")
    print(f"DB: {DB_FILE}")

    # -------- tables involved --------
    required_tables = [
        "jobs",
        "job_employees",
        "job_appointments",
        "employees",
        "invoices",
        "invoice_items",
        "pricebook_services",
    ]

    print("\nTABLE PRESENCE + SCHEMAS")
    for t in required_tables:
        exists = table_exists(conn, t)
        print(f"\n--- {t} exists? {exists}")
        if exists:
            print(describe(conn, t))

    # -------- row counts --------
    print("\nROW COUNTS")
    for t in required_tables:
        if table_exists(conn, t):
            df = q(conn, f"SELECT COUNT(*) AS n FROM {t}")
            print_df(f"count({t})", df)

    # -------- key uniqueness checks --------
    # These are the keys we’ll rely on for joins.
    print("\nKEY UNIQUENESS / DUPLICATION RISK")

    if table_exists(conn, "employees"):
        df = q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT employee_id) AS distinct_employee_id,
              SUM(CASE WHEN employee_id IS NULL OR trim(employee_id) = '' THEN 1 ELSE 0 END) AS null_employee_id
            FROM employees
        """)
        print_df("employees key health", df)

    if table_exists(conn, "jobs"):
        df = q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT job_id) AS distinct_job_id,
              SUM(CASE WHEN job_id IS NULL OR trim(job_id) = '' THEN 1 ELSE 0 END) AS null_job_id
            FROM jobs
        """)
        print_df("jobs key health", df)

    if table_exists(conn, "invoices"):
        df = q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT invoice_id) AS distinct_invoice_id,
              SUM(CASE WHEN invoice_id IS NULL OR trim(invoice_id) = '' THEN 1 ELSE 0 END) AS null_invoice_id,
              COUNT(DISTINCT job_id) AS distinct_job_id_in_invoices,
              SUM(CASE WHEN job_id IS NULL OR trim(job_id) = '' THEN 1 ELSE 0 END) AS null_job_id_in_invoices
            FROM invoices
        """)
        print_df("invoices key health", df)

    if table_exists(conn, "invoice_items"):
        df = q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT invoice_id) AS distinct_invoice_id,
              COUNT(DISTINCT job_id) AS distinct_job_id,
              COUNT(DISTINCT item_id) AS distinct_item_id,
              SUM(CASE WHEN invoice_id IS NULL OR trim(invoice_id) = '' THEN 1 ELSE 0 END) AS null_invoice_id,
              SUM(CASE WHEN item_id IS NULL OR trim(item_id) = '' THEN 1 ELSE 0 END) AS null_item_id
            FROM invoice_items
        """)
        print_df("invoice_items key health", df)

    if table_exists(conn, "job_appointments"):
        # In your ingest, job_appointments is 1 row per (job_id, appointment_id, dispatched_employee_id)
        df = q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT job_id) AS distinct_job_id,
              COUNT(DISTINCT appointment_id) AS distinct_appointment_id,
              COUNT(DISTINCT job_id || '|' || appointment_id || '|' || coalesce(dispatched_employee_id,'')) AS distinct_triplet,
              SUM(CASE WHEN appointment_id IS NULL OR trim(appointment_id) = '' THEN 1 ELSE 0 END) AS null_appointment_id,
              SUM(CASE WHEN job_id IS NULL OR trim(job_id) = '' THEN 1 ELSE 0 END) AS null_job_id,
              SUM(CASE WHEN dispatched_employee_id IS NULL OR trim(dispatched_employee_id) = '' THEN 1 ELSE 0 END) AS null_dispatched_employee_id
            FROM job_appointments
        """)
        print_df("job_appointments key health", df)

    # -------- date coverage --------
    # We need YTD and all-time coverage for:
    # - invoices.invoice_date (billed side)
    # - job_appointments.start_time/end_time (paid side)
    print("\nDATE COVERAGE (YTD + ALL TIME)")

    ytd_start = "DATE_TRUNC('year', CURRENT_DATE)"

    if table_exists(conn, "invoices"):
        inv_cols = has_cols(conn, "invoices", ["invoice_date", "service_date", "paid_at", "status"])
        print_df("invoices columns present", pd.DataFrame([inv_cols]))

        # invoice_date coverage
        df = q(conn, f"""
            WITH base AS (
              SELECT
                {safe_cast_date_expr("invoice_date")} AS invoice_dt,
                lower(trim(status)) AS status,
                amount
              FROM invoices
            )
            SELECT
              COUNT(*) AS rows,
              SUM(CASE WHEN invoice_dt IS NULL THEN 1 ELSE 0 END) AS null_invoice_date,
              MIN(invoice_dt) AS min_invoice_dt,
              MAX(invoice_dt) AS max_invoice_dt
            FROM base
        """)
        print_df("invoices invoice_date coverage (all-time)", df)

        df = q(conn, f"""
            WITH base AS (
              SELECT
                {safe_cast_date_expr("invoice_date")} AS invoice_dt,
                lower(trim(status)) AS status,
                amount
              FROM invoices
              WHERE {safe_cast_date_expr("invoice_date")} >= {ytd_start}
            )
            SELECT
              COUNT(*) AS rows_ytd,
              SUM(CASE WHEN invoice_dt IS NULL THEN 1 ELSE 0 END) AS null_invoice_date_ytd,
              MIN(invoice_dt) AS min_invoice_dt_ytd,
              MAX(invoice_dt) AS max_invoice_dt_ytd,
              COUNT(DISTINCT status) AS distinct_statuses_ytd
            FROM base
        """)
        print_df("invoices invoice_date coverage (YTD)", df)

        df = q(conn, f"""
            SELECT
              lower(trim(status)) AS status,
              COUNT(*) AS cnt,
              MIN({safe_cast_date_expr("invoice_date")}) AS min_dt,
              MAX({safe_cast_date_expr("invoice_date")}) AS max_dt
            FROM invoices
            GROUP BY 1
            ORDER BY cnt DESC
        """)
        print_df("invoice statuses (all-time)", df)

        df = q(conn, f"""
            SELECT
              lower(trim(status)) AS status,
              COUNT(*) AS cnt
            FROM invoices
            WHERE {safe_cast_date_expr("invoice_date")} >= {ytd_start}
            GROUP BY 1
            ORDER BY cnt DESC
        """)
        print_df("invoice statuses (YTD)", df)

        # quick magnitude check: are amounts cents-like or dollars-like?
        df = q(conn, f"""
            WITH base AS (
              SELECT amount
              FROM invoices
              WHERE amount IS NOT NULL
                AND {safe_cast_date_expr("invoice_date")} >= {ytd_start}
            )
            SELECT
              COUNT(*) AS n,
              MIN(amount) AS min_amount,
              APPROX_QUANTILE(amount, 0.50) AS p50_amount,
              APPROX_QUANTILE(amount, 0.90) AS p90_amount,
              MAX(amount) AS max_amount
            FROM base
        """)
        print_df("invoices.amount magnitude check (YTD)", df)

    if table_exists(conn, "job_appointments"):
        appt_cols = has_cols(conn, "job_appointments", ["start_time", "end_time", "appointment_id", "dispatched_employee_id"])
        print_df("job_appointments columns present", pd.DataFrame([appt_cols]))

        df = q(conn, f"""
            WITH base AS (
              SELECT
                {safe_cast_date_expr("start_time")} AS start_dt,
                {safe_cast_date_expr("end_time")} AS end_dt,
                dispatched_employee_id
              FROM job_appointments
            )
            SELECT
              COUNT(*) AS rows,
              SUM(CASE WHEN start_dt IS NULL THEN 1 ELSE 0 END) AS null_start,
              SUM(CASE WHEN end_dt IS NULL THEN 1 ELSE 0 END) AS null_end,
              MIN(start_dt) AS min_start,
              MAX(start_dt) AS max_start
            FROM base
        """)
        print_df("appointments start/end coverage (all-time)", df)

        df = q(conn, f"""
            WITH base AS (
              SELECT
                {safe_cast_date_expr("start_time")} AS start_dt,
                {safe_cast_date_expr("end_time")} AS end_dt
              FROM job_appointments
              WHERE {safe_cast_date_expr("start_time")} >= {ytd_start}
            )
            SELECT
              COUNT(*) AS rows_ytd,
              SUM(CASE WHEN start_dt IS NULL THEN 1 ELSE 0 END) AS null_start_ytd,
              SUM(CASE WHEN end_dt IS NULL THEN 1 ELSE 0 END) AS null_end_ytd,
              MIN(start_dt) AS min_start_ytd,
              MAX(start_dt) AS max_start_ytd
            FROM base
        """)
        print_df("appointments start/end coverage (YTD)", df)

        # duration checks (minutes and hours), flag negatives/extremes
        df = q(conn, f"""
            WITH base AS (
              SELECT
                {safe_cast_date_expr("start_time")} AS start_dt,
                {safe_cast_date_expr("end_time")} AS end_dt
              FROM job_appointments
              WHERE {safe_cast_date_expr("start_time")} >= {ytd_start}
            ),
            dur AS (
              SELECT
                DATE_DIFF('minute', start_dt, end_dt) AS minutes
              FROM base
              WHERE start_dt IS NOT NULL AND end_dt IS NOT NULL
            )
            SELECT
              COUNT(*) AS n,
              SUM(CASE WHEN minutes < 0 THEN 1 ELSE 0 END) AS negative_minutes,
              SUM(CASE WHEN minutes = 0 THEN 1 ELSE 0 END) AS zero_minutes,
              SUM(CASE WHEN minutes > 12*60 THEN 1 ELSE 0 END) AS over_12_hours,
              MIN(minutes) AS min_minutes,
              APPROX_QUANTILE(minutes, 0.50) AS p50_minutes,
              APPROX_QUANTILE(minutes, 0.90) AS p90_minutes,
              MAX(minutes) AS max_minutes
            FROM dur
        """)
        print_df("appointment duration quality (YTD)", df)

        # sample of worst offenders
        df = q(conn, f"""
            WITH base AS (
              SELECT
                job_id,
                appointment_id,
                dispatched_employee_id,
                {safe_cast_date_expr("start_time")} AS start_dt,
                {safe_cast_date_expr("end_time")} AS end_dt
              FROM job_appointments
              WHERE {safe_cast_date_expr("start_time")} >= {ytd_start}
            )
            SELECT
              job_id,
              appointment_id,
              dispatched_employee_id,
              start_dt,
              end_dt,
              DATE_DIFF('minute', start_dt, end_dt) AS minutes
            FROM base
            WHERE start_dt IS NOT NULL AND end_dt IS NOT NULL
            ORDER BY minutes ASC
            LIMIT 20
        """)
        print_df("appointments with smallest durations (YTD)", df)

        df = q(conn, f"""
            WITH base AS (
              SELECT
                job_id,
                appointment_id,
                dispatched_employee_id,
                {safe_cast_date_expr("start_time")} AS start_dt,
                {safe_cast_date_expr("end_time")} AS end_dt
              FROM job_appointments
              WHERE {safe_cast_date_expr("start_time")} >= {ytd_start}
            )
            SELECT
              job_id,
              appointment_id,
              dispatched_employee_id,
              start_dt,
              end_dt,
              DATE_DIFF('minute', start_dt, end_dt) AS minutes
            FROM base
            WHERE start_dt IS NOT NULL AND end_dt IS NOT NULL
            ORDER BY minutes DESC
            LIMIT 20
        """)
        print_df("appointments with largest durations (YTD)", df)

    # -------- join coverage --------
    print("\nJOIN COVERAGE TESTS")

    # 1) invoice -> job join coverage
    if table_exists(conn, "invoices") and table_exists(conn, "jobs"):
        df = q(conn, f"""
            WITH inv AS (
              SELECT
                invoice_id,
                job_id,
                lower(trim(status)) AS status,
                {safe_cast_date_expr("invoice_date")} AS invoice_dt
              FROM invoices
              WHERE {safe_cast_date_expr("invoice_date")} >= {ytd_start}
            ),
            j AS (
              SELECT job_id FROM jobs
            )
            SELECT
              COUNT(*) AS inv_rows_ytd,
              SUM(CASE WHEN inv.job_id IS NULL OR trim(inv.job_id) = '' THEN 1 ELSE 0 END) AS inv_null_job_id,
              SUM(CASE WHEN j.job_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_jobs,
              SUM(CASE WHEN j.job_id IS NULL THEN 1 ELSE 0 END) AS unmatched_jobs
            FROM inv
            LEFT JOIN j ON inv.job_id = j.job_id
        """)
        print_df("invoice.job_id -> jobs.job_id coverage (YTD)", df)

        df = q(conn, f"""
            SELECT
              inv.job_id,
              COUNT(*) AS inv_cnt
            FROM invoices inv
            LEFT JOIN jobs j ON inv.job_id = j.job_id
            WHERE {safe_cast_date_expr("inv.invoice_date")} >= {ytd_start}
              AND j.job_id IS NULL
              AND inv.job_id IS NOT NULL
            GROUP BY 1
            ORDER BY inv_cnt DESC
            LIMIT 25
        """)
        print_df("sample invoices with job_id not found in jobs (YTD)", df)

    # 2) dispatched_employee_id -> employees coverage
    if table_exists(conn, "job_appointments") and table_exists(conn, "employees"):
        df = q(conn, f"""
            WITH ap AS (
              SELECT
                dispatched_employee_id AS emp_id,
                {safe_cast_date_expr("start_time")} AS start_dt
              FROM job_appointments
              WHERE {safe_cast_date_expr("start_time")} >= {ytd_start}
                AND dispatched_employee_id IS NOT NULL
                AND trim(dispatched_employee_id) <> ''
            ),
            e AS (
              SELECT employee_id FROM employees
            )
            SELECT
              COUNT(*) AS appt_emp_rows_ytd,
              COUNT(DISTINCT ap.emp_id) AS distinct_dispatched_emp_ids_ytd,
              SUM(CASE WHEN e.employee_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_rows,
              SUM(CASE WHEN e.employee_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
            FROM ap
            LEFT JOIN e ON ap.emp_id = e.employee_id
        """)
        print_df("job_appointments.dispatched_employee_id -> employees.employee_id coverage (YTD)", df)

        df = q(conn, f"""
            WITH ap AS (
              SELECT
                dispatched_employee_id AS emp_id,
                COUNT(*) AS cnt
              FROM job_appointments
              WHERE {safe_cast_date_expr("start_time")} >= {ytd_start}
                AND dispatched_employee_id IS NOT NULL
                AND trim(dispatched_employee_id) <> ''
              GROUP BY 1
            )
            SELECT ap.emp_id, ap.cnt
            FROM ap
            LEFT JOIN employees e ON ap.emp_id = e.employee_id
            WHERE e.employee_id IS NULL
            ORDER BY ap.cnt DESC
            LIMIT 25
        """)
        print_df("dispatched emp IDs not found in employees (YTD)", df)

    # 3) job assigned employees vs dispatched employees (mismatch check)
    if table_exists(conn, "job_employees") and table_exists(conn, "job_appointments"):
        df = q(conn, f"""
            WITH ja AS (
              SELECT
                job_id,
                COUNT(DISTINCT coalesce(dispatched_employee_id,'')) AS dispatched_emp_count,
                SUM(CASE WHEN dispatched_employee_id IS NULL OR trim(dispatched_employee_id) = '' THEN 1 ELSE 0 END) AS null_dispatched_rows
              FROM job_appointments
              WHERE {safe_cast_date_expr("start_time")} >= {ytd_start}
              GROUP BY 1
            ),
            je AS (
              SELECT
                job_id,
                COUNT(DISTINCT employee_id) AS assigned_emp_count
              FROM job_employees
              GROUP BY 1
            )
            SELECT
              COUNT(*) AS jobs_seen,
              SUM(CASE WHEN je.assigned_emp_count IS NULL THEN 1 ELSE 0 END) AS jobs_missing_assigned_emps,
              SUM(CASE WHEN ja.dispatched_emp_count IS NULL THEN 1 ELSE 0 END) AS jobs_missing_dispatched_emps,
              SUM(CASE WHEN coalesce(je.assigned_emp_count,0) <> coalesce(ja.dispatched_emp_count,0) THEN 1 ELSE 0 END) AS jobs_assigned_vs_dispatched_mismatch
            FROM ja
            LEFT JOIN je ON ja.job_id = je.job_id
        """)
        print_df("assigned vs dispatched employee counts (YTD appointments)", df)

        df = q(conn, f"""
            WITH ja AS (
              SELECT
                job_id,
                COUNT(DISTINCT coalesce(dispatched_employee_id,'')) AS dispatched_emp_count
              FROM job_appointments
              WHERE {safe_cast_date_expr("start_time")} >= {ytd_start}
              GROUP BY 1
            ),
            je AS (
              SELECT
                job_id,
                COUNT(DISTINCT employee_id) AS assigned_emp_count
              FROM job_employees
              GROUP BY 1
            )
            SELECT
              ja.job_id,
              coalesce(je.assigned_emp_count,0) AS assigned_emp_count,
              coalesce(ja.dispatched_emp_count,0) AS dispatched_emp_count
            FROM ja
            LEFT JOIN je ON ja.job_id = je.job_id
            WHERE coalesce(je.assigned_emp_count,0) <> coalesce(ja.dispatched_emp_count,0)
            ORDER BY ABS(coalesce(je.assigned_emp_count,0) - coalesce(ja.dispatched_emp_count,0)) DESC
            LIMIT 25
        """)
        print_df("sample mismatch jobs (assigned vs dispatched)", df)

    # -------- invoice_items inspection for billed-hours proxies --------
    print("\nINVOICE ITEMS SHAPE (BILLED SIDE PROXIES)")

    if table_exists(conn, "invoice_items"):
        df = q(conn, f"""
            SELECT
              lower(trim(type)) AS type,
              COUNT(*) AS cnt
            FROM invoice_items
            GROUP BY 1
            ORDER BY cnt DESC
        """)
        print_df("invoice_items types (all-time)", df)

        df = q(conn, f"""
            SELECT
              lower(trim(type)) AS type,
              COUNT(*) AS cnt
            FROM invoice_items it
            JOIN invoices inv ON it.invoice_id = inv.invoice_id
            WHERE {safe_cast_date_expr("inv.invoice_date")} >= {ytd_start}
            GROUP BY 1
            ORDER BY cnt DESC
        """)
        print_df("invoice_items types (YTD billed invoices)", df)

        # qty/unit_cost/unit_price null rates
        df = q(conn, f"""
            SELECT
              COUNT(*) AS rows,
              SUM(CASE WHEN qty_in_hundredths IS NULL THEN 1 ELSE 0 END) AS null_qty,
              SUM(CASE WHEN unit_cost IS NULL THEN 1 ELSE 0 END) AS null_unit_cost,
              SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
              SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amount
            FROM invoice_items it
            JOIN invoices inv ON it.invoice_id = inv.invoice_id
            WHERE {safe_cast_date_expr("inv.invoice_date")} >= {ytd_start}
        """)
        print_df("invoice_items null rates (YTD billed invoices)", df)

        # magnitude checks (are these cents-like?)
        df = q(conn, f"""
            WITH base AS (
              SELECT it.unit_price, it.unit_cost, it.amount
              FROM invoice_items it
              JOIN invoices inv ON it.invoice_id = inv.invoice_id
              WHERE {safe_cast_date_expr("inv.invoice_date")} >= {ytd_start}
            )
            SELECT
              COUNT(*) AS n,
              APPROX_QUANTILE(unit_price, 0.50) AS p50_unit_price,
              APPROX_QUANTILE(unit_price, 0.90) AS p90_unit_price,
              APPROX_QUANTILE(amount, 0.50) AS p50_amount,
              APPROX_QUANTILE(amount, 0.90) AS p90_amount
            FROM base
        """)
        print_df("invoice_items magnitude check (YTD)", df)

        # sample top items for labor-ish naming (to see if labor is encoded as items)
        df = q(conn, f"""
            SELECT
              it.name,
              lower(trim(it.type)) AS type,
              COUNT(*) AS cnt
            FROM invoice_items it
            JOIN invoices inv ON it.invoice_id = inv.invoice_id
            WHERE {safe_cast_date_expr("inv.invoice_date")} >= {ytd_start}
            GROUP BY 1,2
            ORDER BY cnt DESC
            LIMIT 50
        """)
        print_df("most common invoice item names (YTD billed invoices)", df)

    # -------- pricebook services coverage --------
    print("\nPRICEBOOK SERVICES COVERAGE (OPTIONAL, FOR FUTURE BILLED-HOURS MAPPING)")

    if table_exists(conn, "pricebook_services"):
        cols_ok = has_cols(conn, "pricebook_services", ["service_uuid", "name", "duration", "cost", "price", "updated_at"])
        print_df("pricebook_services columns present", pd.DataFrame([cols_ok]))

        df = q(conn, """
            SELECT
              COUNT(*) AS rows,
              SUM(CASE WHEN duration IS NULL THEN 1 ELSE 0 END) AS null_duration,
              SUM(CASE WHEN cost IS NULL THEN 1 ELSE 0 END) AS null_cost,
              SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price,
              MIN(duration) AS min_duration,
              APPROX_QUANTILE(duration, 0.50) AS p50_duration,
              APPROX_QUANTILE(duration, 0.90) AS p90_duration,
              MAX(duration) AS max_duration
            FROM pricebook_services
        """)
        print_df("pricebook_services completeness + duration stats", df)

        df = q(conn, """
            SELECT name, duration, cost, price
            FROM pricebook_services
            ORDER BY updated_at DESC
            LIMIT 25
        """)
        print_df("pricebook_services sample (latest updated)", df)

    # -------- a single joined “audit view” sample --------
    # This is the practical “do we have everything to compute it?” slice:
    # For a sample of billed invoices YTD:
    # - invoice header (status, amount, date)
    # - job tags (category)
    # - appointment durations + dispatched tech
    # - employee names
    print("\nJOINED SAMPLE VIEW (YTD billed invoices): invoice -> job -> appointments -> tech")

    if table_exists(conn, "invoices") and table_exists(conn, "jobs") and table_exists(conn, "job_appointments"):
        joined = q(conn, f"""
            WITH inv AS (
              SELECT
                invoice_id,
                job_id,
                lower(trim(status)) AS status,
                amount,
                {safe_cast_date_expr("invoice_date")} AS invoice_dt
              FROM invoices
              WHERE {safe_cast_date_expr("invoice_date")} >= {ytd_start}
                AND lower(trim(status)) IN ('open','pending_payment')
            ),
            ap AS (
              SELECT
                job_id,
                appointment_id,
                dispatched_employee_id,
                {safe_cast_date_expr("start_time")} AS start_dt,
                {safe_cast_date_expr("end_time")} AS end_dt,
                DATE_DIFF('minute', {safe_cast_date_expr("start_time")}, {safe_cast_date_expr("end_time")}) AS minutes
              FROM job_appointments
              WHERE {safe_cast_date_expr("start_time")} >= {ytd_start}
            ),
            emp AS (
              SELECT
                employee_id,
                trim(coalesce(first_name,'')) || ' ' || trim(coalesce(last_name,'')) AS employee_name,
                role
              FROM employees
            )
            SELECT
              inv.invoice_id,
              inv.job_id,
              inv.status,
              inv.invoice_dt,
              inv.amount AS invoice_amount_raw,
              j.work_status AS job_work_status,
              j.tags AS job_tags_raw,
              ap.appointment_id,
              ap.dispatched_employee_id,
              emp.employee_name,
              emp.role,
              ap.start_dt,
              ap.end_dt,
              ap.minutes
            FROM inv
            LEFT JOIN jobs j ON inv.job_id = j.job_id
            LEFT JOIN ap ON inv.job_id = ap.job_id
            LEFT JOIN emp ON ap.dispatched_employee_id = emp.employee_id
            ORDER BY inv.invoice_dt DESC
            LIMIT 200
        """)
        print_df("joined audit view sample", joined, max_rows=200)

    conn.close()


if __name__ == "__main__":
    main()
