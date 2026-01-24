# query_service_efficiency_audit.py
import os
from pathlib import Path
import duckdb
import pandas as pd

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

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
        return pd.DataFrame({"error": [str(e)], "sql": [sql[:800]]})

def print_df(title: str, df: pd.DataFrame, max_rows: int = 50):
    print(f"\n=== {title} ===")
    if df is None:
        print("(None)")
        return
    with pd.option_context(
        "display.max_rows", max_rows,
        "display.max_columns", 200,
        "display.width", 200
    ):
        print(df)

def column_list(conn, table: str) -> list[str]:
    try:
        return conn.execute(f"DESCRIBE {table}").df()["column_name"].tolist()
    except Exception:
        return []

def has_cols(conn, table: str, cols: list[str]) -> dict[str, bool]:
    existing = set([c.lower() for c in column_list(conn, table)])
    return {c: (c.lower() in existing) for c in cols}

def safe_cast_ts_expr(col: str) -> str:
    return f"try_cast({col} AS TIMESTAMP)"

def main():
    conn = duckdb.connect(DB_FILE, read_only=True)

    print("\nSERVICE EFFICIENCY DATA AUDIT")
    print(f"DB: {DB_FILE}")

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

    print("\nROW COUNTS")
    for t in required_tables:
        if table_exists(conn, t):
            print_df(f"count({t})", q(conn, f"SELECT COUNT(*) AS n FROM {t}"))

    print("\nKEY UNIQUENESS / DUPLICATION RISK")

    if table_exists(conn, "employees"):
        print_df("employees key health", q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT employee_id) AS distinct_employee_id,
              SUM(CASE WHEN employee_id IS NULL OR trim(employee_id) = '' THEN 1 ELSE 0 END) AS null_employee_id
            FROM employees
        """))

    if table_exists(conn, "jobs"):
        print_df("jobs key health", q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT job_id) AS distinct_job_id,
              SUM(CASE WHEN job_id IS NULL OR trim(job_id) = '' THEN 1 ELSE 0 END) AS null_job_id
            FROM jobs
        """))

    if table_exists(conn, "invoices"):
        print_df("invoices key health", q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT invoice_id) AS distinct_invoice_id,
              SUM(CASE WHEN invoice_id IS NULL OR trim(invoice_id) = '' THEN 1 ELSE 0 END) AS null_invoice_id,
              COUNT(DISTINCT job_id) AS distinct_job_id_in_invoices,
              SUM(CASE WHEN job_id IS NULL OR trim(job_id) = '' THEN 1 ELSE 0 END) AS null_job_id_in_invoices
            FROM invoices
        """))

    if table_exists(conn, "invoice_items"):
        print_df("invoice_items key health", q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT invoice_id) AS distinct_invoice_id,
              COUNT(DISTINCT job_id) AS distinct_job_id,
              COUNT(DISTINCT item_id) AS distinct_item_id,
              SUM(CASE WHEN invoice_id IS NULL OR trim(invoice_id) = '' THEN 1 ELSE 0 END) AS null_invoice_id,
              SUM(CASE WHEN item_id IS NULL OR trim(item_id) = '' THEN 1 ELSE 0 END) AS null_item_id
            FROM invoice_items
        """))

    if table_exists(conn, "job_appointments"):
        print_df("job_appointments key health", q(conn, """
            SELECT
              COUNT(*) AS rows,
              COUNT(DISTINCT job_id) AS distinct_job_id,
              COUNT(DISTINCT appointment_id) AS distinct_appointment_id,
              COUNT(DISTINCT job_id || '|' || appointment_id || '|' || coalesce(dispatched_employee_id,'')) AS distinct_triplet,
              SUM(CASE WHEN appointment_id IS NULL OR trim(appointment_id) = '' THEN 1 ELSE 0 END) AS null_appointment_id,
              SUM(CASE WHEN job_id IS NULL OR trim(job_id) = '' THEN 1 ELSE 0 END) AS null_job_id,
              SUM(CASE WHEN dispatched_employee_id IS NULL OR trim(dispatched_employee_id) = '' THEN 1 ELSE 0 END) AS null_dispatched_employee_id
            FROM job_appointments
        """))

    print("\nDATE COVERAGE (YTD + ALL TIME)")
    ytd_start = "DATE_TRUNC('year', CURRENT_DATE)"

    if table_exists(conn, "invoices"):
        print_df("invoices columns present", pd.DataFrame([has_cols(conn, "invoices", ["invoice_date", "service_date", "paid_at", "status"])]))

        print_df("invoices invoice_date coverage (all-time)", q(conn, f"""
            WITH base AS (
              SELECT
                {safe_cast_ts_expr("invoice_date")} AS invoice_dt
              FROM invoices
            )
            SELECT
              COUNT(*) AS rows,
              SUM(CASE WHEN invoice_dt IS NULL THEN 1 ELSE 0 END) AS null_invoice_date,
              MIN(invoice_dt) AS min_invoice_dt,
              MAX(invoice_dt) AS max_invoice_dt
            FROM base
        """))

        print_df("invoice statuses (all-time)", q(conn, f"""
            SELECT
              lower(trim(status)) AS status,
              COUNT(*) AS cnt,
              MIN({safe_cast_ts_expr("invoice_date")}) AS min_dt,
              MAX({safe_cast_ts_expr("invoice_date")}) AS max_dt
            FROM invoices
            GROUP BY 1
            ORDER BY cnt DESC
        """))

    if table_exists(conn, "job_appointments"):
        print_df("job_appointments columns present", pd.DataFrame([has_cols(conn, "job_appointments", ["start_time", "end_time", "appointment_id", "dispatched_employee_id"])]))

        print_df("appointments start/end coverage (YTD)", q(conn, f"""
            WITH base AS (
              SELECT
                {safe_cast_ts_expr("start_time")} AS start_dt,
                {safe_cast_ts_expr("end_time")} AS end_dt
              FROM job_appointments
              WHERE {safe_cast_ts_expr("start_time")} >= {ytd_start}
            )
            SELECT
              COUNT(*) AS rows_ytd,
              SUM(CASE WHEN start_dt IS NULL THEN 1 ELSE 0 END) AS null_start_ytd,
              SUM(CASE WHEN end_dt IS NULL THEN 1 ELSE 0 END) AS null_end_ytd,
              MIN(start_dt) AS min_start_ytd,
              MAX(start_dt) AS max_start_ytd
            FROM base
        """))

        print_df("appointment duration quality (YTD)", q(conn, f"""
            WITH base AS (
              SELECT
                {safe_cast_ts_expr("start_time")} AS start_dt,
                {safe_cast_ts_expr("end_time")} AS end_dt
              FROM job_appointments
              WHERE {safe_cast_ts_expr("start_time")} >= {ytd_start}
            ),
            dur AS (
              SELECT DATE_DIFF('minute', start_dt, end_dt) AS minutes
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
        """))

    print("\nJOIN COVERAGE TESTS")

    if table_exists(conn, "invoices") and table_exists(conn, "jobs"):
        print_df("invoice.job_id -> jobs.job_id coverage (YTD)", q(conn, f"""
            WITH inv AS (
              SELECT invoice_id, job_id, {safe_cast_ts_expr("invoice_date")} AS invoice_dt
              FROM invoices
              WHERE {safe_cast_ts_expr("invoice_date")} >= {ytd_start}
            ),
            j AS (SELECT job_id FROM jobs)
            SELECT
              COUNT(*) AS inv_rows_ytd,
              SUM(CASE WHEN inv.job_id IS NULL OR trim(inv.job_id) = '' THEN 1 ELSE 0 END) AS inv_null_job_id,
              SUM(CASE WHEN j.job_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_jobs,
              SUM(CASE WHEN j.job_id IS NULL THEN 1 ELSE 0 END) AS unmatched_jobs
            FROM inv
            LEFT JOIN j ON inv.job_id = j.job_id
        """))

    if table_exists(conn, "job_appointments") and table_exists(conn, "employees"):
        print_df("job_appointments.dispatched_employee_id -> employees.employee_id coverage (YTD)", q(conn, f"""
            WITH ap AS (
              SELECT dispatched_employee_id AS emp_id
              FROM job_appointments
              WHERE {safe_cast_ts_expr("start_time")} >= {ytd_start}
                AND dispatched_employee_id IS NOT NULL
                AND trim(dispatched_employee_id) <> ''
            ),
            e AS (SELECT employee_id FROM employees)
            SELECT
              COUNT(*) AS appt_rows_ytd,
              COUNT(DISTINCT ap.emp_id) AS distinct_dispatched_emp_ids_ytd,
              SUM(CASE WHEN e.employee_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_rows,
              SUM(CASE WHEN e.employee_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
            FROM ap
            LEFT JOIN e ON ap.emp_id = e.employee_id
        """))

    print("\nINVOICE ITEMS SHAPE (BILLED SIDE PROXIES)")

    if table_exists(conn, "invoice_items") and table_exists(conn, "invoices"):
        print_df("invoice_items types (YTD)", q(conn, f"""
            SELECT
              lower(trim(it.type)) AS type,
              COUNT(*) AS cnt
            FROM invoice_items it
            JOIN invoices inv ON it.invoice_id = inv.invoice_id
            WHERE {safe_cast_ts_expr("inv.invoice_date")} >= {ytd_start}
            GROUP BY 1
            ORDER BY cnt DESC
        """))

        # Fix ambiguous amount: use it.amount
        print_df("invoice_items null rates (YTD)", q(conn, f"""
            SELECT
              COUNT(*) AS rows,
              SUM(CASE WHEN it.qty_in_hundredths IS NULL THEN 1 ELSE 0 END) AS null_qty,
              SUM(CASE WHEN it.unit_cost IS NULL THEN 1 ELSE 0 END) AS null_unit_cost,
              SUM(CASE WHEN it.unit_price IS NULL THEN 1 ELSE 0 END) AS null_unit_price,
              SUM(CASE WHEN it.amount IS NULL THEN 1 ELSE 0 END) AS null_amount
            FROM invoice_items it
            JOIN invoices inv ON it.invoice_id = inv.invoice_id
            WHERE {safe_cast_ts_expr("inv.invoice_date")} >= {ytd_start}
        """))

        # New: billed hours candidate from qty_in_hundredths on labor lines
        print_df("labor qty_in_hundredths coverage + implied hours (YTD)", q(conn, f"""
            WITH base AS (
              SELECT
                it.qty_in_hundredths,
                (it.qty_in_hundredths / 100.0) AS qty_hours
              FROM invoice_items it
              JOIN invoices inv ON it.invoice_id = inv.invoice_id
              WHERE {safe_cast_ts_expr("inv.invoice_date")} >= {ytd_start}
                AND lower(trim(it.type)) = 'labor'
            )
            SELECT
              COUNT(*) AS labor_rows,
              SUM(CASE WHEN qty_in_hundredths IS NULL THEN 1 ELSE 0 END) AS null_qty,
              SUM(CASE WHEN qty_in_hundredths IS NOT NULL AND qty_in_hundredths <= 0 THEN 1 ELSE 0 END) AS nonpositive_qty,
              MIN(qty_hours) AS min_hours,
              APPROX_QUANTILE(qty_hours, 0.50) AS p50_hours,
              APPROX_QUANTILE(qty_hours, 0.90) AS p90_hours,
              MAX(qty_hours) AS max_hours
            FROM base
        """))

        # New: are labor rows “per-visit” with qty missing? show top labor names with qty stats
        print_df("labor item names with qty stats (YTD)", q(conn, f"""
            SELECT
              it.name,
              COUNT(*) AS cnt,
              SUM(CASE WHEN it.qty_in_hundredths IS NULL THEN 1 ELSE 0 END) AS null_qty_cnt,
              APPROX_QUANTILE(it.qty_in_hundredths / 100.0, 0.50) AS p50_hours_if_present
            FROM invoice_items it
            JOIN invoices inv ON it.invoice_id = inv.invoice_id
            WHERE {safe_cast_ts_expr("inv.invoice_date")} >= {ytd_start}
              AND lower(trim(it.type)) = 'labor'
            GROUP BY 1
            ORDER BY cnt DESC
            LIMIT 50
        """))

    print("\nPRICEBOOK SERVICES COVERAGE")

    if table_exists(conn, "pricebook_services"):
        print_df("pricebook_services completeness + duration stats", q(conn, """
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
        """))

        print_df("pricebook_services sample (latest updated)", q(conn, """
            SELECT name, duration, cost, price, updated_at
            FROM pricebook_services
            ORDER BY try_cast(updated_at AS TIMESTAMP) DESC
            LIMIT 25
        """))

        # New: can we map invoice labor item names to pricebook service names?
        # This is a fuzzy “contains” check. It will be slow if pricebook is huge.
        # We cap invoice labor names to top 50 (YTD) so it's manageable.
        if table_exists(conn, "invoice_items") and table_exists(conn, "invoices"):
            print_df("invoice labor name -> pricebook name mapping (top 50 labor names, YTD)", q(conn, f"""
                WITH top_labor AS (
                  SELECT
                    lower(trim(it.name)) AS labor_name,
                    COUNT(*) AS cnt
                  FROM invoice_items it
                  JOIN invoices inv ON it.invoice_id = inv.invoice_id
                  WHERE {safe_cast_ts_expr("inv.invoice_date")} >= {ytd_start}
                    AND lower(trim(it.type)) = 'labor'
                    AND it.name IS NOT NULL
                    AND trim(it.name) <> ''
                  GROUP BY 1
                  ORDER BY cnt DESC
                  LIMIT 50
                )
                SELECT
                  tl.labor_name,
                  tl.cnt,
                  COUNT(*) FILTER (WHERE pb.service_uuid IS NOT NULL) AS matched_services
                FROM top_labor tl
                LEFT JOIN pricebook_services pb
                  ON lower(pb.name) LIKE '%' || tl.labor_name || '%'
                  OR tl.labor_name LIKE '%' || lower(pb.name) || '%'
                GROUP BY 1,2
                ORDER BY matched_services DESC, cnt DESC
            """), max_rows=200)

    print("\nINVOICE-LEVEL RECONCILIATION: billed hours vs paid hours (YTD)")

    # New: This is the most important KPI feasibility check.
    # It creates a single view per invoice/job:
    # - billed_hours_from_qty: sum(qty_in_hundredths)/100 for labor items
    # - paid_hours_from_appts: sum(appointment minutes)/60
    if table_exists(conn, "invoices") and table_exists(conn, "invoice_items") and table_exists(conn, "job_appointments"):
        recon = q(conn, f"""
            WITH inv AS (
              SELECT
                invoice_id,
                job_id,
                lower(trim(status)) AS status,
                {safe_cast_ts_expr("invoice_date")} AS invoice_dt
              FROM invoices
              WHERE {safe_cast_ts_expr("invoice_date")} >= {ytd_start}
                AND lower(trim(status)) IN ('paid','open')
                AND job_id IS NOT NULL
                AND trim(job_id) <> ''
            ),
            billed AS (
              SELECT
                it.invoice_id,
                SUM(CASE
                      WHEN lower(trim(it.type)) = 'labor' AND it.qty_in_hundredths IS NOT NULL
                      THEN it.qty_in_hundredths
                      ELSE 0
                    END) / 100.0 AS billed_hours_from_qty,
                COUNT(*) FILTER (WHERE lower(trim(it.type))='labor') AS labor_line_cnt,
                SUM(CASE WHEN lower(trim(it.type))='labor' AND it.qty_in_hundredths IS NULL THEN 1 ELSE 0 END) AS labor_lines_missing_qty
              FROM invoice_items it
              GROUP BY 1
            ),
            paid AS (
              SELECT
                job_id,
                SUM(
                  CASE
                    WHEN {safe_cast_ts_expr("start_time")} IS NOT NULL AND {safe_cast_ts_expr("end_time")} IS NOT NULL
                    THEN DATE_DIFF('minute', {safe_cast_ts_expr("start_time")}, {safe_cast_ts_expr("end_time")})
                    ELSE 0
                  END
                ) / 60.0 AS paid_hours_from_appts,
                COUNT(*) AS appt_rows
              FROM job_appointments
              WHERE {safe_cast_ts_expr("start_time")} >= {ytd_start}
              GROUP BY 1
            )
            SELECT
              inv.invoice_id,
              inv.job_id,
              inv.status,
              inv.invoice_dt,
              billed.billed_hours_from_qty,
              billed.labor_line_cnt,
              billed.labor_lines_missing_qty,
              paid.paid_hours_from_appts,
              paid.appt_rows,
              CASE
                WHEN paid.paid_hours_from_appts IS NULL OR paid.paid_hours_from_appts = 0 THEN NULL
                ELSE billed.billed_hours_from_qty / paid.paid_hours_from_appts
              END AS billed_to_paid_ratio
            FROM inv
            LEFT JOIN billed ON inv.invoice_id = billed.invoice_id
            LEFT JOIN paid ON inv.job_id = paid.job_id
            ORDER BY inv.invoice_dt DESC
            LIMIT 200
        """)
        print_df("invoice/job billed vs paid reconciliation sample (YTD)", recon, max_rows=200)

        print_df("reconciliation summary stats (YTD)", q(conn, f"""
            WITH recon AS (
              WITH inv AS (
                SELECT invoice_id, job_id
                FROM invoices
                WHERE {safe_cast_ts_expr("invoice_date")} >= {ytd_start}
                  AND lower(trim(status)) IN ('paid','open')
                  AND job_id IS NOT NULL
                  AND trim(job_id) <> ''
              ),
              billed AS (
                SELECT
                  invoice_id,
                  SUM(CASE WHEN lower(trim(type))='labor' AND qty_in_hundredths IS NOT NULL THEN qty_in_hundredths ELSE 0 END)/100.0 AS billed_hours
                FROM invoice_items
                GROUP BY 1
              ),
              paid AS (
                SELECT
                  job_id,
                  SUM(CASE WHEN {safe_cast_ts_expr("start_time")} IS NOT NULL AND {safe_cast_ts_expr("end_time")} IS NOT NULL
                           THEN DATE_DIFF('minute', {safe_cast_ts_expr("start_time")}, {safe_cast_ts_expr("end_time")})
                           ELSE 0 END)/60.0 AS paid_hours
                FROM job_appointments
                WHERE {safe_cast_ts_expr("start_time")} >= {ytd_start}
                GROUP BY 1
              )
              SELECT
                inv.invoice_id,
                inv.job_id,
                billed.billed_hours,
                paid.paid_hours
              FROM inv
              LEFT JOIN billed ON inv.invoice_id = billed.invoice_id
              LEFT JOIN paid ON inv.job_id = paid.job_id
            )
            SELECT
              COUNT(*) AS invoices_ytd,
              SUM(CASE WHEN billed_hours IS NOT NULL AND billed_hours > 0 THEN 1 ELSE 0 END) AS invoices_with_billed_hours,
              SUM(CASE WHEN paid_hours IS NOT NULL AND paid_hours > 0 THEN 1 ELSE 0 END) AS invoices_with_paid_hours,
              SUM(CASE WHEN (billed_hours IS NOT NULL AND billed_hours > 0) AND (paid_hours IS NOT NULL AND paid_hours > 0) THEN 1 ELSE 0 END) AS invoices_with_both,
              APPROX_QUANTILE(billed_hours, 0.50) AS p50_billed_hours,
              APPROX_QUANTILE(paid_hours, 0.50) AS p50_paid_hours
            FROM recon
        """))

    conn.close()

if __name__ == "__main__":
    main()
