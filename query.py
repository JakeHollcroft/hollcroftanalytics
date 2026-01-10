# query_test_data.py
# Run: python query_test_data.py
# Optional: set PERSIST_DIR to your data folder (default ".")
#
# This script prints schemas + sanity checks for the new tables:
# employees, estimates, estimate_options, estimate_employees, invoice_items, job_appointments

import os
from pathlib import Path

import duckdb
import pandas as pd

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 180)

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"


def header(title: str):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def show_schema(conn, table: str):
    header(f"SCHEMA: {table}")
    try:
        print(conn.execute(f"DESCRIBE {table}").df())
    except Exception as e:
        print(f"Could not DESCRIBE {table}: {e}")


def show_count(conn, table: str):
    try:
        n = conn.execute(f"SELECT COUNT(*) AS rows FROM {table}").df().iloc[0, 0]
        print(f"{table}: {n:,} rows")
    except Exception as e:
        print(f"{table}: count failed ({e})")


def show_sample(conn, table: str, order_by: str | None = None, limit: int = 25):
    header(f"SAMPLE: {table} (limit {limit})")
    try:
        if order_by:
            q = f"SELECT * FROM {table} ORDER BY {order_by} DESC LIMIT {limit}"
        else:
            q = f"SELECT * FROM {table} LIMIT {limit}"
        print(conn.execute(q).df())
    except Exception as e:
        print(f"Sample failed for {table}: {e}")


def main():
    print(f"DB_FILE = {DB_FILE.resolve()}")
    conn = duckdb.connect(DB_FILE)

    # ---- core tables ----
    for t in ["metadata", "jobs", "job_employees", "customers", "invoices"]:
        show_schema(conn, t)

    # ---- new tables we added for future KPIs ----
    for t in [
        "employees",
        "job_appointments",
        "invoice_items",
        "estimates",
        "estimate_options",
        "estimate_employees",
    ]:
        show_schema(conn, t)

    # ---- row counts (quick health check) ----
    header("ROW COUNTS")
    for t in [
        "jobs",
        "job_employees",
        "customers",
        "invoices",
        "invoice_items",
        "employees",
        "job_appointments",
        "estimates",
        "estimate_options",
        "estimate_employees",
    ]:
        show_count(conn, t)

    # ---- last refresh metadata ----
    header("METADATA")
    try:
        print(conn.execute("SELECT * FROM metadata ORDER BY key").df())
    except Exception as e:
        print(f"Metadata read failed: {e}")

    # ---- samples ----
    show_sample(conn, "jobs", order_by="updated_at", limit=30)
    show_sample(conn, "invoices", order_by="invoice_date", limit=30)
    show_sample(conn, "invoice_items", order_by=None, limit=30)
    show_sample(conn, "employees", order_by=None, limit=30)
    show_sample(conn, "job_appointments", order_by=None, limit=30)
    show_sample(conn, "estimates", order_by="updated_at", limit=30)
    show_sample(conn, "estimate_options", order_by="updated_at", limit=30)
    show_sample(conn, "estimate_employees", order_by=None, limit=30)

    # ---- tags sanity ----
    header("DISTINCT JOB TAGS (top 200)")
    try:
        # tags stored as comma-separated string; show top values + counts
        print(
            conn.execute(
                """
                SELECT tags, COUNT(*) AS job_count
                FROM jobs
                WHERE tags IS NOT NULL AND tags <> ''
                GROUP BY tags
                ORDER BY job_count DESC
                LIMIT 200
                """
            ).df()
        )
    except Exception as e:
        print(f"Tags query failed: {e}")

    # ---- critical join sanity: do invoice items map to jobs & techs? ----
    header("JOIN CHECK: invoice_items -> invoices -> jobs -> job_employees (sample 50)")
    try:
        print(
            conn.execute(
                """
                SELECT
                    ii.invoice_id,
                    i.invoice_number,
                    i.status,
                    i.invoice_date,
                    ii.type AS item_type,
                    ii.name AS item_name,
                    ii.amount AS item_amount,
                    ii.unit_cost AS item_unit_cost,
                    j.job_id,
                    j.work_status,
                    j.updated_at AS job_updated_at,
                    je.employee_id,
                    je.first_name || ' ' || je.last_name AS tech_name,
                    je.role AS tech_role
                FROM invoice_items ii
                LEFT JOIN invoices i ON i.invoice_id = ii.invoice_id
                LEFT JOIN jobs j ON j.job_id = ii.job_id
                LEFT JOIN job_employees je ON je.job_id = j.job_id
                ORDER BY i.invoice_date DESC NULLS LAST
                LIMIT 50
                """
            ).df()
        )
    except Exception as e:
        print(f"Join check failed: {e}")

    # ---- appointments sanity: do we have dispatched tech ids? ----
    header("APPOINTMENTS CHECK: dispatched tech IDs present (top 50)")
    try:
        print(
            conn.execute(
                """
                SELECT
                    ja.job_id,
                    ja.appointment_id,
                    ja.start_time,
                    ja.end_time,
                    ja.dispatched_employee_id,
                    e.first_name || ' ' || e.last_name AS dispatched_name
                FROM job_appointments ja
                LEFT JOIN employees e ON e.employee_id = ja.dispatched_employee_id
                ORDER BY ja.start_time DESC NULLS LAST
                LIMIT 50
                """
            ).df()
        )
    except Exception as e:
        print(f"Appointments check failed: {e}")

    # ---- estimate pipeline sanity: options + assigned employees ----
    header("ESTIMATES CHECK: options + assigned employees (top 50)")
    try:
        print(
            conn.execute(
                """
                SELECT
                    est.estimate_id,
                    est.estimate_number,
                    est.work_status,
                    est.updated_at,
                    opt.option_id,
                    opt.name AS option_name,
                    opt.status AS option_status,
                    opt.total_amount AS option_total_amount,
                    ee.employee_id,
                    e.first_name || ' ' || e.last_name AS assigned_employee_name
                FROM estimates est
                LEFT JOIN estimate_options opt ON opt.estimate_id = est.estimate_id
                LEFT JOIN estimate_employees ee ON ee.estimate_id = est.estimate_id
                LEFT JOIN employees e ON e.employee_id = ee.employee_id
                ORDER BY est.updated_at DESC NULLS LAST
                LIMIT 50
                """
            ).df()
        )
    except Exception as e:
        print(f"Estimates check failed: {e}")

    conn.close()
    header("DONE")


if __name__ == "__main__":
    main()
