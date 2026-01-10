# query.py
# Run: python query.py
# Optional: set PERSIST_DIR to your data folder (default ".")
#
# Goal: validate GP/hr inputs using actual schema:
# - Revenue: invoices.amount (cents)
# - Parts cost: invoice_items.unit_cost * (qty_in_hundredths/100)
# - Fixed labor: $525/job
# - Hours: job_appointments(start/end) (strings -> timestamps)
# - Tech attribution: job_employees (exclude installers via role)

import os
from pathlib import Path
import duckdb
import pandas as pd

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 220)

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

COMPLETED_STATUSES = {"complete unrated", "complete rated"}
FIXED_LABOR_COST_CENTS = 52500  # $525


def header(title: str):
    print("\n" + "=" * 110)
    print(title)
    print("=" * 110)


def describe(conn, table: str):
    header(f"DESCRIBE {table}")
    print(conn.execute(f"DESCRIBE {table}").df())


def count(conn, table: str):
    n = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()[0]
    print(f"{table}: {n:,} rows")


def main():
    print(f"DB_FILE = {DB_FILE.resolve()}")
    conn = duckdb.connect(DB_FILE)

    tables = ["jobs", "job_employees", "employees", "invoices", "invoice_items", "job_appointments"]

    header("1) SCHEMAS (GP/HR TABLES)")
    for t in tables:
        describe(conn, t)

    header("2) ROW COUNTS")
    for t in tables:
        count(conn, t)

    # ---------------------------------------------------------------------------------
    # 3) Revenue proof: invoices
    # ---------------------------------------------------------------------------------
    header("3) REVENUE PROOF: recent invoices + status distribution + zeros")
    print(
        conn.execute(
            """
            SELECT
                invoice_id,
                job_id,
                status,
                invoice_date,
                amount AS invoice_amount_cents,
                subtotal AS invoice_subtotal_cents,
                due_amount AS invoice_due_amount_cents
            FROM invoices
            ORDER BY invoice_date DESC NULLS LAST
            LIMIT 50
            """
        ).df()
    )

    print(
        conn.execute(
            """
            SELECT status, COUNT(*) AS n
            FROM invoices
            GROUP BY 1
            ORDER BY n DESC
            """
        ).df()
    )

    print(
        conn.execute(
            """
            SELECT
                COUNT(*) AS invoices_total,
                SUM(CASE WHEN amount = 0 OR amount IS NULL THEN 1 ELSE 0 END) AS invoices_amount_zero_or_null,
                SUM(CASE WHEN subtotal = 0 OR subtotal IS NULL THEN 1 ELSE 0 END) AS invoices_subtotal_zero_or_null,
                SUM(CASE WHEN due_amount = 0 OR due_amount IS NULL THEN 1 ELSE 0 END) AS invoices_due_amount_zero_or_null
            FROM invoices
            """
        ).df()
    )

    # Compare invoices.amount vs jobs.total_amount for same jobs (sanity)
    header("3B) SANITY: invoices.amount vs jobs.total_amount (recent 50 jobs with invoices)")
    print(
        conn.execute(
            """
            SELECT
                i.job_id,
                j.work_status,
                j.updated_at,
                j.total_amount AS job_total_amount_cents,
                SUM(i.amount) AS sum_invoice_amount_cents,
                SUM(i.subtotal) AS sum_invoice_subtotal_cents,
                COUNT(*) AS invoice_count,
                MAX(i.status) AS any_status
            FROM invoices i
            LEFT JOIN jobs j ON j.job_id = i.job_id
            WHERE i.job_id IS NOT NULL
            GROUP BY 1,2,3,4
            ORDER BY j.updated_at DESC NULLS LAST
            LIMIT 50
            """
        ).df()
    )

    # ---------------------------------------------------------------------------------
    # 4) Parts cost proof: invoice_items
    # ---------------------------------------------------------------------------------
    header("4) PARTS COST PROOF: raw invoice_items sample (no joins)")
    print(
        conn.execute(
            """
            SELECT
                invoice_id,
                job_id,
                item_id,
                type,
                name,
                qty_in_hundredths,
                unit_cost,
                unit_price,
                amount
            FROM invoice_items
            LIMIT 50
            """
        ).df()
    )

    header("4B) invoice_items type distribution")
    print(
        conn.execute(
            """
            SELECT type, COUNT(*) AS n
            FROM invoice_items
            GROUP BY 1
            ORDER BY n DESC
            """
        ).df()
    )

    header("4C) invoice_items join coverage: do invoice_ids match invoices?")
    print(
        conn.execute(
            """
            SELECT
                COUNT(*) AS items_total,
                SUM(CASE WHEN invoice_id IS NULL OR invoice_id = '' THEN 1 ELSE 0 END) AS items_invoice_id_null_or_blank,
                SUM(CASE WHEN job_id IS NULL OR job_id = '' THEN 1 ELSE 0 END) AS items_job_id_null_or_blank
            FROM invoice_items
            """
        ).df()
    )

    print(
        conn.execute(
            """
            SELECT
                COUNT(*) AS items_total,
                SUM(CASE WHEN i.invoice_id IS NOT NULL THEN 1 ELSE 0 END) AS items_with_matching_invoice,
                SUM(CASE WHEN i.invoice_id IS NULL THEN 1 ELSE 0 END) AS items_without_matching_invoice
            FROM invoice_items ii
            LEFT JOIN invoices i ON i.invoice_id = ii.invoice_id
            """
        ).df()
    )

    header("4D) invoice_items -> invoices -> jobs (recent items)")
    print(
        conn.execute(
            """
            SELECT
                ii.invoice_id,
                i.invoice_date,
                i.status AS invoice_status,
                i.job_id AS invoice_job_id,
                ii.job_id AS item_job_id,
                ii.type AS item_type,
                ii.name AS item_name,
                ii.qty_in_hundredths,
                ii.unit_cost,
                ii.unit_price,
                ii.amount AS item_amount
            FROM invoice_items ii
            LEFT JOIN invoices i ON i.invoice_id = ii.invoice_id
            ORDER BY i.invoice_date DESC NULLS LAST
            LIMIT 50
            """
        ).df()
    )

    # ---------------------------------------------------------------------------------
    # 5) Hours proof: appointments (cast strings to timestamps)
    # ---------------------------------------------------------------------------------
    header("5) HOURS PROOF: appointment hours sample (string -> TIMESTAMPTZ)")
    print(
        conn.execute(
            """
            SELECT
                job_id,
                appointment_id,
                start_time,
                end_time,
                (date_diff(
                    'minute',
                    try_cast(start_time AS TIMESTAMPTZ),
                    try_cast(end_time AS TIMESTAMPTZ)
                ) / 60.0) AS hours
            FROM job_appointments
            WHERE start_time IS NOT NULL AND end_time IS NOT NULL
            ORDER BY start_time DESC NULLS LAST
            LIMIT 50
            """
        ).df()
    )

    header("5B) HOURS: how many appointments have parsable timestamps?")
    print(
        conn.execute(
            """
            SELECT
                COUNT(*) AS appts_total,
                SUM(CASE WHEN try_cast(start_time AS TIMESTAMPTZ) IS NULL THEN 1 ELSE 0 END) AS start_unparsable,
                SUM(CASE WHEN try_cast(end_time AS TIMESTAMPTZ) IS NULL THEN 1 ELSE 0 END) AS end_unparsable
            FROM job_appointments
            """
        ).df()
    )

    # ---------------------------------------------------------------------------------
    # 6) PER-JOB BREAKDOWN: revenue, parts, labor, hours, tech attribution
    # ---------------------------------------------------------------------------------
    header("6) PER-JOB BREAKDOWN (last 25 completed jobs)")

    completed_list_sql = ", ".join([f"'{s}'" for s in sorted(COMPLETED_STATUSES)])

    df = conn.execute(
        f"""
        WITH completed_jobs AS (
            SELECT job_id, work_status, updated_at
            FROM jobs
            WHERE LOWER(work_status) IN ({completed_list_sql})
        ),
        rev AS (
            SELECT
                job_id,
                -- show both paid-only and all-nonvoid totals to learn the right revenue definition
                SUM(CASE WHEN LOWER(status) = 'paid' THEN amount ELSE 0 END) AS revenue_paid_cents,
                SUM(CASE WHEN LOWER(status) NOT IN ('voided','canceled') THEN amount ELSE 0 END) AS revenue_nonvoid_cents,
                COUNT(*) AS invoice_count,
                SUM(CASE WHEN amount = 0 OR amount IS NULL THEN 1 ELSE 0 END) AS invoice_amount_zero_cnt
            FROM invoices
            WHERE job_id IS NOT NULL
            GROUP BY 1
        ),
        parts AS (
            SELECT
                COALESCE(ii.job_id, i.job_id) AS job_id,
                -- qty is stored in hundredths
                SUM(
                    COALESCE(ii.unit_cost, 0)
                    * (COALESCE(ii.qty_in_hundredths, 0) / 100.0)
                ) AS parts_cost_cents_estimate,
                COUNT(*) AS item_count,
                SUM(CASE WHEN ii.unit_cost IS NULL OR ii.unit_cost = 0 THEN 1 ELSE 0 END) AS unit_cost_zero_cnt
            FROM invoice_items ii
            LEFT JOIN invoices i ON i.invoice_id = ii.invoice_id
            WHERE COALESCE(ii.job_id, i.job_id) IS NOT NULL
            GROUP BY 1
        ),
        appt_hours AS (
            SELECT
                job_id,
                SUM(
                    CASE
                        WHEN try_cast(start_time AS TIMESTAMPTZ) IS NOT NULL
                         AND try_cast(end_time AS TIMESTAMPTZ) IS NOT NULL
                        THEN date_diff(
                            'minute',
                            try_cast(start_time AS TIMESTAMPTZ),
                            try_cast(end_time AS TIMESTAMPTZ)
                        ) / 60.0
                        ELSE 0
                    END
                ) AS hours
            FROM job_appointments
            WHERE job_id IS NOT NULL
            GROUP BY 1
        ),
        techs AS (
            SELECT
                job_id,
                COUNT(DISTINCT employee_id) AS tech_count_non_install,
                STRING_AGG(DISTINCT TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')), ', ') AS tech_names
            FROM job_employees
            WHERE LOWER(COALESCE(role,'')) NOT LIKE '%install%'
            GROUP BY 1
        )
        SELECT
            cj.job_id,
            cj.work_status,
            cj.updated_at,
            COALESCE(r.invoice_count, 0) AS invoice_count,
            COALESCE(r.invoice_amount_zero_cnt, 0) AS invoice_amount_zero_cnt,
            COALESCE(r.revenue_paid_cents, 0) AS revenue_paid_cents,
            COALESCE(r.revenue_nonvoid_cents, 0) AS revenue_nonvoid_cents,
            COALESCE(p.item_count, 0) AS item_count,
            COALESCE(p.unit_cost_zero_cnt, 0) AS unit_cost_zero_cnt,
            COALESCE(p.parts_cost_cents_estimate, 0) AS parts_cost_cents_estimate,
            {FIXED_LABOR_COST_CENTS} AS labor_fixed_cents,
            COALESCE(a.hours, 0) AS hours,
            COALESCE(t.tech_count_non_install, 0) AS tech_count_non_install,
            t.tech_names
        FROM completed_jobs cj
        LEFT JOIN rev r ON r.job_id = cj.job_id
        LEFT JOIN parts p ON p.job_id = cj.job_id
        LEFT JOIN appt_hours a ON a.job_id = cj.job_id
        LEFT JOIN techs t ON t.job_id = cj.job_id
        ORDER BY cj.updated_at DESC NULLS LAST
        LIMIT 25
        """
    ).df()

    # Add readable dollars + gp/hr for both revenue definitions
    df["revenue_paid_$"] = df["revenue_paid_cents"] / 100.0
    df["revenue_nonvoid_$"] = df["revenue_nonvoid_cents"] / 100.0
    df["parts_cost_$"] = df["parts_cost_cents_estimate"] / 100.0
    df["labor_fixed_$"] = df["labor_fixed_cents"] / 100.0
    df["hours"] = df["hours"].astype(float)

    df["gp_paid_$"] = df["revenue_paid_$"] - df["parts_cost_$"] - df["labor_fixed_$"]
    df["gp_nonvoid_$"] = df["revenue_nonvoid_$"] - df["parts_cost_$"] - df["labor_fixed_$"]

    df["gp_per_hr_paid_$"] = df.apply(lambda r: (r["gp_paid_$"] / r["hours"]) if r["hours"] > 0 else 0.0, axis=1)
    df["gp_per_hr_nonvoid_$"] = df.apply(lambda r: (r["gp_nonvoid_$"] / r["hours"]) if r["hours"] > 0 else 0.0, axis=1)

    # flags
    df["FLAG_hours_zero"] = df["hours"] <= 0
    df["FLAG_parts_zero"] = df["parts_cost_cents_estimate"] <= 0
    df["FLAG_no_techs"] = df["tech_count_non_install"] <= 0
    df["FLAG_rev_paid_zero"] = df["revenue_paid_cents"] <= 0
    df["FLAG_rev_nonvoid_zero"] = df["revenue_nonvoid_cents"] <= 0

    print(
        df[
            [
                "job_id",
                "work_status",
                "updated_at",
                "invoice_count",
                "invoice_amount_zero_cnt",
                "item_count",
                "unit_cost_zero_cnt",
                "hours",
                "tech_count_non_install",
                "tech_names",
                "revenue_paid_$",
                "revenue_nonvoid_$",
                "parts_cost_$",
                "labor_fixed_$",
                "gp_paid_$",
                "gp_nonvoid_$",
                "gp_per_hr_paid_$",
                "gp_per_hr_nonvoid_$",
                "FLAG_hours_zero",
                "FLAG_parts_zero",
                "FLAG_no_techs",
                "FLAG_rev_paid_zero",
                "FLAG_rev_nonvoid_zero",
            ]
        ]
    )

    header("DONE")
    conn.close()


if __name__ == "__main__":
    main()
