# query.py
# Run: python query.py
# Optional: set PERSIST_DIR to your data folder (default ".")
#
# PURPOSE:
# Prove (with data) why Gross Profit / Hour is $0/hr.
# - Revenue basis: invoices.amount vs sum(invoice_items.amount)
# - Cost basis: invoice_items.unit_cost * qty_in_hundredths (materials + labor)
# - Hours basis: job_appointments duration vs jobs.scheduled_start/end fallback
# - Coverage stats: how many jobs have hours, revenue, costs
# - Per-job breakdown: last 50 completed jobs with flags

import os
from pathlib import Path
import duckdb
import pandas as pd

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 220)

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"


def header(title: str):
    print("\n" + "=" * 110)
    print(title)
    print("=" * 110)


def safe_df(conn, sql: str, title: str | None = None):
    if title:
        header(title)
    try:
        df = conn.execute(sql).df()
        print(df)
        return df
    except Exception as e:
        print(f"FAILED: {e}")
        return None


def table_exists(conn, name: str) -> bool:
    q = f"""
    SELECT notice
    FROM duckdb_tables()
    WHERE table_name = '{name}'
    """
    try:
        df = conn.execute(q).df()
        return len(df) > 0
    except Exception:
        # fallback
        try:
            df = conn.execute(
                f"SELECT 1 FROM information_schema.tables WHERE table_name = '{name}' LIMIT 1"
            ).df()
            return len(df) > 0
        except Exception:
            return False


def main():
    print(f"DB_FILE = {DB_FILE.resolve()}")
    conn = duckdb.connect(DB_FILE)

    # --------------------------------------------------------------------------------
    # 0) What tables exist (so we stop guessing)
    # --------------------------------------------------------------------------------
    header("0) TABLE INVENTORY (relevant to GP/HR)")
    safe_df(
        conn,
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND (
            table_name IN ('jobs','invoices','invoice_items','job_appointments','job_employees','employees')
            OR table_name ILIKE '%price%'
            OR table_name ILIKE '%service%'
            OR table_name ILIKE '%labor%'
            OR table_name ILIKE '%material%'
          )
        ORDER BY table_name
        """,
    )

    # --------------------------------------------------------------------------------
    # 1) Schemas we care about
    # --------------------------------------------------------------------------------
    header("1) SCHEMAS (GP/HR TABLES)")
    for t in ["jobs", "invoices", "invoice_items", "job_appointments", "job_employees", "employees"]:
        safe_df(conn, f"DESCRIBE {t}", f"DESCRIBE {t}")

    # --------------------------------------------------------------------------------
    # 2) Row counts + coverage basics
    # --------------------------------------------------------------------------------
    header("2) ROW COUNTS + COVERAGE")
    safe_df(
        conn,
        """
        SELECT
          (SELECT COUNT(*) FROM jobs) AS jobs_rows,
          (SELECT COUNT(*) FROM invoices) AS invoices_rows,
          (SELECT COUNT(*) FROM invoice_items) AS invoice_items_rows,
          (SELECT COUNT(*) FROM job_appointments) AS job_appointments_rows
        """,
    )

    safe_df(
        conn,
        """
        SELECT
          COUNT(*) AS jobs_total,
          SUM(CASE WHEN completed_at IS NULL OR completed_at = '' THEN 1 ELSE 0 END) AS jobs_completed_at_missing,
          SUM(CASE WHEN scheduled_start IS NULL OR scheduled_start = '' THEN 1 ELSE 0 END) AS jobs_sched_start_missing,
          SUM(CASE WHEN scheduled_end   IS NULL OR scheduled_end   = '' THEN 1 ELSE 0 END) AS jobs_sched_end_missing
        FROM jobs
        """,
        "Jobs timestamp field null-rate (completed_at / scheduled_start / scheduled_end)",
    )

    # --------------------------------------------------------------------------------
    # 3) Revenue proof: invoice totals + zero-rate + status distribution
    # --------------------------------------------------------------------------------
    header("3) REVENUE PROOF")
    safe_df(
        conn,
        """
        SELECT status, COUNT(*) AS n
        FROM invoices
        GROUP BY status
        ORDER BY n DESC
        """,
        "Invoice status distribution",
    )

    safe_df(
        conn,
        """
        SELECT
          COUNT(*) AS invoices_total,
          SUM(CASE WHEN amount IS NULL OR amount = 0 THEN 1 ELSE 0 END) AS invoices_amount_zero_or_null,
          SUM(CASE WHEN subtotal IS NULL OR subtotal = 0 THEN 1 ELSE 0 END) AS invoices_subtotal_zero_or_null
        FROM invoices
        """,
        "Invoice amount/subtotal zero-rate (this matters if you use invoices.amount for revenue)",
    )

    safe_df(
        conn,
        """
        SELECT
          invoice_id,
          job_id,
          status,
          invoice_date,
          amount   AS invoice_amount_cents,
          subtotal AS invoice_subtotal_cents
        FROM invoices
        ORDER BY invoice_date DESC NULLS LAST
        LIMIT 30
        """,
        "Recent invoices (showing amount/subtotal as stored)",
    )

    # --------------------------------------------------------------------------------
    # 4) Invoice items proof: do we have cost + qty for materials + labor?
    # --------------------------------------------------------------------------------
    header("4) INVOICE_ITEMS PROOF (REVENUE + COST INPUTS)")
    safe_df(
        conn,
        """
        SELECT type, COUNT(*) AS n
        FROM invoice_items
        GROUP BY type
        ORDER BY n DESC
        """,
        "invoice_items type distribution",
    )

    safe_df(
        conn,
        """
        SELECT
          COUNT(*) AS items_total,
          SUM(CASE WHEN unit_cost IS NULL THEN 1 ELSE 0 END) AS unit_cost_null,
          SUM(CASE WHEN unit_cost = 0 THEN 1 ELSE 0 END) AS unit_cost_zero,
          SUM(CASE WHEN qty_in_hundredths IS NULL THEN 1 ELSE 0 END) AS qty_null,
          SUM(CASE WHEN qty_in_hundredths = 0 THEN 1 ELSE 0 END) AS qty_zero,
          SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_null,
          SUM(CASE WHEN amount = 0 THEN 1 ELSE 0 END) AS amount_zero
        FROM invoice_items
        """,
        "invoice_items null/zero profile (unit_cost / qty_in_hundredths / amount)",
    )

    safe_df(
        conn,
        """
        SELECT
          invoice_id, job_id, type, name, description,
          qty_in_hundredths, unit_cost, unit_price, amount
        FROM invoice_items
        ORDER BY invoice_id DESC
        LIMIT 30
        """,
        "Recent invoice_items sample (raw)",
    )

    # Revenue cross-check: invoice.amount vs sum(invoice_items.amount) by invoice
    safe_df(
        conn,
        """
        SELECT
          i.invoice_id,
          i.status,
          i.invoice_date,
          i.amount AS invoice_amount_cents,
          SUM(ii.amount) AS sum_item_amount_cents,
          SUM(CASE WHEN ii.type='material' THEN ii.amount ELSE 0 END) AS material_revenue_cents,
          SUM(CASE WHEN ii.type='labor' THEN ii.amount ELSE 0 END) AS labor_revenue_cents,
          (SUM(ii.amount) - i.amount) AS diff_items_minus_invoice_cents
        FROM invoices i
        LEFT JOIN invoice_items ii ON ii.invoice_id = i.invoice_id
        GROUP BY 1,2,3,4
        ORDER BY i.invoice_date DESC NULLS LAST
        LIMIT 30
        """,
        "Revenue check: invoices.amount vs SUM(invoice_items.amount) (per invoice)",
    )

    # Cost compute preview by invoice_items: unit_cost * qty (qty_in_hundredths/100)
    safe_df(
        conn,
        """
        SELECT
          ii.invoice_id,
          ii.job_id,
          ii.type,
          ii.name,
          ii.qty_in_hundredths,
          ii.unit_cost,
          (ii.qty_in_hundredths / 100.0) AS qty,
          (ii.unit_cost * (ii.qty_in_hundredths / 100.0)) AS computed_cost_cents,
          ii.amount AS item_amount_cents
        FROM invoice_items ii
        WHERE (ii.unit_cost IS NOT NULL AND ii.unit_cost <> 0)
        ORDER BY ii.invoice_id DESC
        LIMIT 40
        """,
        "Cost check: unit_cost * qty (shows what cost would be if you use these columns)",
    )

    # --------------------------------------------------------------------------------
    # 5) Hours proof: appointments vs jobs.scheduled_start/end
    # --------------------------------------------------------------------------------
    header("5) HOURS PROOF")
    safe_df(
        conn,
        """
        SELECT
          COUNT(*) AS appts_total,
          SUM(CASE WHEN try_cast(start_time AS TIMESTAMPTZ) IS NULL THEN 1 ELSE 0 END) AS start_unparsable,
          SUM(CASE WHEN try_cast(end_time   AS TIMESTAMPTZ) IS NULL THEN 1 ELSE 0 END) AS end_unparsable
        FROM job_appointments
        """,
        "Appointments timestamp parse rate",
    )

    safe_df(
        conn,
        """
        SELECT
          job_id,
          appointment_id,
          start_time,
          end_time,
          ROUND(
            date_diff('minute', try_cast(start_time AS TIMESTAMPTZ), try_cast(end_time AS TIMESTAMPTZ)) / 60.0,
            2
          ) AS hours
        FROM job_appointments
        ORDER BY start_time DESC NULLS LAST
        LIMIT 30
        """,
        "Recent job_appointments with computed hours",
    )

    safe_df(
        conn,
        """
        SELECT
          COUNT(*) AS jobs_total,
          SUM(CASE WHEN try_cast(scheduled_start AS TIMESTAMPTZ) IS NULL THEN 1 ELSE 0 END) AS sched_start_unparsable,
          SUM(CASE WHEN try_cast(scheduled_end   AS TIMESTAMPTZ) IS NULL THEN 1 ELSE 0 END) AS sched_end_unparsable
        FROM jobs
        """,
        "Jobs scheduled_start/end timestamp parse rate",
    )

    safe_df(
        conn,
        """
        SELECT
          job_id,
          work_status,
          scheduled_start,
          scheduled_end,
          ROUND(
            date_diff('minute', try_cast(scheduled_start AS TIMESTAMPTZ), try_cast(scheduled_end AS TIMESTAMPTZ)) / 60.0,
            2
          ) AS sched_hours
        FROM jobs
        WHERE scheduled_start IS NOT NULL AND scheduled_start <> ''
          AND scheduled_end   IS NOT NULL AND scheduled_end <> ''
        ORDER BY updated_at DESC NULLS LAST
        LIMIT 30
        """,
        "Jobs scheduled_start/end computed hours (sample)",
    )

    # --------------------------------------------------------------------------------
    # 6) PER-JOB breakdown (the GP/HR smoking gun)
    #     Revenue: sum invoice_items.amount on non-voided/canceled invoices
    #     Cost: sum(unit_cost * qty) across material + labor (same filter)
    #     Hours: sum appointment hours, fallback to scheduled hours
    # --------------------------------------------------------------------------------
    header("6) PER-JOB BREAKDOWN (last 50 completed jobs)")

    safe_df(
        conn,
        """
        WITH inv_good AS (
          SELECT *
          FROM invoices
          WHERE status NOT IN ('voided','canceled')
        ),
        job_rev_cost AS (
          SELECT
            j.job_id,
            j.work_status,
            j.updated_at,
            -- Revenue based on invoice_items (most reliable when invoice totals are 0)
            COALESCE(SUM(CASE WHEN ig.invoice_id IS NOT NULL THEN ii.amount ELSE 0 END), 0) AS revenue_cents_items,
            -- Revenue based on invoice header amounts (can be 0 often)
            COALESCE(SUM(CASE WHEN ig.invoice_id IS NOT NULL THEN ig.amount ELSE 0 END), 0) AS revenue_cents_invoice,
            -- Cost from invoice_items: unit_cost * qty (qty_in_hundredths/100)
            COALESCE(SUM(
              CASE
                WHEN ig.invoice_id IS NOT NULL
                  AND ii.unit_cost IS NOT NULL
                  AND ii.qty_in_hundredths IS NOT NULL
                THEN (ii.unit_cost * (ii.qty_in_hundredths / 100.0))
                ELSE 0
              END
            ), 0) AS cost_cents_unitcost_qty,
            COUNT(DISTINCT ig.invoice_id) AS invoice_count
          FROM jobs j
          LEFT JOIN inv_good ig ON ig.job_id = j.job_id
          LEFT JOIN invoice_items ii ON ii.invoice_id = ig.invoice_id
          WHERE j.work_status IN ('complete unrated','complete rated')
          GROUP BY 1,2,3
        ),
        appt_hours AS (
          SELECT
            job_id,
            COALESCE(SUM(
              CASE
                WHEN try_cast(start_time AS TIMESTAMPTZ) IS NOT NULL
                 AND try_cast(end_time AS TIMESTAMPTZ)   IS NOT NULL
                THEN date_diff('minute', try_cast(start_time AS TIMESTAMPTZ), try_cast(end_time AS TIMESTAMPTZ)) / 60.0
                ELSE 0
              END
            ), 0) AS appt_hours
          FROM job_appointments
          GROUP BY 1
        ),
        sched_hours AS (
          SELECT
            job_id,
            CASE
              WHEN try_cast(scheduled_start AS TIMESTAMPTZ) IS NOT NULL
               AND try_cast(scheduled_end   AS TIMESTAMPTZ) IS NOT NULL
              THEN date_diff('minute', try_cast(scheduled_start AS TIMESTAMPTZ), try_cast(scheduled_end AS TIMESTAMPTZ)) / 60.0
              ELSE 0
            END AS sched_hours
          FROM jobs
        )
        SELECT
          jrc.job_id,
          jrc.work_status,
          jrc.updated_at,
          jrc.invoice_count,
          ROUND(jrc.revenue_cents_items / 100.0, 2)   AS revenue_items_usd,
          ROUND(jrc.revenue_cents_invoice / 100.0, 2) AS revenue_invoice_usd,
          ROUND(jrc.cost_cents_unitcost_qty / 100.0, 2) AS cost_unitcost_qty_usd,
          ROUND((jrc.revenue_cents_items - jrc.cost_cents_unitcost_qty) / 100.0, 2) AS gross_profit_usd,
          ROUND(COALESCE(ah.appt_hours, 0), 2) AS appt_hours,
          ROUND(COALESCE(sh.sched_hours, 0), 2) AS sched_hours,
          ROUND(
            CASE
              WHEN COALESCE(ah.appt_hours, 0) > 0 THEN (jrc.revenue_cents_items - jrc.cost_cents_unitcost_qty) / 100.0 / ah.appt_hours
              WHEN COALESCE(sh.sched_hours, 0) > 0 THEN (jrc.revenue_cents_items - jrc.cost_cents_unitcost_qty) / 100.0 / sh.sched_hours
              ELSE NULL
            END,
            2
          ) AS gp_per_hour_usd,
          -- Flags to show why GP/HR becomes 0 in dashboards
          (COALESCE(ah.appt_hours, 0) = 0 AND COALESCE(sh.sched_hours, 0) = 0) AS FLAG_no_hours,
          (jrc.revenue_cents_items = 0) AS FLAG_no_revenue_items,
          (jrc.cost_cents_unitcost_qty = 0) AS FLAG_no_costs
        FROM job_rev_cost jrc
        LEFT JOIN appt_hours ah ON ah.job_id = jrc.job_id
        LEFT JOIN sched_hours sh ON sh.job_id = jrc.job_id
        ORDER BY jrc.updated_at DESC NULLS LAST
        LIMIT 50
        """,
        "Per-job GP/HR breakdown (this should explain $0/hr immediately)",
    )

    # --------------------------------------------------------------------------------
    # 7) Technician allocation sanity (optional, but helps validate card rollups)
    # --------------------------------------------------------------------------------
    header("7) TECH ALLOCATION CHECK (jobs -> job_employees)")
    safe_df(
        conn,
        """
        SELECT
          je.employee_id,
          COALESCE(NULLIF(je.first_name,''),'') || ' ' || COALESCE(NULLIF(je.last_name,''),'') AS tech_name,
          je.role,
          COUNT(DISTINCT je.job_id) AS jobs_linked
        FROM job_employees je
        GROUP BY 1,2,3
        ORDER BY jobs_linked DESC
        LIMIT 30
        """,
        "job_employees linkage (how many jobs per employee_id)",
    )

    conn.close()
    header("DONE")


if __name__ == "__main__":
    main()