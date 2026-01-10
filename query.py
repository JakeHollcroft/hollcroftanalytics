# query_test_data.py
# Run: python query_test_data.py
# Optional: set PERSIST_DIR to your data folder (default ".")
#
# Purpose: print ONLY the minimum evidence needed to validate GP/hr:
# Revenue (invoices) - Parts cost (invoice_items) - Fixed labor ($525/job) - Hours (item durations OR appointments)
# and how it attributes to NON-INSTALL techs (job_employees/employees).

import os
from pathlib import Path
import duckdb
import pandas as pd

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 220)

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

COMPLETED_STATUSES = {"complete unrated", "complete rated"}  # adjust if yours differ
FIXED_LABOR_COST_CENTS = 52500  # $525 fixed labor per job

TABLES = [
    "jobs",
    "job_employees",
    "employees",
    "invoices",
    "invoice_items",
    "job_appointments",
]


def header(title: str):
    print("\n" + "=" * 110)
    print(title)
    print("=" * 110)


def describe(conn, table: str):
    header(f"DESCRIBE {table}")
    try:
        print(conn.execute(f"DESCRIBE {table}").df())
    except Exception as e:
        print(f"DESCRIBE failed for {table}: {e}")


def cols(conn, table: str) -> list[str]:
    try:
        return conn.execute(f"DESCRIBE {table}").df()["column_name"].tolist()
    except Exception:
        return []


def pick_col(col_list: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in col_list:
            return c
    return None


def has_table(conn, table: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
        return True
    except Exception:
        return False


def main():
    print(f"DB_FILE = {DB_FILE.resolve()}")
    conn = duckdb.connect(DB_FILE)

    # ---- 1) Schemas (only the 6 tables needed) ----
    header("1) SCHEMAS (ONLY TABLES NEEDED FOR GP/HR)")
    for t in TABLES:
        if has_table(conn, t):
            describe(conn, t)
        else:
            print(f"\nMissing table: {t}")

    # Collect actual columns
    jobs_cols = cols(conn, "jobs")
    je_cols = cols(conn, "job_employees")
    emp_cols = cols(conn, "employees")
    inv_cols = cols(conn, "invoices")
    ii_cols = cols(conn, "invoice_items")
    appt_cols = cols(conn, "job_appointments")

    # ---- Column picks (based on what actually exists) ----
    # jobs
    jobs_job_id = pick_col(jobs_cols, ["job_id", "id"])
    jobs_status = pick_col(jobs_cols, ["work_status", "status"])
    jobs_updated = pick_col(jobs_cols, ["updated_at", "completed_at", "created_at", "job_date"])

    # invoices
    inv_invoice_id = pick_col(inv_cols, ["invoice_id", "id"])
    inv_job_id = pick_col(inv_cols, ["job_id"])
    inv_status = pick_col(inv_cols, ["status"])
    inv_date = pick_col(inv_cols, ["invoice_date", "created_at", "updated_at"])
    inv_total = pick_col(inv_cols, ["total_amount", "amount", "grand_total", "invoice_total", "total"])

    # invoice_items
    ii_invoice_id = pick_col(ii_cols, ["invoice_id"])
    ii_job_id = pick_col(ii_cols, ["job_id"])  # may or may not exist
    ii_type = pick_col(ii_cols, ["type", "item_type", "kind"])
    ii_name = pick_col(ii_cols, ["name", "item_name", "description"])
    ii_qty = pick_col(ii_cols, ["quantity", "qty", "item_quantity"])
    ii_unit_cost = pick_col(ii_cols, ["unit_cost", "cost", "unitCost"])
    ii_amount = pick_col(ii_cols, ["amount", "total_amount", "line_total"])

    # duration-ish columns (we don’t assume; we discover)
    duration_cols = [c for c in ii_cols if any(k in c.lower() for k in ["duration", "minute", "minutes", "hour", "hours"])]
    # appointments
    appt_job_id = pick_col(appt_cols, ["job_id"])
    appt_start = pick_col(appt_cols, ["start_time", "start", "starts_at"])
    appt_end = pick_col(appt_cols, ["end_time", "end", "ends_at"])

    # job_employees / employees
    je_job_id = pick_col(je_cols, ["job_id"])
    je_emp_id = pick_col(je_cols, ["employee_id"])
    je_role = pick_col(je_cols, ["role"])
    je_first = pick_col(je_cols, ["first_name"])
    je_last = pick_col(je_cols, ["last_name"])

    emp_emp_id = pick_col(emp_cols, ["employee_id", "id"])
    emp_first = pick_col(emp_cols, ["first_name"])
    emp_last = pick_col(emp_cols, ["last_name"])
    emp_role = pick_col(emp_cols, ["role"])

    header("COLUMN PICKS (THE SCRIPT WILL USE THESE)")
    print("jobs:", {"job_id": jobs_job_id, "status": jobs_status, "updated_at": jobs_updated})
    print("invoices:", {"invoice_id": inv_invoice_id, "job_id": inv_job_id, "status": inv_status, "date": inv_date, "total": inv_total})
    print("invoice_items:", {"invoice_id": ii_invoice_id, "job_id": ii_job_id, "type": ii_type, "name": ii_name, "qty": ii_qty, "unit_cost": ii_unit_cost, "amount": ii_amount})
    print("invoice_items duration-ish cols:", duration_cols)
    print("appointments:", {"job_id": appt_job_id, "start": appt_start, "end": appt_end})
    print("job_employees:", {"job_id": je_job_id, "employee_id": je_emp_id, "role": je_role, "first": je_first, "last": je_last})
    print("employees:", {"employee_id": emp_emp_id, "first": emp_first, "last": emp_last, "role": emp_role})

    # Hard requirements for revenue proof
    if not inv_total or not inv_invoice_id:
        header("STOP: Could not identify invoice total and/or invoice_id column")
        print("invoices columns:", inv_cols)
        conn.close()
        return

    # ---- 2) Revenue proof: invoices -> jobs ----
    header("2) REVENUE PROOF: Recent invoices + status distribution + job_id null-rate")

    # Recent invoices
    try:
        order_col = inv_date or inv_invoice_id
        q = f"""
        SELECT
            {inv_invoice_id} AS invoice_id
            {"," if inv_job_id else ""}
            {inv_job_id + " AS job_id" if inv_job_id else ""}
            {"," if inv_status else ""}
            {inv_status + " AS status" if inv_status else ""}
            {"," if inv_date else ""}
            {inv_date + " AS invoice_date" if inv_date else ""}
            , {inv_total} AS invoice_total
        FROM invoices
        ORDER BY {order_col} DESC NULLS LAST
        LIMIT 50
        """
        print(conn.execute(q).df())
    except Exception as e:
        print("Recent invoices query failed:", e)

    # Status counts
    if inv_status:
        try:
            q = f"""
            SELECT {inv_status} AS status, COUNT(*) AS n
            FROM invoices
            GROUP BY 1
            ORDER BY n DESC
            """
            print(conn.execute(q).df())
        except Exception as e:
            print("Invoice status distribution failed:", e)
    else:
        print("No invoices.status column found to summarize statuses.")

    # job_id null-rate
    if inv_job_id:
        try:
            q = f"""
            SELECT
                SUM(CASE WHEN {inv_job_id} IS NULL THEN 1 ELSE 0 END) AS invoices_job_id_null,
                COUNT(*) AS invoices_total
            FROM invoices
            """
            print(conn.execute(q).df())
        except Exception as e:
            print("Invoice job_id null-rate failed:", e)
    else:
        print("No invoices.job_id column found; revenue-to-job linkage might be via another key.")

    # ---- 3) Parts cost proof: invoice_items linkage + cost field sanity ----
    header("3) PARTS COST PROOF: invoice_items joined to invoices (and job_id if available)")

    # Join proof: invoice_items -> invoices (+ job_id)
    try:
        # Build select dynamically
        select_bits = []
        select_bits.append(f"ii.{ii_invoice_id} AS invoice_id" if ii_invoice_id else "NULL AS invoice_id")
        if ii_job_id:
            select_bits.append(f"ii.{ii_job_id} AS item_job_id")
        if inv_job_id:
            select_bits.append(f"i.{inv_job_id} AS invoice_job_id")
        if ii_type:
            select_bits.append(f"ii.{ii_type} AS item_type")
        if ii_name:
            select_bits.append(f"ii.{ii_name} AS item_name")
        if ii_qty:
            select_bits.append(f"ii.{ii_qty} AS qty")
        if ii_unit_cost:
            select_bits.append(f"ii.{ii_unit_cost} AS unit_cost")
        if ii_amount:
            select_bits.append(f"ii.{ii_amount} AS item_amount")
        if inv_total:
            select_bits.append(f"i.{inv_total} AS invoice_total")
        if inv_status:
            select_bits.append(f"i.{inv_status} AS invoice_status")
        if inv_date:
            select_bits.append(f"i.{inv_date} AS invoice_date")

        select_sql = ",\n            ".join(select_bits) if select_bits else "*"

        q = f"""
        SELECT
            {select_sql}
        FROM invoice_items ii
        LEFT JOIN invoices i
            ON i.{inv_invoice_id} = ii.{ii_invoice_id}
        ORDER BY {("i."+inv_date) if inv_date else ("i."+inv_invoice_id)} DESC NULLS LAST
        LIMIT 50
        """
        print(conn.execute(q).df())
    except Exception as e:
        print("invoice_items -> invoices join proof failed:", e)

    # Null-rate / sanity for qty & unit_cost
    if ii_qty and ii_unit_cost:
        try:
            q = f"""
            SELECT
                COUNT(*) AS items_total,
                SUM(CASE WHEN {ii_qty} IS NULL THEN 1 ELSE 0 END) AS qty_null,
                SUM(CASE WHEN {ii_unit_cost} IS NULL THEN 1 ELSE 0 END) AS unit_cost_null,
                SUM(CASE WHEN COALESCE({ii_qty}, 0) = 0 THEN 1 ELSE 0 END) AS qty_zero,
                SUM(CASE WHEN COALESCE({ii_unit_cost}, 0) = 0 THEN 1 ELSE 0 END) AS unit_cost_zero
            FROM invoice_items
            """
            print(conn.execute(q).df())
        except Exception as e:
            print("invoice_items qty/unit_cost null-rate failed:", e)
    else:
        print("Missing invoice_items.quantity/qty and/or unit_cost columns; cannot validate parts cost via unit_cost*qty.")

    # Item type distribution (helps determine what counts as parts)
    if ii_type:
        try:
            q = f"""
            SELECT {ii_type} AS item_type, COUNT(*) AS n
            FROM invoice_items
            GROUP BY 1
            ORDER BY n DESC
            LIMIT 50
            """
            print(conn.execute(q).df())
        except Exception as e:
            print("invoice_items type distribution failed:", e)
    else:
        print("No invoice_items.type column found; we may need to infer parts vs labor/services by name.")

    # ---- 4) Hours proof: durations if present, else appointments ----
    header("4) HOURS PROOF: duration columns (if any) + appointment hours fallback check")

    if duration_cols:
        print("Duration-ish columns found on invoice_items:", duration_cols)
        # Show a sample with those duration columns populated
        try:
            dur_select = ", ".join([f"ii.{c}" for c in duration_cols[:6]])  # cap at 6 to keep output readable
            base_cols = []
            if ii_invoice_id:
                base_cols.append(f"ii.{ii_invoice_id} AS invoice_id")
            if ii_name:
                base_cols.append(f"ii.{ii_name} AS item_name")
            if ii_type:
                base_cols.append(f"ii.{ii_type} AS item_type")
            if ii_qty:
                base_cols.append(f"ii.{ii_qty} AS qty")
            select_sql = ", ".join(base_cols + [dur_select])

            q = f"""
            SELECT {select_sql}
            FROM invoice_items ii
            WHERE {" OR ".join([f"ii.{c} IS NOT NULL" for c in duration_cols[:6]])}
            LIMIT 50
            """
            print(conn.execute(q).df())
        except Exception as e:
            print("Duration sample query failed:", e)
    else:
        print("No duration-ish columns found on invoice_items. Using appointments as the only hours evidence.")
        if appt_job_id and appt_start and appt_end:
            try:
                q = f"""
                SELECT
                    {appt_job_id} AS job_id,
                    {appt_start} AS start_time,
                    {appt_end} AS end_time,
                    DATE_DIFF('minute', {appt_start}, {appt_end}) / 60.0 AS hours
                FROM job_appointments
                WHERE {appt_start} IS NOT NULL AND {appt_end} IS NOT NULL
                ORDER BY {appt_start} DESC NULLS LAST
                LIMIT 50
                """
                print(conn.execute(q).df())
            except Exception as e:
                print("Appointment hours sample failed:", e)
        else:
            print("Missing job_appointments columns needed to compute hours (job_id/start/end).")

    # ---- 5) Per-job breakdown (last 25 completed jobs) ----
    header("5) PER-JOB BREAKDOWN (THIS IS THE GP/HR SMOKING GUN)")

    if not (jobs_job_id and jobs_status):
        print("STOP: jobs.job_id and/or jobs.work_status missing; cannot define completed job set.")
        conn.close()
        return

    # Determine completed status filter SQL
    completed_list_sql = ", ".join([f"'{s}'" for s in sorted(COMPLETED_STATUSES)])

    # Revenue: sum invoice totals per job, excluding obviously non-final statuses if possible
    invoice_status_filter = ""
    if inv_status:
        # Don’t over-assume: show ALL statuses in revenue table output, but for breakdown we exclude common non-revenue.
        # If your statuses differ, this is exactly what the earlier status table is for.
        invoice_status_filter = f"""
            AND LOWER(COALESCE(i.{inv_status}, '')) NOT IN ('draft','void','canceled','cancelled')
        """

    # Parts cost: unit_cost*qty; if missing, we still compute NULL and show it
    parts_cost_expr = "NULL"
    if ii_unit_cost and ii_qty:
        parts_cost_expr = f"SUM(COALESCE(ii.{ii_unit_cost},0) * COALESCE(ii.{ii_qty},0))"

    # Hours: prefer duration columns if present; else appointments hours sum per job
    # If multiple duration columns exist, we still don't guess which one is correct; we surface each as evidence.
    # For the numeric breakdown, we pick the first duration column as "candidate_hours" (so you can see if it's 0/null).
    candidate_item_hours_expr = "NULL"
    if duration_cols:
        # Pick first duration-ish column and interpret minutes vs hours by name (only for a candidate calc)
        c = duration_cols[0]
        if "min" in c.lower() or "minute" in c.lower():
            candidate_item_hours_expr = f"SUM(COALESCE(ii.{c}, 0)) / 60.0"
        else:
            candidate_item_hours_expr = f"SUM(COALESCE(ii.{c}, 0))"

    appt_hours_expr = "NULL"
    if appt_job_id and appt_start and appt_end:
        appt_hours_expr = f"""
            SUM(
                CASE
                    WHEN ja.{appt_start} IS NOT NULL AND ja.{appt_end} IS NOT NULL
                    THEN DATE_DIFF('minute', ja.{appt_start}, ja.{appt_end}) / 60.0
                    ELSE 0
                END
            )
        """

    # Tech attribution (exclude installers by role text)
    # job_employees sometimes already has names; otherwise join employees
    tech_name_expr = None
    if je_first and je_last:
        tech_name_expr = f"TRIM(COALESCE(je.{je_first},'') || ' ' || COALESCE(je.{je_last},''))"
    elif emp_first and emp_last and je_emp_id and emp_emp_id:
        tech_name_expr = f"TRIM(COALESCE(e.{emp_first},'') || ' ' || COALESCE(e.{emp_last},''))"

    role_expr = None
    if je_role:
        role_expr = f"LOWER(COALESCE(je.{je_role}, ''))"
    elif emp_role and je_emp_id and emp_emp_id:
        role_expr = f"LOWER(COALESCE(e.{emp_role}, ''))"

    tech_filter_sql = ""
    if role_expr:
        tech_filter_sql = f"WHERE {role_expr} NOT LIKE '%install%'"  # exclude installers
    else:
        tech_filter_sql = ""  # cannot exclude installers if no role column

    try:
        q = f"""
        WITH completed_jobs AS (
            SELECT
                {jobs_job_id} AS job_id,
                {jobs_status} AS work_status,
                {jobs_updated if jobs_updated else 'NULL'} AS job_updated_at
            FROM jobs
            WHERE LOWER({jobs_status}) IN ({completed_list_sql})
        ),
        rev AS (
            SELECT
                i.{inv_job_id} AS job_id,
                SUM(i.{inv_total}) AS revenue_cents,
                COUNT(*) AS invoice_count
            FROM invoices i
            WHERE i.{inv_job_id} IS NOT NULL
            {invoice_status_filter}
            GROUP BY 1
        ),
        parts AS (
            SELECT
                COALESCE(ii.{ii_job_id}, i.{inv_job_id}) AS job_id,
                {parts_cost_expr} AS parts_cost_cents,
                COUNT(*) AS item_count
            FROM invoice_items ii
            LEFT JOIN invoices i
                ON i.{inv_invoice_id} = ii.{ii_invoice_id}
            WHERE COALESCE(ii.{ii_job_id}, i.{inv_job_id}) IS NOT NULL
            GROUP BY 1
        ),
        item_hours AS (
            SELECT
                COALESCE(ii.{ii_job_id}, i.{inv_job_id}) AS job_id,
                {candidate_item_hours_expr} AS item_hours_candidate
            FROM invoice_items ii
            LEFT JOIN invoices i
                ON i.{inv_invoice_id} = ii.{ii_invoice_id}
            WHERE COALESCE(ii.{ii_job_id}, i.{inv_job_id}) IS NOT NULL
            GROUP BY 1
        ),
        appt_hours AS (
            SELECT
                ja.{appt_job_id} AS job_id,
                {appt_hours_expr} AS appt_hours
            FROM job_appointments ja
            WHERE ja.{appt_job_id} IS NOT NULL
            GROUP BY 1
        ),
        techs AS (
            SELECT
                je.{je_job_id} AS job_id,
                COUNT(DISTINCT je.{je_emp_id}) AS tech_count,
                {("STRING_AGG(DISTINCT " + tech_name_expr + ", ', ')") if tech_name_expr else "NULL"} AS tech_names
            FROM job_employees je
            {("LEFT JOIN employees e ON e." + emp_emp_id + " = je." + je_emp_id) if (not (je_first and je_last) and emp_emp_id and je_emp_id) else ""}
            {tech_filter_sql}
            GROUP BY 1
        )
        SELECT
            cj.job_id,
            cj.work_status,
            cj.job_updated_at,
            COALESCE(r.revenue_cents, 0) AS revenue_cents,
            COALESCE(r.invoice_count, 0) AS invoice_count,
            COALESCE(p.parts_cost_cents, 0) AS parts_cost_cents,
            {FIXED_LABOR_COST_CENTS} AS labor_cost_cents_fixed,
            (COALESCE(r.revenue_cents, 0) - COALESCE(p.parts_cost_cents, 0) - {FIXED_LABOR_COST_CENTS}) AS gross_profit_cents,
            COALESCE(ih.item_hours_candidate, NULL) AS item_hours_candidate,
            COALESCE(ah.appt_hours, NULL) AS appt_hours,
            COALESCE(t.tech_count, 0) AS tech_count_non_install,
            t.tech_names
        FROM completed_jobs cj
        LEFT JOIN rev r ON r.job_id = cj.job_id
        LEFT JOIN parts p ON p.job_id = cj.job_id
        LEFT JOIN item_hours ih ON ih.job_id = cj.job_id
        LEFT JOIN appt_hours ah ON ah.job_id = cj.job_id
        LEFT JOIN techs t ON t.job_id = cj.job_id
        ORDER BY cj.job_updated_at DESC NULLS LAST
        LIMIT 25
        """
        df = conn.execute(q).df()

        # Add computed dollar/hour columns for quick eyeballing (no guesses; we show both hour sources)
        df["revenue_$"] = df["revenue_cents"] / 100.0
        df["parts_cost_$"] = df["parts_cost_cents"] / 100.0
        df["labor_fixed_$"] = df["labor_cost_cents_fixed"] / 100.0
        df["gross_profit_$"] = df["gross_profit_cents"] / 100.0

        # compute gp/hr using item_hours_candidate if present else appt_hours (just for visibility)
        def pick_hours(row):
            ih = row.get("item_hours_candidate")
            ah = row.get("appt_hours")
            if pd.notna(ih) and float(ih) > 0:
                return float(ih)
            if pd.notna(ah) and float(ah) > 0:
                return float(ah)
            return 0.0

        df["hours_used"] = df.apply(pick_hours, axis=1)
        df["gp_per_hour_$"] = df.apply(lambda r: (r["gross_profit_$"] / r["hours_used"]) if r["hours_used"] > 0 else 0.0, axis=1)

        # Helpful red flags
        df["FLAG_no_revenue"] = df["revenue_cents"].fillna(0).astype(float) <= 0
        df["FLAG_no_parts_cost"] = df["parts_cost_cents"].fillna(0).astype(float) <= 0
        df["FLAG_no_hours"] = df["hours_used"].fillna(0).astype(float) <= 0
        df["FLAG_no_techs"] = df["tech_count_non_install"].fillna(0).astype(float) <= 0

        print(df[
            [
                "job_id", "work_status", "job_updated_at",
                "invoice_count", "revenue_$",
                "parts_cost_$", "labor_fixed_$", "gross_profit_$",
                "item_hours_candidate", "appt_hours", "hours_used", "gp_per_hour_$",
                "tech_count_non_install", "tech_names",
                "FLAG_no_revenue", "FLAG_no_parts_cost", "FLAG_no_hours", "FLAG_no_techs",
            ]
        ])
    except Exception as e:
        print("Per-job breakdown query failed:", e)
        print("\nNOTE: This usually fails if one of the join key columns isn't present.")
        print("Double-check the COLUMN PICKS section above to see what exists in your DB.")

    header("DONE")
    conn.close()


if __name__ == "__main__":
    main()
