import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

PERSIST_DIR = Path(os.environ.get("PERSIST_DIR", "."))
DB_FILE = PERSIST_DIR / "housecall_data.duckdb"

CENTRAL_TZ = ZoneInfo("America/Chicago")


# -----------------------------
# Tables / schema (read-side safety)
# -----------------------------
def ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    This is defensive only.
    Your ingestion script should be the source of truth for table creation.
    We keep this to prevent runtime crashes if DB is empty or partially initialized.
    """

    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # Core tables (legacy safe)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT,
            invoice_number TEXT,
            description TEXT,
            work_status TEXT,
            total_amount DOUBLE,
            outstanding_balance DOUBLE,
            company_name TEXT,
            company_id TEXT,
            created_at TEXT,
            updated_at TEXT,
            completed_at TEXT,
            customer_id TEXT,
            num_appointments INTEGER,
            tags TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_employees (
            job_id TEXT,
            employee_id TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            mobile_number TEXT,
            role TEXT,
            avatar_url TEXT,
            color_hex TEXT,
            company_id TEXT,
            company_name TEXT,
            updated_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            mobile_number TEXT,
            home_number TEXT,
            work_number TEXT,
            company TEXT,
            notifications_enabled BOOLEAN,
            lead_source TEXT,
            notes TEXT,
            created_at TEXT,
            updated_at TEXT,
            company_name TEXT,
            company_id TEXT,
            tags TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id TEXT,
            job_id TEXT,
            invoice_number TEXT,
            status TEXT,
            amount DOUBLE,
            subtotal DOUBLE,
            due_amount DOUBLE,
            due_at TEXT,
            paid_at TEXT,
            sent_at TEXT,
            service_date TEXT,
            invoice_date TEXT,
            display_due_concept TEXT,
            due_concept TEXT
        )
    """)

    # Newer ingestion tables (safe stubs)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            employee_id TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            mobile_number TEXT,
            role TEXT,
            avatar_url TEXT,
            color_hex TEXT,
            company_id TEXT,
            company_name TEXT,
            updated_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS job_appointments (
            job_id TEXT,
            appointment_id TEXT,
            start_time TEXT,
            end_time TEXT,
            dispatched_employee_id TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS invoice_items (
            invoice_id TEXT,
            job_id TEXT,
            item_id TEXT,
            name TEXT,
            type TEXT,
            unit_cost DOUBLE,
            unit_price DOUBLE,
            qty_in_hundredths DOUBLE,
            amount DOUBLE
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS estimates (
            estimate_id TEXT,
            estimate_number TEXT,
            work_status TEXT,
            lead_source TEXT,
            customer_id TEXT,
            created_at TEXT,
            updated_at TEXT,
            company_id TEXT,
            company_name TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS estimate_options (
            estimate_id TEXT,
            option_id TEXT,
            name TEXT,
            option_number TEXT,
            total_amount DOUBLE,
            approval_status TEXT,
            status TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS estimate_employees (
            estimate_id TEXT,
            employee_id TEXT,
            first_name TEXT,
            last_name TEXT,
            role TEXT,
            avatar_url TEXT,
            color_hex TEXT
        )
    """)


# -----------------------------
# Helpers
# -----------------------------
def _safe_df(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    try:
        return conn.execute(sql).df()
    except Exception:
        return pd.DataFrame()


def _ensure_columns(df: pd.DataFrame, cols_with_dtypes: dict) -> pd.DataFrame:
    for col, dtype in cols_with_dtypes.items():
        if col not in df.columns:
            try:
                df[col] = pd.Series(dtype=dtype)
            except Exception:
                df[col] = pd.Series(dtype="object")
    return df


def _to_dt_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _convert_cents_to_dollars(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0) / 100.0
    return df


def _normalize_tags(tags_val) -> str:
    if tags_val is None or (isinstance(tags_val, float) and pd.isna(tags_val)):
        return ""
    s = str(tags_val)
    parts = [p.strip().lower() for p in s.split(",") if p.strip()]
    return ",".join(parts)


def _tag_has(tags_norm: str, needle: str) -> bool:
    if not tags_norm:
        return False
    needle = needle.strip().lower()
    return needle in tags_norm.split(",")


def _format_currency(x: float) -> str:
    try:
        return "${:,.0f}".format(float(x))
    except Exception:
        return "$0"


def _map_category_from_tags(tags_norm: str) -> str:
    """
    JC requested revenue categories (7):
      - Residential Install
      - Residential Service
      - Residential Maintenance
      - Commercial Install
      - Commercial Service
      - Commercial Maintenance
      - New Construction

    Notes:
    - We intentionally bucket all revenue into these 7 categories to keep charts consistent.
    - If a job cannot be classified, it will default to New Construction (catch-all) so totals reconcile.
    """

    if not tags_norm:
        return "New Construction"

    s = tags_norm

    is_res = _tag_has(s, "residential") or ("residential " in s)
    is_com = _tag_has(s, "commercial") or ("commercial " in s)

    is_new = (
        _tag_has(s, "new construction")
        or _tag_has(s, "new-construction")
        or _tag_has(s, "new build")
        or _tag_has(s, "new-build")
        or ("new construction" in s)
        or ("new build" in s)
    )

    is_install = (
        _tag_has(s, "install")
        or _tag_has(s, "installation")
        or _tag_has(s, "change out")
        or _tag_has(s, "change-out")
        or (" install" in s)
        or ("change out" in s)
        or ("change-out" in s)
    )

    is_maint = _tag_has(s, "maintenance") or ("maintenance" in s)

    # Service: excludes installs/change-outs when tag says so
    is_service = (
        (_tag_has(s, "service") or _tag_has(s, "demand service") or _tag_has(s, "demand") or (" service" in s))
        and (not is_install)
    )

    # Explicit phrases win
    if "residential install" in s:
        is_res, is_install = True, True
    if "commercial install" in s:
        is_com, is_install = True, True
    if "residential maintenance" in s:
        is_res, is_maint = True, True
    if "commercial maintenance" in s:
        is_com, is_maint = True, True
    if "residential service" in s or "residential demand service" in s:
        is_res, is_service = True, True
    if "commercial service" in s or "commercial demand service" in s:
        is_com, is_service = True, True

    if is_new:
        return "New Construction"

    if is_res and is_install:
        return "Residential Install"
    if is_res and is_service:
        return "Residential Service"
    if is_res and is_maint:
        return "Residential Maintenance"

    if is_com and is_install:
        return "Commercial Install"
    if is_com and is_service:
        return "Commercial Service"
    if is_com and is_maint:
        return "Commercial Maintenance"

    # Catch-all to keep exactly 7 buckets
    return "New Construction"


def _is_install_job(tags_norm: str) -> bool:
    if not tags_norm:
        return False
    s = tags_norm
    return (
        _tag_has(s, "install")
        or _tag_has(s, "installation")
        or _tag_has(s, "change out")
        or _tag_has(s, "change-out")
        or ("residential install" in s)
        or ("commercial install" in s)
        or ("change out" in s)
        or ("change-out" in s)
    )


def _is_maintenance_job(tags_norm: str) -> bool:
    if not tags_norm:
        return False
    return _tag_has(tags_norm, "maintenance") or ("maintenance" in tags_norm)


def _is_new_construction_job(tags_norm: str) -> bool:
    if not tags_norm:
        return False
    s = tags_norm
    return (
        _tag_has(s, "new construction")
        or _tag_has(s, "new-construction")
        or _tag_has(s, "new build")
        or _tag_has(s, "new-build")
        or ("new construction" in s)
        or ("new build" in s)
    )


def _is_dfo_job(tags_norm: str) -> bool:
    # DFO = Diagnostic Fee Only (tag-driven)
    return _tag_has(tags_norm, "dfo")


def _is_callback_job(tags_norm: str) -> bool:
    needles = ["callback", "call back", "call-back", "recall", "warranty", "warrantee"]
    return any(_tag_has(tags_norm, n) for n in needles)


def _is_service_job(tags_norm: str) -> bool:
    """
    Best-effort filter for 'service' jobs.
    Excludes installs/change-outs when the tag says so.
    """
    if not tags_norm:
        return False
    if _tag_has(tags_norm, "install") or _tag_has(tags_norm, "installation") or _tag_has(tags_norm, "change out") or _tag_has(tags_norm, "change-out"):
        return False
    # service / demand service / residential service / commercial service etc
    return _tag_has(tags_norm, "service") or _tag_has(tags_norm, "demand service") or _tag_has(tags_norm, "demand")


def _start_of_year_utc() -> datetime:
    now_central = datetime.now(CENTRAL_TZ)
    start_central = datetime(now_central.year, 1, 1, tzinfo=CENTRAL_TZ)
    return start_central.astimezone(timezone.utc)


def _largest_remainder_int_percentages(values: list[float]) -> list[int]:
    """
    Convert raw percentages into ints that sum to 100 using largest remainder.
    """
    if not values:
        return []
    floors = [int(v) for v in values]
    remainder = 100 - sum(floors)
    fracs = sorted([(i, values[i] - floors[i]) for i in range(len(values))], key=lambda x: x[1], reverse=True)

    out = floors[:]
    if remainder > 0:
        for j in range(remainder):
            out[fracs[j % len(fracs)][0]] += 1
    elif remainder < 0:
        # in weird cases, remove from smallest fractional values
        fracs_asc = sorted(fracs, key=lambda x: x[1])
        for j in range(abs(remainder)):
            idx = fracs_asc[j % len(fracs_asc)][0]
            out[idx] = max(0, out[idx] - 1)
    return out


# -----------------------------
# KPI builder
# -----------------------------
def get_dashboard_kpis():
    conn = duckdb.connect(DB_FILE)
    try:
        ensure_tables(conn)

        # Load all relevant tables
        df_jobs = _safe_df(conn, "SELECT * FROM jobs")
        df_job_emps = _safe_df(conn, "SELECT * FROM job_employees")
        df_customers = _safe_df(conn, "SELECT * FROM customers")
        df_invoices = _safe_df(conn, "SELECT * FROM invoices")

        df_employees = _safe_df(conn, "SELECT * FROM employees")
        df_job_appts = _safe_df(conn, "SELECT * FROM job_appointments")
        df_invoice_items = _safe_df(conn, "SELECT * FROM invoice_items")

        df_estimates = _safe_df(conn, "SELECT * FROM estimates")
        df_estimate_options = _safe_df(conn, "SELECT * FROM estimate_options")
        df_estimate_emps = _safe_df(conn, "SELECT * FROM estimate_employees")

        # Ensure expected columns exist
        df_jobs = _ensure_columns(df_jobs, {
            "job_id": "object",
            "invoice_number": "object",
            "description": "object",
            "work_status": "object",
            "total_amount": "float64",
            "outstanding_balance": "float64",
            "company_name": "object",
            "company_id": "object",
            "created_at": "object",
            "updated_at": "object",
            "completed_at": "object",
            "customer_id": "object",
            "num_appointments": "int64",
            "tags": "object",
        })

        df_job_emps = _ensure_columns(df_job_emps, {
            "job_id": "object",
            "employee_id": "object",
            "first_name": "object",
            "last_name": "object",
            "role": "object",
            "avatar_url": "object",
            "color_hex": "object",
        })

        df_customers = _ensure_columns(df_customers, {
            "customer_id": "object",
            "first_name": "object",
            "last_name": "object",
        })

        df_invoices = _ensure_columns(df_invoices, {
            "invoice_id": "object",
            "job_id": "object",
            "status": "object",
            "amount": "float64",
            "subtotal": "float64",
            "due_amount": "float64",
            "paid_at": "object",
            "service_date": "object",
            "invoice_date": "object",
        })

        df_employees = _ensure_columns(df_employees, {
            "employee_id": "object",
            "first_name": "object",
            "last_name": "object",
            "role": "object",
            "avatar_url": "object",
            "color_hex": "object",
        })

        df_job_appts = _ensure_columns(df_job_appts, {
            "job_id": "object",
            "appointment_id": "object",
            "start_time": "object",
            "end_time": "object",
            "dispatched_employee_id": "object",
        })

        df_invoice_items = _ensure_columns(df_invoice_items, {
            "invoice_id": "object",
            "job_id": "object",
            "item_id": "object",
            "name": "object",
            "type": "object",
            "unit_cost": "float64",
            "unit_price": "float64",
            "qty_in_hundredths": "float64",
            "amount": "float64",
        })

        # Convert cents -> dollars (HCP returns cents)
        df_jobs = _convert_cents_to_dollars(df_jobs, ["total_amount", "outstanding_balance"])
        df_invoices = _convert_cents_to_dollars(df_invoices, ["amount", "subtotal", "due_amount"])
        df_invoice_items = _convert_cents_to_dollars(df_invoice_items, ["unit_cost", "unit_price", "amount"])

        # Normalize tags
        df_jobs["tags_norm"] = df_jobs["tags"].apply(_normalize_tags) if not df_jobs.empty else ""

        # Datetimes
        for col in ["created_at", "updated_at", "completed_at"]:
            df_jobs[col] = _to_dt_utc(df_jobs[col]) if col in df_jobs.columns else pd.NaT

        df_invoices["paid_at_dt"] = _to_dt_utc(df_invoices["paid_at"]) if "paid_at" in df_invoices.columns else pd.NaT
        df_invoices["service_date_dt"] = _to_dt_utc(df_invoices["service_date"]) if "service_date" in df_invoices.columns else pd.NaT
        df_invoices["invoice_date_dt"] = _to_dt_utc(df_invoices["invoice_date"]) if "invoice_date" in df_invoices.columns else pd.NaT

        df_job_appts["start_dt"] = _to_dt_utc(df_job_appts["start_time"]) if "start_time" in df_job_appts.columns else pd.NaT
        df_job_appts["end_dt"] = _to_dt_utc(df_job_appts["end_time"]) if "end_time" in df_job_appts.columns else pd.NaT

        # Join jobs -> customers for name
        if not df_jobs.empty and not df_customers.empty:
            df_jobs = df_jobs.merge(
                df_customers[["customer_id", "first_name", "last_name"]],
                how="left",
                on="customer_id",
            )
        df_jobs["customer_name"] = (
            df_jobs.get("first_name", pd.Series(["Unknown"] * len(df_jobs))).fillna("Unknown").astype(str).str.strip()
            + " "
            + df_jobs.get("last_name", pd.Series([""] * len(df_jobs))).fillna("").astype(str).str.strip()
        ).str.strip()

        # Prefer appointment count from job_appointments if present
        if not df_job_appts.empty and "job_id" in df_job_appts.columns:
            appt_counts = df_job_appts.groupby("job_id")["appointment_id"].nunique().reset_index()
            appt_counts.rename(columns={"appointment_id": "num_appointments_calc"}, inplace=True)
            df_jobs = df_jobs.merge(appt_counts, how="left", on="job_id")
            df_jobs["num_appointments"] = df_jobs["num_appointments_calc"].fillna(df_jobs["num_appointments"]).fillna(0).astype(int)
            df_jobs.drop(columns=["num_appointments_calc"], inplace=True, errors="ignore")
        else:
            df_jobs["num_appointments"] = pd.to_numeric(df_jobs["num_appointments"], errors="coerce").fillna(0).astype(int)

        # YTD window
        start_of_year_utc = _start_of_year_utc()

        # Completed jobs YTD
        completed_statuses = {"complete unrated", "complete rated"}
        df_completed = df_jobs[df_jobs["work_status"].astype(str).str.lower().isin(completed_statuses)].copy()
        df_completed = df_completed[df_completed["completed_at"] >= start_of_year_utc].copy()

        completed_jobs = int(len(df_completed))

        # -----------------------------------
        # KPI: First-Time Completion (FTC)
        # Definition: completed jobs with exactly 1 appointment
        # -----------------------------------
        first_time_completed = int((df_completed["num_appointments"] == 1).sum()) if completed_jobs > 0 else 0
        repeat_visit_completed = int((df_completed["num_appointments"] >= 2).sum()) if completed_jobs > 0 else 0

        first_time_completion_pct = round((first_time_completed / completed_jobs) * 100.0, 2) if completed_jobs > 0 else 0.0
        repeat_visit_pct = round((repeat_visit_completed / completed_jobs) * 100.0, 2) if completed_jobs > 0 else 0.0
        first_time_completion_target = 85

        # Repeat visit jobs table (action list)
        repeat_jobs_df = df_completed[df_completed["num_appointments"] >= 2].copy().sort_values("completed_at", ascending=False)

        def _fmt_dt_central(dt):
            if pd.isna(dt):
                return ""
            try:
                return dt.tz_convert(CENTRAL_TZ).strftime("%b %d, %Y %I:%M %p")
            except Exception:
                return str(dt)

        repeat_jobs_df["completed_at_central"] = repeat_jobs_df["completed_at"].apply(_fmt_dt_central)

        repeat_jobs_table = repeat_jobs_df[[
            "job_id", "customer_name", "work_status", "num_appointments", "completed_at_central", "description"
        ]].head(25).to_dict(orient="records")

        # -----------------------------------
        # KPI: DFO (Diagnostic Fee Only) %
        # Definition: completed jobs tagged DFO / Diagnostic Fee Only
        # -----------------------------------
        df_dfo = df_completed[df_completed["tags_norm"].apply(_is_dfo_job)].copy()
        dfo_count = int(len(df_dfo))
        dfo_pct = round((dfo_count / completed_jobs) * 100.0, 2) if completed_jobs > 0 else 0.0

        if dfo_pct < 5:
            dfo_status_color = "success"
        elif dfo_pct < 10:
            dfo_status_color = "warning"
        else:
            dfo_status_color = "danger"

        # DFO monthly breakdown
        dfo_monthly = []
        if not df_dfo.empty:
            df_dfo_m = df_dfo.copy()
            df_dfo_m["month"] = df_dfo_m["completed_at"].dt.to_period("M")

            df_total_m = df_completed.copy()
            df_total_m["month"] = df_total_m["completed_at"].dt.to_period("M")

            m_dfo = df_dfo_m.groupby("month").size()
            m_total = df_total_m.groupby("month").size()

            for month in m_total.index.sort_values():
                dfo_cnt = int(m_dfo.get(month, 0))
                total_cnt = int(m_total.get(month, 0))
                pct = round((dfo_cnt / total_cnt) * 100.0, 1) if total_cnt > 0 else 0.0
                dfo_monthly.append({
                    "month": str(month),
                    "dfo_count": dfo_cnt,
                    "total_count": total_cnt,
                    "dfo_pct": pct,
                })

        # -----------------------------------
        # Revenue (YTD) + Breakdown (YTD)
        # JC request: Revenue should be BILLED (what we invoice), not cash collected.
        # So we EXCLUDE paid invoices and only include invoice statuses that represent billed AR.
        # -----------------------------------
        revenue_statuses = {"open", "pending_payment"}
        df_rev = df_invoices[df_invoices["status"].astype(str).str.lower().isin(revenue_statuses)].copy()

        def _effective_dt(row):
            # Billed timing: prefer service_date, else invoice_date (ignore paid_at)
            if pd.notna(row.get("service_date_dt")):
                return row.get("service_date_dt")
            if pd.notna(row.get("invoice_date_dt")):
                return row.get("invoice_date_dt")
            return pd.NaT

        if not df_rev.empty:
            df_rev["effective_dt"] = df_rev.apply(_effective_dt, axis=1)
            df_rev = df_rev[df_rev["effective_dt"] >= start_of_year_utc].copy()
        else:
            df_rev["effective_dt"] = pd.NaT

        # Join tags onto revenue for category breakdown
        if not df_rev.empty and not df_jobs.empty:
            df_rev = df_rev.merge(df_jobs[["job_id", "tags_norm"]], how="left", on="job_id")
        df_rev["tags_norm"] = df_rev.get("tags_norm", "").fillna("")
        df_rev["category"] = df_rev["tags_norm"].apply(_map_category_from_tags)

        df_rev["amount_dollars"] = pd.to_numeric(df_rev["amount"], errors="coerce").fillna(0.0).astype(float)

        total_revenue_ytd = float(df_rev["amount_dollars"].sum()) if not df_rev.empty else 0.0
        total_revenue_ytd_display = _format_currency(total_revenue_ytd)

        # Force exactly 7 buckets in a stable order for the chart
        CATEGORY_ORDER = [
            "Residential Install",
            "Residential Service",
            "Residential Maintenance",
            "Commercial Install",
            "Commercial Service",
            "Commercial Maintenance",
            "New Construction",
        ]

        if not df_rev.empty:
            breakdown_series = df_rev.groupby("category", dropna=False)["amount_dollars"].sum()
            # Ensure all 7 show up (even if 0)
            breakdown_rows = []
            for cat in CATEGORY_ORDER:
                breakdown_rows.append({"category": cat, "revenue": float(breakdown_series.get(cat, 0.0))})
            breakdown = pd.DataFrame(breakdown_rows)
        else:
            breakdown = pd.DataFrame([{"category": c, "revenue": 0.0} for c in CATEGORY_ORDER])

        revenue_breakdown_ytd = []
        total_breakdown = float(breakdown["revenue"].sum()) if not breakdown.empty else 0.0

        rows = []
        for _, row in breakdown.iterrows():
            rev = float(row["revenue"])
            cat = str(row["category"]).strip()
            pct_raw = (rev / total_breakdown * 100.0) if total_breakdown > 0 else 0.0
            rows.append({"category": cat, "revenue": rev, "pct_raw": pct_raw})

        pct_ints = _largest_remainder_int_percentages([r["pct_raw"] for r in rows]) if rows else []
        max_rev = max((r["revenue"] for r in rows), default=0.0)

        for r, pct_int in zip(rows, pct_ints):
            pct_of_max = (r["revenue"] / max_rev * 100.0) if max_rev > 0 else 0.0
            pct_of_max = max(0.0, min(100.0, pct_of_max))
            revenue_breakdown_ytd.append({
                "category": r["category"],
                "revenue": r["revenue"],
                "revenue_display": _format_currency(r["revenue"]),
                "pct_of_total": float(pct_int),                 # integer percent, sums to 100
                "pct_of_total_display": f"{int(pct_int)}%",
                "pct_of_max": pct_of_max,
            })

        # -----------------------------------
        # KPI: Average Ticket (YTD) - split Service vs Install
        # Threshold (invoice) remains $450 per KPI chart.
        # -----------------------------------
        df_rev["is_service_job"] = df_rev["tags_norm"].apply(_is_service_job)
        df_rev["is_install_job"] = df_rev["tags_norm"].apply(_is_install_job)

        df_rev_service = df_rev[df_rev["is_service_job"]].copy()
        df_rev_install = df_rev[df_rev["is_install_job"]].copy()

        service_invoice_count = int(len(df_rev_service)) if not df_rev_service.empty else 0
        install_invoice_count = int(len(df_rev_install)) if not df_rev_install.empty else 0

        service_avg_ticket_value = (float(df_rev_service["amount_dollars"].sum()) / service_invoice_count) if service_invoice_count > 0 else 0.0
        install_avg_ticket_value = (float(df_rev_install["amount_dollars"].sum()) / install_invoice_count) if install_invoice_count > 0 else 0.0

        service_avg_ticket_display = _format_currency(service_avg_ticket_value)
        install_avg_ticket_display = _format_currency(install_avg_ticket_value)

        average_ticket_threshold = 450.0
        # keep legacy fields too (defaults to SERVICE, since this KPI is "Techs on Service")
        invoice_count = service_invoice_count
        average_ticket_value = service_avg_ticket_value
        average_ticket_status = "success" if average_ticket_value >= average_ticket_threshold else "danger"
        average_ticket_display = _format_currency(average_ticket_value)
# -----------------------------------
        # Employee KPIs (cards)
        # - Completed jobs YTD
        # - Average ticket (overall) YTD (not split)
        # - Callback % YTD
        # - Appointment hours YTD (from job_appointments)
        # - Gross profit per hour (approx) YTD using invoice_items unit_cost as cost
        # -----------------------------------
        employee_cards = []

        # Build a canonical employee dimension (prefer employees table, fallback to job_employees)
        df_emp_dim = df_employees.copy()
        if df_emp_dim.empty:
            df_emp_dim = df_job_emps[["employee_id", "first_name", "last_name", "role", "avatar_url", "color_hex"]].drop_duplicates().copy()

        df_emp_dim = _ensure_columns(df_emp_dim, {
            "employee_id": "object",
            "first_name": "object",
            "last_name": "object",
            "role": "object",
            "avatar_url": "object",
            "color_hex": "object",
        })

        df_emp_dim["employee_name"] = (
            df_emp_dim["first_name"].fillna("").astype(str).str.strip()
            + " "
            + df_emp_dim["last_name"].fillna("").astype(str).str.strip()
        ).str.strip()
        df_emp_dim["employee_name"] = df_emp_dim["employee_name"].replace("", "Unknown")

        # Completed jobs per tech YTD (assigned_employees based)
        df_completed_jobs_emp = pd.DataFrame(columns=["employee_id", "completed_jobs_ytd"])
        if not df_completed.empty and not df_job_emps.empty:
            df_emp_completed = df_completed[["job_id", "tags_norm"]].merge(df_job_emps, how="inner", on="job_id")
            df_emp_completed = df_emp_completed.dropna(subset=["employee_id"])
            df_completed_jobs_emp = (
                df_emp_completed.groupby("employee_id")
                .agg(completed_jobs_ytd=("job_id", "nunique"))
                .reset_index()
            )

                # Avg ticket (SERVICE) per tech YTD (billed invoices attributed to assigned techs on the job)
        # Target: $450 per invoice (JC KPI chart)
        df_emp_atv = pd.DataFrame(columns=["employee_id", "avg_ticket_overall", "overall_invoice_count"])
        if 'df_rev_service' in locals() and not df_rev_service.empty and not df_job_emps.empty:
            df_inv_emp = df_rev_service[["invoice_id", "job_id", "amount_dollars", "effective_dt"]].merge(
                df_job_emps[["job_id", "employee_id"]].dropna(subset=["employee_id"]),
                how="inner",
                on="job_id",
            )
            if not df_inv_emp.empty:
                # Split invoice amounts across techs assigned to the job (avoids double counting)
                tech_count = (
                    df_inv_emp.groupby(["invoice_id", "job_id"])["employee_id"]
                    .nunique()
                    .reset_index()
                    .rename(columns={"employee_id": "tech_count"})
                )
                df_inv_emp = df_inv_emp.merge(tech_count, how="left", on=["invoice_id", "job_id"])
                df_inv_emp["tech_count"] = df_inv_emp["tech_count"].fillna(1).astype(int)
                df_inv_emp["amount_alloc"] = df_inv_emp["amount_dollars"] / df_inv_emp["tech_count"]

                tmp = (
                    df_inv_emp.groupby("employee_id")
                    .agg(
                        overall_revenue=("amount_alloc", "sum"),
                        overall_invoice_count=("invoice_id", "nunique"),
                    )
                    .reset_index()
                )
                tmp["avg_ticket_overall"] = tmp.apply(
                    lambda r: (float(r["overall_revenue"]) / float(r["overall_invoice_count"])) if float(r["overall_invoice_count"]) > 0 else 0.0,
                    axis=1,
                )
                df_emp_atv = tmp[["employee_id", "avg_ticket_overall", "overall_invoice_count"]]

        # Service revenue per day per tech (SERVICE only)
        # Target: $1350 per day per tech (JC KPI chart)
        df_emp_daily = pd.DataFrame(columns=["employee_id", "service_revenue_per_day_ytd"])
        if 'df_rev_service' in locals() and not df_rev_service.empty and not df_job_emps.empty:
            df_inv_emp2 = df_rev_service[["invoice_id", "job_id", "amount_dollars", "effective_dt"]].merge(
                df_job_emps[["job_id", "employee_id"]].dropna(subset=["employee_id"]),
                how="inner",
                on="job_id",
            )
            if not df_inv_emp2.empty:
                tech_count2 = (
                    df_inv_emp2.groupby(["invoice_id", "job_id"])["employee_id"]
                    .nunique()
                    .reset_index()
                    .rename(columns={"employee_id": "tech_count"})
                )
                df_inv_emp2 = df_inv_emp2.merge(tech_count2, how="left", on=["invoice_id", "job_id"])
                df_inv_emp2["tech_count"] = df_inv_emp2["tech_count"].fillna(1).astype(int)
                df_inv_emp2["amount_alloc"] = df_inv_emp2["amount_dollars"] / df_inv_emp2["tech_count"]

                # Compute "work day" in Central time
                def _day_central(dt):
                    if pd.isna(dt):
                        return pd.NaT
                    try:
                        return dt.tz_convert(CENTRAL_TZ).date()
                    except Exception:
                        return pd.NaT

                df_inv_emp2["day_central"] = df_inv_emp2["effective_dt"].apply(_day_central)

                daily = (
                    df_inv_emp2.dropna(subset=["day_central"])
                    .groupby(["employee_id", "day_central"])["amount_alloc"]
                    .sum()
                    .reset_index()
                )

                if not daily.empty:
                    per_emp = (
                        daily.groupby("employee_id")
                        .agg(total_revenue=("amount_alloc", "sum"), days_worked=("day_central", "nunique"))
                        .reset_index()
                    )
                    per_emp["service_revenue_per_day_ytd"] = per_emp.apply(
                        lambda r: (float(r["total_revenue"]) / float(r["days_worked"])) if float(r["days_worked"]) > 0 else 0.0,
                        axis=1,
                    )
                    df_emp_daily = per_emp[["employee_id", "service_revenue_per_day_ytd"]]
        df_emp_cb = pd.DataFrame(columns=["employee_id", "callback_jobs_ytd"])
        if not df_completed.empty and not df_job_emps.empty:
            df_cb_jobs = df_completed[df_completed["tags_norm"].fillna("").apply(_is_callback_job)][["job_id"]].copy()
            if not df_cb_jobs.empty:
                df_cb_emp = df_cb_jobs.merge(
                    df_job_emps[["job_id", "employee_id"]].dropna(subset=["employee_id"]),
                    how="inner",
                    on="job_id"
                )
                df_emp_cb = (
                    df_cb_emp.groupby("employee_id")
                    .agg(callback_jobs_ytd=("job_id", "nunique"))
                    .reset_index()
                )

        # Appointment hours per tech YTD (dispatched_employees_ids)
        df_emp_hours = pd.DataFrame(columns=["employee_id", "appt_hours_ytd"])
        if not df_job_appts.empty:
            df_ap = df_job_appts.copy()
            # Attach completed_at to filter YTD using job completion date
            if not df_jobs.empty:
                df_ap = df_ap.merge(df_jobs[["job_id", "completed_at"]], how="left", on="job_id")
                df_ap["completed_at"] = _to_dt_utc(df_ap["completed_at"])
                df_ap = df_ap[df_ap["completed_at"] >= start_of_year_utc].copy()

            df_ap["duration_hours"] = (df_ap["end_dt"] - df_ap["start_dt"]).dt.total_seconds() / 3600.0
            df_ap["duration_hours"] = pd.to_numeric(df_ap["duration_hours"], errors="coerce").fillna(0.0)
            df_ap = df_ap[df_ap["duration_hours"] > 0].copy()

            df_ap = df_ap.dropna(subset=["dispatched_employee_id"])
            if not df_ap.empty:
                df_emp_hours = (
                    df_ap.groupby("dispatched_employee_id")
                    .agg(appt_hours_ytd=("duration_hours", "sum"))
                    .reset_index()
                    .rename(columns={"dispatched_employee_id": "employee_id"})
                )

                # Gross profit per hour (service techs) YTD
                # Target definition (JC):
                #   Gross profit dollars per job per technician per hour (NOT installers)
                #   Gross profit = Revenue - (Labor cost + Parts cost)
                #   Labor cost = $525 (fixed per job)
                #   Parts cost = sum(invoice_items.unit_cost * qty)
                #   Hours = use price-book duration if you have it; otherwise fall back to appointment hours on the job
                #
                # Notes / assumptions:
                # - We only compute this for SERVICE jobs (excludes installs/change-outs) using tags.
                # - We attribute each job's GP and hours equally across NON-install technicians assigned to the job.
                # - If no job-level hours are available, metric returns 0 (to avoid divide-by-zero).
                gp_per_hour_target = 150.0
                labor_cost_per_job = 525.0  # JC rule

                # Optional: price-book durations (hours) keyed by normalized service name.
                # If the table doesn't exist yet (or duration is missing), we fall back to appointment hours.
                PRICEBOOK_DURATION_HOURS: dict[str, float] = {}
                try:
                    if _table_exists(conn, "pricebook_services"):
                        df_pb = conn.execute(
                            "SELECT name, duration FROM pricebook_services WHERE duration IS NOT NULL"
                        ).df()
                        if not df_pb.empty:
                            df_pb["name_norm"] = df_pb["name"].astype(str).map(_norm_item_name)
                            # duration from HCP is minutes (int). Convert -> hours.
                            df_pb["dur_hours"] = pd.to_numeric(df_pb["duration"], errors="coerce").fillna(0.0) / 60.0
                            df_pb = df_pb[(df_pb["name_norm"] != "") & (df_pb["dur_hours"] > 0)].copy()
                            # If duplicates exist, keep the max duration as a conservative estimate.
                            PRICEBOOK_DURATION_HOURS = (
                                df_pb.groupby("name_norm")["dur_hours"].max().to_dict()
                            )
                except Exception:
                    PRICEBOOK_DURATION_HOURS = {}

                def _norm_item_name(x) -> str:
                    if x is None or (isinstance(x, float) and pd.isna(x)):
                        return ""
                    return str(x).strip().lower()

                def _item_duration_hours(name: str) -> float:
                    key = _norm_item_name(name)
                    if not key:
                        return 0.0
                    # exact match
                    if key in PRICEBOOK_DURATION_HOURS:
                        return float(PRICEBOOK_DURATION_HOURS[key])
                    # substring match (so "Capacitor - 45/5" can match "capacitor")
                    for k, v in PRICEBOOK_DURATION_HOURS.items():
                        if k and k in key:
                            return float(v)
                    return 0.0

                df_emp_gp = pd.DataFrame(columns=["employee_id", "gross_profit_per_hour_ytd"])

                if (not df_rev.empty) and (not df_invoice_items.empty) and (not df_job_emps.empty):
                    # Revenue by job (YTD invoices)
                    rev_by_job = (
                        df_rev.groupby("job_id")["amount_dollars"]
                        .sum()
                        .reset_index()
                        .rename(columns={"amount_dollars": "job_revenue"})
                    )

                    # Parts cost by job using invoice_items (materials only)
                    df_items = df_invoice_items.copy()
                    df_items["type"] = df_items.get("type", "").astype(str).str.lower().str.strip()
                    df_items = df_items[df_items["type"] == "material"].copy()

                    df_items["qty"] = pd.to_numeric(df_items.get("qty_in_hundredths", 0.0), errors="coerce").fillna(0.0) / 100.0
                    # Some material lines may not have a reliable qty; treat missing/0 qty as 1 for costing.
                    df_items.loc[df_items["qty"] <= 0, "qty"] = 1.0

                    df_items["unit_cost_d"] = pd.to_numeric(df_items.get("unit_cost", 0.0), errors="coerce").fillna(0.0)

                    # If unit_cost is missing/zero, we can't infer cost from invoice_items.
                    # (Optional future: join to pricebook_materials by name and use its cost as a fallback.)
                    df_items["parts_cost"] = df_items["unit_cost_d"] * df_items["qty"]

                    parts_cost_by_job = (
                        df_items.groupby("job_id")["parts_cost"]
                        .sum()
                        .reset_index()
                        .rename(columns={"parts_cost": "parts_cost"})
                    )

                    # Optional: price-book duration by job from invoice items (sum of per-item durations * qty)
                    df_items["item_name_norm"] = df_items["name"].apply(_norm_item_name)
                    df_items["item_duration_hr"] = df_items["item_name_norm"].apply(_item_duration_hours)
                    df_items["duration_hr"] = df_items["item_duration_hr"] * df_items["qty"]
                    duration_by_job_pricebook = (
                        df_items.groupby("job_id")["duration_hr"]
                        .sum()
                        .reset_index()
                        .rename(columns={"duration_hr": "job_hours_pricebook"})
                    )

                    # Appointment duration by job (fallback)
                    job_hours_appt = pd.DataFrame(columns=["job_id", "job_hours_appt"])
                    if not df_job_appts.empty:
                        df_ap_job = df_job_appts.copy()
                        # Filter to YTD completed jobs using job completion date
                        if not df_jobs.empty:
                            df_ap_job = df_ap_job.merge(df_jobs[["job_id", "completed_at", "tags_norm"]], how="left", on="job_id")
                            df_ap_job["completed_at"] = _to_dt_utc(df_ap_job["completed_at"])
                            df_ap_job = df_ap_job[df_ap_job["completed_at"] >= start_of_year_utc].copy()

                        df_ap_job["duration_hours"] = (df_ap_job["end_dt"] - df_ap_job["start_dt"]).dt.total_seconds() / 3600.0
                        df_ap_job["duration_hours"] = pd.to_numeric(df_ap_job["duration_hours"], errors="coerce").fillna(0.0)
                        df_ap_job = df_ap_job[df_ap_job["duration_hours"] > 0].copy()

                        if not df_ap_job.empty:
                            job_hours_appt = (
                                df_ap_job.groupby("job_id")["duration_hours"]
                                .sum()
                                .reset_index()
                                .rename(columns={"duration_hours": "job_hours_appt"})
                            )

                    # Tags by job for service filtering
                    tags_by_job = df_jobs[["job_id", "tags_norm"]].drop_duplicates() if (not df_jobs.empty) else pd.DataFrame(columns=["job_id", "tags_norm"])

                    # Combine job-level revenue/cost/hours
                    job_gp = (
                        rev_by_job
                        .merge(parts_cost_by_job, how="left", on="job_id")
                        .merge(duration_by_job_pricebook, how="left", on="job_id")
                        .merge(job_hours_appt, how="left", on="job_id")
                        .merge(tags_by_job, how="left", on="job_id")
                    )
                    job_gp["parts_cost"] = job_gp["parts_cost"].fillna(0.0)
                    job_gp["tags_norm"] = job_gp["tags_norm"].fillna("")
                    job_gp["job_hours_pricebook"] = job_gp.get("job_hours_pricebook", 0.0).fillna(0.0)
                    job_gp["job_hours_appt"] = job_gp.get("job_hours_appt", 0.0).fillna(0.0)

                    # Only SERVICE jobs for this metric
                    job_gp = job_gp[job_gp["tags_norm"].apply(_is_service_job)].copy()

                    # Choose job hours: price-book if present, else appointment hours
                    job_gp["job_hours"] = job_gp.apply(
                        lambda r: float(r["job_hours_pricebook"]) if float(r["job_hours_pricebook"]) > 0 else float(r["job_hours_appt"]),
                        axis=1
                    )

                    # Gross profit per job per JC rules
                    job_gp["job_cost"] = job_gp["parts_cost"] + float(labor_cost_per_job)
                    job_gp["job_gross_profit"] = job_gp["job_revenue"] - job_gp["job_cost"]

                    # Assigned employees (exclude installers by ROLE)
                    df_job_assigned = df_job_emps[["job_id", "employee_id"]].dropna(subset=["employee_id"]).copy()

                    if not df_emp_dim.empty and "role" in df_emp_dim.columns:
                        emp_roles = df_emp_dim[["employee_id", "role"]].copy()
                        emp_roles["role_norm"] = emp_roles["role"].fillna("").astype(str).str.lower()
                        df_job_assigned = df_job_assigned.merge(emp_roles[["employee_id", "role_norm"]], how="left", on="employee_id")
                        df_job_assigned = df_job_assigned[~df_job_assigned["role_norm"].str.contains("install", na=False)].copy()
                        df_job_assigned.drop(columns=["role_norm"], inplace=True, errors="ignore")

                    job_gp_emp = job_gp.merge(df_job_assigned, how="inner", on="job_id")

                    if not job_gp_emp.empty:
                        # Tech count per job (for equal split)
                        tech_count = (
                            job_gp_emp.groupby("job_id")["employee_id"]
                            .nunique()
                            .reset_index()
                            .rename(columns={"employee_id": "tech_count"})
                        )
                        job_gp_emp = job_gp_emp.merge(tech_count, how="left", on="job_id")
                        job_gp_emp["tech_count"] = job_gp_emp["tech_count"].fillna(1).astype(int)

                        # Allocate GP and hours equally
                        job_gp_emp["gp_alloc"] = job_gp_emp["job_gross_profit"] / job_gp_emp["tech_count"]
                        job_gp_emp["hours_alloc"] = job_gp_emp["job_hours"] / job_gp_emp["tech_count"]

                        gp_emp = (
                            job_gp_emp.groupby("employee_id")
                            .agg(gross_profit_ytd=("gp_alloc", "sum"), hours_ytd=("hours_alloc", "sum"))
                            .reset_index()
                        )

                        gp_emp["gross_profit_per_hour_ytd"] = gp_emp.apply(
                            lambda r: (float(r["gross_profit_ytd"]) / float(r["hours_ytd"])) if float(r["hours_ytd"]) > 0 else 0.0,
                            axis=1
                        )

                        df_emp_gp = gp_emp[["employee_id", "gross_profit_per_hour_ytd"]]

                # Build employee card records
        df_cards = df_emp_dim[["employee_id", "employee_name", "role", "avatar_url", "color_hex"]].copy()
        df_cards = df_cards.merge(df_completed_jobs_emp, how="left", on="employee_id")
        df_cards = df_cards.merge(df_emp_atv, how="left", on="employee_id")
        df_cards = df_cards.merge(df_emp_daily, how="left", on="employee_id")
        df_cards = df_cards.merge(df_emp_cb, how="left", on="employee_id")
        df_cards = df_cards.merge(df_emp_hours, how="left", on="employee_id")
        df_cards = df_cards.merge(df_emp_gp, how="left", on="employee_id")

        df_cards["completed_jobs_ytd"] = df_cards["completed_jobs_ytd"].fillna(0).astype(int)
        df_cards["callback_jobs_ytd"] = df_cards.get("callback_jobs_ytd", 0).fillna(0).astype(int)
        df_cards["overall_invoice_count"] = df_cards.get("overall_invoice_count", 0).fillna(0).astype(int)
        df_cards["avg_ticket_overall"] = df_cards.get("avg_ticket_overall", 0.0).fillna(0.0).astype(float)
        df_cards["appt_hours_ytd"] = df_cards.get("appt_hours_ytd", 0.0).fillna(0.0).astype(float)
        df_cards["gross_profit_per_hour_ytd"] = df_cards.get("gross_profit_per_hour_ytd", 0.0).fillna(0.0).astype(float)

        df_cards["callback_pct_ytd"] = df_cards.apply(
            lambda r: (float(r["callback_jobs_ytd"]) / float(r["completed_jobs_ytd"]) * 100.0)
            if float(r["completed_jobs_ytd"]) > 0 else 0.0,
            axis=1
        )

        avg_ticket_threshold = 450.0
        callback_target = 3.0

        for _, r in df_cards.sort_values("completed_jobs_ytd", ascending=False).iterrows():
            callback_pct = float(r["callback_pct_ytd"])
            if callback_pct <= callback_target:
                callback_status = "success"
            elif callback_pct <= 7.0:
                callback_status = "warning"
            else:
                callback_status = "danger"

            gp_hr = float(r["gross_profit_per_hour_ytd"])
            if gp_hr >= gp_per_hour_target:
                gp_status = "success"
            elif gp_hr >= (gp_per_hour_target * 0.75):
                gp_status = "warning"
            else:
                gp_status = "danger"

            employee_cards.append({
                "employee_id": r["employee_id"],
                "name": r["employee_name"] or "Unknown",
                "role": r["role"] or "Technician",
                "avatar_url": r["avatar_url"],
                "color_hex": r["color_hex"],

                "completed_jobs_ytd": int(r["completed_jobs_ytd"]),

                # Average ticket (overall)
                "avg_ticket_overall": float(r["avg_ticket_overall"]),
                "avg_ticket_overall_display": _format_currency(float(r["avg_ticket_overall"])),
                "avg_ticket_overall_status": "success" if float(r["avg_ticket_overall"]) >= avg_ticket_threshold else "danger",
                "overall_invoice_count": int(r["overall_invoice_count"]),

                # Service revenue per day (YTD)
                "daily_revenue": float(r.get("service_revenue_per_day_ytd", 0.0)),
                "daily_revenue_display": _format_currency(float(r.get("service_revenue_per_day_ytd", 0.0))),
                "daily_revenue_status": "success" if float(r.get("service_revenue_per_day_ytd", 0.0)) >= 1350.0 else "danger",

                # Callback
                "callback_jobs_ytd": int(r["callback_jobs_ytd"]),
                "callback_pct_ytd": float(callback_pct),
                "callback_pct_display": f"{callback_pct:.1f}%",
                "callback_status": callback_status,
                "callback_target": callback_target,

                # Hours (from appointments)
                "appt_hours_ytd": float(r["appt_hours_ytd"]),
                "appt_hours_ytd_display": f"{float(r['appt_hours_ytd']):.1f} hrs",

                # Gross profit per hour (approx)
                "gross_profit_per_hour_ytd": gp_hr,
                "gross_profit_per_hour_ytd_display": _format_currency(gp_hr) + "/hr" if gp_hr else "$0/hr",
                "gp_per_hour_target": gp_per_hour_target,
                "gp_per_hour_status": gp_status,
            })

        # -----------------------------------
        # Estimates KPIs (best-effort)
        # What we can do right now:
        # - total estimates YTD
        # - estimate status mix by option.status and/or approval_status
        # Note: "won vs lost" requires consistent status mapping (approved/declined)
        # -----------------------------------
        estimates_kpis = {
            "estimates_ytd_count": 0,
            "estimate_options_status_breakdown": [],
        }

        if not df_estimates.empty:
            df_estimates = _ensure_columns(df_estimates, {
                "created_at": "object",
                "estimate_id": "object",
            })
            df_estimates["created_at_dt"] = _to_dt_utc(df_estimates["created_at"])
            df_est_ytd = df_estimates[df_estimates["created_at_dt"] >= start_of_year_utc].copy()
            estimates_kpis["estimates_ytd_count"] = int(len(df_est_ytd))

        if not df_estimate_options.empty:
            df_estimate_options = _ensure_columns(df_estimate_options, {
                "status": "object",
                "approval_status": "object",
                "total_amount": "float64",
            })

            tmp = df_estimate_options.copy()
            tmp["status_norm"] = tmp["status"].fillna("").astype(str).str.lower().str.strip()
            tmp["approval_norm"] = tmp["approval_status"].fillna("").astype(str).str.lower().str.strip()
            tmp["bucket"] = tmp.apply(
                lambda r: r["approval_norm"] if r["approval_norm"] else (r["status_norm"] if r["status_norm"] else "unknown"),
                axis=1
            )

            b = (
                tmp.groupby("bucket")
                .agg(
                    option_count=("bucket", "count"),
                    total_amount=("total_amount", "sum"),
                )
                .reset_index()
                .sort_values("option_count", ascending=False)
            )

            estimates_kpis["estimate_options_status_breakdown"] = [
                {
                    "bucket": str(row["bucket"]),
                    "option_count": int(row["option_count"]),
                    "total_amount": float(row["total_amount"] or 0.0),
                    "total_amount_display": _format_currency(float(row["total_amount"] or 0.0)),
                }
                for _, row in b.iterrows()
            ]

        # -----------------------------------
        # Refresh status + last_refresh formatting
        # -----------------------------------
        refresh_status = get_refresh_status()

        return {
            # FTC / Repeat visits
            "first_time_completion_pct": first_time_completion_pct,
            "first_time_completion_target": first_time_completion_target,
            "first_time_completed": first_time_completed,
            "repeat_visit_completed": repeat_visit_completed,
            "completed_jobs": completed_jobs,
            "repeat_visit_pct": repeat_visit_pct,
            "repeat_jobs": repeat_jobs_table,

            # Revenue
            "total_revenue_ytd": total_revenue_ytd,
            "total_revenue_ytd_display": total_revenue_ytd_display,
            "revenue_breakdown_ytd": revenue_breakdown_ytd,

            # Avg ticket overall (dashboard-level)
            "average_ticket_value": average_ticket_value,
            "average_ticket_display": average_ticket_display,

            # Average ticket split (dashboard-level)
            "service_avg_ticket_display": service_avg_ticket_display,
            "install_avg_ticket_display": install_avg_ticket_display,
            "service_invoice_count": service_invoice_count,
            "install_invoice_count": install_invoice_count,
            "average_ticket_status": average_ticket_status,
            "average_ticket_threshold": average_ticket_threshold,
            "invoice_count": invoice_count,

            # DFO
            "dfo_pct": dfo_pct,
            "dfo_count": dfo_count,
            "dfo_status_color": dfo_status_color,
            "dfo_monthly": dfo_monthly,

            # Employees
            "employee_cards": employee_cards,

            # GP/hour config (useful for UI labels)
            "gp_per_hour_target": gp_per_hour_target,

            # Estimates
            **estimates_kpis,

            # Meta
            "last_refresh": refresh_status.get("last_refresh_display", ""),
            **refresh_status,
        }

    finally:
        conn.close()


def get_refresh_status():
    conn = duckdb.connect(DB_FILE)
    try:
        ensure_tables(conn)

        row = conn.execute("""
            SELECT value FROM metadata WHERE key = 'last_refresh'
        """).fetchone()

        if not row or not row[0]:
            return {
                "can_refresh": True,
                "next_refresh": "Now",
                "last_refresh_display": "No data yet",
            }

        # 1-hour constraint
        try:
            last_refresh_utc = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
            if last_refresh_utc.tzinfo is None:
                last_refresh_utc = last_refresh_utc.replace(tzinfo=timezone.utc)
        except Exception:
            return {
                "can_refresh": True,
                "next_refresh": "Now",
                "last_refresh_display": "Unknown",
            }

        next_allowed = last_refresh_utc + timedelta(hours=1)
        now_utc = datetime.now(timezone.utc)

        last_refresh_central = last_refresh_utc.astimezone(CENTRAL_TZ)
        last_refresh_display = last_refresh_central.strftime("%b %d, %Y %I:%M %p %Z")

        return {
            "can_refresh": now_utc >= next_allowed,
            "next_refresh": next_allowed.astimezone(CENTRAL_TZ).strftime("%b %d, %Y %I:%M %p %Z"),
            "last_refresh_display": last_refresh_display,
        }

    finally:
        conn.close()
