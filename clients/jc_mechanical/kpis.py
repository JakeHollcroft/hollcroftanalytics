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
    """
    Robust tag matcher.

    Works for:
      - comma-separated tags (e.g., "commercial, service")
      - single-phrase tags (e.g., "Commercial Service")

    Rules:
      - For multi-word needles (phrases), use substring match.
      - For single-word needles, use a word-boundary style match on common separators.
    """
    if not tags_norm:
        return False

    s = str(tags_norm).strip().lower()
    n = str(needle).strip().lower()
    if not n:
        return False

    # Phrase match
    if " " in n:
        return n in s

    # Word-style match across separators
    import re
    return re.search(r'(^|[\s,])' + re.escape(n) + r'($|[\s,])', s) is not None


def _format_currency(x: float) -> str:
    try:
        v = float(x)
        # Guard against NaN/inf
        if v != v or v == float("inf") or v == float("-inf"):
            v = 0.0
        return "${:,.0f}".format(v)
    except Exception:
        return "$0"


def _map_category_from_tags(tags_norm: str) -> str:
    """
    Revenue categories requested by JC Mechanical:
      - Residential Install
      - Residential Service
      - Residential Maintenance
      - Commercial Install
      - Commercial Service
      - Commercial Maintenance
      - New Construction

    Everything else -> Other / Unclassified
    """

    s = (tags_norm or "").strip().lower()

    is_res = _tag_has(s, "residential") or ("residential" in s)
    is_com = _tag_has(s, "commercial") or ("commercial" in s)

    is_new_construction = (
        _tag_has(s, "new construction")
        or _tag_has(s, "new-construction")
        or _tag_has(s, "new build")
        or _tag_has(s, "new-build")
        or _tag_has(s, "newconstruction")
    )

    is_install = (
        _tag_has(s, "install")
        or _tag_has(s, "installation")
        or _tag_has(s, "change out")
        or _tag_has(s, "change-out")
        or _tag_has(s, "changeout")
    )

    is_maint = _tag_has(s, "maintenance")
    is_service = (
        _tag_has(s, "service")
        or _tag_has(s, "demand service")
        or _tag_has(s, "demand")
    )

    if is_new_construction:
        return "New Construction"

    if is_res and is_install:
        return "Residential Install"
    if is_res and is_maint:
        return "Residential Maintenance"
    if is_res and is_service and not is_install:
        return "Residential Service"

    if is_com and is_install:
        return "Commercial Install"
    if is_com and is_maint:
        return "Commercial Maintenance"
    if is_com and is_service and not is_install:
        return "Commercial Service"

    # If tags have install/service/maintenance but no res/com qualifier, keep it unclassified for now
    return "Other / Unclassified"


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
        # IMPORTANT: "Revenue" here is what we BILL for (not what is PAID).
        # Population = invoices with status IN ('open','pending_payment') and invoice_date in current year.
        # -----------------------------------
        billed_statuses = {"open", "pending_payment"}

        df_rev = df_invoices[df_invoices["status"].astype(str).str.lower().isin(billed_statuses)].copy()

        if not df_rev.empty:
            # Use invoice_date as the time anchor for billed revenue
            df_rev["invoice_date_dt"] = _to_dt_utc(df_rev["invoice_date"]) if "invoice_date" in df_rev.columns else pd.NaT
            df_rev = df_rev[df_rev["invoice_date_dt"] >= start_of_year_utc].copy()
        else:
            df_rev["invoice_date_dt"] = pd.NaT

        # Join tags onto revenue for category breakdown
        if not df_rev.empty and not df_jobs.empty:
            df_rev = df_rev.merge(df_jobs[["job_id", "tags_norm"]], how="left", on="job_id")

        df_rev["tags_norm"] = df_rev.get("tags_norm", "").fillna("")
        df_rev["category"] = df_rev["tags_norm"].apply(_map_category_from_tags)

        df_rev["amount_dollars"] = pd.to_numeric(df_rev["amount"], errors="coerce").fillna(0.0).astype(float)

        total_revenue_ytd = float(df_rev["amount_dollars"].sum()) if not df_rev.empty else 0.0
        total_revenue_ytd_display = _format_currency(total_revenue_ytd)

        breakdown = (
            df_rev.groupby("category", dropna=False)["amount_dollars"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"amount_dollars": "revenue"})
        )

        revenue_breakdown_ytd = []
        if not breakdown.empty:
            total_breakdown = float(breakdown["revenue"].sum())
            rows = []
            for _, row in breakdown.iterrows():
                rev = float(row["revenue"])
                cat = str(row["category"]).strip() or "Other / Unclassified"
                pct_raw = (rev / total_breakdown * 100.0) if total_breakdown > 0 else 0.0
                rows.append({"category": cat, "revenue": rev, "pct_raw": pct_raw})

            pct_ints = _largest_remainder_int_percentages([r["pct_raw"] for r in rows])
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
        # KPI: Average Ticket (overall) YTD (BILLED ONLY)
        # Definition: total billed revenue YTD / billed invoice count YTD
        # -----------------------------------
        # -----------------------------------
        invoice_count = int(len(df_rev)) if not df_rev.empty else 0
        average_ticket_value = (total_revenue_ytd / invoice_count) if invoice_count > 0 else 0.0
        average_ticket_threshold = 450.0
        average_ticket_status = "success" if average_ticket_value >= average_ticket_threshold else "danger"
        average_ticket_display = _format_currency(average_ticket_value)

        # Split Average Ticket (Service vs Install) - billed invoices only
        # Service = categories Residential Service + Commercial Service
        # Install = categories Residential Install + Commercial Install (+ New Construction counted as install bucket)
        df_rev_service = df_rev[df_rev["category"].isin(["Residential Service", "Commercial Service"])].copy() if not df_rev.empty else pd.DataFrame()
        df_rev_install = df_rev[df_rev["category"].isin(["Residential Install", "Commercial Install", "New Construction"])].copy() if not df_rev.empty else pd.DataFrame()

        service_invoice_count = int(len(df_rev_service)) if not df_rev_service.empty else 0
        install_invoice_count = int(len(df_rev_install)) if not df_rev_install.empty else 0

        service_revenue = float(df_rev_service["amount_dollars"].sum()) if not df_rev_service.empty else 0.0
        install_revenue = float(df_rev_install["amount_dollars"].sum()) if not df_rev_install.empty else 0.0

        service_avg_ticket = (service_revenue / service_invoice_count) if service_invoice_count > 0 else 0.0
        install_avg_ticket = (install_revenue / install_invoice_count) if install_invoice_count > 0 else 0.0

        service_avg_ticket_display = _format_currency(service_avg_ticket)
        install_avg_ticket_display = _format_currency(install_avg_ticket)

        service_avg_ticket_status = "success" if service_avg_ticket >= average_ticket_threshold else "danger"
        install_avg_ticket_status = "success" if install_avg_ticket >= average_ticket_threshold else "danger"

        # -----------------------------------
        # Employee KPIs (cards)
        # - Completed jobs YTD
        # - Average ticket (overall) YTD (not split)
        # - Callback % YTD
        # - Appointment hours YTD (from job_appointments)
        # - Gross profit per hour (approx) YTD using invoice_items unit_cost as cost
        # -----------------------------------
        employee_cards = []

        # Gross profit per hour (YTD)
        # PENDING: we are intentionally not calculating GP/hr until the business rules are confirmed.
        gp_per_hour_pending = True
        gp_per_hour_target = 0.0  # placeholder
        df_emp_gp = pd.DataFrame(columns=["employee_id", "gross_profit_per_hour_ytd"])

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

        # Avg ticket overall per tech YTD (invoice amounts attributed to all assigned techs on job)
        df_emp_atv = pd.DataFrame(columns=["employee_id", "avg_ticket_overall", "overall_invoice_count"])
        if not df_rev.empty and not df_job_emps.empty:
            df_inv_emp = df_rev[["invoice_id", "job_id", "amount_dollars"]].merge(
                df_job_emps[["job_id", "employee_id"]].dropna(subset=["employee_id"]),
                how="inner",
                on="job_id",
            )
            if not df_inv_emp.empty:
                tmp = (
                    df_inv_emp.groupby("employee_id")
                    .agg(
                        overall_revenue=("amount_dollars", "sum"),
                        overall_invoice_count=("invoice_id", "nunique"),
                    )
                    .reset_index()
                )
                tmp["avg_ticket_overall"] = tmp.apply(
                    lambda r: (float(r["overall_revenue"]) / float(r["overall_invoice_count"]))
                    if float(r["overall_invoice_count"]) > 0 else 0.0,
                    axis=1,
                )
                df_emp_atv = tmp[["employee_id", "avg_ticket_overall", "overall_invoice_count"]]

        
        # $ / Day (Service) per tech (billed invoices only)
        # Definition: service billed revenue / distinct invoice dates (central) for that tech
        df_emp_daily = pd.DataFrame(columns=["employee_id", "daily_revenue_service"])
        if (not df_rev_service.empty) and (not df_job_emps.empty):
            df_tmp = df_rev_service[["invoice_id", "job_id", "amount_dollars", "invoice_date_dt"]].copy()
            # Central date bucket
            try:
                df_tmp["inv_day_central"] = df_tmp["invoice_date_dt"].dt.tz_convert(CENTRAL_TZ).dt.date
            except Exception:
                df_tmp["inv_day_central"] = pd.to_datetime(df_tmp["invoice_date_dt"], errors="coerce").dt.date

            df_tmp = df_tmp.merge(
                df_job_emps[["job_id", "employee_id"]].dropna(subset=["employee_id"]),
                how="inner",
                on="job_id",
            )

            if not df_tmp.empty:
                g = (
                    df_tmp.groupby("employee_id")
                    .agg(service_revenue=("amount_dollars", "sum"), days=("inv_day_central", "nunique"))
                    .reset_index()
                )
                g["daily_revenue_service"] = g.apply(
                    lambda r: (float(r["service_revenue"]) / float(r["days"])) if float(r["days"]) > 0 else 0.0,
                    axis=1,
                )
                df_emp_daily = g[["employee_id", "daily_revenue_service"]]
# Callback % per tech YTD (completed jobs tagged callback/recall/warranty)
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
        df_cards["daily_revenue_service"] = df_cards.get("daily_revenue_service", 0.0).fillna(0.0).astype(float)
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

            gp_hr = 0.0
            gp_status = "pending"

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

                # $ / Day (Service)
                "daily_revenue": float(r.get("daily_revenue_service", 0.0)),
                "daily_revenue_display": _format_currency(float(r.get("daily_revenue_service", 0.0))),
                "daily_revenue_status": "success" if float(r.get("daily_revenue_service", 0.0)) >= 1350.0 else "danger",

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
                "gross_profit_per_hour_ytd_display": "Pending",
                "gp_per_hour_target": gp_per_hour_target,
            "gp_per_hour_pending": gp_per_hour_pending,
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
            "average_ticket_status": average_ticket_status,
            "average_ticket_threshold": average_ticket_threshold,
            "invoice_count": invoice_count,

            # Average ticket split (billed invoices only)
            "service_avg_ticket": service_avg_ticket,
            "service_avg_ticket_display": service_avg_ticket_display,
            "service_avg_ticket_status": service_avg_ticket_status,
            "service_invoice_count": service_invoice_count,

            "install_avg_ticket": install_avg_ticket,
            "install_avg_ticket_display": install_avg_ticket_display,
            "install_avg_ticket_status": install_avg_ticket_status,
            "install_invoice_count": install_invoice_count,

            # DFO
            "dfo_pct": dfo_pct,
            "dfo_count": dfo_count,
            "dfo_status_color": dfo_status_color,
            "dfo_monthly": dfo_monthly,

            # Employees
            "employee_cards": employee_cards,

            # GP/hour config (useful for UI labels)
            "gp_per_hour_target": gp_per_hour_target,
            "gp_per_hour_pending": gp_per_hour_pending,

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