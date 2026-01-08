import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os

PERSIST_DIR = Path(os.environ.get("PERSIST_DIR", "."))
DB_FILE = PERSIST_DIR / "housecall_data.duckdb"


# -----------------------------
# Tables / schema
# -----------------------------
def ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    # metadata
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    # jobs
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
    # Backward-compatible schema upgrade
    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN tags TEXT")
    except Exception:
        pass

    # customers
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

    # invoices
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


# -----------------------------
# Helpers
# -----------------------------
def _safe_df(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    try:
        return conn.execute(sql).df()
    except Exception:
        return pd.DataFrame()


def _ensure_columns(df: pd.DataFrame, cols_with_dtypes: dict) -> pd.DataFrame:
    """
    Ensures df has the columns listed. If missing, create empty column with dtype.
    """
    for col, dtype in cols_with_dtypes.items():
        if col not in df.columns:
            try:
                df[col] = pd.Series(dtype=dtype)
            except Exception:
                df[col] = pd.Series(dtype="object")
    return df


def _convert_cents_to_dollars(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Convert specified monetary columns from cents to dollars.
    Divides by 100 and handles NaN values gracefully.
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x / 100 if pd.notna(x) else 0.0
            )
    return df


def _normalize_tags(tags_val) -> str:
    """
    Normalize tags into a single lowercased string with comma delimiters.
    Example: "Residential Service, Callback" -> "residential service,callback"
    """
    if tags_val is None or (isinstance(tags_val, float) and pd.isna(tags_val)):
        return ""
    s = str(tags_val)
    parts = [p.strip().lower() for p in s.split(",") if p.strip()]
    return ",".join(parts)


def _tag_has(tags_norm: str, needle: str) -> bool:
    """
    Check membership on normalized comma-delimited tags.
    """
    if not tags_norm:
        return False
    needle = needle.strip().lower()
    parts = tags_norm.split(",")
    return needle in parts


def _map_category_from_tags(tags_norm: str) -> str:
    """
    Map a job's tags to ONE revenue category bucket.
    Priority: install/change-out > maintenance > service.
    """
    is_res = _tag_has(tags_norm, "residential")
    is_com = _tag_has(tags_norm, "commercial")

    is_install = _tag_has(tags_norm, "install") or _tag_has(tags_norm, "installation") or _tag_has(tags_norm, "change out") or _tag_has(tags_norm, "change-out")
    is_maint = _tag_has(tags_norm, "maintenance")
    is_service = _tag_has(tags_norm, "service") or _tag_has(tags_norm, "demand service") or _tag_has(tags_norm, "demand")

    s = tags_norm
    if "residential install" in s or "residential change out" in s or "residential change-out" in s:
        is_res, is_install = True, True
    if "commercial install" in s or "commercial change out" in s or "commercial change-out" in s:
        is_com, is_install = True, True
    if "residential maintenance" in s:
        is_res, is_maint = True, True
    if "commercial maintenance" in s:
        is_com, is_maint = True, True
    if "residential service" in s or "residential demand service" in s:
        is_res, is_service = True, True
    if "commercial service" in s or "commercial demand service" in s:
        is_com, is_service = True, True

    if is_install and is_res:
        return "Residential change out"
    if is_install and is_com:
        return "Commercial change out"
    if is_maint and is_res:
        return "Residential maintenance"
    if is_maint and is_com:
        return "Commercial maintenance"
    if is_service and is_res:
        return "Residential demand service"
    if is_service and is_com:
        return "Commercial demand service"

    return "Other / Unclassified"


def _is_dfo_job(tags_norm: str) -> bool:
    """
    DFO = Diagnostic Fee Only
    A job is DFO if it has service/demand service tag but NOT install/change-out.
    Meaning: service was rendered without parts installation.
    """
    is_service = _tag_has(tags_norm, "service") or _tag_has(tags_norm, "demand service") or _tag_has(tags_norm, "demand")
    is_install = _tag_has(tags_norm, "install") or _tag_has(tags_norm, "installation") or _tag_has(tags_norm, "change out") or _tag_has(tags_norm, "change-out")
    
    return is_service and not is_install


def _format_currency(x: float) -> str:
    try:
        return "${:,.0f}".format(float(x))
    except Exception:
        return "$0"


# Get last_refresh_display for module use
last_refresh_display = ""

# -----------------------------
# KPI builder
# -----------------------------
def get_dashboard_kpis():
    global last_refresh_display
    
    conn = duckdb.connect(DB_FILE)
    try:
        ensure_tables(conn)

        # Load data (safe even if first run or DB contention)
        df_jobs = _safe_df(conn, "SELECT * FROM jobs")
        df_customers = _safe_df(conn, "SELECT * FROM customers")
        df_invoices = _safe_df(conn, "SELECT * FROM invoices")

        # Ensure expected columns exist even if tables are empty
        df_jobs = _ensure_columns(
            df_jobs,
            {
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
            },
        )

        df_customers = _ensure_columns(
            df_customers,
            {
                "customer_id": "object",
                "first_name": "object",
                "last_name": "object",
                "email": "object",
                "mobile_number": "object",
                "home_number": "object",
                "work_number": "object",
                "company": "object",
                "notifications_enabled": "bool",
                "lead_source": "object",
                "notes": "object",
                "created_at": "object",
                "updated_at": "object",
                "company_name": "object",
                "company_id": "object",
                "tags": "object",
            },
        )

        df_invoices = _ensure_columns(
            df_invoices,
            {
                "invoice_id": "object",
                "job_id": "object",
                "invoice_number": "object",
                "status": "object",
                "amount": "float64",
                "subtotal": "float64",
                "due_amount": "float64",
                "due_at": "object",
                "paid_at": "object",
                "sent_at": "object",
                "service_date": "object",
                "invoice_date": "object",
                "display_due_concept": "object",
                "due_concept": "object",
            },
        )

        # Convert monetary values from cents to dollars
        df_jobs = _convert_cents_to_dollars(df_jobs, ["total_amount", "outstanding_balance"])
        df_invoices = _convert_cents_to_dollars(df_invoices, ["amount", "subtotal", "due_amount"])

        # Normalize tags
        if not df_jobs.empty:
            df_jobs["tags_norm"] = df_jobs["tags"].apply(_normalize_tags)
        else:
            df_jobs["tags_norm"] = ""

        # Parse datetime columns
        for col in ["created_at", "updated_at", "completed_at"]:
            if col in df_jobs.columns:
                df_jobs[col] = pd.to_datetime(df_jobs[col], errors="coerce", utc=True)

        for col in ["paid_at", "sent_at", "service_date", "invoice_date"]:
            if col in df_invoices.columns:
                df_invoices[col] = pd.to_datetime(df_invoices[col], errors="coerce", utc=True)

        # Ensure paid_at_dt exists for revenue filtering
        if "paid_at" in df_invoices.columns:
            df_invoices["paid_at_dt"] = df_invoices["paid_at"]
        else:
            df_invoices["paid_at_dt"] = pd.NaT

        # invoices -> job data
        if (
            not df_invoices.empty
            and not df_jobs.empty
            and "job_id" in df_invoices.columns
            and "job_id" in df_jobs.columns
        ):
            df_invoices = df_invoices.merge(
                df_jobs[["job_id", "customer_id"]],
                how="left",
                on="job_id",
            )
        else:
            if "customer_id" not in df_invoices.columns:
                df_invoices["customer_id"] = pd.Series(dtype="object")

        # jobs -> customer name
        if (
            not df_jobs.empty
            and not df_customers.empty
            and "customer_id" in df_jobs.columns
            and "customer_id" in df_customers.columns
        ):
            df_jobs = df_jobs.merge(
                df_customers[["customer_id", "first_name", "last_name"]],
                how="left",
                on="customer_id",
            )
        else:
            if "first_name" not in df_jobs.columns:
                df_jobs["first_name"] = pd.Series(dtype="object")
            if "last_name" not in df_jobs.columns:
                df_jobs["last_name"] = pd.Series(dtype="object")

        df_jobs["customer_name"] = (
            df_jobs["first_name"].fillna("Unknown").astype(str) + " " +
            df_jobs["last_name"].fillna("").astype(str)
        ).str.strip()

        # -----------------------------------
        # Time windows (YTD)
        # -----------------------------------
        central = ZoneInfo("America/Chicago")
        now_central = datetime.now(central)
        start_of_year_central = datetime(now_central.year, 1, 1, tzinfo=central)
        start_of_year_utc = start_of_year_central.astimezone(ZoneInfo("UTC"))

        # -----------------------------------
        # Revenue KPI (YTD)
        # paid + open + pending_payment
        # -----------------------------------
        revenue_statuses = {"paid", "open", "pending_payment"}
        df_rev = df_invoices[df_invoices["status"].astype(str).str.lower().isin(revenue_statuses)].copy()

        # YTD filter logic:
        # - paid: use paid_at when available; if missing, fall back to service_date/invoice_date
        # - open/pending_payment: use service_date or invoice_date (since paid_at is usually null)
        df_rev["service_date_dt"] = pd.to_datetime(df_rev.get("service_date"), errors="coerce", utc=True)
        df_rev["invoice_date_dt"] = pd.to_datetime(df_rev.get("invoice_date"), errors="coerce", utc=True)

        def _row_effective_dt(row):
            st = str(row.get("status") or "").lower()
            if st == "paid":
                if pd.notna(row.get("paid_at_dt")):
                    return row.get("paid_at_dt")
            # fallbacks
            if pd.notna(row.get("service_date_dt")):
                return row.get("service_date_dt")
            if pd.notna(row.get("invoice_date_dt")):
                return row.get("invoice_date_dt")
            return pd.NaT

        if not df_rev.empty:
            df_rev["effective_dt"] = df_rev.apply(_row_effective_dt, axis=1)
            df_rev = df_rev[df_rev["effective_dt"] >= start_of_year_utc].copy()
        else:
            df_rev["effective_dt"] = pd.NaT

        # Join tags onto invoices for category breakdown
        if not df_rev.empty and not df_jobs.empty and "job_id" in df_jobs.columns:
            df_rev = df_rev.merge(
                df_jobs[["job_id", "tags_norm"]],
                how="left",
                on="job_id",
            )
        else:
            if "tags_norm" not in df_rev.columns:
                df_rev["tags_norm"] = ""

        df_rev["category"] = df_rev["tags_norm"].fillna("").apply(_map_category_from_tags)

        total_revenue_ytd = float(df_rev["amount"].sum()) if not df_rev.empty else 0.0

        breakdown = (
            df_rev.groupby("category", dropna=False)["amount"]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"amount": "revenue"})
        )

        revenue_breakdown = []
        if not breakdown.empty:
            for _, row in breakdown.iterrows():
                revenue_breakdown.append(
                    {
                        "category": str(row["category"]),
                        "revenue": float(row["revenue"]),
                        "revenue_display": _format_currency(row["revenue"]),
                    }
                )

        total_revenue_display = _format_currency(total_revenue_ytd)

        # -----------------------------------
        # Job subsets: completed jobs THIS YEAR and forward
        # -----------------------------------
        completed_statuses = ["complete unrated", "complete rated"]
        df_completed = df_jobs[df_jobs["work_status"].isin(completed_statuses)].copy()

        # completed_at is UTC-aware datetime because we parsed with utc=True
        df_completed = df_completed[df_completed["completed_at"] >= start_of_year_utc].copy()

        # -----------------------------------
        # First-Time Completion KPI
        # Completed jobs with exactly 1 appointment.
        # -----------------------------------
        completed_jobs = len(df_completed)

        first_time_completed = int((df_completed["num_appointments"] == 1).sum()) if completed_jobs > 0 else 0
        repeat_visit_completed = int((df_completed["num_appointments"] >= 2).sum()) if completed_jobs > 0 else 0

        if completed_jobs > 0:
            first_time_completion_pct = round((first_time_completed / completed_jobs) * 100, 2)
            repeat_visit_pct = round((repeat_visit_completed / completed_jobs) * 100, 2)
        else:
            first_time_completion_pct = 0.0
            repeat_visit_pct = 0.0

        first_time_completion_target = 85

        # -----------------------------------
        # DFO (Diagnostic Fee Only) KPI
        # Service jobs that are NOT installations/change-outs
        # -----------------------------------
        df_dfo = df_completed[df_completed["tags_norm"].apply(_is_dfo_job)].copy()
        dfo_count = len(df_dfo)
        
        if completed_jobs > 0:
            dfo_pct = round((dfo_count / completed_jobs) * 100, 2)
        else:
            dfo_pct = 0.0

        # DFO color status: < 5% = green, 5-10% = orange, >= 10% = red
        if dfo_pct < 5:
            dfo_status_color = "success"  # green
        elif dfo_pct < 10:
            dfo_status_color = "warning"  # orange
        else:
            dfo_status_color = "danger"   # red

        # Monthly breakdown for DFO
        dfo_monthly = []
        if not df_dfo.empty:
            df_dfo_monthly = df_dfo.copy()
            df_dfo_monthly["month"] = pd.to_datetime(df_dfo_monthly["completed_at"]).dt.to_period("M")
            monthly_counts = df_dfo_monthly.groupby("month").size()
            
            # Also get total jobs per month for percentage
            df_completed_monthly = df_completed.copy()
            df_completed_monthly["month"] = pd.to_datetime(df_completed_monthly["completed_at"]).dt.to_period("M")
            monthly_total = df_completed_monthly.groupby("month").size()
            
            for month in monthly_counts.index:
                dfo_cnt = monthly_counts.get(month, 0)
                total_cnt = monthly_total.get(month, 1)
                pct = round((dfo_cnt / total_cnt) * 100, 1) if total_cnt > 0 else 0
                dfo_monthly.append({
                    "month": str(month),
                    "dfo_count": int(dfo_cnt),
                    "total_count": int(total_cnt),
                    "dfo_pct": float(pct)
                })

        # -----------------------------------
        # Repeat-visit table (action list)
        # Show most recent completed jobs with num_appointments >= 2
        # -----------------------------------
        repeat_jobs_df = df_completed[df_completed["num_appointments"] >= 2].copy()
        repeat_jobs_df = repeat_jobs_df.sort_values(by="completed_at", ascending=False)

        def fmt_dt(dt):
            if pd.isna(dt):
                return ""
            try:
                return dt.tz_convert(central).strftime("%b %d, %Y %I:%M %p")
            except Exception:
                return str(dt)

        repeat_jobs_df["completed_at_central"] = repeat_jobs_df["completed_at"].apply(fmt_dt)

        # Keep table small + useful
        needed_cols = [
            "job_id",
            "customer_name",
            "work_status",
            "num_appointments",
            "completed_at_central",
            "description",
        ]
        for c in needed_cols:
            if c not in repeat_jobs_df.columns:
                repeat_jobs_df[c] = ""

        repeat_jobs_table = repeat_jobs_df[needed_cols].head(25)

        # -----------------------------------
        # Refresh status
        # -----------------------------------
        refresh_status = get_refresh_status()
        
        # Set global for module use
        last_refresh_display = refresh_status.get("last_refresh_display", "")

        return {
            # Existing KPI
            "first_time_completion_pct": first_time_completion_pct,
            "first_time_completion_target": first_time_completion_target,
            "first_time_completed": first_time_completed,
            "repeat_visit_completed": repeat_visit_completed,
            "completed_jobs": completed_jobs,
            "repeat_visit_pct": repeat_visit_pct,
            "repeat_jobs": repeat_jobs_table.to_dict(orient="records"),

            # New KPI (YTD)
            "total_revenue_ytd": total_revenue_ytd,
            "total_revenue_ytd_display": total_revenue_display,
            "revenue_breakdown_ytd": revenue_breakdown,

            # DFO KPI
            "dfo_pct": dfo_pct,
            "dfo_count": dfo_count,
            "dfo_status_color": dfo_status_color,
            "dfo_monthly": dfo_monthly,

            # Meta
            "last_refresh": last_refresh_display,
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

        if not row:
            return {
                "can_refresh": True,
                "next_refresh": "Now",
                "last_refresh_display": "No data yet"
            }

        # 1-hour constraint
        try:
            last_refresh_utc = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
        except Exception:
            return {
                "can_refresh": True,
                "next_refresh": "Now",
                "last_refresh_display": "Unknown"
            }

        next_allowed = last_refresh_utc + timedelta(hours=1)
        now_utc = datetime.now(timezone.utc)
        central = ZoneInfo("America/Chicago")
        
        last_refresh_central = last_refresh_utc.astimezone(central)
        last_refresh_display = last_refresh_central.strftime("%b %d, %Y %I:%M %p %Z")

        return {
            "can_refresh": now_utc >= next_allowed,
            "next_refresh": next_allowed.astimezone(central).strftime("%b %d, %Y %I:%M %p %Z"),
            "last_refresh_display": last_refresh_display,
        }

    finally:
        conn.close()