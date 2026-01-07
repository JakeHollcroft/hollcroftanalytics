# kpis.py
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import re

PERSIST_DIR = Path(os.environ.get("PERSIST_DIR", "."))
DB_FILE = PERSIST_DIR / "housecall_data.duckdb"


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
    # backward compatible
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


def _money_fmt(x: float) -> str:
    try:
        return f"${x:,.2f}"
    except Exception:
        return "$0.00"


def _normalize_tags(tags_val) -> list[str]:
    """
    jobs.tags is stored as comma-separated string (per ingest).
    Return a list of clean tag strings.
    """
    if tags_val is None:
        return []
    if isinstance(tags_val, float) and pd.isna(tags_val):
        return []
    s = str(tags_val).strip()
    if not s or s.lower() == "none":
        return []
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _category_from_tags(tags: list[str]) -> str:
    """
    Map job tags into one bucket for revenue breakdown.
    Tweak these rules as your tagging matures.
    """
    if not tags:
        return "Other / Unclassified"

    t = " | ".join(tags).lower()

    # Prioritize commercial vs residential
    if "commercial" in t:
        if "maintenance" in t:
            return "Commercial Maintenance"
        if "install" in t:
            return "Commercial Install"
        if "service" in t:
            return "Commercial Service"
        return "Commercial"

    if "residential" in t:
        if "maintenance" in t:
            return "Residential Maintenance"
        if "install" in t:
            return "Residential Install"
        if "service" in t:
            return "Residential Service"
        return "Residential"

    # Other common buckets
    if "new construction" in t:
        return "New Construction"
    if "campaign" in t:
        return "Campaigns"
    if "warranty" in t:
        return "Warranty"
    if "callback" in t:
        return "Callback"

    return "Other / Unclassified"


def _coerce_dt_series(series: pd.Series) -> pd.Series:
    # Handles ISO with Z and nulls safely.
    return pd.to_datetime(series, errors="coerce", utc=True)


def get_dashboard_kpis():
    conn = duckdb.connect(DB_FILE)
    try:
        ensure_tables(conn)

        df_jobs = _safe_df(conn, "SELECT * FROM jobs")
        df_customers = _safe_df(conn, "SELECT * FROM customers")
        df_invoices = _safe_df(conn, "SELECT * FROM invoices")

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
                "due_amount": "float64",
                "invoice_date": "object",
                "paid_at": "object",
                "sent_at": "object",
                "due_at": "object",
            },
        )

        # -----------------------------
        # Last refresh
        # -----------------------------
        last_refresh = None
        try:
            row = conn.execute("SELECT value FROM metadata WHERE key = 'last_refresh'").fetchone()
            last_refresh = row[0] if row else None
        except Exception:
            last_refresh = None

        if last_refresh:
            try:
                utc_dt = datetime.fromisoformat(last_refresh).replace(tzinfo=ZoneInfo("UTC"))
                central_dt = utc_dt.astimezone(ZoneInfo("America/Chicago"))
                last_refresh_display = central_dt.strftime("%b %d, %Y %I:%M %p %Z")
            except Exception:
                last_refresh_display = "Unknown"
        else:
            last_refresh_display = "Never"

        # -----------------------------
        # Data cleanup
        # -----------------------------
        # amounts are cents -> dollars
        df_jobs["total_amount"] = pd.to_numeric(df_jobs.get("total_amount", 0), errors="coerce").fillna(0) / 100
        df_jobs["outstanding_balance"] = pd.to_numeric(df_jobs.get("outstanding_balance", 0), errors="coerce").fillna(0) / 100
        df_jobs["num_appointments"] = pd.to_numeric(df_jobs.get("num_appointments", 0), errors="coerce").fillna(0).astype(int)
        df_jobs["completed_at"] = _coerce_dt_series(df_jobs.get("completed_at"))

        df_invoices["amount"] = pd.to_numeric(df_invoices.get("amount", 0), errors="coerce").fillna(0) / 100
        df_invoices["due_amount"] = pd.to_numeric(df_invoices.get("due_amount", 0), errors="coerce").fillna(0) / 100

        # -----------------------------
        # Joins for FTC and table
        # -----------------------------
        if not df_jobs.empty and not df_customers.empty and {"customer_id"}.issubset(df_jobs.columns) and {"customer_id"}.issubset(df_customers.columns):
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
            df_jobs["first_name"].fillna("Unknown").astype(str)
            + " "
            + df_jobs["last_name"].fillna("").astype(str)
        ).str.strip()

        # -----------------------------
        # FTC: completed jobs YTD
        # -----------------------------
        completed_statuses = ["complete unrated", "complete rated"]
        df_completed = df_jobs[df_jobs["work_status"].isin(completed_statuses)].copy()

        central = ZoneInfo("America/Chicago")
        now_central = datetime.now(central)
        start_of_year_central = datetime(now_central.year, 1, 1, tzinfo=central)
        start_of_year_utc = start_of_year_central.astimezone(ZoneInfo("UTC"))

        df_completed = df_completed[df_completed["completed_at"] >= start_of_year_utc].copy()

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

        # repeat jobs table
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

        # -----------------------------
        # Revenue: invoices YTD, statuses paid + open + pending_payment
        # -----------------------------
        revenue_statuses = {"paid", "open", "pending_payment"}

        df_rev = df_invoices.copy()
        if not df_rev.empty:
            df_rev["status"] = df_rev["status"].astype(str).str.strip().str.lower()
            df_rev = df_rev[df_rev["status"].isin(revenue_statuses)].copy()

            # Choose an invoice "as-of" date for YTD:
            # invoice_date preferred, else paid_at, else sent_at, else due_at
            inv_dt = _coerce_dt_series(df_rev.get("invoice_date"))
            paid_dt = _coerce_dt_series(df_rev.get("paid_at"))
            sent_dt = _coerce_dt_series(df_rev.get("sent_at"))
            due_dt = _coerce_dt_series(df_rev.get("due_at"))

            df_rev["rev_dt"] = inv_dt
            df_rev.loc[df_rev["rev_dt"].isna(), "rev_dt"] = paid_dt[df_rev["rev_dt"].isna()]
            df_rev.loc[df_rev["rev_dt"].isna(), "rev_dt"] = sent_dt[df_rev["rev_dt"].isna()]
            df_rev.loc[df_rev["rev_dt"].isna(), "rev_dt"] = due_dt[df_rev["rev_dt"].isna()]

            df_rev = df_rev[df_rev["rev_dt"] >= start_of_year_utc].copy()

            # join tags from jobs
            if not df_jobs.empty and "job_id" in df_jobs.columns and "job_id" in df_rev.columns:
                df_rev = df_rev.merge(
                    df_jobs[["job_id", "tags"]],
                    how="left",
                    on="job_id",
                )
            else:
                df_rev["tags"] = None

            # normalize + categorize
            df_rev["tags_list"] = df_rev["tags"].apply(_normalize_tags)
            df_rev["category"] = df_rev["tags_list"].apply(_category_from_tags)

        total_revenue_ytd = float(df_rev["amount"].sum()) if not df_rev.empty else 0.0

        # breakdown
        if not df_rev.empty:
            breakdown = (
                df_rev.groupby("category", as_index=False)["amount"]
                .sum()
                .sort_values("amount", ascending=False)
            )
        else:
            breakdown = pd.DataFrame(columns=["category", "amount"])

        # format for template
        revenue_breakdown_ytd = []
        for _, row in breakdown.iterrows():
            revenue_breakdown_ytd.append(
                {
                    "category": str(row["category"]),
                    "revenue": float(row["amount"]),
                    "revenue_display": _money_fmt(float(row["amount"])),
                }
            )

        # -----------------------------
        # Refresh status
        # -----------------------------
        refresh_status = get_refresh_status()

        return {
            # existing FTC fields (kept)
            "first_time_completion_pct": first_time_completion_pct,
            "first_time_completion_target": first_time_completion_target,
            "first_time_completed": first_time_completed,
            "repeat_visit_completed": repeat_visit_completed,
            "completed_jobs": completed_jobs,
            "repeat_visit_pct": repeat_visit_pct,
            "repeat_jobs": repeat_jobs_table.to_dict(orient="records"),
            "last_refresh": last_refresh_display,

            # new revenue fields
            "total_revenue_ytd": total_revenue_ytd,
            "total_revenue_ytd_display": _money_fmt(total_revenue_ytd),
            "revenue_breakdown_ytd": revenue_breakdown_ytd,

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
            return {"can_refresh": True, "next_refresh": "Now"}

        try:
            last_refresh_utc = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
        except Exception:
            return {"can_refresh": True, "next_refresh": "Now"}

        next_allowed = last_refresh_utc + timedelta(hours=1)

        now_utc = datetime.now(timezone.utc)
        central = ZoneInfo("America/Chicago")

        return {
            "can_refresh": now_utc >= next_allowed,
            "next_refresh": next_allowed.astimezone(central).strftime("%b %d, %Y %I:%M %p %Z"),
        }

    finally:
        conn.close()
