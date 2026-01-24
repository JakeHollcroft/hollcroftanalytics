import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from .kpi_parts.helpers import (
    CENTRAL_TZ,
    _safe_df,
    _ensure_columns,
    _to_dt_utc,
    _convert_cents_to_dollars,
    _normalize_tags,
    _start_of_year_utc,
    _map_category_from_tags,
)

from .kpi_parts import (
    kpi_quality,
    kpi_revenue,
    kpi_employees,
    kpi_estimates,
    kpi_lead_turnover,
)


# ---- Persistence / DuckDB location ----
def _resolve_persist_dir() -> Path:
    env = os.environ.get("PERSIST_DIR")
    if env:
        return Path(env)
    # Render persistent disk commonly mounts at /var/data
    if Path("/var/data").exists():
        return Path("/var/data")
    # Fallback: project root (works for local dev)
    return Path(__file__).resolve().parents[2]


PERSIST_DIR = _resolve_persist_dir()
DB_FILE = PERSIST_DIR / "housecall_data.duckdb"


def get_dashboard_kpis():
    conn = duckdb.connect(DB_FILE, read_only=True)
    try:
        # -----------------------------
        # Load tables (best-effort)
        # -----------------------------
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

        # -----------------------------
        # Ensure expected columns exist
        # -----------------------------
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

        # -----------------------------
        # Convert cents -> dollars (HCP returns cents)
        # -----------------------------
        df_jobs = _convert_cents_to_dollars(df_jobs, ["total_amount", "outstanding_balance"])
        df_invoices = _convert_cents_to_dollars(df_invoices, ["amount", "subtotal", "due_amount"])
        df_invoice_items = _convert_cents_to_dollars(df_invoice_items, ["unit_cost", "unit_price", "amount"])

        # -----------------------------
        # Normalize tags + datetimes
        # -----------------------------
        df_jobs["tags_norm"] = df_jobs["tags"].apply(_normalize_tags) if not df_jobs.empty else ""

        for col in ["created_at", "updated_at", "completed_at"]:
            df_jobs[col] = _to_dt_utc(df_jobs[col]) if col in df_jobs.columns else pd.NaT

        df_invoices["paid_at_dt"] = _to_dt_utc(df_invoices["paid_at"]) if "paid_at" in df_invoices.columns else pd.NaT
        df_invoices["service_date_dt"] = _to_dt_utc(df_invoices["service_date"]) if "service_date" in df_invoices.columns else pd.NaT
        df_invoices["invoice_date_dt"] = _to_dt_utc(df_invoices["invoice_date"]) if "invoice_date" in df_invoices.columns else pd.NaT

        df_job_appts["start_dt"] = _to_dt_utc(df_job_appts["start_time"]) if "start_time" in df_job_appts.columns else pd.NaT
        df_job_appts["end_dt"] = _to_dt_utc(df_job_appts["end_time"]) if "end_time" in df_job_appts.columns else pd.NaT

        # -----------------------------
        # Join jobs -> customers for name
        # -----------------------------
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

        # -----------------------------
        # Prefer appointment count from job_appointments if present
        # -----------------------------
        if not df_job_appts.empty and "job_id" in df_job_appts.columns:
            appt_counts = df_job_appts.groupby("job_id")["appointment_id"].nunique().reset_index()
            appt_counts.rename(columns={"appointment_id": "num_appointments_calc"}, inplace=True)
            df_jobs = df_jobs.merge(appt_counts, how="left", on="job_id")
            df_jobs["num_appointments"] = (
                df_jobs["num_appointments_calc"]
                .fillna(df_jobs["num_appointments"])
                .fillna(0)
                .astype(int)
            )
            df_jobs.drop(columns=["num_appointments_calc"], inplace=True, errors="ignore")
        else:
            df_jobs["num_appointments"] = pd.to_numeric(df_jobs["num_appointments"], errors="coerce").fillna(0).astype(int)

        # -----------------------------
        # YTD window + completed jobs population
        # -----------------------------
        start_of_year_utc = _start_of_year_utc()

        completed_statuses = {"complete unrated", "complete rated"}
        df_completed = df_jobs[df_jobs["work_status"].astype(str).str.lower().isin(completed_statuses)].copy()
        df_completed = df_completed[df_completed["completed_at"] >= start_of_year_utc].copy()
        completed_jobs = int(len(df_completed))

        # -----------------------------
        # Revenue population (BILLED, not PAID)
        # invoices.status IN ('open','pending_payment') and invoice_date in current year
        # -----------------------------
        billed_statuses = {"open", "pending_payment"}
        df_rev = df_invoices[df_invoices["status"].astype(str).str.lower().isin(billed_statuses)].copy()

        if not df_rev.empty:
            df_rev["invoice_date_dt"] = _to_dt_utc(df_rev["invoice_date"]) if "invoice_date" in df_rev.columns else pd.NaT
            df_rev = df_rev[df_rev["invoice_date_dt"] >= start_of_year_utc].copy()
        else:
            df_rev["invoice_date_dt"] = pd.NaT

        # Join tags -> revenue rows for category breakdown
        if not df_rev.empty and not df_jobs.empty:
            df_rev = df_rev.merge(df_jobs[["job_id", "tags_norm"]], how="left", on="job_id")

        df_rev["tags_norm"] = df_rev.get("tags_norm", "").fillna("")
        df_rev["category"] = df_rev["tags_norm"].apply(_map_category_from_tags)
        df_rev["is_install"] = df_rev["category"].astype(str).str.contains("Install", case=False, na=False)
        df_rev["amount_dollars"] = pd.to_numeric(df_rev["amount"], errors="coerce").fillna(0.0).astype(float)

        # -----------------------------
        # Employee dim (needed by lead turnover module)
        # -----------------------------
        df_emp_dim = df_employees.copy()
        if df_emp_dim.empty and not df_job_emps.empty:
            df_emp_dim = df_job_emps[
                ["employee_id", "first_name", "last_name", "role", "avatar_url", "color_hex"]
            ].drop_duplicates().copy()

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

        # -----------------------------
        # Delegate KPI computation to modules
        # -----------------------------
        ctx = {
            "start_of_year_utc": start_of_year_utc,

            "df_jobs": df_jobs,
            "df_completed": df_completed,
            "completed_jobs": completed_jobs,

            "df_invoices": df_invoices,
            "df_rev": df_rev,

            "df_job_emps": df_job_emps,
            "df_employees": df_employees,
            "df_emp_dim": df_emp_dim,

            "df_job_appts": df_job_appts,
            "df_invoice_items": df_invoice_items,

            "df_estimates": df_estimates,
            "df_estimate_options": df_estimate_options,
            "df_estimate_emps": df_estimate_emps,
        }

        out = {}
        out.update(kpi_quality.compute(ctx))
        out.update(kpi_revenue.compute(ctx))
        out.update(kpi_employees.compute(ctx))
        out.update(kpi_estimates.compute(ctx))
        out.update(kpi_lead_turnover.compute(ctx))

        # Refresh status
        refresh_status = get_refresh_status()
        out["last_refresh"] = refresh_status.get("last_refresh_display", "")
        out.update(refresh_status)

        return out

    finally:
        conn.close()


def get_refresh_status():
    conn = duckdb.connect(DB_FILE, read_only=True)
    try:
        # metadata table might not exist in read-only contexts, so be defensive
        try:
            row = conn.execute("""
                SELECT value FROM metadata WHERE key = 'last_refresh'
            """).fetchone()
        except Exception:
            row = None

        if not row or not row[0]:
            return {
                "can_refresh": True,
                "next_refresh": "Now",
                "last_refresh_display": "No data yet",
            }

        # 1-hour constraint (same as your original logic)
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
