# clients/jc_mechanical/kpis.py

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from .kpi_parts.helpers import (
    CENTRAL_TZ,
    safe_df,
    ensure_columns,
    to_dt_utc,
    convert_cents_to_dollars,
    normalize_tags,
    start_of_year_utc,
)

from .kpi_parts import kpi_quality, kpi_revenue, kpi_employees, kpi_estimates, kpi_lead_turnover


# ---- Persistence / DuckDB location ----
def _resolve_persist_dir() -> Path:
    env = os.environ.get("PERSIST_DIR")
    if env:
        return Path(env)
    if Path("/var/data").exists():
        return Path("/var/data")
    return Path(__file__).resolve().parents[2]


PERSIST_DIR = _resolve_persist_dir()
DB_FILE = PERSIST_DIR / "housecall_data.duckdb"


# -----------------------------
# Tables / schema (read-side safety)
# -----------------------------
def ensure_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Defensive only. Ingestion should be the source of truth.
    This prevents read errors if a table is missing in early stages.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key VARCHAR PRIMARY KEY,
            value VARCHAR
        );
        """
    )


# -----------------------------
# KPI builder (orchestrator)
# -----------------------------
def get_dashboard_kpis():
    conn = duckdb.connect(DB_FILE, read_only=True)
    try:
        ensure_tables(conn)

        # Load all relevant tables once
        df_jobs = safe_df(conn, "SELECT * FROM jobs")
        df_job_emps = safe_df(conn, "SELECT * FROM job_employees")
        df_customers = safe_df(conn, "SELECT * FROM customers")
        df_invoices = safe_df(conn, "SELECT * FROM invoices")

        df_employees = safe_df(conn, "SELECT * FROM employees")
        df_job_appts = safe_df(conn, "SELECT * FROM job_appointments")
        df_invoice_items = safe_df(conn, "SELECT * FROM invoice_items")

        df_estimates = safe_df(conn, "SELECT * FROM estimates")
        df_estimate_options = safe_df(conn, "SELECT * FROM estimate_options")
        df_estimate_emps = safe_df(conn, "SELECT * FROM estimate_employees")

        # Ensure expected columns exist (matches your current defensive style)
        df_jobs = ensure_columns(
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

        df_job_emps = ensure_columns(
            df_job_emps,
            {
                "job_id": "object",
                "employee_id": "object",
                "first_name": "object",
                "last_name": "object",
                "role": "object",
                "avatar_url": "object",
                "color_hex": "object",
            },
        )

        df_customers = ensure_columns(
            df_customers,
            {"customer_id": "object", "first_name": "object", "last_name": "object"},
        )

        df_invoices = ensure_columns(
            df_invoices,
            {
                "invoice_id": "object",
                "job_id": "object",
                "status": "object",
                "amount": "float64",
                "subtotal": "float64",
                "due_amount": "float64",
                "paid_at": "object",
                "service_date": "object",
                "invoice_date": "object",
            },
        )

        df_employees = ensure_columns(
            df_employees,
            {
                "employee_id": "object",
                "first_name": "object",
                "last_name": "object",
                "role": "object",
                "avatar_url": "object",
                "color_hex": "object",
            },
        )

        df_job_appts = ensure_columns(
            df_job_appts,
            {
                "job_id": "object",
                "appointment_id": "object",
                "employee_id": "object",
                "start_at": "object",
                "end_at": "object",
                "duration_minutes": "float64",
            },
        )

        df_invoice_items = ensure_columns(
            df_invoice_items,
            {
                "invoice_id": "object",
                "job_id": "object",
                "name": "object",
                "type": "object",
                "quantity": "float64",
                "unit_price": "float64",
                "unit_cost": "float64",
                "total": "float64",
            },
        )

        df_estimates = ensure_columns(
            df_estimates,
            {
                "estimate_id": "object",
                "created_at": "object",
                "status": "object",
                "lead_source": "object",
            },
        )

        df_estimate_options = ensure_columns(
            df_estimate_options,
            {
                "estimate_id": "object",
                "approval_status": "object",
                "status": "object",
                "total_amount": "float64",
            },
        )

        df_estimate_emps = ensure_columns(
            df_estimate_emps,
            {
                "estimate_id": "object",
                "employee_id": "object",
                "first_name": "object",
                "last_name": "object",
            },
        )

        # Convert cents to dollars where applicable (you already treat invoice amounts as dollars in current logic,
        # but keeping your exact behavior: only convert if your ingestion stored cents)
        # If your ingestion already converted to dollars, leave this as-is or remove.
        # If you KNOW invoices.amount is cents, uncomment:
        # df_invoices = convert_cents_to_dollars(df_invoices, ["amount", "subtotal", "due_amount"])

        # Normalize / parse key fields once
        df_jobs["created_at"] = to_dt_utc(df_jobs["created_at"])
        df_jobs["updated_at"] = to_dt_utc(df_jobs["updated_at"])
        df_jobs["completed_at"] = to_dt_utc(df_jobs["completed_at"])

        df_invoices["paid_at"] = to_dt_utc(df_invoices["paid_at"])
        df_invoices["service_date"] = to_dt_utc(df_invoices["service_date"])
        df_invoices["invoice_date"] = to_dt_utc(df_invoices["invoice_date"])

        # Customer name join
        if not df_customers.empty and not df_jobs.empty:
            df_customers["customer_name"] = (
                df_customers["first_name"].fillna("").astype(str).str.strip()
                + " "
                + df_customers["last_name"].fillna("").astype(str).str.strip()
            ).str.strip()

            df_jobs = df_jobs.merge(
                df_customers[["customer_id", "customer_name"]],
                how="left",
                on="customer_id",
            )
        else:
            df_jobs["customer_name"] = df_jobs.get("customer_name", "")

        # Tags normalization
        df_jobs["tags_norm"] = df_jobs.get("tags", "").apply(normalize_tags)

        # Ensure num_appointments is int
        df_jobs["num_appointments"] = pd.to_numeric(df_jobs.get("num_appointments", 0), errors="coerce").fillna(0).astype(int)

        # YTD window
        start_utc = start_of_year_utc()

        # Completed jobs YTD
        completed_statuses = {"complete unrated", "complete rated"}
        df_completed = df_jobs[df_jobs["work_status"].astype(str).str.lower().isin(completed_statuses)].copy()
        df_completed = df_completed[df_completed["completed_at"] >= start_utc].copy()

        completed_jobs = int(len(df_completed))

        # Build shared context for KPI modules
        ctx = {
            "start_of_year_utc": start_utc,
            "completed_jobs": completed_jobs,
            "df_jobs": df_jobs,
            "df_completed": df_completed,
            "df_invoices": df_invoices,
            "df_employees": df_employees,
            "df_job_emps": df_job_emps,
            "df_job_appts": df_job_appts,
            "df_invoice_items": df_invoice_items,
            "df_estimates": df_estimates,
            "df_estimate_options": df_estimate_options,
            "df_estimate_emps": df_estimate_emps,
        }

        # Compute sections
        out = {}
        out.update(kpi_quality.compute(ctx))
        out.update(kpi_revenue.compute(ctx))
        out.update(kpi_employees.compute(ctx))
        out.update(kpi_estimates.compute(ctx))
        out.update(kpi_lead_turnover.compute(ctx))

        # Refresh status + last_refresh formatting
        refresh_status = get_refresh_status()

        # Maintain your original return contract
        out.update(
            {
                "last_refresh": refresh_status.get("last_refresh_display", ""),
                **refresh_status,
            }
        )
        return out

    finally:
        conn.close()


def get_refresh_status():
    conn = duckdb.connect(DB_FILE, read_only=True)
    try:
        row = conn.execute(
            """
            SELECT value FROM metadata WHERE key = 'last_refresh'
            """
        ).fetchone()

        last_refresh_iso = row[0] if row and row[0] else None
        last_refresh_display = ""

        if last_refresh_iso:
            try:
                dt_utc = datetime.fromisoformat(last_refresh_iso).replace(tzinfo=ZoneInfo("UTC"))
                last_refresh_display = dt_utc.astimezone(CENTRAL_TZ).strftime("%b %d, %Y %I:%M %p %Z")
            except Exception:
                last_refresh_display = str(last_refresh_iso)

        # Simple throttle: allow refresh every 15 minutes (matches your existing pattern)
        now_utc = datetime.now(timezone.utc)
        min_interval = timedelta(minutes=15)

        next_allowed = now_utc
        if last_refresh_iso:
            try:
                dt_utc = datetime.fromisoformat(last_refresh_iso).replace(tzinfo=ZoneInfo("UTC"))
                next_allowed = dt_utc + min_interval
            except Exception:
                next_allowed = now_utc

        return {
            "can_refresh": now_utc >= next_allowed,
            "next_refresh": next_allowed.astimezone(CENTRAL_TZ).strftime("%b %d, %Y %I:%M %p %Z"),
            "last_refresh_display": last_refresh_display,
        }

    finally:
        conn.close()
