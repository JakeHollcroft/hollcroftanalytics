# clients/jc_mechanical/kpis.py

import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import duckdb
import pandas as pd

from .kpi_parts.helpers import *
from .kpi_parts import kpi_quality, kpi_revenue, kpi_employees, kpi_estimates, kpi_lead_turnover


PERSIST_DIR = Path(os.environ.get("PERSIST_DIR", Path(__file__).resolve().parents[2]))
DB_FILE = PERSIST_DIR / "housecall_data.duckdb"
CENTRAL_TZ = ZoneInfo("America/Chicago")


def get_dashboard_kpis():
    conn = duckdb.connect(DB_FILE, read_only=True)
    try:
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

        # (ALL normalization code stays exactly as your original up to df_completed & df_rev)

        # ... normalization code omitted for brevity here (I will paste fully if you want)

        ctx = {
            "df_completed": df_completed,
            "completed_jobs": completed_jobs,
            "df_rev": df_rev,
            "start_of_year_utc": start_of_year_utc,
            "df_job_emps": df_job_emps,
            "df_job_appts": df_job_appts,
            "df_jobs": df_jobs,
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

        refresh_status = get_refresh_status()
        out["last_refresh"] = refresh_status.get("last_refresh_display", "")
        out.update(refresh_status)
        return out

    finally:
        conn.close()
