import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

PERSIST_DIR = Path(os.environ.get("PERSIST_DIR", Path(__file__).resolve().parents[2]))
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

DB_FILE = PERSIST_DIR / "housecall_data.duckdb"

def get_dashboard_kpis():
    conn = duckdb.connect(DB_FILE)

    df_jobs = conn.execute("SELECT * FROM jobs").df()
    df_customers = conn.execute("SELECT * FROM customers").df()
    df_invoices = conn.execute("SELECT * FROM invoices").df()

    # -----------------------------------
    # Last refresh
    # -----------------------------------
    try:
        last_refresh_row = conn.execute("""
            SELECT value FROM metadata WHERE key = 'last_refresh'
        """).fetchone()
        last_refresh = last_refresh_row[0] if last_refresh_row else None
    except:
        last_refresh = None

    if last_refresh:
        utc_dt = datetime.fromisoformat(last_refresh).replace(tzinfo=ZoneInfo("UTC"))
        central_dt = utc_dt.astimezone(ZoneInfo("America/Chicago"))
        last_refresh_display = central_dt.strftime("%b %d, %Y %I:%M %p %Z")
    else:
        last_refresh_display = "Never"

    # -----------------------------------
    # Data cleanup
    # -----------------------------------
    df_jobs["total_amount"] = (
        pd.to_numeric(df_jobs.get("total_amount", 0), errors="coerce")
        .fillna(0) / 100
    )

    df_jobs["outstanding_balance"] = (
        pd.to_numeric(df_jobs.get("outstanding_balance", 0), errors="coerce")
        .fillna(0) / 100
    )

    df_jobs["num_appointments"] = (
        pd.to_numeric(df_jobs.get("num_appointments", 0), errors="coerce")
        .fillna(0)
        .astype(int)
    )

    df_jobs["completed_at"] = pd.to_datetime(df_jobs.get("completed_at"), errors="coerce", utc=True)

    df_invoices["amount"] = (
        pd.to_numeric(df_invoices.get("amount", 0), errors="coerce")
        .fillna(0) / 100
    )

    df_invoices["due_amount"] = (
        pd.to_numeric(df_invoices.get("due_amount", 0), errors="coerce")
        .fillna(0) / 100
    )

    # -----------------------------------
    # Joins
    # -----------------------------------
    # invoices -> customer_id via jobs
    df_invoices = df_invoices.merge(
        df_jobs[["job_id", "customer_id"]],
        how="left",
        on="job_id"
    )

    # jobs -> customer name
    df_jobs = df_jobs.merge(
        df_customers[["customer_id", "first_name", "last_name"]],
        how="left",
        on="customer_id"
    )
    df_jobs["customer_name"] = (
        df_jobs["first_name"].fillna("Unknown") + " " +
        df_jobs["last_name"].fillna("")
    ).str.strip()

    # -----------------------------------
    # Job subsets
    # -----------------------------------
    completed_statuses = ["complete unrated", "complete rated"]
    df_completed = df_jobs[df_jobs["work_status"].isin(completed_statuses)].copy()# -----------------------------------

    central = ZoneInfo("America/Chicago")
    now_central = datetime.now(central)
    start_of_year_central = datetime(now_central.year, 1, 1, tzinfo=central)

    start_of_year_utc = start_of_year_central.astimezone(ZoneInfo("UTC"))

    df_completed = df_completed[df_completed["completed_at"] >= start_of_year_utc].copy()

   # -----------------------------------
    # First-Time Completion KPI
    # Definition used here:
    # Completed jobs that had exactly 1 appointment.
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
    # Repeat-visit table (action list)
    # -----------------------------------
    # Show the most recent “needed return trip” completed jobs
    # (num_appointments >= 2)
    repeat_jobs_df = df_completed[df_completed["num_appointments"] >= 2].copy()
    repeat_jobs_df = repeat_jobs_df.sort_values(by="completed_at", ascending=False)

    # Convert completed_at to Central display string for table
    central = ZoneInfo("America/Chicago")
    def fmt_dt(dt):
        if pd.isna(dt):
            return ""
        try:
            return dt.tz_convert(central).strftime("%b %d, %Y %I:%M %p")
        except Exception:
            return str(dt)

    repeat_jobs_df["completed_at_central"] = repeat_jobs_df["completed_at"].apply(fmt_dt)

    # Keep table small + useful
    repeat_jobs_table = repeat_jobs_df[[
        "job_id",
        "customer_name",
        "work_status",
        "num_appointments",
        "completed_at_central",
        "description",
    ]].head(25)

    # -----------------------------------
    # Refresh status
    # -----------------------------------
    refresh_status = get_refresh_status()

    conn.close()

    return {
        # KPI outputs used by template
        "first_time_completion_pct": first_time_completion_pct,
        "first_time_completion_target": first_time_completion_target,
        "first_time_completed": first_time_completed,
        "repeat_visit_completed": repeat_visit_completed,
        "completed_jobs": completed_jobs,
        "repeat_visit_pct": repeat_visit_pct,

        "repeat_jobs": repeat_jobs_table.to_dict(orient="records"),

        # existing refresh outputs
        "last_refresh": last_refresh_display,
        **refresh_status
    }


def get_refresh_status():
    conn = duckdb.connect(DB_FILE)

    row = conn.execute("""
        SELECT value FROM metadata WHERE key = 'last_refresh'
    """).fetchone()

    conn.close()

    if not row:
        return {
            "can_refresh": True,
            "next_refresh": "Now"
        }

    # 1-hour constraint is here:
    last_refresh_utc = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
    next_allowed = last_refresh_utc + timedelta(hours=1)

    now_utc = datetime.now(timezone.utc)
    central = ZoneInfo("America/Chicago")

    return {
        "can_refresh": now_utc >= next_allowed,
        "next_refresh": next_allowed.astimezone(central).strftime("%b %d, %Y %I:%M %p %Z")
    }
