import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DB_FILE = BASE_DIR / "data" / "housecall_data.duckdb"

def get_dashboard_kpis():
    conn = duckdb.connect(DB_FILE)

    df_jobs = conn.execute("SELECT * FROM jobs").df()
    df_customers = conn.execute("SELECT * FROM customers").df()
    df_invoices = conn.execute("SELECT * FROM invoices").df()

    try:
        last_refresh = conn.execute("""
            SELECT value FROM metadata WHERE key = 'last_refresh'
        """).fetchone()
        last_refresh = last_refresh[0] if last_refresh else None
    except:
        last_refresh = None

    if last_refresh:
        utc_dt = datetime.fromisoformat(last_refresh).replace(tzinfo=ZoneInfo("UTC"))
        central_dt = utc_dt.astimezone(ZoneInfo("America/Chicago"))
        last_refresh = central_dt.strftime("%b %d, %Y %I:%M %p %Z")
    else:
        last_refresh = "Never"



    # Ensure numeric columns exist
    df_jobs["total_amount"] = pd.to_numeric(df_jobs.get("total_amount", 0), errors="coerce").fillna(0) / 100
    df_jobs["outstanding_balance"] = pd.to_numeric(df_jobs.get("outstanding_balance", 0), errors="coerce").fillna(0) / 100

    df_invoices["amount"] = pd.to_numeric(df_invoices.get("amount", 0), errors="coerce").fillna(0) / 100
    df_invoices["due_amount"] = pd.to_numeric(df_invoices.get("due_amount", 0), errors="coerce").fillna(0) / 100

    # Merge invoices with jobs to get customer_id
    df_invoices = df_invoices.merge(
        df_jobs[['job_id', 'customer_id']],
        how='left',
        left_on='job_id',
        right_on='job_id'
    )

    # Merge customer names
    df_jobs = df_jobs.merge(
        df_customers[['customer_id', 'first_name', 'last_name']],
        how='left', left_on='customer_id', right_on='customer_id'
    )
    df_jobs["customer_name"] = df_jobs["first_name"].fillna('Unknown') + " " + df_jobs["last_name"].fillna('')

    df_invoices = df_invoices.merge(
        df_customers[['customer_id', 'first_name', 'last_name']],
        how='left', left_on='customer_id', right_on='customer_id'
    )
    df_invoices["customer_name"] = df_invoices["first_name"].fillna('Unknown') + " " + df_invoices["last_name"].fillna('')

    df_scheduled = df_jobs[df_jobs["work_status"]=="scheduled"].copy()

    # KPIs
    total_jobs = len(df_jobs)
    completed_jobs = len(df_jobs[df_jobs["work_status"].isin(["complete unrated", "complete rated"])])
    total_customers = len(df_customers)
    scheduled_jobs_count = len(df_scheduled)

    total_invoices = len(df_invoices)
    total_invoice_amount = df_invoices["amount"].sum()
    total_paid_amount = (df_invoices["amount"] - df_invoices["due_amount"]).sum()
    total_outstanding_invoices = df_invoices["due_amount"].sum()

    df_invoices["due_at"] = pd.to_datetime(df_invoices.get("due_at"))
    overdue_invoices_count = len(df_invoices[df_invoices["due_at"] < pd.Timestamp.now(tz='UTC')])

    top_customers = df_invoices.groupby("customer_name").agg(
        num_invoices=('invoice_id','count'),
        total_invoice=('amount','sum'),
        total_outstanding=('due_amount','sum')
    ).reset_index().sort_values(by="total_outstanding", ascending=False).head(10)

    raw_jobs = conn.execute("SELECT * FROM jobs").df()
    raw_customers = conn.execute("SELECT * FROM customers").df()
    raw_invoices = conn.execute("SELECT * FROM invoices").df()

    refresh_status = get_refresh_status()

    return {
        "total_jobs": total_jobs,
        "completed_jobs": completed_jobs,
        "scheduled_jobs_count": scheduled_jobs_count,
        "total_customers": total_customers,
        "total_invoices": total_invoices,
        "total_invoice_amount": total_invoice_amount,
        "total_paid_amount": total_paid_amount,
        "total_outstanding": total_outstanding_invoices,
        "overdue_count": overdue_invoices_count,
        "top_customers": top_customers.to_dict(orient="records"),
        "last_refresh": last_refresh,
        "raw_jobs": raw_jobs.to_dict(orient="records"),
        "raw_customers": raw_customers.to_dict(orient="records"),
        "raw_invoices": raw_invoices.to_dict(orient="records"),
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
            "last_refresh": "Never",
            "next_refresh": "Now"
        }

    last_refresh_utc = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
    next_allowed = last_refresh_utc + timedelta(hours=1)

    now_utc = datetime.now(timezone.utc)

    central = ZoneInfo("America/Chicago")

    return {
        "can_refresh": now_utc >= next_allowed,
        "last_refresh": last_refresh_utc.astimezone(central).strftime("%b %d, %Y %I:%M %p %Z"),
        "next_refresh": next_allowed.astimezone(central).strftime("%b %d, %Y %I:%M %p %Z")
    }