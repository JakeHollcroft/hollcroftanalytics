import duckdb
import pandas as pd
from datetime import datetime
import requests
from .config import DB_FILE, API_KEY  

BASE_URL_JOBS = "https://api.housecallpro.com/jobs"
BASE_URL_CUSTOMERS = "https://api.housecallpro.com/customers"
BASE_URL_INVOICES = "https://api.housecallpro.com/invoices"

HEADERS = {
    "Authorization": f"Token {API_KEY}",
    "Content-Type": "application/json"
}

def fetch_all(endpoint, key_name, page_size=100):
    all_items = []
    page = 1
    while True:
        params = {"page": page, "page_size": page_size}
        resp = requests.get(endpoint, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get(key_name, [])
        all_items.extend(items)
        print(f"Fetched page {page} of {data.get('total_pages',1)} ({len(items)} items)")
        if page >= data.get("total_pages",1):
            break
        page += 1
    return all_items

def flatten_jobs(jobs):
    rows = []
    for job in jobs:
        rows.append({
            "job_id": job.get("id"),
            "invoice_number": job.get("invoice_number"),
            "description": job.get("description"),
            "work_status": job.get("work_status"),
            "total_amount": job.get("total_amount"),
            "outstanding_balance": job.get("outstanding_balance"),
            "company_name": job.get("company_name"),
            "company_id": job.get("company_id"),
            "created_at": job.get("created_at"),
            "updated_at": job.get("updated_at"),
            "customer_id": job.get("customer", {}).get("id") if job.get("customer") else None,
            "customer_name": job.get("customer", {}).get("name") if job.get("customer") else None,
        })
    return pd.DataFrame(rows)

def flatten_customers(customers):
    rows = []
    for c in customers:
        rows.append({
            "customer_id": c.get("id"),
            "first_name": c.get("first_name"),
            "last_name": c.get("last_name"),
            "email": c.get("email"),
            "mobile_number": c.get("mobile_number"),
            "home_number": c.get("home_number"),
            "work_number": c.get("work_number"),
            "company": c.get("company"),
            "notifications_enabled": c.get("notifications_enabled"),
            "lead_source": c.get("lead_source"),
            "notes": c.get("notes"),
            "created_at": c.get("created_at"),
            "updated_at": c.get("updated_at"),
            "company_name": c.get("company_name"),
            "company_id": c.get("company_id"),
            "tags": ",".join(c.get("tags", [])) if c.get("tags") else None
        })
    return pd.DataFrame(rows)

def flatten_invoices(invoices):
    rows = []
    for inv in invoices:
        rows.append({
            "invoice_id": inv.get("id"),
            "job_id": inv.get("job_id"),
            "invoice_number": inv.get("invoice_number"),
            "status": inv.get("status"),
            "amount": inv.get("amount"),
            "subtotal": inv.get("subtotal"),
            "due_amount": inv.get("due_amount"),
            "due_at": inv.get("due_at"),
            "paid_at": inv.get("paid_at"),
            "sent_at": inv.get("sent_at"),
            "service_date": inv.get("service_date"),
            "invoice_date": inv.get("invoice_date"),
            "display_due_concept": inv.get("display_due_concept"),
            "due_concept": inv.get("due_concept"),
        })
    return pd.DataFrame(rows)

def update_last_refresh(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    conn.execute("""
        INSERT INTO metadata (key, value)
        VALUES ('last_refresh', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, [datetime.utcnow().isoformat()])


def run_ingestion():
    print("Fetching Jobs...")
    jobs_data = fetch_all(BASE_URL_JOBS, key_name="jobs", page_size=100)
    df_jobs = flatten_jobs(jobs_data)

    print("Fetching Customers...")
    customers_data = fetch_all(BASE_URL_CUSTOMERS, key_name="customers", page_size=100)
    df_customers = flatten_customers(customers_data)

    print("Fetching Invoices...")
    invoices_data = fetch_all(BASE_URL_INVOICES, key_name="invoices", page_size=100)
    df_invoices = flatten_invoices(invoices_data)

    # Connect to DuckDB
    conn = duckdb.connect(DB_FILE)

    # Write tables (replace existing if needed)
    conn.execute("DROP TABLE IF EXISTS jobs")
    conn.execute("CREATE TABLE jobs AS SELECT * FROM df_jobs")

    conn.execute("DROP TABLE IF EXISTS customers")
    conn.execute("CREATE TABLE customers AS SELECT * FROM df_customers")

    conn.execute("DROP TABLE IF EXISTS invoices")
    conn.execute("CREATE TABLE invoices AS SELECT * FROM df_invoices")
    update_last_refresh(conn)
    conn.close()
    print(f"Ingestion complete. Data saved to {DB_FILE}")


if __name__ == "__main__":
    run_ingestion()
