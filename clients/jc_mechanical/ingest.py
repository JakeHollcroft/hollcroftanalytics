# clients/jc_mechanical/ingest.py

import os
import time
import random
import duckdb
import pandas as pd
from datetime import datetime
import requests
from requests.exceptions import (
    HTTPError,
    ConnectionError,
    Timeout,
    ChunkedEncodingError,
    RequestException,
)
import uuid
from .config import DB_FILE, API_KEY

LOCK_KEY = "ingest_lock"
LOCK_TTL_SECONDS = 60 * 60 * 6  

RUN_ID = uuid.uuid4().hex[:8]

BASE_URL_JOBS = "https://api.housecallpro.com/jobs"
BASE_URL_JOB_DETAIL = "https://api.housecallpro.com/jobs/{id}"
BASE_URL_CUSTOMERS = "https://api.housecallpro.com/customers"
BASE_URL_INVOICES = "https://api.housecallpro.com/invoices"

HEADERS = {
    "Authorization": f"Token {API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "HollcroftAnalytics/1.0 (+https://hollcroftanalytics.com)",
}

COMPLETED_STATUSES = {"complete unrated", "complete rated"}

# ---- Retry / throttling controls (tune later) ----
DETAIL_MAX_RETRIES = 6
DETAIL_BASE_BACKOFF_SEC = 0.8
DETAIL_JITTER_SEC = 0.35
DETAIL_MIN_DELAY_EACH_CALL_SEC = 0.12
DETAIL_BATCH_PAUSE_EVERY = 25
DETAIL_BATCH_PAUSE_SEC = 0.8

# Use connect timeout + read timeout
REQUEST_TIMEOUT = (10, 45)

# Reuse a session for connection pooling (reduces resets)
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Simple in-process guard to prevent overlapping runs
_INGEST_RUNNING = False

def _ensure_metadata_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

def _get_metadata(conn, key: str):
    _ensure_metadata_table(conn)
    row = conn.execute("SELECT value FROM metadata WHERE key = ?", [key]).fetchone()
    return row[0] if row else None

def _set_metadata(conn, key: str, value: str):
    _ensure_metadata_table(conn)
    conn.execute("""
        INSERT INTO metadata (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, [key, value])

def _try_acquire_ingest_lock(conn) -> bool:
    """
    Cross-process lock using metadata table.
    - If lock missing => acquire
    - If lock exists but older than TTL => steal
    """
    now = time.time()
    raw = _get_metadata(conn, LOCK_KEY)

    if raw:
        try:
            lock_ts = float(raw)
        except Exception:
            lock_ts = 0.0

        age = now - lock_ts
        if age < LOCK_TTL_SECONDS:
            print(f"[{_ts()}] Lock exists (age {age:.0f}s). Another ingest is running. Skipping.")
            return False

        print(f"[{_ts()}] Lock is stale (age {age:.0f}s). Stealing lock...")

    _set_metadata(conn, LOCK_KEY, str(now))
    return True

def _release_ingest_lock(conn):
    _ensure_metadata_table(conn)
    conn.execute("DELETE FROM metadata WHERE key = ?", [LOCK_KEY])


def _ts():
    # tiny helper for consistent logs
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def _sleep_with_log(seconds: float, reason: str):
    """
    Centralized backoff sleep with a couple print statements so you can see
    when you're backing off for rate limits / server errors / network issues.
    """
    seconds = max(0.0, float(seconds))
    print(f"[{_ts()}] BACKOFF: {reason}")
    print(f"[{_ts()}] Sleeping {seconds:.2f}s...")
    time.sleep(seconds)


def fetch_all(endpoint, key_name, page_size=100):
    all_items = []
    page = 1
    while True:
        params = {"page": page, "page_size": page_size}
        resp = SESSION.get(endpoint, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        data = resp.json()
        items = data.get(key_name, [])
        all_items.extend(items)

        print(f"Fetched page {page} of {data.get('total_pages', 1)} ({len(items)} items)")

        if page >= data.get("total_pages", 1):
            break
        page += 1

    return all_items


def fetch_job_details(job_id):
    """
    Robust job detail fetch w/ retry/backoff for:
      - Connection reset / aborted connections
      - Timeouts / chunked encoding errors
      - 429 rate limits
      - transient 5xx
    Also handles expand parameter variations.
    """
    url = BASE_URL_JOB_DETAIL.format(id=job_id)

    expand_params_primary = [("expand[]", "appointments")]
    expand_params_fallback = {"expand": "appointments"}

    last_err = None

    for attempt in range(1, DETAIL_MAX_RETRIES + 1):
        # Always throttle a bit between calls; log it occasionally for visibility
        if DETAIL_MIN_DELAY_EACH_CALL_SEC > 0:
            if attempt == 1:
                print(
                    f"[{_ts()}] Throttle: sleeping {DETAIL_MIN_DELAY_EACH_CALL_SEC:.2f}s "
                    f"before detail call for job {job_id}"
                )
            time.sleep(DETAIL_MIN_DELAY_EACH_CALL_SEC)

        try:
            resp = SESSION.get(url, params=expand_params_primary, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 400:
                # Some endpoints accept expand=... instead of expand[]=...
                resp = SESSION.get(url, params=expand_params_fallback, timeout=REQUEST_TIMEOUT)

            # ---- 429 rate limit handling ----
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait = float(retry_after)
                    reason = f"429 rate limit for job {job_id} (Retry-After={retry_after}) (attempt {attempt}/{DETAIL_MAX_RETRIES})"
                else:
                    wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(
                        0, DETAIL_JITTER_SEC
                    )
                    reason = f"429 rate limit for job {job_id} (no Retry-After) (attempt {attempt}/{DETAIL_MAX_RETRIES})"

                _sleep_with_log(wait, reason)
                continue

            # ---- transient server errors ----
            if resp.status_code in (500, 502, 503, 504):
                wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(
                    0, DETAIL_JITTER_SEC
                )
                reason = (
                    f"Server error {resp.status_code} for job {job_id} "
                    f"(attempt {attempt}/{DETAIL_MAX_RETRIES})"
                )
                _sleep_with_log(wait, reason)
                continue

            resp.raise_for_status()
            return resp.json()

        except (ConnectionError, Timeout, ChunkedEncodingError) as e:
            last_err = e
            wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(
                0, DETAIL_JITTER_SEC
            )
            reason = (
                f"Network/timeout error for job {job_id}: {type(e).__name__}: {e} "
                f"(attempt {attempt}/{DETAIL_MAX_RETRIES})"
            )
            _sleep_with_log(wait, reason)
            continue

        except HTTPError as e:
            last_err = e
            status = getattr(e.response, "status_code", None)

            # Do not retry on typical terminal errors
            if status in (400, 401, 403, 404):
                raise

            wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(
                0, DETAIL_JITTER_SEC
            )
            reason = (
                f"HTTP error for job {job_id}: status={status} {e} "
                f"(attempt {attempt}/{DETAIL_MAX_RETRIES})"
            )
            _sleep_with_log(wait, reason)
            continue

        except RequestException as e:
            last_err = e
            wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(
                0, DETAIL_JITTER_SEC
            )
            reason = (
                f"RequestException for job {job_id}: {type(e).__name__}: {e} "
                f"(attempt {attempt}/{DETAIL_MAX_RETRIES})"
            )
            _sleep_with_log(wait, reason)
            continue

    raise last_err if last_err else RuntimeError(f"Failed to fetch job details for {job_id}")


def appointment_count_from_job(job_obj):
    schedule = job_obj.get("schedule") or {}
    appts = schedule.get("appointments")
    if isinstance(appts, list):
        return len(appts)

    appts2 = job_obj.get("appointments")
    if isinstance(appts2, list):
        return len(appts2)

    return 0


def flatten_jobs(jobs):
    rows = []
    for job in jobs:
        rows.append(
            {
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
                "completed_at": (job.get("work_timestamps") or {}).get("completed_at"),
                "customer_id": (job.get("customer") or {}).get("id") if job.get("customer") else None,
                # filled after detail fetch
                "num_appointments": 0,
            }
        )
    return pd.DataFrame(rows)


def flatten_customers(customers):
    rows = []
    for c in customers:
        rows.append(
            {
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
                "tags": ",".join(c.get("tags", [])) if c.get("tags") else None,
            }
        )
    return pd.DataFrame(rows)


def flatten_invoices(invoices):
    rows = []
    for inv in invoices:
        rows.append(
            {
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
            }
        )
    return pd.DataFrame(rows)


def update_last_refresh(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO metadata (key, value)
        VALUES ('last_refresh', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        [datetime.utcnow().isoformat()],
    )


def run_ingestion():
    """
    One ingestion run. Protected by a DB-backed lock so only one process runs it.
    """
    conn = duckdb.connect(DB_FILE)

    # Acquire cross-process lock
    if not _try_acquire_ingest_lock(conn):
        conn.close()
        return

    try:
        print(f"[{_ts()}] Ingest lock acquired. Starting ingestion...")
        print(f"[{_ts()}] RUN {RUN_ID} lock acquired")
        print("Fetching Jobs...")
        jobs_data = fetch_all(BASE_URL_JOBS, key_name="jobs", page_size=100)
        df_jobs = flatten_jobs(jobs_data)

        print("Fetching appointment counts for completed jobs (job detail endpoint)...")
        appt_counts = {}
        completed_ids = (
            df_jobs[df_jobs["work_status"].isin(COMPLETED_STATUSES)]["job_id"]
            .dropna()
            .tolist()
        )

        for i, job_id in enumerate(completed_ids, start=1):
            try:
                detail = fetch_job_details(job_id)
                appt_counts[job_id] = appointment_count_from_job(detail)
                print(f"[{_ts()}] RUN {RUN_ID} Fetched details for job {job_id} ({i}/{len(completed_ids)})")
            except HTTPError as e:
                print(f"Warning: HTTP error for {job_id}: {e}")
                appt_counts[job_id] = 0
            except Exception as e:
                print(f"Warning: unexpected error for {job_id}: {e}")
                appt_counts[job_id] = 0

            if i % DETAIL_BATCH_PAUSE_EVERY == 0:
                print(f"[{_ts()}] Batch throttle: processed {i}; sleeping {DETAIL_BATCH_PAUSE_SEC:.2f}s")
                time.sleep(DETAIL_BATCH_PAUSE_SEC)

        df_jobs["num_appointments"] = df_jobs["job_id"].map(appt_counts).fillna(0).astype(int)

        print("Fetching Customers...")
        customers_data = fetch_all(BASE_URL_CUSTOMERS, key_name="customers", page_size=100)
        df_customers = flatten_customers(customers_data)

        print("Fetching Invoices...")
        invoices_data = fetch_all(BASE_URL_INVOICES, key_name="invoices", page_size=100)
        df_invoices = flatten_invoices(invoices_data)

        # IMPORTANT: don't drop tables mid-run if the web app is reading.
        # Safer: write staging then swap.
        conn.execute("CREATE OR REPLACE TABLE jobs_stage AS SELECT * FROM df_jobs")
        conn.execute("CREATE OR REPLACE TABLE customers_stage AS SELECT * FROM df_customers")
        conn.execute("CREATE OR REPLACE TABLE invoices_stage AS SELECT * FROM df_invoices")

        conn.execute("DROP TABLE IF EXISTS jobs")
        conn.execute("ALTER TABLE jobs_stage RENAME TO jobs")

        conn.execute("DROP TABLE IF EXISTS customers")
        conn.execute("ALTER TABLE customers_stage RENAME TO customers")

        conn.execute("DROP TABLE IF EXISTS invoices")
        conn.execute("ALTER TABLE invoices_stage RENAME TO invoices")

        update_last_refresh(conn)

        print(f"[{_ts()}] Ingestion complete. Data saved to {DB_FILE}")

    finally:
        try:
            _release_ingest_lock(conn)
            print(f"[{_ts()}] Ingest lock released.")
        finally:
            conn.close()



def run_ingestion_forever(interval_seconds=3600):
    """
    Runs ingestion in a loop every interval_seconds (default 1 hour).
    IMPORTANT: Use this in a separate worker process/service,
    not inside the Flask web server process.
    """
    print(f"Starting ingestion loop: every {interval_seconds} seconds.")
    while True:
        start = time.time()
        try:
            run_ingestion()
        except Exception as e:
            print(f"Periodic ingestion error: {e}")

        elapsed = time.time() - start
        sleep_for = max(0, interval_seconds - elapsed)
        print(f"Next run in {sleep_for:.0f}s.")
        time.sleep(sleep_for)


if __name__ == "__main__":
    # If you run this file directly, you can choose:
    # - one-shot: python -m clients.jc_mechanical.ingest
    # - loop hourly: INGEST_LOOP=1 python -m clients.jc_mechanical.ingest
    loop = os.environ.get("INGEST_LOOP", "").strip() == "1"
    if loop:
        run_ingestion_forever(interval_seconds=3600)
    else:
        run_ingestion()
