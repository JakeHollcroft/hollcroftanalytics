# clients/jc_mechanical/ingest.py

import os
import time
import random
import uuid
import duckdb
import pandas as pd
from datetime import datetime, timezone
import requests
from requests.exceptions import (
    HTTPError,
    ConnectionError,
    Timeout,
    ChunkedEncodingError,
    RequestException,
)

from .config import DB_FILE, API_KEY

# -----------------------------
# Constants / config
# -----------------------------

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

# ---- Retry / throttling controls ----
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

# Unique-ish id so you can see which process/run produced logs
RUN_ID = uuid.uuid4().hex[:8]

# -----------------------------
# DB-backed cross-process lock
# -----------------------------

LOCK_KEY = "ingest_lock"
LOCK_TTL_SECONDS = 60 * 60 * 6  # 6 hours


def _ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


def _ensure_metadata_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )


def _get_metadata(conn: duckdb.DuckDBPyConnection, key: str):
    _ensure_metadata_table(conn)
    row = conn.execute("SELECT value FROM metadata WHERE key = ?", [key]).fetchone()
    return row[0] if row else None


def _set_metadata(conn: duckdb.DuckDBPyConnection, key: str, value: str) -> None:
    _ensure_metadata_table(conn)
    conn.execute(
        """
        INSERT INTO metadata (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        [key, value],
    )


def _try_acquire_ingest_lock(conn: duckdb.DuckDBPyConnection) -> bool:
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
            print(
                f"[{_ts()}] RUN {RUN_ID} Lock exists (age {age:.0f}s). Raw lock='{raw}'. Skipping."
            )
            return False

        print(
            f"[{_ts()}] RUN {RUN_ID} Lock is stale (age {age:.0f}s). Stealing lock..."
        )

    _set_metadata(conn, LOCK_KEY, str(now))
    return True


def _release_ingest_lock(conn: duckdb.DuckDBPyConnection) -> None:
    _ensure_metadata_table(conn)
    conn.execute("DELETE FROM metadata WHERE key = ?", [LOCK_KEY])


def update_last_refresh(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Store last_refresh as ISO UTC timestamp in metadata.
    """
    _set_metadata(conn, "last_refresh", datetime.now(timezone.utc).isoformat())


def _ingest_due(conn: duckdb.DuckDBPyConnection, min_interval_seconds: int) -> bool:
    """
    Prevents re-running ingestion immediately after it completes.
    Uses metadata.last_refresh to decide if another run is due.
    """
    raw = _get_metadata(conn, "last_refresh")
    if not raw:
        return True

    try:
        last = datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
    except Exception:
        return True

    age = (datetime.now(timezone.utc) - last).total_seconds()
    return age >= float(min_interval_seconds)


def clear_ingest_lock() -> bool:
    """
    Utility: remove the ingest lock manually if you need to.
    Returns True if it deleted a lock row, False if no lock existed.
    """
    conn = duckdb.connect(DB_FILE)
    try:
        _ensure_metadata_table(conn)
        exists = conn.execute(
            "SELECT 1 FROM metadata WHERE key = ?",
            [LOCK_KEY],
        ).fetchone()
        if not exists:
            return False
        conn.execute("DELETE FROM metadata WHERE key = ?", [LOCK_KEY])
        return True
    finally:
        conn.close()


# -----------------------------
# Networking helpers
# -----------------------------


def _sleep_with_log(seconds: float, reason: str) -> None:
    seconds = max(0.0, float(seconds))
    print(f"[{_ts()}] RUN {RUN_ID} BACKOFF: {reason}")
    print(f"[{_ts()}] RUN {RUN_ID} Sleeping {seconds:.2f}s...")
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

        print(
            f"[{_ts()}] RUN {RUN_ID} Fetched page {page} of {data.get('total_pages', 1)} ({len(items)} items)"
        )

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
        if DETAIL_MIN_DELAY_EACH_CALL_SEC > 0:
            if attempt == 1:
                print(
                    f"[{_ts()}] RUN {RUN_ID} Throttle: sleeping {DETAIL_MIN_DELAY_EACH_CALL_SEC:.2f}s "
                    f"before detail call for job {job_id}"
                )
            time.sleep(DETAIL_MIN_DELAY_EACH_CALL_SEC)

        try:
            resp = SESSION.get(url, params=expand_params_primary, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 400:
                resp = SESSION.get(
                    url, params=expand_params_fallback, timeout=REQUEST_TIMEOUT
                )

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait = float(retry_after)
                    reason = (
                        f"429 rate limit for job {job_id} (Retry-After={retry_after}) "
                        f"(attempt {attempt}/{DETAIL_MAX_RETRIES})"
                    )
                else:
                    wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(
                        0, DETAIL_JITTER_SEC
                    )
                    reason = (
                        f"429 rate limit for job {job_id} (no Retry-After) "
                        f"(attempt {attempt}/{DETAIL_MAX_RETRIES})"
                    )

                _sleep_with_log(wait, reason)
                continue

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

    raise last_err if last_err else RuntimeError(
        f"Failed to fetch job details for {job_id}"
    )


# -----------------------------
# Flatteners
# -----------------------------


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
                "customer_id": (job.get("customer") or {}).get("id")
                if job.get("customer")
                else None,
                "num_appointments": 0,  # filled after details
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


# -----------------------------
# Main ingestion
# -----------------------------


def run_ingestion(min_interval_seconds: int = 3600):
    """
    One ingestion run.
    - DB-backed cross-process lock prevents overlap across threads/processes
    - "due" check prevents immediate re-runs right after completion
    - staging+swap prevents the web app from seeing missing tables mid-run
    """
    if not API_KEY:
        raise RuntimeError(
            "HOUSECALLPRO_API_KEY is not set. Add it to your Render env vars."
        )

    conn = duckdb.connect(DB_FILE)

    # Acquire lock
    if not _try_acquire_ingest_lock(conn):
        conn.close()
        return

    try:
        # Due check (prevents re-run immediately after completion / restarts)
        if not _ingest_due(conn, min_interval_seconds):
            print(f"[{_ts()}] RUN {RUN_ID} Not due yet. Skipping ingestion.")
            return

        print(f"[{_ts()}] RUN {RUN_ID} Ingest lock acquired. Starting ingestion...")

        print(f"[{_ts()}] RUN {RUN_ID} Fetching Jobs...")
        jobs_data = fetch_all(BASE_URL_JOBS, key_name="jobs", page_size=100)
        df_jobs = flatten_jobs(jobs_data)

        print(
            f"[{_ts()}] RUN {RUN_ID} Fetching appointment counts for completed jobs (job detail endpoint)..."
        )
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
                if i % 10 == 0 or i == 1:
                    print(
                        f"[{_ts()}] RUN {RUN_ID} Detail progress: {i}/{len(completed_ids)}"
                    )
            except HTTPError as e:
                print(f"[{_ts()}] RUN {RUN_ID} Warning: HTTP error for {job_id}: {e}")
                appt_counts[job_id] = 0
            except Exception as e:
                print(
                    f"[{_ts()}] RUN {RUN_ID} Warning: unexpected error for {job_id}: {e}"
                )
                appt_counts[job_id] = 0

            if i % DETAIL_BATCH_PAUSE_EVERY == 0:
                print(
                    f"[{_ts()}] RUN {RUN_ID} Batch throttle: processed {i}; sleeping {DETAIL_BATCH_PAUSE_SEC:.2f}s"
                )
                time.sleep(DETAIL_BATCH_PAUSE_SEC)

        df_jobs["num_appointments"] = (
            df_jobs["job_id"].map(appt_counts).fillna(0).astype(int)
        )

        print(f"[{_ts()}] RUN {RUN_ID} Fetching Customers...")
        customers_data = fetch_all(BASE_URL_CUSTOMERS, key_name="customers", page_size=100)
        df_customers = flatten_customers(customers_data)

        print(f"[{_ts()}] RUN {RUN_ID} Fetching Invoices...")
        invoices_data = fetch_all(BASE_URL_INVOICES, key_name="invoices", page_size=100)
        df_invoices = flatten_invoices(invoices_data)

        # Staging tables first
        conn.execute("CREATE OR REPLACE TABLE jobs_stage AS SELECT * FROM df_jobs")
        conn.execute("CREATE OR REPLACE TABLE customers_stage AS SELECT * FROM df_customers")
        conn.execute("CREATE OR REPLACE TABLE invoices_stage AS SELECT * FROM df_invoices")

        # Swap in a way that minimizes missing-table windows
        conn.execute("DROP TABLE IF EXISTS jobs")
        conn.execute("ALTER TABLE jobs_stage RENAME TO jobs")

        conn.execute("DROP TABLE IF EXISTS customers")
        conn.execute("ALTER TABLE customers_stage RENAME TO customers")

        conn.execute("DROP TABLE IF EXISTS invoices")
        conn.execute("ALTER TABLE invoices_stage RENAME TO invoices")

        update_last_refresh(conn)

        print(f"[{_ts()}] RUN {RUN_ID} Ingestion complete. Data saved to {DB_FILE}")

    finally:
        try:
            _release_ingest_lock(conn)
            print(f"[{_ts()}] RUN {RUN_ID} Ingest lock released.")
        finally:
            conn.close()


def run_ingestion_forever(interval_seconds=3600):
    """
    Optional: loop runner. If you use this, keep it in ONE process only.
    """
    print(f"[{_ts()}] RUN {RUN_ID} Starting ingestion loop: every {interval_seconds} seconds.")
    while True:
        start = time.time()
        try:
            run_ingestion(min_interval_seconds=interval_seconds)
        except Exception as e:
            print(f"[{_ts()}] RUN {RUN_ID} Periodic ingestion error: {e}")

        elapsed = time.time() - start
        sleep_for = max(0, interval_seconds - elapsed)
        print(f"[{_ts()}] RUN {RUN_ID} Next loop wake in {sleep_for:.0f}s.")
        time.sleep(sleep_for)


if __name__ == "__main__":
    loop = os.environ.get("INGEST_LOOP", "").strip() == "1"
    if loop:
        run_ingestion_forever(interval_seconds=3600)
    else:
        run_ingestion(min_interval_seconds=3600)