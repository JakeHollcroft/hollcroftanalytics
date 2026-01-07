# clients/jc_mechanical/ingest.py

import os
import time
import random
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta

import duckdb
import pandas as pd
import requests
from requests.exceptions import (
    HTTPError,
    ConnectionError,
    Timeout,
    ChunkedEncodingError,
    RequestException,
)

# -----------------------------
# Storage / DB
# -----------------------------
PERSIST_DIR = Path(os.environ.get("PERSIST_DIR", "."))
DB_FILE = PERSIST_DIR / "housecall_data.duckdb"

# -----------------------------
# API config
# -----------------------------
API_KEY = os.environ.get("HOUSECALLPRO_API_KEY", "").strip()

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

# -----------------------------
# Rolling window controls
# -----------------------------
# How many days back to pull for incremental runs
INGEST_WINDOW_DAYS = int(os.environ.get("INGEST_WINDOW_DAYS", "7"))

# How many days back to call job detail endpoint (appointments) for completed jobs
DETAIL_WINDOW_DAYS = int(os.environ.get("DETAIL_WINDOW_DAYS", "14"))

# One-time full backfill mode
BACKFILL = os.environ.get("BACKFILL", "").strip() == "1"

# -----------------------------
# Retry / throttling controls
# -----------------------------
DETAIL_MAX_RETRIES = 6
DETAIL_BASE_BACKOFF_SEC = 0.8
DETAIL_JITTER_SEC = 0.35
DETAIL_MIN_DELAY_EACH_CALL_SEC = 0.12
DETAIL_BATCH_PAUSE_EVERY = 25
DETAIL_BATCH_PAUSE_SEC = 0.8

REQUEST_TIMEOUT = (10, 45)

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

RUN_ID = uuid.uuid4().hex[:8]

# -----------------------------
# DB-backed cross-process lock
# -----------------------------
LOCK_KEY = "ingest_lock"
LOCK_TTL_SECONDS = 60 * 60  # 1 hour (was 6h)

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
    now = time.time()
    raw = _get_metadata(conn, LOCK_KEY)

    if raw:
        try:
            lock_ts = float(raw)
        except Exception:
            lock_ts = 0.0

        age = now - lock_ts
        if age < LOCK_TTL_SECONDS:
            print(f"[{_ts()}] RUN {RUN_ID} Lock exists (age {age:.0f}s). Skipping.")
            return False

        print(f"[{_ts()}] RUN {RUN_ID} Lock stale (age {age:.0f}s). Stealing lock...")

    _set_metadata(conn, LOCK_KEY, str(now))
    return True

def _release_ingest_lock(conn: duckdb.DuckDBPyConnection) -> None:
    _ensure_metadata_table(conn)
    conn.execute("DELETE FROM metadata WHERE key = ?", [LOCK_KEY])

def update_last_refresh(conn: duckdb.DuckDBPyConnection) -> None:
    _set_metadata(conn, "last_refresh", datetime.now(timezone.utc).isoformat())

def _ingest_due(conn: duckdb.DuckDBPyConnection, min_interval_seconds: int) -> bool:
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
    conn = duckdb.connect(DB_FILE)
    try:
        _ensure_metadata_table(conn)
        exists = conn.execute("SELECT 1 FROM metadata WHERE key = ?", [LOCK_KEY]).fetchone()
        if not exists:
            return False
        conn.execute("DELETE FROM metadata WHERE key = ?", [LOCK_KEY])
        return True
    finally:
        conn.close()

# -----------------------------
# Table creation (create once)
# -----------------------------
def ensure_core_tables(conn: duckdb.DuckDBPyConnection) -> None:
    # Keep schema simple and stable. You can tighten types later.
    conn.execute(
        """
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
        """
    )
    # Backward-compatible schema upgrade (adds missing columns)
    try:
        conn.execute("ALTER TABLE jobs ADD COLUMN tags TEXT")
    except Exception:
        pass

    
    conn.execute(
        """
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
        """
    )
    conn.execute(
        """
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
        """
    )
    _ensure_metadata_table(conn)

# -----------------------------
# Networking helpers
# -----------------------------
def _sleep_with_log(seconds: float, reason: str) -> None:
    seconds = max(0.0, float(seconds))
    print(f"[{_ts()}] RUN {RUN_ID} BACKOFF: {reason}")
    print(f"[{_ts()}] RUN {RUN_ID} Sleeping {seconds:.2f}s...")
    time.sleep(seconds)

def _request_json_with_params(url: str, params: dict, timeout=REQUEST_TIMEOUT):
    resp = SESSION.get(url, params=params, timeout=timeout)

    # If API returns 429 here, raise so caller can handle (we keep paging simpler)
    if resp.status_code == 429:
        resp.raise_for_status()

    # 400 is used below to detect "bad filter param"
    if resp.status_code >= 400:
        resp.raise_for_status()

    return resp.json()

def fetch_all(endpoint: str, key_name: str, page_size: int = 100, extra_params: dict | None = None):
    all_items = []
    page = 1

    while True:
        params = {"page": page, "page_size": page_size}
        if extra_params:
            params.update(extra_params)

        resp = SESSION.get(endpoint, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()

        data = resp.json()
        items = data.get(key_name, [])
        all_items.extend(items)

        total_pages = data.get("total_pages", 1)
        print(f"[{_ts()}] RUN {RUN_ID} Fetched page {page} of {total_pages} ({len(items)} items)")

        if page >= total_pages:
            break
        page += 1

    return all_items

def fetch_all_recent(endpoint: str, key_name: str, since_iso: str, page_size: int = 100):
    """
    Housecall Pro filtering parameters can vary depending on endpoint/version.
    We try a handful of common patterns. If all fail with 400, we fall back to full pull.
    """
    candidates = [
        {"updated_since": since_iso},
        {"updated_at_min": since_iso},
        {"updated_at_start": since_iso},
        {"updated_at[gte]": since_iso},
        {"from": since_iso},
    ]

    last_400 = None
    for p in candidates:
        try:
            print(f"[{_ts()}] RUN {RUN_ID} Trying filtered pull params: {p}")
            return fetch_all(endpoint, key_name=key_name, page_size=page_size, extra_params=p)
        except HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 400:
                last_400 = e
                print(f"[{_ts()}] RUN {RUN_ID} Filter params rejected (400). Trying next...")
                continue
            raise

    print(f"[{_ts()}] RUN {RUN_ID} WARNING: Could not filter by updated-since (400). Falling back to full pull.")
    if last_400:
        print(f"[{_ts()}] RUN {RUN_ID} Last 400 error was: {last_400}")
    return fetch_all(endpoint, key_name=key_name, page_size=page_size, extra_params=None)

def fetch_job_details(job_id: str):
    url = BASE_URL_JOB_DETAIL.format(id=job_id)
    expand_params_primary = [("expand[]", "appointments")]
    expand_params_fallback = {"expand": "appointments"}

    last_err = None
    for attempt in range(1, DETAIL_MAX_RETRIES + 1):
        if DETAIL_MIN_DELAY_EACH_CALL_SEC > 0:
            if attempt == 1:
                print(f"[{_ts()}] RUN {RUN_ID} Throttle: sleeping {DETAIL_MIN_DELAY_EACH_CALL_SEC:.2f}s before detail call for job {job_id}")
            time.sleep(DETAIL_MIN_DELAY_EACH_CALL_SEC)

        try:
            resp = SESSION.get(url, params=expand_params_primary, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 400:
                resp = SESSION.get(url, params=expand_params_fallback, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    wait = float(retry_after)
                    reason = f"429 rate limit for job {job_id} (Retry-After={retry_after}) attempt {attempt}/{DETAIL_MAX_RETRIES}"
                else:
                    wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(0, DETAIL_JITTER_SEC)
                    reason = f"429 rate limit for job {job_id} (no Retry-After) attempt {attempt}/{DETAIL_MAX_RETRIES}"
                _sleep_with_log(wait, reason)
                continue

            if resp.status_code in (500, 502, 503, 504):
                wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(0, DETAIL_JITTER_SEC)
                reason = f"Server error {resp.status_code} for job {job_id} attempt {attempt}/{DETAIL_MAX_RETRIES}"
                _sleep_with_log(wait, reason)
                continue

            resp.raise_for_status()
            return resp.json()

        except (ConnectionError, Timeout, ChunkedEncodingError) as e:
            last_err = e
            wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(0, DETAIL_JITTER_SEC)
            reason = f"Network/timeout error for job {job_id}: {type(e).__name__}: {e} attempt {attempt}/{DETAIL_MAX_RETRIES}"
            _sleep_with_log(wait, reason)
            continue

        except HTTPError as e:
            last_err = e
            status = getattr(e.response, "status_code", None)
            if status in (400, 401, 403, 404):
                raise
            wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(0, DETAIL_JITTER_SEC)
            reason = f"HTTP error for job {job_id}: status={status} {e} attempt {attempt}/{DETAIL_MAX_RETRIES}"
            _sleep_with_log(wait, reason)
            continue

        except RequestException as e:
            last_err = e
            wait = (DETAIL_BASE_BACKOFF_SEC * (2 ** (attempt - 1))) + random.uniform(0, DETAIL_JITTER_SEC)
            reason = f"RequestException for job {job_id}: {type(e).__name__}: {e} attempt {attempt}/{DETAIL_MAX_RETRIES}"
            _sleep_with_log(wait, reason)
            continue

    raise last_err if last_err else RuntimeError(f"Failed to fetch job details for {job_id}")

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
                "job_id": str(job.get("id")) if job.get("id") is not None else None,
                "invoice_number": job.get("invoice_number"),
                "description": job.get("description"),
                "work_status": job.get("work_status"),
                "total_amount": job.get("total_amount"),
                "outstanding_balance": job.get("outstanding_balance"),
                "company_name": job.get("company_name"),
                "company_id": str(job.get("company_id")) if job.get("company_id") is not None else None,
                "created_at": job.get("created_at"),
                "updated_at": job.get("updated_at"),
                "completed_at": (job.get("work_timestamps") or {}).get("completed_at"),
                "customer_id": str((job.get("customer") or {}).get("id")) if job.get("customer") else None,
                "num_appointments": 0,
                "tags": ",".join(job.get("tags", [])) if job.get("tags") else None,
            }
        )
    return pd.DataFrame(rows)

def flatten_customers(customers):
    rows = []
    for c in customers:
        rows.append(
            {
                "customer_id": str(c.get("id")) if c.get("id") is not None else None,
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
                "company_id": str(c.get("company_id")) if c.get("company_id") is not None else None,
                "tags": ",".join(c.get("tags", [])) if c.get("tags") else None,
            }
        )
    return pd.DataFrame(rows)

def flatten_invoices(invoices):
    rows = []
    for inv in invoices:
        rows.append(
            {
                "invoice_id": str(inv.get("id")) if inv.get("id") is not None else None,
                "job_id": str(inv.get("job_id")) if inv.get("job_id") is not None else None,
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
# Upsert helpers (DuckDB MERGE)
# -----------------------------
def _create_temp_incoming(conn: duckdb.DuckDBPyConnection, temp_name: str, df: pd.DataFrame):
    # Register DF as a view, then materialize into TEMP TABLE for MERGE stability
    conn.register("incoming_df", df)
    conn.execute(f"CREATE OR REPLACE TEMP TABLE {temp_name} AS SELECT * FROM incoming_df")
    conn.unregister("incoming_df")

def upsert_jobs(conn: duckdb.DuckDBPyConnection, df_jobs: pd.DataFrame):
    if df_jobs.empty:
        print(f"[{_ts()}] RUN {RUN_ID} No jobs rows to upsert.")
        return

    _create_temp_incoming(conn, "jobs_incoming", df_jobs)

    conn.execute(
        """
        MERGE INTO jobs AS t
        USING jobs_incoming AS s
        ON t.job_id = s.job_id
        WHEN MATCHED THEN UPDATE SET
            invoice_number = s.invoice_number,
            description = s.description,
            work_status = s.work_status,
            total_amount = s.total_amount,
            outstanding_balance = s.outstanding_balance,
            company_name = s.company_name,
            company_id = s.company_id,
            created_at = s.created_at,
            updated_at = s.updated_at,
            completed_at = s.completed_at,
            customer_id = s.customer_id,
            num_appointments = s.num_appointments,
            tags = s.tags

        WHEN NOT MATCHED THEN INSERT (
            job_id, invoice_number, description, work_status, total_amount, outstanding_balance,
            company_name, company_id, created_at, updated_at, completed_at, customer_id, num_appointments, tags
        ) VALUES (
            s.job_id, s.invoice_number, s.description, s.work_status, s.total_amount, s.outstanding_balance,
            s.company_name, s.company_id, s.created_at, s.updated_at, s.completed_at, s.customer_id, s.num_appointments, s.tags
        )
        """
    )
    print(f"[{_ts()}] RUN {RUN_ID} Upserted jobs: {len(df_jobs)} incoming rows.")

def upsert_customers(conn: duckdb.DuckDBPyConnection, df_customers: pd.DataFrame):
    if df_customers.empty:
        print(f"[{_ts()}] RUN {RUN_ID} No customers rows to upsert.")
        return

    _create_temp_incoming(conn, "customers_incoming", df_customers)

    conn.execute(
        """
        MERGE INTO customers AS t
        USING customers_incoming AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET
            first_name = s.first_name,
            last_name = s.last_name,
            email = s.email,
            mobile_number = s.mobile_number,
            home_number = s.home_number,
            work_number = s.work_number,
            company = s.company,
            notifications_enabled = s.notifications_enabled,
            lead_source = s.lead_source,
            notes = s.notes,
            created_at = s.created_at,
            updated_at = s.updated_at,
            company_name = s.company_name,
            company_id = s.company_id,
            tags = s.tags
        WHEN NOT MATCHED THEN INSERT (
            customer_id, first_name, last_name, email, mobile_number, home_number, work_number,
            company, notifications_enabled, lead_source, notes, created_at, updated_at,
            company_name, company_id, tags
        ) VALUES (
            s.customer_id, s.first_name, s.last_name, s.email, s.mobile_number, s.home_number, s.work_number,
            s.company, s.notifications_enabled, s.lead_source, s.notes, s.created_at, s.updated_at,
            s.company_name, s.company_id, s.tags
        )
        """
    )
    print(f"[{_ts()}] RUN {RUN_ID} Upserted customers: {len(df_customers)} incoming rows.")

def upsert_invoices(conn: duckdb.DuckDBPyConnection, df_invoices: pd.DataFrame):
    if df_invoices.empty:
        print(f"[{_ts()}] RUN {RUN_ID} No invoices rows to upsert.")
        return

    _create_temp_incoming(conn, "invoices_incoming", df_invoices)

    conn.execute(
        """
        MERGE INTO invoices AS t
        USING invoices_incoming AS s
        ON t.invoice_id = s.invoice_id
        WHEN MATCHED THEN UPDATE SET
            job_id = s.job_id,
            invoice_number = s.invoice_number,
            status = s.status,
            amount = s.amount,
            subtotal = s.subtotal,
            due_amount = s.due_amount,
            due_at = s.due_at,
            paid_at = s.paid_at,
            sent_at = s.sent_at,
            service_date = s.service_date,
            invoice_date = s.invoice_date,
            display_due_concept = s.display_due_concept,
            due_concept = s.due_concept
        WHEN NOT MATCHED THEN INSERT (
            invoice_id, job_id, invoice_number, status, amount, subtotal, due_amount, due_at, paid_at,
            sent_at, service_date, invoice_date, display_due_concept, due_concept
        ) VALUES (
            s.invoice_id, s.job_id, s.invoice_number, s.status, s.amount, s.subtotal, s.due_amount, s.due_at, s.paid_at,
            s.sent_at, s.service_date, s.invoice_date, s.display_due_concept, s.due_concept
        )
        """
    )
    print(f"[{_ts()}] RUN {RUN_ID} Upserted invoices: {len(df_invoices)} incoming rows.")

# -----------------------------
# Main ingestion
# -----------------------------
def run_ingestion(min_interval_seconds: int = 3600):
    if not API_KEY:
        raise RuntimeError("HOUSECALLPRO_API_KEY is not set. Add it to your Render env vars.")

    conn = duckdb.connect(DB_FILE)

    if not _try_acquire_ingest_lock(conn):
        conn.close()
        return

    try:
        ensure_core_tables(conn)

        if not BACKFILL and not _ingest_due(conn, min_interval_seconds):
            print(f"[{_ts()}] RUN {RUN_ID} Not due yet. Skipping ingestion.")
            return

        now_utc = datetime.now(timezone.utc)
        window_days = INGEST_WINDOW_DAYS
        window_start = now_utc - timedelta(days=window_days)
        since_iso = window_start.isoformat()

        print(f"[{_ts()}] RUN {RUN_ID} Ingest lock acquired. Starting ingestion...")
        print(f"[{_ts()}] RUN {RUN_ID} Mode: {'BACKFILL (full pull)' if BACKFILL else f'Incremental ({window_days}d window)'}")

        _set_metadata(conn, "last_window_start", since_iso)
        _set_metadata(conn, "last_window_end", now_utc.isoformat())

        # Jobs
        print(f"[{_ts()}] RUN {RUN_ID} Fetching Jobs...")
        if BACKFILL:
            jobs_data = fetch_all(BASE_URL_JOBS, key_name="jobs", page_size=100)
        else:
            jobs_data = fetch_all_recent(BASE_URL_JOBS, key_name="jobs", since_iso=since_iso, page_size=100)

        df_jobs = flatten_jobs(jobs_data)

        # Appointment counts (detail calls) only for recently completed jobs
        print(f"[{_ts()}] RUN {RUN_ID} Fetching appointment counts for completed jobs (limited window)...")
        appt_counts = {}

        if not df_jobs.empty:
            detail_window_start = now_utc - timedelta(days=DETAIL_WINDOW_DAYS)
            detail_since_iso = detail_window_start.isoformat()

            # Use completed_at if available; otherwise fall back to updated_at; otherwise include and let it be small anyway
            def _is_recent(row) -> bool:
                for col in ("completed_at", "updated_at"):
                    v = row.get(col)
                    if isinstance(v, str) and v:
                        try:
                            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            return dt >= detail_window_start
                        except Exception:
                            pass
                return True  # if unparseable, keep it (still limited by overall df_jobs size)

            completed_df = df_jobs[df_jobs["work_status"].isin(COMPLETED_STATUSES)].copy()
            if not completed_df.empty:
                completed_df = completed_df[completed_df.apply(_is_recent, axis=1)]

            completed_ids = completed_df["job_id"].dropna().tolist()

            for i, job_id in enumerate(completed_ids, start=1):
                try:
                    detail = fetch_job_details(job_id)
                    appt_counts[job_id] = appointment_count_from_job(detail)
                    if i % 10 == 0 or i == 1:
                        print(f"[{_ts()}] RUN {RUN_ID} Detail progress: {i}/{len(completed_ids)}")
                except HTTPError as e:
                    print(f"[{_ts()}] RUN {RUN_ID} Warning: HTTP error for {job_id}: {e}")
                    appt_counts[job_id] = 0
                except Exception as e:
                    print(f"[{_ts()}] RUN {RUN_ID} Warning: unexpected error for {job_id}: {e}")
                    appt_counts[job_id] = 0

                if i % DETAIL_BATCH_PAUSE_EVERY == 0:
                    print(f"[{_ts()}] RUN {RUN_ID} Batch throttle: processed {i}; sleeping {DETAIL_BATCH_PAUSE_SEC:.2f}s")
                    time.sleep(DETAIL_BATCH_PAUSE_SEC)

            df_jobs["num_appointments"] = df_jobs["job_id"].map(appt_counts).fillna(0).astype(int)

        # Customers
        print(f"[{_ts()}] RUN {RUN_ID} Fetching Customers...")
        if BACKFILL:
            customers_data = fetch_all(BASE_URL_CUSTOMERS, key_name="customers", page_size=100)
        else:
            customers_data = fetch_all_recent(BASE_URL_CUSTOMERS, key_name="customers", since_iso=since_iso, page_size=100)
        df_customers = flatten_customers(customers_data)

        # Invoices
        print(f"[{_ts()}] RUN {RUN_ID} Fetching Invoices...")
        if BACKFILL:
            invoices_data = fetch_all(BASE_URL_INVOICES, key_name="invoices", page_size=100)
        else:
            invoices_data = fetch_all_recent(BASE_URL_INVOICES, key_name="invoices", since_iso=since_iso, page_size=100)
        df_invoices = flatten_invoices(invoices_data)

        # Upsert into main tables (no drops)
        upsert_jobs(conn, df_jobs)
        upsert_customers(conn, df_customers)
        upsert_invoices(conn, df_invoices)

        update_last_refresh(conn)

        print(f"[{_ts()}] RUN {RUN_ID} Ingestion complete. Data saved to {DB_FILE}")

    finally:
        try:
            _release_ingest_lock(conn)
            print(f"[{_ts()}] RUN {RUN_ID} Ingest lock released.")
        finally:
            conn.close()

def run_ingestion_forever(interval_seconds=3600):
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
