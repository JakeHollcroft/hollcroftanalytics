# clients/jc_mechanical/ingest.py

import os
import time
import random
import duckdb
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests
from requests.exceptions import (
    HTTPError,
    ConnectionError,
    Timeout,
    ChunkedEncodingError,
    RequestException,
)

from .config import DB_FILE, API_KEY

BASE_URL_JOBS = "https://api.housecallpro.com/jobs"
BASE_URL_JOB_DETAIL = "https://api.housecallpro.com/jobs/{id}"
BASE_URL_CUSTOMERS = "https://api.housecallpro.com/customers"
BASE_URL_INVOICES = "https://api.housecallpro.com/invoices"

# NEW
BASE_URL_EMPLOYEES = "https://api.housecallpro.com/employees"
BASE_URL_ESTIMATES = "https://api.housecallpro.com/estimates"

HEADERS = {
    "Authorization": f"Token {API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "HollcroftAnalytics/1.0 (+https://hollcroftanalytics.com)",
}

COMPLETED_STATUSES = {"complete unrated", "complete rated"}

# ---- Retry / throttling controls (tune later) ----
DETAIL_WINDOW_DAYS = int(os.environ.get("HCP_DETAIL_WINDOW_DAYS", "45"))
DETAIL_BATCH_PAUSE_EVERY = int(os.environ.get("HCP_DETAIL_BATCH_PAUSE_EVERY", "25"))
DETAIL_BATCH_PAUSE_SEC = float(os.environ.get("HCP_DETAIL_BATCH_PAUSE_SEC", "0.6"))

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
LOCK_KEY = "ingest_lock"


# -----------------------------
# Utilities
# -----------------------------
def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _sleep_with_log(base: float, jitter: float = 0.25):
    delay = max(0.0, base + random.uniform(0, jitter))
    print(f"[{_ts()}] RUN {RUN_ID} Sleeping {delay:.2f}s")
    time.sleep(delay)

def _add_column_if_missing(conn: duckdb.DuckDBPyConnection, table: str, col: str, col_type: str) -> None:
    cols = conn.execute(f"DESCRIBE {table}").df()["column_name"].astype(str).str.lower().tolist()
    if col.lower() not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
        print(f"[{_ts()}] RUN {RUN_ID} Schema migrate: added {table}.{col} {col_type}")


def _request_with_retry(method: str, url: str, *, headers=None, params=None, timeout=30, max_retries=6):
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.request(method, url, headers=headers, params=params, timeout=timeout)
            # Handle rate limits / transient errors
            if resp.status_code == 429 or (500 <= resp.status_code < 600):
                if attempt >= max_retries:
                    resp.raise_for_status()
                retry_after = resp.headers.get("Retry-After")
                backoff = float(retry_after) if retry_after else min(8.0, 0.7 * (2 ** (attempt - 1)))
                print(
                    f"[{_ts()}] RUN {RUN_ID} Retryable HTTP {resp.status_code} on {url}. "
                    f"Attempt {attempt}/{max_retries}. Backing off {backoff:.2f}s"
                )
                _sleep_with_log(backoff, jitter=0.6)
                continue

            resp.raise_for_status()
            return resp
        except (ConnectionError, Timeout, ChunkedEncodingError) as e:
            if attempt >= max_retries:
                raise
            backoff = min(8.0, 0.7 * (2 ** (attempt - 1)))
            print(f"[{_ts()}] RUN {RUN_ID} Network error on {url}: {e}. Attempt {attempt}/{max_retries}. Backoff {backoff:.2f}s")
            _sleep_with_log(backoff, jitter=0.6)
        except HTTPError:
            raise
        except RequestException as e:
            if attempt >= max_retries:
                raise
            backoff = min(8.0, 0.7 * (2 ** (attempt - 1)))
            print(f"[{_ts()}] RUN {RUN_ID} RequestException on {url}: {e}. Attempt {attempt}/{max_retries}. Backoff {backoff:.2f}s")
            _sleep_with_log(backoff, jitter=0.6)


# -----------------------------
# DuckDB setup
# -----------------------------
def ensure_core_tables(conn: duckdb.DuckDBPyConnection):
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
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            invoice_number TEXT,
            description TEXT,
            customer_id TEXT,
            work_status TEXT,
            total_amount DOUBLE,
            outstanding_balance DOUBLE,
            subtotal DOUBLE,
            tags TEXT,
            created_at TEXT,
            updated_at TEXT,
            completed_at TEXT,
            scheduled_start TEXT,
            scheduled_end TEXT,
            num_appointments INTEGER
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_employees (
            job_id TEXT,
            employee_id TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            mobile_number TEXT,
            color_hex TEXT,
            avatar_url TEXT,
            role TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            mobile_number TEXT,
            company TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_id TEXT PRIMARY KEY,
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

    # -----------------------------
    # employees (master list)
    # -----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS employees (
            employee_id TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            mobile_number TEXT,
            role TEXT,
            avatar_url TEXT,
            color_hex TEXT,
            company_id TEXT,
            company_name TEXT
        )
        """
    )

    # -----------------------------
    # job_appointments (normalized appointments + dispatched tech IDs)
    # One row per (job_id, appointment_id, dispatched_employee_id)
    # -----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS job_appointments (
            job_id TEXT,
            appointment_id TEXT,
            start_time TEXT,
            end_time TEXT,
            anytime BOOLEAN,
            arrival_window_minutes INTEGER,
            dispatched_employee_id TEXT
        )
        """
    )

    # -----------------------------
    # invoice_items (line items from invoices)
    # cost comes from unit_cost for now (materials/labor)
    # -----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_items (
            invoice_id TEXT,
            job_id TEXT,
            item_id TEXT,
            name TEXT,
            type TEXT,
            qty_in_hundredths INTEGER,
            unit_cost DOUBLE,
            unit_price DOUBLE,
            amount DOUBLE,
            description TEXT
        )
        """
    )

    # -----------------------------
    # estimates + options + assigned employees
    # -----------------------------
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS estimates (
            estimate_id TEXT PRIMARY KEY,
            estimate_number TEXT,
            work_status TEXT,
            lead_source TEXT,
            customer_id TEXT,
            scheduled_start TEXT,
            scheduled_end TEXT,
            created_at TEXT,
            updated_at TEXT,
            company_id TEXT,
            company_name TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS estimate_options (
            option_id TEXT PRIMARY KEY,
            estimate_id TEXT,
            name TEXT,
            option_number TEXT,
            total_amount DOUBLE,
            status TEXT,
            approval_status TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS estimate_employees (
            estimate_id TEXT,
            employee_id TEXT
        )
        """
    )

    # ---- schema migrations for older DBs (CREATE IF NOT EXISTS doesn't add columns) ----
    _add_column_if_missing(conn, "jobs", "subtotal", "DOUBLE")
    _add_column_if_missing(conn, "invoices", "subtotal", "DOUBLE")


def should_run_min_interval(conn: duckdb.DuckDBPyConnection, min_interval_seconds: int) -> bool:
    """
    Returns True if enough time has elapsed since last_refresh.
    If last_refresh is missing/unparseable, we run.
    """
    if min_interval_seconds <= 0:
        return True

    last_refresh = _get_metadata(conn, "last_refresh")
    if not last_refresh:
        return True

    try:
        dt = datetime.fromisoformat(last_refresh.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        return True

    now = datetime.now(timezone.utc)
    age_seconds = (now - dt).total_seconds()
    return age_seconds >= float(min_interval_seconds)


def _get_metadata(conn: duckdb.DuckDBPyConnection, key: str) -> str | None:
    try:
        row = conn.execute("SELECT value FROM metadata WHERE key = ?", [key]).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _set_metadata(conn: duckdb.DuckDBPyConnection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO metadata(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        [key, value],
    )


def acquire_lock(conn: duckdb.DuckDBPyConnection, ttl_minutes: int = 20) -> bool:
    now = datetime.now(timezone.utc)
    lock_val = _get_metadata(conn, LOCK_KEY)
    if lock_val:
        try:
            lock_time = datetime.fromisoformat(lock_val)
            if lock_time.tzinfo is None:
                lock_time = lock_time.replace(tzinfo=timezone.utc)
            age = now - lock_time
            if age < timedelta(minutes=ttl_minutes):
                print(f"[{_ts()}] RUN {RUN_ID} Lock present (age {age}). Skipping run.")
                return False
        except Exception:
            pass

    _set_metadata(conn, LOCK_KEY, str(now))
    return True


def release_lock(conn: duckdb.DuckDBPyConnection):
    try:
        conn.execute("DELETE FROM metadata WHERE key = ?", [LOCK_KEY])
    except Exception:
        pass


# -----------------------------
# Fetchers (paged)
# -----------------------------
def fetch_all(url: str, *, key_name: str, page_size: int = 100):
    page = 1
    out = []
    while True:
        params = {"page": page, "page_size": page_size}
        resp = _request_with_retry("GET", url, headers=HEADERS, params=params)
        data = resp.json()

        items = data.get(key_name) or data.get("data") or []
        out.extend(items)

        total_pages = data.get("total_pages") or data.get("total_pages_count") or 1
        if page >= int(total_pages):
            break
        page += 1
        _sleep_with_log(0.05, jitter=0.15)

    return out


def fetch_all_recent(url: str, *, key_name: str, since_iso: str, page_size: int = 100):
    page = 1
    out = []
    while True:
        params = {"page": page, "page_size": page_size, "updated_at_min": since_iso}
        resp = _request_with_retry("GET", url, headers=HEADERS, params=params)
        data = resp.json()

        items = data.get(key_name) or data.get("data") or []
        out.extend(items)

        total_pages = data.get("total_pages") or data.get("total_pages_count") or 1
        if page >= int(total_pages):
            break
        page += 1
        _sleep_with_log(0.05, jitter=0.15)

    return out


def fetch_job_details(job_id: str) -> dict:
    url = BASE_URL_JOB_DETAIL.format(id=job_id)
    # Important: expand appointments so we can store them for tech-hours KPIs
    params = {"expand[]": "appointments"}
    resp = _request_with_retry("GET", url, headers=HEADERS, params=params)
    return resp.json()


# -----------------------------
# Flatteners
# -----------------------------
def flatten_jobs(jobs):
    rows = []
    emp_rows = []

    for j in jobs:
        job_id = j.get("id")
        customer = j.get("customer") or {}
        schedule = j.get("schedule") or {}
        tags = j.get("tags") or []

        completed_at = None
        try:
            wt = j.get("work_timestamps") or {}
            completed_at = wt.get("completed_at")
        except Exception:
            completed_at = None

        rows.append(
            {
                "job_id": job_id,
                "invoice_number": j.get("invoice_number"),
                "description": j.get("description"),
                "customer_id": customer.get("id"),
                "work_status": j.get("work_status"),
                "total_amount": j.get("total_amount"),
                "outstanding_balance": j.get("outstanding_balance"),
                "subtotal": j.get("subtotal"),
                "tags": ",".join([str(t) for t in tags]) if tags else None,
                "created_at": j.get("created_at"),
                "updated_at": j.get("updated_at"),
                "completed_at": completed_at,
                "scheduled_start": schedule.get("scheduled_start"),
                "scheduled_end": schedule.get("scheduled_end"),
                "num_appointments": None,  # filled later from details (recent completed)
            }
        )

        for e in (j.get("assigned_employees") or []):
            emp_rows.append(
                {
                    "job_id": job_id,
                    "employee_id": e.get("id"),
                    "first_name": e.get("first_name"),
                    "last_name": e.get("last_name"),
                    "email": e.get("email"),
                    "mobile_number": e.get("mobile_number"),
                    "color_hex": e.get("color_hex"),
                    "avatar_url": e.get("avatar_url"),
                    "role": e.get("role"),
                }
            )

    return pd.DataFrame(rows), pd.DataFrame(emp_rows)


def appointment_count_from_job(job_detail: dict) -> int:
    schedule = job_detail.get("schedule") or {}
    appts = schedule.get("appointments") or []
    return len(appts)


def flatten_job_appointments(detail_job: dict):
    """
    Takes a job detail response (expanded with appointments) and returns normalized rows.
    One row per (job_id, appointment_id, dispatched_employee_id). If no dispatched list, row with None.
    """
    rows = []
    job_id = str(detail_job.get("id")) if detail_job.get("id") is not None else None
    schedule = detail_job.get("schedule") or {}
    appts = schedule.get("appointments") or []

    for a in appts:
        appt_id = a.get("id")
        dispatched = a.get("dispatched_employees_ids") or []
        if not dispatched:
            dispatched = [None]
        for emp_id in dispatched:
            rows.append(
                {
                    "job_id": job_id,
                    "appointment_id": appt_id,
                    "start_time": a.get("start_time"),
                    "end_time": a.get("end_time"),
                    "anytime": bool(a.get("anytime")) if a.get("anytime") is not None else None,
                    "arrival_window_minutes": a.get("arrival_window_minutes"),
                    "dispatched_employee_id": emp_id,
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
                "company": c.get("company"),
                "created_at": c.get("created_at"),
                "updated_at": c.get("updated_at"),
            }
        )
    return pd.DataFrame(rows)


def flatten_invoices(invoices):
    """
    Returns:
      df_invoices: one row per invoice (header-level)
      df_items: one row per invoice item (line-level)
    """
    inv_rows = []
    item_rows = []

    for inv in invoices:
        invoice_id = str(inv.get("id")) if inv.get("id") is not None else None
        job_id = str(inv.get("job_id")) if inv.get("job_id") is not None else None

        inv_rows.append(
            {
                "invoice_id": invoice_id,
                "job_id": job_id,
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

        for it in (inv.get("items") or []):
            item_rows.append(
                {
                    "invoice_id": invoice_id,
                    "job_id": job_id,
                    "item_id": it.get("id"),
                    "name": it.get("name"),
                    "type": it.get("type"),
                    "qty_in_hundredths": it.get("qty_in_hundredths"),
                    "unit_cost": it.get("unit_cost"),
                    "unit_price": it.get("unit_price"),
                    "amount": it.get("amount"),
                    "description": it.get("description"),
                }
            )

    return pd.DataFrame(inv_rows), pd.DataFrame(item_rows)


def flatten_employees(employees):
    rows = []
    for e in employees:
        rows.append(
            {
                "employee_id": str(e.get("id")) if e.get("id") is not None else None,
                "first_name": e.get("first_name"),
                "last_name": e.get("last_name"),
                "email": e.get("email"),
                "mobile_number": e.get("mobile_number"),
                "role": e.get("role"),
                "avatar_url": e.get("avatar_url"),
                "color_hex": e.get("color_hex"),
                "company_id": e.get("company_id"),
                "company_name": e.get("company_name"),
            }
        )
    return pd.DataFrame(rows)


def flatten_estimates(estimates):
    est_rows = []
    opt_rows = []
    emp_rows = []

    for est in estimates:
        estimate_id = str(est.get("id")) if est.get("id") is not None else None
        customer = est.get("customer") or {}
        schedule = est.get("schedule") or {}

        est_rows.append(
            {
                "estimate_id": estimate_id,
                "estimate_number": est.get("estimate_number"),
                "work_status": est.get("work_status"),
                "lead_source": est.get("lead_source"),
                "customer_id": customer.get("id"),
                "scheduled_start": schedule.get("scheduled_start"),
                "scheduled_end": schedule.get("scheduled_end"),
                "created_at": est.get("created_at"),
                "updated_at": est.get("updated_at"),
                "company_id": est.get("company_id"),
                "company_name": est.get("company_name"),
            }
        )

        for emp in (est.get("assigned_employees") or []):
            emp_rows.append({"estimate_id": estimate_id, "employee_id": emp.get("id")})

        for opt in (est.get("options") or []):
            opt_rows.append(
                {
                    "option_id": opt.get("id"),
                    "estimate_id": estimate_id,
                    "name": opt.get("name"),
                    "option_number": opt.get("option_number"),
                    "total_amount": opt.get("total_amount"),
                    "status": opt.get("status"),
                    "approval_status": opt.get("approval_status"),
                    "created_at": opt.get("created_at"),
                    "updated_at": opt.get("updated_at"),
                }
            )

    return pd.DataFrame(est_rows), pd.DataFrame(opt_rows), pd.DataFrame(emp_rows)


# -----------------------------
# Upsert helpers
# -----------------------------
def _create_temp_incoming(conn: duckdb.DuckDBPyConnection, name: str, df: pd.DataFrame):
    conn.register(name, df)


def upsert_jobs(conn: duckdb.DuckDBPyConnection, df_jobs: pd.DataFrame):
    if df_jobs.empty:
        print(f"[{_ts()}] RUN {RUN_ID} No job rows to upsert.")
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
            customer_id = s.customer_id,
            work_status = s.work_status,
            total_amount = s.total_amount,
            outstanding_balance = s.outstanding_balance,
            subtotal = s.subtotal,
            tags = s.tags,
            created_at = s.created_at,
            updated_at = s.updated_at,
            completed_at = s.completed_at,
            scheduled_start = s.scheduled_start,
            scheduled_end = s.scheduled_end,
            num_appointments = s.num_appointments
        WHEN NOT MATCHED THEN
            INSERT (job_id, invoice_number, description, customer_id, work_status, total_amount, outstanding_balance, subtotal, tags,
                    created_at, updated_at, completed_at, scheduled_start, scheduled_end, num_appointments)
            VALUES (s.job_id, s.invoice_number, s.description, s.customer_id, s.work_status, s.total_amount, s.outstanding_balance, s.subtotal, s.tags,
                    s.created_at, s.updated_at, s.completed_at, s.scheduled_start, s.scheduled_end, s.num_appointments)
        """
    )


def upsert_job_employees(conn: duckdb.DuckDBPyConnection, df_job_emps: pd.DataFrame):
    if df_job_emps.empty:
        return

    _create_temp_incoming(conn, "job_emps_incoming", df_job_emps)

    # Replace-by-key (job_id + employee_id)
    conn.execute(
        """
        DELETE FROM job_employees
        USING job_emps_incoming s
        WHERE job_employees.job_id = s.job_id
          AND job_employees.employee_id = s.employee_id
        """
    )
    conn.execute(
        """
        INSERT INTO job_employees (job_id, employee_id, first_name, last_name, email, mobile_number, color_hex, avatar_url, role)
        SELECT job_id, employee_id, first_name, last_name, email, mobile_number, color_hex, avatar_url, role
        FROM job_emps_incoming
        """
    )


def upsert_customers(conn: duckdb.DuckDBPyConnection, df_customers: pd.DataFrame):
    if df_customers.empty:
        print(f"[{_ts()}] RUN {RUN_ID} No customer rows to upsert.")
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
            company = s.company,
            created_at = s.created_at,
            updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (customer_id, first_name, last_name, email, mobile_number, company, created_at, updated_at)
            VALUES (s.customer_id, s.first_name, s.last_name, s.email, s.mobile_number, s.company, s.created_at, s.updated_at)
        """
    )


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
        WHEN NOT MATCHED THEN
            INSERT (invoice_id, job_id, invoice_number, status, amount, subtotal, due_amount, due_at, paid_at, sent_at, service_date, invoice_date, display_due_concept, due_concept)
            VALUES (s.invoice_id, s.job_id, s.invoice_number, s.status, s.amount, s.subtotal, s.due_amount, s.due_at, s.paid_at, s.sent_at, s.service_date, s.invoice_date, s.display_due_concept, s.due_concept)
        """
    )


def upsert_employees(conn: duckdb.DuckDBPyConnection, df_employees: pd.DataFrame):
    if df_employees.empty:
        print(f"[{_ts()}] RUN {RUN_ID} No employees rows to upsert.")
        return

    _create_temp_incoming(conn, "employees_incoming", df_employees)

    conn.execute(
        """
        MERGE INTO employees AS t
        USING employees_incoming AS s
        ON t.employee_id = s.employee_id
        WHEN MATCHED THEN UPDATE SET
            first_name = s.first_name,
            last_name = s.last_name,
            email = s.email,
            mobile_number = s.mobile_number,
            role = s.role,
            avatar_url = s.avatar_url,
            color_hex = s.color_hex,
            company_id = s.company_id,
            company_name = s.company_name
        WHEN NOT MATCHED THEN
            INSERT (employee_id, first_name, last_name, email, mobile_number, role, avatar_url, color_hex, company_id, company_name)
            VALUES (s.employee_id, s.first_name, s.last_name, s.email, s.mobile_number, s.role, s.avatar_url, s.color_hex, s.company_id, s.company_name)
        """
    )


def upsert_job_appointments(conn: duckdb.DuckDBPyConnection, df_appts: pd.DataFrame):
    if df_appts.empty:
        return

    _create_temp_incoming(conn, "job_appointments_incoming", df_appts)

    conn.execute(
        """
        DELETE FROM job_appointments
        USING job_appointments_incoming s
        WHERE job_appointments.job_id = s.job_id
          AND job_appointments.appointment_id = s.appointment_id
          AND coalesce(job_appointments.dispatched_employee_id,'') = coalesce(s.dispatched_employee_id,'')
        """
    )
    conn.execute(
        """
        INSERT INTO job_appointments (job_id, appointment_id, start_time, end_time, anytime, arrival_window_minutes, dispatched_employee_id)
        SELECT job_id, appointment_id, start_time, end_time, anytime, arrival_window_minutes, dispatched_employee_id
        FROM job_appointments_incoming
        """
    )


def upsert_invoice_items(conn: duckdb.DuckDBPyConnection, df_items: pd.DataFrame):
    if df_items.empty:
        return

    _create_temp_incoming(conn, "invoice_items_incoming", df_items)

    conn.execute(
        """
        DELETE FROM invoice_items
        USING invoice_items_incoming s
        WHERE invoice_items.invoice_id = s.invoice_id
          AND coalesce(invoice_items.item_id,'') = coalesce(s.item_id,'')
        """
    )
    conn.execute(
        """
        INSERT INTO invoice_items (invoice_id, job_id, item_id, name, type, qty_in_hundredths, unit_cost, unit_price, amount, description)
        SELECT invoice_id, job_id, item_id, name, type, qty_in_hundredths, unit_cost, unit_price, amount, description
        FROM invoice_items_incoming
        """
    )


def upsert_estimates(conn: duckdb.DuckDBPyConnection, df_estimates: pd.DataFrame):
    if df_estimates.empty:
        return

    _create_temp_incoming(conn, "estimates_incoming", df_estimates)

    conn.execute(
        """
        MERGE INTO estimates AS t
        USING estimates_incoming AS s
        ON t.estimate_id = s.estimate_id
        WHEN MATCHED THEN UPDATE SET
            estimate_number = s.estimate_number,
            work_status = s.work_status,
            lead_source = s.lead_source,
            customer_id = s.customer_id,
            scheduled_start = s.scheduled_start,
            scheduled_end = s.scheduled_end,
            created_at = s.created_at,
            updated_at = s.updated_at,
            company_id = s.company_id,
            company_name = s.company_name
        WHEN NOT MATCHED THEN
            INSERT (estimate_id, estimate_number, work_status, lead_source, customer_id, scheduled_start, scheduled_end, created_at, updated_at, company_id, company_name)
            VALUES (s.estimate_id, s.estimate_number, s.work_status, s.lead_source, s.customer_id, s.scheduled_start, s.scheduled_end, s.created_at, s.updated_at, s.company_id, s.company_name)
        """
    )


def upsert_estimate_options(conn: duckdb.DuckDBPyConnection, df_options: pd.DataFrame):
    if df_options.empty:
        return

    _create_temp_incoming(conn, "estimate_options_incoming", df_options)

    conn.execute(
        """
        MERGE INTO estimate_options AS t
        USING estimate_options_incoming AS s
        ON t.option_id = s.option_id
        WHEN MATCHED THEN UPDATE SET
            estimate_id = s.estimate_id,
            name = s.name,
            option_number = s.option_number,
            total_amount = s.total_amount,
            status = s.status,
            approval_status = s.approval_status,
            created_at = s.created_at,
            updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (option_id, estimate_id, name, option_number, total_amount, status, approval_status, created_at, updated_at)
            VALUES (s.option_id, s.estimate_id, s.name, s.option_number, s.total_amount, s.status, s.approval_status, s.created_at, s.updated_at)
        """
    )


def upsert_estimate_employees(conn: duckdb.DuckDBPyConnection, df_emp: pd.DataFrame):
    if df_emp.empty:
        return

    _create_temp_incoming(conn, "estimate_employees_incoming", df_emp)

    conn.execute(
        """
        DELETE FROM estimate_employees
        USING estimate_employees_incoming s
        WHERE estimate_employees.estimate_id = s.estimate_id
          AND estimate_employees.employee_id = s.employee_id
        """
    )
    conn.execute(
        """
        INSERT INTO estimate_employees (estimate_id, employee_id)
        SELECT estimate_id, employee_id
        FROM estimate_employees_incoming
        """
    )


# -----------------------------
# Main ingest
# -----------------------------
def run_ingestion(
    window_days: int = 60,
    min_interval_seconds: int = 3600,
):
    """
    window_days: how far back to pull for incremental updates
    min_interval_seconds: if > 0, skip if last_refresh was too recent.
      - scheduler uses 3600
      - button 'force' passes 0 to bypass interval (still lock-protected)
    """
    BACKFILL = os.environ.get("HCP_BACKFILL", "0") == "1"

    now_utc = datetime.now(timezone.utc)
    since_dt = now_utc - timedelta(days=window_days)
    since_iso = since_dt.isoformat()

    conn = duckdb.connect(DB_FILE)
    try:
        ensure_core_tables(conn)

        # lock protects from overlapping runs across scheduler + button
        if not acquire_lock(conn):
            conn.close()
            return

        # Min-interval check (only after lock, so two schedulers don't both run)
        if not should_run_min_interval(conn, int(min_interval_seconds)):
            print(f"[{_ts()}] RUN {RUN_ID} Skipping ingestion (min interval {min_interval_seconds}s not reached).")
            return

        print(f"[{_ts()}] RUN {RUN_ID} Ingest lock acquired. Starting ingestion...")
        print(f"[{_ts()}] RUN {RUN_ID} Mode: {'BACKFILL (full pull)' if BACKFILL else f'Incremental ({window_days}d window)'}")

        _set_metadata(conn, "last_window_start", since_iso)
        _set_metadata(conn, "last_window_end", now_utc.isoformat())

        # Employees
        print(f"[{_ts()}] RUN {RUN_ID} Fetching Employees...")
        employees_data = fetch_all(BASE_URL_EMPLOYEES, key_name="employees", page_size=100)
        df_employees = flatten_employees(employees_data)
        upsert_employees(conn, df_employees)

        # Estimates
        print(f"[{_ts()}] RUN {RUN_ID} Fetching Estimates...")
        if BACKFILL:
            estimates_data = fetch_all(BASE_URL_ESTIMATES, key_name="estimates", page_size=100)
        else:
            estimates_data = fetch_all_recent(BASE_URL_ESTIMATES, key_name="estimates", since_iso=since_iso, page_size=100)
        df_estimates, df_estimate_options, df_estimate_emps = flatten_estimates(estimates_data)
        upsert_estimates(conn, df_estimates)
        upsert_estimate_options(conn, df_estimate_options)
        upsert_estimate_employees(conn, df_estimate_emps)

        # Jobs
        print(f"[{_ts()}] RUN {RUN_ID} Fetching Jobs...")
        if BACKFILL:
            jobs_data = fetch_all(BASE_URL_JOBS, key_name="jobs", page_size=100)
        else:
            jobs_data = fetch_all_recent(BASE_URL_JOBS, key_name="jobs", since_iso=since_iso, page_size=100)

        df_jobs, df_job_emps = flatten_jobs(jobs_data)

        # Appointments (detail fetch for recent completed)
        appt_counts = {}
        if not df_jobs.empty:
            detail_window_start = now_utc - timedelta(days=DETAIL_WINDOW_DAYS)

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
                            continue
                return True

            completed_df = df_jobs[df_jobs["work_status"].isin(list(COMPLETED_STATUSES))]
            completed_df = completed_df[completed_df.apply(_is_recent, axis=1)]

            completed_ids = completed_df["job_id"].dropna().tolist()
            appt_frames = []

            for i, job_id in enumerate(completed_ids, start=1):
                try:
                    detail = fetch_job_details(job_id)

                    appt_counts[job_id] = appointment_count_from_job(detail)
                    df_appts = flatten_job_appointments(detail)
                    if not df_appts.empty:
                        appt_frames.append(df_appts)

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

            if appt_frames:
                df_all_appts = pd.concat(appt_frames, ignore_index=True)
                upsert_job_appointments(conn, df_all_appts)

            df_jobs["num_appointments"] = df_jobs["job_id"].map(appt_counts).fillna(0).astype(int)

            upsert_jobs(conn, df_jobs)
            upsert_job_employees(conn, df_job_emps)

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
            if not invoices_data:
                invoices_data = fetch_all(BASE_URL_INVOICES, key_name="jobs", page_size=100)
        else:
            invoices_data = fetch_all_recent(BASE_URL_INVOICES, key_name="invoices", since_iso=since_iso, page_size=100)
            if not invoices_data:
                invoices_data = fetch_all_recent(BASE_URL_INVOICES, key_name="jobs", since_iso=since_iso, page_size=100)

        df_invoices, df_invoice_items = flatten_invoices(invoices_data)

        upsert_customers(conn, df_customers)
        upsert_invoices(conn, df_invoices)
        upsert_invoice_items(conn, df_invoice_items)

        _set_metadata(conn, "last_refresh", datetime.now(timezone.utc).isoformat())
        print(f"[{_ts()}] RUN {RUN_ID} Ingestion complete.")

    finally:
        release_lock(conn)
        conn.close()


if __name__ == "__main__":
    # Default: 60-day incremental window
    run_ingestion(window_days=int(os.environ.get("HCP_WINDOW_DAYS", "60")))