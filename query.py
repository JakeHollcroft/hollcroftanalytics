import os
import duckdb
import pandas as pd
from pathlib import Path

DB_FILE = Path(os.environ.get("PERSIST_DIR", "/var/data")) / "housecall_data.duckdb"
print(f"DB_FILE = {DB_FILE}")

conn = duckdb.connect(str(DB_FILE), read_only=True)

def hr(title):
    print("\n" + "="*110)
    print(title)
    print("="*110)

# --------------------------------------------------------------------------------------
# 0) TABLE INVENTORY
# --------------------------------------------------------------------------------------
hr("0) TABLE INVENTORY")
print(conn.execute("""
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
ORDER BY table_name
""").df())

# --------------------------------------------------------------------------------------
# 1) QUICK SANITY: money fields look like CENTS?
# --------------------------------------------------------------------------------------
hr("1) QUICK SANITY: money fields look like CENTS?")
print(conn.execute("""
SELECT
  (SELECT MAX(amount) FROM invoices) AS max_invoice_amount_raw,
  (SELECT MAX(amount) FROM invoice_items) AS max_item_amount_raw,
  (SELECT MAX(unit_cost) FROM invoice_items) AS max_item_unit_cost_raw
""").df())

# --------------------------------------------------------------------------------------
# 2) INVOICE STATUS DISTRIBUTION (very important for "billed only")
# --------------------------------------------------------------------------------------
hr("2) INVOICE STATUS DISTRIBUTION")
print(conn.execute("""
SELECT LOWER(TRIM(status)) AS status_norm, COUNT(*) AS n, SUM(amount)/100.0 AS sum_amount_dollars
FROM invoices
GROUP BY 1
ORDER BY n DESC
""").df())

# --------------------------------------------------------------------------------------
# 3) BILLED-ONLY INVOICES (YTD) - the exact population your KPIs are using
#    billed-only means: status IN ('open','pending_payment') and invoice_date >= Jan 1
# --------------------------------------------------------------------------------------
hr("3) BILLED-ONLY INVOICES (YTD) POPULATION")
print(conn.execute("""
WITH billed AS (
  SELECT
    invoice_id,
    job_id,
    LOWER(TRIM(status)) AS status_norm,
    invoice_number,
    invoice_date,
    service_date,
    paid_at,
    amount/100.0 AS amount_dollars
  FROM invoices
  WHERE LOWER(TRIM(status)) IN ('open','pending_payment')
    AND TRY_CAST(invoice_date AS TIMESTAMP) >= date_trunc('year', now())
)
SELECT
  COUNT(*) AS billed_invoice_count,
  SUM(amount_dollars) AS billed_invoice_sum_dollars,
  SUM(CASE WHEN job_id IS NULL OR TRIM(job_id)='' THEN 1 ELSE 0 END) AS billed_missing_job_id,
  SUM(CASE WHEN invoice_date IS NULL OR TRIM(invoice_date)='' THEN 1 ELSE 0 END) AS billed_missing_invoice_date,
  SUM(CASE WHEN service_date IS NULL OR TRIM(service_date)='' THEN 1 ELSE 0 END) AS billed_missing_service_date
FROM billed
""").df())

hr("3a) SAMPLE BILLED-ONLY INVOICES (YTD) - check job_id + dates")
print(conn.execute("""
SELECT
  invoice_id, invoice_number, job_id, LOWER(TRIM(status)) AS status_norm,
  invoice_date, service_date, paid_at,
  amount/100.0 AS amount_dollars
FROM invoices
WHERE LOWER(TRIM(status)) IN ('open','pending_payment')
  AND TRY_CAST(invoice_date AS TIMESTAMP) >= date_trunc('year', now())
ORDER BY TRY_CAST(invoice_date AS TIMESTAMP) DESC
LIMIT 50
""").df())

# --------------------------------------------------------------------------------------
# 4) JOIN COVERAGE: billed invoices -> jobs -> job_employees
#    This will explain why tech cards show 0 invoices if the join is failing.
# --------------------------------------------------------------------------------------
hr("4) JOIN COVERAGE: billed invoices -> jobs -> job_employees")
print(conn.execute("""
WITH billed AS (
  SELECT invoice_id, job_id, amount/100.0 AS amount_dollars
  FROM invoices
  WHERE LOWER(TRIM(status)) IN ('open','pending_payment')
    AND TRY_CAST(invoice_date AS TIMESTAMP) >= date_trunc('year', now())
),
billed_jobs AS (
  SELECT b.*, j.tags
  FROM billed b
  LEFT JOIN jobs j ON j.job_id = b.job_id
),
billed_jobs_emps AS (
  SELECT bj.*, je.employee_id
  FROM billed_jobs bj
  LEFT JOIN job_employees je ON je.job_id = bj.job_id
)
SELECT
  COUNT(*) AS billed_invoices,
  SUM(CASE WHEN tags IS NULL THEN 1 ELSE 0 END) AS billed_missing_job_row,
  SUM(CASE WHEN employee_id IS NULL THEN 1 ELSE 0 END) AS billed_missing_employee_assignment,
  COUNT(DISTINCT job_id) AS distinct_jobs_in_billed,
  COUNT(DISTINCT employee_id) AS distinct_employees_touched
FROM billed_jobs_emps
""").df())

hr("4a) SAMPLE ROWS where billed invoices have NO matching job row (job_id not found in jobs)")
print(conn.execute("""
WITH billed AS (
  SELECT invoice_id, job_id, invoice_number, invoice_date, amount/100.0 AS amount_dollars
  FROM invoices
  WHERE LOWER(TRIM(status)) IN ('open','pending_payment')
    AND TRY_CAST(invoice_date AS TIMESTAMP) >= date_trunc('year', now())
)
SELECT b.*
FROM billed b
LEFT JOIN jobs j ON j.job_id = b.job_id
WHERE j.job_id IS NULL
LIMIT 50
""").df())

hr("4b) SAMPLE ROWS where billed invoices DO match jobs but have NO assigned employees")
print(conn.execute("""
WITH billed AS (
  SELECT invoice_id, job_id, invoice_number, invoice_date, amount/100.0 AS amount_dollars
  FROM invoices
  WHERE LOWER(TRIM(status)) IN ('open','pending_payment')
    AND TRY_CAST(invoice_date AS TIMESTAMP) >= date_trunc('year', now())
),
bj AS (
  SELECT b.*, j.tags
  FROM billed b
  JOIN jobs j ON j.job_id = b.job_id
)
SELECT bj.*
FROM bj
LEFT JOIN job_employees je ON je.job_id = bj.job_id
WHERE je.employee_id IS NULL
LIMIT 50
""").df())

# --------------------------------------------------------------------------------------
# 5) TAG AUDIT (what tags actually exist on billed invoice jobs)
# --------------------------------------------------------------------------------------
hr("5) TAG AUDIT: distinct tags on billed invoice jobs (raw tags strings)")
print(conn.execute("""
WITH billed_jobs AS (
  SELECT DISTINCT j.tags
  FROM invoices i
  JOIN jobs j ON j.job_id = i.job_id
  WHERE LOWER(TRIM(i.status)) IN ('open','pending_payment')
    AND TRY_CAST(i.invoice_date AS TIMESTAMP) >= date_trunc('year', now())
)
SELECT tags
FROM billed_jobs
ORDER BY tags
LIMIT 200
""").df())

# --------------------------------------------------------------------------------------
# 6) CLASSIFICATION CHECK: how many billed invoices are being tagged as service vs install
#    This mirrors the logic in the KPI code (service excludes install/change-out).
# --------------------------------------------------------------------------------------
hr("6) CLASSIFICATION CHECK: billed invoices -> service vs install buckets")
print(conn.execute("""
WITH billed AS (
  SELECT i.invoice_id, i.job_id, i.amount/100.0 AS amount_dollars
  FROM invoices i
  WHERE LOWER(TRIM(i.status)) IN ('open','pending_payment')
    AND TRY_CAST(i.invoice_date AS TIMESTAMP) >= date_trunc('year', now())
),
bj AS (
  SELECT b.*, LOWER(COALESCE(j.tags,'')) AS tags_norm
  FROM billed b
  LEFT JOIN jobs j ON j.job_id = b.job_id
),
flags AS (
  SELECT *,
    CASE
      WHEN tags_norm LIKE '%install%' OR tags_norm LIKE '%change out%' OR tags_norm LIKE '%change-out%' THEN 1
      ELSE 0
    END AS is_install,
    CASE
      WHEN (tags_norm LIKE '%service%' OR tags_norm LIKE '%demand%')
           AND NOT (tags_norm LIKE '%install%' OR tags_norm LIKE '%change out%' OR tags_norm LIKE '%change-out%')
      THEN 1 ELSE 0
    END AS is_service
  FROM bj
)
SELECT
  SUM(is_service) AS service_invoice_count,
  SUM(CASE WHEN is_service=1 THEN amount_dollars ELSE 0 END) AS service_revenue,
  SUM(is_install) AS install_invoice_count,
  SUM(CASE WHEN is_install=1 THEN amount_dollars ELSE 0 END) AS install_revenue,
  SUM(CASE WHEN is_service=0 AND is_install=0 THEN 1 ELSE 0 END) AS unclassified_count,
  SUM(CASE WHEN is_service=0 AND is_install=0 THEN amount_dollars ELSE 0 END) AS unclassified_revenue
FROM flags
""").df())

hr("6a) SAMPLE unclassified billed invoices (these cause service=0 invoices like your screenshot)")
print(conn.execute("""
WITH billed AS (
  SELECT i.invoice_id, i.invoice_number, i.job_id, i.amount/100.0 AS amount_dollars
  FROM invoices i
  WHERE LOWER(TRIM(i.status)) IN ('open','pending_payment')
    AND TRY_CAST(i.invoice_date AS TIMESTAMP) >= date_trunc('year', now())
),
bj AS (
  SELECT b.*, LOWER(COALESCE(j.tags,'')) AS tags_norm
  FROM billed b
  LEFT JOIN jobs j ON j.job_id = b.job_id
),
flags AS (
  SELECT *,
    CASE
      WHEN tags_norm LIKE '%install%' OR tags_norm LIKE '%change out%' OR tags_norm LIKE '%change-out%' THEN 1
      ELSE 0
    END AS is_install,
    CASE
      WHEN (tags_norm LIKE '%service%' OR tags_norm LIKE '%demand%')
           AND NOT (tags_norm LIKE '%install%' OR tags_norm LIKE '%change out%' OR tags_norm LIKE '%change-out%')
      THEN 1 ELSE 0
    END AS is_service
  FROM bj
)
SELECT invoice_id, invoice_number, job_id, amount_dollars, tags_norm
FROM flags
WHERE is_service=0 AND is_install=0
LIMIT 100
""").df())

# --------------------------------------------------------------------------------------
# 7) TECH ATTRIBUTION CHECK (why tech cards show 0 invoices)
# --------------------------------------------------------------------------------------
hr("7) TECH ATTRIBUTION CHECK: billed invoices that actually map to job_employees")
print(conn.execute("""
WITH billed AS (
  SELECT i.invoice_id, i.job_id, TRY_CAST(i.invoice_date AS DATE) AS inv_date,
         i.amount/100.0 AS amount_dollars
  FROM invoices i
  WHERE LOWER(TRIM(i.status)) IN ('open','pending_payment')
    AND TRY_CAST(i.invoice_date AS TIMESTAMP) >= date_trunc('year', now())
),
billed_emp AS (
  SELECT b.*, je.employee_id
  FROM billed b
  JOIN job_employees je ON je.job_id = b.job_id
)
SELECT
  employee_id,
  COUNT(DISTINCT invoice_id) AS invoices,
  SUM(amount_dollars) AS revenue,
  COUNT(DISTINCT inv_date) AS distinct_days_with_invoices
FROM billed_emp
GROUP BY 1
ORDER BY revenue DESC
LIMIT 50
""").df())

hr("DONE")
conn.close()
