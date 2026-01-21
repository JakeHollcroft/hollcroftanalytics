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
# --------------------------------------------------------------------------------------
# 8) ESTIMATES + OPTIONS AUDIT (Lead Turnover KPI validation)
# --------------------------------------------------------------------------------------
hr("8) ESTIMATES AUDIT: YTD counts + coverage")
print(conn.execute("""
WITH est AS (
  SELECT
    estimate_id,
    estimate_number,
    TRY_CAST(created_at AS TIMESTAMP) AS created_dt,
    lead_source,
    customer_id
  FROM estimates
),
ytd AS (
  SELECT *
  FROM est
  WHERE created_dt >= date_trunc('year', now())
)
SELECT
  COUNT(*) AS estimates_ytd,
  SUM(CASE WHEN estimate_id IS NULL OR TRIM(estimate_id)='' THEN 1 ELSE 0 END) AS missing_estimate_id,
  SUM(CASE WHEN created_dt IS NULL THEN 1 ELSE 0 END) AS missing_created_at,
  SUM(CASE WHEN lead_source IS NULL OR TRIM(lead_source)='' THEN 1 ELSE 0 END) AS missing_lead_source
FROM ytd
""").df())

hr("8a) ESTIMATE_OPTIONS status distribution (ALL TIME)")
print(conn.execute("""
SELECT
  LOWER(TRIM(COALESCE(approval_status,''))) AS approval_norm,
  LOWER(TRIM(COALESCE(status,''))) AS status_norm,
  COUNT(*) AS n
FROM estimate_options
GROUP BY 1,2
ORDER BY n DESC
LIMIT 100
""").df())

hr("8b) ESTIMATE_OPTIONS status distribution (YTD estimates only)")
print(conn.execute("""
WITH ytd_est AS (
  SELECT estimate_id
  FROM estimates
  WHERE TRY_CAST(created_at AS TIMESTAMP) >= date_trunc('year', now())
)
SELECT
  LOWER(TRIM(COALESCE(eo.approval_status,''))) AS approval_norm,
  LOWER(TRIM(COALESCE(eo.status,''))) AS status_norm,
  COUNT(*) AS n
FROM estimate_options eo
JOIN ytd_est ye USING(estimate_id)
GROUP BY 1,2
ORDER BY n DESC
LIMIT 100
""").df())

hr("8c) OPTIONS PER ESTIMATE (YTD): do we have multiple options?")
print(conn.execute("""
WITH ytd_est AS (
  SELECT estimate_id
  FROM estimates
  WHERE TRY_CAST(created_at AS TIMESTAMP) >= date_trunc('year', now())
),
opt AS (
  SELECT eo.estimate_id, COUNT(*) AS option_cnt
  FROM estimate_options eo
  JOIN ytd_est ye USING(estimate_id)
  GROUP BY 1
)
SELECT
  COUNT(*) AS estimates_with_options,
  AVG(option_cnt) AS avg_options_per_estimate,
  MAX(option_cnt) AS max_options_per_estimate,
  SUM(CASE WHEN option_cnt=1 THEN 1 ELSE 0 END) AS one_option,
  SUM(CASE WHEN option_cnt=2 THEN 1 ELSE 0 END) AS two_options,
  SUM(CASE WHEN option_cnt>=3 THEN 1 ELSE 0 END) AS three_plus_options
FROM opt
""").df())

hr("8d) ROLLUP RULE TEST (YTD): won/lost/pending per ESTIMATE")
print(conn.execute("""
WITH ytd_est AS (
  SELECT
    estimate_id,
    estimate_number,
    TRY_CAST(created_at AS TIMESTAMP) AS created_dt,
    COALESCE(NULLIF(TRIM(lead_source),''),'(unknown)') AS lead_source
  FROM estimates
  WHERE TRY_CAST(created_at AS TIMESTAMP) >= date_trunc('year', now())
),
eo AS (
  SELECT
    estimate_id,
    LOWER(TRIM(COALESCE(approval_status,''))) AS approval_norm,
    LOWER(TRIM(COALESCE(status,''))) AS status_norm
  FROM estimate_options
),
roll AS (
  SELECT
    y.estimate_id,
    y.estimate_number,
    y.created_dt,
    y.lead_source,

    MAX(CASE
      WHEN approval_norm IN ('approved','accepted','won') THEN 1
      WHEN status_norm IN ('approved','accepted','won') THEN 1
      ELSE 0
    END) AS any_won,

    MAX(CASE
      WHEN approval_norm IN ('declined','rejected','lost') THEN 1
      WHEN status_norm IN ('declined','rejected','lost') THEN 1
      ELSE 0
    END) AS any_lost,

    COUNT(*) AS option_rows
  FROM ytd_est y
  LEFT JOIN eo ON eo.estimate_id = y.estimate_id
  GROUP BY 1,2,3,4
),
bucket AS (
  SELECT *,
    CASE
      WHEN any_won=1 THEN 'won'
      WHEN any_lost=1 THEN 'lost'
      ELSE 'pending'
    END AS outcome
  FROM roll
)
SELECT
  outcome,
  COUNT(*) AS estimates,
  SUM(CASE WHEN option_rows=0 THEN 1 ELSE 0 END) AS estimates_missing_options
FROM bucket
GROUP BY 1
ORDER BY estimates DESC
""").df())

hr("8e) SAMPLE YTD estimates marked PENDING (to see why)")
print(conn.execute("""
WITH ytd_est AS (
  SELECT
    estimate_id,
    estimate_number,
    TRY_CAST(created_at AS TIMESTAMP) AS created_dt,
    COALESCE(NULLIF(TRIM(lead_source),''),'(unknown)') AS lead_source
  FROM estimates
  WHERE TRY_CAST(created_at AS TIMESTAMP) >= date_trunc('year', now())
),
eo AS (
  SELECT
    estimate_id,
    LOWER(TRIM(COALESCE(approval_status,''))) AS approval_norm,
    LOWER(TRIM(COALESCE(status,''))) AS status_norm
  FROM estimate_options
),
roll AS (
  SELECT
    y.estimate_id,
    y.estimate_number,
    y.created_dt,
    y.lead_source,

    MAX(CASE
      WHEN approval_norm IN ('approved','accepted','won') OR status_norm IN ('approved','accepted','won')
      THEN 1 ELSE 0 END) AS any_won,

    MAX(CASE
      WHEN approval_norm IN ('declined','rejected','lost') OR status_norm IN ('declined','rejected','lost')
      THEN 1 ELSE 0 END) AS any_lost
  FROM ytd_est y
  LEFT JOIN eo ON eo.estimate_id = y.estimate_id
  GROUP BY 1,2,3,4
),
pending AS (
  SELECT *
  FROM roll
  WHERE any_won=0 AND any_lost=0
)
SELECT *
FROM pending
ORDER BY created_dt DESC
LIMIT 50
""").df())

hr("8f) LEAD SOURCE BREAKDOWN (YTD) using rollup outcome")
print(conn.execute("""
WITH ytd_est AS (
  SELECT
    estimate_id,
    COALESCE(NULLIF(TRIM(lead_source),''),'(unknown)') AS lead_source
  FROM estimates
  WHERE TRY_CAST(created_at AS TIMESTAMP) >= date_trunc('year', now())
),
eo AS (
  SELECT
    estimate_id,
    LOWER(TRIM(COALESCE(approval_status,''))) AS approval_norm,
    LOWER(TRIM(COALESCE(status,''))) AS status_norm
  FROM estimate_options
),
roll AS (
  SELECT
    y.estimate_id,
    y.lead_source,
    MAX(CASE WHEN approval_norm IN ('approved','accepted','won') OR status_norm IN ('approved','accepted','won') THEN 1 ELSE 0 END) AS any_won,
    MAX(CASE WHEN approval_norm IN ('declined','rejected','lost') OR status_norm IN ('declined','rejected','lost') THEN 1 ELSE 0 END) AS any_lost
  FROM ytd_est y
  LEFT JOIN eo ON eo.estimate_id = y.estimate_id
  GROUP BY 1,2
),
bucket AS (
  SELECT *,
    CASE
      WHEN any_won=1 THEN 'won'
      WHEN any_lost=1 THEN 'lost'
      ELSE 'pending'
    END AS outcome
  FROM roll
)
SELECT
  lead_source,
  SUM(CASE WHEN outcome='won' THEN 1 ELSE 0 END) AS won,
  SUM(CASE WHEN outcome='lost' THEN 1 ELSE 0 END) AS lost,
  SUM(CASE WHEN outcome='pending' THEN 1 ELSE 0 END) AS pending,
  COUNT(*) AS total,
  ROUND(100.0 * SUM(CASE WHEN outcome='won' THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN outcome IN ('won','lost') THEN 1 ELSE 0 END),0), 1) AS win_rate_pct_excl_pending
FROM bucket
GROUP BY 1
ORDER BY total DESC
LIMIT 50
""").df())

conn.close()
