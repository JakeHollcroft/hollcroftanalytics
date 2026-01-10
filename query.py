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

hr("0) TABLE INVENTORY (relevant to GP/HR)")
print(conn.execute("""
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'main'
ORDER BY table_name
""").df())

hr("1) QUICK SANITY: money fields look like CENTS?")
print(conn.execute("""
SELECT
  (SELECT MAX(amount) FROM invoices) AS max_invoice_amount_raw,
  (SELECT MAX(amount) FROM invoice_items) AS max_item_amount_raw,
  (SELECT MAX(unit_cost) FROM invoice_items) AS max_item_unit_cost_raw
""").df())
print("\nIf these maxima are in the tens/hundreds of thousands, they are almost certainly CENTS (e.g., 530500 = $5,305).")

hr("2) PRICEBOOK COVERAGE (counts + cost=0 rate)")
for t in ["pricebook_services","pricebook_materials"]:
    try:
        df = conn.execute(f"""
        SELECT
          COUNT(*) AS n,
          SUM(CASE WHEN cost IS NULL OR cost=0 THEN 1 ELSE 0 END) AS cost_zero_or_null
        FROM {t}
        """).df()
        print(f"\n{t}")
        print(df)
    except Exception as e:
        print(f"\n{t}: not found ({e})")

hr("3) INVOICE_ITEMS: cost=0 rate + qty profile")
print(conn.execute("""
SELECT
  COUNT(*) AS items_total,
  SUM(CASE WHEN unit_cost IS NULL THEN 1 ELSE 0 END) AS unit_cost_null,
  SUM(CASE WHEN unit_cost=0 THEN 1 ELSE 0 END) AS unit_cost_zero,
  SUM(CASE WHEN qty_in_hundredths IS NULL THEN 1 ELSE 0 END) AS qty_null,
  SUM(CASE WHEN qty_in_hundredths=0 THEN 1 ELSE 0 END) AS qty_zero,
  SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS amount_null,
  SUM(CASE WHEN amount=0 THEN 1 ELSE 0 END) AS amount_zero
FROM invoice_items
""").df())

hr("4) TOP 30 'labor' lines with qty + cost/price (RAW)")
print(conn.execute("""
SELECT type, name, qty_in_hundredths, unit_cost, unit_price, amount
FROM invoice_items
WHERE type='labor'
ORDER BY invoice_id DESC
LIMIT 30
""").df())

hr("5) NAME-MATCH CHECK: do labor line names appear in pricebook_services?")
try:
    df_match = conn.execute("""
    WITH labor_names AS (
      SELECT DISTINCT LOWER(TRIM(name)) AS name_norm
      FROM invoice_items
      WHERE type='labor' AND name IS NOT NULL
    ),
    svc AS (
      SELECT DISTINCT LOWER(TRIM(name)) AS name_norm, cost
      FROM pricebook_services
    )
    SELECT
      (SELECT COUNT(*) FROM labor_names) AS distinct_labor_names,
      (SELECT COUNT(*) FROM labor_names ln JOIN svc s USING(name_norm)) AS labor_names_matched_to_pricebook,
      (SELECT COUNT(*) FROM labor_names ln LEFT JOIN svc s USING(name_norm) WHERE s.name_norm IS NULL) AS labor_names_unmatched
    """).df()
    print(df_match)
except Exception as e:
    print("pricebook_services not available or query failed:", e)

hr("6) PER-JOB GP/HR (last 50 completed jobs) using CENTS->DOLLARS + billed hours from labor qty")
print(conn.execute("""
WITH ytd_jobs AS (
  SELECT job_id
  FROM jobs
  WHERE completed_at >= date_trunc('year', now())
),
items AS (
  SELECT
    ii.job_id,
    LOWER(TRIM(ii.type)) AS type_norm,
    LOWER(TRIM(ii.name)) AS name_norm,
    (COALESCE(ii.qty_in_hundredths,0) / 100.0) AS qty,
    (COALESCE(ii.amount,0) / 100.0) AS revenue_dollars,
    (COALESCE(ii.unit_cost,0) / 100.0) AS unit_cost_dollars
  FROM invoice_items ii
  JOIN ytd_jobs yj ON yj.job_id = ii.job_id
),
svc AS (
  SELECT LOWER(TRIM(name)) AS name_norm, (COALESCE(cost,0)/100.0) AS pb_cost
  FROM pricebook_services
),
mat AS (
  SELECT LOWER(TRIM(name)) AS name_norm, (COALESCE(cost,0)/100.0) AS pb_cost
  FROM pricebook_materials
),
items_costed AS (
  SELECT
    i.*,
    CASE
      WHEN i.unit_cost_dollars > 0 THEN i.unit_cost_dollars
      WHEN i.type_norm='labor' THEN COALESCE(s.pb_cost,0)
      WHEN i.type_norm='material' THEN COALESCE(m.pb_cost,0)
      ELSE 0
    END AS unit_cost_eff
  FROM items i
  LEFT JOIN svc s ON i.type_norm='labor' AND i.name_norm = s.name_norm
  LEFT JOIN mat m ON i.type_norm='material' AND i.name_norm = m.name_norm
),
job_rollup AS (
  SELECT
    j.job_id,
    SUM(revenue_dollars) AS job_revenue,
    SUM(unit_cost_eff * qty) AS job_cost,
    SUM(CASE WHEN type_norm='labor' THEN qty ELSE 0 END) AS billed_hours,
    SUM(CASE WHEN unit_cost_dollars=0 AND unit_cost_eff>0 THEN 1 ELSE 0 END) AS backfilled_lines,
    COUNT(*) AS total_lines
  FROM items_costed ic
  JOIN jobs j ON j.job_id = ic.job_id
  GROUP BY 1
)
SELECT
  job_id,
  job_revenue,
  job_cost,
  (job_revenue - job_cost) AS job_gp,
  billed_hours,
  CASE WHEN billed_hours > 0 THEN (job_revenue - job_cost) / billed_hours ELSE 0 END AS gp_per_billed_hour,
  backfilled_lines,
  total_lines
FROM job_rollup
ORDER BY job_id DESC
LIMIT 50
""").df())

hr("DONE")
conn.close()
