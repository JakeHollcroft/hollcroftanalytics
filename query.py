# query_club_agreement_audit_v2.py
import os
from pathlib import Path
import duckdb
import pandas as pd

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

COMPLETED_STATUSES = {"complete rated", "complete unrated"}

# We’ll search these fields for “club signal”
CLUB_KEYWORDS = [
    "wizard club",
    "wizard",
    "club",
    "membership",
    "member",
    "maintenance plan",
    "service plan",
    "agreement",
]

# Used to exclude installs/change-outs from service calls (very basic)
INSTALL_EXCLUDE_KEYWORDS = [
    "install",
    "changeout",
    "change-out",
    "replacement",
    "new system",
]

DEFAULT_MAX_ROWS = 80


def table_exists(conn, table: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def describe(conn, table: str) -> pd.DataFrame:
    try:
        return conn.execute(f"DESCRIBE {table}").df()
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


def q(conn, sql: str, params=None) -> pd.DataFrame:
    try:
        if params:
            return conn.execute(sql, params).df()
        return conn.execute(sql).df()
    except Exception as e:
        return pd.DataFrame({"error": [str(e)], "sql": [sql[:800]]})


def print_df(title: str, df: pd.DataFrame, max_rows: int = DEFAULT_MAX_ROWS):
    print(f"\n=== {title} ===")
    if df is None:
        print("(None)")
        return
    with pd.option_context(
        "display.max_rows", max_rows,
        "display.max_columns", 200,
        "display.width", 220,
        "display.max_colwidth", 160,
    ):
        print(df)


def normalize_tags(tags: str | None) -> str:
    if tags is None:
        return ""
    s = str(tags).strip().lower()
    if not s:
        return ""
    parts = [p.strip() for p in s.split(",") if p and p.strip()]
    # de-dup keep order
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return "|".join(out)


def contains_any(text: str | None, keywords: list[str]) -> bool:
    if not text:
        return False
    t = str(text).lower()
    return any(k.lower() in t for k in keywords if k and k.strip())


def main():
    conn = duckdb.connect(DB_FILE, read_only=True)

    print("\nCLUB AGREEMENT KPI DATA AUDIT (V2 - TAGS vs DESCRIPTION vs INVOICE ITEMS)")
    print(f"DB: {DB_FILE}")

    required_tables = ["jobs", "customers", "invoices", "invoice_items"]
    print("\nTABLE PRESENCE + SCHEMAS")
    for t in required_tables:
        exists = table_exists(conn, t)
        print(f"\n--- {t} exists? {exists}")
        if exists:
            print(describe(conn, t))

    if not table_exists(conn, "jobs"):
        print("\nERROR: jobs table missing. Cannot continue.")
        conn.close()
        return

    # Pull jobs
    df_jobs = q(conn, """
        SELECT
          job_id,
          customer_id,
          work_status,
          description,
          tags,
          created_at,
          updated_at,
          completed_at
        FROM jobs
    """)
    if "error" in df_jobs.columns:
        print_df("jobs query failed", df_jobs)
        conn.close()
        return

    # Parse datetimes
    df_jobs["completed_at_dt"] = pd.to_datetime(df_jobs["completed_at"], errors="coerce", utc=True)
    df_jobs["work_status_norm"] = df_jobs["work_status"].fillna("").astype(str).str.strip().str.lower()
    df_jobs["tags_norm"] = df_jobs["tags"].apply(normalize_tags)
    df_jobs["desc_norm"] = df_jobs["description"].fillna("").astype(str).str.strip().str.lower()

    # YTD start
    now_utc = pd.Timestamp.utcnow().tz_localize("UTC")
    ytd_start = now_utc.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    # Completed jobs YTD
    df_completed_ytd = df_jobs[
        df_jobs["work_status_norm"].isin(COMPLETED_STATUSES)
        & df_jobs["completed_at_dt"].notna()
        & (df_jobs["completed_at_dt"] >= ytd_start)
    ].copy()

    print("\nBASELINE SUMMARY")
    print_df("jobs + completion summary", pd.DataFrame([{
        "jobs_total": int(len(df_jobs)),
        "completed_jobs_ytd_total": int(len(df_completed_ytd)),
        "ytd_start_utc": str(ytd_start),
        "null_tags_all_time": int(df_jobs["tags"].isna().sum()),
        "empty_tags_all_time": int((df_jobs["tags"].fillna("").astype(str).str.strip() == "").sum()),
    }]))

    # Service call population (exclude install-like)
    df_completed_ytd["is_install_like"] = df_completed_ytd["tags_norm"].apply(lambda s: contains_any(s, INSTALL_EXCLUDE_KEYWORDS))
    df_service_ytd = df_completed_ytd[~df_completed_ytd["is_install_like"]].copy()

    # Club signal sources
    df_service_ytd["club_from_tags"] = df_service_ytd["tags_norm"].apply(lambda s: contains_any(s, CLUB_KEYWORDS))
    df_service_ytd["club_from_description"] = df_service_ytd["desc_norm"].apply(lambda s: contains_any(s, CLUB_KEYWORDS))

    # Invoice items source (optional)
    has_invoices = table_exists(conn, "invoices")
    has_items = table_exists(conn, "invoice_items")

    df_invoice_club = pd.DataFrame(columns=["job_id", "club_from_invoice_items"])
    if has_invoices and has_items:
        df_inv = q(conn, "SELECT invoice_id, job_id, status, invoice_date FROM invoices")
        df_it = q(conn, "SELECT invoice_id, job_id, name, type, amount FROM invoice_items")

        if "error" not in df_inv.columns and "error" not in df_it.columns and not df_inv.empty and not df_it.empty:
            df_inv["invoice_date_dt"] = pd.to_datetime(df_inv["invoice_date"], errors="coerce", utc=True)
            df_inv["status_norm"] = df_inv["status"].fillna("").astype(str).str.strip().str.lower()

            # keep invoices YTD (any status, we just want to see membership line items)
            df_inv_ytd = df_inv[df_inv["invoice_date_dt"].notna() & (df_inv["invoice_date_dt"] >= ytd_start)].copy()

            # Join items -> invoices (invoice_id)
            df_it["name_norm"] = df_it["name"].fillna("").astype(str).str.strip().str.lower()
            df_join = df_it.merge(df_inv_ytd[["invoice_id", "job_id"]], on="invoice_id", how="inner", suffixes=("", "_inv"))

            # Determine if any line item name contains club keywords
            df_join["club_item"] = df_join["name_norm"].apply(lambda s: contains_any(s, CLUB_KEYWORDS))
            df_invoice_club = (
                df_join.groupby("job_id")["club_item"]
                .any()
                .reset_index()
                .rename(columns={"club_item": "club_from_invoice_items"})
            )

    # Merge invoice indicator into base
    if not df_invoice_club.empty:
        df_service_ytd = df_service_ytd.merge(df_invoice_club, on="job_id", how="left")
        df_service_ytd["club_from_invoice_items"] = df_service_ytd["club_from_invoice_items"].fillna(False)
    else:
        df_service_ytd["club_from_invoice_items"] = False

    # KPI summaries by source
    total_service = int(len(df_service_ytd))
    club_tags = int(df_service_ytd["club_from_tags"].sum())
    club_desc = int(df_service_ytd["club_from_description"].sum())
    club_items = int(df_service_ytd["club_from_invoice_items"].sum())

    def pct(n, d):
        return round((n / d * 100.0), 2) if d else 0.0

    print("\nKPI SUMMARY (SERVICE CALLS YTD) — BY CLUB SIGNAL SOURCE")
    print_df("club conversion by source", pd.DataFrame([{
        "service_calls_ytd": total_service,
        "club_calls_from_tags": club_tags,
        "club_calls_from_description": club_desc,
        "club_calls_from_invoice_items": club_items,
        "pct_from_tags": pct(club_tags, total_service),
        "pct_from_description": pct(club_desc, total_service),
        "pct_from_invoice_items": pct(club_items, total_service),
        "club_keywords": ", ".join(CLUB_KEYWORDS),
    }]))

    # Show top tags (so we confirm there truly are no wizard/club tags)
    exploded = (
        df_jobs[["job_id", "tags_norm"]]
        .assign(tag=df_jobs["tags_norm"].fillna("").astype(str).str.split(r"\|"))
        .explode("tag")
    )
    exploded["tag"] = exploded["tag"].fillna("").astype(str).str.strip()
    exploded = exploded[exploded["tag"] != ""].copy()

    tag_freq = (
        exploded.groupby("tag")["job_id"]
        .nunique()
        .reset_index()
        .rename(columns={"job_id": "jobs_with_tag"})
        .sort_values("jobs_with_tag", ascending=False)
        .head(200)
    )
    print_df("top 200 individual tags (all-time)", tag_freq, max_rows=200)

    # Candidate tags that match club keywords (should be empty based on your output)
    club_kw_lower = [k.lower() for k in CLUB_KEYWORDS]
    tag_freq["club_candidate"] = tag_freq["tag"].apply(lambda t: any(kw in t for kw in club_kw_lower))
    print_df("club candidate tags (keyword match)", tag_freq[tag_freq["club_candidate"] == True], max_rows=200)

    # IMPORTANT: show jobs where description indicates club but tags do not
    df_desc_only = df_service_ytd[
        (df_service_ytd["club_from_description"] == True)
        & (df_service_ytd["club_from_tags"] == False)
    ].copy()

    print("\nVALIDATION: 'CLUB IN DESCRIPTION' BUT NOT IN TAGS (SERVICE CALLS YTD)")
    cols = ["job_id", "completed_at", "description", "tags", "tags_norm"]
    df_desc_only = df_desc_only.sort_values("completed_at_dt", ascending=False)
    print_df("latest 80 description-club jobs (tags not club)", df_desc_only[cols].head(80), max_rows=120)

    # Also show the most common “club-y” description phrases
    if not df_desc_only.empty:
        df_desc_only["desc_bucket"] = df_desc_only["desc_norm"].str.replace(r"\s+", " ", regex=True).str.strip()
        top_desc = (
            df_desc_only["desc_bucket"]
            .value_counts()
            .head(50)
            .reset_index()
            .rename(columns={"index": "description_norm", "count": "cnt"})
        )
        print_df("top 50 club-y descriptions (service calls ytd)", top_desc, max_rows=60)

    # Sample of all service calls to eyeball overall
    print("\nSAMPLE SERVICE CALLS YTD (LATEST 60)")
    sample = df_service_ytd.sort_values("completed_at_dt", ascending=False).head(60)[
        ["job_id", "completed_at", "work_status", "description", "tags_norm",
         "club_from_tags", "club_from_description", "club_from_invoice_items"]
    ]
    print_df("service sample latest 60", sample, max_rows=80)

    conn.close()


if __name__ == "__main__":
    main()
