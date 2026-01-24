# query_club_agreement_audit.py
"""
Audit + validation for the next KPI:
  Club Agreement Conversion Rate (Wizard Club / Membership)

Goal:
  View all data that would be included in the KPI and verify:
    - which tags exist and how they are formatted
    - whether "club" can be reliably identified via tags
    - service vs install classification (so we don't count installs as "service calls")
    - YTD coverage + sample rows you can eyeball

How this script works:
  1) Prints table presence, schemas, and row counts
  2) Profiles jobs.tags (raw + normalized) and finds candidate "club" tags
  3) Creates an "eligible service-call population" from completed jobs YTD
  4) Shows conversion breakdown by tag, by month, and sample job rows
  5) Helps you tune the "club tag keywords" list safely

Run:
  python query_club_agreement_audit.py
"""

import os
import re
from pathlib import Path
import duckdb
import pandas as pd

DB_FILE = Path(os.environ.get("PERSIST_DIR", ".")) / "housecall_data.duckdb"

# -----------------------------
# Config you will tune
# -----------------------------
# Candidate keywords to detect club/membership tags in jobs.tags.
# Add/remove based on your actual tags.
CLUB_TAG_KEYWORDS = [
    "wizard",
    "club",
    "membership",
    "member",
    "maintenance plan",
    "service plan",
    "agreement",
]

# How to exclude installs/change-outs from the "service call" population.
# This mirrors your current approach of classifying installs via tags.
INSTALL_EXCLUDE_KEYWORDS = [
    "install",
    "changeout",
    "change-out",
    "replacement",
    "new system",
]

# Completed job statuses in your system
COMPLETED_STATUSES = {"complete rated", "complete unrated"}

# Max rows to print in sample outputs
DEFAULT_MAX_ROWS = 80


# -----------------------------
# Helpers
# -----------------------------
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
        "display.max_colwidth", 120,
    ):
        print(df)


def safe_cast_ts_expr(col: str) -> str:
    return f"try_cast({col} AS TIMESTAMP)"


def normalize_tags_py(tags: str | None) -> str:
    """
    Normalize tags similarly to your KPI approach:
      - lower
      - split on commas
      - trim whitespace
      - re-join with '|'
    """
    if tags is None:
        return ""
    s = str(tags).strip().lower()
    if not s:
        return ""
    parts = [p.strip() for p in s.split(",") if p and p.strip()]
    # de-dup while keeping order
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return "|".join(out)


def contains_any_keyword(tag_norm: str, keywords: list[str]) -> bool:
    if not tag_norm:
        return False
    for kw in keywords:
        kw_norm = kw.strip().lower()
        if kw_norm and kw_norm in tag_norm:
            return True
    return False


def main():
    conn = duckdb.connect(DB_FILE, read_only=True)

    print("\nCLUB AGREEMENT KPI DATA AUDIT")
    print(f"DB: {DB_FILE}")

    # -----------------------------
    # 1) Tables + schemas
    # -----------------------------
    required_tables = ["jobs", "customers"]
    print("\nTABLE PRESENCE + SCHEMAS")
    for t in required_tables:
        exists = table_exists(conn, t)
        print(f"\n--- {t} exists? {exists}")
        if exists:
            print(describe(conn, t))

    if not table_exists(conn, "jobs"):
        print("\nERROR: jobs table is missing. Cannot continue.")
        conn.close()
        return

    print("\nROW COUNTS")
    for t in required_tables:
        if table_exists(conn, t):
            print_df(f"count({t})", q(conn, f"SELECT COUNT(*) AS n FROM {t}"))

    # -----------------------------
    # 2) Pull jobs into pandas (we want robust tag parsing + keyword checks)
    # -----------------------------
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

    # Normalize statuses
    df_jobs["work_status_norm"] = df_jobs["work_status"].fillna("").astype(str).str.strip().str.lower()

    # Normalize tags
    df_jobs["tags_raw"] = df_jobs["tags"]
    df_jobs["tags_norm"] = df_jobs["tags_raw"].apply(normalize_tags_py)

    # Parse datetimes
    for c in ["created_at", "updated_at", "completed_at"]:
        df_jobs[c + "_dt"] = pd.to_datetime(df_jobs[c], errors="coerce", utc=True)

    # YTD start in DuckDB (we’ll compute in python for filtering)
    ytd_start = pd.Timestamp.utcnow().tz_convert("UTC").replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    # Completed jobs YTD = your existing KPI population style
    df_completed_ytd = df_jobs[
        df_jobs["work_status_norm"].isin(COMPLETED_STATUSES)
        & (df_jobs["completed_at_dt"].notna())
        & (df_jobs["completed_at_dt"] >= ytd_start)
    ].copy()

    # -----------------------------
    # 3) Tags profiling (raw + normalized)
    # -----------------------------
    print("\nTAGS PROFILING (ALL TIME)")
    print_df("jobs with NULL/empty tags", pd.DataFrame([{
        "jobs_total": int(len(df_jobs)),
        "null_tags": int(df_jobs["tags_raw"].isna().sum()),
        "empty_tags": int((df_jobs["tags_raw"].fillna("").astype(str).str.strip() == "").sum()),
    }]))

    # Distinct raw tags values (will be noisy because it's comma strings)
    # Show the most common tag-strings to spot formatting issues.
    tag_string_counts = (
        df_jobs["tags_raw"]
        .fillna("")
        .astype(str)
        .str.strip()
        .value_counts()
        .head(50)
        .reset_index()
        .rename(columns={"index": "tags_raw_string", "count": "cnt"})
    )
    print_df("top 50 raw tag strings (exact)", tag_string_counts, max_rows=60)

    # Explode normalized tags into individual tags for clean frequency counts
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
        .reset_index(drop=True)
    )
    print_df("top 200 individual tags (by jobs tagged)", tag_freq, max_rows=200)

    # Identify candidate club tags using your keyword list
    club_kw = [k.strip().lower() for k in CLUB_TAG_KEYWORDS if k.strip()]
    tag_freq["is_club_candidate"] = tag_freq["tag"].apply(lambda t: any(kw in t for kw in club_kw))
    club_tag_candidates = tag_freq[tag_freq["is_club_candidate"]].copy()
    print_df("candidate club tags (keyword match)", club_tag_candidates, max_rows=200)

    # -----------------------------
    # 4) Define the KPI population
    #    Eligible service calls = completed jobs YTD excluding install/change-out keywords
    # -----------------------------
    df_completed_ytd["is_install_like"] = df_completed_ytd["tags_norm"].apply(
        lambda s: contains_any_keyword(s, INSTALL_EXCLUDE_KEYWORDS)
    )
    df_completed_ytd["is_service_call"] = ~df_completed_ytd["is_install_like"]

    # Club membership flag (by keywords in normalized tags)
    df_completed_ytd["is_club"] = df_completed_ytd["tags_norm"].apply(
        lambda s: contains_any_keyword(s, CLUB_TAG_KEYWORDS)
    )

    # Build KPI base
    df_kpi_base = df_completed_ytd[df_completed_ytd["is_service_call"]].copy()

    total_service_calls = int(len(df_kpi_base))
    club_service_calls = int(df_kpi_base["is_club"].sum())
    nonclub_service_calls = total_service_calls - club_service_calls
    conversion_pct = (club_service_calls / total_service_calls * 100.0) if total_service_calls else 0.0

    print("\nKPI POPULATION SUMMARY (COMPLETED JOBS YTD, SERVICE ONLY)")
    print_df("club conversion summary", pd.DataFrame([{
        "ytd_start_utc": str(ytd_start),
        "completed_jobs_ytd_total": int(len(df_completed_ytd)),
        "eligible_service_calls_ytd": total_service_calls,
        "club_service_calls_ytd": club_service_calls,
        "nonclub_service_calls_ytd": nonclub_service_calls,
        "club_conversion_pct": round(conversion_pct, 2),
        "club_tag_keywords": ", ".join(CLUB_TAG_KEYWORDS),
        "install_exclude_keywords": ", ".join(INSTALL_EXCLUDE_KEYWORDS),
    }]))

    # -----------------------------
    # 5) Breakdown views to validate behavior
    # -----------------------------
    # 5a) Show top tags among club vs non-club service calls (to validate logic)
    print("\nTAG BREAKDOWN (SERVICE CALLS YTD): CLUB vs NON-CLUB")
    df_tags_service = df_kpi_base[["job_id", "is_club", "tags_norm"]].copy()
    df_tags_service = df_tags_service.assign(tag=df_tags_service["tags_norm"].fillna("").astype(str).str.split(r"\|")).explode("tag")
    df_tags_service["tag"] = df_tags_service["tag"].fillna("").astype(str).str.strip()
    df_tags_service = df_tags_service[df_tags_service["tag"] != ""]

    tag_break = (
        df_tags_service.groupby(["is_club", "tag"])["job_id"]
        .nunique()
        .reset_index()
        .rename(columns={"job_id": "jobs"})
        .sort_values(["is_club", "jobs"], ascending=[False, False])
    )

    print_df("top 60 tags among CLUB service calls (YTD)", tag_break[tag_break["is_club"] == True].head(60), max_rows=60)
    print_df("top 60 tags among NON-CLUB service calls (YTD)", tag_break[tag_break["is_club"] == False].head(60), max_rows=60)

    # 5b) Month breakdown (service calls YTD)
    df_kpi_base["month"] = df_kpi_base["completed_at_dt"].dt.to_period("M").astype(str)
    month_break = (
        df_kpi_base.groupby("month")
        .agg(
            service_calls=("job_id", "count"),
            club_calls=("is_club", "sum"),
        )
        .reset_index()
        .sort_values("month")
    )
    month_break["club_conversion_pct"] = month_break.apply(
        lambda r: round((r["club_calls"] / r["service_calls"] * 100.0), 2) if r["service_calls"] else 0.0,
        axis=1
    )
    print_df("monthly club conversion (service calls only, YTD)", month_break, max_rows=200)

    # 5c) Sample rows (club + non-club) to eyeball tags/descriptions
    print("\nSAMPLE JOBS INCLUDED IN KPI (SERVICE CALLS YTD)")
    cols = ["job_id", "work_status", "completed_at", "description", "tags_raw", "tags_norm", "is_club", "is_install_like"]
    sample_club = df_kpi_base[df_kpi_base["is_club"] == True].sort_values("completed_at_dt", ascending=False).head(50)[cols]
    sample_nonclub = df_kpi_base[df_kpi_base["is_club"] == False].sort_values("completed_at_dt", ascending=False).head(50)[cols]

    print_df("sample CLUB service calls (latest 50)", sample_club, max_rows=60)
    print_df("sample NON-CLUB service calls (latest 50)", sample_nonclub, max_rows=60)

    # 5d) Surface possible false positives: club keyword match but tag list looks suspicious
    # Example: "club" contained in unrelated word. Rare, but this helps catch it.
    print("\nFALSE POSITIVE CHECKS")
    suspicious = df_kpi_base[df_kpi_base["is_club"] == True].copy()
    # highlight which keywords matched
    def matched_keywords(tag_norm: str) -> str:
        hits = []
        t = (tag_norm or "").lower()
        for kw in club_kw:
            if kw and kw in t:
                hits.append(kw)
        return ", ".join(hits)

    suspicious["club_kw_hits"] = suspicious["tags_norm"].apply(matched_keywords)
    suspicious_view = suspicious.sort_values("completed_at_dt", ascending=False).head(80)[
        ["job_id", "completed_at", "description", "tags_norm", "club_kw_hits"]
    ]
    print_df("club-flagged jobs with keyword hits (latest 80)", suspicious_view, max_rows=100)

    # 5e) Surface possible false negatives: tags that look like membership but didn't match keywords
    # This is a heuristic: show top tags containing "plan" or "maint" etc that aren't in keyword list.
    print("\nFALSE NEGATIVE HINTS (TAGS THAT MAY INDICATE MEMBERSHIP)")
    maybe_membership = tag_freq.copy()
    maybe_membership["hint_membership"] = maybe_membership["tag"].apply(
        lambda t: ("plan" in t) or ("maint" in t) or ("member" in t) or ("club" in t) or ("agreement" in t)
    )
    maybe_membership = maybe_membership[maybe_membership["hint_membership"]].copy()
    maybe_membership["matches_current_keywords"] = maybe_membership["tag"].apply(lambda t: any(kw in t for kw in club_kw))
    maybe_membership = maybe_membership.sort_values(["matches_current_keywords", "jobs_with_tag"], ascending=[True, False])
    print_df("membership-ish tags NOT matched by current keywords (review these)", maybe_membership[maybe_membership["matches_current_keywords"] == False].head(120), max_rows=120)

    conn.close()


if __name__ == "__main__":
    main()
