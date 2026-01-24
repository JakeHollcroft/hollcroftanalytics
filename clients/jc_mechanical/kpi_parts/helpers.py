# clients/jc_mechanical/kpi_parts/helpers.py

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd
import duckdb

CENTRAL_TZ = ZoneInfo("America/Chicago")


def safe_exec(conn, sql, params=None):
    """Execute SQL but ignore failures (useful when running in read-only mode)."""
    try:
        if params is None:
            return conn.execute(sql)
        return conn.execute(sql, params)
    except Exception:
        return None


def safe_df(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    try:
        return conn.execute(sql).df()
    except Exception:
        return pd.DataFrame()


def ensure_columns(df: pd.DataFrame, cols_with_dtypes: dict) -> pd.DataFrame:
    for col, dtype in cols_with_dtypes.items():
        if col not in df.columns:
            try:
                df[col] = pd.Series(dtype=dtype)
            except Exception:
                df[col] = pd.Series(dtype="object")
    return df


def to_dt_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def convert_cents_to_dollars(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0) / 100.0
    return df


def normalize_tags(tags_val) -> str:
    if tags_val is None or (isinstance(tags_val, float) and pd.isna(tags_val)):
        return ""
    s = str(tags_val)
    parts = [p.strip().lower() for p in s.split(",") if p.strip()]
    return ",".join(parts)


def tag_has(tags_norm: str, needle: str) -> bool:
    """
    Robust tag matcher.

    Works for:
      - comma-separated tags (e.g., "commercial, service")
      - single-phrase tags (e.g., "Commercial Service")

    Rules:
      - For multi-word needles (phrases), use substring match.
      - For single-word needles, use a word-boundary style match on common separators.
    """
    if not tags_norm:
        return False

    s = str(tags_norm).strip().lower()
    n = str(needle).strip().lower()
    if not n:
        return False

    # Phrase match
    if " " in n:
        return n in s

    import re
    return re.search(r"(^|[\s,])" + re.escape(n) + r"($|[\s,])", s) is not None


def format_currency(x: float) -> str:
    try:
        v = float(x)
        if v != v or v == float("inf") or v == float("-inf"):
            v = 0.0
        return "${:,.0f}".format(v)
    except Exception:
        return "$0"


def map_category_from_tags(tags_norm: str) -> str:
    """
    Revenue categories requested by JC Mechanical:
      - Residential Install
      - Residential Service
      - Residential Maintenance
      - Commercial Install
      - Commercial Service
      - Commercial Maintenance
      - New Construction

    Everything else -> Other / Unclassified
    """
    s = (tags_norm or "").strip().lower()

    is_res = tag_has(s, "residential") or ("residential" in s)
    is_com = tag_has(s, "commercial") or ("commercial" in s)

    is_new_construction = (
        tag_has(s, "new construction")
        or tag_has(s, "new-construction")
        or tag_has(s, "new build")
        or tag_has(s, "new-build")
        or tag_has(s, "newconstruction")
    )

    is_install = (
        tag_has(s, "install")
        or tag_has(s, "installation")
        or tag_has(s, "change out")
        or tag_has(s, "change-out")
        or tag_has(s, "changeout")
    )

    is_maint = tag_has(s, "maintenance")
    is_service = tag_has(s, "service") or tag_has(s, "demand service") or tag_has(s, "demand")

    if is_new_construction:
        return "New Construction"

    if is_res and is_install:
        return "Residential Install"
    if is_res and is_maint:
        return "Residential Maintenance"
    if is_res and is_service and not is_install:
        return "Residential Service"

    if is_com and is_install:
        return "Commercial Install"
    if is_com and is_maint:
        return "Commercial Maintenance"
    if is_com and is_service and not is_install:
        return "Commercial Service"

    return "Other / Unclassified"


def is_dfo_job(tags_norm: str) -> bool:
    return tag_has(tags_norm, "dfo")


def is_callback_job(tags_norm: str) -> bool:
    needles = ["callback", "call back", "call-back", "recall", "warranty", "warrantee"]
    return any(tag_has(tags_norm, n) for n in needles)


def is_service_job(tags_norm: str) -> bool:
    if not tags_norm:
        return False
    if (
        tag_has(tags_norm, "install")
        or tag_has(tags_norm, "installation")
        or tag_has(tags_norm, "change out")
        or tag_has(tags_norm, "change-out")
    ):
        return False
    return tag_has(tags_norm, "service") or tag_has(tags_norm, "demand service") or tag_has(tags_norm, "demand")


def start_of_year_utc() -> datetime:
    now_central = datetime.now(CENTRAL_TZ)
    start_central = datetime(now_central.year, 1, 1, tzinfo=CENTRAL_TZ)
    return start_central.astimezone(timezone.utc)


def largest_remainder_int_percentages(values: list[float]) -> list[int]:
    """
    Convert raw percentages into ints that sum to 100 using largest remainder.
    """
    if not values:
        return []
    floors = [int(v) for v in values]
    remainder = 100 - sum(floors)
    fracs = sorted(
        [(i, values[i] - floors[i]) for i in range(len(values))],
        key=lambda x: x[1],
        reverse=True,
    )

    out = floors[:]
    if remainder > 0:
        for j in range(remainder):
            out[fracs[j % len(fracs)][0]] += 1
    elif remainder < 0:
        fracs_asc = sorted(fracs, key=lambda x: x[1])
        for j in range(abs(remainder)):
            idx = fracs_asc[j % len(fracs_asc)][0]
            out[idx] = max(0, out[idx] - 1)
    return out
