# clients/jc_mechanical/kpi_parts/helpers.py

import pandas as pd
import duckdb
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import os
from pathlib import Path
import re

CENTRAL_TZ = ZoneInfo("America/Chicago")


def _safe_df(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    try:
        return conn.execute(sql).df()
    except Exception:
        return pd.DataFrame()


def _ensure_columns(df: pd.DataFrame, cols_with_dtypes: dict) -> pd.DataFrame:
    for col, dtype in cols_with_dtypes.items():
        if col not in df.columns:
            try:
                df[col] = pd.Series(dtype=dtype)
            except Exception:
                df[col] = pd.Series(dtype="object")
    return df


def _to_dt_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _convert_cents_to_dollars(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0) / 100.0
    return df


def _normalize_tags(tags_val) -> str:
    if tags_val is None or (isinstance(tags_val, float) and pd.isna(tags_val)):
        return ""
    s = str(tags_val)
    parts = [p.strip().lower() for p in s.split(",") if p.strip()]
    return ",".join(parts)


def _tag_has(tags_norm: str, needle: str) -> bool:
    if not tags_norm:
        return False
    s = str(tags_norm).strip().lower()
    n = str(needle).strip().lower()
    if not n:
        return False
    if " " in n:
        return n in s
    return re.search(r"(^|[\s,])" + re.escape(n) + r"($|[\s,])", s) is not None


def _format_currency(x: float) -> str:
    try:
        v = float(x)
        if v != v or v == float("inf") or v == float("-inf"):
            v = 0.0
        return "${:,.0f}".format(v)
    except Exception:
        return "$0"


def _map_category_from_tags(tags_norm: str) -> str:
    s = (tags_norm or "").strip().lower()

    is_res = _tag_has(s, "residential") or ("residential" in s)
    is_com = _tag_has(s, "commercial") or ("commercial" in s)

    is_new_construction = (
        _tag_has(s, "new construction")
        or _tag_has(s, "new-construction")
        or _tag_has(s, "new build")
        or _tag_has(s, "new-build")
        or _tag_has(s, "newconstruction")
    )

    is_install = (
        _tag_has(s, "install")
        or _tag_has(s, "installation")
        or _tag_has(s, "change out")
        or _tag_has(s, "change-out")
        or _tag_has(s, "changeout")
    )

    is_maint = _tag_has(s, "maintenance")
    is_service = _tag_has(s, "service") or _tag_has(s, "demand service") or _tag_has(s, "demand")

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


def _is_dfo_job(tags_norm: str) -> bool:
    return _tag_has(tags_norm, "dfo")


def _is_callback_job(tags_norm: str) -> bool:
    needles = ["callback", "call back", "call-back", "recall", "warranty", "warrantee"]
    return any(_tag_has(tags_norm, n) for n in needles)


def _start_of_year_utc() -> datetime:
    now_central = datetime.now(CENTRAL_TZ)
    start_central = datetime(now_central.year, 1, 1, tzinfo=CENTRAL_TZ)
    return start_central.astimezone(timezone.utc)


def _largest_remainder_int_percentages(values: list[float]) -> list[int]:
    if not values:
        return []
    floors = [int(v) for v in values]
    remainder = 100 - sum(floors)
    fracs = sorted([(i, values[i] - floors[i]) for i in range(len(values))], key=lambda x: x[1], reverse=True)
    out = floors[:]
    for j in range(abs(remainder)):
        idx = fracs[j % len(fracs)][0]
        out[idx] += 1 if remainder > 0 else -1
    return out
