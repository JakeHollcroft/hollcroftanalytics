# clients/jc_mechanical/kpi_parts/kpi_estimates.py

from __future__ import annotations

import pandas as pd


def _ensure_columns(df: pd.DataFrame, cols_with_dtypes: dict) -> pd.DataFrame:
    """
    Minimal local version so this module doesn't depend on your main kpis.py helpers.
    """
    if df is None or df.empty:
        # still return a DataFrame with expected columns if possible
        out = pd.DataFrame()
        for col, dtype in cols_with_dtypes.items():
            try:
                out[col] = pd.Series(dtype=dtype)
            except Exception:
                out[col] = pd.Series(dtype="object")
        return out

    df = df.copy()
    for col, dtype in cols_with_dtypes.items():
        if col not in df.columns:
            try:
                df[col] = pd.Series(dtype=dtype)
            except Exception:
                df[col] = pd.Series(dtype="object")
    return df


def _to_dt_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _format_currency(x: float) -> str:
    try:
        v = float(x)
        if v != v or v in (float("inf"), float("-inf")):
            v = 0.0
        return "${:,.0f}".format(v)
    except Exception:
        return "$0"


def compute(ctx: dict) -> dict:
    """
    Estimates KPIs (best-effort) - matches the earlier in-file logic:

    - total estimates YTD
    - estimate status mix by option.status and/or approval_status

    Returns keys:
      - estimates_ytd_count
      - estimate_options_status_breakdown
    """
    start_of_year_utc = ctx["start_of_year_utc"]

    df_estimates: pd.DataFrame = ctx.get("df_estimates", pd.DataFrame())
    df_estimate_options: pd.DataFrame = ctx.get("df_estimate_options", pd.DataFrame())

    estimates_kpis = {
        "estimates_ytd_count": 0,
        "estimate_options_status_breakdown": [],
    }

    # total estimates YTD
    if df_estimates is not None and not df_estimates.empty:
        df_estimates = _ensure_columns(
            df_estimates,
            {
                "created_at": "object",
                "estimate_id": "object",
            },
        )
        df_estimates["created_at_dt"] = _to_dt_utc(df_estimates["created_at"])
        df_est_ytd = df_estimates[df_estimates["created_at_dt"] >= start_of_year_utc].copy()
        estimates_kpis["estimates_ytd_count"] = int(len(df_est_ytd))

    # option status / approval status breakdown
    if df_estimate_options is not None and not df_estimate_options.empty:
        df_estimate_options = _ensure_columns(
            df_estimate_options,
            {
                "status": "object",
                "approval_status": "object",
                "total_amount": "float64",
            },
        )

        tmp = df_estimate_options.copy()
        tmp["status_norm"] = tmp["status"].fillna("").astype(str).str.lower().str.strip()
        tmp["approval_norm"] = tmp["approval_status"].fillna("").astype(str).str.lower().str.strip()
        tmp["bucket"] = tmp.apply(
            lambda r: r["approval_norm"] if r["approval_norm"] else (r["status_norm"] if r["status_norm"] else "unknown"),
            axis=1,
        )

        b = (
            tmp.groupby("bucket")
            .agg(
                option_count=("bucket", "count"),
                total_amount=("total_amount", "sum"),
            )
            .reset_index()
            .sort_values("option_count", ascending=False)
        )

        estimates_kpis["estimate_options_status_breakdown"] = [
            {
                "bucket": str(row["bucket"]),
                "option_count": int(row["option_count"]),
                "total_amount": float(row["total_amount"] or 0.0),
                "total_amount_display": _format_currency(float(row["total_amount"] or 0.0)),
            }
            for _, row in b.iterrows()
        ]

    return estimates_kpis
