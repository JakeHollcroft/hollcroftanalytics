# clients/jc_mechanical/kpi_parts/kpi_lead_turnover.py

from __future__ import annotations

import pandas as pd


def compute(ctx: dict) -> dict:
    completed_jobs = int(ctx.get("completed_jobs", 0))
    start_utc = ctx.get("start_of_year_utc")

    df_estimates: pd.DataFrame = ctx.get("df_estimates", pd.DataFrame()).copy()
    df_estimate_options: pd.DataFrame = ctx.get("df_estimate_options", pd.DataFrame()).copy()
    df_estimate_emps: pd.DataFrame = ctx.get("df_estimate_emps", pd.DataFrame()).copy()

    lead_turnover = {
        "estimates_created_ytd": 0,
        "estimates_won_ytd": 0,
        "estimates_lost_ytd": 0,
        "estimates_pending_ytd": 0,
        "win_rate_ytd": 0.0,
        "calls_ytd": int(completed_jobs),  # proxy
        "estimate_to_call_ratio": 0.0,
        "month_breakdown": [],
        "employee_breakdown": [],
        "lead_source_breakdown": [],
    }

    if df_estimates.empty:
        return lead_turnover

    for col in ["estimate_id", "created_at", "lead_source"]:
        if col not in df_estimates.columns:
            df_estimates[col] = None

    df_estimates["created_at_dt"] = pd.to_datetime(df_estimates["created_at"], errors="coerce", utc=True)
    if start_utc is not None:
        df_estimates = df_estimates[df_estimates["created_at_dt"] >= start_utc].copy()

    lead_turnover["estimates_created_ytd"] = int(len(df_estimates))
    if lead_turnover["calls_ytd"] > 0:
        lead_turnover["estimate_to_call_ratio"] = round(lead_turnover["estimates_created_ytd"] / lead_turnover["calls_ytd"], 3)

    def _norm_str(x) -> str:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        return str(x).strip().lower()

    def _bucket_option_status(approval_status_val, status_val) -> str:
        a = _norm_str(approval_status_val)
        s = _norm_str(status_val)

        won_keys = {
            "approved", "pro approved", "accepted", "won", "sold", "selected",
            "created job from estimate", "complete rated", "complete unrated",
            "scheduled", "in progress",
        }
        lost_keys = {
            "declined", "pro declined", "rejected", "lost", "expired",
            "canceled", "cancelled", "deleted", "void",
        }

        if a in won_keys or s in won_keys:
            return "won"
        if a in lost_keys or s in lost_keys:
            return "lost"
        return "pending"

    # Determine won/lost/pending primarily from estimate_options when present
    if not df_estimate_options.empty:
        for col in ["estimate_id", "approval_status", "status"]:
            if col not in df_estimate_options.columns:
                df_estimate_options[col] = None

        eo = df_estimate_options[df_estimate_options["estimate_id"].isin(df_estimates["estimate_id"])].copy()
        eo["bucket"] = eo.apply(lambda r: _bucket_option_status(r.get("approval_status"), r.get("status")), axis=1)

        # Collapse multiple options per estimate to a single bucket
        # Priority: won > lost > pending
        bucket_rank = {"won": 3, "lost": 2, "pending": 1}
        eo["rank"] = eo["bucket"].map(bucket_rank).fillna(1).astype(int)
        best = eo.sort_values(["estimate_id", "rank"], ascending=[True, False]).drop_duplicates("estimate_id")

        won = int((best["bucket"] == "won").sum())
        lost = int((best["bucket"] == "lost").sum())
        pending = int((best["bucket"] == "pending").sum())
    else:
        # Fallback to estimate status
        s = df_estimates.get("status", "").astype(str).str.lower()
        won = int(s.isin(["approved", "accepted", "won", "sold"]).sum())
        lost = int(s.isin(["declined", "rejected", "lost", "expired", "canceled", "cancelled", "void"]).sum())
        pending = int(len(df_estimates) - won - lost)

    lead_turnover["estimates_won_ytd"] = won
    lead_turnover["estimates_lost_ytd"] = lost
    lead_turnover["estimates_pending_ytd"] = pending
    lead_turnover["win_rate_ytd"] = round((won / (won + lost)) * 100.0, 1) if (won + lost) > 0 else 0.0

    # Month breakdown
    df_m = df_estimates.copy()
    df_m["month"] = df_m["created_at_dt"].dt.to_period("M")
    month_counts = df_m.groupby("month").size()

    for month, cnt in month_counts.sort_index().items():
        lead_turnover["month_breakdown"].append({"month": str(month), "estimates_created": int(cnt)})

    # Lead source breakdown
    if "lead_source" in df_estimates.columns:
        ls = df_estimates["lead_source"].fillna("").astype(str).str.strip()
        ls_counts = ls.value_counts()
        for src, cnt in ls_counts.items():
            lead_turnover["lead_source_breakdown"].append({"lead_source": src or "Unknown", "count": int(cnt)})

    # Employee breakdown (best-effort)
    # If estimate_employees ties estimate -> employee, use it.
    if not df_estimate_emps.empty:
        for col in ["estimate_id", "employee_id", "first_name", "last_name"]:
            if col not in df_estimate_emps.columns:
                df_estimate_emps[col] = None

        ee = df_estimate_emps[df_estimate_emps["estimate_id"].isin(df_estimates["estimate_id"])].copy()
        if not ee.empty:
            emp_counts = ee.groupby(["employee_id", "first_name", "last_name"]).size().reset_index(name="created")
            for _, r in emp_counts.iterrows():
                name = f"{str(r.get('first_name','') or '').strip()} {str(r.get('last_name','') or '').strip()}".strip()
                lead_turnover["employee_breakdown"].append(
                    {"employee_id": r.get("employee_id", ""), "employee_name": name or "Unknown", "estimates_created": int(r["created"])}
                )

    return lead_turnover
