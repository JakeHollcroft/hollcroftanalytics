# clients/jc_mechanical/kpi_parts/kpi_lead_turnover.py

import pandas as pd

from .helpers import _ensure_columns


def compute(ctx):
    completed_jobs = int(ctx.get("completed_jobs", 0))
    start_of_year_utc = ctx["start_of_year_utc"]

    df_estimates = ctx.get("df_estimates", pd.DataFrame()).copy()
    df_estimate_options = ctx.get("df_estimate_options", pd.DataFrame()).copy()
    df_estimate_emps = ctx.get("df_estimate_emps", pd.DataFrame()).copy()
    df_emp_dim = ctx.get("df_emp_dim", pd.DataFrame()).copy()

    # -----------------------------------
    # Lead Turnover KPIs (YTD)
    # - Estimates created
    # - Won vs Lost (derived from estimate_options approval_status/status rolled up per estimate)
    # - Win rate
    # - Estimate to Call ratio (calls proxied by completed jobs YTD, until a true calls table exists)
    # - Optional breakdowns: by month, by employee, by lead source
    # -----------------------------------
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

    def _norm_str(x) -> str:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        return str(x).strip().lower()

    def _bucket_option_status(approval_status_val, status_val) -> str:
        a = _norm_str(approval_status_val)
        s = _norm_str(status_val)

        won_keys = {
            "approved",
            "pro approved",
            "accepted",
            "won",
            "sold",
            "selected",
            "created job from estimate",
            "complete rated",
            "complete unrated",
            "scheduled",
            "in progress",
        }
        lost_keys = {
            "declined",
            "pro declined",
            "rejected",
            "lost",
            "expired",
            "canceled",
            "cancelled",
            "deleted",
            "void",
        }

        if a in won_keys or s in won_keys:
            return "won"
        if a in lost_keys or s in lost_keys:
            return "lost"
        return "pending"

    if df_estimates.empty:
        return lead_turnover

    df_estimates = _ensure_columns(
        df_estimates,
        {
            "estimate_id": "object",
            "created_at": "object",
            "lead_source": "object",
        },
    )

    df_estimates["created_at_dt"] = pd.to_datetime(df_estimates["created_at"], errors="coerce", utc=True)
    df_est_ytd = df_estimates[df_estimates["created_at_dt"] >= start_of_year_utc].copy()

    lead_turnover["estimates_created_ytd"] = int(len(df_est_ytd))
    if lead_turnover["calls_ytd"] > 0:
        lead_turnover["estimate_to_call_ratio"] = round(
            lead_turnover["estimates_created_ytd"] / lead_turnover["calls_ytd"], 3
        )

    # Derive outcome per estimate (won/lost/pending) using estimate_options if available
    if not df_estimate_options.empty:
        eo = df_estimate_options.copy()
        eo = _ensure_columns(
            eo,
            {
                "estimate_id": "object",
                "approval_status": "object",
                "status": "object",
            },
        )

        eo["estimate_id"] = eo["estimate_id"].astype(str)
        df_est_ytd["estimate_id"] = df_est_ytd["estimate_id"].astype(str)

        eo = eo[eo["estimate_id"].isin(df_est_ytd["estimate_id"])].copy()
        if not eo.empty:
            eo["outcome"] = eo.apply(
                lambda r: _bucket_option_status(r.get("approval_status"), r.get("status")),
                axis=1,
            )

            # Collapse to one row per estimate (priority: won > lost > pending)
            outcome_rank = {"won": 3, "lost": 2, "pending": 1}
            eo["rank"] = eo["outcome"].map(outcome_rank).fillna(1).astype(int)
            eo_best = (
                eo.sort_values(["estimate_id", "rank"], ascending=[True, False])
                .drop_duplicates("estimate_id")
                .copy()
            )

            df_est_ytd = df_est_ytd.merge(
                eo_best[["estimate_id", "outcome"]],
                how="left",
                on="estimate_id",
            )
        else:
            df_est_ytd["outcome"] = "pending"
    else:
        df_est_ytd["outcome"] = "pending"

    # Fill any missing outcomes
    df_est_ytd["outcome"] = df_est_ytd["outcome"].fillna("pending").astype(str)

    won = int((df_est_ytd["outcome"] == "won").sum())
    lost = int((df_est_ytd["outcome"] == "lost").sum())
    pending = int((df_est_ytd["outcome"] == "pending").sum())

    lead_turnover["estimates_won_ytd"] = won
    lead_turnover["estimates_lost_ytd"] = lost
    lead_turnover["estimates_pending_ytd"] = pending
    lead_turnover["win_rate_ytd"] = round((won / (won + lost)) * 100.0, 1) if (won + lost) > 0 else 0.0

    # Monthly breakdown
    df_m = df_est_ytd.copy()
    df_m["month"] = df_m["created_at_dt"].dt.to_period("M")
    mb = (
        df_m.groupby("month")
        .agg(
            estimates=("estimate_id", "nunique"),
            won=("outcome", lambda s: int((s == "won").sum())),
            lost=("outcome", lambda s: int((s == "lost").sum())),
            pending=("outcome", lambda s: int((s == "pending").sum())),
        )
        .reset_index()
        .sort_values("month", ascending=True)
    )

    lead_turnover["month_breakdown"] = [
        {
            "month": str(r["month"]),
            "estimates": int(r["estimates"]),
            "won": int(r["won"]),
            "lost": int(r["lost"]),
            "pending": int(r["pending"]),
            "win_rate": round(
                (int(r["won"]) / (int(r["won"]) + int(r["lost"])) * 100.0), 1
            )
            if (int(r["won"]) + int(r["lost"])) > 0
            else 0.0,
        }
        for _, r in mb.iterrows()
    ]

    # Lead source breakdown
    df_ls = df_est_ytd.copy()
    df_ls["lead_source_norm"] = df_ls["lead_source"].fillna("").astype(str).str.strip()
    if not df_ls.empty:
        lsb = (
            df_ls.groupby("lead_source_norm")
            .agg(
                estimates=("estimate_id", "nunique"),
                won=("outcome", lambda s: int((s == "won").sum())),
                lost=("outcome", lambda s: int((s == "lost").sum())),
                pending=("outcome", lambda s: int((s == "pending").sum())),
            )
            .reset_index()
            .sort_values("estimates", ascending=False)
        )
        lead_turnover["lead_source_breakdown"] = [
            {
                "lead_source": str(r["lead_source_norm"]) or "Unknown",
                "estimates": int(r["estimates"]),
                "won": int(r["won"]),
                "lost": int(r["lost"]),
                "pending": int(r["pending"]),
                "win_rate": round(
                    (int(r["won"]) / (int(r["won"]) + int(r["lost"])) * 100.0), 1
                )
                if (int(r["won"]) + int(r["lost"])) > 0
                else 0.0,
            }
            for _, r in lsb.iterrows()
        ]

    # Employee breakdown (best-effort via estimate_employees -> employee_id)
    emp_breakdown = []
    if not df_estimate_emps.empty:
        ee = df_estimate_emps.copy()
        ee = _ensure_columns(
            ee,
            {
                "estimate_id": "object",
                "employee_id": "object",
                "first_name": "object",
                "last_name": "object",
            },
        )

        # Normalize join keys
        ee["employee_id"] = ee["employee_id"].astype(str)

        if not df_emp_dim.empty:
            df_emp_dim["employee_id"] = df_emp_dim["employee_id"].astype(str)

        # Prefer name from estimate_employees, then fall back to employee dimension name
        ee["employee_name_from_est"] = (
            ee["first_name"].fillna("").astype(str).str.strip()
            + " "
            + ee["last_name"].fillna("").astype(str).str.strip()
        ).str.strip()

        if not df_emp_dim.empty and "employee_name" in df_emp_dim.columns:
            ee = ee.merge(
                df_emp_dim[["employee_id", "employee_name"]],
                how="left",
                on="employee_id",
            )
        else:
            ee["employee_name"] = ""

        ee["employee_name"] = ee["employee_name"].fillna("").astype(str).str.strip()
        ee.loc[ee["employee_name"] == "", "employee_name"] = ee.loc[
            ee["employee_name"] == "", "employee_name_from_est"
        ]
        ee["employee_name"] = ee["employee_name"].fillna("").astype(str).str.strip()

        _missing_name = ee["employee_name"].eq("") | ee["employee_name"].isna()
        ee.loc[_missing_name, "employee_name"] = "Unassigned"

        looks_like_id = ee["employee_name"].astype(str).str.match(r"^pro_[0-9a-f]{32}$", na=False)
        ee.loc[looks_like_id, "employee_name"] = "Unassigned"

        # Limit to YTD estimates only
        ee["estimate_id"] = ee["estimate_id"].astype(str)
        ytd_ids = set(df_est_ytd["estimate_id"].astype(str).tolist())
        ee = ee[ee["estimate_id"].isin(ytd_ids)].copy()

        # Attach outcome for rollups
        ee = ee.merge(
            df_est_ytd[["estimate_id", "outcome"]],
            how="left",
            on="estimate_id",
        )

        eb = (
            ee.groupby("employee_name")
            .agg(
                estimates=("estimate_id", "nunique"),
                won=("outcome", lambda s: int((s == "won").sum())),
                lost=("outcome", lambda s: int((s == "lost").sum())),
                pending=("outcome", lambda s: int((s == "pending").sum())),
            )
            .reset_index()
            .sort_values("estimates", ascending=False)
        )

        emp_breakdown = [
            {
                "employee_name": str(r["employee_name"]) or "Unknown",
                "estimates": int(r["estimates"]),
                "won": int(r["won"]),
                "lost": int(r["lost"]),
                "pending": int(r.get("pending", 0)),
                "win_rate": round(
                    (int(r["won"]) / (int(r["won"]) + int(r["lost"])) * 100.0),
                    1,
                )
                if (int(r["won"]) + int(r["lost"])) > 0
                else 0.0,
            }
            for _, r in eb.iterrows()
        ]

    lead_turnover["employee_breakdown"] = emp_breakdown

    return lead_turnover
