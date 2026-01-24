# clients/jc_mechanical/kpi_parts/kpi_estimates.py

from __future__ import annotations

import pandas as pd
from .helpers import format_currency


def compute(ctx: dict) -> dict:
    df_estimates: pd.DataFrame = ctx.get("df_estimates", pd.DataFrame()).copy()
    df_estimate_options: pd.DataFrame = ctx.get("df_estimate_options", pd.DataFrame()).copy()
    df_estimate_emps: pd.DataFrame = ctx.get("df_estimate_emps", pd.DataFrame()).copy()

    start_utc = ctx.get("start_of_year_utc")

    # Default output shape (safe even if tables are empty)
    out = {
        "estimates_created_ytd": 0,
        "estimates_approved_ytd": 0,
        "estimates_declined_ytd": 0,
        "estimates_pending_ytd": 0,
        "estimates_total_value_ytd": 0.0,
        "estimates_total_value_ytd_display": "$0",
    }

    if df_estimates.empty:
        return out

    # Defensive columns
    for col in ["estimate_id", "created_at", "status", "lead_source"]:
        if col not in df_estimates.columns:
            df_estimates[col] = None

    df_estimates["created_at_dt"] = pd.to_datetime(df_estimates["created_at"], errors="coerce", utc=True)
    if start_utc is not None:
        df_estimates = df_estimates[df_estimates["created_at_dt"] >= start_utc].copy()

    out["estimates_created_ytd"] = int(len(df_estimates))

    # Option statuses (best-effort)
    # If your system defines approvals on estimate_options, use it; otherwise fallback to estimates.status
    if not df_estimate_options.empty:
        for col in ["estimate_id", "approval_status", "status", "total_amount"]:
            if col not in df_estimate_options.columns:
                df_estimate_options[col] = None

        df_estimate_options["total_amount_dollars"] = pd.to_numeric(
            df_estimate_options["total_amount"], errors="coerce"
        ).fillna(0.0).astype(float)

        opt = df_estimate_options.copy()
        opt = opt[opt["estimate_id"].isin(df_estimates["estimate_id"])].copy()

        def _norm(x):
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return ""
            return str(x).strip().lower()

        def _bucket(approval_status_val, status_val) -> str:
            a = _norm(approval_status_val)
            s = _norm(status_val)
            won_keys = {"approved", "accepted", "won", "sold", "selected"}
            lost_keys = {"declined", "rejected", "lost", "expired", "canceled", "cancelled", "void", "deleted"}

            if a in won_keys or s in won_keys:
                return "won"
            if a in lost_keys or s in lost_keys:
                return "lost"
            return "pending"

        opt["bucket"] = opt.apply(lambda r: _bucket(r.get("approval_status"), r.get("status")), axis=1)

        out["estimates_approved_ytd"] = int((opt["bucket"] == "won").sum())
        out["estimates_declined_ytd"] = int((opt["bucket"] == "lost").sum())
        out["estimates_pending_ytd"] = int((opt["bucket"] == "pending").sum())

        out["estimates_total_value_ytd"] = float(opt["total_amount_dollars"].sum())
        out["estimates_total_value_ytd_display"] = format_currency(out["estimates_total_value_ytd"])

    else:
        # Fallback to estimate status only
        s = df_estimates["status"].astype(str).str.lower()
        out["estimates_approved_ytd"] = int(s.isin(["approved", "accepted", "won", "sold"]).sum())
        out["estimates_declined_ytd"] = int(s.isin(["declined", "rejected", "lost", "expired", "canceled", "cancelled"]).sum())
        out["estimates_pending_ytd"] = out["estimates_created_ytd"] - out["estimates_approved_ytd"] - out["estimates_declined_ytd"]

    return out
