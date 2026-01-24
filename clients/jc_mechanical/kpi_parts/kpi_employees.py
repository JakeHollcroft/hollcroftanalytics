# clients/jc_mechanical/kpi_parts/kpi_employees.py

import pandas as pd

from .helpers import (
    CENTRAL_TZ,
    _ensure_columns,
    _format_currency,
    _is_callback_job,
    _to_dt_utc,
)


def compute(ctx):
    df_employees = ctx.get("df_employees", pd.DataFrame()).copy()
    df_job_emps = ctx.get("df_job_emps", pd.DataFrame()).copy()
    df_completed = ctx.get("df_completed", pd.DataFrame()).copy()
    df_rev = ctx.get("df_rev", pd.DataFrame()).copy()
    df_job_appts = ctx.get("df_job_appts", pd.DataFrame()).copy()

    employee_cards = []

    # Gross profit per hour (YTD)
    # PENDING: we are intentionally not calculating GP/hr until the business rules are confirmed.
    gp_per_hour_pending = True
    gp_per_hour_target = 0.0  # placeholder
    df_emp_gp = pd.DataFrame(columns=["employee_id", "gross_profit_per_hour_ytd"])

    # Build a canonical employee dimension (prefer employees table, fallback to job_employees)
    df_emp_dim = df_employees.copy()
    if df_emp_dim.empty:
        df_emp_dim = df_job_emps[["employee_id", "first_name", "last_name", "role", "avatar_url", "color_hex"]].drop_duplicates().copy()

    df_emp_dim = _ensure_columns(df_emp_dim, {
        "employee_id": "object",
        "first_name": "object",
        "last_name": "object",
        "role": "object",
        "avatar_url": "object",
        "color_hex": "object",
    })

    df_emp_dim["employee_name"] = (
        df_emp_dim["first_name"].fillna("").astype(str).str.strip()
        + " "
        + df_emp_dim["last_name"].fillna("").astype(str).str.strip()
    ).str.strip()
    df_emp_dim["employee_name"] = df_emp_dim["employee_name"].replace("", "Unknown")

    # Completed jobs per tech YTD (assigned_employees based)
    df_completed_jobs_emp = pd.DataFrame(columns=["employee_id", "completed_jobs_ytd"])
    if not df_completed.empty and not df_job_emps.empty:
        df_emp_completed = df_completed[["job_id", "tags_norm"]].merge(df_job_emps, how="inner", on="job_id")
        df_emp_completed = df_emp_completed.dropna(subset=["employee_id"])
        df_completed_jobs_emp = (
            df_emp_completed.groupby("employee_id")
            .agg(completed_jobs_ytd=("job_id", "nunique"))
            .reset_index()
        )

    # Avg ticket overall per tech YTD (invoice amounts attributed to all assigned techs on job)
    df_emp_atv = pd.DataFrame(columns=["employee_id", "avg_ticket_overall", "overall_invoice_count"])
    avg_ticket_threshold = 450.0
    if not df_rev.empty and not df_job_emps.empty:
        df_tmp = df_rev[["job_id", "amount_dollars", "invoice_date_dt"]].copy()
        df_tmp = df_tmp.merge(df_job_emps[["job_id", "employee_id"]], how="inner", on="job_id")
        df_tmp = df_tmp.dropna(subset=["employee_id"])

        a = (
            df_tmp.groupby("employee_id")
            .agg(
                overall_revenue=("amount_dollars", "sum"),
                overall_invoice_count=("job_id", "nunique"),
            )
            .reset_index()
        )
        a["avg_ticket_overall"] = a.apply(
            lambda r: (float(r["overall_revenue"]) / float(r["overall_invoice_count"])) if float(r["overall_invoice_count"]) > 0 else 0.0,
            axis=1,
        )
        df_emp_atv = a[["employee_id", "avg_ticket_overall", "overall_invoice_count"]].copy()

    # Callback % per tech YTD (callbacks tagged on completed jobs)
    df_emp_cb = pd.DataFrame(columns=["employee_id", "callback_jobs_ytd"])
    callback_target = 5.0
    if not df_completed.empty and not df_job_emps.empty:
        df_cb_jobs = df_completed[df_completed["tags_norm"].fillna("").apply(_is_callback_job)][["job_id"]].copy()
        if not df_cb_jobs.empty:
            df_cb_jobs = df_cb_jobs.merge(df_job_emps[["job_id", "employee_id"]], how="inner", on="job_id")
            df_cb_jobs = df_cb_jobs.dropna(subset=["employee_id"])
            df_emp_cb = (
                df_cb_jobs.groupby("employee_id")
                .agg(callback_jobs_ytd=("job_id", "nunique"))
                .reset_index()
            )

    # Appointment hours per tech YTD (from job_appointments)
    df_emp_hours = pd.DataFrame(columns=["employee_id", "appt_hours_ytd"])
    if not df_job_appts.empty:
        df_ap = df_job_appts.copy()

        if "start_at" in df_ap.columns:
            df_ap["start_at_dt"] = _to_dt_utc(df_ap["start_at"])
        else:
            df_ap["start_at_dt"] = pd.NaT

        if "end_at" in df_ap.columns:
            df_ap["end_at_dt"] = _to_dt_utc(df_ap["end_at"])
        else:
            df_ap["end_at_dt"] = pd.NaT

        if "duration_minutes" in df_ap.columns:
            df_ap["duration_minutes"] = pd.to_numeric(df_ap["duration_minutes"], errors="coerce").fillna(0.0)
        else:
            df_ap["duration_minutes"] = 0.0

        # Prefer duration_minutes; else compute from end-start if available
        df_ap["minutes"] = df_ap["duration_minutes"]
        mask_missing = (df_ap["minutes"] <= 0) & df_ap["start_at_dt"].notna() & df_ap["end_at_dt"].notna()
        df_ap.loc[mask_missing, "minutes"] = (
            (df_ap.loc[mask_missing, "end_at_dt"] - df_ap.loc[mask_missing, "start_at_dt"]).dt.total_seconds() / 60.0
        )

        df_ap["minutes"] = pd.to_numeric(df_ap["minutes"], errors="coerce").fillna(0.0)
        df_ap["hours"] = df_ap["minutes"] / 60.0

        if "employee_id" in df_ap.columns:
            df_emp_hours = (
                df_ap.groupby("employee_id")
                .agg(appt_hours_ytd=("hours", "sum"))
                .reset_index()
            )

    # $ / Day (service) per tech: use invoice_date converted to date, grouped to daily totals
    df_emp_daily = pd.DataFrame(columns=["employee_id", "daily_revenue_service"])
    if not df_rev.empty and not df_job_emps.empty:
        df_tmp = df_rev[["job_id", "amount_dollars", "invoice_date_dt", "is_install"]].copy()
        df_tmp = df_tmp.merge(df_job_emps[["job_id", "employee_id"]], how="inner", on="job_id")
        df_tmp = df_tmp.dropna(subset=["employee_id"])

        # service only
        df_tmp = df_tmp[df_tmp["is_install"] == False].copy()

        if not df_tmp.empty:
            df_tmp["inv_day_central"] = pd.to_datetime(df_tmp["invoice_date_dt"], errors="coerce").dt.date

            d = (
                df_tmp.groupby(["employee_id", "inv_day_central"])
                .agg(day_revenue=("amount_dollars", "sum"))
                .reset_index()
            )

            b = (
                d.groupby("employee_id")
                .agg(
                    total_service_revenue=("day_revenue", "sum"),
                    service_days=("inv_day_central", "nunique"),
                )
                .reset_index()
            )

            b["daily_revenue_service"] = b.apply(
                lambda r: (float(r["total_service_revenue"]) / float(r["service_days"])) if float(r["service_days"]) > 0 else 0.0,
                axis=1,
            )

            df_emp_daily = b[["employee_id", "daily_revenue_service"]].copy()

    # Assemble employee cards
    df_cards = df_emp_dim.copy()
    df_cards = df_cards.merge(df_completed_jobs_emp, how="left", on="employee_id")
    df_cards = df_cards.merge(df_emp_atv, how="left", on="employee_id")
    df_cards = df_cards.merge(df_emp_cb, how="left", on="employee_id")
    df_cards = df_cards.merge(df_emp_hours, how="left", on="employee_id")
    df_cards = df_cards.merge(df_emp_daily, how="left", on="employee_id")
    df_cards = df_cards.merge(df_emp_gp, how="left", on="employee_id")

    df_cards["completed_jobs_ytd"] = pd.to_numeric(df_cards.get("completed_jobs_ytd", 0), errors="coerce").fillna(0).astype(int)
    df_cards["avg_ticket_overall"] = pd.to_numeric(df_cards.get("avg_ticket_overall", 0.0), errors="coerce").fillna(0.0).astype(float)
    df_cards["overall_invoice_count"] = pd.to_numeric(df_cards.get("overall_invoice_count", 0), errors="coerce").fillna(0).astype(int)
    df_cards["callback_jobs_ytd"] = pd.to_numeric(df_cards.get("callback_jobs_ytd", 0), errors="coerce").fillna(0).astype(int)
    df_cards["appt_hours_ytd"] = pd.to_numeric(df_cards.get("appt_hours_ytd", 0.0), errors="coerce").fillna(0.0).astype(float)
    df_cards["daily_revenue_service"] = pd.to_numeric(df_cards.get("daily_revenue_service", 0.0), errors="coerce").fillna(0.0).astype(float)
    df_cards["gross_profit_per_hour_ytd"] = pd.to_numeric(df_cards.get("gross_profit_per_hour_ytd", 0.0), errors="coerce").fillna(0.0).astype(float)

    # callback pct/status
    df_cards["callback_pct_ytd"] = df_cards.apply(
        lambda r: (float(r["callback_jobs_ytd"]) / float(r["completed_jobs_ytd"]) * 100.0) if float(r["completed_jobs_ytd"]) > 0 else 0.0,
        axis=1,
    )

    for _, r in df_cards.iterrows():
        callback_pct = float(r["callback_pct_ytd"])
        if callback_pct <= callback_target:
            callback_status = "success"
        elif callback_pct <= callback_target * 2:
            callback_status = "warning"
        else:
            callback_status = "danger"

        # GP/hr placeholder (pending)
        gp_hr = float(r.get("gross_profit_per_hour_ytd", 0.0)) if not gp_per_hour_pending else 0.0
        gp_status = "secondary" if gp_per_hour_pending else ("success" if gp_hr >= gp_per_hour_target else "danger")

        employee_cards.append({
            "employee_id": r["employee_id"],
            "name": r["employee_name"] or "Unknown",
            "role": r["role"] or "Technician",
            "avatar_url": r["avatar_url"],
            "color_hex": r["color_hex"],

            "completed_jobs_ytd": int(r["completed_jobs_ytd"]),

            # Average ticket (overall)
            "avg_ticket_overall": float(r["avg_ticket_overall"]),
            "avg_ticket_overall_display": _format_currency(float(r["avg_ticket_overall"])),
            "avg_ticket_overall_status": "success" if float(r["avg_ticket_overall"]) >= avg_ticket_threshold else "danger",
            "overall_invoice_count": int(r["overall_invoice_count"]),

            # $ / Day (Service)
            "daily_revenue": float(r.get("daily_revenue_service", 0.0)),
            "daily_revenue_display": _format_currency(float(r.get("daily_revenue_service", 0.0))),
            "daily_revenue_status": "success" if float(r.get("daily_revenue_service", 0.0)) >= 1350.0 else "danger",

            # Callback
            "callback_jobs_ytd": int(r["callback_jobs_ytd"]),
            "callback_pct_ytd": float(callback_pct),
            "callback_pct_display": f"{callback_pct:.1f}%",
            "callback_status": callback_status,
            "callback_target": callback_target,

            # Hours (from appointments)
            "appt_hours_ytd": float(r["appt_hours_ytd"]),
            "appt_hours_ytd_display": f"{float(r['appt_hours_ytd']):.1f} hrs",

            # Gross profit per hour (approx)
            "gross_profit_per_hour_ytd": gp_hr,
            "gross_profit_per_hour_ytd_display": "Pending",
            "gp_per_hour_target": gp_per_hour_target,
            "gp_per_hour_pending": gp_per_hour_pending,
            "gp_per_hour_status": gp_status,
        })

    # -----------------------------------
    employee_cards.sort(key=lambda x: x.get("completed_jobs_ytd", 0), reverse=True)

    return {
        "employee_cards": employee_cards,
        "gp_per_hour_target": gp_per_hour_target,
        "gp_per_hour_pending": gp_per_hour_pending,
    }
