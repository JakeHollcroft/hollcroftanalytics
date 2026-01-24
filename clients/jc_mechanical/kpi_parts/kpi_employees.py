# clients/jc_mechanical/kpi_parts/kpi_employees.py

from __future__ import annotations

import pandas as pd
from .helpers import format_currency, is_callback_job


def compute(ctx: dict) -> dict:
    df_completed: pd.DataFrame = ctx.get("df_completed", pd.DataFrame()).copy()
    df_invoices: pd.DataFrame = ctx.get("df_invoices", pd.DataFrame()).copy()

    df_employees: pd.DataFrame = ctx.get("df_employees", pd.DataFrame()).copy()
    df_job_emps: pd.DataFrame = ctx.get("df_job_emps", pd.DataFrame()).copy()
    df_job_appts: pd.DataFrame = ctx.get("df_job_appts", pd.DataFrame()).copy()
    df_invoice_items: pd.DataFrame = ctx.get("df_invoice_items", pd.DataFrame()).copy()

    start_utc = ctx.get("start_of_year_utc")
    if start_utc is not None and "completed_at" in df_completed.columns:
        df_completed = df_completed[df_completed["completed_at"] >= start_utc].copy()

    employee_cards = []

    # Gross profit per hour (YTD)
    # PENDING: intentionally not calculating GP/hr until business rules are confirmed.
    gp_per_hour_pending = True
    gp_per_hour_target = 0.0  # placeholder
    df_emp_gp = pd.DataFrame(columns=["employee_id", "gross_profit_per_hour_ytd"])

    # Build employee dimension: prefer employees table, fallback to job_employees
    df_emp_dim = df_employees.copy()
    if df_emp_dim.empty:
        df_emp_dim = (
            df_job_emps[["employee_id", "first_name", "last_name", "role", "avatar_url", "color_hex"]]
            .drop_duplicates()
            .copy()
        )

    # Defensive columns
    for col in ["employee_id", "first_name", "last_name", "role", "avatar_url", "color_hex"]:
        if col not in df_emp_dim.columns:
            df_emp_dim[col] = ""

    # If we can map jobs -> employees (job_employees table), compute per-employee stats
    if not df_job_emps.empty and not df_completed.empty:
        # Completed jobs per employee
        df_ce = df_completed.merge(df_job_emps[["job_id", "employee_id"]], how="left", on="job_id")
        jobs_by_emp = df_ce.groupby("employee_id").size().rename("completed_jobs_ytd").reset_index()

        # Callback % per employee (tag-driven)
        df_ce["is_callback"] = df_ce["tags_norm"].apply(is_callback_job)
        cb = df_ce.groupby("employee_id")["is_callback"].sum().rename("callbacks_ytd").reset_index()

        # Avg ticket per employee (join billed invoices to job -> employees)
        billed_statuses = {"paid", "partial", "open", "past due", "unpaid"}
        df_billed = df_invoices[df_invoices["status"].astype(str).str.lower().isin(billed_statuses)].copy()

        if not df_billed.empty:
            df_billed["amount_dollars"] = pd.to_numeric(df_billed["amount"], errors="coerce").fillna(0.0).astype(float)

            df_billed = df_billed.merge(df_job_emps[["job_id", "employee_id"]], how="left", on="job_id")
            rev_by_emp = (
                df_billed.groupby("employee_id")["amount_dollars"]
                .sum()
                .rename("revenue_ytd")
                .reset_index()
            )
            invcnt_by_emp = (
                df_billed.groupby("employee_id")
                .size()
                .rename("invoice_count_ytd")
                .reset_index()
            )
            df_emp_rev = rev_by_emp.merge(invcnt_by_emp, on="employee_id", how="outer").fillna(0)
            df_emp_rev["avg_ticket_ytd"] = df_emp_rev.apply(
                lambda r: (float(r["revenue_ytd"]) / float(r["invoice_count_ytd"])) if float(r["invoice_count_ytd"]) > 0 else 0.0,
                axis=1,
            )
        else:
            df_emp_rev = pd.DataFrame(columns=["employee_id", "revenue_ytd", "invoice_count_ytd", "avg_ticket_ytd"])

        # Appointment hours per employee (job_appointments)
        # Note: this is best-effort; if your appts table uses different columns, adjust here.
        appt_hours = pd.DataFrame(columns=["employee_id", "appointment_hours_ytd"])
        if not df_job_appts.empty:
            # Try a few common patterns
            for col in ["duration_minutes", "duration_min", "duration"]:
                if col in df_job_appts.columns:
                    df_job_appts[col] = pd.to_numeric(df_job_appts[col], errors="coerce").fillna(0.0)
                    df_job_appts["_hours"] = df_job_appts[col] / 60.0
                    break
            if "_hours" not in df_job_appts.columns:
                df_job_appts["_hours"] = 0.0

            if "employee_id" in df_job_appts.columns:
                appt_hours = (
                    df_job_appts.groupby("employee_id")["_hours"]
                    .sum()
                    .rename("appointment_hours_ytd")
                    .reset_index()
                )

        # Merge everything
        df_cards = df_emp_dim.merge(jobs_by_emp, on="employee_id", how="left")
        df_cards = df_cards.merge(cb, on="employee_id", how="left")
        df_cards = df_cards.merge(df_emp_rev[["employee_id", "avg_ticket_ytd"]], on="employee_id", how="left")
        df_cards = df_cards.merge(appt_hours, on="employee_id", how="left")
        df_cards = df_cards.merge(df_emp_gp, on="employee_id", how="left")

        df_cards["completed_jobs_ytd"] = pd.to_numeric(df_cards.get("completed_jobs_ytd", 0), errors="coerce").fillna(0).astype(int)
        df_cards["callbacks_ytd"] = pd.to_numeric(df_cards.get("callbacks_ytd", 0), errors="coerce").fillna(0).astype(int)
        df_cards["avg_ticket_ytd"] = pd.to_numeric(df_cards.get("avg_ticket_ytd", 0.0), errors="coerce").fillna(0.0).astype(float)
        df_cards["appointment_hours_ytd"] = pd.to_numeric(df_cards.get("appointment_hours_ytd", 0.0), errors="coerce").fillna(0.0).astype(float)
        df_cards["gross_profit_per_hour_ytd"] = pd.to_numeric(df_cards.get("gross_profit_per_hour_ytd", 0.0), errors="coerce").fillna(0.0).astype(float)

        # Callback %
        df_cards["callback_pct_ytd"] = df_cards.apply(
            lambda r: round((int(r["callbacks_ytd"]) / int(r["completed_jobs_ytd"]) * 100.0), 2)
            if int(r["completed_jobs_ytd"]) > 0
            else 0.0,
            axis=1,
        )

        # Build card list
        for _, r in df_cards.iterrows():
            employee_cards.append(
                {
                    "employee_id": r.get("employee_id", ""),
                    "name": f"{str(r.get('first_name','') or '').strip()} {str(r.get('last_name','') or '').strip()}".strip(),
                    "role": str(r.get("role", "") or "").strip(),
                    "avatar_url": str(r.get("avatar_url", "") or "").strip(),
                    "color_hex": str(r.get("color_hex", "") or "").strip(),
                    "completed_jobs_ytd": int(r.get("completed_jobs_ytd", 0)),
                    "callbacks_ytd": int(r.get("callbacks_ytd", 0)),
                    "callback_pct_ytd": float(r.get("callback_pct_ytd", 0.0)),
                    "avg_ticket_ytd": float(r.get("avg_ticket_ytd", 0.0)),
                    "avg_ticket_ytd_display": format_currency(float(r.get("avg_ticket_ytd", 0.0))),
                    "appointment_hours_ytd": float(r.get("appointment_hours_ytd", 0.0)),
                    "gross_profit_per_hour_ytd": float(r.get("gross_profit_per_hour_ytd", 0.0)),
                    "gross_profit_per_hour_ytd_display": format_currency(float(r.get("gross_profit_per_hour_ytd", 0.0))),
                }
            )

        # Sort by completed jobs desc
        employee_cards = sorted(employee_cards, key=lambda x: x.get("completed_jobs_ytd", 0), reverse=True)

    return {
        "employee_cards": employee_cards,
        "gp_per_hour_target": gp_per_hour_target,
        "gp_per_hour_pending": gp_per_hour_pending,
    }
