# clients/jc_mechanical/kpi_parts/kpi_revenue.py

from __future__ import annotations

import pandas as pd
from .helpers import (
    map_category_from_tags,
    format_currency,
    largest_remainder_int_percentages,
    start_of_year_utc,
)


def compute(ctx: dict) -> dict:
    df_invoices: pd.DataFrame = ctx.get("df_invoices", pd.DataFrame()).copy()
    df_jobs: pd.DataFrame = ctx.get("df_jobs", pd.DataFrame()).copy()

    start_utc = ctx.get("start_of_year_utc") or start_of_year_utc()

    # -----------------------------------
    # Revenue (YTD) – billed invoices only
    # -----------------------------------
    billed_statuses = {"paid", "partial", "open", "past due", "unpaid"}  # keep consistent with your current logic
    df_rev = df_invoices[df_invoices["status"].astype(str).str.lower().isin(billed_statuses)].copy()

    if not df_rev.empty:
        df_rev["invoice_date_dt"] = pd.to_datetime(df_rev.get("invoice_date"), errors="coerce", utc=True)
        df_rev = df_rev[df_rev["invoice_date_dt"] >= start_utc].copy()
    else:
        df_rev["invoice_date_dt"] = pd.NaT

    if not df_rev.empty and not df_jobs.empty:
        df_rev = df_rev.merge(df_jobs[["job_id", "tags_norm"]], how="left", on="job_id")

    df_rev["tags_norm"] = df_rev.get("tags_norm", "").fillna("")
    df_rev["category"] = df_rev["tags_norm"].apply(map_category_from_tags)

    df_rev["amount_dollars"] = pd.to_numeric(df_rev["amount"], errors="coerce").fillna(0.0).astype(float)

    total_revenue_ytd = float(df_rev["amount_dollars"].sum()) if not df_rev.empty else 0.0
    total_revenue_ytd_display = format_currency(total_revenue_ytd)

    breakdown = (
        df_rev.groupby("category", dropna=False)["amount_dollars"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"amount_dollars": "revenue"})
    )

    revenue_breakdown_ytd = []
    if not breakdown.empty:
        total_breakdown = float(breakdown["revenue"].sum())
        rows = []
        for _, row in breakdown.iterrows():
            rev = float(row["revenue"])
            cat = str(row["category"]).strip() or "Other / Unclassified"
            pct_raw = (rev / total_breakdown * 100.0) if total_breakdown > 0 else 0.0
            rows.append({"category": cat, "revenue": rev, "pct_raw": pct_raw})

        pct_ints = largest_remainder_int_percentages([r["pct_raw"] for r in rows])
        for i, r in enumerate(rows):
            revenue_breakdown_ytd.append(
                {
                    "category": r["category"],
                    "revenue": r["revenue"],
                    "revenue_display": format_currency(r["revenue"]),
                    "pct_of_total": float(r["pct_raw"]),          # <-- add this (template expects it)
                    "pct_of_total_int": int(pct_ints[i]) if i < len(pct_ints) else int(round(r["pct_raw"])),
                    "pct_raw": float(r["pct_raw"]),
                }
            )


    # -----------------------------------
    # Average Ticket (overall, YTD, billed invoices)
    # -----------------------------------
    invoice_count = int(len(df_rev))
    average_ticket_threshold = 500.0  # keep your current threshold; update if you change business rule

    average_ticket_value = (total_revenue_ytd / invoice_count) if invoice_count > 0 else 0.0
    average_ticket_display = format_currency(average_ticket_value)
    average_ticket_status = "success" if average_ticket_value >= average_ticket_threshold else "danger"

    # -----------------------------------
    # Average Ticket split: Service vs Install (by tags on job)
    # -----------------------------------
    service_revenue = 0.0
    install_revenue = 0.0
    service_invoice_count = 0
    install_invoice_count = 0

    if not df_rev.empty:
        # Treat anything tagged "Install" (via category mapping) as install, else service bucket
        is_install = df_rev["category"].astype(str).str.contains("Install", case=False, na=False)

        install_revenue = float(df_rev.loc[is_install, "amount_dollars"].sum())
        service_revenue = float(df_rev.loc[~is_install, "amount_dollars"].sum())

        install_invoice_count = int(is_install.sum())
        service_invoice_count = int((~is_install).sum())

    service_avg_ticket = (service_revenue / service_invoice_count) if service_invoice_count > 0 else 0.0
    install_avg_ticket = (install_revenue / install_invoice_count) if install_invoice_count > 0 else 0.0

    service_avg_ticket_display = format_currency(service_avg_ticket)
    install_avg_ticket_display = format_currency(install_avg_ticket)

    service_avg_ticket_status = "success" if service_avg_ticket >= average_ticket_threshold else "danger"
    install_avg_ticket_status = "success" if install_avg_ticket >= average_ticket_threshold else "danger"

    return {
        "total_revenue_ytd": total_revenue_ytd,
        "total_revenue_ytd_display": total_revenue_ytd_display,
        "revenue_breakdown_ytd": revenue_breakdown_ytd,
        "average_ticket_value": average_ticket_value,
        "average_ticket_display": average_ticket_display,
        "average_ticket_status": average_ticket_status,
        "average_ticket_threshold": average_ticket_threshold,
        "invoice_count": invoice_count,
        "service_avg_ticket": service_avg_ticket,
        "service_avg_ticket_display": service_avg_ticket_display,
        "service_avg_ticket_status": service_avg_ticket_status,
        "service_invoice_count": service_invoice_count,
        "install_avg_ticket": install_avg_ticket,
        "install_avg_ticket_display": install_avg_ticket_display,
        "install_avg_ticket_status": install_avg_ticket_status,
        "install_invoice_count": install_invoice_count,
    }
