# clients/jc_mechanical/kpi_parts/kpi_revenue.py

import pandas as pd
from .helpers import _format_currency, _largest_remainder_int_percentages


AVG_TICKET_TARGET = 450.0


def _status_from_threshold(value: float, target: float) -> str:
    try:
        v = float(value)
    except Exception:
        v = 0.0
    return "success" if v >= target else "danger"


def compute(ctx):
    df_rev = ctx["df_rev"]

    # -----------------------------
    # Total revenue + breakdown (existing)
    # -----------------------------
    total_revenue_ytd = float(df_rev["amount_dollars"].sum()) if not df_rev.empty else 0.0
    total_revenue_ytd_display = _format_currency(total_revenue_ytd)

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

        pct_ints = _largest_remainder_int_percentages([r["pct_raw"] for r in rows])
        max_rev = max((r["revenue"] for r in rows), default=0.0)

        for r, pct_int in zip(rows, pct_ints):
            pct_of_max = (r["revenue"] / max_rev * 100.0) if max_rev > 0 else 0.0
            pct_of_max = max(0.0, min(100.0, pct_of_max))
            revenue_breakdown_ytd.append({
                "category": r["category"],
                "revenue": r["revenue"],
                "revenue_display": _format_currency(r["revenue"]),
                "pct_of_total": float(pct_int),
                "pct_of_total_display": f"{int(pct_int)}%",
                "pct_of_max": pct_of_max,
            })

    # -----------------------------
    # Average Ticket (YTD) (NEW — matches template keys)
    # -----------------------------
    invoice_count = 0
    average_ticket = 0.0

    if not df_rev.empty:
        # prefer invoice_id if present; else fall back to job_id
        if "invoice_id" in df_rev.columns:
            invoice_count = int(df_rev["invoice_id"].nunique())
        else:
            invoice_count = int(df_rev["job_id"].nunique())

        total_amt = float(df_rev["amount_dollars"].sum())
        average_ticket = (total_amt / invoice_count) if invoice_count > 0 else 0.0

    average_ticket_display = _format_currency(average_ticket)
    average_ticket_status = _status_from_threshold(average_ticket, AVG_TICKET_TARGET)

    # split (service/install) if is_install exists
    service_avg_ticket = 0.0
    install_avg_ticket = 0.0
    service_invoice_count = 0
    install_invoice_count = 0

    if (not df_rev.empty) and ("is_install" in df_rev.columns):
        df_service = df_rev[df_rev["is_install"] == False].copy()
        df_install = df_rev[df_rev["is_install"] == True].copy()

        def _avg_and_count(dfx: pd.DataFrame):
            if dfx.empty:
                return 0.0, 0
            cnt = int(dfx["invoice_id"].nunique()) if "invoice_id" in dfx.columns else int(dfx["job_id"].nunique())
            tot = float(dfx["amount_dollars"].sum())
            return (tot / cnt) if cnt > 0 else 0.0, cnt

        service_avg_ticket, service_invoice_count = _avg_and_count(df_service)
        install_avg_ticket, install_invoice_count = _avg_and_count(df_install)

    return {
        "total_revenue_ytd": total_revenue_ytd,
        "total_revenue_ytd_display": total_revenue_ytd_display,
        "revenue_breakdown_ytd": revenue_breakdown_ytd,

        # legacy fallback keys (template uses these)
        "average_ticket": average_ticket,
        "average_ticket_display": average_ticket_display,
        "average_ticket_status": average_ticket_status,
        "invoice_count": invoice_count,
        "avg_ticket_target": AVG_TICKET_TARGET,

        # split keys (template prefers these when defined)
        "service_avg_ticket": service_avg_ticket,
        "service_avg_ticket_display": _format_currency(service_avg_ticket),
        "service_avg_ticket_status": _status_from_threshold(service_avg_ticket, AVG_TICKET_TARGET),
        "service_invoice_count": service_invoice_count,

        "install_avg_ticket": install_avg_ticket,
        "install_avg_ticket_display": _format_currency(install_avg_ticket),
        "install_avg_ticket_status": _status_from_threshold(install_avg_ticket, AVG_TICKET_TARGET),
        "install_invoice_count": install_invoice_count,
    }
