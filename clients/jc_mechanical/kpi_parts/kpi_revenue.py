# clients/jc_mechanical/kpi_parts/kpi_revenue.py

from .helpers import _map_category_from_tags, _format_currency, _largest_remainder_int_percentages, _to_dt_utc


def compute(ctx):
    df_rev = ctx["df_rev"]
    start_of_year_utc = ctx["start_of_year_utc"]

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

    return {
        "total_revenue_ytd": total_revenue_ytd,
        "total_revenue_ytd_display": total_revenue_ytd_display,
        "revenue_breakdown_ytd": revenue_breakdown_ytd,
    }
