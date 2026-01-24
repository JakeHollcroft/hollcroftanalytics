# clients/jc_mechanical/kpi_parts/kpi_club.py

import pandas as pd

# Per your notes: < 50% is red
CLUB_CONVERSION_TARGET = 50.0


def _status_from_threshold(value_pct: float, target_pct: float) -> str:
    try:
        v = float(value_pct)
    except Exception:
        v = 0.0
    return "success" if v >= float(target_pct) else "danger"


def _contains_any(text: str, keywords: list[str]) -> bool:
    if text is None:
        return False
    t = str(text).lower()
    if not t.strip():
        return False
    return any((kw and kw.strip() and kw.lower() in t) for kw in keywords)


def compute(ctx):
    """
    Club Agreement Conversion (YTD) — Wizard Club

    IMPORTANT: This KPI is independent of revenue cards/filters.
    - Population is based on COMPLETED jobs YTD (from jobs table), filtered to SERVICE calls only (exclude installs).
    - Club signal sources (OR):
        1) Tags (rare, but include it)
        2) Job description
        3) Invoice item names (strong signal)

    Output keys match the dashboard HTML card:
      - club_conversion_pct_display
      - eligible_service_calls_ytd
      - club_calls_ytd
      - nonclub_service_calls_ytd
      - club_calls_from_tags / club_calls_from_description / club_calls_from_invoice_items
      - pct_from_tags / pct_from_description / pct_from_invoice_items  (numbers without % sign)
      - club_keywords (list)
      - club_monthly (list[dict])
    """

    start_of_year_utc = ctx.get("start_of_year_utc")
    df_completed = ctx.get("df_completed")
    df_invoices = ctx.get("df_invoices")
    df_invoice_items = ctx.get("df_invoice_items")

    club_keywords = [
        "wizard club",
        "wizard",
        "club",
        "membership",
        "member",
        "maintenance plan",
        "service plan",
        "agreement",
    ]

    # Defensive defaults
    if df_completed is None or df_completed.empty:
        return {
            "club_conversion_pct": 0.0,
            "club_conversion_pct_display": "0.00%",
            "club_conversion_status": _status_from_threshold(0.0, CLUB_CONVERSION_TARGET),
            "eligible_service_calls_ytd": 0,
            "club_calls_ytd": 0,
            "nonclub_service_calls_ytd": 0,
            "club_calls_from_tags": 0,
            "club_calls_from_description": 0,
            "club_calls_from_invoice_items": 0,
            "pct_from_tags": 0.0,
            "pct_from_description": 0.0,
            "pct_from_invoice_items": 0.0,
            "club_keywords": club_keywords,
            "club_monthly": [],
            "club_conversion_target": CLUB_CONVERSION_TARGET,
        }

    dfx = df_completed.copy()

    # Ensure needed columns exist
    for col in ["job_id", "tags_norm", "tags", "description", "completed_at"]:
        if col not in dfx.columns:
            dfx[col] = pd.NA

    # Normalize tags_norm if missing/empty
    if "tags_norm" not in dfx.columns or dfx["tags_norm"].isna().all():
        # fall back to raw tags string
        dfx["tags_norm"] = dfx["tags"].fillna("").astype(str).str.strip().str.lower()
    else:
        dfx["tags_norm"] = dfx["tags_norm"].fillna("").astype(str).str.strip().str.lower()

    # Service calls only: treat anything with "install" in tags_norm as install-like.
    dfx["is_install_like"] = dfx["tags_norm"].str.contains("install", case=False, na=False)

    df_service = dfx[~dfx["is_install_like"]].copy()
    df_service["job_id"] = df_service["job_id"].astype(str)
    df_service = df_service[df_service["job_id"].str.strip() != ""].copy()

    eligible_service_calls = int(df_service["job_id"].nunique()) if not df_service.empty else 0

    # -----------------------------
    # Signal 1: TAGS (rare)
    # -----------------------------
    df_service["club_from_tags"] = df_service["tags_norm"].apply(lambda s: _contains_any(s, club_keywords))

    # -----------------------------
    # Signal 2: DESCRIPTION
    # -----------------------------
    df_service["desc_norm"] = df_service["description"].fillna("").astype(str).str.strip().str.lower()
    df_service["club_from_description"] = df_service["desc_norm"].apply(lambda s: _contains_any(s, club_keywords))

    # -----------------------------
    # Signal 3: INVOICE ITEMS
    # -----------------------------
    job_ids_from_invoice_items = set()

    if (
        start_of_year_utc is not None
        and df_invoices is not None and not df_invoices.empty
        and df_invoice_items is not None and not df_invoice_items.empty
    ):
        inv = df_invoices.copy()
        it = df_invoice_items.copy()

        # Ensure columns exist
        for col in ["invoice_id", "job_id", "invoice_date_dt", "invoice_date"]:
            if col not in inv.columns:
                inv[col] = pd.NA

        # Ensure datetime is usable; DO NOT filter by invoice status here (this KPI is independent of revenue filters)
        if "invoice_date_dt" not in inv.columns or inv["invoice_date_dt"].isna().all():
            inv["invoice_date_dt"] = pd.to_datetime(inv["invoice_date"], errors="coerce", utc=True)

        inv_ytd = inv[inv["invoice_date_dt"].notna() & (inv["invoice_date_dt"] >= start_of_year_utc)].copy()
        inv_ytd["invoice_id"] = inv_ytd["invoice_id"].astype(str)

        if not inv_ytd.empty and "invoice_id" in it.columns:
            it["invoice_id"] = it["invoice_id"].astype(str)
            it["name_norm"] = it.get("name", "").fillna("").astype(str).str.strip().str.lower()

            joined = it.merge(
                inv_ytd[["invoice_id", "job_id"]],
                on="invoice_id",
                how="inner",
                suffixes=("_item", "_inv"),
            )

            # Use invoice job_id as source of truth
            joined["job_id"] = joined.get("job_id", pd.NA)
            joined["job_id"] = joined["job_id"].astype(str)
            joined = joined[joined["job_id"].str.strip() != ""].copy()

            joined["club_item"] = joined["name_norm"].apply(lambda s: _contains_any(s, club_keywords))
            joined = joined[joined["club_item"] == True].copy()

            job_ids_from_invoice_items = set(joined["job_id"].astype(str).tolist())

    df_service["club_from_invoice_items"] = df_service["job_id"].astype(str).isin(job_ids_from_invoice_items)

    # -----------------------------
    # Final club flag + source counts
    # -----------------------------
    df_service["is_club_call"] = (
        df_service["club_from_tags"]
        | df_service["club_from_description"]
        | df_service["club_from_invoice_items"]
    )

    club_calls = int(df_service[df_service["is_club_call"] == True]["job_id"].nunique()) if not df_service.empty else 0
    nonclub_calls = int(max(eligible_service_calls - club_calls, 0))

    # unique-job counts by source (not mutually exclusive)
    club_from_tags = int(df_service[df_service["club_from_tags"] == True]["job_id"].nunique()) if not df_service.empty else 0
    club_from_desc = int(df_service[df_service["club_from_description"] == True]["job_id"].nunique()) if not df_service.empty else 0
    club_from_items = int(df_service[df_service["club_from_invoice_items"] == True]["job_id"].nunique()) if not df_service.empty else 0

    pct = (club_calls / eligible_service_calls * 100.0) if eligible_service_calls > 0 else 0.0
    pct_display = f"{pct:.2f}%"
    status = _status_from_threshold(pct, CLUB_CONVERSION_TARGET)

    pct_from_tags = (club_from_tags / eligible_service_calls * 100.0) if eligible_service_calls > 0 else 0.0
    pct_from_desc = (club_from_desc / eligible_service_calls * 100.0) if eligible_service_calls > 0 else 0.0
    pct_from_items = (club_from_items / eligible_service_calls * 100.0) if eligible_service_calls > 0 else 0.0

    # -----------------------------
    # Monthly trend (YTD), based on completed_at month
    # -----------------------------
    club_monthly = []
    if "completed_at" in df_service.columns:
        completed_dt = pd.to_datetime(df_service["completed_at"], errors="coerce", utc=True)
        df_m = df_service.copy()
        df_m["completed_at_dt"] = completed_dt
        df_m = df_m[df_m["completed_at_dt"].notna()].copy()

        if not df_m.empty:
            df_m["month"] = df_m["completed_at_dt"].dt.to_period("M").astype(str)

            rows = []
            for month, g in df_m.groupby("month"):
                sc = int(g["job_id"].nunique())
                cc = int(g[g["is_club_call"] == True]["job_id"].nunique())
                conv = (cc / sc * 100.0) if sc > 0 else 0.0
                rows.append(
                    {
                        "month": month,
                        "service_calls": sc,
                        "club_calls": cc,
                        "club_conversion_pct": float(f"{conv:.2f}"),
                        "club_conversion_pct_display": f"{conv:.2f}%",
                    }
                )

            club_monthly = sorted(rows, key=lambda r: r["month"])

    return {
        "club_conversion_pct": float(f"{pct:.2f}"),
        "club_conversion_pct_display": pct_display,
        "club_conversion_status": status,

        "eligible_service_calls_ytd": int(eligible_service_calls),
        "club_calls_ytd": int(club_calls),
        "nonclub_service_calls_ytd": int(nonclub_calls),

        "club_calls_from_tags": int(club_from_tags),
        "club_calls_from_description": int(club_from_desc),
        "club_calls_from_invoice_items": int(club_from_items),

        # numbers (HTML adds % sign)
        "pct_from_tags": float(f"{pct_from_tags:.2f}"),
        "pct_from_description": float(f"{pct_from_desc:.2f}"),
        "pct_from_invoice_items": float(f"{pct_from_items:.2f}"),

        "club_keywords": club_keywords,
        "club_monthly": club_monthly,

        "club_conversion_target": CLUB_CONVERSION_TARGET,
    }
