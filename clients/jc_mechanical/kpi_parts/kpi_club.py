# clients/jc_mechanical/kpi_parts/kpi_club.py

import pandas as pd

CLUB_CONVERSION_TARGET = 50.0  # per your notes: < 50% red


def _status_from_threshold(value_pct: float, target_pct: float) -> str:
    try:
        v = float(value_pct)
    except Exception:
        v = 0.0
    return "success" if v >= float(target_pct) else "danger"


def _contains_any(text: str, keywords: list[str]) -> bool:
    if not text:
        return False
    t = str(text).lower()
    return any((kw and kw.strip() and kw.lower() in t) for kw in keywords)


def compute(ctx):
    """
    Wizard Club Conversion (YTD)

    Population:
      - Completed jobs YTD
      - Service calls only (exclude installs) using tags_norm category inference

    Club-member service call signal (OR):
      - invoice_items.name contains club keywords (primary)
      - jobs.description contains club keywords (fallback)

    Returns:
      - club_conversion_pct_ytd + display + status
      - club_service_calls_ytd (count)
      - total_service_calls_ytd (count)
    """
    start_of_year_utc = ctx["start_of_year_utc"]
    df_completed = ctx["df_completed"]
    df_invoices = ctx["df_invoices"]
    df_invoice_items = ctx["df_invoice_items"]

    # Keywords used in your audit (kept consistent)
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
            "club_conversion_pct_ytd": 0.0,
            "club_conversion_pct_ytd_display": "0.00%",
            "club_conversion_status": _status_from_threshold(0.0, CLUB_CONVERSION_TARGET),
            "club_service_calls_ytd": 0,
            "total_service_calls_ytd": 0,
            "club_conversion_target": CLUB_CONVERSION_TARGET,
        }

    dfx = df_completed.copy()

    # Ensure needed columns exist
    for col in ["job_id", "tags_norm", "description", "completed_at"]:
        if col not in dfx.columns:
            dfx[col] = ""

    # Service calls only: infer install using tags_norm content
    # We treat anything with "install" in tags_norm as install-like.
    # (This matches how your tags are structured: Residential Install / Commercial Install)
    dfx["tags_norm"] = dfx["tags_norm"].fillna("").astype(str)
    dfx["is_install_like"] = dfx["tags_norm"].str.contains("install", case=False, na=False)

    df_service = dfx[~dfx["is_install_like"]].copy()
    total_service_calls = int(df_service["job_id"].nunique()) if not df_service.empty else 0

    # Description-based signal (fallback)
    df_service["desc_norm"] = df_service["description"].fillna("").astype(str).str.strip().str.lower()
    df_service["club_from_description"] = df_service["desc_norm"].apply(lambda s: _contains_any(s, club_keywords))

    # Invoice-item signal (primary)
    # Use invoices within YTD window and then join to invoice_items via invoice_id.
    job_ids_from_invoice_items = set()
    if df_invoices is not None and not df_invoices.empty and df_invoice_items is not None and not df_invoice_items.empty:
        inv = df_invoices.copy()
        it = df_invoice_items.copy()

        # Ensure columns exist
        for col in ["invoice_id", "job_id", "invoice_date_dt", "invoice_date"]:
            if col not in inv.columns:
                inv[col] = pd.NA

        if "invoice_date_dt" not in inv.columns or inv["invoice_date_dt"].isna().all():
            # fall back to parsing raw invoice_date if dt not present
            inv["invoice_date_dt"] = pd.to_datetime(inv["invoice_date"], errors="coerce", utc=True)

        inv_ytd = inv[inv["invoice_date_dt"].notna() & (inv["invoice_date_dt"] >= start_of_year_utc)].copy()

        if not inv_ytd.empty and "invoice_id" in it.columns:
            # normalize item name
            it["name_norm"] = it.get("name", "").fillna("").astype(str).str.strip().str.lower()

            joined = it.merge(
                inv_ytd[["invoice_id", "job_id"]],
                on="invoice_id",
                how="inner",
                suffixes=("_item", "_inv"),
            )

            # stable job_id (prefer invoice job_id)
            if "job_id_inv" in joined.columns and "job_id_item" in joined.columns:
                joined["job_id"] = joined["job_id_inv"].fillna(joined["job_id_item"])
            elif "job_id" not in joined.columns:
                joined["job_id"] = joined.get("job_id_inv", joined.get("job_id_item", pd.NA))

            joined["club_item"] = joined["name_norm"].apply(lambda s: _contains_any(s, club_keywords))
            joined = joined[joined["club_item"] == True].copy()
            joined = joined[joined["job_id"].notna() & (joined["job_id"].astype(str).str.strip() != "")]

            job_ids_from_invoice_items = set(joined["job_id"].astype(str).tolist())

    df_service["club_from_invoice_items"] = df_service["job_id"].astype(str).isin(job_ids_from_invoice_items)

    # Final club flag: invoice-items OR description
    df_service["is_club_call"] = df_service["club_from_invoice_items"] | df_service["club_from_description"]
    club_service_calls = int(df_service[df_service["is_club_call"] == True]["job_id"].nunique()) if not df_service.empty else 0

    pct = (club_service_calls / total_service_calls * 100.0) if total_service_calls > 0 else 0.0
    pct_display = f"{pct:.2f}%"
    status = _status_from_threshold(pct, CLUB_CONVERSION_TARGET)

    return {
        "club_conversion_pct_ytd": float(pct),
        "club_conversion_pct_ytd_display": pct_display,
        "club_conversion_status": status,
        "club_service_calls_ytd": club_service_calls,
        "total_service_calls_ytd": total_service_calls,
        "club_conversion_target": CLUB_CONVERSION_TARGET,
    }
