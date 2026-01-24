# clients/jc_mechanical/kpi_parts/kpi_quality.py

from .helpers import CENTRAL_TZ, _is_dfo_job
import pandas as pd


def compute(ctx):
    df_completed = ctx["df_completed"]
    completed_jobs = ctx["completed_jobs"]

    first_time_completed = int((df_completed["num_appointments"] == 1).sum()) if completed_jobs > 0 else 0
    repeat_visit_completed = int((df_completed["num_appointments"] >= 2).sum()) if completed_jobs > 0 else 0

    first_time_completion_pct = round((first_time_completed / completed_jobs) * 100.0, 2) if completed_jobs > 0 else 0.0
    repeat_visit_pct = round((repeat_visit_completed / completed_jobs) * 100.0, 2) if completed_jobs > 0 else 0.0
    first_time_completion_target = 85

    repeat_jobs_df = df_completed[df_completed["num_appointments"] >= 2].copy().sort_values("completed_at", ascending=False)

    def _fmt_dt_central(dt):
        if pd.isna(dt):
            return ""
        try:
            return dt.tz_convert(CENTRAL_TZ).strftime("%b %d, %Y %I:%M %p")
        except Exception:
            return str(dt)

    repeat_jobs_df["completed_at_central"] = repeat_jobs_df["completed_at"].apply(_fmt_dt_central)

    repeat_jobs_table = repeat_jobs_df[[
        "job_id", "customer_name", "work_status", "num_appointments", "completed_at_central", "description"
    ]].head(25).to_dict(orient="records")

    df_dfo = df_completed[df_completed["tags_norm"].apply(_is_dfo_job)].copy()
    dfo_count = int(len(df_dfo))
    dfo_pct = round((dfo_count / completed_jobs) * 100.0, 2) if completed_jobs > 0 else 0.0

    if dfo_pct < 5:
        dfo_status_color = "success"
    elif dfo_pct < 10:
        dfo_status_color = "warning"
    else:
        dfo_status_color = "danger"

    dfo_monthly = []
    if not df_dfo.empty:
        df_dfo_m = df_dfo.copy()
        df_dfo_m["month"] = df_dfo_m["completed_at"].dt.to_period("M")

        df_total_m = df_completed.copy()
        df_total_m["month"] = df_total_m["completed_at"].dt.to_period("M")

        m_dfo = df_dfo_m.groupby("month").size()
        m_total = df_total_m.groupby("month").size()

        for month in m_total.index.sort_values():
            dfo_cnt = int(m_dfo.get(month, 0))
            total_cnt = int(m_total.get(month, 0))
            pct = round((dfo_cnt / total_cnt) * 100.0, 1) if total_cnt > 0 else 0.0
            dfo_monthly.append({
                "month": str(month),
                "dfo_count": dfo_cnt,
                "total_count": total_cnt,
                "dfo_pct": pct,
            })

    return {
        "first_time_completion_pct": first_time_completion_pct,
        "first_time_completion_target": first_time_completion_target,
        "first_time_completed": first_time_completed,
        "repeat_visit_completed": repeat_visit_completed,
        "completed_jobs": completed_jobs,
        "repeat_visit_pct": repeat_visit_pct,
        "repeat_jobs": repeat_jobs_table,
        "dfo_pct": dfo_pct,
        "dfo_count": dfo_count,
        "dfo_status_color": dfo_status_color,
        "dfo_monthly": dfo_monthly,
    }
