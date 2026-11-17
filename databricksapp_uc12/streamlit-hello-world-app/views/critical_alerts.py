"""Page 4: Critical Alerts - critical incidents from Gold and the alerts routed to Lakebase."""
import streamlit as st

from gridwatch import config, db, lakebase, theme

theme.page_header("Critical Alerts", "Critical incident notes from the Gold layer and the alerts stored in Lakebase.")

# ---- Gold: critical notes ------------------------------------------------------------------------------
gold = db.load(f"""
    SELECT window_id, window_start, asset_id, asset_type, substation_name, maintenance_status,
           triggered_metrics, likely_category, deviation, recommended_check, note
    FROM {db.T('gold_incident_notes')}
    WHERE upper(trim(severity)) = 'CRITICAL'
    ORDER BY window_start DESC""", what="critical notes")

# ---- Lakebase: routed alerts (a failure here must not break the Gold half of the page) -----------------
alerts, lb_error = None, None
try:
    alerts = lakebase.fetch_alerts()
except lakebase.LakebaseError as exc:
    lb_error = str(exc)

new_count = int((alerts["alert_status"] == "NEW").sum()) if alerts is not None and "alert_status" in alerts else None
missing = []
if alerts is not None and "incident_id" in alerts:
    missing = sorted(set(gold["window_id"]) - set(alerts["incident_id"]))

theme.kpi_row([
    ("Critical notes in Gold", f"{len(gold):,}", "gold_incident_notes", theme.CORAL),
    ("Alerts in Lakebase", f"{len(alerts):,}" if alerts is not None else "n/a", config.LAKEBASE_ALERTS_TABLE, theme.CYAN),
    ("Unacknowledged", f"{new_count:,}" if new_count is not None else "n/a", "status NEW", theme.AMBER),
    ("Routing status", ("In sync" if alerts is not None and not missing else f"{len(missing)} not routed") if alerts is not None else "Unknown",
     "Gold critical vs Lakebase", theme.TEAL if alerts is not None and not missing else theme.AMBER),
])

tab_lb, tab_gold = st.tabs(["Lakebase alerts", "Gold critical notes"])

with tab_lb:
    if lb_error:
        st.warning("Lakebase alerts could not be loaded.")
        st.markdown(lb_error)
        st.caption("The Gold tab still works. Lakebase setup is described in the README (steps 2 and 4).")
    elif alerts is None or alerts.empty:
        theme.empty_state("No alerts in Lakebase yet.", "Run the alert-routing notebook after the pipeline.")
    else:
        status_options = sorted(alerts["alert_status"].dropna().unique()) if "alert_status" in alerts else []
        chosen = st.multiselect("Status", status_options, default=status_options) if status_options else []
        view = alerts[alerts["alert_status"].isin(chosen)] if status_options else alerts
        theme.html_table(
            view,
            [("window_start", "Window start"), ("asset_id", "Asset"), ("substation_name", "Substation"),
             ("severity", "Severity"), ("alert_status", "Status"), ("triggered_metrics", "Metrics"),
             ("likely_category", "Category"), ("recommended_check", "Recommended check"),
             ("alert_created_at", "Alert created")],
            badge_cols={"severity", "alert_status", "likely_category"}, time_cols={"window_start", "alert_created_at"}, max_rows=100)

        if missing:
            with st.expander(f"{len(missing)} critical incident(s) in Gold are not in Lakebase yet"):
                st.write(", ".join(missing[:50]))
                st.caption("Run the alert-routing notebook to route them.")

        if "alert_status" in alerts and "incident_id" in alerts:
            theme.section("Acknowledge an alert")
            open_ids = alerts.loc[alerts["alert_status"] == "NEW", "incident_id"].tolist()
            if not open_ids:
                st.caption("Every alert has been acknowledged.")
            else:
                pick = st.selectbox("Alert", open_ids)
                if st.button("Mark as acknowledged", type="primary"):
                    try:
                        lakebase.acknowledge(pick)
                        st.success(f"{pick} acknowledged.")
                        st.rerun()
                    except lakebase.LakebaseError as exc:
                        st.error("Could not update the alert.")
                        st.markdown(str(exc))

with tab_gold:
    if gold.empty:
        theme.empty_state("No critical notes in Gold.")
    else:
        theme.html_table(
            gold,
            [("window_start", "Window start"), ("asset_id", "Asset"), ("substation_name", "Substation"),
             ("maintenance_status", "Status"), ("triggered_metrics", "Metrics"), ("likely_category", "Category"),
             ("deviation", "Deviation"), ("recommended_check", "Recommended check")],
            badge_cols={"maintenance_status", "likely_category"}, time_cols={"window_start"}, max_rows=100)
        theme.section("Full notes", "latest 10")
        for r in gold.head(10).itertuples():
            with st.expander(f"{theme.fmt_ts(r.window_start)}  \u00b7  {r.asset_id}  \u00b7  {r.substation_name}"):
                st.write(r.note or "No note text.")
