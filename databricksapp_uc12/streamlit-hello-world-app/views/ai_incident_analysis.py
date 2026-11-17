"""Page 3: AI Incident Analysis - pick an incident window and ask the existing agent for a summary and checks."""
import streamlit as st

from gridwatch import agent_client, config, db, theme

theme.page_header("AI Incident Analysis", "Pick an incident window. The deployed GridWatch agent writes the summary and recommended checks.")

# ---- Choose an incident ----------------------------------------------------------------------------
c1, c2 = st.columns([1, 1])
severities = c1.multiselect("Severity", ["CRITICAL", "WARNING"], default=["CRITICAL"])
asset_list = db.load(f"SELECT DISTINCT asset_id FROM {db.T('gold_anomaly_windows')} ORDER BY asset_id", what="assets")["asset_id"].tolist()
asset_pick = c2.selectbox("Asset", ["All assets"] + asset_list)

if not severities:
    theme.empty_state("Select at least one severity.")
    st.stop()

sev_sql, params = db.in_clause("s", severities)
asset_sql = ""
if asset_pick != "All assets":
    asset_sql, params["asset"] = "AND asset_id = :asset", asset_pick

windows = db.load(f"""
    SELECT window_id, window_start, window_end, asset_id, asset_type, substation_name, maintenance_status,
           firmware_version, severity, triggered_metrics, anomalous_readings,
           min_voltage_kv, max_voltage_kv, min_frequency_hz, max_frequency_hz, max_temperature_c
    FROM {db.T('gold_anomaly_windows')}
    WHERE severity IN ({sev_sql}) {asset_sql}
    ORDER BY window_start DESC LIMIT 300""", params, what="incident windows")

if windows.empty:
    theme.empty_state("No incidents match these filters.")
    st.stop()

labels = {r.window_id: f"{theme.fmt_ts(r.window_start)}  \u00b7  {r.asset_id}  \u00b7  {r.severity}  \u00b7  {r.triggered_metrics}"
          for r in windows.itertuples()}
window_id = st.selectbox("Incident window", list(labels), format_func=lambda w: labels[w])
inc = windows[windows["window_id"] == window_id].iloc[0]

# ---- Incident facts (from Gold) ----------------------------------------------------------------------
theme.section("Incident facts", "from gold_anomaly_windows")
sev_color = theme.SEVERITY_COLORS.get(str(inc["severity"]).upper(), theme.CYAN)
theme.kpi_row([
    ("Asset", inc["asset_id"], f"{inc['asset_type']} \u00b7 {inc['substation_name']}", theme.CYAN),
    ("Severity", str(inc["severity"]).title(), f"{int(inc['anomalous_readings'])} anomalous readings", sev_color),
    ("Status in window", str(inc["maintenance_status"]).replace("_", " ").title(), f"firmware {inc['firmware_version']}", theme.AMBER if inc["maintenance_status"] != "ACTIVE" else theme.TEAL),
    ("Max temperature", f"{inc['max_temperature_c']:.1f} \u00b0C", "in the window", theme.CORAL),
    ("Voltage / frequency", f"{inc['min_voltage_kv']:.2f}-{inc['max_voltage_kv']:.2f} kV", f"{inc['min_frequency_hz']:.2f}-{inc['max_frequency_hz']:.2f} Hz", theme.CYAN),
])
st.caption(f"Window: {theme.fmt_ts(inc['window_start'])} to {theme.fmt_ts(inc['window_end'])}  \u00b7  triggered metrics: {inc['triggered_metrics']}")

# ---- Note already stored in Gold ---------------------------------------------------------------------
theme.section("Pre-generated note", "written by ai_query in the pipeline (gold_incident_notes)")
note = db.load(f"""SELECT deviation, likely_category, recommended_check, note
                   FROM {db.T('gold_incident_notes')} WHERE window_id = :w""", {"w": window_id}, what="the stored note")
if note.empty:
    theme.empty_state("No stored note for this window.")
else:
    n = note.iloc[0]
    st.markdown(
        f'<div class="gw-card"><b>Deviation</b><br>{theme.esc(n["deviation"])}<br><br>'
        f'<b>Likely category</b><br>{theme.badge(n["likely_category"])}<br><br>'
        f'<b>Recommended check</b><br>{theme.esc(n["recommended_check"])}<br><br>'
        f'<b>Note</b><br>{theme.esc(n["note"])}</div>', unsafe_allow_html=True)

# ---- Ask the agent -----------------------------------------------------------------------------------
theme.section("Agent analysis", "live answer from the deployed GridWatch agent")
if not config.agent_configured():
    st.warning("The agent endpoint is not configured. Set AGENT_ENDPOINT in the app settings (README, step 2).")
    st.stop()

prompt = (
    f"Analyse the incident window {window_id}: asset {inc['asset_id']} ({inc['asset_type']}) at {inc['substation_name']}, "
    f"severity {inc['severity']}, from {inc['window_start']} to {inc['window_end']}, triggered metrics {inc['triggered_metrics']}. "
    "Use the incident notes tool for this asset to find this window. "
    "Reply with: Asset, Deviation, Likely category, Recommended check, and one sentence on the risk if it is ignored. "
    "Use only data returned by the tools."
)
cache_key = f"analysis::{window_id}"

if st.button("Generate analysis with the agent", type="primary"):
    with st.spinner("The agent is reading the incident notes and writing the analysis..."):
        try:
            st.session_state[cache_key] = agent_client.ask([{"role": "user", "content": prompt}])
        except agent_client.AgentError as exc:
            st.session_state.pop(cache_key, None)
            st.error(str(exc))

reply = st.session_state.get(cache_key)
if reply:
    st.markdown('<div class="gw-card">', unsafe_allow_html=True)
    st.markdown(reply.answer)
    st.markdown("</div>", unsafe_allow_html=True)
    st.caption(f"Answered in {reply.seconds}s by {config.AGENT_ENDPOINT}")
    if reply.tools:
        with st.expander(f"Tools the agent used ({len(reply.tools)})"):
            for t in reply.tools:
                st.markdown(f"**{t['name']}**")
                st.code(str(t.get("arguments")), language="json")
                if t.get("output"):
                    st.code(str(t["output"])[:1500])
    with st.expander("Prompt sent to the agent"):
        st.code(prompt)
else:
    st.caption("Click the button to generate a fresh analysis. Nothing is pre-written.")
