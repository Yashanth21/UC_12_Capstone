"""Page 7: About / System Info - architecture, data sources and live connection checks."""
import time

import streamlit as st

from gridwatch import agent_client, config, db, lakebase, theme

theme.page_header("About / System Info", "How GRIDWATCH AI is built and whether each connection is working right now.")

# ---- Architecture ------------------------------------------------------------------------------------------
theme.section("Architecture", "UC-12 Grid Sensor Anomaly Watch")
st.graphviz_chart("""
digraph G {
  rankdir=LR; bgcolor="transparent"; nodesep=0.35; ranksep=0.5;
  node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, fontcolor="#F1F5F9", color="#22D3EE", fillcolor="#111C2E"];
  edge [color="#94A3B8", fontcolor="#94A3B8", fontname="Helvetica", fontsize=9];
  PG [label="PostgreSQL\\nsource"]; LC [label="Lakeflow\\nConnect"]; B [label="Bronze"];
  S [label="Silver\\nAUTO CDC + SCD2"]; G [label="Gold tables\\n+ metric views"]; AI [label="ai_query\\nincident notes"];
  GE [label="Genie Space"]; AG [label="Agent endpoint\\n(tool-calling)"]; LB [label="Lakebase\\ncritical_grid_alerts", color="#FB7185"];
  APP [label="GRIDWATCH AI\\nDatabricks App", color="#2DD4A7"];
  PG -> LC -> B -> S -> G; G -> AI [label="notes"]; AI -> G; G -> GE; GE -> AG; G -> AG [label="UC function"];
  G -> LB [label="critical only"]; G -> APP [label="SQL warehouse"]; AG -> APP [label="chat"]; LB -> APP [label="alerts"];
}
""", use_container_width=True)

# ---- Data sources --------------------------------------------------------------------------------------------
theme.section("Data sources")
st.markdown(f"""
| Source | Location | Used by |
|---|---|---|
| Anomaly readings | `{config.CATALOG}.{config.GOLD_SCHEMA}.gold_anomaly_readings` | Fleet Overview, Sensor Analytics, Asset Health |
| Anomaly windows | `{config.CATALOG}.{config.GOLD_SCHEMA}.gold_anomaly_windows` | Fleet Overview, AI Incident Analysis |
| Asset health | `{config.CATALOG}.{config.GOLD_SCHEMA}.gold_asset_health` | Fleet Overview, Asset Health |
| Incident notes (AI) | `{config.CATALOG}.{config.GOLD_SCHEMA}.gold_incident_notes` | AI Incident Analysis, Critical Alerts |
| Clean telemetry | `{config.CATALOG}.{config.SILVER_SCHEMA}.silver_grid_telemetry` | Sensor Analytics |
| Asset registry (SCD2) | `{config.CATALOG}.{config.SILVER_SCHEMA}.silver_asset_registry` | Asset Health |
| Alerts (Lakebase) | `{config.LAKEBASE_SCHEMA}.{config.LAKEBASE_ALERTS_TABLE}` | Critical Alerts |
| AI agent | `{config.AGENT_ENDPOINT or 'not configured'}` | AI Chat Assistant, AI Incident Analysis |
""")

# ---- Configuration -------------------------------------------------------------------------------------------
theme.section("Configuration", "values come from environment variables; no secrets are stored in the code")
settings = [
    ("SQL warehouse (DATABRICKS_WAREHOUSE_ID)", config.WAREHOUSE_ID),
    ("Agent endpoint (AGENT_ENDPOINT)", config.AGENT_ENDPOINT),
    ("Lakebase endpoint (LAKEBASE_ENDPOINT)", config.LAKEBASE_ENDPOINT),
    ("Lakebase host (PGHOST)", config.PGHOST),
    ("Lakebase user (PGUSER)", "set" if config.PGUSER else ""),
]
dot = lambda ok: f'<span class="gw-dot" style="background:{theme.TEAL if ok else theme.CORAL}"></span>'
st.markdown('<div class="gw-card">' + "".join(
    f'<div style="margin:.25rem 0">{dot(bool(v))}<b>{theme.esc(k)}</b> &nbsp; <span style="color:{theme.MUTED}">{theme.esc(v) if v else "not set"}</span></div>'
    for k, v in settings) + "</div>", unsafe_allow_html=True)

# ---- Live connection checks ----------------------------------------------------------------------------------
theme.section("Connection checks", "runs real queries and calls")
if st.button("Run connection checks", type="primary"):
    results = []

    # 1) SQL warehouse + tables
    if not config.sql_configured():
        results.append(("SQL warehouse", False, "DATABRICKS_WAREHOUSE_ID is not set"))
    else:
        try:
            t0 = time.time()
            db.query("SELECT 1 AS ok", cache=False)
            results.append(("SQL warehouse", True, f"connected in {time.time() - t0:.1f}s"))
            for name, silver in [(t, False) for t in config.GOLD_TABLES] + [("silver_grid_telemetry", True), ("silver_asset_registry", True)]:
                try:
                    n = int(db.query(f"SELECT COUNT(*) AS n FROM {db.T(name, silver=silver)}", cache=False).iloc[0]["n"])
                    results.append((f"Table {name}", True, f"{n:,} rows"))
                except db.DataError as exc:
                    results.append((f"Table {name}", False, str(exc)[:160]))
        except db.DataError as exc:
            results.append(("SQL warehouse", False, str(exc)[:200]))

    # 2) Agent endpoint
    try:
        state = agent_client.endpoint_state()
        results.append(("Agent endpoint", state["ready"] == "READY", f"{state['name']} is {state['ready']}"))
    except agent_client.AgentError as exc:
        results.append(("Agent endpoint", False, str(exc)))

    # 3) Lakebase
    if not config.lakebase_configured():
        results.append(("Lakebase", False, "Lakebase is not configured (LAKEBASE_ENDPOINT / PGHOST)"))
    else:
        try:
            info = lakebase.health_check()
            results.append(("Lakebase", True, f"connected as {info['user']} to {info['database']}; {info['alerts']} alerts"))
        except lakebase.LakebaseError as exc:
            results.append(("Lakebase", False, str(exc)))

    st.session_state.checks = results

for name, ok, detail in st.session_state.get("checks", []):
    st.markdown(f'<div class="gw-card" style="margin:.35rem 0">{dot(ok)}<b>{theme.esc(name)}</b> &nbsp; '
                f'<span style="color:{theme.MUTED}">{theme.esc(detail)}</span></div>', unsafe_allow_html=True)

if config.agent_configured():
    st.caption("An end-to-end test sends one real question to the agent (uses a little model quota).")
    if st.button("Send a test question to the agent"):
        with st.spinner("Waiting for the agent..."):
            try:
                reply = agent_client.ask([{"role": "user", "content": "How many critical readings are there in total?"}])
                st.success(f"Agent answered in {reply.seconds}s")
                st.markdown(reply.answer)
            except agent_client.AgentError as exc:
                st.error(str(exc))

st.caption(f"{config.APP_NAME} v{config.APP_VERSION}")
