"""Page 1: Fleet Overview - KPIs, anomaly trend, equipment breakdown, recent critical events."""
import plotly.express as px
import streamlit as st

from gridwatch import db, theme

theme.page_header("Fleet Overview", "Health of the whole grid fleet at a glance, from the UC-12 Gold layer.")

# ---- KPIs ------------------------------------------------------------------------------------
health = db.load(f"""
    SELECT COUNT(*) AS assets,
           COALESCE(SUM(total_readings), 0)   AS readings,
           COALESCE(SUM(warning_readings), 0) AS warnings,
           COALESCE(SUM(critical_readings), 0) AS criticals,
           SUM(CASE WHEN maintenance_status <> 'ACTIVE' THEN 1 ELSE 0 END) AS in_maintenance
    FROM {db.T('gold_asset_health')}""", what="fleet health").iloc[0]

windows = db.load(f"""
    SELECT COUNT(*) AS windows,
           COALESCE(SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END), 0) AS critical_windows,
           COUNT(DISTINCT asset_id) AS assets_affected
    FROM {db.T('gold_anomaly_windows')}""", what="incident windows").iloc[0]

readings = int(health["readings"] or 0)
anomalies = int(health["warnings"] or 0) + int(health["criticals"] or 0)
rate = (100.0 * anomalies / readings) if readings else 0.0

theme.kpi_row([
    ("Assets monitored", f"{int(health['assets'])}", f"{int(health['in_maintenance'] or 0)} under maintenance", theme.CYAN),
    ("Critical readings", f"{int(health['criticals']):,}", f"{int(windows['critical_windows'])} critical windows", theme.CORAL),
    ("Warning readings", f"{int(health['warnings']):,}", "outside the normal band", theme.AMBER),
    ("Anomaly windows", f"{int(windows['windows']):,}", f"{int(windows['assets_affected'])} assets affected", theme.CYAN),
    ("Anomaly rate", f"{rate:.1f}%", f"of {readings:,} valid readings", theme.TEAL),
])

if readings == 0:
    theme.empty_state("No readings found in gold_asset_health.", "Run the UC-12 pipeline first.")
    st.stop()

# ---- Anomaly trend -----------------------------------------------------------------------------
theme.section("Anomaly trend", "abnormal readings over time")
granularity = st.radio("Time bucket", ["1 hour", "15 minutes"], horizontal=True, label_visibility="collapsed")
bucket_sql = ("date_trunc('hour', event_ts)" if granularity == "1 hour"
              else "to_timestamp(from_unixtime(floor(unix_timestamp(event_ts) / 900) * 900))")   # fixed text, not user input
trend = db.load(f"""
    SELECT {bucket_sql} AS bucket, severity, COUNT(*) AS readings
    FROM {db.T('gold_anomaly_readings')}
    GROUP BY 1, 2 ORDER BY 1""", what="the anomaly trend")

if trend.empty:
    theme.empty_state("No anomalous readings to chart.")
else:
    fig = px.bar(trend, x="bucket", y="readings", color="severity", barmode="stack",
                 color_discrete_map=theme.SEVERITY_COLORS, labels={"bucket": "", "readings": "Readings"})
    theme.show_fig(fig, height=330)

# ---- Equipment breakdown -----------------------------------------------------------------------
theme.section("Equipment breakdown")
left, right = st.columns(2)
by_type = db.load(f"""
    SELECT asset_type, severity, COUNT(*) AS readings
    FROM {db.T('gold_anomaly_readings')} GROUP BY 1, 2 ORDER BY 1""", what="the equipment breakdown")
by_site = db.load(f"""
    SELECT substation_name, severity, COUNT(*) AS readings
    FROM {db.T('gold_anomaly_readings')} GROUP BY 1, 2 ORDER BY 1""", what="the substation breakdown")

with left:
    if by_type.empty:
        theme.empty_state("No data")
    else:
        fig = px.bar(by_type, x="asset_type", y="readings", color="severity", barmode="stack",
                     color_discrete_map=theme.SEVERITY_COLORS, labels={"asset_type": "", "readings": "Readings"},
                     title="By equipment type")
        theme.show_fig(fig, height=320)
with right:
    if by_site.empty:
        theme.empty_state("No data")
    else:
        fig = px.bar(by_site, x="substation_name", y="readings", color="severity", barmode="stack",
                     color_discrete_map=theme.SEVERITY_COLORS, labels={"substation_name": "", "readings": "Readings"},
                     title="By substation")
        theme.show_fig(fig, height=320)

# ---- Recent critical events --------------------------------------------------------------------
theme.section("Recent critical events", "latest 10 critical windows")
recent = db.load(f"""
    SELECT window_start, asset_id, substation_name, maintenance_status, triggered_metrics, severity,
           max_temperature_c, min_voltage_kv, max_voltage_kv, min_frequency_hz, max_frequency_hz
    FROM {db.T('gold_anomaly_windows')}
    WHERE severity = 'CRITICAL'
    ORDER BY window_start DESC LIMIT 10""", what="recent critical events")

if recent.empty:
    theme.empty_state("No critical windows right now.")
else:
    theme.html_table(
        recent,
        [("window_start", "Window start"), ("asset_id", "Asset"), ("substation_name", "Substation"),
         ("maintenance_status", "Status"), ("triggered_metrics", "Metrics"), ("severity", "Severity"),
         ("max_temperature_c", "Max temp C"), ("min_voltage_kv", "Min kV"), ("max_voltage_kv", "Max kV"),
         ("min_frequency_hz", "Min Hz"), ("max_frequency_hz", "Max Hz")],
        badge_cols={"severity", "maintenance_status"}, time_cols={"window_start"})
