"""Page 2: Sensor Analytics - voltage, frequency and temperature trends with threshold references."""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from gridwatch import config, db, theme

theme.page_header("Sensor Analytics", "Minute-by-minute readings per asset, with the alert thresholds and flagged anomalies.")

ASSET_COLORS = [theme.CYAN, "#A78BFA", "#60A5FA", "#F472B6", "#34D399", "#F59E0B",
                "#38BDF8", "#C084FC", "#4ADE80", "#FB923C", "#2DD4BF", "#E879F9"]

# ---- Filters -------------------------------------------------------------------------------------
assets = db.load(f"SELECT asset_id, asset_type FROM {db.T('gold_asset_health')} ORDER BY asset_id", what="the asset list")
if assets.empty:
    theme.empty_state("No assets found.", "Run the UC-12 pipeline first.")
    st.stop()

span = db.load(f"SELECT MIN(event_ts) AS t0, MAX(event_ts) AS t1 FROM {db.T('silver_grid_telemetry', silver=True)}",
               what="the telemetry time range").iloc[0]
if pd.isna(span["t0"]):
    theme.empty_state("No telemetry found in silver_grid_telemetry.")
    st.stop()
t_min, t_max = span["t0"].to_pydatetime(), span["t1"].to_pydatetime()

f1, f2 = st.columns([1, 2])
types = f1.multiselect("Equipment type", sorted(assets["asset_type"].unique()), default=sorted(assets["asset_type"].unique()))
available = assets[assets["asset_type"].isin(types)]["asset_id"].tolist()
selected = f2.multiselect("Assets (up to 6)", available, default=available[:3], max_selections=6)
window = st.slider("Time range", min_value=t_min, max_value=t_max, value=(t_min, t_max), format="MMM DD, HH:mm")

if not selected:
    theme.empty_state("Pick at least one asset.")
    st.stop()

# ---- Data ----------------------------------------------------------------------------------------
placeholders, id_params = db.in_clause("a", selected)
params = {**id_params, "t0": window[0], "t1": window[1]}
tele = db.load(f"""
    SELECT event_ts, asset_id, asset_type, voltage_kv, frequency_hz, temperature_c
    FROM {db.T('silver_grid_telemetry', silver=True)}
    WHERE asset_id IN ({placeholders}) AND event_ts BETWEEN :t0 AND :t1
    ORDER BY event_ts""", params, what="telemetry")
flags = db.load(f"""
    SELECT event_ts, asset_id, severity, triggered_metrics, voltage_kv, frequency_hz, temperature_c
    FROM {db.T('gold_anomaly_readings')}
    WHERE asset_id IN ({placeholders}) AND event_ts BETWEEN :t0 AND :t1""", params, what="flagged readings")

if tele.empty:
    theme.empty_state("No readings for this selection.")
    st.stop()

METRICS = {
    "Voltage": ("voltage_kv", "kV", "VOLTAGE"),
    "Frequency": ("frequency_hz", "Hz", "FREQUENCY"),
    "Temperature": ("temperature_c", "\u00b0C", "TEMPERATURE"),
}


def add_thresholds(fig, metric: str) -> None:
    if metric == "Voltage":
        ref = config.VOLTAGE
        for v in ref["warn"]:
            fig.add_hline(y=v, line_dash="dot", line_color=theme.AMBER, opacity=.7, annotation_text="warning",
                          annotation_font_color=theme.AMBER, annotation_position="top left")
        for v in ref["crit"]:
            fig.add_hline(y=v, line_dash="dash", line_color=theme.CORAL, opacity=.8, annotation_text="critical",
                          annotation_font_color=theme.CORAL, annotation_position="top left")
    elif metric == "Frequency":
        ref = config.FREQUENCY
        for v in ref["warn"]:
            fig.add_hline(y=v, line_dash="dot", line_color=theme.AMBER, opacity=.7, annotation_text="warning",
                          annotation_font_color=theme.AMBER, annotation_position="top left")
        for v in ref["crit"]:
            fig.add_hline(y=v, line_dash="dash", line_color=theme.CORAL, opacity=.8, annotation_text="critical",
                          annotation_font_color=theme.CORAL, annotation_position="top left")
    else:
        for asset_type in sorted(tele["asset_type"].dropna().unique()):
            ref = config.TEMPERATURE.get(asset_type)
            if not ref:
                continue
            fig.add_hline(y=ref["warn"], line_dash="dot", line_color=theme.AMBER, opacity=.7,
                          annotation_text=f"{asset_type} warning", annotation_font_color=theme.AMBER,
                          annotation_position="top left")
            fig.add_hline(y=ref["crit"], line_dash="dash", line_color=theme.CORAL, opacity=.8,
                          annotation_text=f"{asset_type} critical", annotation_font_color=theme.CORAL,
                          annotation_position="top left")


tabs = st.tabs(list(METRICS))
for tab, (metric, (column, unit, flag_name)) in zip(tabs, METRICS.items()):
    with tab:
        series = tele[column].dropna()
        theme.kpi_row([
            (f"Average {metric.lower()}", f"{series.mean():.2f} {unit}" if len(series) else "n/a", "selected assets and range", theme.CYAN),
            ("Minimum", f"{series.min():.2f} {unit}" if len(series) else "n/a", "", theme.TEAL),
            ("Maximum", f"{series.max():.2f} {unit}" if len(series) else "n/a", "", theme.AMBER),
            ("Flagged readings",
             f"{int(flags['triggered_metrics'].fillna('').str.contains(flag_name).sum()):,}",
             f"{flag_name.lower()} anomalies in range", theme.CORAL),
        ])
        fig = go.Figure()
        for i, asset in enumerate(selected):
            part = tele[tele["asset_id"] == asset]
            fig.add_trace(go.Scatter(x=part["event_ts"], y=part[column], mode="lines", name=asset,
                                     line=dict(width=1.6, color=ASSET_COLORS[i % len(ASSET_COLORS)])))
        marks = flags[flags["triggered_metrics"].fillna("").str.contains(flag_name)]
        if not marks.empty:
            for severity, color in (("WARNING", theme.AMBER), ("CRITICAL", theme.CORAL)):
                part = marks[marks["severity"] == severity]
                if not part.empty:
                    fig.add_trace(go.Scatter(x=part["event_ts"], y=part[column], mode="markers", name=f"{severity.title()} reading",
                                             marker=dict(size=7, color=color, line=dict(width=1, color=theme.NAVY))))
        add_thresholds(fig, metric)
        fig.update_yaxes(title_text=unit)
        theme.show_fig(fig, height=420)
        st.caption("Dotted lines are warning limits, dashed lines are critical limits. Dots mark readings the Gold layer flagged.")
