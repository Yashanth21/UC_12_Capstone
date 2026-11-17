"""Page 5: Asset Health - indicators per asset, filters, details and registry history."""
import pandas as pd
import streamlit as st

from gridwatch import config, db, theme

theme.page_header("Asset Health", "Condition of every asset, with its current registry details and history.")

health = db.load(f"""
    SELECT asset_id, asset_type, substation_name, rated_capacity_mva, firmware_version, maintenance_status,
           total_readings, warning_readings, critical_readings, avg_temperature_c, max_temperature_c
    FROM {db.T('gold_asset_health')} ORDER BY asset_id""", what="asset health")
if health.empty:
    theme.empty_state("No assets found.", "Run the UC-12 pipeline first.")
    st.stop()

health["anomaly_rate"] = (100.0 * (health["warning_readings"] + health["critical_readings"])
                          / health["total_readings"].where(health["total_readings"] > 0))
low, high = config.HEALTH_BANDS


def state_of(row) -> str:
    if row["maintenance_status"] != "ACTIVE":
        return "UNDER_MAINTENANCE"
    rate = row["anomaly_rate"]
    if pd.isna(rate) or rate < low:
        return "HEALTHY"
    return "WATCH" if rate < high else "AT RISK"


health["state"] = health.apply(state_of, axis=1)
STATE_COLOR = {"HEALTHY": theme.TEAL, "WATCH": theme.AMBER, "AT RISK": theme.CORAL, "UNDER_MAINTENANCE": theme.AMBER}

# ---- Filters -------------------------------------------------------------------------------------------
f1, f2, f3 = st.columns(3)
types = f1.multiselect("Equipment type", sorted(health["asset_type"].unique()), default=sorted(health["asset_type"].unique()))
sites = f2.multiselect("Substation", sorted(health["substation_name"].unique()), default=sorted(health["substation_name"].unique()))
states = f3.multiselect("Health state", list(STATE_COLOR), default=list(STATE_COLOR), format_func=lambda s: s.replace("_", " ").title())
view = health[health["asset_type"].isin(types) & health["substation_name"].isin(sites) & health["state"].isin(states)]

theme.kpi_row([
    ("Healthy", int((health["state"] == "HEALTHY").sum()), f"anomaly rate under {low:g}%", theme.TEAL),
    ("Watch", int((health["state"] == "WATCH").sum()), f"{low:g}% to {high:g}%", theme.AMBER),
    ("At risk", int((health["state"] == "AT RISK").sum()), f"above {high:g}%", theme.CORAL),
    ("Under maintenance", int((health["state"] == "UNDER_MAINTENANCE").sum()), "status from the SCD2 registry", theme.CYAN),
])
st.caption("Health state is based on the share of readings that were anomalous. The bands are set in config.py (HEALTH_BANDS).")

# ---- Asset tiles -----------------------------------------------------------------------------------------
theme.section("Assets", f"{len(view)} shown")
if view.empty:
    theme.empty_state("No assets match these filters.")
else:
    view = view.sort_values("anomaly_rate", ascending=False)
    rows = [view.iloc[i:i + 4] for i in range(0, len(view), 4)]
    for chunk in rows:
        cols = st.columns(4)
        for col, (_, a) in zip(cols, chunk.iterrows()):
            rate = "n/a" if pd.isna(a["anomaly_rate"]) else f"{a['anomaly_rate']:.2f}%"
            col.markdown(
                f'<div class="gw-tile" style="--accent:{STATE_COLOR[a["state"]]}">'
                f'<div class="gw-tile-id">{theme.esc(a["asset_id"])} &nbsp;{theme.badge(a["state"])}</div>'
                f'<div class="gw-tile-meta">{theme.esc(a["asset_type"])} \u00b7 {theme.esc(a["substation_name"])}</div>'
                f'<div class="gw-tile-row">Anomaly rate<b>{rate}</b></div>'
                f'<div class="gw-tile-row">Critical / warning<b>{int(a["critical_readings"])} / {int(a["warning_readings"])}</b></div>'
                f'<div class="gw-tile-row">Avg / max temp<b>{a["avg_temperature_c"]:.1f} / {a["max_temperature_c"]:.1f} \u00b0C</b></div>'
                f'<div class="gw-tile-row">Firmware<b>{theme.esc(a["firmware_version"])}</b></div></div>',
                unsafe_allow_html=True)
            col.write("")

# ---- Asset detail ------------------------------------------------------------------------------------------
theme.section("Asset detail")
asset_id = st.selectbox("Select an asset", health["asset_id"].tolist())
a = health[health["asset_id"] == asset_id].iloc[0]
theme.kpi_row([
    ("Rated capacity", f"{a['rated_capacity_mva']:g} MVA", f"{a['asset_type']} \u00b7 {a['substation_name']}", theme.CYAN),
    ("Status", str(a["maintenance_status"]).replace("_", " ").title(), f"firmware {a['firmware_version']}", STATE_COLOR[a["state"]]),
    ("Readings", f"{int(a['total_readings']):,}", f"{int(a['critical_readings'])} critical \u00b7 {int(a['warning_readings'])} warning", theme.TEAL),
    ("Max temperature", f"{a['max_temperature_c']:.1f} \u00b0C", f"average {a['avg_temperature_c']:.1f} \u00b0C", theme.CORAL),
])

left, right = st.columns(2)
with left:
    theme.section("Registry history", "SCD Type 2: every version of this asset")
    history = db.load(f"""
        SELECT `__START_AT` AS valid_from, `__END_AT` AS valid_to, firmware_version, maintenance_status, substation_name
        FROM {db.T('silver_asset_registry', silver=True)}
        WHERE asset_id = :a ORDER BY `__START_AT` DESC""", {"a": asset_id}, what="registry history")
    if history.empty:
        theme.empty_state("No registry history.")
    else:
        history["valid_to"] = history["valid_to"].apply(lambda v: "current" if pd.isna(v) else theme.fmt_ts(v))
        theme.html_table(history, [("valid_from", "Valid from"), ("valid_to", "Valid to"), ("firmware_version", "Firmware"),
                                   ("maintenance_status", "Status")], badge_cols={"maintenance_status"}, time_cols={"valid_from"})
with right:
    theme.section("Latest anomalies", "most recent 10 readings")
    recent = db.load(f"""
        SELECT event_ts, severity, triggered_metrics, voltage_kv, frequency_hz, temperature_c
        FROM {db.T('gold_anomaly_readings')} WHERE asset_id = :a ORDER BY event_ts DESC LIMIT 10""",
                     {"a": asset_id}, what="recent anomalies")
    if recent.empty:
        theme.empty_state("No anomalies for this asset.")
    else:
        theme.html_table(recent, [("event_ts", "Time"), ("severity", "Severity"), ("triggered_metrics", "Metrics"),
                                  ("voltage_kv", "kV"), ("frequency_hz", "Hz"), ("temperature_c", "\u00b0C")],
                         badge_cols={"severity"}, time_cols={"event_ts"})
