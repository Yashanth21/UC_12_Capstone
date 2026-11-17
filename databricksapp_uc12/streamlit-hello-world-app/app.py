
"""GRIDWATCH AI - Streamlit application entry point."""

import streamlit as st

# Configure the page before any other Streamlit commands.
st.set_page_config(
    page_title="GRIDWATCH AI",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

<<<<<<< Updated upstream
st.header("Hello world!!!")
apps = st.slider("Number of apps", max_value=60, value=10)
chart_data = pd.DataFrame({'y':[2 ** x for x in range(apps)]})
st.bar_chart(chart_data, height=500, width=min(100+50*apps, 1000), 
             x_label="Apps", y_label="Fun with data")
=======
# Optional diagnostics: initialize after page configuration.
try:
    import app_diagnostics

    app_diagnostics.install_diagnostics()
except ImportError:
    # The app can still run if the optional diagnostics file is absent.
    pass

from gridwatch import config, db, theme

# Apply the application's custom styling.
theme.inject_css()

# Configure application navigation.
pages = {
    "Monitor": [
        st.Page(
            "views/fleet_overview.py",
            title="Fleet Overview",
            icon="📊",
            default=True,
        ),
        st.Page(
            "views/sensor_analytics.py",
            title="Sensor Analytics",
            icon="📈",
        ),
        st.Page(
            "views/asset_health.py",
            title="Asset Health",
            icon="🛠️",
        ),
    ],
    "Respond": [
        st.Page(
            "views/critical_alerts.py",
            title="Critical Alerts",
            icon="🚨",
        ),
        st.Page(
            "views/ai_incident_analysis.py",
            title="AI Incident Analysis",
            icon="🧠",
        ),
    ],
    "Assistant": [
        st.Page(
            "views/ai_chat.py",
            title="AI Chat Assistant",
            icon="💬",
        ),
    ],
    "System": [
        st.Page(
            "views/about.py",
            title="About / System Info",
            icon="ℹ️",
        ),
    ],
}

page = st.navigation(pages)

# Sidebar connection status and controls.
with st.sidebar:
    st.markdown(
        '<div class="gw-side-title">Connections</div>',
        unsafe_allow_html=True,
    )

    connections = [
        ("SQL warehouse", config.sql_configured()),
        ("Agent endpoint", config.agent_configured()),
        ("Lakebase", config.lakebase_configured()),
    ]

    for label, is_configured in connections:
        color = theme.TEAL if is_configured else theme.CORAL
        state = "configured" if is_configured else "not configured"

        st.markdown(
            f'<div style="font-size:.82rem;margin:.2rem 0">'
            f'<span class="gw-dot" style="background:{color}"></span>'
            f'{label} <span style="color:{theme.MUTED}">'
            f'· {state}</span></div>',
            unsafe_allow_html=True,
        )

    st.write("")

    if st.button("Refresh data", use_container_width=True):
        db.clear_cache()
        st.rerun()

    st.caption(
        f"{config.APP_NAME} v{config.APP_VERSION} "
        f"· data cached {config.CACHE_SECONDS // 60} min"
    )

# Render the selected page.
page.run()
>>>>>>> Stashed changes
