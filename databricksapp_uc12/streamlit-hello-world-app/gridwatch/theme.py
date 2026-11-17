"""Look and feel for GRIDWATCH AI: palette, CSS, Plotly styling and small HTML components."""
import html
from typing import Iterable, Optional, Sequence

import pandas as pd
import streamlit as st

# ---- Palette -------------------------------------------------------------------------------
NAVY = "#0B1220"      # background
SIDEBAR = "#080F1C"
CARD = "#111C2E"
CYAN = "#22D3EE"      # primary accent
TEAL = "#2DD4A7"      # healthy
AMBER = "#FBBF24"     # warning
CORAL = "#FB7185"     # critical
TEXT = "#F1F5F9"
MUTED = "#94A3B8"
BORDER = "rgba(148,163,184,0.16)"

SEVERITY_COLORS = {"CRITICAL": CORAL, "WARNING": AMBER, "NORMAL": TEAL}
STATUS_COLORS = {
    "ACTIVE": TEAL, "UNDER_MAINTENANCE": AMBER, "NEW": CORAL, "ACKNOWLEDGED": TEAL,
    "HEALTHY": TEAL, "WATCH": AMBER, "AT RISK": CORAL,
}

_CSS = """
<style>
:root { --navy:__NAVY__; --card:__CARD__; --cyan:__CYAN__; --teal:__TEAL__; --amber:__AMBER__;
        --coral:__CORAL__; --text:__TEXT__; --muted:__MUTED__; --border:__BORDER__; }
.stApp { background: var(--navy); color: var(--text);
         font-family: -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }
header[data-testid="stHeader"] { background: transparent; }
#MainMenu, footer { visibility: hidden; }
.block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 1400px; }

/* Sidebar */
[data-testid="stSidebar"] { background: __SIDEBAR__; border-right: 1px solid var(--border); }
[data-testid="stSidebarNav"]::before { content: "\\26A1  GRIDWATCH AI"; display: block; padding: 1.1rem 1rem .2rem 1rem;
    font-weight: 800; letter-spacing: .14em; font-size: 1.02rem; color: var(--cyan); }
[data-testid="stSidebarNav"]::after { content: "Grid Sensor Anomaly Watch"; display: block; padding: 0 1rem .9rem 1rem;
    font-size: .74rem; letter-spacing: .06em; color: var(--muted); border-bottom: 1px solid var(--border); margin-bottom: .4rem; }
[data-testid="stSidebar"] .gw-side-title { font-size: .7rem; letter-spacing: .12em; text-transform: uppercase;
    color: var(--muted); margin: 1rem 0 .4rem 0; }

/* Page titles */
.gw-title { font-size: 1.85rem; font-weight: 750; letter-spacing: -.01em; margin: 0; color: var(--text); }
.gw-sub { color: var(--muted); font-size: .95rem; margin: .15rem 0 1.4rem 0; }
.gw-section { font-size: 1.02rem; font-weight: 650; margin: 1.6rem 0 .6rem 0; color: var(--text); }
.gw-section span { color: var(--muted); font-weight: 400; font-size: .85rem; margin-left: .5rem; }

/* Cards */
.gw-card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 1.05rem 1.2rem; }
.gw-kpi { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 1rem 1.15rem;
          border-top: 3px solid var(--accent, var(--cyan)); height: 100%; }
.gw-kpi-label { color: var(--muted); font-size: .74rem; letter-spacing: .09em; text-transform: uppercase; }
.gw-kpi-value { font-size: 2rem; font-weight: 750; margin-top: .3rem; color: var(--text); line-height: 1.1; }
.gw-kpi-sub { color: var(--muted); font-size: .8rem; margin-top: .35rem; }

/* Badges */
.gw-badge { display: inline-block; padding: .12rem .6rem; border-radius: 999px; font-size: .72rem; font-weight: 650;
            letter-spacing: .04em; border: 1px solid currentColor; background: rgba(255,255,255,.03); white-space: nowrap; }

/* Tables */
.gw-table-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: 14px; background: var(--card); }
table.gw-table { width: 100%; border-collapse: collapse; font-size: .85rem; }
table.gw-table th { text-align: left; color: var(--muted); font-weight: 600; font-size: .72rem; letter-spacing: .08em;
    text-transform: uppercase; padding: .75rem .9rem; border-bottom: 1px solid var(--border); white-space: nowrap; }
table.gw-table td { padding: .7rem .9rem; border-bottom: 1px solid rgba(148,163,184,.08); color: var(--text); vertical-align: top; }
table.gw-table tr:last-child td { border-bottom: none; }
table.gw-table tr:hover td { background: rgba(34,211,238,.04); }

/* Asset tiles */
.gw-tile { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: .95rem 1.05rem;
           border-left: 4px solid var(--accent, var(--teal)); }
.gw-tile-id { font-weight: 750; font-size: 1.05rem; }
.gw-tile-meta { color: var(--muted); font-size: .78rem; margin: .1rem 0 .6rem 0; }
.gw-tile-row { display: flex; justify-content: space-between; font-size: .8rem; color: var(--muted); margin-top: .22rem; }
.gw-tile-row b { color: var(--text); font-weight: 600; }

/* Status dots (system page) */
.gw-dot { display: inline-block; width: .6rem; height: .6rem; border-radius: 50%; margin-right: .5rem; }

/* Streamlit widgets */
.stButton > button { border-radius: 10px; border: 1px solid var(--border); background: var(--card); color: var(--text); }
.stButton > button:hover { border-color: var(--cyan); color: var(--cyan); }
.stButton > button[kind="primary"] { background: var(--cyan); color: #04202A; border: none; font-weight: 650; }
[data-testid="stChatMessage"] { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: .8rem 1rem; }
[data-testid="stChatInput"] { border-radius: 12px; }
button[data-baseweb="tab"] { color: var(--muted); }
button[data-baseweb="tab"][aria-selected="true"] { color: var(--cyan); }
[data-testid="stExpander"] { background: var(--card); border: 1px solid var(--border); border-radius: 12px; }
div[data-testid="stDataFrame"] { border: 1px solid var(--border); border-radius: 12px; }
</style>
"""


def inject_css() -> None:
    css = (_CSS.replace("__NAVY__", NAVY).replace("__CARD__", CARD).replace("__CYAN__", CYAN)
           .replace("__TEAL__", TEAL).replace("__AMBER__", AMBER).replace("__CORAL__", CORAL)
           .replace("__TEXT__", TEXT).replace("__MUTED__", MUTED).replace("__BORDER__", BORDER)
           .replace("__SIDEBAR__", SIDEBAR))
    st.markdown(css, unsafe_allow_html=True)


def esc(value) -> str:
    """HTML-escape any value (AI notes and database text are never trusted as HTML)."""
    return html.escape("" if value is None or (isinstance(value, float) and pd.isna(value)) else str(value))


# ---- Components -----------------------------------------------------------------------------
def page_header(title: str, subtitle: str = "") -> None:
    st.markdown(f'<p class="gw-title">{esc(title)}</p><p class="gw-sub">{esc(subtitle)}</p>', unsafe_allow_html=True)


def section(title: str, hint: str = "") -> None:
    hint_html = f"<span>{esc(hint)}</span>" if hint else ""
    st.markdown(f'<div class="gw-section">{esc(title)}{hint_html}</div>', unsafe_allow_html=True)


def kpi_card(label: str, value, sub: str = "", accent: str = CYAN) -> str:
    return (f'<div class="gw-kpi" style="--accent:{accent}"><div class="gw-kpi-label">{esc(label)}</div>'
            f'<div class="gw-kpi-value">{esc(value)}</div><div class="gw-kpi-sub">{esc(sub)}</div></div>')


def kpi_row(items: Sequence[tuple]) -> None:
    """items: (label, value, sub, accent)"""
    cols = st.columns(len(items))
    for col, item in zip(cols, items):
        label, value = item[0], item[1]
        sub = item[2] if len(item) > 2 else ""
        accent = item[3] if len(item) > 3 else CYAN
        col.markdown(kpi_card(label, value, sub, accent), unsafe_allow_html=True)


def badge(text) -> str:
    key = str(text or "").upper()
    color = SEVERITY_COLORS.get(key) or STATUS_COLORS.get(key) or MUTED
    label = key.replace("_", " ") if key else "N/A"
    return f'<span class="gw-badge" style="color:{color}">{esc(label)}</span>'


def empty_state(message: str, hint: str = "") -> None:
    hint_html = f'<div style="color:{MUTED};font-size:.85rem;margin-top:.3rem">{esc(hint)}</div>' if hint else ""
    st.markdown(f'<div class="gw-card" style="text-align:center;padding:2rem">{esc(message)}{hint_html}</div>',
                unsafe_allow_html=True)


def fmt_ts(value) -> str:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return ""
    try:
        return pd.Timestamp(value).strftime("%b %d, %H:%M")
    except Exception:
        return str(value)


def html_table(df: pd.DataFrame, columns: Iterable[tuple], badge_cols: Optional[set] = None,
               time_cols: Optional[set] = None, max_rows: int = 50) -> None:
    """Render a DataFrame as a styled table. columns: (dataframe_column, header_label)."""
    badge_cols, time_cols = badge_cols or set(), time_cols or set()
    cols = [(c, label) for c, label in columns if c in df.columns]
    head = "".join(f"<th>{esc(label)}</th>" for _, label in cols)
    body = []
    for _, row in df.head(max_rows).iterrows():
        cells = []
        for c, _ in cols:
            v = row[c]
            if c in badge_cols:
                cells.append(f"<td>{badge(v)}</td>")
            elif c in time_cols:
                cells.append(f"<td>{esc(fmt_ts(v))}</td>")
            elif isinstance(v, float):
                cells.append(f"<td>{esc(round(v, 2))}</td>")
            else:
                cells.append(f"<td>{esc(v)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    st.markdown(f'<div class="gw-table-wrap"><table class="gw-table"><thead><tr>{head}</tr></thead>'
                f'<tbody>{"".join(body)}</tbody></table></div>', unsafe_allow_html=True)


def style_fig(fig, height: int = 340, legend: bool = True):
    """Dark, minimal Plotly styling that matches the app."""
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=28, b=8),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=MUTED, size=12), showlegend=legend,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0, bgcolor="rgba(0,0,0,0)"),
        hoverlabel=dict(bgcolor=CARD, font_color=TEXT, bordercolor=BORDER),
        title=dict(font=dict(color=TEXT, size=14)),
    )
    fig.update_xaxes(gridcolor="rgba(148,163,184,0.10)", zerolinecolor="rgba(148,163,184,0.10)", linecolor=BORDER)
    fig.update_yaxes(gridcolor="rgba(148,163,184,0.10)", zerolinecolor="rgba(148,163,184,0.10)", linecolor=BORDER)
    return fig


def show_fig(fig, height: int = 340, legend: bool = True) -> None:
    st.plotly_chart(style_fig(fig, height, legend), use_container_width=True, config={"displayModeBar": False})
