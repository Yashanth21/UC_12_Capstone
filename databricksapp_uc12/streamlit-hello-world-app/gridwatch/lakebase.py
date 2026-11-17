"""Lakebase (PostgreSQL) access for the existing `critical_grid_alerts` table.

* Driver: pg8000 (pure Python, no native libraries).
* Authentication: a short-lived OAuth token from the Databricks SDK is used as the password.
  The Postgres user is the app's service principal. Nothing is stored in code.
* The table already exists (created by the alert-routing notebook). This module only reads it,
  and can set `alert_status` when someone acknowledges an alert.
"""
import pandas as pd
import pg8000.dbapi
import streamlit as st

from . import config, db


class LakebaseError(Exception):
    """A readable error for the UI."""


def _qident(*parts: str) -> str:
    return ".".join('"' + str(p).replace('"', '""') + '"' for p in parts)


TABLE = _qident(config.LAKEBASE_SCHEMA, config.LAKEBASE_ALERTS_TABLE)

# Columns the page would like to show, in order. Only the ones that really exist are selected.
WANTED_COLUMNS = [
    "incident_id", "asset_id", "asset_type", "substation_name", "maintenance_status", "severity",
    "triggered_metrics", "likely_category", "deviation", "incident_note", "recommended_check",
    "window_start", "window_end", "alert_status", "alert_created_at", "alert_updated_at",
]


def _friendly(exc: Exception) -> str:
    text = str(exc)
    low = text.lower()
    if "42501" in text or "permission denied" in low:
        return ("The app's Postgres role has no access to the alerts table. Run the Lakebase grants in the "
                "README, step 4.")
    if "does not exist" in low or "42p01" in low:
        return f"The table {config.LAKEBASE_SCHEMA}.{config.LAKEBASE_ALERTS_TABLE} was not found in Lakebase."
    if "timeout" in low or "timed out" in low or "could not connect" in low or "connection" in low:
        return "Could not reach Lakebase. The compute may be starting from zero; wait a minute and refresh."
    if "password authentication" in low or "28p01" in low or "role" in low and "does not exist" in low:
        return "Lakebase rejected the login. Check that the app has the Lakebase resource and a Postgres role."
    return text[:400]


def connect():
    """Open a Postgres connection with a fresh OAuth token as the password."""
    if not config.lakebase_configured():
        raise LakebaseError("Lakebase is not configured. Add the Lakebase resource and LAKEBASE_ENDPOINT (README, step 2).")
    try:
        token = db.get_workspace_client().postgres.generate_database_credential(
            endpoint=config.LAKEBASE_ENDPOINT).token
        return pg8000.dbapi.connect(
            user=config.PGUSER, password=token, host=config.PGHOST, port=config.PGPORT,
            database=config.PGDATABASE, ssl_context=True, timeout=30)
    except LakebaseError:
        raise
    except Exception as exc:                          # noqa: BLE001
        raise LakebaseError(_friendly(exc)) from exc


def _columns(cur) -> list[str]:
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
                (config.LAKEBASE_SCHEMA, config.LAKEBASE_ALERTS_TABLE))
    return [r[0] for r in cur.fetchall()]


@st.cache_data(ttl=60, show_spinner=False)
def fetch_alerts(limit: int = 500) -> pd.DataFrame:
    """All alerts, newest first. Reads the real column list instead of assuming it."""
    conn = connect()
    try:
        cur = conn.cursor()
        existing = _columns(cur)
        if not existing:
            raise LakebaseError(f"The table {config.LAKEBASE_SCHEMA}.{config.LAKEBASE_ALERTS_TABLE} was not found "
                                "(or the app role cannot see it). See README, step 4.")
        selected = [c for c in WANTED_COLUMNS if c in existing] or existing
        order = "window_start" if "window_start" in selected else selected[0]
        cur.execute(f"SELECT {', '.join(_qident(c) for c in selected)} FROM {TABLE} "
                    f"ORDER BY {_qident(order)} DESC NULLS LAST LIMIT %s", (int(limit),))
        return pd.DataFrame([list(r) for r in cur.fetchall()], columns=selected)
    except LakebaseError:
        raise
    except Exception as exc:                          # noqa: BLE001
        raise LakebaseError(_friendly(exc)) from exc
    finally:
        conn.close()


def acknowledge(incident_id: str) -> None:
    """Mark one alert as ACKNOWLEDGED (needs UPDATE permission on the table)."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute(f"UPDATE {TABLE} SET alert_status = %s, alert_updated_at = now() WHERE incident_id = %s",
                    ("ACKNOWLEDGED", incident_id))
        conn.commit()
    except Exception as exc:                          # noqa: BLE001
        raise LakebaseError(_friendly(exc)) from exc
    finally:
        conn.close()
    fetch_alerts.clear()


def health_check() -> dict:
    """Connect and count rows (used by the System page)."""
    conn = connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_user, current_database()")
        user, database = cur.fetchone()
        cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
        return {"user": user, "database": database, "alerts": cur.fetchone()[0]}
    except Exception as exc:                          # noqa: BLE001
        raise LakebaseError(_friendly(exc)) from exc
    finally:
        conn.close()
