"""Unity Catalog access through the app's SQL warehouse resource.

* Authentication: the app's own service principal, picked up automatically by `Config()`.
  No token or password is stored anywhere in this code.
* All values go through named parameters (`:name`), never string formatting.
  Only table names (from config.py) are formatted into SQL.
"""
import pandas as pd
import streamlit as st
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from . import config


class DataError(Exception):
    """A readable error for the UI."""


def T(table: str, silver: bool = False) -> str:
    """Fully qualified, quoted table name for a UC-12 table."""
    schema = config.SILVER_SCHEMA if silver else config.GOLD_SCHEMA
    return f"`{config.CATALOG}`.`{schema}`.`{table}`"


@st.cache_resource(show_spinner=False)
def get_workspace_client() -> WorkspaceClient:
    # A long HTTP timeout because the agent can take a while (Genie + model).
    return WorkspaceClient(config=Config(http_timeout_seconds=300))


@st.cache_resource(show_spinner=False)
def _connection():
    cfg = Config()
    return dbsql.connect(
        server_hostname=cfg.host.replace("https://", "").rstrip("/"),
        http_path=f"/sql/1.0/warehouses/{config.WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


def _execute(statement: str, params: dict) -> pd.DataFrame:
    last_error = None
    for attempt in (1, 2):                      # one retry with a fresh connection
        try:
            with _connection().cursor() as cursor:
                cursor.execute(statement, params or None)
                try:
                    return cursor.fetchall_arrow().to_pandas()
                except Exception:
                    rows = cursor.fetchall()
                    cols = [d[0] for d in cursor.description]
                    return pd.DataFrame([list(r) for r in rows], columns=cols)
        except Exception as exc:                # noqa: BLE001
            last_error = exc
            _connection.clear()
    raise DataError(str(last_error))


@st.cache_data(ttl=config.CACHE_SECONDS, show_spinner=False)
def _cached(statement: str, params_items: tuple) -> pd.DataFrame:
    return _execute(statement, dict(params_items))


def query(statement: str, params: dict | None = None, cache: bool = True) -> pd.DataFrame:
    items = tuple(sorted((params or {}).items()))
    return _cached(statement, items) if cache else _execute(statement, dict(items))


def require_sql() -> None:
    """Stop the page with a clear message if the SQL warehouse resource is missing."""
    if not config.sql_configured():
        st.error("The SQL warehouse is not configured.")
        st.markdown(
            "Add a **SQL warehouse** resource to the app (resource key `sql-warehouse`) and make sure "
            "`app.yaml` maps it to `DATABRICKS_WAREHOUSE_ID`. See the README, step 2."
        )
        st.stop()


def load(statement: str, params: dict | None = None, what: str = "data", cache: bool = True) -> pd.DataFrame:
    """Run a query; on failure show a friendly error and stop this page."""
    require_sql()
    try:
        return query(statement, params, cache=cache)
    except DataError as exc:
        text = str(exc)
        st.error(f"Could not load {what}.")
        if "PERMISSION" in text.upper() or "INSUFFICIENT" in text.upper() or "USE CATALOG" in text.upper():
            st.markdown("The app's service principal needs access to the tables. Run the grants in the README, step 3.")
        elif "TABLE_OR_VIEW_NOT_FOUND" in text.upper() or "NOT FOUND" in text.upper():
            st.markdown("A table was not found. Check that the UC-12 pipeline has run and the catalog and schema names are right.")
        elif "WAREHOUSE" in text.upper() or "STOPPED" in text.upper():
            st.markdown("The SQL warehouse may be starting or stopped. Wait a minute and refresh.")
        with st.expander("Technical details"):
            st.code(text[:1500])
        st.stop()


def in_clause(prefix: str, values: list) -> tuple[str, dict]:
    """Build `:p0, :p1, ...` placeholders and their parameter dict for an IN (...) list."""
    params = {f"{prefix}{i}": v for i, v in enumerate(values)}
    return ", ".join(f":{k}" for k in params), params


def clear_cache() -> None:
    st.cache_data.clear()
