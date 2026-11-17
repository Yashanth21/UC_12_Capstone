"""Central configuration for GRIDWATCH AI.

Nothing secret lives here. Everything comes from environment variables that Databricks Apps
injects (app.yaml `env` + app resources). See README.md for the list.
"""
import os


def _env(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()
    return default


APP_NAME = "GRIDWATCH AI"
APP_VERSION = "1.0"

# ---- Unity Catalog locations (the existing UC-12 objects) --------------------------------
CATALOG = _env("GRIDWATCH_CATALOG", default="uc_grid_sensor")
GOLD_SCHEMA = _env("GRIDWATCH_GOLD_SCHEMA", default="grid_gold")
SILVER_SCHEMA = _env("GRIDWATCH_SILVER_SCHEMA", default="grid_silver")

GOLD_TABLES = [
    "gold_anomaly_readings",
    "gold_anomaly_windows",
    "gold_asset_health",
    "gold_incident_notes",
]

# ---- Resources injected by Databricks Apps (via app.yaml valueFrom) -----------------------
WAREHOUSE_ID = _env("DATABRICKS_WAREHOUSE_ID", "WAREHOUSE_ID")
AGENT_ENDPOINT = _env("AGENT_ENDPOINT", "SERVING_ENDPOINT")

# Lakebase (PostgreSQL). PGHOST / PGDATABASE / PGPORT / PGUSER are injected when the app has a
# Lakebase (postgres) resource; LAKEBASE_ENDPOINT comes from app.yaml.
LAKEBASE_ENDPOINT = _env("LAKEBASE_ENDPOINT")
PGHOST = _env("PGHOST")
PGDATABASE = _env("PGDATABASE", default="databricks_postgres")
PGPORT = int(_env("PGPORT", default="5432"))
PGUSER = _env("PGUSER", "DATABRICKS_CLIENT_ID")   # an app's Postgres role is its service principal
LAKEBASE_SCHEMA = _env("LAKEBASE_SCHEMA", default="public")
LAKEBASE_ALERTS_TABLE = _env("LAKEBASE_ALERTS_TABLE", default="critical_grid_alerts")

CACHE_SECONDS = int(_env("GRIDWATCH_CACHE_SECONDS", default="300"))


def sql_configured() -> bool:
    return bool(WAREHOUSE_ID)


def agent_configured() -> bool:
    return bool(AGENT_ENDPOINT)


def lakebase_configured() -> bool:
    return bool(LAKEBASE_ENDPOINT and PGHOST and PGUSER)


# ---- Thresholds: the same limits the Gold pipeline uses (for chart reference lines) -------
VOLTAGE = {"nominal": 11.0, "warn": (10.5, 11.5), "crit": (10.0, 12.0)}
FREQUENCY = {"nominal": 50.0, "warn": (49.5, 50.5), "crit": (49.0, 51.0)}
TEMPERATURE = {
    "Generator": {"nominal": 80, "warn": 95, "crit": 110},
    "Transformer": {"nominal": 70, "warn": 85, "crit": 100},
    "Circuit Breaker": {"nominal": 45, "warn": 60, "crit": 75},
}

# UI bands for the Asset Health indicator (share of readings that were anomalous, in percent)
HEALTH_BANDS = (1.0, 3.0)   # below 1 = healthy, 1 to 3 = watch, above 3 = at risk

SUGGESTED_QUESTIONS = [
    "How many critical readings are there in total?",
    "Which substation has the most critical anomaly windows?",
    "Which assets are currently under maintenance?",
    "What was the latest critical incident for TR-002?",
    "Which asset has the highest anomaly rate?",
    "What was the hottest reading in the fleet?",
]
