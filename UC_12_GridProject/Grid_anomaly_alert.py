# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "\"databricks-sdk>=0.89.0\"",
#   "pg8000",
#   "\"databricks-sdk==0.89.0\"",
# ]
# ///
# MAGIC %md
# MAGIC # UC-12 Severity-Based Alert Routing to Lakebase (fixed version)
# MAGIC **Rule:** only `CRITICAL` AI-generated incident notes are written to the Lakebase (PostgreSQL) table **`critical_grid_alerts`**. WARNING and any other severity are never routed.
# MAGIC
# MAGIC **What changed vs. the original**
# MAGIC * `psycopg[binary]` was replaced by **`pg8000`**, a pure-Python Postgres driver. The binary psycopg wheel bundles its own `libpq`/OpenSSL, which can segfault the Python process on Databricks (that was the crash).
# MAGIC * `-U` removed from the `%pip` line so the runtime's own packages are not force-upgraded.
# MAGIC * Empty-result guards added (no critical rows no longer breaks the Postgres steps).
# MAGIC * Connections are always closed, even when a cell fails.
# MAGIC
# MAGIC **Where each step runs**
# MAGIC * **SPARK** cells read and filter the Delta table `uc_grid_sensor.grid_gold.gold_incident_notes` (Unity Catalog).
# MAGIC * **POSTGRES** cells talk to Lakebase. Nothing is substituted with a Delta table: if Lakebase is not reachable, the notebook stops and tells you what to fix.
# MAGIC
# MAGIC **Prerequisites (details in Step 4)**
# MAGIC 1. A Lakebase project exists in your workspace (Free Edition gives you one).
# MAGIC 2. You have its **host**, **database** and **endpoint name** (Lakebase console > your project > **Connect**).
# MAGIC 3. You can log in with OAuth: either as yourself, or as a service principal whose credentials are stored in a Databricks **secret scope**. No password is ever typed into this notebook.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk==0.89.0"

# COMMAND ----------

# MAGIC %pip install pg8000

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - SPARK: inspect the source table
# MAGIC We read the real schema first and use the real column names. Nothing is assumed.

# COMMAND ----------

# ----- SPARK -----
from pyspark.sql import functions as F

SOURCE_TABLE = "uc_grid_sensor.grid_gold.gold_incident_notes"

source_df = spark.table(SOURCE_TABLE)
print("Columns:", source_df.columns)
source_df.printSchema()
print("Total rows in source:", source_df.count())
display(source_df.limit(10))

if "severity" not in source_df.columns:
    raise ValueError(f"{SOURCE_TABLE} has no 'severity' column, so severity-based routing is not possible.")

# How many rows per severity value (shows if there are odd spellings or blanks)
display(source_df.groupBy("severity").count().orderBy("severity"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - SPARK: keep only CRITICAL rows
# MAGIC `upper(trim(...))` makes the filter safe against `critical`, `Critical` or ` CRITICAL `. Every other value is dropped.

# COMMAND ----------

# ----- SPARK -----
severity_clean = F.upper(F.trim(F.col("severity").cast("string")))

critical_df = source_df.filter(severity_clean == "CRITICAL").cache()
print("Critical rows selected:", critical_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - SPARK: validate the selection

# COMMAND ----------

# ----- SPARK -----
critical_count = critical_df.count()
not_critical = critical_df.filter(severity_clean != "CRITICAL").count()
distinct_severities = [r[0] for r in critical_df.select(severity_clean.alias("s")).distinct().collect()]

print(f"Critical rows            : {critical_count}")
print(f"Non-critical rows inside : {not_critical}   (must be 0)")
print(f"Distinct severities      : {distinct_severities}   (must be ['CRITICAL'] or empty)")
assert not_critical == 0, "A non-critical row slipped into the selection"
assert set(distinct_severities) <= {"CRITICAL"}, "Unexpected severity values in the selection"

if critical_count == 0:
    print("\nNo CRITICAL rows found. Check that the pipeline (including the AI notes file) has run.")
display(critical_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Connect to Lakebase (read this once)
# MAGIC Lakebase is PostgreSQL. You connect with a standard Postgres driver (`pg8000`) and an **OAuth token** from the Databricks SDK (`generate_database_credential`) used as the password. The token lasts 60 minutes, so the notebook asks for a fresh one each time it connects.
# MAGIC
# MAGIC **Get your connection details** (Lakebase console > your project > **Connect**, choose branch and endpoint):
# MAGIC * **Host**, e.g. `<endpoint-id>.database.<region>.cloud.databricks.com`
# MAGIC * **Database**, usually `databricks_postgres`
# MAGIC * **Endpoint name**, format `projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id>`
# MAGIC
# MAGIC **Choose an authentication mode** (set `AUTH_MODE` in the next cell):
# MAGIC * `user_oauth` (simplest): the notebook connects as **you** (your Databricks email). You need access to the Lakebase project (the project owner has it). This exact notebook flow is not confirmed in the docs; the Lakebase **Connect** dialog offers OAuth with your own login. Try this first.
# MAGIC * `service_principal_oauth` (the documented route): create a service principal with an OAuth secret, turn on **Workspace access** for it, then create its Postgres role by running this in the Lakebase SQL editor (replace `<client-id>` with the service principal's client ID, a UUID):
# MAGIC ```sql
# MAGIC CREATE EXTENSION IF NOT EXISTS databricks_auth;
# MAGIC SELECT databricks_create_role('<client-id>', 'SERVICE_PRINCIPAL');
# MAGIC GRANT CONNECT ON DATABASE databricks_postgres TO "<client-id>";
# MAGIC GRANT USAGE, CREATE ON SCHEMA public TO "<client-id>";
# MAGIC GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "<client-id>";
# MAGIC ```
# MAGIC Store the client ID and OAuth secret in a **Databricks secret scope** (for example scope `lakebase`, keys `sp-client-id` and `sp-client-secret`). Never paste them here.

# COMMAND ----------

# ----- CONFIGURATION: the only cell where you type values -----
AUTH_MODE = "user_oauth"            # "user_oauth" or "service_principal_oauth"

PGHOST        = "ep-hidden-frog-d8xv7o5x.database.us-east-2.cloud.databricks.com"
PGDATABASE    = "databricks_postgres"
PGPORT        = 5432
ENDPOINT_NAME = "projects/gridwatch/branches/production/endpoints/primary"

# Only used when AUTH_MODE = "service_principal_oauth" (values live in a secret scope, not here)
SECRET_SCOPE             = "lakebase"
SECRET_KEY_CLIENT_ID     = "sp-client-id"
SECRET_KEY_CLIENT_SECRET = "sp-client-secret"

TARGET_TABLE = "critical_grid_alerts"
TARGET_SCHEMA = "public"

# COMMAND ----------

# ----- POSTGRES: connection helpers (pure-Python driver, no native libraries) -----
from contextlib import contextmanager
import pg8000.dbapi
from databricks.sdk import WorkspaceClient

LAKEBASE_READY = False


def qident(*parts):
    """Safely quote SQL identifiers (schema, table, column names)."""
    return ".".join('"' + str(p).replace('"', '""') + '"' for p in parts)


TABLE_ID = qident(TARGET_SCHEMA, TARGET_TABLE)


def check_config():
    """Stop early, with a clear message, if the settings above were not filled in."""
    problems = []
    if not PGHOST or "<" in PGHOST:
        problems.append("PGHOST is not set")
    if not ENDPOINT_NAME or "<" in ENDPOINT_NAME:
        problems.append("ENDPOINT_NAME is not set")
    if AUTH_MODE not in ("user_oauth", "service_principal_oauth"):
        problems.append("AUTH_MODE must be 'user_oauth' or 'service_principal_oauth'")
    if problems:
        raise ValueError("Lakebase is not configured yet: " + "; ".join(problems) +
                         ". Fill in the configuration cell (Lakebase console > Connect).")


def _identity():
    """Return (workspace_client, postgres_user) for the chosen auth mode."""
    if AUTH_MODE == "user_oauth":
        return WorkspaceClient(), spark.sql("SELECT current_user()").first()[0]   # your own login
    client_id = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY_CLIENT_ID)
    client_secret = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY_CLIENT_SECRET)
    host = WorkspaceClient().config.host
    return WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret), client_id


def connect_lakebase():
    """Open a Postgres connection using a fresh 60-minute OAuth token as the password."""
    check_config()
    w, pg_user = _identity()
    credential = w.postgres.generate_database_credential(endpoint=ENDPOINT_NAME)
    return pg8000.dbapi.connect(
        user=pg_user,
        password=credential.token,
        host=PGHOST,
        port=PGPORT,
        database=PGDATABASE,
        ssl_context=True,        # TLS required by Lakebase
        timeout=60,
    )


@contextmanager
def lakebase_conn():
    """Connection that is always closed, even if the cell fails (uncommitted work is discarded)."""
    conn = connect_lakebase()
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def require_lakebase():
    if not LAKEBASE_READY:
        raise RuntimeError("Lakebase connection has not succeeded yet. Fix the previous cell first. "
                           "Rows are NOT written anywhere else on purpose.")


def sqlstate(exc):
    """Extract the 5-character Postgres error code from a pg8000 exception, if present."""
    arg = exc.args[0] if exc.args else None
    if isinstance(arg, dict):
        return arg.get("C")
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the connection
# MAGIC The first connection after a quiet period can take a while because Lakebase compute starts from zero.

# COMMAND ----------

# ----- POSTGRES -----
try:
    with lakebase_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT current_user, current_database()")
        print("Connected as / to:", cur.fetchone())
    LAKEBASE_READY = True
except ValueError as e:
    print(e)
except Exception as e:
    print(f"Could not connect to Lakebase: {type(e).__name__}: {e}\n")
    print("What to check:")
    print(" 1. Is the Lakebase project running (open it in the console)? Retry once; the first start can time out.")
    print(" 2. Are PGHOST and ENDPOINT_NAME copied exactly from the Connect dialog?")
    print(" 3. user_oauth: do you have access to the project? If not, switch to service_principal_oauth.")
    print(" 4. service_principal_oauth: is 'Workspace access' enabled, does the Postgres role exist (SQL in Step 4),")
    print("    and are the secret scope and keys correct?")
    print("The notebook stops here on purpose. It will not write the alerts anywhere else.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - POSTGRES: create the destination table
# MAGIC * `incident_id` is the **primary key** (taken from the source window/incident ID), so one incident can only have one alert.
# MAGIC * The `CHECK` rule makes the database itself refuse any severity other than `CRITICAL`.
# MAGIC * `alert_created_at` is filled in by the database. `alert_status` starts as `NEW`.

# COMMAND ----------

# ----- POSTGRES -----
require_lakebase()

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_ID} (
    incident_id        TEXT PRIMARY KEY,
    asset_id           TEXT,
    asset_type         TEXT,
    substation_name    TEXT,
    maintenance_status TEXT,
    severity           TEXT NOT NULL CHECK (severity = 'CRITICAL'),
    triggered_metrics  TEXT,
    likely_category    TEXT,
    deviation          TEXT,
    incident_note      TEXT,
    recommended_check  TEXT,
    window_start       TIMESTAMPTZ,
    window_end         TIMESTAMPTZ,
    alert_status       TEXT NOT NULL DEFAULT 'NEW',
    alert_created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    alert_updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

with lakebase_conn() as conn:
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
print(f"Table {TARGET_SCHEMA}.{TARGET_TABLE} is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6a - SPARK: map the source columns to the alert columns
# MAGIC For each alert column we look for a matching column in the real source schema. If none exists the value is stored as `NULL` (nothing is invented). The incident ID and severity are required.

# COMMAND ----------

# ----- SPARK -----
# destination column -> source column names to look for (first one found is used)
COLUMN_CANDIDATES = {
    "incident_id":        ["window_id", "incident_id", "event_id"],
    "asset_id":           ["asset_id"],
    "asset_type":         ["asset_type"],
    "substation_name":    ["substation_name", "substation"],
    "maintenance_status": ["maintenance_status"],
    "severity":           ["severity"],
    "triggered_metrics":  ["triggered_metrics"],
    "likely_category":    ["likely_category", "category"],
    "deviation":          ["deviation"],
    "incident_note":      ["note", "incident_note", "summary"],
    "recommended_check":  ["recommended_check", "recommended_action"],
    "window_start":       ["window_start", "event_ts", "event_timestamp"],
    "window_end":         ["window_end"],
}
DEST_TYPES = {c: "string" for c in COLUMN_CANDIDATES}
DEST_TYPES.update({"window_start": "timestamp", "window_end": "timestamp"})
DEST_COLS = list(COLUMN_CANDIDATES)

available = {c.lower(): c for c in critical_df.columns}


def pick_source(candidates):
    for name in candidates:
        if name.lower() in available:
            return available[name.lower()]
    return None


mapping = {dest: pick_source(cands) for dest, cands in COLUMN_CANDIDATES.items()}

print("Alert column            <- Source column")
for dest, src in mapping.items():
    print(f"  {dest:<20} <- {src if src else 'NOT FOUND (will be NULL)'}")

missing_required = [d for d in ("incident_id", "severity") if mapping[d] is None]
if missing_required:
    raise ValueError(f"Required source columns not found for: {missing_required}. "
                     f"Available columns: {critical_df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6b - SPARK: build the rows to insert

# COMMAND ----------

# ----- SPARK -----
select_exprs = []
for dest, src in mapping.items():
    dtype = DEST_TYPES[dest]
    expr = F.col(src).cast(dtype) if src else F.lit(None).cast(dtype)
    select_exprs.append(expr.alias(dest))

alerts_df = (
    critical_df.select(*select_exprs)
    .withColumn("severity", F.upper(F.trim(F.col("severity"))))   # store the clean value
    .filter(F.col("incident_id").isNotNull())                     # an alert needs an ID
    .dropDuplicates(["incident_id"])                              # one alert per incident
    .cache()
)

alerts_count = alerts_df.count()
skipped = critical_df.count() - alerts_count
print("Alerts to write:", alerts_count, "| skipped (no ID or duplicate):", skipped)

# Small table (tens of rows), so collecting to the driver is fine
rows = [tuple(r[c] for c in DEST_COLS) for r in alerts_df.collect()]
incident_ids = [r[0] for r in rows]
display(alerts_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - POSTGRES: insert with upsert (no duplicates on re-run)
# MAGIC `ON CONFLICT (incident_id) DO UPDATE` inserts a new alert, or refreshes the text of an existing one. It **keeps** `alert_status` and `alert_created_at`, so re-running never creates duplicates and never resets an acknowledged alert. All values are passed as query parameters and column names are quoted by `qident`, so the SQL is safe.

# COMMAND ----------

# ----- POSTGRES -----
require_lakebase()

col_list = ", ".join(qident(c) for c in DEST_COLS)
placeholders = ", ".join(["%s"] * len(DEST_COLS))
update_list = ", ".join(f"{qident(c)} = EXCLUDED.{qident(c)}" for c in DEST_COLS if c != "incident_id")

UPSERT_SQL = (
    f"INSERT INTO {TABLE_ID} ({col_list}) VALUES ({placeholders}) "
    f"ON CONFLICT (incident_id) DO UPDATE SET {update_list}, alert_updated_at = now()"
)

if not rows:
    print("Nothing to write: there are no CRITICAL incidents with an ID.")
else:
    try:
        with lakebase_conn() as conn:
            cur = conn.cursor()
            # How many of these incidents already have an alert? (to report new vs refreshed)
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_ID} WHERE incident_id = ANY(%s)", (incident_ids,))
            already = cur.fetchone()[0]

            cur.executemany(UPSERT_SQL, rows)
            conn.commit()
        print(f"Done. {len(rows) - already} new alerts, {already} existing alerts refreshed.")
    except Exception as e:
        print(f"Insert failed: {type(e).__name__}: {e}")
        print("Nothing was written to any other table. Fix the error above and re-run this cell (it is safe to re-run).")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - POSTGRES: verify what is in Lakebase

# COMMAND ----------

# ----- POSTGRES (results shown with pandas) -----
import pandas as pd
require_lakebase()

with lakebase_conn() as conn:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_ID}")
    total_alerts = cur.fetchone()[0]

    cur.execute(f"""
        SELECT incident_id, asset_id, asset_type, severity, triggered_metrics, likely_category,
               recommended_check, incident_note, window_start, alert_status, alert_created_at
        FROM {TABLE_ID} ORDER BY window_start DESC NULLS LAST LIMIT 20
    """)
    cols = [d[0].decode() if isinstance(d[0], bytes) else d[0] for d in cur.description]
    latest = pd.DataFrame([list(r) for r in cur.fetchall()], columns=cols)

print("Total alerts in Lakebase:", total_alerts)
display(latest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Validate the routing
# MAGIC Three checks: (1) only CRITICAL rows are stored, (2) every critical incident from the Gold table has an alert, (3) the database refuses a WARNING row.

# COMMAND ----------

# ----- POSTGRES -----
require_lakebase()

with lakebase_conn() as conn:
    cur = conn.cursor()
    cur.execute(f"SELECT severity, COUNT(*) FROM {TABLE_ID} GROUP BY severity")
    by_severity = {r[0]: r[1] for r in cur.fetchall()}

    matched = 0
    if incident_ids:
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_ID} WHERE incident_id = ANY(%s)", (incident_ids,))
        matched = cur.fetchone()[0]

print("Alerts by severity in Lakebase:", by_severity)
assert set(by_severity) <= {"CRITICAL"}, "Non-critical rows found in Lakebase!"
print("Check 1 passed: only CRITICAL rows are stored")

print(f"Critical incidents in Gold: {len(incident_ids)} | found in Lakebase: {matched}")
assert matched == len(incident_ids), "Some critical incidents are missing from Lakebase"
print("Check 2 passed: every critical incident has an alert")

# Check 3: try to insert a WARNING row; the database must refuse it (SQLSTATE 23514 = check_violation)
refused = False
try:
    with lakebase_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {TABLE_ID} (incident_id, severity) VALUES (%s, %s)",
                    ("TEST_WARNING_ROW", "WARNING"))
        conn.commit()
except pg8000.dbapi.DatabaseError as e:
    if sqlstate(e) == "23514":
        refused = True
    else:
        raise

assert refused, "A WARNING row was accepted. The CHECK rule is missing."
print("Check 3 passed: the database refused a WARNING row")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run it automatically
# MAGIC Add this notebook as a **second task** in a Lakeflow Job that runs after your pipeline, so new critical notes are routed after every refresh. The notebook is safe to re-run.
# MAGIC
# MAGIC If `Step 4` failed, the notebook stops by design. The next practical step is to fix the connection settings (or switch `AUTH_MODE`), not to write the alerts to a Delta table instead.