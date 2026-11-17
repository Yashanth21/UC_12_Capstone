# GRIDWATCH AI

Production-style monitoring app for **UC-12 Grid Sensor Anomaly Watch**, built with Streamlit on Databricks Apps.
It reads your real Gold data through a SQL warehouse, chats with your **deployed agent endpoint**, and shows the
critical alerts stored in **Lakebase**. Nothing is mocked and no credentials are stored in the code.

## Pages

| Page | What it shows | Data |
|---|---|---|
| Fleet Overview | KPIs, anomaly trend, equipment and substation breakdown, recent critical events | Gold |
| Sensor Analytics | Voltage, frequency and temperature trends, asset filters, threshold lines, flagged readings | Silver telemetry + Gold |
| Asset Health | Health state per asset, filters, details, SCD2 registry history | Gold + Silver registry |
| Critical Alerts | Critical notes from Gold and alerts in Lakebase, sync check, acknowledge button | Gold + Lakebase |
| AI Incident Analysis | Pick an incident, the agent writes the summary and recommended checks | Gold + agent |
| AI Chat Assistant | Full-page chat with history, suggested questions, tool trace, transcript download | Agent |
| About / System Info | Architecture, data sources, live connection checks | All |

## Files

```
app.py                      entry point: theme, navigation, sidebar  (keeps the template's diagnostics bootstrap)
app.yaml                    runtime command + environment (values come from app resources)
requirements.txt            extra Python packages
.streamlit/config.toml      dark theme colours
gridwatch/config.py         all settings (environment variables) and thresholds
gridwatch/theme.py          palette, CSS, KPI cards, badges, tables, chart styling
gridwatch/db.py             SQL warehouse access (parameterized, cached)
gridwatch/agent_client.py   calls the deployed agent endpoint
gridwatch/lakebase.py       reads and updates the Lakebase alerts table
views/*.py                  the seven pages
```
Keep your template's `app_diagnostics.py` and `manifest.yaml` as they are.

## Deploy (5 steps)

**1. Copy the files into your app folder** (the Databricks Apps Streamlit template). Replace `app.py`, `app.yaml` and
`requirements.txt`; add the `gridwatch/`, `views/` and `.streamlit/` folders.

**2. Add three resources** (Apps > your app > Edit > Resources). The resource keys must match `app.yaml`:

| Resource | Key | Choose | Permission |
|---|---|---|---|
| SQL warehouse | `sql-warehouse` | your serverless warehouse | Can use |
| Serving endpoint | `serving-endpoint` | `agents_uc_grid_sensor-grid_agent-grid_incident_agent` | Can query |
| Lakebase (Postgres) | `postgres` | project `gridwatch`, branch `production`, database `databricks_postgres` | Can connect and create |

The agent endpoint name is read from the `AGENT_ENDPOINT` setting, so you can point the app at another endpoint
without touching the code.

**3. Give the app's service principal access to the data.** Copy its **client ID** from the app's Authorization tab,
then run in a SQL editor:

```sql
GRANT USE CATALOG ON CATALOG uc_grid_sensor TO `<app-client-id>`;
GRANT USE SCHEMA, SELECT ON SCHEMA uc_grid_sensor.grid_gold   TO `<app-client-id>`;
GRANT USE SCHEMA, SELECT ON SCHEMA uc_grid_sensor.grid_silver TO `<app-client-id>`;
```

**4. Give the app's Postgres role access to the existing alerts table.** Run in the Lakebase SQL editor:

```sql
GRANT USAGE ON SCHEMA public TO "<app-client-id>";
GRANT SELECT, UPDATE ON TABLE public.critical_grid_alerts TO "<app-client-id>";
```
`UPDATE` is only needed for the Acknowledge button. Without it the page still works and shows a clear message.

**5. Deploy**, open the app, go to **About / System Info** and click **Run connection checks**. All rows should be green.

## Settings (environment variables)

| Name | Source | Meaning |
|---|---|---|
| `DATABRICKS_WAREHOUSE_ID` | resource `sql-warehouse` | SQL warehouse used for all queries |
| `AGENT_ENDPOINT` | resource `serving-endpoint` | deployed agent endpoint |
| `LAKEBASE_ENDPOINT` | resource `postgres` | Lakebase endpoint, used to get a login token |
| `PGHOST`, `PGDATABASE`, `PGPORT`, `PGUSER` | injected by the Lakebase resource | connection details |
| `GRIDWATCH_CATALOG`, `GRIDWATCH_GOLD_SCHEMA`, `GRIDWATCH_SILVER_SCHEMA` | `app.yaml` | Unity Catalog locations |
| `LAKEBASE_ALERTS_TABLE`, `LAKEBASE_SCHEMA` | `app.yaml` | alerts table name and schema |
| `GRIDWATCH_CACHE_SECONDS` | optional | query cache time, default 300 |

## How it connects

* **Data:** `databricks-sql-connector` with the app's own service principal (`Config()`); all values are passed as
  named parameters, and results are cached for 5 minutes (use **Refresh data** in the sidebar).
* **Agent:** `POST /serving-endpoints/<name>/invocations` with `{"input": [{"role", "content"}]}`; the answer and the
  tools the agent used are read from the `output` list.
* **Lakebase:** `pg8000` with a short-lived OAuth token from `generate_database_credential` as the password.

## Troubleshooting

| Message | Fix |
|---|---|
| "SQL warehouse is not configured" | Add the `sql-warehouse` resource (step 2) and redeploy |
| "Could not load ... PERMISSION" | Run the grants in step 3 |
| "not allowed to query the agent endpoint" | Add the serving endpoint resource with Can query |
| "endpoint ... was not found" | Check the endpoint name in the resource / `AGENT_ENDPOINT` |
| Agent answers slowly the first time | The endpoint scales to zero; wait a minute and retry |
| "rate limit was reached" | AI Gateway limit on the endpoint; wait and retry |
| Lakebase "no access" or "not found" | Run the grants in step 4 and check the table name |
| Lakebase times out | The compute scales to zero; retry after a minute |
| App stops after 24 hours | Free Edition apps auto-stop; press Start in the Apps page |

## Notes

* Sensor Analytics and Asset Health also read `silver_grid_telemetry` and `silver_asset_registry` (full minute-level
  readings and the SCD2 history are not in Gold), so the grants in step 3 include the Silver schema.
* The health indicator uses the share of anomalous readings, with bands set in `gridwatch/config.py` (`HEALTH_BANDS`).
* Threshold lines on charts use the same limits as the Gold pipeline.
