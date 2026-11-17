# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # UC-12 Agent - Notebook 3: Log, register and deploy
# MAGIC **What this notebook does**
# MAGIC 1. **Logs** the agent (`agent.py`) with MLflow, declaring the resources it uses (LLM, Genie Space, UC function, tables).
# MAGIC 2. **Registers** it in Unity Catalog as `uc_grid_sensor.grid_agent.grid_incident_agent`.
# MAGIC 3. **Deploys** it to a model serving endpoint and prints the **endpoint name and URL**.
# MAGIC 4. Sends a test question to the endpoint.
# MAGIC
# MAGIC Run notebooks 1 and 2 first. `agent.py` must be in the same folder. Deployment can take up to about 15 minutes.

# COMMAND ----------

# MAGIC %pip install -U "mlflow>=3.1.3" "databricks-agents>=1.1.0" databricks-mcp "databricks-sdk[openai]" databricks-openai

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksGenieSpace,
    DatabricksFunction,
    DatabricksTable,
    DatabricksSQLWarehouse,
)

mlflow.set_registry_uri("databricks-uc")

# Traces from the deployed endpoint are written to the active experiment, so set a normal
# (not Git-linked) experiment BEFORE deploying.
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/uc12-grid-agent")

UC_MODEL_NAME = "uc_grid_sensor.grid_agent.grid_incident_agent"

# Must match the settings at the top of agent.py
from agent import LLM_ENDPOINT, GENIE_SPACE_ID, WAREHOUSE_ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Log the agent
# MAGIC The **resources** list tells Databricks what the deployed agent is allowed to use, so it can give the endpoint the right access automatically:
# MAGIC the LLM endpoint, your Genie Space, the UC function, and the gold tables the Genie Space reads.
# MAGIC
# MAGIC The SQL warehouse is added automatically when you set `WAREHOUSE_ID` in `agent.py` (needed in direct-SDK mode).

# COMMAND ----------

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksGenieSpace(genie_space_id=GENIE_SPACE_ID),
    DatabricksSQLWarehouse(warehouse_id=WAREHOUSE_ID),
    DatabricksFunction(function_name="uc_grid_sensor.grid_agent.get_incident_notes"),

    # gold tables
    DatabricksTable(table_name="uc_grid_sensor.grid_gold.gold_anomaly_readings"),
    DatabricksTable(table_name="uc_grid_sensor.grid_gold.gold_anomaly_windows"),
    DatabricksTable(table_name="uc_grid_sensor.grid_gold.gold_asset_health"),
    DatabricksTable(table_name="uc_grid_sensor.grid_gold.gold_incident_notes"),

    # metric views (these were missing)
    DatabricksTable(table_name="uc_grid_sensor.grid_gold.mv_anomaly_readings"),
    DatabricksTable(table_name="uc_grid_sensor.grid_gold.mv_anomaly_windows"),
    DatabricksTable(table_name="uc_grid_sensor.grid_gold.mv_asset_health"),
    DatabricksTable(table_name="uc_grid_sensor.grid_gold.mv_incident_notes"),
]

# If the agent runs in direct-SDK mode (managed MCP not available), it needs the SQL warehouse too.
# Set WAREHOUSE_ID at the top of agent.py.
if WAREHOUSE_ID:
    from mlflow.models.resources import DatabricksSQLWarehouse
    resources.append(DatabricksSQLWarehouse(warehouse_id=WAREHOUSE_ID))

input_example = {"input": [{"role": "user", "content": "How many critical readings are there in total?"}]}

with mlflow.start_run(run_name="grid_incident_agent"):
    logged_agent = mlflow.pyfunc.log_model(
        name="agent",
        python_model="agent.py",          # models-from-code: the file we wrote
        resources=resources,
        input_example=input_example,
        pip_requirements=[
            f"mlflow=={mlflow.__version__}",
            "databricks-mcp",
            "databricks-sdk[openai]",
            "databricks-openai",
            "databricks-agents",
        ],
    )

print("Logged model:", logged_agent.model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register in Unity Catalog
# MAGIC Registering gives the agent a name, a version number and permissions in Unity Catalog.

# COMMAND ----------

uc_model_info = mlflow.register_model(model_uri=logged_agent.model_uri, name=UC_MODEL_NAME)
print(f"Registered {UC_MODEL_NAME} version {uc_model_info.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Deploy to a serving endpoint
# MAGIC `agents.deploy` creates the endpoint, gives it access to the declared resources, and connects it to MLflow tracing.
# MAGIC It can take up to about 15 minutes. If you re-run with a new version, the same endpoint is updated.

# COMMAND ----------

from databricks import agents

deployment = agents.deploy(UC_MODEL_NAME, uc_model_info.version,scale_to_zero=True)

print("Endpoint name :", deployment.endpoint_name)
print("Query endpoint:", deployment.query_endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC **Save these two values** (endpoint name and query URL). The Databricks App in the next step needs the endpoint name.
# MAGIC You can also find it any time under **Serving** in the left menu. Wait until its state is **Ready**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test the live endpoint

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

response = client.predict(
    endpoint=deployment.endpoint_name,
    inputs={"input": [{"role": "user", "content": "Which asset is under maintenance, and what was its latest critical incident?"}]},
)

for item in response["output"]:
    if item.get("type") == "message":
        print("".join(c.get("text", "") for c in item.get("content", [])))

# COMMAND ----------

# MAGIC %md
# MAGIC ## After deployment (settings, no code)
# MAGIC 1. **AI Gateway limits (S8 requirement):** open **Serving**, click your endpoint, open the **AI Gateway** tab (or the edit/gateway option), and set a rate limit (for example, a per-user queries-per-minute cap). Keep a screenshot as evidence.
# MAGIC 2. **Tracing in production:** open the endpoint's **Traces** or your `uc12-grid-agent` experiment to see traces from live requests.
# MAGIC 3. **Databricks App:** the chat UI calls this endpoint by its name.
# MAGIC
# MAGIC **If `agents.deploy` is not allowed on your Free Edition workspace**, the error message will say so. In that case the fallback is to run the same `agent.py` inside a Databricks App instead of a serving endpoint. Tell me the error and I will adjust.