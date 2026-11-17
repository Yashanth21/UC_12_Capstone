# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # UC-12 Agent - Notebook 1: Setup and quick test
# MAGIC **What this notebook does**
# MAGIC 1. Creates the schema `uc_grid_sensor.grid_agent` and the **UC function tool** `get_incident_notes`.
# MAGIC 2. Tests the function on its own.
# MAGIC 3. Runs the agent (from `agent.py`) on a few questions so you can see both tools working.
# MAGIC
# MAGIC **Before you start:** put `agent.py` in the same workspace folder as this notebook, and use serverless compute.

# COMMAND ----------

# MAGIC %pip install -U "mlflow>=3.1.3" "databricks-agents>=1.1.0" databricks-mcp "databricks-sdk[openai]" databricks-openai

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Schema and the UC function tool
# MAGIC The agent's second tool is a Unity Catalog function. It reads the AI-written notes in `gold_incident_notes` for ONE asset and returns them as JSON (newest first, max 30).
# MAGIC The comments on the function and its parameters are what the model reads to decide when and how to call it.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS uc_grid_sensor.grid_agent")

spark.sql("""
CREATE OR REPLACE FUNCTION uc_grid_sensor.grid_agent.get_incident_notes(
  p_asset_id STRING COMMENT 'Asset ID such as TR-002, GN-001 or CB-003',
  p_severity STRING COMMENT 'CRITICAL, WARNING, or ALL for no severity filter'
)
RETURNS STRING
COMMENT 'Returns the most recent AI-written incident notes (up to 30) for ONE asset as a JSON array, newest first. Each item has window_start, window_id, severity, maintenance_status, triggered_metrics, likely_category, deviation, recommended_check and note. Use p_severity = ALL for no severity filter.'
RETURN (
  SELECT to_json(sort_array(collect_list(struct(
           window_start, window_id, severity, maintenance_status, triggered_metrics,
           likely_category, deviation, recommended_check, note)), false))
  FROM (
    SELECT *
    FROM uc_grid_sensor.grid_gold.gold_incident_notes
    WHERE asset_id = upper(trim(p_asset_id))
      AND (upper(trim(p_severity)) = 'ALL' OR severity = upper(trim(p_severity)))
    ORDER BY window_start DESC
    LIMIT 30
  )
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test the function by itself
# MAGIC Expect a JSON list. For TR-002 you should see windows where `maintenance_status` is `UNDER_MAINTENANCE` after 08:30 on 2026-10-24.

# COMMAND ----------

display(spark.sql("SELECT uc_grid_sensor.grid_agent.get_incident_notes('TR-002', 'CRITICAL') AS notes_json"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Run the agent
# MAGIC `agent.py` has two tools: **Genie** (your Genie Space) and the **UC function** above.
# MAGIC It tries the managed **MCP** servers first. If your workspace does not have them (common on Free Edition: *"managed MCP servers are not enabled"*), it automatically falls back to calling the same two tools directly through the Databricks SDK, and prints which one it used.
# MAGIC In that fallback mode the UC function runs through a SQL warehouse: set `WAREHOUSE_ID` at the top of `agent.py` (SQL Warehouses > your warehouse > Connection details).
# MAGIC The model is a Databricks-hosted LLM set in `agent.py` (`LLM_ENDPOINT`). If you get a model error, copy the exact endpoint name from the **Serving** page and change it there.
# MAGIC
# MAGIC The helper below prints which tools were called and the final answer.

# COMMAND ----------

import json
import mlflow

# Send traces to a normal, active experiment. (The notebook's default experiment can be deleted,
# which causes "experiment ... lifecycle stage is: deleted" warnings and loses the traces.)
user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/uc12-grid-agent")

from agent import AGENT

def ask(question: str):
    resp = AGENT.predict({"input": [{"role": "user", "content": question}]})
    answer = ""
    for item in resp.output:
        d = item if isinstance(item, dict) else item.model_dump()
        if d.get("type") == "function_call":
            print(f"[tool call] {d.get('name')}  {d.get('arguments')}")
        elif d.get("type") == "message":
            answer = "".join(c.get("text", "") for c in d.get("content", []))
    print("\nANSWER:\n" + answer)
    return answer

# COMMAND ----------

# Fleet question -> should use the Genie tool. Expected: 66
_ = ask("How many critical readings are there in total?")

# COMMAND ----------

# Incident-note question -> should use get_incident_notes. TR-002 is under maintenance after 08:30 on 24 Oct.
_ = ask("What was the latest critical incident for TR-002, and was it under maintenance?")

# COMMAND ----------

# Out-of-scope / read-only question -> should politely decline
_ = ask("Please change the generator critical temperature threshold to 100 C.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to check
# MAGIC * Question 1 printed a Genie tool call and answered **66**.
# MAGIC * Question 2 printed a `get_incident_notes` call and mentioned UNDER_MAINTENANCE.
# MAGIC * Question 3 was declined (no tool call that changes anything).
# MAGIC * Open **Experiments** in the left menu: each run above created a **trace** (model calls and tool calls). That is your MLflow tracing.
# MAGIC
# MAGIC **If Genie returns "still running":** the agent should call the Genie poll tool by itself. If it stops early, ask the question again.
# MAGIC **If you see a permission error:** your user needs CAN RUN on the Genie Space, and access to the gold tables and the SQL warehouse.