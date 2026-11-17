# =====================================================================
# UC-12 Grid Sensor Anomaly Watch - AGENT CODE  (file: agent.py)
#
# A tool-calling agent built with the Databricks Agent Framework
# (MLflow ResponsesAgent). It has TWO tools:
#   1) Genie MCP tool   -> fleet-level questions over the 4 gold tables + 4 metric views
#   2) UC function tool -> uc_grid_sensor.grid_agent.get_incident_notes(asset_id, severity)
#
# Put this file in the SAME workspace folder as the three notebooks.
# The notebooks import it (setup/test, evaluation) and log it (deploy).
#
# CHANGES IN THIS VERSION
#   * WAREHOUSE_ID must be set (the SQL warehouse your Genie space uses). Notebook 3 must
#     declare it as a DatabricksSQLWarehouse resource, otherwise the deployed endpoint cannot
#     run Genie's SQL and you get "failed to reach COMPLETED, got MessageStatus.FAILED".
#   * _ask_genie now reports Genie's real error text instead of the generic FAILED message.
# =====================================================================
import asyncio
import concurrent.futures
import json
import uuid
from datetime import timedelta
from typing import Any, Generator

import mlflow
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    to_chat_completions_input,
)
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

# ---------------------------------------------------------------------
# SETTINGS  (change here if needed)
# ---------------------------------------------------------------------
# A Databricks-hosted (free-tier pay-per-token) model that supports tool calling.
# Copy the exact name from your Serving page; change it if this one is not listed.
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

GENIE_SPACE_ID = "01f1d13164551043b1ad349e8620ac6c"   # your Genie Space
UC_CATALOG = "uc_grid_sensor"
UC_SCHEMA = "grid_agent"                               # schema that holds the UC function tool

# How the agent reaches its two tools:
#   "auto" = try the managed MCP servers first; if the workspace does not have them
#            (Free Edition often does not), fall back to direct Databricks SDK tools
#   "mcp"  = MCP only (error if unavailable)      "sdk" = direct SDK tools only
TOOL_MODE = "auto"

# REQUIRED: the SQL warehouse ID used by your Genie space (and by the UC function in SDK mode).
# Copy it from SQL Warehouses > your warehouse > Connection details (HTTP path ends with /<id>).
# Notebook 3 declares it as a resource so the deployed endpoint is allowed to use it.
WAREHOUSE_ID = "1068521a8907acee"

MAX_STEPS = 10            # safety limit on tool-calling rounds per question
MAX_TOOL_CHARS = 6000     # long tool results are cut to this size
GENIE_TIMEOUT_MIN = 5     # how long to wait for Genie to answer


def _run_sync(fn, *args, **kwargs):
    """Run a function that uses asyncio.run() inside it (the MCP client does).
    Notebooks already have a running event loop, so asyncio.run() fails there with
    "cannot be called from a running event loop". In that case we run the call in a
    separate thread, which has no running loop. On a serving endpoint (no loop) it just runs directly."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return fn(*args, **kwargs)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(fn, *args, **kwargs).result()


# ---------------------------------------------------------------------
# TRACING: logs every model call as a trace in MLflow (the "MLflow tracing" requirement).
# Tool calls get their own spans through @mlflow.trace below.
# ---------------------------------------------------------------------
mlflow.openai.autolog()

SYSTEM_PROMPT = """You are the Grid Sensor Anomaly Watch assistant for grid engineers.

You have two kinds of tools:
1. A Genie tool for fleet-level questions: counts, totals, rankings, averages, comparisons by substation or asset type, asset status and trends. Use it for ANY number or comparison. If a Genie tool says the query is still running, keep polling with the ids it returned until the result is ready.
2. get_incident_notes for the written incident notes of ONE asset. Pass the asset_id (for example TR-002) and a severity of CRITICAL, WARNING or ALL. Use it when asked what happened to an asset or for the note of an incident.

Rules:
- Never invent numbers, assets, times or notes. Use tool results only. If a tool returns nothing, say so.
- When you present an incident, use this layout: Asset (with substation and status), Deviation, Likely category, Recommended check.
- You are read-only. You cannot change thresholds, data or equipment. Politely decline such requests.
- If a question is not about this grid sensor data, say you can only help with grid sensor anomalies.
- If an asset does not exist in the data, say so instead of guessing.

Background:
- 12 assets: circuit breakers (CB-001 to CB-004), generators (GN-001 to GN-004), transformers (TR-001 to TR-004) across North, West, East and South substations.
- Readings arrive every minute from 2026-10-23 22:00 to 2026-10-24 14:39.
- Incidents are 15-minute windows with severity WARNING or CRITICAL.
- Normal values: about 11 kV, 50 Hz, and 80 / 70 / 45 degrees C for generators / transformers / circuit breakers.
"""


# Tool definitions for the direct-SDK fallback (same two tools as the MCP route)
SDK_TOOL_SPECS = [
    {"type": "function", "function": {
        "name": "ask_genie",
        "description": "Ask the Grid Sensor Genie space a fleet-level question in plain English "
                       "(counts, totals, rankings, comparisons by substation or asset type, asset status). "
                       "Returns Genie's answer, the SQL it ran and the result rows.",
        "parameters": {"type": "object",
                       "properties": {"question": {"type": "string", "description": "The question in plain English"}},
                       "required": ["question"]}}},
    {"type": "function", "function": {
        "name": "get_incident_notes",
        "description": "Returns the most recent AI-written incident notes (up to 30) for ONE asset as a JSON array, "
                       "newest first. Each item has window_start, window_id, severity, maintenance_status, "
                       "triggered_metrics, likely_category, deviation, recommended_check and note.",
        "parameters": {"type": "object",
                       "properties": {
                           "asset_id": {"type": "string", "description": "Asset ID such as TR-002, GN-001 or CB-003"},
                           "severity": {"type": "string", "description": "CRITICAL, WARNING, or ALL for no severity filter"}},
                       "required": ["asset_id", "severity"]}}},
]


class GridIncidentAgent(ResponsesAgent):
    def __init__(self):
        self.ws = WorkspaceClient()
        # OpenAI-compatible client that talks to Databricks model serving
        self.client = self.ws.serving_endpoints.get_open_ai_client()
        host = self.ws.config.host.rstrip("/")
        self.server_urls = [
            f"{host}/api/2.0/mcp/genie/{GENIE_SPACE_ID}",            # tool 1: Genie
            f"{host}/api/2.0/mcp/functions/{UC_CATALOG}/{UC_SCHEMA}",  # tool 2: UC functions in grid_agent
        ]
        self._tool_specs = None
        self._tool_server = {}   # MCP mode: tool name -> MCP server url
        self.mode = None         # "mcp" or "sdk"
        self._warehouse = WAREHOUSE_ID if WAREHOUSE_ID and "PASTE_" not in WAREHOUSE_ID else None

    # ---- Load the tools: managed MCP first, direct SDK as fallback ------
    def _load_mcp_tools(self):
        specs, tool_server = [], {}
        for url in self.server_urls:
            mcp = DatabricksMCPClient(server_url=url, workspace_client=self.ws)
            for t in _run_sync(mcp.list_tools):
                schema = dict(t.inputSchema or {})
                schema.setdefault("type", "object")
                schema.setdefault("properties", {})
                specs.append({"type": "function",
                              "function": {"name": t.name, "description": t.description or "", "parameters": schema}})
                tool_server[t.name] = url
        self._tool_specs, self._tool_server, self.mode = specs, tool_server, "mcp"

    def _load_tools(self):
        if self._tool_specs is not None:
            return
        if TOOL_MODE in ("auto", "mcp"):
            try:
                self._load_mcp_tools()
                return
            except BaseException as e:
                if isinstance(e, (KeyboardInterrupt, SystemExit)) or TOOL_MODE == "mcp":
                    raise
                print(f"[agent] Managed MCP servers not available ({type(e).__name__}); using direct SDK tools instead.")
        self._tool_specs, self.mode = SDK_TOOL_SPECS, "sdk"

    # ---- SDK fallback tool 1: Genie Conversation API --------------------
    def _ask_genie(self, question: str) -> str:
        waiter = self.ws.genie.start_conversation(space_id=GENIE_SPACE_ID, content=question)
        conv_id = getattr(waiter, "conversation_id", None)
        msg_id = getattr(waiter, "message_id", None)
        try:
            msg = waiter.result(timeout=timedelta(minutes=GENIE_TIMEOUT_MIN))
        except Exception as e:
            # The SDK only says "FAILED". Ask Genie for the real reason so it is visible in the tool output.
            detail = str(e)
            try:
                m = self.ws.genie.get_message(space_id=GENIE_SPACE_ID, conversation_id=conv_id, message_id=msg_id)
                err = getattr(m, "error", None)
                if err:
                    detail = f"{getattr(err, 'type', '')}: {getattr(err, 'error', '')}".strip(": ")
                else:
                    detail = f"status={getattr(m, 'status', None)}; {detail}"
            except Exception as inner:  # noqa: BLE001
                detail = f"{detail} (could not read Genie error: {inner})"
            raise RuntimeError(f"Genie failed: {detail}")

        parts = []
        for att in (msg.attachments or []):
            if att.text and att.text.content:
                parts.append(att.text.content)
            if att.query:
                if att.query.description:
                    parts.append(f"Genie description: {att.query.description}")
                if att.query.query:
                    parts.append(f"SQL: {att.query.query}")
                res = self.ws.genie.get_message_attachment_query_result(
                    space_id=GENIE_SPACE_ID, conversation_id=msg.conversation_id,
                    message_id=getattr(msg, "message_id", None) or msg.id, attachment_id=att.attachment_id)
                sr = res.statement_response
                if sr and sr.manifest and sr.result:
                    cols = [c.name for c in sr.manifest.schema.columns]
                    rows = (sr.result.data_array or [])[:50]
                    parts.append("RESULT: " + json.dumps({"columns": cols, "rows": rows}))
        return "\n".join(parts) or "Genie returned no answer."

    # ---- SDK fallback tool 2: run the UC function through a SQL warehouse
    def _warehouse_id(self) -> str:
        if not self._warehouse:
            self._warehouse = next(iter(self.ws.warehouses.list())).id
        return self._warehouse

    def _get_incident_notes(self, asset_id: str, severity: str) -> str:
        from databricks.sdk.service.sql import StatementParameterListItem
        resp = self.ws.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id(),
            statement=f"SELECT {UC_CATALOG}.{UC_SCHEMA}.get_incident_notes(:a, :s)",
            parameters=[StatementParameterListItem(name="a", value=asset_id),
                        StatementParameterListItem(name="s", value=severity)],
            wait_timeout="30s",
        )
        if resp.result and resp.result.data_array:
            return resp.result.data_array[0][0] or "[]"
        return "No result (the warehouse may still be starting; try again)."

    # ---- Run one tool (own trace span so it shows in MLflow tracing) ---
    @mlflow.trace(span_type=SpanType.TOOL)
    def _call_tool(self, name: str, args: dict[str, Any]) -> str:
        try:
            if self.mode == "mcp":
                mcp = DatabricksMCPClient(server_url=self._tool_server[name], workspace_client=self.ws)
                result = _run_sync(mcp.call_tool, name, args)
                text = "".join(c.text for c in result.content if hasattr(c, "text"))
            elif name == "ask_genie":
                text = self._ask_genie(args.get("question", ""))
            elif name == "get_incident_notes":
                text = self._get_incident_notes(args.get("asset_id", ""), args.get("severity", "ALL"))
            else:
                text = f"Unknown tool: {name}"
        except Exception as e:  # return the error to the model instead of crashing
            text = f"Tool error: {e}"
        return text[:MAX_TOOL_CHARS]

    # ---- The agent loop: model -> tool calls -> model -> ... -> answer --
    def predict_stream(self, request: ResponsesAgentRequest) -> Generator[ResponsesAgentStreamEvent, None, None]:
        self._load_tools()
        # MEMORY HOOK (next step): request.custom_inputs.get("thread_id") will identify the chat
        # so Lakebase short-term / long-term memory can be loaded here.
        messages = [{"role": "system", "content": SYSTEM_PROMPT}] + to_chat_completions_input(
            [i.model_dump() for i in request.input]
        )

        for _ in range(MAX_STEPS):
            resp = self.client.chat.completions.create(
                model=LLM_ENDPOINT, messages=messages, tools=self._tool_specs
            )
            msg = resp.choices[0].message

            # No tool requested -> this is the final answer
            if not msg.tool_calls:
                yield ResponsesAgentStreamEvent(
                    type="response.output_item.done",
                    item=self.create_text_output_item(text=msg.content or "", id=str(uuid.uuid4())),
                )
                return

            # Remember the model's tool request, then run each tool
            messages.append({
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {"id": tc.id, "type": "function",
                     "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                    for tc in msg.tool_calls
                ],
            })
            for tc in msg.tool_calls:
                yield ResponsesAgentStreamEvent(
                    type="response.output_item.done",
                    item=self.create_function_call_item(
                        id=str(uuid.uuid4()), call_id=tc.id,
                        name=tc.function.name, arguments=tc.function.arguments or "{}",
                    ),
                )
                try:
                    args = json.loads(tc.function.arguments or "{}")
                except json.JSONDecodeError:
                    args = {}
                output = self._call_tool(tc.function.name, args)
                yield ResponsesAgentStreamEvent(
                    type="response.output_item.done",
                    item=self.create_function_call_output_item(call_id=tc.id, output=output),
                )
                messages.append({"role": "tool", "tool_call_id": tc.id, "content": output})

        # Safety net if the model keeps calling tools
        yield ResponsesAgentStreamEvent(
            type="response.output_item.done",
            item=self.create_text_output_item(
                text="I could not finish this request within the allowed number of steps. Please try a narrower question.",
                id=str(uuid.uuid4()),
            ),
        )

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [e.item for e in self.predict_stream(request) if e.type == "response.output_item.done"]
        return ResponsesAgentResponse(output=outputs, custom_outputs=request.custom_inputs)


AGENT = GridIncidentAgent()
mlflow.models.set_model(AGENT)