"""Client for the existing deployed UC-12 agent (a Databricks model serving endpoint).

Request  : POST /serving-endpoints/<name>/invocations   {"input": [{"role": "...", "content": "..."}]}
Response : {"output": [function_call..., function_call_output..., message{content[{text}]}]}
The endpoint name comes from the AGENT_ENDPOINT environment variable (set in the app settings).
"""
import time
from dataclasses import dataclass, field

from . import config, db


class AgentError(Exception):
    """A readable error for the UI."""


@dataclass
class AgentReply:
    answer: str
    tools: list = field(default_factory=list)      # [{"name", "arguments", "output"}]
    seconds: float = 0.0


def parse_response(resp) -> AgentReply:
    """Turn the endpoint's JSON into an answer plus the list of tools the agent used."""
    if not isinstance(resp, dict):
        raise AgentError("The agent returned an unexpected response.")
    if resp.get("error") or resp.get("error_code"):
        raise AgentError(str(resp.get("message") or resp.get("error") or resp.get("error_code")))

    texts, tools, outputs = [], [], {}
    for item in resp.get("output", []) or []:
        kind = item.get("type")
        if kind == "message":
            for part in item.get("content", []) or []:
                if isinstance(part, dict) and part.get("text"):
                    texts.append(part["text"])
        elif kind == "function_call":
            tools.append({"name": item.get("name"), "arguments": item.get("arguments"),
                          "call_id": item.get("call_id")})
        elif kind == "function_call_output":
            outputs[item.get("call_id")] = item.get("output")
    for tool in tools:
        tool["output"] = outputs.get(tool.get("call_id"))

    answer = "\n\n".join(t.strip() for t in texts if t and t.strip())
    if not answer:
        raise AgentError("The agent returned an empty answer. Please try again.")
    return AgentReply(answer=answer, tools=tools)


def _friendly(exc: Exception) -> str:
    text = str(exc)
    low = text.lower()
    if "permission" in low or "403" in low or "can query" in low:
        return ("The app is not allowed to query the agent endpoint. Add the endpoint as an app resource "
                "with permission 'Can query' (README, step 2).")
    if "not found" in low or "does not exist" in low or "404" in low:
        return f"The endpoint '{config.AGENT_ENDPOINT}' was not found. Check the AGENT_ENDPOINT setting."
    if "429" in low or "rate limit" in low or "too many" in low:
        return "The agent's rate limit was reached (AI Gateway). Wait a moment and try again."
    if "timed out" in low or "timeout" in low:
        return "The agent took too long to answer. Try a more specific question."
    if "not ready" in low or "503" in low or "scale" in low:
        return "The agent endpoint is starting up. Wait a minute and try again."
    return f"The agent returned an error: {text[:400]}"

def ask(history: list[dict]) -> AgentReply:
    """Send the conversation (list of {role, content}) to the agent and return its reply."""
    if not config.agent_configured():
        raise AgentError("The agent endpoint is not configured. Set AGENT_ENDPOINT in the app settings (README, step 2).")
    body = {"input": [{"role": m["role"], "content": m["content"]} for m in history]}
    started = time.time()
    try:
        resp = db.get_workspace_client().api_client.do(
            "POST", f"/serving-endpoints/{config.AGENT_ENDPOINT}/invocations", body=body)
    except Exception as exc:                          # noqa: BLE001
        raise AgentError(_friendly(exc)) from exc
    reply = parse_response(resp)
    reply.seconds = round(time.time() - started, 1)
    return reply


def endpoint_state() -> dict:
    """State of the serving endpoint (used by the System page)."""
    if not config.agent_configured():
        raise AgentError("AGENT_ENDPOINT is not set")
    try:
        ep = db.get_workspace_client().serving_endpoints.get(config.AGENT_ENDPOINT)
    except Exception as exc:                          # noqa: BLE001
        raise AgentError(_friendly(exc)) from exc
    ready = getattr(getattr(ep, "state", None), "ready", None)
    return {"name": ep.name, "ready": str(getattr(ready, "value", ready) or "UNKNOWN")}
