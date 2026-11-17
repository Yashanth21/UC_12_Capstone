"""Page 6: AI Chat Assistant - full-page chat with the existing deployed GridWatch agent."""
from datetime import datetime

import streamlit as st

from gridwatch import agent_client, config, theme

theme.page_header("AI Chat Assistant", "Ask about the fleet, a single asset or an incident. Answers come live from the deployed agent.")

HISTORY_SENT = 12      # how many recent messages are sent to the agent as context

if "chat" not in st.session_state:
    st.session_state.chat = []          # [{"role", "content", "tools", "seconds", "error", "time"}]

if not config.agent_configured():
    st.error("The agent endpoint is not configured.")
    st.markdown("Set `AGENT_ENDPOINT` in the app settings (README, step 2), then redeploy.")
    st.stop()

top_left, top_right = st.columns([4, 1])
top_left.caption(f"Connected endpoint: `{config.AGENT_ENDPOINT}`")
if top_right.button("Clear conversation", use_container_width=True, disabled=not st.session_state.chat):
    st.session_state.chat = []
    st.rerun()

# ---- Suggested questions (only shown before the first message) -------------------------------------------
pending = st.session_state.pop("pending_question", None)
if not st.session_state.chat and not pending:
    theme.section("Try asking", "click a question to start")
    cols = st.columns(2)
    for i, question in enumerate(config.SUGGESTED_QUESTIONS):
        if cols[i % 2].button(question, key=f"suggest_{i}", use_container_width=True):
            st.session_state.pending_question = question
            st.rerun()


def render(message: dict) -> None:
    with st.chat_message(message["role"], avatar="\U0001F9D1\u200D\U0001F4BB" if message["role"] == "user" else "\u26A1"):
        if message.get("error"):
            st.error(message["content"])
        else:
            st.markdown(message["content"])
        if message.get("tools"):
            with st.expander(f"Tools used ({len(message['tools'])})"):
                for t in message["tools"]:
                    st.markdown(f"**{t['name']}**")
                    st.code(str(t.get("arguments")), language="json")
                    if t.get("output"):
                        st.code(str(t["output"])[:1200])
        if message["role"] == "assistant" and message.get("seconds"):
            st.caption(f"{message['seconds']}s")


for message in st.session_state.chat:
    render(message)

prompt = st.chat_input("Ask GridWatch AI about your grid...") or pending
if prompt:
    user_message = {"role": "user", "content": prompt, "time": datetime.now().isoformat(timespec="seconds")}
    st.session_state.chat.append(user_message)
    render(user_message)

    context = [m for m in st.session_state.chat if not m.get("error")][-HISTORY_SENT:]
    with st.chat_message("assistant", avatar="\u26A1"):
        with st.spinner("GridWatch AI is checking the data..."):
            try:
                reply = agent_client.ask(context)
                assistant = {"role": "assistant", "content": reply.answer, "tools": reply.tools,
                             "seconds": reply.seconds, "time": datetime.now().isoformat(timespec="seconds")}
            except agent_client.AgentError as exc:
                assistant = {"role": "assistant", "content": str(exc), "error": True,
                             "time": datetime.now().isoformat(timespec="seconds")}
    st.session_state.chat.append(assistant)
    st.rerun()

if st.session_state.chat:
    transcript = "\n\n".join(f"**{m['role'].title()}** ({m.get('time', '')}): {m['content']}" for m in st.session_state.chat)
    st.download_button("Download transcript", transcript, file_name="gridwatch_chat.md", mime="text/markdown")
