"""Chat with the refund agent, backed by a real model.

    temporal server start-dev                         # terminal 1
    OPENAI_API_KEY=... uv run python -m examples.refund_agent.live   # terminal 2, repo root

Optional: ``REFUND_AGENT_JUDGE=jev`` and ``TYPESAFE_API_KEY=...`` to have Jev review in-warrant
refunds; ``--order ord_2210`` to work the $965 ticket, where refunds over $500 come to you.

Runs the single agent+effects worker in this process, opens a ticket for the order
(minting its warrant), and then relays each line you type to the agent. Tool calls, refusals
and approval requests print as they happen; answer an approval with y or n.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from datetime import timedelta

from temporal_agent_harness.ai_sdks.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
)
from temporal_agent_harness.harness.agent_client import AgentClient
from temporal_agent_harness.harness.agent_protocol import AgentConfig, AgentEventType

from . import stack
from .tools import get_order
from .warrants import mint_ticket_warrant, sign_refund_approval
from .workflow import TASK_QUEUE, RefundAgentWorkflow


async def _turn(agent: AgentClient, client, warrant, approver_key, text: str) -> None:
    stream = await agent.send_message(
        "ask", {"text": text}, on_item=lambda out, _offset: out, timeout=600
    )
    async for out in stream:
        ev = getattr(out, "event", None)
        kind = getattr(ev, "type", None)
        if kind == AgentEventType.TOOL_START:
            print(f"  [tool] {ev.tool_name}({ev.tool_input})")
        elif kind == AgentEventType.TOOL_APPROVAL_RESOLVED and not ev.approved:
            print(f"  [refused] {ev.reason}")
        elif kind == AgentEventType.TOOL_APPROVAL_REQUESTED:
            answer = await asyncio.to_thread(
                input, f"  [approval] {ev.tool_name}({ev.tool_input})? [y/n] "
            )
            approved = answer.strip().lower() == "y"
            update_id = f"approve-{ev.tool_id}"
            if approved and ev.tool_name == "issue_refund":
                # The harness's own yes is not enough on its own: sign a real
                # approval bound to this exact call, from this ticket's approver
                # key, so the warrant's approval gate is satisfied too.
                approval = sign_refund_approval(warrant, approver_key, ev.tool_input)
                stack.client_interceptor(client).set_approvals_for_update(
                    update_id, [approval],
                )
            await agent.approve_tool(
                ev.tool_id, approved=approved, update_id=update_id,
            )
        elif kind == AgentEventType.MESSAGE_HANDLER_END:
            print(f"agent> {ev.output.get('text')}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--order", default="ord_1042")
    parser.add_argument(
        "--target", default=os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    )
    args = parser.parse_args()
    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit(
            "error: OPENAI_API_KEY is not set (examples.refund_agent.demo needs no keys)"
        )

    keys = stack.Keys.generate()
    openai = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(minutes=2)
        )
    )
    agent_client = await stack.connect_agent(keys, args.target, ai_sdk_plugins=[openai])

    order = get_order(args.order)
    warrant = mint_ticket_warrant(
        order, issuer_key=keys.issuer, holder_key=keys.holder, approver_key=keys.approver,
    )

    async with stack.agent_worker(agent_client, live=True):
        handle = await agent_client.start_workflow(
            RefundAgentWorkflow.run,
            AgentConfig(),
            id=f"refund-live-{order.order_id}-{uuid.uuid4().hex[:6]}",
            task_queue=TASK_QUEUE,
        )
        await handle.execute_update(
            RefundAgentWorkflow.open_ticket, warrant.to_base64()
        )
        print(
            f"Ticket open for {order.order_id} ({order.customer}, "
            f"${order.total_cents / 100:.2f}, paid with {order.payment_method}).\n"
            f"Workflow {handle.id}. Type as the customer; Ctrl-D to quit."
        )
        agent = AgentClient(agent_client, handle.id)
        while True:
            try:
                line = await asyncio.to_thread(input, "you> ")
            except EOFError:
                break
            if line.strip():
                await _turn(agent, agent_client, warrant, keys.approver, line)


if __name__ == "__main__":
    asyncio.run(main())
