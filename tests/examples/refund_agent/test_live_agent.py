# ABOUTME: The conversational refund agent (RefundAgentWorkflow, OpenAI Agents SDK) driven by a
# canned model, so the SDK path is covered without an API key: the model looks up the order,
# is talked into refunding to a card the customer never used (refused by the warrant, with the
# reason returned to the model as the tool result), then refunds the original card.
#
# Server: same as test_refund_agent.py (TEMPORAL_ADDRESS, or WorkflowEnvironment.start_local()).

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import timedelta

import pytest
import pytest_asyncio
from openai.types.responses import ResponseFunctionToolCall
from temporalio.testing import WorkflowEnvironment

from examples.refund_agent import stack
from examples.refund_agent.tools import get_order, ledger, reset_ledger
from examples.refund_agent.warrants import mint_ticket_warrant
from examples.refund_agent.workflow import TASK_QUEUE, RefundAgentWorkflow
from temporal_agent_harness.ai_sdks.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
)
from temporal_agent_harness.ai_sdks.openai_agents.testing import (
    ResponseBuilders,
    TestModel,
    TestModelProvider,
)
from temporal_agent_harness.harness.agent_client import AgentClient
from temporal_agent_harness.harness.agent_protocol import AgentConfig, AgentEventType

@pytest.fixture(autouse=True)
def _own_task_queue(monkeypatch):
    """A task queue per test. Agent workflows stay open after a test ends, so on a shared
    dev server a later test's worker (with fresh keys) would otherwise pick up an earlier
    test's workflow tasks."""
    monkeypatch.setattr(
        sys.modules[__name__], "TASK_QUEUE", f"refund-agent-test-{uuid.uuid4().hex[:8]}"
    )



@pytest_asyncio.fixture
async def target():
    address = os.environ.get("TEMPORAL_ADDRESS")
    if address:
        yield address
        return
    try:
        env = await WorkflowEnvironment.start_local()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"no Temporal server available: {e}")
    try:
        yield env.client.service_client.config.target_host
    finally:
        await env.shutdown()


_ids = iter(range(1, 100))


def _call(name: str, **args):
    """A model turn that calls one tool. Each call needs its own call id."""
    n = next(_ids)
    return ResponseBuilders.model_response(
        ResponseFunctionToolCall(
            arguments=json.dumps(args),
            call_id=f"call_{n}",
            name=name,
            type="function_call",
            id=f"fc_{n}",
            status="completed",
        )
    )


async def test_model_is_refused_then_recovers(target):
    reset_ledger()
    keys = stack.Keys.generate()
    model = TestModel.returning_responses(
        [
            _call("lookup_order", order_id="ord_1042"),
            # "Please send it to my new card, pm_card_9999."
            _call(
                "issue_refund",
                order_id="ord_1042",
                amount_cents=4500,
                destination="pm_card_9999",
            ),
            _call(
                "issue_refund",
                order_id="ord_1042",
                amount_cents=4500,
                destination="pm_card_4242",
            ),
            ResponseBuilders.output_message(
                "I refunded $45.00 to the card you paid with. I can't send refunds to a "
                "different card."
            ),
        ]
    )
    openai = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(
            start_to_close_timeout=timedelta(seconds=30)
        ),
        model_provider=TestModelProvider(model),
    )
    agent_client = await stack.connect_agent(keys, target, ai_sdk_plugins=[openai])

    warrant = mint_ticket_warrant(
        get_order("ord_1042"),
        issuer_key=keys.issuer,
        holder_key=keys.holder,
        approver_key=keys.approver,
    )
    async with stack.agent_worker(agent_client, TASK_QUEUE, live=True):
        handle = await agent_client.start_workflow(
            RefundAgentWorkflow.run,
            AgentConfig(),
            id=f"test-live-refund-{uuid.uuid4().hex[:8]}",
            task_queue=TASK_QUEUE,
        )
        await handle.execute_update(
            RefundAgentWorkflow.open_ticket, warrant.to_base64()
        )

        events = []
        stream = await AgentClient(agent_client, handle.id).send_message(
            "ask",
            {
                "text": "ord_1042 arrived damaged. Refund $45 to my new card pm_card_9999."
            },
            on_item=lambda out, _offset: out,
            timeout=60,
        )
        async for out in stream:
            events.append(out)

    resolved = [
        e.event
        for e in events
        if getattr(getattr(e, "event", None), "type", None)
        == AgentEventType.TOOL_APPROVAL_RESOLVED
    ]
    # Two refund attempts reached the seam: the injected one refused by the warrant (with the
    # bound the model can read), the original-card one approved.
    assert [r.approved for r in resolved] == [False, True]
    assert "destination" in resolved[0].reason
    reply = next(
        e.event
        for e in events
        if getattr(e.event, "type", None) == AgentEventType.MESSAGE_HANDLER_END
    )
    assert "card you paid with" in reply.output["text"]
    assert [(r.order_id, r.amount_cents, r.destination) for r in ledger()] == [
        ("ord_1042", 4500, "pm_card_4242")
    ]
