# ABOUTME: End-to-end tests for the refund agent example (examples/refund_agent): auto mode on
# the harness with a Tenuo task warrant as the ceiling. They run the scripted agent (model
# replaced by a list of tool calls) against a real Temporal server, with a single worker running
# both the agent workflow and its Tenuo-protected activity-backed tools under
# require_warrant=True, and check the ledger to prove denied refunds never ran.
#
# Server: set TEMPORAL_ADDRESS to reuse a running dev server (`temporal server start-dev`).
# Otherwise the tests start a local one with WorkflowEnvironment.start_local(), which downloads
# the dev server on first use. If neither works the tests are skipped.
#
# Run with: uv run pytest tests/examples/refund_agent -v

from __future__ import annotations

import asyncio
import json
import os
import uuid

import pytest
import pytest_asyncio
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

from examples.refund_agent import stack
from examples.refund_agent.tools import get_order, ledger, reset_ledger
from examples.refund_agent.warrants import mint_ticket_warrant, sign_refund_approval
from examples.refund_agent.workflow import TASK_QUEUE, ScriptedRefundAgentWorkflow
from temporal_agent_harness.harness.agent import ToolApprovalPolicy
from temporal_agent_harness.harness.agent_client import AgentClient
from temporal_agent_harness.harness.agent_protocol import (
    SEND_AGENT_MESSAGE_UPDATE,
    AgentConfig,
    AgentMessage,
    AgentMessageReply,
)

A = "ord_1042"  # $189.00, paid with pm_card_4242
B = "ord_2210"  # $965.00, paid with pm_card_1881


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


@pytest_asyncio.fixture
async def rig(target):
    """Single agent+effects worker, fresh keys and a clean ledger per test."""
    reset_ledger()
    keys = stack.Keys.generate()
    agent_client = await stack.connect_agent(keys, target)
    async with stack.agent_worker(agent_client):
        yield keys, agent_client


async def _open(client: Client, keys, order_id: str, *, ttl: int = 1800, config=None):
    warrant = mint_ticket_warrant(
        get_order(order_id),
        issuer_key=keys.issuer,
        holder_key=keys.holder,
        approver_key=keys.approver,
        ttl_seconds=ttl,
    )
    handle = await client.start_workflow(
        ScriptedRefundAgentWorkflow.run,
        config or AgentConfig(),
        id=f"test-refund-{uuid.uuid4().hex[:8]}",
        task_queue=TASK_QUEUE,
    )
    await handle.execute_update(
        ScriptedRefundAgentWorkflow.open_ticket, warrant.to_base64()
    )
    return handle, warrant


async def _run(handle, calls: list[dict]) -> list[dict]:
    await handle.execute_update(
        SEND_AGENT_MESSAGE_UPDATE,
        AgentMessage(type="act", payload={"text": json.dumps(calls)}),
        result_type=AgentMessageReply,
    )
    async with asyncio.timeout(30):
        while True:
            outcomes = await handle.query(ScriptedRefundAgentWorkflow.outcomes)
            if len(outcomes) >= len(calls):
                return outcomes
            await asyncio.sleep(0.1)


def _refund(call_id: str, order_id: str, cents: int, dest: str) -> dict:
    return {
        "id": call_id,
        "tool": "issue_refund",
        "args": {"order_id": order_id, "amount_cents": cents, "destination": dest},
    }


async def test_in_bounds_refund_runs(rig):
    keys, client = rig
    handle, _warrant = await _open(client, keys, A)
    [out] = await _run(handle, [_refund("r1", A, 4500, "pm_card_4242")])
    assert out["status"] == "ran"
    assert [(r.order_id, r.amount_cents, r.destination) for r in ledger()] == [
        (A, 4500, "pm_card_4242")
    ]


@pytest.mark.parametrize(
    "call, bound",
    [
        (
            _refund("x1", A, 4500, "pm_card_9999"),
            "destination",
        ),  # a card they never used
        (_refund("x2", A, 18_901, "pm_card_4242"), "amount_cents"),  # one cent over
        (
            _refund("x3", B, 4500, "pm_card_4242"),
            "order_id",
        ),  # another customer's order
    ],
)
async def test_out_of_warrant_refund_is_denied_in_the_seam(rig, call, bound):
    keys, client = rig
    handle, _warrant = await _open(client, keys, A)
    [out] = await _run(handle, [call])
    assert out["status"] == "denied_at_gate"
    assert bound in out["detail"]
    assert ledger() == []


async def test_pre_approved_tool_is_still_bound_on_the_worker(rig):
    """lookup_order skips the approval seam entirely. The effects worker still checks it."""
    keys, client = rig
    handle, _warrant = await _open(client, keys, A)
    ok, other = await _run(
        handle,
        [
            {"id": "l1", "tool": "lookup_order", "args": {"order_id": A}},
            {"id": "l2", "tool": "lookup_order", "args": {"order_id": B}},
        ],
    )
    assert ok["status"] == "ran"
    assert other["status"] == "denied_at_worker"
    assert "order_id" in other["detail"]


async def test_no_approval_gate_at_all_still_cannot_exceed_the_warrant(rig):
    """A misconfigured policy that skips every gate: the worker is the backstop."""
    keys, client = rig
    config = AgentConfig(approval_policy=ToolApprovalPolicy.dangerously_skip_all())
    handle, _warrant = await _open(client, keys, A, config=config)
    inside, outside = await _run(
        handle,
        [
            _refund("s1", A, 4500, "pm_card_4242"),
            _refund("s2", A, 4500, "pm_card_9999"),
        ],
    )
    assert inside["status"] == "ran"
    assert outside["status"] == "denied_at_worker"
    assert [r.destination for r in ledger()] == ["pm_card_4242"]


async def test_expired_warrant_is_denied(rig):
    keys, client = rig
    handle, _warrant = await _open(client, keys, A, ttl=1)
    await asyncio.sleep(2)
    [out] = await _run(handle, [_refund("e1", A, 4500, "pm_card_4242")])
    assert out["status"] == "denied_at_gate"
    assert "expired" in out["detail"]
    assert ledger() == []


async def test_large_refund_waits_for_a_human_and_survives_a_worker_restart(target):
    """Inside the warrant but above the $500 review threshold: escalated to the human gate.
    The agent worker goes away while the call waits, a new one comes up, the approval lands,
    and the refund runs exactly once."""
    reset_ledger()
    keys = stack.Keys.generate()
    first = await stack.connect_agent(keys, target)
    async with stack.agent_worker(first):
        handle, warrant = await _open(first, keys, B)
        await handle.execute_update(
            SEND_AGENT_MESSAGE_UPDATE,
            AgentMessage(
                type="act",
                payload={
                    "text": json.dumps([_refund("h1", B, 60_000, "pm_card_1881")])
                },
            ),
            result_type=AgentMessageReply,
        )
        async with asyncio.timeout(30):
            while not (
                pending := await AgentClient(
                    first, handle.id
                ).get_pending_approvals()
            ):
                await asyncio.sleep(0.1)
        assert pending[0].tool_name == "issue_refund"
        assert ledger() == []

    second = await stack.connect_agent(keys, target)
    async with stack.agent_worker(second):
        update_id = "approve-h1"
        approval = sign_refund_approval(
            warrant, keys.approver,
            {"order_id": B, "amount_cents": 60_000, "destination": "pm_card_1881"},
        )
        stack.client_interceptor(second).set_approvals_for_update(update_id, [approval])
        await AgentClient(second, handle.id).approve_tool(
            "h1", approved=True, update_id=update_id,
        )
        async with asyncio.timeout(30):
            while not (
                outs := await handle.query(ScriptedRefundAgentWorkflow.outcomes)
            ):
                await asyncio.sleep(0.1)
    assert outs[0]["status"] == "ran"
    assert [(r.order_id, r.amount_cents) for r in ledger()] == [(B, 60_000)]


async def test_human_denial_is_final(rig):
    keys, client = rig
    handle, _warrant = await _open(client, keys, B)
    await handle.execute_update(
        SEND_AGENT_MESSAGE_UPDATE,
        AgentMessage(
            type="act",
            payload={"text": json.dumps([_refund("d1", B, 60_000, "pm_card_1881")])},
        ),
        result_type=AgentMessageReply,
    )
    agent = AgentClient(client, handle.id)
    async with asyncio.timeout(30):
        while not await agent.get_pending_approvals():
            await asyncio.sleep(0.1)
    await agent.approve_tool("d1", approved=False, reason="Only one chair came back.")
    async with asyncio.timeout(30):
        while not (outs := await handle.query(ScriptedRefundAgentWorkflow.outcomes)):
            await asyncio.sleep(0.1)
    assert outs[0]["status"] == "denied_at_gate"
    assert ledger() == []
