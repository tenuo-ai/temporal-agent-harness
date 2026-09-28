"""Client and worker wiring shared by the demo, the worker entry point, and the tests.

One worker, running both the agent workflow and its activity-backed tools
(``lookup_order``, ``issue_refund``) under ``require_warrant=True``. The harness's own
internal activities (model calls, the Jev approval activity, ...) are exempt via
``tenuo.temporal.harness.HARNESS_INTERNAL_ACTIVITIES`` — they are not effects, so they
need no warrant — while every real tool call is verified against the ticket's warrant,
transparently, by the same ``TenuoWorkerInterceptor``. There is no separate effects
worker and no inline-tool workaround: this is what native harness support in Tenuo was
built for.
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass

from temporalio.client import Client
from temporalio.worker import Worker
from tenuo import SigningKey
from tenuo.temporal import EnvKeyResolver, InMemoryPopDedupStore, TenuoClientInterceptor, TenuoTemporalPlugin
from tenuo.temporal.harness import harness_plugin_config

from temporal_agent_harness.plugin import AgentHarnessPlugin

from .tools import ALL_TOOLS, HOLDER_KEY_ID, TOOL_ACTIVITIES
from .workflow import TASK_QUEUE, RefundAgentWorkflow, ScriptedRefundAgentWorkflow


@dataclass
class Keys:
    """Demo keys. In production the issuer is your control plane (or Tenuo Cloud), the
    holder key comes from Vault, a cloud secret manager, or a KMS-backed signer, and the
    approver key belongs to whoever is allowed to sign off on an escalated refund (e.g. an
    approval-UI service, itself backed by a real signer)."""

    issuer: SigningKey
    holder: SigningKey
    approver: SigningKey

    @classmethod
    def generate(cls) -> Keys:
        keys = cls(
            issuer=SigningKey.generate(),
            holder=SigningKey.generate(),
            approver=SigningKey.generate(),
        )
        # EnvKeyResolver reads TENUO_KEY_<key_id>.
        os.environ[f"TENUO_KEY_{HOLDER_KEY_ID}"] = base64.b64encode(
            keys.holder.secret_key_bytes()
        ).decode("ascii")
        return keys


def _tenuo(keys: Keys) -> TenuoTemporalPlugin:
    return TenuoTemporalPlugin(
        harness_plugin_config(
            key_resolver=EnvKeyResolver(),
            trusted_roots=[keys.issuer.public_key],
            # Named-argument proofs: the warrant constrains order_id, amount_cents,
            # destination — and pop_exclude_args needs this to resolve tool_ctx by name.
            activity_fns=TOOL_ACTIVITIES,
            strict_mode=True,
            require_warrant=True,
            # Single-process demo. Use a shared store (Redis etc.) across real workers.
            pop_dedup_store=InMemoryPopDedupStore(),
        )
    )


async def connect_agent(
    keys: Keys, target: str = "localhost:7233", *, ai_sdk_plugins=(), **kw
) -> Client:
    """Client for the single agent+effects worker and for callers that talk to it.

    ``ai_sdk_plugins`` go first, so the AI SDK's payload converter wins (see
    ``AgentHarnessPlugin``). The live worker passes the OpenAI Agents plugin here.
    """
    return await Client.connect(
        target,
        plugins=[
            *ai_sdk_plugins,
            AgentHarnessPlugin(tools=ALL_TOOLS),
            _tenuo(keys),
        ],
        **kw,
    )


def client_interceptor(client: Client) -> TenuoClientInterceptor:
    """The TenuoClientInterceptor installed on *client* by ``connect_agent()``.

    Used to stage a signed approval for one workflow update, e.g.
    ``client_interceptor(client).set_approvals_for_update(update_id, [signed])``
    before calling ``handle.execute_update(..., id=update_id)``.
    """
    from tenuo.temporal._workflow import _resolve_client_interceptor

    return _resolve_client_interceptor(client, None)


def agent_worker(
    client: Client, task_queue: str = TASK_QUEUE, *, live: bool = False
) -> Worker:
    workflows: list[type] = [ScriptedRefundAgentWorkflow]
    if live:
        workflows.append(RefundAgentWorkflow)
    return Worker(
        client,
        task_queue=task_queue,
        workflows=workflows,
        activities=TOOL_ACTIVITIES,
    )
