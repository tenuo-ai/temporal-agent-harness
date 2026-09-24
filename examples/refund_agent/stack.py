"""Client and worker wiring shared by the demo, the worker entry point, and the tests.

Two workers, split by what they can touch:

* **The agent worker** runs the agent workflow and the harness's own activities (the Jev call,
  stream publishing). It holds the agent's holder key, so the Tenuo plugin here signs a proof
  of possession for every effect the agent dispatches. It never holds payment credentials.
* **The effects worker** runs ``lookup_order`` and ``issue_refund`` and is the only process
  with access to the order and payment systems. Its Tenuo plugin requires a warrant on every
  activity (``require_warrant=True``) and verifies warrant, proof and arguments before the
  activity body starts.

In one process for the demo; in production these are separate deployments.
"""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass

from temporalio.client import Client
from temporalio.worker import Worker
from tenuo import SigningKey
from tenuo.temporal import (
    EnvKeyResolver,
    InMemoryPopDedupStore,
    TenuoPluginConfig,
    TenuoTemporalPlugin,
)

from temporal_agent_harness.plugin import AgentHarnessPlugin

from .tools import ALL_TOOLS, EFFECT_ACTIVITIES, EFFECTS_TASK_QUEUE, HOLDER_KEY_ID
from .workflow import TASK_QUEUE, RefundAgentWorkflow, ScriptedRefundAgentWorkflow


@dataclass
class Keys:
    """Demo keys. In production the issuer is your control plane (or Tenuo Cloud) and the
    holder key comes from Vault, a cloud secret manager, or a KMS-backed signer."""

    issuer: SigningKey
    holder: SigningKey

    @classmethod
    def generate(cls) -> Keys:
        keys = cls(issuer=SigningKey.generate(), holder=SigningKey.generate())
        # EnvKeyResolver reads TENUO_KEY_<key_id>. Only the agent worker needs this.
        os.environ[f"TENUO_KEY_{HOLDER_KEY_ID}"] = base64.b64encode(
            keys.holder.secret_key_bytes()
        ).decode("ascii")
        return keys


def _tenuo(keys: Keys, *, require_warrant: bool) -> TenuoTemporalPlugin:
    return TenuoTemporalPlugin(
        TenuoPluginConfig(
            key_resolver=EnvKeyResolver(),
            trusted_roots=[keys.issuer.public_key],
            # Named-argument proofs: the warrant constrains order_id, amount_cents, destination.
            activity_fns=EFFECT_ACTIVITIES,
            strict_mode=True,
            require_warrant=require_warrant,
            # Single-process demo. Use a shared store (Redis etc.) across real workers.
            pop_dedup_store=InMemoryPopDedupStore(),
        )
    )


async def connect_agent(
    keys: Keys, target: str = "localhost:7233", *, ai_sdk_plugins=(), **kw
) -> Client:
    """Client for the agent worker and for callers that talk to the agent.

    ``ai_sdk_plugins`` go first, so the AI SDK's payload converter wins (see
    ``AgentHarnessPlugin``). The live worker passes the OpenAI Agents plugin here.
    """
    return await Client.connect(
        target,
        plugins=[
            *ai_sdk_plugins,
            AgentHarnessPlugin(tools=ALL_TOOLS),
            # Harness activities carry no warrant and are not effects, so the agent worker
            # does not require one. The effects worker does.
            _tenuo(keys, require_warrant=False),
        ],
        **kw,
    )


async def connect_effects(keys: Keys, target: str = "localhost:7233", **kw) -> Client:
    """Client for the effects worker: every activity must carry a valid warrant."""
    return await Client.connect(
        target, plugins=[_tenuo(keys, require_warrant=True)], **kw
    )


def agent_worker(
    client: Client, task_queue: str = TASK_QUEUE, *, live: bool = False
) -> Worker:
    workflows: list[type] = [ScriptedRefundAgentWorkflow]
    if live:
        workflows.append(RefundAgentWorkflow)
    return Worker(client, task_queue=task_queue, workflows=workflows)


def effects_worker(client: Client, task_queue: str = EFFECTS_TASK_QUEUE) -> Worker:
    return Worker(client, task_queue=task_queue, activities=EFFECT_ACTIVITIES)
