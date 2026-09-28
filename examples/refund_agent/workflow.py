"""The refund agent: auto mode on the harness, with a Tenuo task warrant as the ceiling.

One support ticket, one agent, two tools (``lookup_order``, ``issue_refund``). Three layers
decide whether a refund happens, and each does a different job:

* **Temporal makes the agent durable and the wait cheap.** Every gated call pauses in workflow
  state. The worker can restart, the approver can answer tomorrow, and the agent resumes
  mid-turn without re-running the model.
* **The reviewer decides whether a refund is a good idea.** Auto mode puts each gated call to
  an evaluator: Jev in production, judging against the operator's criteria, with large refunds
  going straight to a human.
* **The warrant (Tenuo) decides what is possible at all.** Minted per ticket and handed to the
  agent with ``open_ticket``: this order, at most what the customer paid, back to the card they
  paid with, while the ticket is open. Checked first in the approval seam
  (``warrant_evaluator``), and verified again — proof of possession over the exact arguments,
  ``tool_ctx`` excluded — by the same worker's ``TenuoWorkerInterceptor`` before the tool body
  runs. One worker, ``require_warrant=True``: no separate effects worker, no inline-tool
  workaround (see ``stack.py`` and ``tenuo.temporal.harness``).

Two workflows share that setup:

* ``RefundAgentWorkflow`` converses through the OpenAI Agents SDK (needs ``OPENAI_API_KEY``).
* ``ScriptedRefundAgentWorkflow`` replaces the model with a script of tool calls, so the
  approval seam, the warrant checks and the worker-side enforcement run deterministically in
  tests and in ``demo.py``. Everything except the model is the real code.

Set ``REFUND_AGENT_JUDGE=jev`` (and ``TYPESAFE_API_KEY``) on the agent worker to put Jev behind
the warrant check. Otherwise a stand-in reviewer approves in-warrant calls under the review
threshold, which keeps the example runnable offline.
"""

from __future__ import annotations

import json
import os
from typing import ClassVar

from temporalio import workflow
from temporalio.contrib.workflow_streams import WorkflowStream
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from tenuo import Warrant

    from temporal_agent_harness.harness import agent
    from temporal_agent_harness.harness.agent import (
        AutoApprovalContext,
        AutoApprovalCriteria,
        AutoApprovalCriteriaSet,
        AutoApprovalDecision,
        AutoApprovalVerdict,
    )
    from temporal_agent_harness.harness.agent_protocol import (
        AgentConfig,
        TextMessage,
        TextReply,
        ToolApprovalPolicy,
    )
    from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner

    from tenuo.temporal import tenuo_install_warrant
    from tenuo.temporal.harness import warrant_evaluator

    from .tools import ALL_TOOLS, HOLDER_KEY_ID, issue_refund, lookup_order


TASK_QUEUE = "refund-agent"
DEFAULT_MODEL = "gpt-5.6-luna"

# Refunds above this go to a human even when the warrant allows them. $500, in cents.
REVIEW_ABOVE_CENTS = 50_000

# Read once at import, on the worker, so it is fixed per worker process.
JUDGE = os.environ.get("REFUND_AGENT_JUDGE", "stand-in").lower()

SYSTEM_INSTRUCTION = """\
You are a support agent resolving one ticket. Look up the order before acting. If the customer
is owed a refund, issue it with `issue_refund`, in cents, to the payment method on the order.
If a tool call is refused, read the reason, tell the customer plainly what you could and could
not do, and do not retry the same call."""


# The operator's rulebook, in the harness's own format. Jev judges calls against these. The
# warrant check runs before Jev is asked.
CRITERIA = AutoApprovalCriteria(
    sets={
        "read_only": AutoApprovalCriteriaSet(
            effect="Reads an order record. Changes nothing.",
            approve_when=("the order is the one the ticket is about",),
            min_confidence=0.7,
        ),
        "financial": AutoApprovalCriteriaSet(
            effect="Moves money. Cannot be taken back once the processor accepts.",
            approve_when=(
                "the customer reports the item missing, damaged, or not as described",
            ),
            deny_when=("the customer is asking for more than they paid",),
            escalate_when=("the conversation suggests the account may be compromised",),
            min_confidence=0.95,
        ),
    },
    default="financial",
)


async def _stand_in_reviewer(ctx: AutoApprovalContext) -> AutoApprovalDecision:
    """Stands in for Jev when no TypeSafe key is configured: approve in-warrant calls.

    Reached only for calls the warrant already allows and that are under the review threshold.
    """
    return AutoApprovalDecision(
        AutoApprovalVerdict.APPROVE,
        reason="stand-in reviewer (set REFUND_AGENT_JUDGE=jev to use Jev)",
    )


def _current_ticket_warrant() -> Warrant:
    warrant = getattr(workflow.instance(), "ticket_warrant", None)
    if warrant is None:
        raise RuntimeError("No ticket is open on this agent; call open_ticket first.")
    return warrant


def _build_evaluator():
    reviewer = agent.jev_evaluator() if JUDGE == "jev" else _stand_in_reviewer
    return warrant_evaluator(
        _current_ticket_warrant,
        reviewer,
        review_above={"issue_refund": ("amount_cents", REVIEW_ABOVE_CENTS)},
    )


class _RefundAgentBase:
    ticket_warrant: Warrant | None = None

    def _init_runner(self, config: AgentConfig, stream: WorkflowStream) -> None:
        self.ticket_warrant = None
        self._runner = AgentWorkflowRunner(
            config,
            stream=stream,
            # lookup_order is inherently safe, so auto mode never spends a call on it. It is
            # still bound by the warrant: the effects worker checks it like any other call.
            approval_policy_default=ToolApprovalPolicy.auto_mode(
                pre_approve_inherently_safe=True
            ),
            auto_approval_criteria_default=CRITERIA,
            auto_mode_evaluator=_build_evaluator(),
        )

    @workflow.update
    def open_ticket(self, warrant_b64: str) -> str:
        """Hand the agent the task warrant for the ticket it is working.

        The warrant is not a secret: it is bound to the agent's holder key, which never leaves
        the worker's key resolver. A copy of the warrant cannot be exercised without that key.

        ``tenuo_install_warrant`` makes this the SAME ambient warrant context a
        header-carried warrant would set up at workflow start: it validates the chain against
        this worker's trusted roots and confirms ``HOLDER_KEY_ID`` actually resolves to this
        warrant's holder key (a warrant minted for a different holder is rejected, not
        installed) before every later ``lookup_order``/``issue_refund`` call is transparently
        signed against it — no per-call warrant= needed, unlike the workaround this replaces.
        """
        self.ticket_warrant = Warrant.from_base64(warrant_b64)
        tenuo_install_warrant(self.ticket_warrant, HOLDER_KEY_ID)
        return self.ticket_warrant.id


@workflow.defn(name="ScriptedRefundAgent")
@agent.defn
class ScriptedRefundAgentWorkflow(_RefundAgentBase):
    """The refund agent with the model replaced by a script of tool calls.

    Each message is a JSON list of ``{"id", "tool", "args"}`` calls, run in order through the
    same ``run_tool`` path the model's calls take. The reply lists each call's outcome.
    """

    _TOOLS: ClassVar[dict] = {
        "lookup_order": lookup_order,
        "issue_refund": issue_refund,
    }

    @workflow.init
    def __init__(self, config: AgentConfig) -> None:
        # The harness requires the stream to be built directly in @workflow.init.
        self._init_runner(config, WorkflowStream())
        self._outcomes: list[dict] = []

    @workflow.run
    async def run(self, _config: AgentConfig) -> None:
        await self._runner.run(self)

    @workflow.query
    def outcomes(self) -> list[dict]:
        return self._outcomes

    @agent.accepts
    async def act(self, message: TextMessage) -> TextReply:
        """Run a scripted list of tool calls: [{"id", "tool", "args"}, ...]."""
        lines = []
        for call in json.loads(message.text):
            outcome = await self._run_one(call)
            self._outcomes.append(outcome)
            lines.append(f"{call['id']}: {outcome['status']}: {outcome['detail']}")
        return TextReply(text="\n".join(lines))

    async def _run_one(self, call: dict) -> dict:
        tool = self._TOOLS[call["tool"]]
        base = {"id": call["id"], "tool": call["tool"], "args": call["args"]}
        try:
            result = await self._runner.run_tool(call["id"], tool, **call["args"])
            return {**base, "status": "ran", "detail": result}
        except agent.ToolApprovalDenied as e:
            return {**base, "status": "denied_at_gate", "detail": e.reason}
        except ActivityError as e:
            cause = e.cause
            return {
                **base,
                "status": "denied_at_worker",
                "detail": str(cause) if cause is not None else str(e),
                "error_type": cause.type
                if isinstance(cause, ApplicationError)
                else None,
            }


@workflow.defn(name="RefundAgent")
@agent.defn
class RefundAgentWorkflow(_RefundAgentBase):
    """The refund agent, conversing through the OpenAI Agents SDK."""

    @workflow.init
    def __init__(self, config: AgentConfig) -> None:
        self._init_runner(config, WorkflowStream())
        self._conversation: list = []

    @workflow.run
    async def run(self, _config: AgentConfig) -> None:
        await self._runner.run(self)

    @agent.accepts
    async def ask(self, message: TextMessage) -> TextReply:
        """Chat with the support agent about the ticket."""
        with workflow.unsafe.imports_passed_through():
            from agents import Agent as OpenAIAgent
            from agents import Runner

            from temporal_agent_harness.ai_sdks.openai_agents_harness import (
                as_openai_agent_tools,
            )

        sdk_agent = OpenAIAgent(
            name="RefundAgent",
            instructions=SYSTEM_INSTRUCTION,
            model=DEFAULT_MODEL,
            tools=as_openai_agent_tools(self._runner, ALL_TOOLS),
        )
        result = await Runner.run(
            sdk_agent,
            input=[*self._conversation, {"role": "user", "content": message.text}],
        )
        self._conversation = result.to_input_list()
        return TextReply(text=str(result.final_output))
