"""A Tenuo layer for the harness's auto mode.

The harness gives every gated tool call a durable seam: the call pauses in the workflow, an
auto mode evaluator may approve, deny, or escalate it, and anything escalated waits for a
human, for hours or days if need be. ``tenuo_evaluator`` puts the task warrant at the front of
that seam:

1. **Outside the warrant: DENY.** Wrong order, more than the customer paid, a different card,
   a tool the task was never given, or an expired ticket. The call never reaches Jev or a
   human, and the model is told exactly which bound it hit so it can try something allowed.
2. **Inside the warrant, above the operator's review threshold: ESCALATE** to the harness's
   human gate.
3. **Inside the warrant, below the threshold:** hand the call to the next evaluator (Jev in
   production) for the judgment call the warrant cannot make: is this refund a good idea?

The warrant is whatever ``get_warrant`` returns for the current workflow (the refund agent
keeps it on the workflow instance). Everything here is pure computation over workflow state
and ``workflow.now()``, so it is deterministic and replays cleanly.

This is the *early* check, and it exists for the model's sake: a precise refusal it can act
on, before anyone is asked to approve something that could never run. The binding check runs
on the effects worker, where the Tenuo plugin verifies the warrant and a proof-of-possession
signature over the exact arguments before the activity body starts. A call that reaches the
worker by another route (a pre-approved tool, a human approval, a bug in an evaluator) is
still held to the same bounds.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from tenuo import Warrant

    from temporal_agent_harness.harness.agent import (
        AutoApprovalContext,
        AutoApprovalDecision,
        AutoApprovalVerdict,
    )
    from temporal_agent_harness.harness.agent_workflow import AUTO_MODE_EVALUATOR_ATTR

Evaluator = Callable[[AutoApprovalContext], Awaitable[AutoApprovalDecision]]


async def _approve(ctx: AutoApprovalContext) -> AutoApprovalDecision:
    return AutoApprovalDecision(
        AutoApprovalVerdict.APPROVE, reason="within the task warrant"
    )


def tenuo_evaluator(
    get_warrant: Callable[[], Warrant],
    next_evaluator: Evaluator | None = None,
    *,
    review_above: dict[str, tuple[str, int]] | None = None,
) -> Evaluator:
    """Build an auto mode evaluator that checks the task warrant first.

    Args:
        get_warrant: Returns the task warrant for the current workflow. Called in-workflow.
        next_evaluator: Consulted for calls that are inside the warrant and under the review
            threshold. Pass ``agent.jev_evaluator()`` in production. ``None`` approves them.
        review_above: Per-tool review thresholds, ``{tool: (argument, limit)}``. A call whose
            ``argument`` exceeds ``limit`` is escalated to a human even though the warrant
            allows it. This is operator policy, not authority: it decides who looks at a call,
            never what the call may do.
    """
    review_above = dict(review_above or {})
    inner = next_evaluator or _approve

    async def evaluate(ctx: AutoApprovalContext) -> AutoApprovalDecision:
        warrant = get_warrant()
        facts: dict[str, Any] = {
            "warrant_id": warrant.id,
            "tool": ctx.tool_name,
        }

        expires_at = datetime.fromisoformat(warrant.expires_at())
        if workflow.now() >= expires_at:
            return AutoApprovalDecision(
                AutoApprovalVerdict.DENY,
                reason="The task warrant for this ticket has expired.",
                details={**facts, "denied_by": "tenuo", "bound": "expiry"},
            )

        violation = warrant.check_constraints(ctx.tool_name, dict(ctx.tool_input))
        if violation is not None:
            return AutoApprovalDecision(
                AutoApprovalVerdict.DENY,
                reason=f"Outside this ticket's warrant: {violation}",
                details={**facts, "denied_by": "tenuo", "bound": violation},
            )

        if ctx.tool_name in review_above:
            arg, limit = review_above[ctx.tool_name]
            value = ctx.tool_input.get(arg)
            if isinstance(value, (int, float)) and value > limit:
                return AutoApprovalDecision(
                    AutoApprovalVerdict.ESCALATE,
                    reason=f"Within the warrant, but {arg}={value} is above the "
                    f"review threshold of {limit}.",
                    details={**facts, "escalated_by": "tenuo", "threshold": limit},
                )

        decision = await inner(ctx)
        decision.details = {**facts, "within_warrant": True, **(decision.details or {})}
        return decision

    inner_label = getattr(inner, AUTO_MODE_EVALUATOR_ATTR, None) or getattr(
        inner, "__qualname__", "evaluator"
    )
    # Published as ``evaluator`` on every auto_approval_evaluation_* event.
    setattr(evaluate, AUTO_MODE_EVALUATOR_ATTR, f"tenuo -> {inner_label}")
    return evaluate
