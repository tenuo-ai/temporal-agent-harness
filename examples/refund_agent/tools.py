"""Support tools for the refund agent.

Each tool has two halves:

* **The agent-facing tool** (``@agent.tool_defn``) is what the model calls. It runs inline in
  the workflow, so the harness approval policy (and auto mode) gates it before anything
  happens.
* **The effect** is an ordinary Temporal activity on a separate task queue, served by a worker
  whose Tenuo plugin requires a warrant. The tool dispatches it with
  ``tenuo_execute_activity``, which attaches this ticket's warrant and signs a proof of
  possession over the exact arguments. The effects worker verifies both before the activity
  body runs.

So the model never touches the payment API directly: every refund crosses the approval seam
in the workflow and then a warrant check on the worker that holds the payment credentials.

The payment processor is an in-memory fake so the example runs offline. Swap ``_ORDERS`` and
``_LEDGER`` for your order service and payment API.

No ``from __future__ import annotations``: activity args cross Temporal's data converter.
"""

from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from tenuo.temporal import tenuo_execute_activity

    from temporal_agent_harness.harness import agent

EFFECTS_TASK_QUEUE = "refund-agent-effects"
HOLDER_KEY_ID = "refund-agent"

_EFFECT_OPTIONS = {
    "task_queue": EFFECTS_TASK_QUEUE,
    "start_to_close_timeout": timedelta(seconds=30),
    # Authorization denials are non-retryable anyway; one attempt keeps output readable.
    "retry_policy": RetryPolicy(maximum_attempts=1),
}


@dataclass
class Order:
    order_id: str
    customer: str
    total_cents: int
    payment_method: str
    status: str


@dataclass
class Receipt:
    refund_id: str
    order_id: str
    amount_cents: int
    destination: str


# Demo data. ord_1042 is the order the support ticket is about.
_ORDERS: dict[str, Order] = {
    "ord_1042": Order(
        "ord_1042", "Dana Whitfield", 18_900, "pm_card_4242", "delivered"
    ),
    "ord_2210": Order("ord_2210", "Sam Ortega", 96_500, "pm_card_1881", "delivered"),
}

# Every refund that actually executed. Tests read this to prove a denied call never ran.
_LEDGER: list[Receipt] = []


def ledger() -> list[Receipt]:
    return list(_LEDGER)


def reset_ledger() -> None:
    _LEDGER.clear()


def get_order(order_id: str) -> Order:
    return _ORDERS[order_id]


# ---------------------------------------------------------------------------
# Effects: plain Temporal activities, served by the Tenuo-protected effects worker.
# Activity names match the warrant's capability names.
# ---------------------------------------------------------------------------


@activity.defn(name="lookup_order")
async def lookup_order_activity(order_id: str) -> str:
    order = _ORDERS.get(order_id)
    if order is None:
        return f"No order {order_id}."
    return (
        f"{order.order_id}: {order.customer}, total ${order.total_cents / 100:.2f}, "
        f"paid with {order.payment_method}, status {order.status}"
    )


@activity.defn(name="issue_refund")
async def issue_refund_activity(
    order_id: str, amount_cents: int, destination: str
) -> str:
    receipt = Receipt(
        refund_id=f"re_{len(_LEDGER) + 1:04d}",
        order_id=order_id,
        amount_cents=amount_cents,
        destination=destination,
    )
    _LEDGER.append(receipt)
    return (
        f"Refunded ${amount_cents / 100:.2f} on {order_id} to {destination} "
        f"({receipt.refund_id})."
    )


EFFECT_ACTIVITIES = [lookup_order_activity, issue_refund_activity]


# ---------------------------------------------------------------------------
# Agent-facing tools: gated by the harness, dispatched with the ticket's warrant.
# ---------------------------------------------------------------------------


def _ticket_warrant():
    """The warrant for the ticket this agent is working, held on the workflow instance."""
    warrant = getattr(workflow.instance(), "ticket_warrant", None)
    if warrant is None:
        raise RuntimeError("No ticket is open on this agent; call open_ticket first.")
    return warrant


@agent.tool_defn(inherently_safe=True, auto_approval_criteria="read_only")
async def lookup_order(order_id: str) -> str:
    """Look up an order: customer, total, original payment method, and status.

    Args:
        order_id: The order id, e.g. "ord_1042".
    """
    return await tenuo_execute_activity(
        lookup_order_activity,
        args=[order_id],
        warrant=_ticket_warrant(),
        key_id=HOLDER_KEY_ID,
        **_EFFECT_OPTIONS,
    )


@agent.tool_defn(auto_approval_criteria="financial")
async def issue_refund(order_id: str, amount_cents: int, destination: str) -> str:
    """Refund money to a customer. Cannot be taken back once the processor accepts it.

    Args:
        order_id: The order being refunded.
        amount_cents: Amount to refund, in cents.
        destination: Payment method to send the refund to.
    """
    return await tenuo_execute_activity(
        issue_refund_activity,
        args=[order_id, amount_cents, destination],
        warrant=_ticket_warrant(),
        key_id=HOLDER_KEY_ID,
        **_EFFECT_OPTIONS,
    )


ALL_TOOLS = [lookup_order, issue_refund]
