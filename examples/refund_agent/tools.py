"""Support tools for the refund agent.

Each tool is a single ``@agent.activity_tool_defn`` — durable, activity-backed, and
gated by the harness approval policy (and auto mode) before it runs. There is no
separate effects worker and no manual ``tenuo_execute_activity`` dispatch: the
worker's ``TenuoWorkerInterceptor`` transparently signs a proof of possession over
every activity this workflow schedules (including these), using whatever warrant is
ambient on the workflow (set by ``open_ticket``), and the SAME worker's inbound
interceptor verifies it under ``require_warrant=True`` before the tool body runs.

The trailing ``tool_ctx: AgentToolContext`` argument the harness appends to every
activity-backed tool call (used to publish ``tool_start``/``tool_end`` events) is
excluded from the signed/verified payload via ``pop_exclude_args`` in
``tenuo.temporal.harness.HARNESS_TOOL_CTX_EXCLUDE_ARGS`` (see ``stack.py``) — it
carries no capability a warrant should ever constrain.

The payment processor is an in-memory fake so the example runs offline. Swap
``_ORDERS`` and ``_LEDGER`` for your order service and payment API.

No ``from __future__ import annotations``: activity args cross Temporal's data
converter.
"""

from dataclasses import dataclass
from datetime import timedelta

from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityConfig, unsafe

with unsafe.imports_passed_through():
    from temporal_agent_harness.harness import agent

HOLDER_KEY_ID = "refund-agent"

_TOOL_CONFIG = ActivityConfig(
    start_to_close_timeout=timedelta(seconds=30),
    # Authorization denials are non-retryable anyway; one attempt keeps output readable.
    retry_policy=RetryPolicy(maximum_attempts=1),
)


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
# Agent-facing, activity-backed tools. Activity names (the decorator's default:
# the function's own name) match the warrant's capability names.
# ---------------------------------------------------------------------------


@agent.activity_tool_defn(
    inherently_safe=True,
    auto_approval_criteria="read_only",
    activity_config=_TOOL_CONFIG,
)
async def lookup_order(order_id: str) -> str:
    """Look up an order: customer, total, original payment method, and status.

    Args:
        order_id: The order id, e.g. "ord_1042".
    """
    order = _ORDERS.get(order_id)
    if order is None:
        return f"No order {order_id}."
    return (
        f"{order.order_id}: {order.customer}, total ${order.total_cents / 100:.2f}, "
        f"paid with {order.payment_method}, status {order.status}"
    )


@agent.activity_tool_defn(
    auto_approval_criteria="financial",
    activity_config=_TOOL_CONFIG,
)
async def issue_refund(order_id: str, amount_cents: int, destination: str) -> str:
    """Refund money to a customer. Cannot be taken back once the processor accepts it.

    Args:
        order_id: The order being refunded.
        amount_cents: Amount to refund, in cents.
        destination: Payment method to send the refund to.
    """
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


ALL_TOOLS = [lookup_order, issue_refund]
TOOL_ACTIVITIES = [agent.tool_activity(t) for t in ALL_TOOLS]
