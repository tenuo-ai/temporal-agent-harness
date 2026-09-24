"""Minting the task warrant for one support ticket.

In production the issuer key belongs to your control plane (or Tenuo Cloud) and the holder key
is resolved from a secrets manager. Here both are generated per run so the example is
self-contained.

The warrant is the ceiling for everything this agent does while it works the ticket:

* ``lookup_order`` for this order only;
* ``issue_refund`` for this order only, at most what the customer paid, and only back to the
  payment method they paid with;
* for as long as the ticket is open (``ttl``).

Nothing else. A tool the warrant does not name is denied, whatever the model, the evaluator,
or a human approver says about it.
"""

from __future__ import annotations

from tenuo import Exact, Range, SigningKey, Warrant

from .tools import Order


def mint_ticket_warrant(
    order: Order,
    *,
    issuer_key: SigningKey,
    holder_key: SigningKey,
    ttl_seconds: int = 1800,
) -> Warrant:
    return (
        Warrant.mint_builder()
        .holder(holder_key.public_key)
        .capability("lookup_order", order_id=Exact(order.order_id))
        .capability(
            "issue_refund",
            order_id=Exact(order.order_id),
            amount_cents=Range(0, order.total_cents),
            destination=Exact(order.payment_method),
        )
        .ttl(ttl_seconds)
        .mint(issuer_key)
    )
