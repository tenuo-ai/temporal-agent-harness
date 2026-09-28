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

A refund above ``review_above_cents`` additionally requires a **cryptographic** approval from
``approver_key`` (``required_approvers`` + ``min_approvals`` + an ``approval_gates`` exemption
for everything at or under the threshold) — the same $500 line the harness's own auto-mode
review threshold uses, now also enforced as a signing requirement on the effects worker. The
harness's ``approve_tool`` update is a human's yes/no in the approval seam; this is the
independent, worker-side guarantee that only a hash-bound signature from ``approver_key`` can
satisfy it — a forged or unsigned approval is rejected there even if the seam said yes.
"""

from __future__ import annotations

import os
import time

from tenuo import Exact, Range, SigningKey, Warrant

from .tools import Order


def mint_ticket_warrant(
    order: Order,
    *,
    issuer_key: SigningKey,
    holder_key: SigningKey,
    approver_key: SigningKey,
    ttl_seconds: int = 1800,
    review_above_cents: int = 50_000,
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
        .required_approvers([approver_key.public_key])
        .min_approvals(1)
        .approval_gates(
            {"issue_refund": {"amount_cents": {"exempt": Range(0, review_above_cents)}}}
        )
        .ttl(ttl_seconds)
        .mint(issuer_key)
    )


def sign_refund_approval(warrant: Warrant, approver_key: SigningKey, args: dict) -> object:
    """Sign a cryptographic approval for one ``issue_refund`` call.

    ``args`` must be exactly the arguments that call will dispatch with
    (``order_id``, ``amount_cents``, ``destination``) — the signature is bound to
    this exact request via a hash over ``(warrant.id, tool, args, holder_key)``,
    not just to the warrant or the approver's identity. Signing with a key that
    is not one of the warrant's ``required_approvers`` (e.g. a forged approval)
    produces a real ``SignedApproval`` object that verifies cryptographically
    against *itself*, but the effects worker rejects it: the warrant's chain
    check only trusts approvals from the specific keys named in
    ``required_approvers``.
    """
    import tenuo_core

    request_hash = tenuo_core.py_compute_request_hash(
        warrant.id, "issue_refund", args, warrant.holder_key,
    )
    now = int(time.time())
    payload = tenuo_core.ApprovalPayload(
        request_hash=request_hash,
        nonce=os.urandom(16),
        external_id="approver@example.com",
        approved_at=now,
        expires_at=now + 300,
    )
    return tenuo_core.SignedApproval.create(payload, approver_key)
