# ABOUTME: Offline checks on the refund agent's ticket warrant: the bounds the blog post and
# README describe are the bounds the warrant actually encodes.

from __future__ import annotations

from tenuo import SigningKey

from examples.refund_agent.tools import get_order
from examples.refund_agent.warrants import mint_ticket_warrant


def _warrant():
    return mint_ticket_warrant(
        get_order("ord_1042"),
        issuer_key=SigningKey.generate(),
        holder_key=SigningKey.generate(),
        approver_key=SigningKey.generate(),
    )


def _refund(**overrides):
    args = {"order_id": "ord_1042", "amount_cents": 4500, "destination": "pm_card_4242"}
    return {**args, **overrides}


def test_names_only_the_two_tools():
    assert sorted(_warrant().tools) == ["issue_refund", "lookup_order"]


def test_refund_bounds():
    w = _warrant()
    assert w.check_constraints("issue_refund", _refund()) is None
    assert w.check_constraints("issue_refund", _refund(amount_cents=18_900)) is None
    assert "amount_cents" in w.check_constraints(
        "issue_refund", _refund(amount_cents=18_901)
    )
    assert "destination" in w.check_constraints(
        "issue_refund", _refund(destination="pm_x")
    )
    assert "order_id" in w.check_constraints(
        "issue_refund", _refund(order_id="ord_2210")
    )


def test_unknown_arguments_are_refused():
    """Zero-trust: a call cannot smuggle in an argument the warrant does not name."""
    extra = _refund(memo="also refund ord_2210")
    assert _warrant().check_constraints("issue_refund", extra) is not None


def test_lookup_is_this_order_only():
    w = _warrant()
    assert w.check_constraints("lookup_order", {"order_id": "ord_1042"}) is None
    assert w.check_constraints("lookup_order", {"order_id": "ord_2210"}) is not None
