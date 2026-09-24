# Refund agent: auto mode with a task warrant

A support agent that can actually issue refunds. The harness gives every refund a durable
approval seam; auto mode reviews it (Jev in production); a [Tenuo](https://github.com/tenuo-ai/tenuo)
warrant, minted per ticket, sets the bounds no reviewer can move.

```text
model picks issue_refund(...)
        |
        v
harness approval seam (in the workflow, durable)
   1. Tenuo: inside this ticket's warrant?         no  -> DENY, reason goes back to the model
   2. over the $500 review threshold?              yes -> ESCALATE to a human, wait durably
   3. Jev (or the stand-in): is this a good idea?  approve / deny / escalate
        |
        v
tenuo_execute_activity: attach the warrant, sign proof of possession over the exact arguments
        |
        v
effects worker (holds the payment credentials, require_warrant=True)
   Tenuo plugin verifies warrant + proof + arguments   no  -> non-retryable denial
        |
        v
issue_refund activity runs
```

The warrant for ticket `ord_1042` allows `lookup_order` for that order and `issue_refund` for
that order, up to what the customer paid, back to the card they paid with, for 30 minutes.
Everything else is outside it.

## Run it

```bash
temporal server start-dev                       # terminal 1
uv run python -m examples.refund_agent.demo     # terminal 2, from the repo root
```

No API keys. The demo replaces the model with a script of tool calls and uses a stand-in for Jev;
everything else is the real path. It walks two tickets:

| Call | Outcome | Where |
|---|---|---|
| `lookup_order(ord_1042)` | runs | pre-approved, checked on the effects worker |
| `issue_refund(ord_1042, $45, pm_card_4242)` | runs | reviewer approves |
| `issue_refund(ord_1042, $45, pm_card_9999)` | denied: `destination` | approval seam |
| `issue_refund(ord_1042, $1,890, pm_card_4242)` | denied: `amount_cents` | approval seam |
| `lookup_order(ord_2210)` | denied: `order_id` | effects worker (the tool skips the seam) |
| `issue_refund(ord_2210, $600, pm_card_1881)` | waits for a human; the agent worker restarts; approved; runs once | approval seam, then worker |

With a real model:

```bash
OPENAI_API_KEY=... uv run python -m examples.refund_agent.live
OPENAI_API_KEY=... REFUND_AGENT_JUDGE=jev TYPESAFE_API_KEY=... \
    uv run python -m examples.refund_agent.live --order ord_2210
```

## Tests

```bash
TEMPORAL_ADDRESS=localhost:7233 uv run pytest tests/examples/refund_agent -v
```

Without `TEMPORAL_ADDRESS` the tests start a local dev server with
`WorkflowEnvironment.start_local()`. They cover each outcome above, a policy that skips every
gate (the effects worker still holds the line), an expired warrant, a human denial, and the
OpenAI Agents SDK path with a canned model that is refused and then recovers.

## Files

| File | What it does |
|---|---|
| `workflow.py` | The agent (`RefundAgentWorkflow`, OpenAI Agents SDK) and its scripted twin, auto mode policy and criteria, `open_ticket` |
| `tenuo_gate.py` | `tenuo_evaluator`: the warrant check in front of any auto mode evaluator |
| `tools.py` | Agent-facing tools (`@agent.tool_defn`) and the effect activities they dispatch with the warrant |
| `warrants.py` | Minting the per-ticket warrant |
| `stack.py` | Clients and the two workers |
| `demo.py`, `live.py` | Scripted walkthrough; interactive chat with a real model |

## Design notes

- **Why the tools are inline and the effects are separate activities.** The agent-facing tool
  runs in the workflow so the harness can gate it; the effect runs as a plain Temporal activity
  on the effects task queue, dispatched with `tenuo_execute_activity` and a per-dispatch
  warrant. The agent worker's own activities (model calls, the Jev call) carry no warrant and
  are not effects, which is why the agent worker runs with `require_warrant=False` and the
  effects worker with `require_warrant=True`. Tenuo logs a warning for the former; that is
  expected here.
- **Why the seam check and the worker check both exist.** The seam check is for the model: a
  precise refusal before anyone is asked to approve something that could never run. The worker
  check is the guarantee: it also covers pre-approved tools, human approvals, and a
  misconfigured policy.
- **The review threshold is policy, not authority.** It decides who looks at a refund. The
  warrant decides what a refund can be.
