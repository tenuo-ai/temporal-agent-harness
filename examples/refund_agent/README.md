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
issue_refund runs as a durable, activity-backed tool (@agent.activity_tool_defn) —
the worker's TenuoWorkerInterceptor transparently signs proof of possession over the
exact arguments, using this ticket's warrant
        |
        v
same worker, require_warrant=True
   Tenuo plugin verifies warrant + proof + arguments   no  -> non-retryable denial
        |
        v
issue_refund activity body runs
```

The warrant for ticket `ord_1042` allows `lookup_order` for that order and `issue_refund` for
that order, up to what the customer paid, back to the card they paid with, for 30 minutes.
Everything else is outside it.

One worker runs the agent workflow AND its tools, under `require_warrant=True` — there is no
separate effects worker and no inline-tool workaround. `tenuo.temporal.harness` supplies the
three presets that make this possible: `pop_exclude_args` drops the harness's own
`tool_ctx: AgentToolContext` argument from the signed payload, `unwarranted_activities` exempts
the harness's internal activities (model calls, the Jev approval activity, ...) from the warrant
requirement, and `mcp_call_tool_activities` (not used by this example, no MCP tools here) would
unwrap an MCP call-tool wrapper the same way. See
[`docs/temporal-harness.md`](https://github.com/tenuo-ai/tenuo/blob/main/docs/temporal-harness.md)
in the Tenuo repo.

## Run it

```bash
temporal server start-dev                       # terminal 1
uv run python -m examples.refund_agent.demo     # terminal 2, from the repo root
```

No API keys. The demo replaces the model with a script of tool calls and uses a stand-in for Jev;
everything else is the real path. It walks two tickets:

| Call | Outcome | Where |
|---|---|---|
| `lookup_order(ord_1042)` | runs | pre-approved, checked when the activity dispatches |
| `issue_refund(ord_1042, $45, pm_card_4242)` | runs | reviewer approves |
| `issue_refund(ord_1042, $45, pm_card_9999)` | denied: `destination` | approval seam |
| `issue_refund(ord_1042, $1,890, pm_card_4242)` | denied: `amount_cents` | approval seam |
| `lookup_order(ord_2210)` | denied: `order_id` | activity dispatch (the tool skips the seam) |
| `issue_refund(ord_2210, $600, pm_card_1881)` | waits for a human; the worker restarts; approved; runs once | approval seam, then activity dispatch |

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
gate (the worker still holds the line), an expired warrant, a human denial, and the
OpenAI Agents SDK path with a canned model that is refused and then recovers.

## Files

| File | What it does |
|---|---|
| `workflow.py` | The agent (`RefundAgentWorkflow`, OpenAI Agents SDK) and its scripted twin, auto mode policy and criteria, `open_ticket` |
| `tools.py` | The agent's two durable, activity-backed tools (`@agent.activity_tool_defn`) |
| `warrants.py` | Minting the per-ticket warrant |
| `stack.py` | The client and the single agent+effects worker, wired with `tenuo.temporal.harness.harness_plugin_config` |
| `demo.py`, `live.py` | Scripted walkthrough; interactive chat with a real model |

`tenuo_gate.py` no longer exists here — the warrant-checking auto-mode evaluator it defined is
now `tenuo.temporal.harness.warrant_evaluator`, shipped in Tenuo itself.

## Design notes

- **Why one worker is enough now.** Every `@agent.activity_tool_defn` tool is a real Temporal
  activity; `TenuoWorkerInterceptor` signs and verifies it like any other, using whatever warrant
  is ambient on the workflow. The only thing that used to force a split — the harness's own
  `tool_ctx` argument and its own internal activities having no warrant — is handled by the two
  `tenuo.temporal.harness` presets in `stack.py`. A production deployment may still choose to
  split workers for blast-radius reasons; this example shows that it is a choice, not a
  requirement.
- **Why the seam check and the dispatch-time check both exist.** The seam check is for the
  model: a precise refusal before anyone is asked to approve something that could never run. The
  dispatch-time check is the guarantee: it also covers pre-approved tools, human approvals, and a
  misconfigured policy.
- **The review threshold is policy, not authority.** It decides who looks at a refund. The
  warrant decides what a refund can be.
