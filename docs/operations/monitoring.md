# Monitoring

The instruments, their exact names, what each one counts, and what to alert on.

## Turning it on

Two lines. **The names are the contract** — a subscriber names the meter and the
activity source, never a type:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("Orleans.Dataflow"))
    .WithTracing(tracing => tracing.AddSource("Orleans.Dataflow"));
```

A silo hosting runs emits engine metrics and run spans automatically. A client
emits the materialize span.

## The instruments

| Instrument | Kind | Unit | What it counts |
|---|---|---|---|
| `orleans.dataflow.runs.started` | counter | `{run}` | Run attempts started, fresh and resumed alike; the `dataflow.run.resumed` tag tells them apart. |
| `orleans.dataflow.runs.ended` | counter | `{run}` | Run attempts that reached a terminal state; `dataflow.run.outcome` says which. |
| `orleans.dataflow.checkpoint.hold.duration` | histogram | `s` | How long each checkpoint held its run quiescent — **including holds whose write failed or was skipped**. |
| `orleans.dataflow.elements.dropped` | observable counter | `{element}` | Elements discarded by declared overflow policies at engine-owned boundaries. |
| `orleans.dataflow.failures.supervised` | observable counter | `{failure}` | Failures a supervision scope intercepted, one per failed attempt. |
| `orleans.dataflow.elements.poison` | observable counter | `{element}` | Elements that exhausted every retry attempt a scope declared. |
| `orleans.dataflow.checkpoints.written` | observable counter | `{checkpoint}` | Checkpoint documents the store accepted. |

Collected from a real run — six elements through a buffer of two with
`DropOldest`, durable at a cadence of two — the whole surface is:

```text
orleans.dataflow.checkpoint.hold.duration 0     {dataflow.graph=<fingerprint>}
orleans.dataflow.checkpoint.hold.duration 0     {dataflow.graph=<fingerprint>}
orleans.dataflow.checkpoint.hold.duration 0.004 {dataflow.graph=<fingerprint>}
orleans.dataflow.checkpoints.written      3     {dataflow.graph=<fingerprint>}
orleans.dataflow.elements.dropped         1     {dataflow.graph=<fingerprint>}
orleans.dataflow.elements.poison          0     {dataflow.graph=<fingerprint>}
orleans.dataflow.failures.supervised      0     {dataflow.graph=<fingerprint>}
orleans.dataflow.runs.ended               1     {dataflow.graph=<fingerprint>, dataflow.run.outcome=completed}
orleans.dataflow.runs.started             1     {dataflow.graph=<fingerprint>, dataflow.run.resumed=False}
```

Three notes an operator needs about what those numbers mean.

**The cumulative counters are the runs' own counters, read rather than
duplicated.** On each collection the library sums every live run's counters with
the totals runs left behind when they settled, under one gate, so a run is
counted exactly once whether it is still live or already folded. They keep
counting a graph's totals after its runs settle, which is what makes rates read
across run boundaries.

**An adapter's private ingress is not folded in.** `elements.dropped` counts
drops at engine-owned boundaries. A stream, broadcast, observer, or reminder
source counts its own ingress drops internally and does not contribute here. If
you depend on a push adapter's ingress policy, watch the sink's own count of what
arrived as well.

**Nothing is on the element hot path.** A stage pays nothing for metrics nobody
is collecting, and the same nothing when they are, because the collector reads
state the run already keeps. The only eager emissions are one event per run
start, one per run end, and one histogram sample per checkpoint hold — all cold
paths.

And one that changes what you can rely on: **telemetry never fails a run.** Every
emission swallows, because a listener that throws from a measurement callback is
a broken observer and a run that died of being observed would be a worse defect
than any lost sample.

## The tags

| Tag | On | Values |
|---|---|---|
| `dataflow.graph` | Every instrument, and both spans | The document fingerprint, `sha256:` and 64 hex digits — or `(other)`. |
| `dataflow.run.outcome` | `runs.ended`, and the `dataflow.run` span | `completed`, `failed`, `canceled` |
| `dataflow.run.resumed` | `runs.started`, and the `dataflow.run` span | `True`, `False` |
| `dataflow.run.id` | The `dataflow.materialize` span **only** | The run identity. |
| `dataflow.run.durable` | The `dataflow.materialize` span **only** | Whether the materialization declared durability. |

**Run identities are never metric tags.** They are unbounded by construction, so
they appear only on activities, where per-occurrence identity is the point and no
aggregation is paying for it.

### The cardinality bound, and the overflow bucket

`dataflow.graph` is bounded **by this library rather than by your deployment**:
the first **1,024** distinct fingerprints a process sees keep their own tag value,
permanently, and every fingerprint after that is reported under the single value
`(other)`.

The bound is needed because a fingerprint covers every number in a document. A
graph whose buffer capacity, take count, or collect bound comes from a request
mints a fresh fingerprint **per request** — which is unbounded cardinality on
seven instruments plus an entry per value in the settled table, and nothing
prunes either.

**Seeing `(other)` means:** this process has run more distinct graph *shapes*
than it keeps series for, and the counters under that value are the **sum** of
every graph folded into it. It is a real bucket, not a discard, and it is spelled
so it cannot be mistaken for a fingerprint — a fingerprint is `sha256:` and
sixty-four hex digits, and this is the only value of the tag that is not.

**The fix is on your side, not in a configuration knob:** parameterise graphs
from a fixed set rather than from request values. A pipeline whose buffer
capacity is one of three constants mints three fingerprints; one whose capacity
is `request.PageSize` mints one per distinct page size anyone has ever asked for.

Two properties worth knowing before you build a dashboard on it:

- **Naming is first-come and never revised.** A fingerprint that has been named
  keeps its name for the life of the process, and one that overflowed stays
  overflowed. That is what keeps every cumulative reading monotonic: each graph
  contributes to exactly one series, and the bucket a run's counters land in
  cannot change between the run starting and settling.
- **The bound is per process, not per deployment.** Two silos each name their own
  first thousand, so a fingerprint named on one and overflowed on another is
  possible — and is the honest reading: what a tag says is what *that process*
  could still tell apart.

## The spans

| Span | Emitted by | Covers |
|---|---|---|
| `dataflow.materialize` | The client host | The materialization call: validating, declaring, and starting. |
| `dataflow.run` | The silo hosting the run | The run's whole life, from start to terminal state. |

`dataflow.run` is started with whatever ambient parent the materializing caller
had — a client's materialize span, a grain call's span — and it **outlives the
call that started it**, which is why it is the span to look at for a run's
duration and the materialize span is the one to look at for admission latency.

The span keeps the fingerprint itself even when the metrics have folded that
graph into `(other)`: a span is one occurrence and carries a run id already, so
nothing aggregates over it and there is no cardinality to save by blurring it.

## The three reading surfaces of a run

Choosing between them is choosing what a failure should do to *you*.

| Surface | Answers with | Use it when |
|---|---|---|
| `Completion` | The outcome as a **task outcome**. Awaiting it makes the run's failure your own — the exception instance locally, a `PipelineRunFailedException` carrying the type name and message over the wire. | Your code should stop when the run does. |
| `WatchTermination` | The [ending](../reference/glossary.md#ending) as a **value**: `Completed`, or `Failed(type, message)`. Cancels for a cancelled run, and — cluster only — faults with `PipelineRunLostException` when no ending will ever come. | You are a coordinator, a log, or a metric reacting to endings rather than inheriting them. |
| `Snapshot()` / `SnapshotAsync()` | Status plus five counters: dropped elements, supervised failures, poison elements, checkpoints written, total checkpoint hold. | You are sampling a run that is still going. |

Three things about the snapshot specifically:

- **Not a consistent cut.** Each number is exact on its own; they are read at
  slightly different moments.
- **Over the wire the counters describe the answering attempt.** A durable run's
  ending re-read after its activation died comes from the coordinator's register,
  which records outcomes and not diagnostics — so those counters read **zero**.
  History belongs to the meter.
- **`SnapshotAsync` is one grain call per reading.** Unlike `Completion` it
  neither starts nor joins the poll loop, so a monitor sampling on its own
  schedule costs exactly the calls it makes. Its cancellation token cancels *your
  wait* and nothing else; the run neither notices nor changes.

### The fourth reading: an ending nobody recorded

A durable attempt can reach a terminal state and fail to record it — the
coordinator may refuse the report as stale, or be unreachable. Nothing on the
attempt's side can fix that, but an operator seeing it knows the register and the
attempt disagree, and which one watched the run end.

That fact is carried on **every later reading of the attempt**, as
`RunStatusSnapshot.UnrecordedEnding`, and it is reached through the run grain
rather than through the run handle:

```csharp
IPipelineRunGrain run = grains.GetGrain<IPipelineRunGrain>($"{graphId}/{runId}");
RunStatusSnapshot status = await run.GetStatusAsync(epoch);

if (status.UnrecordedEnding is { } refusal)
{
    // The attempt ended, and the register does not know how. `refusal` is the coordinator's own words.
}
```

`OrleansRunHandle.SnapshotAsync` returns a `RunSnapshot`, which does not carry
this field — so a monitor that needs it reads the grain's status directly.

It is a **reading and never a refusal**, deliberately: a poll that faulted on it
would stop reporting the outcome it was polling for, and a client watching a
completed run would learn that the register is unhappy instead of learning that
its run completed. Proven by
`TrustBoundaryTests.AnEndingThatCouldNotBeRecordedIsCarriedOnEveryLaterReadingOfTheAttempt`.

**Why it matters:** a durable run whose ending nobody wrote down is a run a later
activation resumes and re-runs the tail of.

## What user data can reach a failure message

A run's failure message is stored on its run grain, returned to every caller that
polls it, and — for a durable run — written into the coordinator's **persistent
state, which nothing prunes**. Treat it as durable, widely readable text.

Two things put an author's own data into it:

- **A `GroupBy` that exceeds its active-key bound names the offending key**,
  rendered by that key's own `ToString()` and cut to the first 64 characters with
  the full length reported after it. If keys are email addresses, account
  numbers, or tenant identifiers, that prefix reaches durable storage.
- **A distributed keyed grain call whose callee throws reports the executor
  grain's address**, which is `{graph}/{run}/{node}/{key}` and carries the
  routing key **in full**. It is an address rather than a diagnostic, and
  truncating it would make two partitions collide, so it is not cut.

Everything else this library puts in a message is a type name, a node identifier,
a count, or a bound — never an element's value.

**To keep identifying values out of durable failure text**, group and route over
an opaque or hashed key and carry the identifying value in the element instead.
Do this before you deploy rather than after an incident: the messages already
written are in the coordinator's state.

## Alerting starting points

Ordered by how much they mean.

| Signal | Reading | Action |
|---|---|---|
| `runs.ended{outcome="failed"}` rate above zero | A run failed outright. | **Page.** Read the failure type on the handle or the grain status. |
| `elements.poison` moving **at all** | Retries are being exhausted; elements are being dropped, replaced, or failing the run according to what the scope declared. | Investigate. A non-zero rate here means a declared policy is now load-bearing. |
| `failures.supervised` rate rising | A run limping *inside* its declared policies. Nothing is broken yet. | Watch. Rising with a flat `poison` count means the retries are working and something downstream is degraded. |
| `checkpoint.hold.duration` p99 growing | Capture cost is eating throughput. | Lengthen the interval or shrink the state. There is no overlapped capture — a hold stops the whole run. |
| `checkpoint.hold.duration` p99 above ~4 s | The store is timing out and the write is being retried to exhaustion. | Treat as a store outage: [the runbook](runbooks.md#recovering-from-a-checkpoint-store-outage). |
| `runs.started{resumed="True"}` rate rising | Runs are being resumed more often than usual — silos are recycling, or attempts are stranding. | Correlate with silo restarts and with `CheckpointWriteFailedException`. |
| `elements.dropped` rate above your declared budget | A declared overflow policy is discarding elements. | This is a decision you made, not a fault. Alert on it if the budget matters. |
| `dataflow.graph="(other)"` appearing | This process ran more than 1,024 distinct graph shapes. | Parameterise graphs from a fixed set; the counters under that value are a sum. |

Two things worth building even though they are not instruments:

- **A check on the durable register's size** against the cap of 1,000 identities
  per pipeline. The refusal when it fills stops *every* start of that pipeline,
  and the fix — [retirement](runbooks.md#retiring-a-run-identity) — is a
  procedure someone has to have run before it is urgent.
- **A watch for `UnrecordedEnding`** on the durable runs you care about. Nothing
  emits a metric for it, because it is a property of one attempt rather than a
  rate.

## What monitoring cannot give you

- **A counter that survives its attempt in the cluster's register.** Use the
  meter for history; the register records outcomes, not diagnostics.
- **Per-element instrumentation.** The observable counters read the same state
  the snapshot reads, and no element-path instrumentation exists — by design,
  because it would be on the hot path.
- **An ending for a lost ordinary run.** `PipelineRunLostException` is the honest
  report. Name a run durable if that is not acceptable.

## Next

- [Runbooks](runbooks.md) — what to do about each of the signals above.
- [Checkpoint stores](checkpoint-stores.md) — why the hold histogram is where a slow store shows up.
- [Testing and observability](../guides/testing-and-observability.md) — the same instruments from the author's side, plus the deterministic testing tools.
