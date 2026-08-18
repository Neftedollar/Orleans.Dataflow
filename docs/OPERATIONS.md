# Operating Orleans.Dataflow

What a deployment needs to know to run pipelines in production: what a durable
run demands of its store, where runs are placed, what the identities mean, how
to replace and upgrade, and how to watch all of it. Everything here restates
contracts the design documents establish — [ORLEANS-RUNTIME.md](design/ORLEANS-RUNTIME.md)
and [LOCAL-RUNTIME.md](design/LOCAL-RUNTIME.md) are the authorities; this page
is the operator's ordering of them.

## Registration

Every silo that may host runs calls `AddOrleansDataflow` on its
`ISiloBuilder`, and registers the same stages with the same versions on every
silo — the coordinator refuses a document a silo cannot resolve, and a rolling
upgrade is exactly the window where silos disagree (see
[Rolling upgrade](#rolling-upgrade)). A client materializes through
`AddOrleansDataflowClient` and `OrleansDataflowHost`.

A silo that may host a **durable** run additionally calls
`UseCheckpointStore(...)` — every such silo, and over the same store. A cluster
whose silos disagree about the store accepts a declaration on one host and
cannot honor it on another; the refusal a run then gets names exactly this.

## What a durable run demands of its store

`ICheckpointStore` is three members, and the duties are the contract — a store
that shirks any of them turns at-least-once into silent loss:

- **`WriteAsync` is atomic per document.** A reader never observes a torn
  checkpoint: it sees the previous document or the new one, whole. This is
  the one duty no test can hold for you — the suite's store lives in the
  test process and cannot be torn by a silo dying (the recorded M5.3
  limit) — so the contract states it and your store implementation carries
  it.
- **`WriteAsync` is a compare-and-swap on the ETag.** A write presenting a
  stale ETag throws `CheckpointConflictException`, and that refusal is load
  bearing: it is how a superseded attempt — a zombie writer on a silo the
  cluster has moved past — is fenced out. A store that "helpfully" last-writer-
  wins re-opens the very race the epoch protocol closes.
- **`ClearAsync` is the destructive half of replacement** and honors the same
  ETag discipline.

The in-memory store shipped for tests models the contract exactly; a
production store maps it onto any document store with conditional writes
(blob leases, CosmosDB ETags, SQL rowversion — anything that can refuse a
stale writer).

## Identities

- **A graph is identified by its document fingerprint** (canonical bytes,
  SHA-256). Two builds of the same shape share one; delegates never enter it.
- **An ordinary run is named per materialization.** Two `MaterializeAsync`
  calls are two runs. Its results and its fate live exactly as long as its
  activation; a recycled activation is a lost run, reported as
  `PipelineRunLostException`, never waited out.
- **A durable run is named by its author** (`DurablePipelineOptions.RunId`),
  and the name is the unit of continuation: two `MaterializeDurableAsync`
  calls under one name are one run — the second returns a handle to the run
  that exists, or continues it from its checkpoint if its silo died. One
  document per name; a name holding a different document is refused with both
  fingerprints, because checkpoints do not migrate across documents (ADR 0007).

## Checkpoint timing, and what the numbers mean

`Interval` and/or `EveryElements` on the durable options. A capture **holds
the run for its duration** — pause, snapshot, write, release; nothing overlaps
— and the cost is observable as the checkpoint-hold histogram and the
snapshot's `TotalCheckpointHold`. Delivery between marks is **at-least-once,
never exactly-once**: a crash replays from the last stored capture, and the
replay window per adapter is stated in that adapter's
[ADAPTERS.md](ADAPTERS.md) row (a stream source's window includes the element
its cursor names; the grain-call sink's mark can lag by up to `maxInFlight`
and is exact at a bound of one). A run that completes writes no checkpoint —
its outcome is recorded on the coordinator's register instead, which is what
keeps a finished run finished across activations.

## Runbook: replacing a durable run

`ReplaceDurableRunAsync` is the deployment saying "destroy what the name
holds": the stored checkpoint is cleared, the previous attempt is superseded
by a fresh epoch, and the document runs from the beginning under the name.
Notes an operator needs:

- Replacing with the **same** document is how a finished durable run is run
  again; a finished run is otherwise permanently finished.
- `CheckpointConflictException` from a replace means something was still
  writing under the identity between your read and your clear. Retrying the
  replacement is safe and is the answer.
- The old attempt is stopped by the replacement's second hop reaching its
  activation. A caller that only talks to the coordinator and never starts
  the replacement leaves the old attempt running until its next capture is
  refused — or forever, if it declared no timing. Use the host method, not
  the grain.

## Runbook: rolling upgrade

The register keeps a **catalog fingerprint** beside each declaration, and a
resume re-runs the catalog discipline on whichever silo catches the
activation. During a roll, silos disagree about the catalog; the outcomes are
the designed ones:

- A resume landing on a silo that resolves the document's stages continues.
- A resume landing on a silo that cannot is **refused by name**, not guessed
  at; the run continues when a capable silo picks it up (or the deployment
  finishes rolling). Nothing is lost — the checkpoint is untouched by a
  refusal.
- New documents (new revisions) materialize beside old runs freely; a
  revision **replaces** a name only through the replace runbook above.
  "Same fingerprint and same revision, or refuse" is the whole resume rule —
  there is no cross-revision checkpoint migration in v1.

## Watching a run

Three reading surfaces, in order of closeness:

- **`Completion`** — the outcome as a task outcome: await it to make the
  run's failure your own. The exception instance locally; the type-name and
  message pair (as `PipelineRunFailedException`) over the wire.
- **`WatchTermination`** — the ending as a value: resolves `Completed` or
  `Failed(type, message)`, cancels for a cancelled run, and — cluster only —
  faults with `PipelineRunLostException` when no ending will ever come. This
  is the surface for coordinators, logs, and metrics reacting to endings.
- **`Snapshot()` / `SnapshotAsync()`** — one reading of status plus five
  counters: dropped elements, supervised failures, poison elements,
  checkpoints written, total checkpoint hold. Not a consistent cut; each
  number exact. Over the wire, the counters describe **the answering
  attempt** — a durable ending re-read after its activation died reports the
  outcome with zeroed counters, because the register records outcomes, not
  diagnostics. History belongs to the meter.

## OpenTelemetry

Opt in with two lines — the names are the contract:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics.AddMeter("Orleans.Dataflow"))
    .WithTracing(tracing => tracing.AddSource("Orleans.Dataflow"));
```

A silo hosting runs emits engine metrics and run spans automatically; a
client emits the materialize span.

| Instrument | Kind | What it counts |
|---|---|---|
| `orleans.dataflow.runs.started` | counter | Run attempts, fresh and resumed (`dataflow.run.resumed` tag). |
| `orleans.dataflow.runs.ended` | counter | Terminal states, with `dataflow.run.outcome` = `completed` \| `failed` \| `canceled`. |
| `orleans.dataflow.elements.dropped` | observable counter | Elements discarded by declared overflow policies at engine-owned boundaries. An adapter's private ingress (stream, broadcast, observer, reminder sources) counts its drops internally and is not folded in yet — a recorded deferral. |
| `orleans.dataflow.failures.supervised` | observable counter | Failures supervision scopes intercepted, one per failed attempt. |
| `orleans.dataflow.elements.poison` | observable counter | Elements that exhausted every declared retry. |
| `orleans.dataflow.checkpoints.written` | observable counter | Checkpoint documents the store accepted. |
| `orleans.dataflow.checkpoint.hold.duration` | histogram (s) | How long each capture held its run, successful or not. |

Every instrument carries `dataflow.graph` — the document fingerprint, bounded
by how many graph shapes the deployment runs. **Run identities are never
metric tags** (unbounded cardinality); they appear on the `dataflow.run` and
`dataflow.materialize` activities. The observable counters read the same
state the snapshot reads — no element-path instrumentation exists — and keep
counting a graph's totals after its runs settle, so rates read across run
boundaries. Telemetry never fails a run: a throwing listener is a broken
observer, and every emission swallows.

Alerting starting points: a rising `failures.supervised` rate is a run
limping inside its declared policies; `elements.poison` moving at all means
retries are being exhausted; `runs.ended{outcome="failed"}` is the page;
`checkpoint.hold.duration` growing is capture cost eating throughput —
lengthen the interval or shrink the state before overlapped capture exists
(it does not, by design, in v1).

## Placement

Durable-run placement follows the silo's dataflow placement configuration
(`DataflowPlacement`, e.g. `PreferLocal`), resolved through Orleans' own
`IPlacementStrategyResolver`. A resumed activation lands wherever Orleans
places it — the checkpoint travels through the store, not the silo, so
placement is a performance choice and never a correctness one.

## What no runbook can give you

- **Exactly-once.** Between commit marks the promise is at-least-once, and
  every stronger claim is a specific adapter's, stated on its row with its
  window.
- **A counter that survives its attempt** in the cluster's register — use the
  meter for history.
- **A run surviving the loss of its store.** The store is the durability; its
  availability and retention are the deployment's.
- **An ending for a lost ordinary run.** `PipelineRunLostException` is the
  honest report; ordinary runs are as durable as their activation, by
  definition. Name a run durable if that is not acceptable.
