# Runbooks

Four procedures. Each one says when you need it, the exact calls, what it
destroys, what it leaves alone, and how to tell it worked.

| If you are seeing | Go to |
|---|---|
| `PipelineResumeRefusedException`, a durable run refusing a new revision, or a finished run you need to run again | [Replacing a durable run](#replacing-a-durable-run) |
| A refusal naming a cap of 1,000 durable run identities, or names you need to give back | [Retiring a run identity](#retiring-a-run-identity) |
| `PipelineRejectedException` naming a stage a silo "does not register", or runs failing on a coin flip | [Rolling an upgrade across silos that disagree](#rolling-an-upgrade-across-silos-that-disagree) |
| `CheckpointWriteFailedException`, a stalled run, a rising checkpoint-hold histogram, or a blob store that was briefly unavailable | [Recovering from a checkpoint store outage](#recovering-from-a-checkpoint-store-outage) |

---

## Replacing a durable run

### When you need it

Three situations, and all three are the same operation:

- A [run identity](../reference/glossary.md#run-identity) holds one document and
  you want it to hold a different one. `MaterializeDurableAsync` refuses that with
  `PipelineResumeRefusedException`, naming both
  [fingerprints](../reference/glossary.md#fingerprint), because
  [checkpoints](../reference/glossary.md#checkpoint) do not migrate across
  documents.
- A durable run **finished**, and you want to run it again. A finished run stays
  finished; no poll revives it.
- A run is wedged in a state you have decided to abandon, and you accept losing
  its position.

**Do not use it to recover from a checkpoint store outage.** Replacing clears the
checkpoint, which is the one thing an outage did not damage. Use
[the store outage runbook](#recovering-from-a-checkpoint-store-outage) instead.

### The call

```csharp
OrleansDataflowHost host = services.GetRequiredService<OrleansDataflowHost>();

await using OrleansRunHandle replacement = await host.ReplaceDurableRunAsync(
    pipeline,
    new DurablePipelineOptions { RunId = "orders-of-the-day", EveryElements = 100 },
    cancellationToken);
```

**Use the host method, not the coordinator grain.** This is two hops: the
coordinator rewrites the register and mints a fresh
[epoch](../reference/glossary.md#epoch), and then the run grain is asked to
start. A caller that only talks to the coordinator and never starts the
replacement leaves the old attempt running until its next capture is refused —
or forever, if it declared no timing at all.

The document does not have to differ. Replacing an identity with the very
document it already held is how a finished durable run is run again.

### What it destroys

- **The stored checkpoint.** Cleared. The run starts from the beginning.
- **The previous attempt's claim.** Superseded by the fresh epoch.
- **Whatever the register held under that name**, replaced by the new
  declaration.

### What it leaves alone

- **Every other run of the pipeline**, and every other durable name.
- **Anything the old run already wrote to the outside world.** Replacing rewinds
  the pipeline's position, not its effects — a sink that is not idempotent will
  do its work again from the beginning.
- **The old attempt's execution, for a moment.** The coordinator only *fences*
  it, because the member that rewrites the register may not await a run grain.
  Orleans permits one activation per run grain, so the activation this call then
  asks to start is the very one hosting the old attempt, and it disposes that
  engine before starting the replacement. What is left over is the window between
  the two hops, in which the old attempt executes under a claim that is already
  stale; a capture taken in it is refused by a store it no longer holds an ETag
  for.

### How to tell it worked

- The call returns a handle whose `Epoch` is **higher** than the one you had.
- The store holds nothing for `(graph, run)` until the replacement's first
  capture.
- The sink sees elements from the start of the stream again.

### If it throws

`CheckpointConflictException` from a replace means something was still writing
under the identity between the read and the clear. **Retrying the replacement is
safe and is the answer.**

---

## Retiring a run identity

### When you need it

- A coordinator refuses a new durable name because the register is full — 1,000
  identities per pipeline. The refusal names both the cap and this remedy.
- You name durable runs after something outside your control — a tenant, a day,
  an import — and the finished ones need giving back.
- A name is done with and you want it free.

The cap exists because a record holds the document it names and the whole
register is rewritten on every declaration, so an unbounded register eventually
exceeds the storage provider's per-document limit — after which the coordinator
cannot write at all and **every** start of that pipeline stops with it. Retiring
is what makes room.

### The call

```csharp
bool retired = await host.RetireDurableRunAsync("orders-pipeline", "orders-2026-08-01", cancellationToken);
```

It takes **names rather than a pipeline**, because an operator carrying out a
runbook has the two identifiers and no reason to be able to rebuild the document.

### What it destroys

- **The stored checkpoint.** Cleared.
- **The record naming the run.** Removed, so the identity is free again.
- **The document that record carried.** Gone with it.

Retiring is destructive and unlogged. What the name held is not recoverable from
the cluster.

### What it leaves alone

- **What is running.** Like a replacement, it takes effect on the executing
  attempt only when that attempt's next capture is refused. Retire a live run and
  it goes on until then — and a run that declared no timing at all never
  captures, so it runs until something else ends it. That is why both operations
  are an operator's decision rather than the cluster's.
- **Every other name in the register**, and every run of them.

### How to tell it worked

- The call returns `true` when a declaration was retired.
- It returns `false` when the coordinator held nothing under that name — which is
  also what a retirement that already happened returns. **Asking twice is safe**,
  which is what a runbook step needs.
- Declaring the name again afterwards is a *first* declaration rather than a
  resume: there is no record and no checkpoint, so the run starts from the
  beginning.

Proven end to end by
`CoordinatorLimitsTests.ARetirementClearsTheCheckpointRemovesTheRecordAndIsSafeToRepeat`.

### Making room in a full register

One retirement makes room for exactly one name. A register that is full still
works for the names already in it — those runs can be addressed, resumed, and
finished — so the failure is confined to *new* names, and the fix is a loop over
the identities you have finished with.

---

## Rolling an upgrade across silos that disagree

### When you need it

Any deployment of a new stage vocabulary. During the roll, silos disagree about
their [catalogs](../reference/glossary.md#catalog), and that window has three
distinct behaviours you should expect rather than diagnose.

### What happens during the window

**1. A silo that does not know a stage refuses the document, and names it.**
The refusal says which node, which stage reference, and that the problem is the
catalog rather than the document — enough to know which package version a silo is
missing. It contains the phrases `does not validate`, `stage catalog`, the stage
reference itself, and `does not register`.

**2. Acceptance by the coordinator is not a promise about where the run
executes.** This is the uncomfortable one, and it is the shape of a real outage:
half a deployment upgraded, a document that validates at the coordinator, and a
run that fails on a coin flip because its run grain landed on the other silo —
which validates the document again, against its own host's catalog. The refusal
reads identically, because it is the same check on a different host.
Proven by
`RollingUpgradeTests.AcceptanceByTheCoordinatorDoesNotPromiseTheRunGrainsSiloAcceptsItToo`.

**3. Two silos that both accept one document still report different catalog
fingerprints for it.** That is the only signal a client gets that its runs are
not all being validated against the same vocabulary.

For a **durable** run, the register keeps a catalog fingerprint beside each
declaration, and a resume re-runs the catalog discipline on whichever silo
catches the activation:

- A resume landing on a silo that resolves the document's stages **continues**.
- A resume landing on a silo that cannot is **refused by name**, not guessed at.
  The run continues when a capable silo picks it up, or when the roll finishes.
  **Nothing is lost** — the checkpoint is untouched by a refusal.

### The procedure

1. **Deploy the new catalog to every silo *before* authoring anything that uses
   it.** A vocabulary is additive: new stage references, or a new major version
   beside the old one. A silo that has both can run old and new documents.
2. **Roll the silos.** Ordinary runs of documents naming only old stages are
   unaffected throughout. Durable runs of those documents resume anywhere.
3. **Wait for every silo to report the new catalog** before starting a pipeline
   that names a new stage. Until then, expect behaviour 2 above.
4. **Then author the new revision**, and let it materialise beside the old runs.
   New documents do not disturb old ones.
5. **Move a durable name onto the new revision** with
   [a replacement](#replacing-a-durable-run) — that is the only way a name moves
   forward, and it clears the position on purpose. Same fingerprint and same
   revision, or refuse, is the whole resume rule; there is no cross-revision
   checkpoint migration.

### What it destroys

Nothing, until step 5. A rolling upgrade is non-destructive by construction: the
worst outcome in the window is a refusal, and a refusal touches no checkpoint.

### What it leaves alone

Every run of every document whose stages both catalogs resolve. Two revisions of
one pipeline run side by side under two names quite happily.

### How to tell it worked

- No `PipelineRejectedException` naming `does not register` for a full sampling
  period.
- Every silo reports the same catalog fingerprint for a document you start on
  each of them.
- Durable runs that were mid-flight report `resumed` on
  `orleans.dataflow.runs.started` and continue rather than restarting.

### Removing a stage

The mirror image, and it needs the same care in the opposite order: stop
authoring documents that name it, wait for every durable run that names it to
finish or be retired, **then** remove it from the catalogs. Removing it while a
durable run's document still names it turns every resume into a refusal until
someone replaces the run.

---

## Recovering from a checkpoint store outage

### When you need it

- A run's completion faulted with `CheckpointWriteFailedException`.
- `orleans.dataflow.checkpoint.hold.duration` is climbing.
- Your document store was briefly unavailable, throttled, or its credentials
  expired.

### The one fact to hold first

**A store that refuses is not retried; a store that does not answer is.** They
are different facts and only the store can tell them apart.

- `CheckpointConflictException` says *somebody else owns this run now*, so the
  stale writer fails immediately. Retrying it would overwrite the position a
  fresh attempt is building.
- Anything else says *nothing at all* about ownership, so the same document is
  presented again five times over roughly four seconds — 0.1 s, 0.3 s, 0.9 s,
  2.7 s — inside the capture's hold. The run is stalled for the whole of it.

A store that misses one write is absorbed entirely and the run never notices.

### What a longer outage does

The attempt ends and **the run is not retired**:

- Its completion faults with `CheckpointWriteFailedException`, carrying your
  store's own exception as the cause.
- **Nothing is written down as the run's outcome**, so the declaration stays
  open with an attempt that stranded — which is a different fact from a run that
  finished.
- **The position the store did accept is still there**, untouched by the failed
  writes.

### The procedure

1. **Fix the store.** Nothing below helps until writes are landing again.
   Confirm with a write of your own, not with the pipeline.

2. **Confirm the declaration is still open.** It should have no outcome:

   ```csharp
   DurableRunClaim? claim = await grains
       .GetGrain<IPipelineCoordinatorGrain>("orders-pipeline")
       .ClaimDurableRunAsync("orders-of-the-day");

   // claim is not null — the declaration exists — and claim.Outcome is null,
   // which is the register saying it does not know how this run ended.
   ```

   Reading a claim **fences nobody**, so this is safe to do against a live run.

3. **Re-declare the run and start it again.** The ordinary call, not the
   destructive one:

   ```csharp
   await using OrleansRunHandle again = await host.MaterializeDurableAsync(
       pipeline,
       new DurablePipelineOptions { RunId = "orders-of-the-day", EveryElements = 5 },
       cancellationToken);
   ```

   This resumes from the last checkpoint the store accepted, taking a fresh
   epoch exactly as a resume after a silo death does.

4. **Do not reach for `ReplaceDurableRunAsync`.** Replacing clears the
   checkpoint — the one thing the outage did not damage — and a long pipeline
   would pay for a store hiccup with all of its progress, through an operator
   action that reads like recovery.

### What it destroys

Nothing. Every step above is non-destructive, which is the point of the runbook
existing separately from the replacement one.

### What it leaves alone

- The stored position, which is what you are recovering *to*.
- The declaration, which was never closed.
- Everything already delivered — except the
  [replay window](../reference/glossary.md#replay-window), which is delivered
  again. That is at-least-once, and it is the same window a crash would produce.

### How to tell it worked

- The new handle's `Epoch` is higher than the stranded attempt's.
- **The sequence proves resumption rather than restart.** The first element is
  delivered exactly once — a run starting from the beginning could not manage
  that — and the elements in the replay window are delivered twice, which is
  precisely the gap between the stored cursor and the attempt that stranded.
- `orleans.dataflow.runs.started` carries `dataflow.run.resumed=True`.
- The checkpoint-hold histogram returns to its usual shape.

All of this is measured by
[`DurableStoreOutageTests`](../../tests/Orleans.Dataflow.OrleansTests/Cluster/DurableStoreOutageTests.cs).

### If a run keeps failing after the store is healthy

Check the failure **type**, not the message:

- `CheckpointWriteFailedException` — the store still is not answering. Go back to
  step 1.
- `CheckpointConflictException` — somebody else owns the name. Another attempt is
  live, or a replacement or retirement happened. Read the claim to find out
  which; do not fight it.

Until something re-declares a stranded run, it keeps answering with its failure
rather than healing on its own. That is deliberate: the caller has to be able to
see what the store did.

---

## What no runbook can give you

- **Exactly-once.** Between commit marks the promise is at-least-once, and every
  stronger claim is a specific adapter's, stated on its row with its window.
- **A counter that survives its attempt** in the cluster's register — use the
  meter for history. See [Monitoring](monitoring.md).
- **A run surviving the loss of its store.** The store *is* the durability; its
  availability and retention are the deployment's.
- **An ending for a lost ordinary run.** `PipelineRunLostException` is the honest
  report; ordinary runs are as durable as their activation, by definition. Name a
  run durable if that is not acceptable.

## Next

- [Checkpoint stores](checkpoint-stores.md) — the contract, and the three answers a store can give.
- [Monitoring](monitoring.md) — the instruments these procedures read.
- [Deploying](deploying.md) — the limits and the trust boundary.
