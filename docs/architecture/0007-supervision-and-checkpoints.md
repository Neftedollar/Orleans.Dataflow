# ADR 0007: Supervision scopes and the checkpoint model

- Status: Accepted for M5
- Date: 2026-08-18
- Depends on: [ADR 0001](0001-definition-runtime-authoring-planes.md) (no
  delegate enters a document — the rule every policy here obeys),
  [ADR 0002](0002-result-slots.md) (slots carry the run's outcome — the rule
  that shaped WatchTermination's deferral and shapes its return),
  [ADR 0005](0005-junction-semantics.md) (failure wins — the rule
  supervision deliberately weakens, scope by declared scope)
- Informed by: the M4.4 group-by, whose payload carries an inner chain —
  the precedent everything below composes from

## Context

Through M4 the engine has exactly one answer to a failure: the run ends
with it. That is the right default and it stays the default. M5 adds the
declared exceptions — a section that drops a poison element, retries a
flaky call, or restarts with its state reset — and the durability that
lets a run outlive its host: checkpoints, cursors, commits, and resume.
Both must live in the document, because a policy the definition plane
cannot see is a policy a cluster cannot honor, and Akka's decider — a
delegate examining the exception — is exactly what ADR 0001 forbids a
document to carry.

The exit criteria this ADR is written against are the matrix's own:
every restart form states what resets and what survives; crash tests at
each boundary prove the stated duplicate/loss window; and no global
exactly-once claim exists anywhere.

## Decision: supervision is a scope, and a scope is a stage

A supervision policy attaches to a **scope**: a declared wrapper stage
whose payload carries the policy and the inner chain, exactly as a
group-by carries its group flow. No `StageNode` schema change, no
per-node annotation, no Abstractions change: a scope is one more stage
whose payload the compiler already validates entry by entry, whose
fingerprint changes when its policy does, and whose inner chain is
restricted the way the group flow is — fusible element stages in v1,
with the restriction stated as v1's honesty and widened only with
evidence.

Three scope forms, from the matrix's rows:

- **`Resume`** — the failing element is dropped; the scope's stage state
  is retained. The failure is counted and observable (the monitor's
  business), never silent in the aggregate.
- **`RestartStage`** — the failing element is dropped; every stage state
  inside the scope resets to its seed. What "reset" means is exact
  because the inner chain is declared: a scan returns to its seed, a
  distinct forgets its keys, a batch abandons its open group.
- **`Retry`** — the element is re-offered to the scope up to a declared
  attempt count with declared backoff (a `TimeSpan` ladder in the
  payload, jittered by the runtime), and a declared answer for
  exhaustion: fail the run (default) or escalate to `Resume`/
  `RestartStage`. An element that exhausts retries and resumes is a
  **poison element**, counted as such.

- **`Recover`** — the failure ends the scope's stream *successfully*,
  after emitting a declared fallback. The fallback is a constant carried
  in the payload as a canonical value (deployable by construction) or,
  local-only, a delegate producing one — the same split every stage
  behavior already lives with. Recovering with an **alternate source**
  is not a fourth knob on this scope but the restart-section form
  pointed at a declared alternate, because switching sources is a
  section decision, and the matrix's own row demands the two boundaries
  — completion-after-fallback and source-switch — stay distinct.

The policy names no exception type in v1. A policy that filtered by
type would need type names in the document — the CLR-name rule refuses
that — or a declared taxonomy, which is real design work owed its own
evidence. V1 supervises every failure inside the scope alike; the
taxonomy is a recorded deferral, not an oversight.

**What a scope does not catch**: cancellation (the run's own stop is
not a failure), a failure outside any scope (the default stands), and a
failure of the machinery itself rather than of an author's stage. The
last distinction is the engine's to draw and to document per seam.

**Restart-section with backoff** — the row that restarts a source, flow,
or sink subgraph — is the same scope machinery at a coarser grain plus a
restart **budget** (attempts within a window) in the payload. Its v1
boundary: a section containing a *source* restarts by re-opening the
source, which meets the cursor model below or loses position, and which
of the two it is appears in the adapter's table rather than in a
general promise.

## Decision: the checkpoint model

A **checkpoint** is a document-addressed value:

    (graph fingerprint, revision, per-source cursors,
     per-scope declared-durable state, per-sink commit marks)

- **Cursors** belong to sources whose adapters declare them (an Orleans
  stream sequence token where the provider is rewindable; an explicit
  position for a registered source that owns one). A source with no
  cursor contributes nothing and resumes from now — stated per adapter,
  never generalized.
- **Durable state** belongs only to stages inside a scope that declares
  the `durable-state` capability token (which has existed since M0 and
  finally earns its keep). Everything else **resets on resume**, and the
  reset is the documented contract, not a caveat.
- **Commit marks** are the sink half: an adapter that can say "elements
  through position P are committed" says it here, and the duplicate
  window of a resume is exactly [last mark, crash] — measured by the
  crash tests, stated in the adapter's table.

The **storage contract** is the coordinator store's shape generalized:
an ETag-guarded read/write keyed by `(GraphId, RunId, "checkpoint")`,
with the same fencing consequence — a superseded writer's checkpoint
write fails, and the failure kills the stale attempt rather than
corrupting the fresh one. The in-memory implementation lives beside the
test store; a durable one is the deployment's, exactly as the
coordinator's is.

**Checkpoint timing** is declared, not implicit: a periodic interval
and/or an every-N-elements bound in the run's durable options. A
checkpoint is taken at a quiescent point the engine already knows how
to reach — the pause machinery's safe points, reused rather than
reinvented: hold, snapshot, resume. The cost of that choice (a
checkpoint pauses the run for its duration) is stated and measured
before any cleverer overlap is attempted.

## Decision: resume

A run started under a declared **durable option** writes checkpoints
and may resume. Resume is the same `RunId` continuing: the M3 rule "a
deactivation mid-run faults the attempt" becomes, under the option,
"the next activation reads the checkpoint and continues it". The
coordinator's epoch still fences: a resumed attempt claims a fresh
epoch, and the stale one's late writes still lose. What resume promises
is **at-least-once between commit marks** — elements after the last
mark are replayed from cursors, sinks see the duplicate window their
table states, and nothing anywhere says exactly-once.

`WatchTermination` returns here in its honest shape: a **control** (per
ADR 0004's control slots), resolving at run start to a task that
completes with how the run ended — completed, failed with what, or
resumed-and-continuing elsewhere — because a control can carry an
outcome without becoming it, which is the tension that deferred it in
M4.

**Amendment (M5.5): the control wording dissolves — the watch is a
member of the handle, and there are two endings rather than three.**
Building it showed the "control" framing answered the right question
with the wrong noun. What ADR 0002's tension required was only this: a
thing that exists while the run is running and *resolves* with a failed
run's outcome instead of faulting with it — which a result slot can
never be. A control slot delivers that, but a control is a document-
declared name for reaching *into* a graph (an ingress queue), resolved
per run and bound by fingerprint; the watch names nothing in any
document, varies with no graph shape, and is a fact about the run
itself — so it ships as `RunHandle.WatchTermination` /
`OrleansRunHandle.WatchTermination`, a `Task<RunEnding>` beside
`Completion`, with the slot machinery left out of it. The paragraph
above stands as the record of why the shape is what it is; only the
noun moved. Two further corrections from building it: **the endings
are two, not three** — "resumed-and-continuing elsewhere" is not an
ending, by M5.4's own rule that a checkpoint says where and only the
register says whether, so a durable run's watch simply keeps waiting
across resumes and reports the one ending the register eventually
records; and **cancellation is not an ending either** — the watch of a
cancelled run cancels rather than resolving, because a watch that
reported it as a third ending would make "this run is over" true of a
run a durable deployment is about to continue. On the cluster handle
the watch additionally *faults* with the lost-run report when no
ending will ever come, which is the one outcome the local handle
cannot have.

## Decision: the failure-injection seam

The crash tests the exit criteria demand cannot be written against luck.
The Testing package gains declared **fault points** — named seams
(before an offer, after a commit mark, inside a checkpoint write,
between retry attempts) that a test arms to throw or to kill the host at
a deterministic moment. The seam is test-support surface, compiled into
the Testing package's providers and hosts, never into a shipping stage;
its shape follows the probes: an ordinary registered occurrence a
document names, so an injected fault is part of the graph under test
rather than a hook reaching into the engine.

## Consequences

- M5's phases fall out: the injection seam and local supervision first
  (the tests need the seam before anything can be proven), then the
  checkpoint model and storage contract, then Orleans resume with the
  crash suite, then revision compatibility and rolling upgrade, then
  telemetry and the monitor snapshots WatchTermination joins.
- The scope-as-stage decision means supervision composes with
  everything that already composes: a scope inside a group flow, a
  scope on a junction leg, a retry around a grain call. Each
  composition is evidence work, not new design.
- Revision compatibility gets its rules where the checkpoint meets a
  new document: a resume against a different fingerprint is refused in
  v1 — same-revision resume only — and cross-revision migration is
  M5's last phase or a recorded deferral, decided by what the
  compatibility rules turn out to cost.

  **Amendment (M5.4): the compatibility rules cost one method, and
  migration is the recorded deferral.** The clause above left the choice
  open on purpose and this is it being made, with the price written down
  rather than described. What the rules turned out to need is three
  statements and no new machinery:

  1. **A new revision under a new run identity runs beside the old one.**
     Nothing had to be built for this: a checkpoint is keyed by
     `(graph, run)`, so two revisions under two names already have two
     positions and two endings. It is proved side by side rather than
     assumed.
  2. **A new revision under an existing run identity is refused by name,
     and the way to mean it anyway is a spelling that says it destroys.**
     `ReplaceDurableRunAsync` clears the stored checkpoint and supersedes
     the previous attempt with a fresh epoch. It is a second method rather
     than a flag on the first because what it does is not a variation of
     declaring; and it does not require the document to differ, because
     "run this name from the beginning again" is the same destruction seen
     from the other side.
  3. **Cross-revision checkpoint migration is deferred**, and the reason is
     what (1) and (2) cost: together they are one method and one register
     member, while a migration would need a declared correspondence between
     two documents' seams — which nodes are the same node across an edit,
     which stage state survives a changed chain, what a cursor of a source
     that was replaced means — and that is a design owed its own evidence,
     not a widening of a comparison. It is deferred until a deployment
     demands it rather than judged impossible.

  The refusal that carries all three is unchanged from M5.2: same
  fingerprint and same revision, or nothing. What M5.4 adds beside it is
  that the refusal is no longer a dead end.

- **Amendment (M5.4): a checkpoint says where, and the register says
  whether.** ADR 0007 defines a checkpoint as a position and says nothing
  about a run being over, and M5.3 discovered the consequence by measuring
  it: a durable run that completed and then lost its activation was
  indistinguishable from one that died at the same position, so the next
  activation continued it and re-ran its tail. The fix is deliberately not
  a sixth member of the checkpoint document — a stored position is a fact
  about a stream and "this run is finished" is a claim about ownership,
  which is the coordinator's register by the same reasoning that put the
  epoch there. So the run grain reports its terminal state (completed or
  failed; **never cancelled**, because a deactivation cancels the run it
  was hosting) to its coordinator, the declaration records it, and a later
  claim answers with the ending instead of a document to continue. **The
  checkpoint of a finished run is kept, not cleared**: where a run got to
  is the question asked after it ends, and forgetting it is an explicit
  operation — `ICheckpointStore.ClearAsync`, or a replacement — rather than
  something a runtime does on a deployment's behalf.

- **Amendment (M5.4): resume re-runs the catalog discipline, and a
  refusal there is about resolution rather than about vocabularies being
  equal.** A resume chooses its host by which silo survived, so a
  half-upgraded cluster can accept a durable run on one silo and be unable
  to execute it on the next. The resumed materialization therefore
  validates against the host's own catalog exactly as a start does, and
  refuses by name with the stage it cannot resolve — leaving the
  declaration and the checkpoint where they are, so a later activation on a
  silo that publishes the vocabulary continues the run. Two silos with
  different **catalog** fingerprints resume one another's runs fine
  whenever every stage still resolves: the only fingerprints a resume
  compares are the checkpoint's and the document's.
- Every duplicate/loss window this ADR names becomes a measured number
  in an adapter's table before its row advances, which is the exit
  criterion restated as the definition of done.
