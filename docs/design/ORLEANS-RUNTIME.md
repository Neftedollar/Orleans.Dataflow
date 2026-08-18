# Orleans runtime design

- Status: M3 architecture; phases 1-3 and 4a are implemented, and 4b's
  delivery-registry half — the broadcast source — is implemented and documented
  below; 4b's failover half is tracked separately. M4.5 added the result-size
  cap and moved the provider seam into the core package, both recorded below.
  M5.3 added durable runs — a checkpoint store behind a silo, activation-driven
  resume, author-named run identities, the stream sequence-token cursor, and the
  crash suite — recorded in its own section below
- Depends on: [ORLEANS-NOTES.md](ORLEANS-NOTES.md) (verified Orleans 10
  facts), [REGISTERED-STAGES.md](REGISTERED-STAGES.md),
  [LOCAL-RUNTIME.md](LOCAL-RUNTIME.md), ADRs 0001-0004

## The central decision: runs distribute before stages do

An M3 run executes as one logical unit — the proven local engine hosted
inside a grain — and Orleans-native stages are adapters inside that run.
Distribution of *stages* across grains arrives only where a stage's
semantics demand it (keyed/partitioned work), not as a default topology.

Why: the local engine's semantics (terminal discipline, boundaries,
drain-versus-abandon, control slots) are tested to death; hosting it whole
preserves every guarantee and makes the Orleans layer additive. A
stage-per-grain default would re-litigate every boundary semantic across a
network hop and turn every fused segment into a distributed system. The
cost, stated honestly: a phase-1 run is bounded by one silo's capacity;
scale-out is per-run (many runs) and per-keyed-stage (phase 4), not
per-arbitrary-stage.

## Grain topology

- **`IPipelineCoordinatorGrain`** — key: `GraphId`. Owns the run registry
  for its pipeline: starts runs, assigns `RunId`, tracks the active epoch.
  State: `IPersistentState<CoordinatorState>` (active runs, per-run epoch).
  Fencing is Orleans-native: a stale coordinator activation writing state
  hits the ETag conflict, `InconsistentStateException` kills it, and the
  fresh activation re-reads truth. Every run-grain call carries
  (RunId, Epoch); a run grain rejects a stale epoch loudly.
- **`IPipelineRunGrain`** — key: composed (GraphId, RunId). Hosts one run:
  validates the document against the silo's catalog (fingerprint check),
  materializes through the runtime-factory seam, drives the local engine,
  reports terminal state to the coordinator. Deactivation while running is
  a failure of that attempt — **unless the run was declared durable and has
  written a checkpoint, in which case the next activation continues it**
  (M5.3, below; phase 1 promised nothing here and this is the promise it
  waited for). `[Reentrant]` is NOT
  used; control calls (`ShutdownAsync`, status) interleave via `[ReadOnly]`
  or one-way signal patterns decided at implementation with the
  non-reentrancy default preserved for the execution path.
- **Client surface** — `OrleansDataflowHost` (or extension on
  `IClusterClient`): `MaterializeAsync(PipelineDefinition, ...)` returning a
  `RunHandle`-shaped remote handle (completion observation via polling or
  observer — decided in phase 1 with a bias to polling first, observers
  being best-effort by design).

## The runtime-factory seam (phase 1 prerequisite)

`IStageRuntimeFactory`, registered in DI per `ProviderId`:
factories turn a resolved `StageNode` (+ its validated payload) into the
executor shapes the local engine already runs (source openers, element
stages, async stages, terminals). The local lambda vocabulary gets the
binding-table-backed factory (local-only); Orleans stages are the first
REAL registered providers — which is why the seam lands here and the M4
provider SDK formalizes what M3 proves.

**Formalized in M4.5**: the public mirror this package shipped for silos —
`IDataflowStageFactory`, `DataflowStageRequest`, `DataflowStageRuntime`,
`DataflowRunTokens` — moved into the core package under the same names in the
same namespace, so one seam now serves a silo and a `LocalDataflowHost` alike
and a provider that never references Orleans can still write a factory. The
shapes grew by two, both of them engine primitives that already existed: a
fan-out and a fan-in, so a provider can register a junction. See
[REGISTERED-STAGES.md](REGISTERED-STAGES.md).

Elements crossing grain or stream boundaries are the author's types and
must satisfy Orleans serialization (`[GenerateSerializer]`/`[Id]` or
registered serializers) — a documented requirement checked at first use,
per the research notes' failure-mode split. Graph documents always travel
as canonical bytes, never as Orleans-serialized object graphs.

## Orleans-native stages (adapters inside the run)

| Stage | Provider/StageId (direction) | Acknowledgement boundary | Notes |
|---|---|---|---|
| Orleans Stream source | `orleans/stream-source` | delivery into the run's ingress (bounded; overflow per options) | implicit-subscription grain feeds the run's queue; provider guarantees reported per provider (`IsRewindable` probed, not assumed) |
| Orleans Stream sink | `orleans/stream-sink` | `OnNextAsync` awaited per element (publication, not end-to-end) | |
| Awaited grain call (flow/sink) | `orleans/grain-call` | the awaited reply | timeout/retry/idempotency are explicit options; one-way is a separate best-effort stage, never the default |
| Keyed grain call | `orleans/grain-call-keyed` | per-key ordered awaited replies | one call in flight per key (that is where the ordering comes from — measured, see phase 4a), the declared bound across keys; per-key executor grains are opt-in per occurrence |
| Grain `IAsyncEnumerable<T>` source | `orleans/grain-enumerable` | call-scoped pull (Orleans batching) | cooperative cancellation; `CancelRequestOnTimeout=false` gotcha handled explicitly |
| Timer trigger source | `orleans/timer` | none (tick generation) | activation-scoped, non-durable — documented |
| Reminder trigger source | `orleans/reminder` | none | definition survives restart, missed ticks not replayed — matrix contract verbatim |
| Observer bridge | `orleans/observer` | best-effort, bounded ingress mandatory | no history; every push answers Accepted/Dropped/Closed/Failed |
| Broadcast Channel sink | `orleans/broadcast-sink` | one awaited `Publish` per element (publication, not end-to-end) | delivery mode declared and checked against the provider |
| Broadcast Channel source | `orleans/broadcast-source` | delivery into the run's bounded ingress (dropping/failing policy only) | implicit-only subscription means one package-owned namespace; a relay grain per channel key holds the delivery registry; no history |
| `IObservable<T>` / .NET events | `dotnet/observable`, `dotnet/event` | bounded ingress buffer mandatory (push source cannot be pulled) | overflow policy required, mirrors the ingress queue |

Backpressure across every boundary is the adapter's await or its bounded
ingress: a grain call in flight is credit spent; a stream delivery into a
full bounded ingress follows its declared overflow policy; nothing anywhere
uses a mailbox as an unbounded buffer.

## Phase 1 — as implemented (rules the later phases inherit)

- **Wire discipline**: no definition-plane identity type crosses a grain
  boundary — RunId/GraphId/fingerprints travel as strings, slots as name
  plus fingerprint text; a reflection test fails any wire member typed from
  the Abstractions assembly, so drift is caught at build, not at first
  send. Grain-thrown refusals carry no inner exception (Orleans serializes
  the whole chain, and an unserializable inner replaces the diagnosis with
  a codec error); the cause folds into the message.
- **Two exception worlds**: `PipelineFencingException` strictly for a stale
  epoch against a live run; `PipelineRunLostException` for an attempt that
  no longer exists — absence is not staleness.
- **Registered sources receive both tokens** (run and stop), so drain and
  abandon stay distinguishable across the seam; the public factory mirror is
  public while the engine seam stays internal. (The mirror shipped here in
  phase 1 and moved to the core package in M4.5, unchanged in name and
  namespace, so that one seam serves both hosts.)
- **Grain turns never park**: status and result calls answer "not yet"
  rather than await; shutdown and cancel request rather than drain; the
  engine's dedicated threads do the waiting.
- **Phase-1 limits, stated**: results live only as long as the run grain's
  activation (proven absent, not promised); a deactivation mid-run faults
  that attempt (**lifted in M5.3 under a declared durable option, and only
  there**); a remote failure arrives as type name plus message — the
  author's exception type does not survive the hop; the coordinator
  persists `LastEpoch` and nothing else (a `Runs` register written "for
  phase-4 reconciliation" was removed after phase 4 shipped without
  reading it: it grew per accepted start with nothing pruning it, and M5's
  durable resume will persist what reconciliation actually reads — **M5.3
  did, as one record per declared durable run**); ETag
  fencing of competing coordinator activations is designed but
  demonstrated only across deliberate deactivation until phase 4's kill
  tests.

## Phase 2 — as implemented

- **The subscription consumer is the silo's hosted client, never the run
  grain** — forced by the never-park-a-grain-turn rule: a grain-consumed
  delivery under the backpressure policy would park a turn or lie. Probed:
  the stream and grain-call APIs work off any grain context inside a silo.
  The source's enumerator owns the subscription, so the engine's guaranteed
  disposal on every terminal path IS the teardown, and the explicit-
  subscription ResumeAsync trap dissolves — there is no grain-owned
  subscription to resume or leak.
- **Backpressure's shared cost, measured**: under the backpressure policy a
  stalled run stalls the provider's pulling agent for the whole queue — an
  unrelated subscriber on the same stream stops receiving. Correct for a
  bounded system, documented on the source with the advice to declare a
  dropping policy where that cost cannot be paid.
- **Stream elements are registered** (`AddStreamElement<T>`): Orleans binds
  one stream identity to one element type per process, so opening streams
  as `object` would break co-hosted grains — probed, not assumed.
- **Declare once, use twice**: binding declarations (stream element, grain
  call, grain-call sink, grain enumerable) serve silo registration and
  authoring; payloads carry the name plus both contract references, so
  validation is contract-to-contract with messages naming both sides.
- **Timeouts are enforced twice** (CancelAfter plus WaitAsync) so they fire
  against uncooperative calls too; a cooperative arrival maps to
  `GrainCallTimeoutException`, distinguished from a real run cancellation
  by the run token.
- **Phase-2 limits, stated**: the terminal seam hands no cancellation
  token, so an in-flight grain-call-sink or stream-sink publication is not
  cancelled — only the next admission is; grain-call emission is ordered
  only; adapter ports declare the opaque `orleans-element@v1` contract, so
  an adapter-to-typed-stage edge needs `OrleansStages.Element<T>()` on the
  other side; ingress remainder at shutdown is abandoned; the registry is
  deployment-scoped behavior — two silos with different bindings accept
  different documents under one catalog fingerprint, stated rather than
  hidden.

## Phase 3 — as implemented

- **The `dotnet` provider lives in the main package** (timer and observable
  bridges need no Orleans and run on both hosts); one binding declaration
  serves `LocalDataflowHost(configure)` and `AddOrleansDataflow`. The
  dotnet ports declare the local opaque contract, so `timer → Take →
  Collect` composes on the local host out of the box; the cross-provider
  seam to `orleans/*` stages needs the explicit element declaration, same
  as every typed seam.
- **Timer has no ingress at all** — the pull is the backpressure, so ticks
  cannot accumulate and none is dropped; the stop token releases the wait
  between ticks.
- **The reminder trigger is its own grain** per occurrence (the seam hands
  a factory no run identity by design; the trigger grain owns the reminder
  and pushes into the run's ingress through a published receiver
  reference). Backpressure is a refused policy for this stage: a clock
  cannot be slowed, and a parking offer would hold the activation that owns
  the cluster's reminder. A tick that reactivates the trigger with no live
  run unregisters the reminder and the attempt stays faulted — no silent
  resume before M5. The configured `MinimumReminderPeriod` floor is
  validated at materialization because Orleans enforces it with a throw.
- **The observer bridge** is a per-run grain addressed from run identity
  (both sides derive the same key; two runs of one graph get distinct
  bridges); every push answers `Accepted/Dropped/Closed/Failed`, making
  best-effort observable. A receiver whose reference is gone would cost the
  full 30 s response timeout per push — measured — so the bridge forgets a
  refusing receiver after one refusal. Source openers now receive a
  per-run `RunIdentity` alongside the two tokens; factories still receive
  none.
- **Broadcast sink** publishes via the provider's channel writer from the
  engine threads (probed to work); `FireAndForgetDelivery` in the payload
  is a checked declaration against the silo's provider options, not a
  per-publication choice. **Broadcast SOURCE stays deferred to phase 4**:
  implicit-only subscription means a run cannot subscribe at all; it needs
  the delivery-registry design that belongs with distribution. (It landed
  there — see the phase-4b section, where the registry turned out to be a
  relay grain per channel key and the deferral's premise turned out to be
  exactly right.)
- Teardown of adapter infrastructure never replaces how a run ended
  (infrastructure release failures are swallowed; an author's disposal
  failure still surfaces) — both bridges self-heal through refusal-driven
  cleanup.

## Phase 4a — as implemented (keyed distribution, credit, placement)

- **The probe answered open question 3, and it answered "no".** Orleans
  documents no pairwise message ordering between activations, and the matrix
  promises per-key ordered awaited replies, so the credit shape could not be
  chosen by taste. A caller pumping 200 sequenced calls at one non-reentrant
  callee without awaiting between them was measured, in three shapes (cold
  callee, warm callee, and a caller that is not a grain — the adapter's own
  shape). **Arrivals were reordered in every round of every shape, inside a
  single in-process silo where every hop is local delivery**: on the deciding
  run the first of 200 arrivals from a grain caller was the 14th call sent, and
  from a client caller the 2nd. In-flight greater than one per key was
  therefore never legal.
- **The credit wire shape, decided by that result: the reply *is* the grant,
  and nothing else is on the wire.** Two bounds hold at once and both are held
  by the run rather than by any message. Within a key: exactly one call in
  flight, so the next element of a key is not sent until the previous one has
  replied — which is where the per-key ordering promise comes from, as a
  property of our accounting rather than of the transport, true on one silo and
  on fifty alike. Across keys: the stage's declared `maxInFlight`, which is the
  engine's own ordered-async-stage bound — a call in flight is credit spent and
  an element cannot enter the stage until a slot frees. No grant message, no
  credit member, no per-key window parameter: a payload member for two in
  flight per key would be a knob for silently losing the ordering the stage
  promises, so the payload has none. `KeyedCallWindow` is the whole protocol,
  and it holds one entry per key *with work in flight* — bounded by the
  declared number, never by the cardinality of the key space.
- **Distribution is opt-in per keyed stage**, declared as `distributed` in the
  occurrence's payload and required rather than defaulted. Off — the default —
  the calls are made from inside the run and the key only orders them; on, each
  key gets an `IKeyedExecutorGrain` and the cluster places it. That preserves
  "runs distribute before stages do": this is the first stage allowed to
  distribute below its run and it does so because a document asked, not because
  it could.
- **Executor identity is `{graph}/{run}/{node}/{key}`** — per run, per
  occurrence, per partition; ephemeral; no cross-run sharing. Where the run
  identity comes from is worth recording, because the seam does not supply one:
  a flow stage is never handed the run's tokens (only source openers are), so
  the factory reads the ambient grain context at materialization, which *is*
  the run grain because materialization happens on its turn. It is captured
  once per occurrence and never read at call time — the engine's threads are
  not inside a grain. A context that is somehow absent falls back to a fresh
  identifier, which keeps executors private to the materialization at the cost
  of an address that no longer names its run.
- **Executors are collected, not torn down, and that is a stated limit.** The
  engine's asynchronous-stage seam has no per-run teardown hook, so a run
  cannot deactivate the executors it used. They hold no state between calls and
  carry a shortened `[CollectionAgeLimit]`, so "dies with the run" is honestly
  "outlives the run by at most that idle period, holding nothing".
- **Failure wins and nothing retries.** A keyed call that throws faults the run
  at the first failure; a lost executor surfaces as the failed grain call it is.
  The two paths report differently and that is the documented cost of the hop:
  run-local, the author's exception reaches the run itself; distributed, the
  executor folds the author's type and message into a
  `KeyedExecutionFailedException` naming the executor's own address, because an
  exception chain is only as serializable as its least prepared link. Retry is
  M5's supervision work.
- **Placement is a hosting decision, through Orleans' own
  `IPlacementStrategyResolver`.** `UsePlacement(runGrains, keyedExecutors)`
  chooses per grain type between the cluster default (defer), random, prefer-local,
  and hash-based; the resolver answers for exactly those two grain types and
  defers for every other, so a deployment that never calls it behaves exactly as
  before. An attribute on the grain classes would have fixed the answer in the
  package; this leaves it with the deployment — which matters because Orleans
  9.2 made `ResourceOptimizedPlacement` the default, so a test meaning to assert
  spread must pin `Random` rather than hope. The grain types are resolved through
  `GrainTypeResolver` rather than spelled as text, so the mapping cannot drift.
- **Phase-4a limits, stated**: single-silo tests, so nothing here proves that
  keyed work actually landed on more than one host, that ordering survives a
  connection re-established mid-run, or anything at all about a silo dying —
  those need the multi-silo fixture that the failover half of phase 4 brings,
  and the placement tests assert which strategy a silo will use rather than
  where activations went. The timeout is the caller's and bounds the whole hop,
  so an executor is never bounded independently of the run waiting on it. Two
  silos with different keyed registrations accept the same document and fail at
  the executor rather than at the start — the same deployment-scoped limit the
  registry has carried since phase 2, now reachable one hop further away.

## Phase 4b — as implemented (the broadcast source and its delivery registry)

- **Seven Orleans facts, all probed, and the design is what they leave.** They live
  in `BroadcastSubscriptionProbeTests` so that a future Orleans re-answers them
  rather than a document asserting them. (1) An implicit channel subscriber is
  activated under a grain key **equal to the channel's key** — not the namespace,
  not a composite — which is the only reason a run can address the very activation
  the runtime feeds. (2) `IOnBroadcastChannelSubscribed.OnSubscribed` fires **once
  per activation per publishing provider**, carrying the `ChannelId` and the
  provider name, and not once per publication; so the handler it attaches is the
  one every later element arrives through, and an attach table can live across
  publications. (3) Two channel keys under one namespace are **two activations**,
  each receiving only its own key's elements — a relay is per channel, never per
  namespace. (4) One key under **two providers is one activation with two
  subscriptions**: a channel's identity is a namespace and a key with no provider
  in it, so provider separation has to be done by us. (5) A subscriber that throws
  **fails a checked publication and is invisible to a fire-and-forget one**, which
  is why nothing thrown ever leaves the relay. (6) A subscriber may
  **`Attach<object>`** and receives the author's own type unchanged, and a grain
  activated *before* the first publication is still subscribed when one arrives —
  which decided the shape more than any of the others, because a relay cannot know
  the CLR type: it comes from the document of whichever run attaches, and that run
  may attach later or never. (7) Two deliveries to one subscriber **never overlap**
  — a slow subscriber was measured entering and leaving each delivery in turn — so
  the attach table can be an ordinary dictionary mutated while forwarding, which is
  a claim about scheduling rather than about an attribute nobody wrote.
- **Consuming one namespace is the platform's answer and not a choice**, and
  ADAPTERS.md states it as such. Broadcast Channel subscription is implicit only:
  a grain *type* names its namespaces in a compile-time attribute. Nothing
  subscribes to a namespace decided at run time, so the subscriber must be a grain
  this package compiled, and consumption is confined to
  `orleans-dataflow-broadcast` with the document's channel id as the **key** inside
  it. `OrleansStages.BroadcastSourceChannel(provider, key)` composes the address a
  publisher writes to. The sink stays namespace-free, because publishing needs no
  subscription — the asymmetry is the rule showing through.
- **The relay grain is the delivery registry**, one activation per channel key,
  attach table in memory, nothing persisted. That matches what a channel is
  (implicit, best-effort, no history) and it is the thing the phase-3 note deferred:
  the keyed stage needed no registry because an executor's address is composed from
  the run's own identity, while a broadcast subscriber's address is the runtime's to
  choose. This is the opposite direction, and the registry is what bridges it.
- **A run attaches at its first pull and detaches in the `finally`** — the
  observer-bridge pattern, with the receiver rooted by `GC.KeepAlive` past the
  detach because Orleans holds observer objects weakly. What differs is that the
  relay holds *many* receivers: an attachment is named `{graph}/{run}/{node}` and
  carries the provider the document declared, so two runs of one pipeline, two
  occurrences of one run, and two runs wanting two providers all coexist on one
  channel. Every push is outcome-aware and a receiver answering `Closed` or
  `Failed` — or raising at all — is forgotten after one refusal, the same
  arithmetic the bridge did: an unreachable receiver costs the whole response
  timeout per push, so remembering it makes every later publication on that channel
  pay for a run that has gone.
- **The turn is never parked, so the backpressuring policy is refused** — in the
  payload reader and in the authoring helper, exactly as the reminder trigger
  refuses it. The relay forwards on its own non-reentrant turn and awaits each
  push, so a run waiting for room would hold the channel for every other run
  listening to it; and under a fire-and-forget provider it would hold it while no
  publisher was waiting at all. The fan-out itself is concurrent (`Task.WhenAll`),
  which changes no ordering that exists — two runs are independent — and bounds one
  lost run's cost at one response timeout rather than at one per listener. Ordering
  within a run survives because the grain is non-reentrant: a publication is fully
  forwarded before the next begins.
- **Nothing thrown leaves the relay, and a contract mismatch fails the run instead.**
  Under checked delivery a subscriber's exception *is* the publisher's exception, so
  raising would let one run's trouble fail a publication that never heard of it —
  and every other listener with it. So failures are outcomes; and the one thing
  that is genuinely wrong, an element of a type the run did not declare, fails
  *that* run with a message naming both types while the publication succeeds. One
  channel key carries one element type, the same shape the stream adapters have,
  and the check runs on the run's own receiver because the relay is subscribed
  untyped.
- **`FireAndForgetDelivery` is absent from the source's payload and that is a
  measured result.** The mode decides whether a publisher waits for its subscribers
  and whether their failures reach it; a subscriber's contract is identical under
  both and this relay fails a publication under neither. A member here would be a
  declaration with nothing to check it against — the sink declares one because a
  sink is the publisher. A test runs the same source against both providers.
- **Phase-4b limits, stated**: the registry lives in an activation, so a relay
  collected while runs are attached loses them and those runs go **quiet rather
  than failing** — nothing links a relay back to the runs it lost, the same
  asymmetry the reminder trigger documents, and blinder than the bridge's because a
  publisher is never told either. That one is reasoned from the design rather than
  measured: the suite never forces a relay to deactivate, so it is a stated limit
  and not a tested one. A publication that arrives before a run attaches
  is gone; there is no history to catch up from and no subscriber list a publisher
  could have consulted. A run lost without detaching costs the *next* publication
  on that channel a full response timeout — and, under a checked-delivery provider,
  costs the publisher the same wait — paid once for the channel rather than once per
  listener, and once rather than repeatedly, which is what the forget-on-refusal
  rule buys. Provider separation is ours and only ours: Orleans will
  deliver one key's publications from every provider to one activation, so a
  deployment that puts two providers on one key is relying on our filter rather than
  on the platform. And these are single-silo tests: nothing here proves that a relay
  activated on one silo reaches a run executing on another, only that the addresses
  agree and that a lost receiver is forgotten.

## M4.5 — as implemented (the result-size cap, and what made it testable)

- **The cap is a silo's option with a default that is not "unbounded".**
  `LimitResultSize(bytes)` on the silo builder, defaulting to one mebibyte. The
  reasoning and the enforcement point are in open question 2 above, which this
  closed; what belongs here is the shape: the run grain measures the value where
  it builds the envelope, and `ResultTooLargeException` carries the slot, the
  measured size, and the bound.
- **Measured, not estimated, and not materialized.** The value is serialized
  through a counting buffer writer, so the exact wire size is known and the bytes
  of an oversized result are never collected — a cap that had to build the array
  before refusing it would allocate the very thing it exists to prevent. The
  measurement is one serialization of a value about to be serialized again,
  which is a stated cost paid per read of one slot.
- **The run is not an event's worth different for having been read.** An
  oversized result leaves the run `Completed`, leaves its completion successful,
  leaves its other slots resolvable, and refuses the same way on every later
  read. Faulting the run instead would rewrite history on a read; refusing at the
  client would mean the bytes had already crossed.
- **A branching pipeline is what made the second claim provable.** Two results
  from one run — one inside the bound, one past it — needs a multi-result
  deployable document, and until a junction could be registered no such document
  existed. So the same milestone that opened junction registration is the one
  that could test the cap properly, and the capability matrix's "the distributed
  half is not proven and cannot be yet" note on named multiple results is lifted
  by the same tests.
- **M4.5 limits, stated**: the cap is checked where a result is read and nowhere
  else, so a run may accumulate a result far larger than the bound and only learn
  of it at the read — the bound is on what crosses the wire and not on what a
  terminal may hold. It is a silo's, so two silos may disagree about one document
  and nothing reconciles them. And these remain single-silo tests: nothing here
  proves anything about the cap under placement or failover, only that the number
  a deployment wrote is the number the grain applied.

## M5.3 — as implemented (durable runs in the host: the store, the resume, the crash suite)

The phase that lifts phase 1's sharpest limit — "a deactivation mid-run faults
that attempt" — and lifts it **only under a declared option**. M5.2 built the
checkpoint model in one process; nothing about it changes here. What this phase
adds is a store behind a silo, a trigger that continues a run, a wire for the
declaration, and the crash evidence the matrix row was waiting for.

### Resume is activation-driven, and there is no second protocol

A run grain now reads its checkpoint key when it activates. **A checkpoint
present means the run is resumed**: the activation claims a fresh epoch from the
coordinator, materializes the plan with the stored cursors, scope states and
marks restored, and reports `Running` — so the client's own status poll, which
is what brought the activation into being, sees a running run.
`PipelineRunLostException` is therefore *unreachable* for a durable run whose
checkpoint exists. **A durable run with no checkpoint yet is a lost attempt
exactly as an ordinary run is**, and that is asserted rather than footnoted:
durability is not a promise that an attempt survives, it is a promise that a
*stored position* is continued.

The gate is the checkpoint and deliberately not the coordinator's register, so
the cost of the lift is one store read per activation on a silo that registers a
store and nothing at all on a silo that does not.

### The store is the deployment's, registered like the coordinator's

`UseCheckpointStore(services => store)` on the silo builder, beside
`AddCatalog` and `AddFactory`. Nothing supplies a default, for the reason the
coordinator's grain storage has none: an in-memory default would let a
deployment believe its runs were durable while their positions died with the
process. **A silo with no store is a legal configuration** — every deployment
before this phase had one — and what it refuses is a *durable declaration*, by
name, at the declaration rather than at the first capture.

The document travels as canonical bytes into the store and out of it, unchanged,
because that is what makes one process' checkpoint another's.

### The coordinator persists what reconciliation actually reads

Phase 1 removed a run register that grew per accepted start and recorded that
"M5's durable resume will persist what reconciliation actually reads". This is
that: `PipelineCoordinatorState.DurableRuns`, one record per **declared durable
run**, carrying the canonical document, its fingerprint, the declared timing, the
epoch, and whether anything has claimed it yet. It differs from what was removed
in the way that matters — a durable run is named by its author, so the register
grows with the names a deployment chose rather than with how often it pressed
start. Serializer id 2, because id 1 stays retired.

`DeclareDurableRunAsync` **declares and does not start**; `EnsureStartedAsync` on
the run grain starts or continues. The split is what makes the resume need no
protocol of its own: an attempt after a crash takes the second half of the very
path the first attempt took.

**Deadlock is closed by shape rather than by timing.** A run grain calls its
coordinator (to claim an epoch) and the coordinator's three status/control
members call run grains; a cycle would be two grains each waiting on the other.
So: nothing that touches the register ever awaits a run grain — the declaration
returns before anything is started — and the three passthroughs, which touch no
state at all, are `[AlwaysInterleave]`. The epoch sequence is still produced one
turn at a time.

### The run identity is the author's, and that is the phase's one API change

`OrleansDataflowHost.MaterializeDurableAsync(pipeline, options)` takes a
`DurablePipelineOptions` naming the run and its checkpoint cadence.
`MaterializeAsync` names each run afresh, so two calls are two runs; **two
durable calls under one name are one run** — the second hands back a handle to
the run already executing, or continues it from its checkpoint if the silo
hosting it has died. A name allocated per attempt would contradict resume
outright: nothing would be able to find the previous attempt's position.

Two ripples fall out of that and both are surface:

- **Declaring one name with two documents is refused by name**
  (`PipelineResumeRefusedException`, carrying both fingerprints). V1 continues
  one document per durable run identity; a changed pipeline runs under a name of
  its own. The same refusal is raised by an activation whose stored checkpoint
  names another fingerprint or another revision, which is the half that catches
  a store somebody else wrote into.
- **A durable handle follows the run rather than the attempt.** A resumed
  attempt claims a fresh epoch, so a handle from before it is out of date rather
  than wrong: it adopts the epoch the fencing refusal names and carries on. Only
  a durable handle does this, and only forward — an ordinary run has no later
  attempt, so a refusal there is somebody else's claim and adopting it would be
  taking over work the handle never started.

### The stream cursor: the position the model was designed for

`orleans/stream-source` declares a cursor. It stores the **sequence token of the
element the run delivered** — promoted when the run reports the delivery and not
when the subscription received it, because a bounded ingress holds elements the
run has not taken and a cursor counting arrivals would skip them — and a resumed
run subscribes at that token. Two probed facts shape it and both are in
[ORLEANS-NOTES.md](ORLEANS-NOTES.md): rewind is **inclusive** of the element the
token names, so the window is one element wider than an index cursor's and no
"token plus one" exists to narrow it; and the memory provider **purges its cache
when its last consumer unsubscribes**, so `IsRewindable` is a statement about
ability rather than about what a provider still holds.

The position is `{"index":n,"sequence":n,"token":"…"}`: the provider's own two
numbers, readable by anybody, beside the token as the silo serializer's bytes in
base64. That base64 member is **the one value in a checkpoint document that is
not portable outside the deployment that wrote it**, and the trade is stated —
the numbers make the position auditable, the blob makes the resume exact, and a
process holding the same stream provider is what another silo of the same
deployment is.

The seam it needed is one public overload in the core package,
`DataflowStageRuntime.Source(open, cursor)` plus the `DataflowSourceCursor` the
opener closes over. That is the only touch this phase made to
`Orleans.Dataflow`, and it was unavoidable: a cursor declared by a *registered*
source has nowhere else to be declared.

### The crash suite, and the numbers it produced

On the three-silo fixture, over the plain test vocabulary extended with a
cursored source and a recording sink:

- **Resume across a kill, with the window measured as a sequence.** Five
  elements, a capture every three, the silo killed after the source parked: the
  store holds cursor three, the sink's log is `[1,2,3,4,5]`, and after the kill
  the client's own poll brings the run back on a surviving silo and the log
  becomes `[1,2,3,4,5,4,5]`. The duplicate window is exactly the two elements
  between the stored cursor and the crash, by value.
- **Repeated kills leave a document that still reads.** Two thousand elements at
  a capture per element, killed three times, restored between: after every kill
  the stored document parses, names this graph, and carries a position inside
  the stream; the union of the attempts covers the whole stream with no gap; and
  the total delivered is between 2000 and 2003 — **at most one replayed element
  per kill**, which is an arithmetic consequence of the bound rather than an
  observation. Over nine kills every one of them landed mid-stream, and the test
  asserts that at least one did so that it cannot pass having proved nothing.
- **A superseded writer's capture is refused and kills that attempt.** Staged
  with the store's `Supersede` — Orleans will not let two activations of one run
  exist, so what can be staged is the state the race leaves — the run fails with
  `CheckpointConflictException` on its next capture, unwrapped, naming both
  ETags. The gate that makes it deterministic is worth a sentence of its own:
  the source stops between the capture at five and the one at ten, at the
  *seventh* element rather than the sixth, because a capture due at element `n`
  does not complete until element `n+1` has been produced.
- **Both fingerprint refusals**, by name, with both fingerprints on the
  exception: a second declaration under one name, and an activation whose stored
  checkpoint names another graph.
- **The contrast, twice.** The same pipeline through the same kill, not declared
  durable, still reports `PipelineRunLostException`, its log holds exactly one
  attempt's worth of elements, and the store holds nothing for it — and so does a
  run that *was* declared durable under a bound it never reached, which is the
  sharper of the two: durability is not a promise that an attempt survives.
- **A resume driven through the coordinator's own status call answers.** The one
  test in the suite that would hang rather than fail if the shape were wrong, and
  it was checked by breaking it: with the passthrough's `[AlwaysInterleave]`
  removed it fails against the response timeout, and with it the call returns a
  fencing refusal naming the epoch the resume had just claimed.
- **A silo with no checkpoint store refuses a durable declaration by name**,
  observed on the rolling-upgrade fixture, which registers none — while an
  ordinary run of the same pipeline on the same silo is unaffected.

The stream cursor's own evidence is on the single-silo adapter fixture, where the
stream provider lives: a token is stored with its two readable numbers, and a
resumed run subscribes at it and receives the tail of the first batch followed by
everything published while nothing was listening.

### M5.3 limits, stated

- **A capture cannot be taken while a source is inside its own step.** The run
  reaches a safe point *between* steps, and a source segment takes its next step
  before it parks — so a capture due at element `n` waits for element `n+1`, and
  a stream source with nothing to deliver holds a timed capture open for as long
  as the stream is quiet. It is a delay and not a loss: the next delivery
  completes the step and the capture that was due is taken. Asserted, both
  halves, on a quiet stream. A cadence that must fire on an idle source is
  unbuilt.
- **A durable run is continued by the next activation whatever ended the
  previous attempt.** A run grain persists nothing, so once its activation is
  gone nothing distinguishes "died mid-run" from "failed" or from "completed" —
  the checkpoint is all there is, and a checkpoint says where, never whether. So
  a durable run that faulted and then lost its activation is resumed and will
  fault again, and one that *completed* and then lost its activation is resumed
  and re-runs its tail. Asserted by value rather than left as a caveat: five
  elements, a capture every two, the run completed and its grain deactivated,
  and the continued run's log ends `…, 4, 5, 5`. That is at-least-once taken to
  its conclusion rather than a defect, and it is why a durable run is declared
  by an author who means it. Reporting the end to the coordinator so that a
  finished run stops being resumable is the obvious next step and is recorded
  rather than built.
- **The stored position of a run that ended is not a number a test may name.** A
  capture the last element made due asks the run to hold; the source's next step
  ends the stream instead of producing one; and whether the hold is reached
  before the run settles — in which case the position is written — or after it,
  where the loop's own "the run is over" guard skips it, is a race. Measured
  rather than reasoned: a four-element run capturing every two stored cursor
  four, which the arithmetic alone would not have predicted. Nothing a resume
  can observe changes, because a resume replays from wherever the store stopped;
  what it means is that a test wanting an exact stored cursor puts its last
  capture somewhere other than on the final element.
- **The suite's store cannot be torn, so nothing here tests a torn write.** The
  checkpoint store lives in the test process and the silos live inside it, so a
  silo dying cannot interrupt a write the store is performing. What the repeated
  kills prove is that no arrangement of them produced a document a resume could
  not read; atomicity of a *real* store's write is that store's contract, which
  `ICheckpointStore` states and this suite does not exercise.
- **The stale-attempt conflict is staged and not raced.** Orleans guarantees one
  activation per run grain, so two attempts writing at once is a state a test
  cannot reach; `Supersede` puts the store into exactly the state it would
  leave. That is the coordinator store's own precedent, and the residual gap is
  the same one: nothing here proves that a real superseded activation writes
  late, only that a writer holding a stale ETag is refused and dies.
- **No cross-revision migration**, unchanged from M5.2: a resume against a
  different fingerprint or revision is refused by name.
- **No commit mark on the registered side.** The seam that lets a *provider*
  declare one does not exist, so the crash suite measures its window against the
  cursor rather than against a mark. For the graph it measures on — a source
  straight into a sink with no buffer between them — the two coincide exactly;
  for a graph that batches they do not, and that gap is the local suite's to
  measure until a marking seam is public.
- **One silo hosts one run, still.** Nothing here distributes a run; what
  survives a silo is the run's *position*, and the resumed attempt is a whole run
  on one host exactly as the first was.

## Phasing

1. **Hosting + coordinator + run grain** — DI registration, the
   runtime-factory seam, fenced run lifecycle, remote handle,
   single-silo `InProcessTestCluster` tests (start/complete/fail/shutdown/
   fence-rejection), catalog-fingerprint refusal test.
2. **Streams + grain calls** — the four core adapters with their
   acknowledgement tables and provider-guarantee reporting; keyed calls
   run-local.
3. **Triggers + bridges** — timer, reminder, observer, broadcast channel,
   observable/event bridges; each with the durability/missed-tick/
   best-effort semantics from the adapter table.
4. **Placement, keyed distribution, failover** — per-key executor grains
   with a credit protocol (grants ride on replies; bounded in-flight per
   key), placement options, multi-silo tests: `KillSiloAsync` +
   `WaitForLivenessToStabilizeAsync`, no split-brain ownership (epoch
   fencing proven under kill), deactivation/reactivation, rolling-upgrade
   catalog refusal. Split in two: **4a** is the keyed stage, its credit
   protocol, and the placement options — landed, see the as-implemented
   section above; **4b** is failover, which needs the multi-silo fixture
   nothing before it required, together with the delivery registry the
   phase-3 note deferred to it — the broadcast source, landed, see its own
   as-implemented section.

## Open questions (answered by their phase, not guessed now)

1. Remote `RunHandle`: polling versus observer for completion/results, and
   what `GetValueAsync` latency contract a remote slot carries. **Half
   settled by M3's close**: polling won phase 1 and phase 4 hardened it —
   an undelivered poll is retried rather than surfaced, because silence is
   not a fact about the run — while an observer push channel remains
   unbuilt and moves to M4+ with the rest of this question.
2. ~~Whether the run grain streams large results or caps result-slot payload
   sizes~~ **Answered in M4.5, and the likely answer was the answer: a cap with
   a named error.** Not streaming — a result is one value a slot resolves, and
   chunking it would make every caller reassemble something the definition plane
   never described as a sequence. `LimitResultSize(bytes)` on the silo builder,
   defaulting to **one mebibyte**, which is comfortably above every result this
   library's own vocabulary produces (a fold's state, a first or last element, a
   count) and comfortably below the size at which one Orleans message is a
   problem for the cluster rather than for the caller. It is a bound rather than
   an absence because the failure it prevents has no good spelling, and it is a
   *silo's* rather than a pipeline's because how much a host will put on one
   message is a property of the deployment and its network — two silos with
   different limits accept the same document and disagree about what it may
   return, which is the same deployment-scoped honesty the binding registry has
   carried since phase 2.
   **Enforced at envelope creation, on the grain side, and only the slot fails.**
   The size is exact rather than estimated: the value is serialized through a
   writer that counts and discards, so an oversized result is refused without
   ever being materialized as bytes, let alone sent. What it costs is one
   serialization of a value that is about to be serialized again, paid once per
   read of one slot and never per element. The run itself is untouched — it has
   already ended successfully, reading a result is not an event in its life, and
   its other slots resolve normally — so `ResultTooLargeException` carries the
   slot, the measured size, and the bound, and is not a codec error, not a
   faulted run, and not a poll that never answers. Proved on a branching
   pipeline whose two legs declare two results, one inside the bound and one
   past it, which is a document a cluster could not have been handed before the
   same milestone made a junction registrable.
3. ~~The exact credit protocol wire shape for phase 4 (grant-on-reply versus
   explicit credit messages)~~ **Resolved by probe (phase 4a)**: grant-on-reply,
   in its strongest form — the reply *is* the grant and there is no credit
   member on the wire at all. Forced rather than preferred: the probe showed
   Orleans reordering pipelined calls between one caller and one non-reentrant
   callee in every round, inside a single silo, so per-key in-flight is one and
   the ordering the matrix promises is a property of the adapter's accounting
   instead of the transport's. The credit window across distinct keys is the
   stage's declared bound, held by the engine. Explicit credit messages would
   have bought nothing: with one call outstanding per key there is exactly one
   message whose arrival could carry a grant, and it already does.
4. The reminder minimum period — the remaining research unknown, probed in
   phase 3 before any contract claims it (memory-stream rewindability
   resolved: true; rewind stays unexposed until a checkpoint/cursor story
   exists to consume it).
5. ~~Per-occurrence port contracts in the definition model~~ **Closed in M4.5,
   and closed by not doing it.** The escalation was that fixed stage ids mean
   one element contract per adapter port, so a typed seam needs the explicit
   opaque-contract escape hatch; the proposed lift was to let an occurrence
   override its specification's port contracts. M4.5's multi-port registered
   stages answer the question the escalation was really asking — *can a provider
   express a branching pipeline with real contracts on every port?* — and the
   answer is yes with the definition model exactly as it is: a registered
   junction's every port carries the provider's own contract, checked port by
   port at handle creation and edge by edge by the graph compiler, and a
   deployable branching pipeline needs no override anywhere.
   **A provider that wants a junction over other contracts registers another
   specification**, which is cheap, and that is the whole of the replacement.
   The alternative was expensive in a way worth stating: an occurrence that
   overrode its stage's port contracts would make a specification a default
   rather than a contract, would make two documents naming one stage describe
   two different stages, and would put the burden of noticing on whoever reads
   the document rather than on whoever registered the stage. What remains true
   and is unchanged is the adapters' own case: an `orleans/*` adapter port
   declares `orleans-element@v1` because a fixed stage id has one contract, so a
   typed seam to one still needs `OrleansStages.Element<T>()`. That is a
   property of those particular stage ids, not of the definition model, and the
   fix for it — if it is ever wanted — is more stage ids rather than fewer
   contracts.
