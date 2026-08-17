# Orleans runtime design

- Status: M3 architecture; phases 1-3 and 4a are implemented, and 4b's
  delivery-registry half — the broadcast source — is implemented and documented
  below; 4b's failover half is tracked separately
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
  a failure of that attempt (phase 1: the run faults; durable resume is
  M5's checkpoint work, not silently promised here). `[Reentrant]` is NOT
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
  abandon stay distinguishable across the seam; the factory mirror in the
  Orleans package is public while the engine seam stays internal.
- **Grain turns never park**: status and result calls answer "not yet"
  rather than await; shutdown and cancel request rather than drain; the
  engine's dedicated threads do the waiting.
- **Phase-1 limits, stated**: results live only as long as the run grain's
  activation (proven absent, not promised); a deactivation mid-run faults
  that attempt; a remote failure arrives as type name plus message — the
  author's exception type does not survive the hop; the coordinator
  persists `LastEpoch` and nothing else (a `Runs` register written "for
  phase-4 reconciliation" was removed after phase 4 shipped without
  reading it: it grew per accepted start with nothing pruning it, and M5's
  durable resume will persist what reconciliation actually reads); ETag
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
   what `GetValueAsync` latency contract a remote slot carries.
2. Whether the run grain streams large results or caps result-slot payload
   sizes (Collect over a cluster is a foot-gun; a cap with a named error is
   the likely answer).
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
5. Per-occurrence port contracts in the definition model — escalated by
   phase 2: fixed stage ids mean one element contract per adapter port, so
   typed seams need the explicit opaque-contract escape hatch today; lifting
   that means the definition model letting an occurrence override its
   specification's port contracts, a change owned by the definition plane
   (considered with M4's provider SDK), not by adapters.
