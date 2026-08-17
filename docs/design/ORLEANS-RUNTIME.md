# Orleans runtime design

- Status: M3 architecture; phase 1 is the implementation target, later phases
  firm up as their predecessors land
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
| Keyed grain call | `orleans/grain-call-keyed` | per-key ordered awaited replies | phase 4 distributes per-key executors; phase 2 executes keyed calls from within the run with bounded per-key in-flight |
| Grain `IAsyncEnumerable<T>` source | `orleans/grain-enumerable` | call-scoped pull (Orleans batching) | cooperative cancellation; `CancelRequestOnTimeout=false` gotcha handled explicitly |
| Timer trigger source | `orleans/timer` | none (tick generation) | activation-scoped, non-durable — documented |
| Reminder trigger source | `orleans/reminder` | none | definition survives restart, missed ticks not replayed — matrix contract verbatim |
| Observer / Broadcast Channel bridges | `orleans/observer`, `orleans/broadcast-channel` | best-effort, bounded ingress mandatory | no history, resubscription rules surfaced |
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
  author's exception type does not survive the hop; the coordinator's
  `Runs` register is written for phase-4 reconciliation but only
  `LastEpoch` is load-bearing today; ETag fencing of competing coordinator
  activations is designed but demonstrated only across deliberate
  deactivation until phase 4's kill tests.

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
   catalog refusal.

## Open questions (answered by their phase, not guessed now)

1. Remote `RunHandle`: polling versus observer for completion/results, and
   what `GetValueAsync` latency contract a remote slot carries.
2. Whether the run grain streams large results or caps result-slot payload
   sizes (Collect over a cluster is a foot-gun; a cap with a named error is
   the likely answer).
3. The exact credit protocol wire shape for phase 4 (grant-on-reply versus
   explicit credit messages) — decided against measured multi-silo behavior,
   not upfront.
4. Memory-stream rewindability and the reminder minimum period — the two
   research unknowns, probed in phase 2/3 respectively before any contract
   claims them.
