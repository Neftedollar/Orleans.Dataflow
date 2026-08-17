# Capability matrix

This document is the working definition of logical parity for Orleans.Dataflow. It tracks capabilities and semantics, not API spelling or implementation similarity.

Akka.NET Streams is the reference for the general streaming model. Microsoft Orleans and .NET documentation define the constraints of Orleans-native adapters. An item is not complete merely because a method with a similar name exists.

## Status vocabulary

- **Research**: semantics are understood and linked, but no contract is approved.
- **Specified**: public and runtime semantics are documented and reviewed.
- **Implemented**: code and focused tests exist.
- **Qualified**: contract, failure, and integration tests prove the documented behavior.
- **Deferred**: intentionally outside the current milestone, with a recorded reason.

Every row starts at **Research** and advances only together with its evidence; Implemented rows have code and focused tests on `main`.

## Capability tiers

- **P0**: required to call the result a bounded dataflow runtime at all.
- **P1**: required for the C# logical-parity gate.
- **P2**: expected before or shortly after 1.0, depending on evidence and package boundaries.

## Graph and materialization

| Capability | Tier | Target | Status | Required semantic proof |
|---|---:|---:|---|---|
| Immutable typed source, flow, and sink blueprints | P0 | M0 | Implemented | Composition does not execute work; instances are thread-safe and reusable. |
| Reusable `Flow<TIn,TOut>` composition | P0 | M0 | Implemented | Reusing one fragment creates independent node identities and runtime state. |
| Closed runnable graphs | P0 | M0 | Implemented | Validation can distinguish open ports from a materializable graph. |
| Named graph, stage, port, and revision identity | P0 | M0 | Implemented | Deterministic serialization, collision checks, and compatible revision rules. |
| Runtime materialization | P0 | M2 | Implemented | Each run allocates independent runtime resources and exposes completion. |
| Typed result slots | P1 | M2 | Implemented | Source, flow, sink, and lifecycle results are declared as typed named slots and resolved per run without persisting runtime objects; a run rejects slots from another graph identity, revision, or import scope. |
| Named multiple results | P1 | M4 | Research | Results remain type-safe and versionable across distributed execution. |
| Explicit fan-in/fan-out graph construction | P1 | M4 | Research | All ports are connected exactly once unless a junction contract states otherwise. |
| Cycles | P2 | M4+ | Research | Liveness validation requires an explicit buffer/delay boundary and has deadlock tests. |
| Custom stage provider SDK | P1 | M4 | Research | Registered implementation and serialized descriptor compatibility are validated. |

## Flow control and execution

| Capability | Tier | Target | Status | Required semantic proof |
|---|---:|---:|---|---|
| Non-blocking demand/backpressure | P0 | M2 | Implemented | A producer never exceeds downstream credit at every bounded boundary. |
| Bounded buffers by default | P0 | M2 | Implemented | Capacity and memory bounds are testable; no hidden unbounded mailbox is used as flow control. |
| Overflow policies | P1 | M2 | Implemented | Backpressure, drop-oldest, drop-newest, drop-buffer, and fail report distinct outcomes. |
| Operator fusion | P1 | M2 | Implemented | Compatible adjacent stages share an executor without changing element semantics. |
| Explicit async boundary | P1 | M2 | Research | Placement/concurrency changes are visible; ordering contract remains explicit. |
| Credit protocol across Orleans boundaries | P0 | M3 | Implemented | A call in flight is credit spent and its reply is the grant; nothing on the wire carries credit. Neither an awaited grain call nor a stream publication is counted as downstream demand: each adapter's bound is what admits the next element. |
| Partition-aware placement | P1 | M3 | Research | Partition ownership, rebalance, ordering, and failover are specified. Half of it exists on `main` and the row does not advance on half: placement of run grains and per-key executor grains is a hosting option (cluster default, random, prefer-local, hash-based) and per-key ordering is proven, while ownership, rebalance, and cross-silo keyed evidence are a recorded M3 deferral (see the roadmap) and a single-silo suite cannot show where activations landed. |
| Pause, resume, drain, shutdown, and abort | P1 | M2 | Implemented | Each control has distinct state transitions and in-flight behavior. |
| Kill switch and external shutdown control | P1 | M2 | Implemented | Single-run control is a RunHandle intrinsic (ADR 0004): shutdown drains, disposal cancels; a switch shared across runs is a separate documented contract. |

## Lifecycle and failure

| Capability | Tier | Target | Status | Required semantic proof |
|---|---:|---:|---|---|
| Completion propagation | P0 | M2 | Implemented | Downstream completion and upstream resource release are deterministic. |
| Failure propagation | P0 | M2 | Implemented | Downstream receives failure; upstream receives cancellation unless recovery intercepts it. |
| Downstream cancellation | P0 | M2 | Implemented | Early sinks cancel upstream and release resources. |
| Cancellation of async work | P0 | M2 | Implemented | Stage cancellation reaches `Task`, `ValueTask`, and later F# `Async` adapters. |
| Supervision: stop | P1 | M5 | Research | The failing stage fails the defined section or graph. |
| Supervision: resume | P1 | M5 | Research | The failing element is dropped while compatible stage state is retained. |
| Supervision: restart stage | P1 | M5 | Research | The failing element is dropped and declared local stage state resets. |
| Retry element | P1 | M5 | Research | Attempt count, backoff, idempotency, and poison-element handling are explicit. |
| Recover with element or alternate source | P1 | M5 | Research | Completion after fallback and source-switch boundaries are distinct. |
| Restart source/flow/sink section with backoff | P1 | M5 | Research | Reset scope, jitter, restart budget, and in-flight loss window are documented. |
| Durable resume after process/silo failure | P1 | M5 | Research | Checkpoint, source cursor, sink commit, and replay semantics are proven together. |

## Linear operators

| Group | Planned operators | Tier | Target | Status |
|---|---|---:|---:|---|
| Stateless mapping | map/select, filter/where, choose/collect, map-concat/select-many | P0 | M2 | Research |
| Async mapping | ordered parallel map, unordered parallel map, sequential async map | P0 | M2 | Implemented |
| Stateful mapping | stateful map-concat, scan, async scan | P1 | M2 | Research |
| Reduction | fold/aggregate, async fold, reduce | P1 | M2 | Research |
| Slicing | skip/drop, take, skip-while, take-while, take-through | P1 | M2 | Implemented |
| Batching | grouped, sliding, grouped-within, weighted grouped-within | P1 | M4 | Research |
| Timing | delay, initial delay, take-within, skip-within, timeout, valve | P1 | M4 | Research |
| Rate | throttle by element/cost, shaping versus enforcing mode | P1 | M4 | Research |
| Observation | tap/also-to, termination watch, monitor, completion callback | P1 | M4 | Research |
| Flattening | concat-map, merge-map with bounded parallelism | P1 | M4 | Research |
| Deduplication | distinct, deduplicate with explicit bounded state policy | P2 | M4 | Research |
| Sequence edits | prepend, append, divert-to side channel | P2 | M4 | Research |

C# names should follow .NET expectations where possible. If the Akka.NET behavior differs from LINQ, Orleans.Dataflow uses an unambiguous additional name rather than silently surprising C# callers. For example, `TakeWhile` should follow the ordinary exclusive predicate boundary, while an inclusive variant can be named `TakeThrough`.

## Junctions and substreams

| Capability | Tier | Target | Status | Contract note |
|---|---:|---:|---|---|
| Merge | P1 | M4 | Implemented | No global cross-input order unless a specialized merge supplies one. The local engine's fan-in pump rotates among the inputs that have an element, so a producer that is merely faster cannot starve an element that has already arrived at another input; every element of every input is emitted, each input's own order is preserved, the junction completes only when every input has, it holds one element outside its declared buffers (proved as a peak measured over the whole run rather than sampled), and a failure on any input fails the run including while the pump is asleep on the others. No C# authoring spelling yet: the graph builder is M4.2, and the proofs are over documents built directly. |
| Merge preferred/prioritized/sorted | P2 | M4+ | Research | Fairness and starvation policies are part of the contract. |
| Concat | P1 | M4 | Implemented | Later input is not consumed as the active input until the prior one completes, except bounded prefetch if declared. The local engine's junction reads one input to its end in port order before reading the next at all and completes when the last one does. "Not consumed" is honest as backpressure rather than as laziness: a run starts every segment, so a later input's source is running and parks in that input's own bounded channel — one element in the channel and one in the source's hand, widened only by a declared buffer — and a downstream completion releases the inputs whose turn never came. Head-of-line waiting composes into a deadlock when a broadcast's legs feed this junction directly, which is documented in LOCAL-RUNTIME.md and resolved by a declared buffer rather than refused at validation. No C# authoring spelling yet. |
| Zip/zip-with | P1 | M4 | Research | Positional pairing and early-completion behavior are deterministic. |
| Broadcast | P1 | M4 | Implemented | Every output receives each element; one slow output backpressures by default. The local engine's junction asks every live leg for room before it pulls, so the slow leg paces the stream (proved by how far a held source gets), holds one element outside its declared buffers, drops a leg whose downstream completed, and completes upstream when the last leg leaves. No C# authoring spelling yet: the graph builder is M4.2, and the proofs are over documents built directly. |
| Balance | P1 | M4 | Implemented | Exactly one available output receives each element; fairness is specified. The local engine rotates among the outputs that have room, so a leg with no room is routed around rather than blocking the others, every element arrives exactly once, and one element is held outside the declared buffers. An overflow policy on a balance's leg is unreachable by construction and is recorded as such. No C# authoring spelling yet. |
| Partition | P1 | M4 | Research | Routing function and invalid partition behavior are explicit. |
| Unzip/unzip-with | P1 | M4 | Implemented | Output backpressure interaction is explicit: both outputs must have room before the row is pulled, so the two legs advance in lockstep and re-zip without skew (proved by pairing the collected halves, since zip does not exist yet). No C# authoring spelling yet. |
| Interleave | P2 | M4 | Implemented | Segment size, fairness, and per-input completion behavior are explicit. The segment size is document payload validated as a positive integer by the reader the runtime itself uses; the rotation is fixed and waits for the input whose turn it is even when another has an element ready, so the emitted sequence is a function of the inputs and the segment size rather than of the scheduler; a completed input leaves the rotation and the remainder continues in order; the junction completes when every input has and holds one element outside its declared buffers. The same head-of-line deadlock a concat has applies for a segment size above one and is resolved by a buffer of that depth on the legs. No C# authoring spelling yet. |
| Combine latest | P2 | M4 | Research | Initial completeness, which-input-emits, and completion rules are deterministic. |
| Group by key | P1 | M4+ | Research | Maximum active keys, eviction, cancellation, and idle cleanup are bounded. |
| Split before/after | P2 | M4+ | Research | Unconsumed substream behavior cannot leak resources or deadlock silently. |
| Prefix and tail | P2 | M4+ | Research | Tail ownership and single-consumption rules are explicit. |
| Dynamic merge/broadcast hubs | P2 | M4+ | Research | Attach/detach, replay, buffer, and subscriber failure behavior are explicit. |

## Core sources

| Source family | Tier | Target | Status | Boundary |
|---|---:|---:|---|---|
| Empty, single, failed, never, repeat, cycle | P0 | M2 | Implemented | Pure lifecycle and constant sources. |
| Enumerable and async enumerable | P0 | M2 | Implemented | Iterator ownership, cancellation, and disposal. |
| Task and deferred factory | P0 | M2 | Implemented | Factory invocation occurs per materialization. |
| Unfold and async unfold | P1 | M2 | Implemented | State is per materialization; completion is explicit. |
| Resource unfold | P1 | M2 | Research | Resource closes on every terminal path. |
| Bounded ingress queue | P0 | M2 | Implemented | Offer result distinguishes accepted, dropped, closed, and failed. |
| Bounded channel | P1 | M2 | Implemented | Channel completion is not durability. |
| Tick/clock source | P1 | M2 | Research | Slow-consumer behavior and missed ticks are explicit. |

## Core sinks

| Sink family | Tier | Target | Status | Boundary |
|---|---:|---:|---|---|
| Ignore and completion | P0 | M2 | Research | Materializes graph completion. |
| First/last/optional first/last | P0 | M2 | Implemented | Early cancellation and empty behavior are explicit. |
| Collect to bounded result | P1 | M2 | Implemented | Maximum element/byte bound is required. |
| Fold/reduce/sum | P1 | M2 | Research | Final result and overflow behavior are specified. |
| Sequential and bounded-parallel callback | P0 | M2 | Implemented | Ordering and exception behavior are explicit. |
| Bounded channel/queue output | P1 | M2 | Implemented | Write acceptance and downstream consumption are distinct. |

## Orleans-native capabilities

| Capability | Tier | Target | Status | Required contract |
|---|---:|---:|---|---|
| Orleans Stream source | P0 | M3 | Implemented | Provider-specific delivery, ordering, rewind token, subscription ownership, and resubscription. |
| Orleans Stream sink | P0 | M3 | Implemented | Publication acknowledgement is not universal end-to-end processing. |
| Awaited grain-call flow/sink | P0 | M3 | Implemented | Awaited reply is the acknowledgement boundary; timeout/retry/idempotency are explicit. |
| Keyed grain-call flow | P1 | M3 | Implemented | One call in flight per key gives per-key ordering without relying on transport ordering (probed: Orleans reorders pipelined calls); the declared bound governs concurrency across keys; per-key executor grains are opt-in per occurrence. |
| Controlled grain group source | P1 | M3 | Research | A coordinator enumerates bounded keys/partitions; never an implicit cluster-wide grain scan. |
| One-way grain-call sink | P2 | M3 | Research | Explicit best-effort adapter; never the default durable sink. |
| Grain `IAsyncEnumerable<T>` source | P1 | M3 | Implemented | Call-scoped backpressure and cancellation; no implicit resume. |
| Timer source | P1 | M3 | Implemented | Run-scoped and non-durable (the run is the activation of this model); pull is the backpressure - no ingress, no drops. |
| Reminder trigger source | P1 | M3 | Implemented | Definition survives restart, but missed ticks are not replayed; a tick reactivating a grain with no live run unregisters the reminder. |
| Observer bridge | P2 | M3 | Implemented | Best-effort made observable: every push answers Accepted/Dropped/Closed/Failed; per-run bridge identity; no replay. |
| Broadcast Channel bridge | P2 | M3 | Implemented | Both directions exist: a publication sink whose declared `FireAndForgetDelivery` is checked against the silo's provider, and a subscription source whose relay grain per channel key holds the delivery registry of the runs listening to it. Best-effort and no history are surfaced rather than described — a publication with nothing attached is dropped silently, one that finds a full ingress is dropped or fails by the declared policy, and the backpressuring policy is refused because a shared relay cannot honor it. Consumption is confined to one package-owned channel namespace, which is Orleans' implicit-only subscription showing through and not a choice; probed, along with the per-key activation, the per-provider subscription callback, and the untyped attach the relay depends on. |
| Durable graph coordinator | P0 | M3 | Implemented | Single logical ownership and failover fencing are proven under silo death: one activation cluster-wide, epochs monotonic across kills, deactivation, and cluster-wide collection, a superseded writer refused by the store's ETag while the fresh activation reads the truth. The coordinator persists one counter; the durable store behind the provider name is the deployment's, and the tests prove the fencing against a real ETag-enforcing store precisely because Orleans' memory storage dies with its silo (measured). Run state transitions stay the run grain's, unpersisted until M5. |
| Checkpointed stage state | P1 | M5 | Research | Schema version, atomicity boundary, and migration policy. |
| Rolling upgrade/catalog negotiation | P1 | M5 | Research | A run never executes an incompatible stage catalog silently. |

## Optional adapter families

| Adapter | Tier | Earliest target | Status | Package direction |
|---|---:|---:|---|---|
| Files, .NET streams, and pipelines (`PipeReader`/`PipeWriter`) | P2 | M4 | Research | `Orleans.Dataflow.IO` |
| HTTP polling/SSE/webhook and HTTP sink | P2 | M4 | Research | `Orleans.Dataflow.Http` |
| SignalR source/sink | P2 | M4 | Research | `Orleans.Dataflow.SignalR` |
| Kafka source/sink | P1 | M4 | Research | Optional provider; partition/offset/commit contract required. |
| Azure Queue/Event Hubs | P2 | M4 | Research | Prefer Orleans provider integration where it preserves semantics. |
| Relational database/outbox/change feed | P1 | M4 | Research | Provider-specific optional package; no generic “database sink” promise. |
| `IObservable<T>` and .NET events | P2 | M3 | Implemented | Explicit bounded buffer and overflow; the notification thread pays backpressure; the event spelling is a documented one-line IObservable wrap, deliberately not a second stage. |
| Reactive Streams interop | P2 | M4+ | Research | Protocol bridge and TCK expectations required. |

## Testing and observability

| Capability | Tier | Target | Status |
|---|---:|---:|---|
| Deterministic graph inspection | P0 | M0 | Research |
| Demand-aware test source and sink probes | P0 | M2 | Implemented |
| Virtual/manual clock for time operators | P1 | M2 | Research |
| Lifecycle and materialized-value assertions | P0 | M2 | Implemented |
| Fault injection at source/stage/sink/boundary | P1 | M3-M5 | Research |
| Multi-silo placement/failover harness | P0 | M3 | Implemented | A three-silo in-process fixture with tuned-down membership, a silo-surviving ETag store, kill-restore per test, and helpers that locate and count activations cluster-wide; ten failover tests run on it. Measured and recorded in its remarks: an in-process kill is self-announced, so the tuning bounds only the unannounced path. |
| OpenTelemetry metrics, traces, and context propagation | P1 | M5 | Research |
| Stage/run monitor snapshots | P1 | M5 | Research |
| Compatibility and golden serialization tests | P0 | M0 onward | Implemented |
| Load, bounded-memory, and recovery benchmarks | P1 | M8 | Research |

## Primary references

- [Akka.NET Streams basics](https://getakka.net/articles/streams/basics.html)
- [Akka.NET built-in stages](https://getakka.net/articles/streams/builtinstages.html)
- [Akka.NET graph composition](https://getakka.net/articles/streams/workingwithgraphs.html)
- [Akka.NET buffers and rate](https://getakka.net/articles/streams/buffersandworkingwithrate.html)
- [Akka.NET error handling](https://getakka.net/articles/streams/error-handling.html)
- [Akka.NET dynamic streams](https://getakka.net/articles/streams/stream-dynamic.html)
- [Akka.NET integration](https://getakka.net/articles/streams/integration.html)
- [Microsoft Orleans streaming](https://learn.microsoft.com/en-us/dotnet/orleans/streaming/)
- [Orleans stream providers](https://learn.microsoft.com/en-us/dotnet/orleans/streaming/stream-providers)
- [Orleans timers and reminders](https://learn.microsoft.com/en-us/dotnet/orleans/grains/timers-and-reminders)
- [Orleans observers](https://learn.microsoft.com/en-us/dotnet/orleans/grains/observers)
- [.NET channels](https://learn.microsoft.com/en-us/dotnet/core/extensions/channels)

The matrix is deliberately stricter than a feature checklist. A row advances only when its semantics and evidence advance together.
