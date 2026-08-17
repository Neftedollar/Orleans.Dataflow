# Roadmap

This roadmap orders the work by architectural risk. Dates are intentionally absent; milestone exit criteria determine progress.

C# is the primary frontend. The F# frontend is an equal API built over the same semantic core, and it starts only after the C# completion gate (M6) passes an independent review.

## M0 — Definition core (closed 2026-08-16)

Deliverables:

- language-neutral immutable graph document;
- stable graph, node, port, stage, contract, and revision identifiers;
- canonical payload values and deterministic serialization with golden compatibility tests;
- stage catalog contracts and provider registration;
- graph compiler and validation diagnostics;
- deterministic fragment composition and identity rebasing;
- typed ports, result slots, execution policies, and capability declarations in the document model.

Exit criteria:

- graph construction is side-effect free;
- the same inputs produce byte-for-byte equivalent graph documents;
- invalid ports, types, duplicate IDs, missing registrations, invalid cycles, and incompatible versions fail before execution;
- no delegate, closure, runtime service, CLR assembly-qualified type, task, grain reference, or channel enters the durable graph document.

## M1 — C# authoring API (closed 2026-08-17)

Deliverables:

- `Source<T>`, `Flow<TIn, TOut>`, `Sink<T>`, graph shapes, `RunnableGraph`, `ResultSlot<T>` authoring types over the definition core;
- an API ADR fixing names and generic shape after comparing variants on several real usage examples;
- fluent composition with good type inference, nullable annotations, and no ambiguous overloads;
- separate source, flow, sink, and run option types;
- deterministic fragment reuse and import scoping at the API level;
- compile tests, construction tests, and runnable examples;
- an independent review of the public API surface;
- an F# compatibility compile prototype so C# decisions cannot make the F# surface impossible.

Exit criteria:

- representative programs compose sources, flows, sinks, and fragments without touching the definition IR by hand;
- authoring produces deterministic graph documents identical to core-built equivalents;
- the API review finds no blocking naming, inference, or overload defects.

## M2 — Local bounded runtime (closed 2026-08-17; two recorded deferrals)

Deferred deliberately, with their rationale: the C# `Choose` spelling moves
to the M4 operator ADR (`Where`+`Select` covers the semantics; the F#
`choose` arrives with the F# frontend over the algebra), and the
controllable time abstraction plus the tick source move to M4 with the
first operator that reads a clock — a clock nothing reads would be dead
code pinned before its consumer exists.

Deliverables:

- reference execution engine for validated graphs;
- bounded demand protocol, bounded buffers, and explicit overflow results;
- core map, filter, choose, async map, scan, take/drop, fold, and callback stages;
- completion, failure, cancellation, shutdown, and abort; pause, resume, and drain controls;
- kill-switch/shutdown control and result-slot resolution through the run handle;
- controllable time abstraction for deterministic tests;
- core local sources and sinks (values, enumerables, tasks, unfold, bounded queues and channels, tick);
- demand-aware test probes.

Exit criteria:

- bounded-memory tests remain bounded under a stalled sink;
- ordered and unordered async stages meet their declared contracts;
- every terminal path releases enumerators, resources, registrations, and in-flight work;
- a graph can be materialized more than once without sharing accidental stage state.

## M3 — Orleans runtime (closed 2026-08-17; three recorded deferrals)

Deferred deliberately, with their rationale: cross-silo evidence for the
distributed keyed stage (executors proven to land on and answer from more
than one silo, cancellation and the credit bound observed across a real
hop) moves to the milestone that builds partition ownership and rebalance
— the capability row honestly says Research, and the protocol itself is
transport-independent by construction; the observer-push completion
channel remains an open question beside the polling that phase 4
hardened; and the result-slot size cap is carried to M4, where
Collect-shaped terminals make it unavoidable. The .NET event source is
not deferred but settled: the matrix records it as a documented one-line
IObservable wrap, deliberately not a second stage.

Deliverables:

- Orleans hosting registration and lifecycle integration;
- coordinator and executor grains with fenced run ownership;
- credit-based flow control across grain boundaries;
- catalog fingerprint/capability validation across eligible silos;
- Orleans Stream source and sink; awaited and keyed grain-call stages; grain async-enumerable source;
- timer and reminder trigger sources with documented durability and missed-tick semantics;
- observer and Broadcast Channel bridges with explicit best-effort semantics;
- `IObservable<T>` and .NET event bridges with mandatory bounded buffering;
- placement/partition ordering contract;
- multi-silo tests for placement, backpressure, cancellation, deactivation/reactivation, and failover.

Exit criteria:

- multi-silo tests prove no split-brain run ownership;
- provider-specific Orleans Stream guarantees are reported rather than generalized;
- an actor mailbox is never used as an unbounded substitute for demand.

## M4 — Graph topology and operator breadth

Deliverables:

- graph builder for typed multi-port shapes;
- merge, concat, zip, broadcast, balance, partition, unzip, interleave, and combine-latest junctions;
- bounded substreams, group-by, split, prefix-and-tail, and dynamic hubs;
- full pipeline composition, including pipelines as branches and cycles behind explicit buffer/delay boundaries;
- operator breadth: batching, windowing, timing, rate, flattening, distinct/deduplicate, prepend/append, divert-to, observation;
- named multiple result slots;
- provider SDK and conformance tests; optional external adapters (Kafka, database/outbox, HTTP, SignalR, files/streams/pipelines) follow the SDK and must not delay the core.

Exit criteria:

- topology liveness, fairness, cancellation, unconsumed-port, and resource-bound tests pass;
- every adapter publishes an acknowledgement/delivery/checkpoint/idempotency table;
- provider packages do not leak their configuration into core option types.

## M5 — Supervision, durability, and compatibility

Deliverables:

- stop/resume/restart-stage supervision; retry, recover, and restart-section policies;
- checkpoint model and storage provider contract;
- source cursor ownership and sink commit/idempotency contract;
- durable run resume and graph revision compatibility;
- rolling upgrade and catalog negotiation;
- failure injection harness;
- OpenTelemetry metrics/traces and stage/run monitor snapshots;
- operational documentation.

Exit criteria:

- crash tests at each checkpoint/side-effect boundary prove the stated duplicate/loss window;
- no global exactly-once claim exists; stronger guarantees are adapter-specific and evidenced;
- state reset and durable-state survival are explicit for every restart form.

## M6 — C# completion gate

An independent review must confirm:

- coherence of the public API;
- no major logical parity gaps against the approved capability matrix;
- backpressure correctness and no unbounded defaults;
- cancellation and failure correctness;
- a working Orleans runtime with multi-silo and recovery evidence;
- documentation describing actual semantics;
- a green Release build and CI.

Only after this gate does F# frontend work begin.

## M7 — Idiomatic F# frontend

Deliverables:

- `Orleans.Dataflow.FSharp` modules and pipeline-first functions over the same semantic core, with no runtime duplication;
- clear source, flow, sink, run, and host option types;
- distinct `Task`, `ValueTask`, and F# `Async` operator families;
- natural reusable flow and graph composition;
- F# examples, compile tests, and documentation authored as F#, not transliterated C#.

Exit criteria:

- representative F# applications require no user-authored C# class;
- public names follow current F# component design guidance;
- the F# API does not depend on overload guessing, SRTP tricks, serialized closures, or mutable builders;
- F#/C# compatibility tests prove both frontends produce compatible graph definitions with identical runtime semantics.

## M8 — 1.0 qualification

Deliverables:

- all approved P0/P1 capability rows at Qualified or explicitly deferred with a release-blocking rationale;
- API and binary compatibility review for both frontends;
- supported .NET and Orleans compatibility matrix;
- load, bounded-memory, recovery, and rolling-upgrade evidence; throughput/latency benchmarks after correctness;
- security and reliability review;
- human-maintainability review: the codebase remains understandable to ordinary .NET developers without AI assistance;
- complete conceptual and API documentation with a clean-room getting-started verification;
- deterministic package and provenance pipeline prepared but not published;
- a release readiness report.

Exit criteria:

- every criterion in [GOAL.md](GOAL.md) is evidenced;
- the user explicitly approves the 1.0 release operation.

## Work selection rule

Within a milestone, prioritize in this order:

1. decisions expensive to reverse;
2. semantic contract tests;
3. minimal implementation proving the contract;
4. breadth of operators and adapters;
5. optimization after a benchmark demonstrates the need.

No milestone label is a release claim. The project remains WIP until the explicit 1.0 decision.
