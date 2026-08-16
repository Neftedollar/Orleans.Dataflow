# Roadmap

This roadmap orders the work by architectural risk. Dates are intentionally absent; milestone exit criteria determine progress.

## M0 — Contract foundation

Deliverables:

- language-neutral immutable graph document;
- stable graph, node, port, stage, contract, and revision identifiers;
- typed source/flow/sink authoring fragments over that graph;
- deterministic fragment composition and identity rebasing;
- stage catalog contracts and provider registration;
- graph compiler and validation diagnostics;
- deterministic serialization with golden compatibility tests;
- initial C# API examples and an F# compatibility compile prototype.

Exit criteria:

- graph construction is side-effect free;
- the same inputs produce byte-for-byte equivalent graph documents;
- invalid ports, types, duplicate IDs, missing registrations, invalid cycles, and incompatible versions fail before execution;
- no delegate, closure, runtime service, CLR assembly-qualified type, task, grain reference, or channel enters the durable graph document.

## M1 — Linear bounded runtime

Deliverables:

- one-source/one-sink runtime;
- bounded demand protocol and ingress queue;
- core map, filter, choose, async map, scan, take/drop, fold, and callback stages;
- bounded buffers and explicit overflow results;
- completion, failure, cancellation, shutdown, and abort;
- run handle and result-slot resolution for local runs;
- demand-aware test probes.

Exit criteria:

- bounded-memory tests remain bounded under a stalled sink;
- ordered and unordered async stages meet their declared contracts;
- every terminal path releases enumerators, resources, registrations, and in-flight work;
- a graph can be materialized more than once without sharing accidental stage state.

## M2 — Orleans distributed execution

Deliverables:

- Orleans hosting registration and lifecycle integration;
- coordinator and executor grains with fenced run ownership;
- credit-based flow control across grain boundaries;
- catalog fingerprint/capability validation across eligible silos;
- Orleans Stream source and sink;
- awaited and keyed grain-call stages;
- timer source and grain async-enumerable source;
- placement/partition ordering contract;
- OpenTelemetry metrics, traces, and graph/run inspection.

Exit criteria:

- multi-silo tests prove placement, backpressure, cancellation, deactivation/reactivation, failover, and no split-brain run ownership;
- provider-specific Orleans Stream guarantees are reported rather than generalized;
- an actor mailbox is never used as an unbounded substitute for demand.

## M3 — Supervision and durable recovery

Deliverables:

- stop/resume/restart-stage supervision;
- retry, recover, and restart-section policies;
- checkpoint model and storage provider contract;
- source cursor ownership and sink commit/idempotency contract;
- durable run resume and graph revision compatibility;
- reminder-trigger source with documented missed-tick behavior;
- failure injection harness.

Exit criteria:

- crash tests at each checkpoint/side-effect boundary prove the stated duplicate/loss window;
- no global exactly-once claim exists; stronger guarantees are adapter-specific and evidenced;
- state reset and durable-state survival are explicit for every restart form.

## M4 — Graph topology and provider ecosystem

Deliverables:

- graph builder for typed multi-port shapes;
- merge, concat, zip, broadcast, balance, partition, and unzip junctions;
- bounded substreams and dynamic hubs;
- provider SDK and conformance tests;
- optional Kafka, database/outbox, HTTP, SignalR, file/stream, and observable adapters according to priority and maintainer capacity;
- named multiple result slots.

Exit criteria:

- topology liveness, fairness, cancellation, unconsumed-port, and resource-bound tests pass;
- every adapter publishes an acknowledgement/delivery/checkpoint/idempotency table;
- provider packages do not leak their configuration into core source/flow/sink option types.

## M5 — C# parity and 1.0 hardening

Deliverables:

- all approved P0/P1 capability rows at Qualified or explicitly deferred with a release-blocking rationale;
- API and binary compatibility review;
- supported .NET and Orleans compatibility matrix;
- load, bounded-memory, recovery, and rolling-upgrade evidence;
- complete conceptual and API documentation;
- clean-room getting-started verification;
- deterministic package and provenance pipeline prepared but not published.

Exit criteria:

- every criterion in [GOAL.md](GOAL.md) is evidenced;
- the user explicitly approves the 1.0 release operation.

## M6 — Idiomatic F# frontend

This milestone may begin earlier as soon as the C# graph contract and parity surface are substantially complete. A compile-only prototype remains part of earlier milestones so C# decisions cannot make the F# surface impossible.

Deliverables:

- `Orleans.Dataflow.FSharp` modules and pipeline-first functions;
- clear source, flow, sink, run, and host option records;
- distinct Task, ValueTask, and F# Async operator families;
- natural reusable flow and graph composition;
- optional `Orleans.Dataflow.OrleansFSharp` adapter for specification-003 functional grain contracts;
- F# examples, tests, and documentation authored as F#, not transliterated C#.

Exit criteria:

- representative F# applications require no user-authored C# class;
- public names follow current F# component design guidance;
- the F# API does not depend on overload guessing, SRTP tricks, serialized closures, or mutable builders.

## Work selection rule

Within a milestone, prioritize in this order:

1. decisions expensive to reverse;
2. semantic contract tests;
3. minimal implementation proving the contract;
4. breadth of operators and adapters;
5. optimization after a benchmark demonstrates the need.

No milestone label is a release claim. The project remains WIP until the explicit 1.0 decision.
