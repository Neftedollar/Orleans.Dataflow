# ADR 0004: C# authoring API baseline

- Status: Accepted for M1
- Date: 2026-08-16
- Amends: [ADR 0002](0002-result-slots.md) (slot-to-run binding; completion as an intrinsic)

## Context

M1 requires fixing the C# API names and generic shapes after comparing
variants on real usage examples. Three result-slot exposure shapes (an `out`
parameter, a typed carrier returning a tuple, and post-hoc exposure by name)
were built as compile prototypes against the real Abstractions assembly, six
representative programs each, with negative probes capturing verbatim
compiler behavior. Every decision below cites that evidence rather than
taste.

## Decisions

### 1. Stream types and factories

`Source<T>`, `Flow<TIn, TOut>`, `Sink<T>`, non-generic `RunnableGraph`,
`ResultSlot<T>`, `RunHandle`, `PipelineDefinition` — as in ADR 0002.
`RunnableGraph<TResult>` was probed and rejected: it shortens exactly one
program, collapses to `Keep.Left/Right`-style tuple threading the moment a
graph has two results, and does not prevent the cross-graph slot mistake it
would exist to prevent (two `RunnableGraph<long>` values stay
interchangeable).

Factories live on non-generic companion classes: `Source.From(...)`,
`Flow.For<T>()`, `Sink.Ignore<T>()`, `Sink.Aggregate(...)`. `Flow.For<T>()` and
`Flow.Create<T>()` are inference-identical (the type argument is required
either way, being return-position-only), so the name is chosen for
readability next to `Source.From`.

### 2. Operators and `To` are instance methods

Instance methods turn a wrong-element-type mistake into an actionable
`CS1503` ("cannot convert `Sink<string>` to `Sink<OrderCreated>`") where
extension methods produce an unhelpful `CS0411`, and they keep the whole
vocabulary in IntelliSense. The tuple-returning `To` overload is unreachable
behind an instance `To` when spelled as an extension, silently dropping the
result — one more reason everything is instance.

### 3. Result exposure: carrier plus mandatory slot name (the hybrid)

`Sink.Aggregate` and every result-bearing sink factory return
`SinkWithResult<TIn, TResult>`, usable wherever `Sink<TIn>` is accepted only
by explicit conversion that discards the result. `To` overloads:

```csharp
RunnableGraph To(Sink<T> sink);
(RunnableGraph Graph, ResultSlot<TResult> Slot) To<TResult>(SinkWithResult<T, TResult> sink, string slotName);
RunnableGraph To<TResult>(SinkWithResult<T, TResult> sink, string slotName, out ResultSlot<TResult> slot);
```

- The mandatory `slotName` (validated as a `ResultSlotId`) separates the
  result-bearing overloads by arity, so dropping a result is an explicit
  one-argument `To(sink)` rather than a silent overload accident, and slot
  names are author-stable durable identities instead of positional
  `result-1` machine names that repoint when a stage is inserted.
- The `out` overload is the fluent form (compile-proven usable in `await`
  argument lists, switch arms, ternaries, initializers, LINQ, and async
  method bodies); the tuple overload is the composable form that survives
  `async` signatures, collections, and interfaces, where `out` is banned
  (`CS1988`, `CS1623`, `CS8198`). F# consumes the `out` form as a natural
  tuple; the C# `ValueTuple` names do not survive into F#, so both forms
  earn their place.
- Sink-factory lambda overloads fix the inference hole where
  `Sink.Aggregate(0L, (count, _) => ...)` cannot infer its element type
  (`CS0411`; partial type-argument lists remain unsupported, `CS0305`):

```csharp
RunnableGraph To<TResult>(Func<SinkFactory<T>, SinkWithResult<T, TResult>> sink, string slotName, out ResultSlot<TResult> slot);
// orders.Via(normalize).To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out var processed);
```

  compiles with zero type arguments and zero lambda annotations, because the
  element type is pinned by the source and the result type flows from the
  lambda's return. Overload resolution between result-bearing and plain
  factory lambdas was probed and is unambiguous.

### 4. Slot-to-run binding is the graph fingerprint

ADR 0002 bound a slot to "the graph identity, revision, and import scope it
was materialized from". The prototypes proved that check vacuous for the
common local case: two anonymous graphs built by the same code share their
identity, so a slot of one resolves against a run of the other silently.

Amendment: a slot binds to the `GraphFingerprint` of the document that
declared it, and any structural difference changes the fingerprint so the
resolution fails loudly.

Implementation of the first authoring slice exposed a second gap: the
fingerprint identifies shape, not behavior. A lambda graph's document never
records what its delegates compute, so two graphs of one shape share a
fingerprint even when one counts and the other sums. A slot of a
`nondeployable` graph therefore additionally binds to a per-instance
authoring nonce allocated when the graph is closed; resolving a slot against
a run of a different instance fails loudly even when the shapes agree. The
nonce never enters the document and does not affect serialization or
fingerprints. Registered stages carry their identity and parameters in the
document, so slots of named deployable pipelines will bind by fingerprint
and `GraphId`-plus-revision lineage without an instance nonce, once those
exist.

### 5. Completion and shutdown are `RunHandle` intrinsics

Every run completes and every run can be shut down; these are properties of
the run, not stage-produced values, and no author should have to declare or
name them. `RunHandle` exposes them directly (completion awaiting, shutdown/
kill-switch, `IAsyncDisposable`). Document result slots are reserved for
stage-produced results and controls. This narrows the examples listed in
ADR 0002; monitors and metrics snapshots will pick a side when the runtime
milestone implements them.

### 6. Identity policy for authoring

- Explicitly named occurrences (`ResultSlotId` names, user-supplied stage
  ids) are the deployable path, per ADR 0001.
- Unnamed occurrences get deterministic sequential local ids in authoring
  order (`stage-1`, `stage-2`); the closed document then carries the
  `ephemeral-identity` capability token automatically, because positional
  ids are not edit-stable. Deployability validation rejects
  `ephemeral-identity` documents for durable pipelines.
- Lambda-implemented stages additionally carry `nondeployable` (ADR 0001):
  the delegate lives in the authoring value and is bound at local
  materialization; it never enters the document. The two tokens are
  orthogonal: a fully named graph of lambdas is `nondeployable` but not
  `ephemeral-identity`; a pipeline of registered stages with unnamed
  occurrences is the reverse.

### 7. Naming

LINQ names where LINQ semantics match (`Select`, `Where`, `SelectMany`,
`Take`, `SkipWhile`, `Distinct`); unambiguous streaming names where they do
not (`TakeThrough`, `GroupedWithin`, `Throttle`, `Buffer`, `RecoverWith`);
explicit async families (`SelectAsync` ordered, `SelectAsyncUnordered`,
`SelectValueTaskAsync`) taking a `CancellationToken`-accepting callback;
per-concern option records, never one options bag.

## Consequences

- `SinkWithResult<TIn, TResult>` joins the public vocabulary; the F#
  frontend maps it to its own typed carrier without threading a generic
  through stream shapes.
- The wrong-element-type diagnostic is a conversion error naming both types.
- `GraphFingerprint` becomes part of the authoring/runtime contract, not
  only the serialization layer.
- The flagship examples in C-SHARP-API.md are updated to the compiling
  forms; the old fold example is kept there as a named counter-example.

## Not covered

- `SelectAsync`/options-record shapes and the operator catalog beyond names
  (M2, with the runtime that executes them).
- The registered-stage authoring surface for deployable pipelines and
  `PipelineDefinition` creation (later M1 checkpoint, on top of this
  baseline).
- IDE discoverability was argued from instance-method mechanics, not
  measured with real IntelliSense.
- The prototypes modeled fragments as toys; nothing here validates the real
  fragment algebra's ergonomics, which the first real authoring checkpoint
  will.
