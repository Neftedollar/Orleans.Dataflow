# ADR 0002: Materialized values are typed result slots

- Status: Accepted
- Date: 2026-08-16
- Amends: [ADR 0001](0001-definition-runtime-authoring-planes.md) (materialized-value shape)

## Context

ADR 0001 deliberately left open whether materialized values thread through
authoring types as an extra generic parameter (`Source<TOut, TMat>`,
Akka-style) or live behind typed named slots resolved from a run handle. The
product direction of 2026-08-16 resolves this in favor of slots. The frontend
order is unchanged: C# is the first public frontend, and the F# frontend
follows as a separate package over the same core.

## Decision

Authoring vocabulary (C# spelling; the future F# frontend uses the same
shapes):

```text
Source<T>            reusable description of where elements enter a graph
Flow<TIn, TOut>      reusable typed transformation
Sink<T>              reusable terminal consumer
RunnableGraph        closed, validatable graph; not generic over results
PipelineDefinition   named, versioned deployable definition
ResultSlot<T>        typed declaration of one materialized result or runtime control
RunHandle            resolves the results and controls of one materialized run
```

Stream-shape types carry element types only. Materialized results and runtime
controls are declared as typed named slots and resolved per run:

```csharp
ValueTask<T> result = runHandle.GetValueTask(resultSlot);
```

Slot examples: completion, fold result, first/last element, ingress control,
queue control, hub endpoint, monitor, metrics snapshot, shutdown control.

A `RunHandle` accepts only slots belonging to the graph identity, revision, and
import scope from which that run was materialized. Slot resolution against a
foreign graph, another revision, or a differently scoped fragment import is an
error, not a best-effort lookup.

## Rationale

- The definition plane already models results as named, versioned
  `ResultSlots` (ADR 0001). Making the authoring plane declare slots directly
  removes a translation layer instead of adding one.
- Threading `TMat` through every combinator forces `Keep.Left`/`Keep.Right`
  style ceremony and hurts C# type inference on long fluent chains; the slot
  model keeps `Via`/`To` signatures minimal, and does the same for the future
  F# modules.
- Durable and distributed runs need stable, versionable result identities
  anyway; object-identity-based materialized values cannot survive a process
  boundary. Slots make the durable path and the local path the same model.
- Arbitrary result-combining delegates cannot be persisted in a durable graph
  (ADR 0001). Slots make the allowed operations (declare, expose, select,
  resolve) explicit rather than restricting a general combinator after the
  fact.

## Consequences

Positive:

- one materialization model serves local, durable, and distributed runs;
- simpler public signatures in both frontends;
- reusing a fragment twice yields distinct slots via import scoping, which the
  identity model already guarantees.

Costs:

- linear compositions that want a sink result need explicit slot exposure or a
  small amount of sugar; Akka's `RunWith(sink).Result` one-liner has no
  literal equivalent;
- slot/identity validation is a new mandatory runtime check;
- Akka idioms such as `MapMaterializedValue` translate to slot-level
  operations, not element-level ones, and the capability matrix must track the
  translation honestly.

## Not covered by this ADR

- The exact C# surface by which linear composition exposes a sink's result
  slot (paired return value, typed result key on the sink descriptor, or a
  carrier type) is an M0 API-baseline decision, to be settled with compile
  prototypes.
- Checkpointing and durability of slot values across failover are M3
  concerns.
- Whichever exposure shape wins, reusing one sink value twice in a graph must
  yield two distinguishable slots, and no shape may bind a slot to a sink
  value rather than to its occurrence in a graph.

## Rejected alternative: fully generic `Source<TOut, TMat>` / `Flow<TIn, TOut, TMat>`

Rejected: the extra generic parameter infects every operator signature and both
frontends; the pain concentrates exactly where Orleans.Dataflow differs from
Akka (durable identity, cross-process runs) without buying expressiveness the
slot model lacks.
