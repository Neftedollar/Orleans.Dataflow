# Fragment algebra

- Status: M1 implementation specification (language-neutral authoring core)
- Depends on: [ADR 0001](../architecture/0001-definition-runtime-authoring-planes.md), [ADR 0002](../architecture/0002-result-slots.md), [DEFINITION-MODEL.md](DEFINITION-MODEL.md)

The fragment algebra is the language-neutral machinery both authoring
frontends compile into. C# fluent types (M1) and F# modules (M7) are facades
over it; it produces `GraphDocument` values and nothing else executes here.

## GraphFragment

An immutable value describing a partial graph:

```text
GraphFragment
  Nodes        stage occurrences with fragment-local NodeIds
  Edges        connections internal to the fragment
  OpenInputs   ordered list of PortAddress: input ports not yet connected
  OpenOutputs  ordered list of PortAddress: output ports not yet connected
```

Invariants (enforced at construction, GraphDocument-style aggregate
reporting):

1. structural rules of the definition model apply to `Nodes`/`Edges`
   (unique node IDs, declared endpoints, no self-loops, single edge per port
   address);
2. every open port references a declared node and is not also an edge
   endpoint;
3. open-port lists are duplicate-free;
4. a fragment is never empty.

Linear shapes are special cases: a source fragment has no open inputs and
one open output; a flow fragment one of each; a sink fragment one open input
and no open outputs. The algebra itself is shape-agnostic; junction shapes
(M4) reuse it unchanged.

## Fragments carry no result slots

`ResultSlotId` is a single segment and cannot be path-rebased the way
`NodeId` can, and ADR 0002 binds a slot to an occurrence in a graph, never
to a reusable value. Both point the same way: a reusable fragment declares
result *ports* implicitly through its stages' specifications, and named
slots are declared only when a graph is closed, referencing the (possibly
scoped) producer address. The "import scope" in ADR 0002's slot-validity
rule is the scope embedded in the producer's `NodeId`.

Consequence recorded for M4: importing an already-closed graph (pipeline as
a branch) must decide what happens to its slot declarations; that decision
is out of M1 scope and is not prejudged here.

## Composition

- `Import(fragment, scopeSegment)`: rebases every fragment-local `NodeId`
  (and every port address and open port with it) below the scope via pure
  prefixing (`NodeId.InScope`). Importing one fragment twice under two
  scopes yields disjoint node sets; nesting composes by nesting prefixes.
  Deterministic: equal inputs, equal outputs.
- `Connect(a, aOutputIndex, b, bInputIndex)` (conceptual form): joins one of
  `a`'s open outputs to one of `b`'s open inputs with a new edge; the result's
  open ports are the remaining ones in stable order. Linear convenience
  (`append`) joins the single open output to the single open input.
- `Wire(fragment, output, input)`: joins one of a fragment's own open outputs
  to one of its own open inputs. `Connect` merges two fragments and can
  therefore never join a fragment to itself, which is exactly the edge two
  legal document shapes need — a re-convergence, where a stream split by one
  junction is rejoined by another, and a cycle, whose relieving edge runs back
  into a node already there. Neither is reachable by folding `Connect`, so the
  operator is part of the algebra rather than a private path around it. It
  judges nothing about direction: a fragment cannot see which stage is upstream
  of which, so wiring an output back into an input the stream already passed
  through builds a cycle deliberately. What it checks is what a fragment can
  check — both ports open, both declared, and no self-loop (added by M4.2, with
  the junction authoring surface).
- `Close(...)`: a fragment with no open ports, plus graph identity, revision,
  capabilities, and slot declarations, becomes a `GraphDocument` through
  `GraphDocument.Create` (which revalidates everything).

Composition never mutates inputs and never executes work.

## Identity policy

Stage occurrences need `NodeId`s at authoring time. Two modes:

- **explicit**: the author names the occurrence (required for durable,
  stateful, or side-effecting stages per ADR 0001);
- **ephemeral**: the frontend allocates deterministic sequential local IDs
  (for example `stage-0001`, `stage-0002` in authoring order, zero-padded so
  ordinal document order equals authoring order). Positional IDs are
  not edit-stable, so a graph containing any ephemeral occurrence is not
  deployable as a durable pipeline.

How ephemerality is represented (a dedicated capability token such as
`ephemeral-identity`, or folding into `nondeployable`) is an ADR 0004
decision made together with the C# API baseline; the algebra only requires
that closing a graph knows whether every identity is author-stable.

## Not in M1

- junction shapes with more than one open port per side (the model supports
  them; `GraphFragment.OfStage` exposes them since M4.2, which is how the C#
  junction surface opens a broadcast's legs and a merge's inputs);
- importing closed graphs as fragments;
- any operator vocabulary (operators are stage specifications plus authoring
  sugar; the algebra does not know operator names).
