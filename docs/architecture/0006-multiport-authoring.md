# ADR 0006: Multi-port authoring for C#

- Status: Accepted for M4
- Date: 2026-08-18
- Depends on: [ADR 0004](0004-csharp-api-baseline.md) (the linear baseline
  this extends and every rule it carries), [ADR 0005](0005-junction-semantics.md)
  (the junction contracts these spellings close over)
- Method: nine compile prototypes against the real assemblies, the ADR 0004
  way — the compiler reviewed every candidate before this document did

## Context

M4.1 gave the engine nine junctions and cycles; nothing can author them
except hand-built documents. The question is the C# spelling, and the
cautionary tale is Akka's GraphDSL: a builder with port objects that its
own users avoid. The goal is the opposite — the common shapes read as
sentences on the existing `Source`/`Flow`/`Sink` values, and the full
generality of the definition plane stays reachable through the fragment
algebra rather than through a second DSL.

## The finding that shaped everything

Type information flows left to right, from sources. A junction's branch is
built right to left — a sink, the flow ahead of it — so a free-standing
branch has no receiver to carry its input type, and the sink-factory
lambdas ADR 0004 chose collapse into CS0411 the moment they leave a
source's shadow. The prototypes proved there is exactly one honest anchor:
the branch starts with `Flow.For<T>()`, the identity flow that already
exists, and its one explicit type argument stands where a reader wants the
branch's input type stated anyway. Everything after that anchor infers —
factory members, aggregate seeds, slot types.

## Decision: the surface

**A branch is a value.** `Branch<TIn>` is a sink-terminated continuation —
everything one junction leg feeds — built by a `To` family on
`Flow<TIn,TOut>` mirroring ADR 0004's on `Source<T>`: plain sinks, sink
factories, and result-bearing sinks with a mandatory slot name and an
`out ResultSlot<TResult>`. Out-parameters are legal exactly because
branches are built at top level, as arguments, never inside configuration
lambdas — the prototype rejected every closure-shaped alternative on that
rule alone.

**Fan-out is a terminal call on the source.** `BroadcastTo`, `BalanceTo`,
`PartitionTo(router, …)`, and `UnzipTo(left, right)` take branches and
close the graph, the way `To` closes a chain; `AlsoTo(branch)` is the tap
— broadcast sugar that keeps the main line flowing. Branch order is
argument order and is identity-bearing: reordering arguments reorders
auto-assigned node identities, exactly as reordering a chain does.

**Fan-in is a combinator on sources.** `a.Merge(b)`, `a.Concat(b)`,
`a.Interleave(b, segmentSize)`, `a.Zip(b)` (tuple) and
`a.Zip(b, combine)`, `a.CombineLatest(b, combine)` — each returns a
`Source` and the chain continues. Inference held everywhere without a
single explicit type argument.

**The diamond is a carrier.** `source.Fork(left, right)` broadcasts one
stream through two flows and returns `Fork<T1,T2>`, whose `Zip()` /
`Zip(combine)` rejoin positionally — legal bufferless, which checkpoint 3
proved is the one join whose contract matches a broadcast's — and
`source.ForkMerge(left, right)` is the unordered rejoin for
race-and-take-first shapes. A tree cannot express re-convergence any other
way, which is why the carrier exists and why nothing else needs it.

## Decision: what stays out

- **Cycles have no fluent spelling.** A loop is authored through the
  fragment algebra, where edges are explicit; a fluent cycle would hide
  the one thing the cycle rule needs an author to see — the relieving
  boundary. Revisited only if real usage produces a recurring shape.
- **No graph-builder DSL.** Nine programs covered every junction without
  one; the algebra remains the escape hatch for arbitrary topology.
- **N-ary spellings stay small.** Two- and three-input overloads; wider
  graphs chain (`a.Merge(b).Merge(c)`), and the chain is honest about
  being two nodes — merge semantics are associative, but the two
  documents are distinct and fingerprint differently, which is stated
  rather than papered over with a flattening rewrite.
- **Junction variant knobs** (eager-complete merge, eager-cancel
  broadcast) arrive as the declared modes ADR 0005 named, not as
  parameters of these spellings, when a use case asks.

## Consequences

- The registered-stage branch forms (`Flow.FromRegistered(...)` chains
  ending in registered sinks) ride the same `To` family; the
  implementation adds the overloads, not a new shape.
- The prototypes live on as compile tests: the nine programs move into
  the test suite verbatim, so the inference this ADR claims is a build
  break if it regresses.
- `Flow.For<T>()` graduates from convenience to load-bearing: it is the
  branch anchor, and its doc says so.

## Implementation notes (M4.2)

Recorded here rather than discovered later; each is a place the built
surface says more than this document did.

- **Where the prototypes live.** `JunctionPrograms` in the C# test project
  holds all nine verbatim, and they are asserted twice:
  `JunctionAuthoringTests` reads the document each one closes, and
  `FluentJunctionTests` runs them and asserts the results. Their compiling
  with no explicit type argument is this ADR's inference claim, and it is
  now a build break.
- **Instance methods, not extensions.** The prototypes had to be extension
  methods — they could not add members to the real assemblies — and the
  built surface makes every junction call an instance method, per ADR 0004
  section 2. The one exception is `Source.UnzipTo`, whose receiver is
  constrained to a source of pairs, which is exactly what an instance
  method cannot say. Call sites are identical either way.
- **A branch's slot exists before its graph does.** The `out
  ResultSlot<TResult>` above is assigned one expression before the junction
  call closes a graph, so there is nothing to fingerprint yet. The slot is
  therefore bound when the junction call closes the graph: reading it
  before then throws, and a branch that declares a result closes exactly
  one graph — a second junction call is refused rather than repointing the
  first graph's slot. A branch that declares no result stays reusable
  without limit.
- **The fragment algebra gained one operator.** `Connect` merges two
  fragments and can therefore never join a fragment to itself, which is
  exactly the edge a diamond's rejoin and a cycle's relieving edge are.
  `GraphFragmentComposer.Wire` joins an open output to an open input of one
  fragment; it is what closes a fork's rejoin here, and it is the
  explicit-edge path this ADR sends a loop to.
- **A junction is a local stage, and it costs the graph both tokens.** A
  graph with a junction declares `nondeployable` and `ephemeral-identity`
  even when its source, its flows, and every branch sink are registered
  stages under names the author chose, because the junction occurrence is
  local and unnamed. Its ports declare `local-opaque@v1`, so each edge
  between it and a registered stage is an `element-contract-mismatch` seam.
  **A fan-out pipeline built entirely from registered stages is therefore
  not expressible until a provider can register a junction**, which is the
  provider SDK's business and not this one's.
