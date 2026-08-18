# ADR 0005: Junction semantics for multi-port graphs

- Status: Accepted for M4
- Date: 2026-08-17
- Depends on: [ADR 0001](0001-definition-runtime-authoring-planes.md) (the
  port model this exercises), [LOCAL-RUNTIME.md](../design/LOCAL-RUNTIME.md)
  (the pull engine these semantics must hold in)
- Ratifies: the two M2 deferrals (the `Choose` spelling and the controllable
  time abstraction), recorded at M2's close for exactly this milestone

## Context

M4 brings the junctions: stages with more than one input or more than one
output. The definition model has carried multi-port stages since M0 — port
lists on specifications, edges between port addresses — so the IR needs
only junction-specific validation. What does not exist yet is the semantic
contract: what each junction does to demand, to completion, to failure, to
ordering, and to memory. Those five dimensions are decided here, before any
engine work, because they are the promises the capability matrix will hold
the implementation to, and because reversing one after operators are built
on it would ripple through every layer at once.

The engine these contracts must hold in is the M2 pull engine: segments on
dedicated threads, bounded channels at boundaries, demand as the act of
pulling. A junction in a pull world is a pump with several channels on one
side; nothing here introduces a push path, a mailbox, or an unbounded
buffer, and every junction's memory bound is stated as a count of elements
it may hold outside any declared buffer.

Akka Streams is the parity reference, not the specification. Where Akka's
default is one of several defensible choices, the choice is made for this
engine's model and the difference is named.

## Decision: shared rules first

Five rules hold for every junction, stated once:

1. **Failure wins.** Any input's failure fails the junction, the run, and
   everything downstream, immediately, whether or not that input was the
   one being pulled. This is the engine's existing rule extended to
   fan-in: a failure on a leg nobody is currently reading is still the
   run's failure.
2. **Demand is a pull.** A junction pulls an input only when it has
   downstream demand it can satisfy from that input. No junction reads
   ahead beyond the elements its contract says it holds.
3. **A completed downstream leg stops feeding, not the world.** When one
   output's downstream completes, a fan-out junction stops offering to
   that leg. The junction itself completes upstream only when *every*
   output has completed — the engine's existing `Stopping` propagation,
   counted per leg.
4. **Per-input order is preserved.** No junction reorders the elements of
   one input relative to each other. What a junction promises *across*
   inputs is its own contract, stated per junction.
5. **Pause parks the pump.** A junction's pump parks at the same safe
   points every segment parks at: between elements. Elements a junction
   holds (a zip's partial row, a partition's routed element) are held
   rather than in flight, exactly as buffered elements are.

## Decision: fan-in junctions

| Junction | Emits | Completes when | Holds at most |
|---|---|---|---|
| `Merge` | any input's element, as available | **all** inputs complete | 1 element |
| `Concat` | input 0 to its end, then input 1, … | the **last** input completes | 1 element |
| `Interleave` | K elements per input, round-robin | **all** inputs complete | 1 element |
| `Zip` | one row per element from **each** input | **any** input completes | N−1 elements |
| `CombineLatest` | a row on every arrival, once every input has produced one | **all** inputs complete | N elements |

- **Merge** pulls whichever inputs have demand-worthy elements and emits in
  arrival order at the pump, round-robin among ready inputs so a fast
  producer cannot starve a slow one's elements once they arrive. No
  cross-input order is promised — "arrival order" is an observation, not a
  contract. An eager-complete variant (complete when *any* input does) is a
  declared mode to add with operator breadth if a use case asks; it is not
  the default, because a merge that ends while an input still has elements
  discards them silently.
- **Concat** gives demand only to the active input; inputs behind it are
  not read at all until their turn. Corrected by checkpoint 2's
  measurement rather than left standing: this engine launches every
  segment, so a later input's source *runs* — up to its boundary's
  capacity plus the one element in its hand — and a source that fails at
  open fails the run at once, not at its turn. What concat withholds is
  reads, and what that buys is backpressure on the waiting inputs, not
  deferred startup; a deployment that needs a source untouched until its
  turn expresses that in the source itself, not in the junction.
- **Zip** completes eagerly on the first completed input: a zip missing a
  leg can never emit another row, and holding the other legs open would
  buffer forever for nobody. The N−1 held elements are the partial row
  awaiting its slowest column; there is no other buffering.
- **CombineLatest** keeps Rx semantics, not Akka's `zipLatest` default: it
  completes when **all** inputs complete, freezing a completed leg's last
  value into later rows. The alternative — complete on first completion —
  ends a dashboard the moment its least important feed does, which is the
  opposite of what the operator is for. Emission requires every input to
  have produced at least once; before that, arrivals update state and emit
  nothing.
- **Interleave** takes a declared segment size K per input in fixed
  rotation. When an input completes, rotation continues over the
  remainder. It is `Merge` with determinism bought at the price of
  head-of-line waiting on the input whose turn it is.

## Decision: fan-out junctions

| Junction | Routes | Pulls upstream when | Holds at most |
|---|---|---|---|
| `Broadcast` | every element to every live output | **every** live output has room | 1 element |
| `Balance` | each element to **one** output with room | **any** output has room | 1 element |
| `Partition` | each element to the output its function names | it holds no routed element (reads first — the target *is* the element's) | 1 element |
| `Unzip` | a row's halves to their outputs | **both** outputs have room | 1 element |

- **Broadcast** is slowest-consumer backpressure by construction: the pull
  happens when every live leg can take the element, so one slow consumer
  paces the stream for all. A leg whose downstream completed leaves the
  set (rule 3); when the last leg leaves, the junction completes upstream.
  An eager-cancel variant (first leg's completion cancels the stream) is a
  possible later mode, same reasoning as merge's eager-complete.
- **Balance** distributes work: the element goes to any output with room,
  round-robin among the willing so distribution is fair on an idle
  cluster rather than accidentally sticky. No promise about *which*
  output receives an element is ever made — code that needs routing uses
  `Partition`.
- **Partition** runs the author's routing function once per element (the
  keyed adapter's read-once rule, for the same reason) and then waits for
  that element's target specifically. One routed element is held; every
  other leg starves while it waits, which is head-of-line blocking one
  element deep and is exactly Akka's contract. A routing result outside
  the declared range fails the run — a misrouted element has no honest
  destination, and dropping it silently would be worse. An element routed
  to a leg that has *left* is abandoned rather than failed on, ratified by
  checkpoint 4's measurement: failing there races an ordinary early
  completion, whose walk closes legs while elements still travel toward
  them, and it is what the engine's offer contract already does at every
  closed channel. Rule 3 stands: the junction keeps feeding the legs that
  remain.
- **Unzip** takes a two-part row and is `Broadcast` for halves: both
  outputs must have room before the pull, each receives its half of the
  same row, so the two legs advance in lockstep and can be re-zipped
  downstream without skew.

## Decision: cycles

A cycle is legal exactly when every cycle in the graph passes through at
least one boundary that can hold an element and answer without waiting for
its own downstream — a buffer whose overflow policy is anything but
`Backpressure`. This ADR originally guessed that an explicit delay would
also qualify once the timing operators existed; wave 1 measured the guess
wrong and it is corrected here: a delay holds elements for a time and then
waits for room below exactly as a backpressuring buffer does, so it
relieves nothing, and a cycle whose only boundary is a delay is refused
with the same sentence.
Validation enforces this before execution: a cycle of nothing but
backpressuring edges is refused with the cycle's node path in the
diagnostic, because in a pull engine such a loop is a deadlock by
construction — every pump in it waits for room that only itself can make.
The rule lives with the host's planner rather than the catalog-generic
graph compiler (amended by checkpoint 4): which stage is a relieving
boundary is vocabulary knowledge the compiler deliberately does not have,
and every shape rule since the junctions arrived has lived there for the
same reason. M0's no-self-loop rule is subsumed: a self-loop is a cycle
and gets the same test rather than a special refusal. What ends a legal
cycle is the design doc's corrected contract: a stream-ending stage on
the loop, or a shutdown severing its feedback edges — never the closing
of its external inputs alone, which for a seeded loop is the moment the
computation starts, not the moment it ends.

## Decision: the ratified M2 deferrals

- **`Choose` stays out of the C# surface.** `Where` composed with `Select`
  covers the semantics; a C# `Choose` would need an option type the
  language does not give it, and every candidate spelling (tuple returns,
  out parameters, sentinel nulls) was worse than the composition it
  replaced. The F# frontend gets `choose` over the algebra in M7, where
  the language provides the type that makes it honest. Recorded here so
  the question does not reopen with every operator wave.
- **Time is `TimeProvider`.** The controllable time abstraction is the
  BCL's, not an invention: operators that read a clock (windowing, timing,
  rate, tick, delay) take a `TimeProvider` resolved at materialization,
  and deterministic tests hand the host a controlled one. The decision is
  this small because .NET 8 made it small; the tick source and the first
  clock-reading operators arrive together in M4's operator waves, which
  was the deferral's condition.

## Consequences

- The engine grows a DAG plan: channels keyed by edge rather than by
  chain position, junction pumps with several readers or writers, and
  completion/stopping propagation that walks a graph instead of counting
  down a line. That design lands in LOCAL-RUNTIME.md with the first
  engine checkpoint of M4.1; this ADR fixes what it must implement.
- Every junction's contract above becomes a capability-matrix row's
  proof text when its implementation lands; a junction that cannot keep
  its stated bound or ordering does not ship with a weaker one under the
  same name.
- The declared-mode escape hatches named here (eager-complete merge,
  eager-cancel broadcast) are the pattern for future variants: a new
  behavior is a new declared mode with its own contract, never a silent
  change to an existing one — the same rule that kept the keyed stage's
  ordering knob out of the payload.
