# Glossary

Every term Orleans.Dataflow uses, in plain language. The product is small in
surface and large in vocabulary, so this page exists to be read once and
returned to often. Terms are grouped by the thing they belong to; each group
reads as a short story, and the index below jumps into it.

**Index.** [activation](#activation) ·
[at-least-once](#at-least-once) · [attempt](#attempt) ·
[backpressure](#backpressure) · [balance](#balance) ·
[boundary](#boundary) · [branch](#branch) · [broadcast](#broadcast) ·
[buffer](#buffer) · [cancellation](#cancellation) ·
[canonical JSON](#canonical-json) · [catalog](#catalog) ·
[checkpoint](#checkpoint) · [checkpoint store](#checkpoint-store) ·
[claim](#claim) · [combine-latest](#combine-latest) ·
[completion](#completion) · [concat](#concat) · [coordinator](#coordinator) ·
[cursor](#cursor) · [demand](#demand) · [drain](#drain) ·
[durable run](#durable-run) · [element](#element) · [ending](#ending) ·
[epoch](#epoch) · [ETag](#etag) · [fencing](#fencing) ·
[fingerprint](#fingerprint) · [Flow](#flow) · [fork](#fork) ·
[fragment](#fragment) · [grain](#grain) · [graph document](#graph-document) ·
[group-by](#group-by) · [interleave](#interleave) · [junction](#junction) ·
[local stage](#local-stage) · [mark](#mark) · [materialize](#materialize) ·
[merge](#merge) · [node](#node) · [occurrence](#occurrence) ·
[ordered / unordered](#ordered--unordered) ·
[overflow policy](#overflow-policy) · [parallelism](#parallelism) ·
[partition](#partition) · [pipeline](#pipeline) ·
[poison element](#poison-element) · [pull](#pull) ·
[registered stage](#registered-stage) · [replay window](#replay-window) ·
[result slot](#result-slot) · [revision](#revision) · [run](#run) ·
[run handle](#run-handle) · [run identity](#run-identity) ·
[RunnableGraph](#runnablegraph) · [shutdown](#shutdown) · [silo](#silo) ·
[Sink](#sink) · [Source](#source) · [stage](#stage) · [stage kind](#stage-kind) ·
[supervision scope](#supervision-scope) · [terminal](#terminal) ·
[unzip](#unzip) · [window](#window) · [zip](#zip)

---

## What flows: elements and demand

### element

One value travelling through a pipeline. An element is whatever type you are
working with — an order, a line of a file, a message — and the library never
inspects it. Elements move one at a time unless something in the pipeline says
otherwise.

### demand

A request for one more element, travelling *backwards*. Nothing in a pipeline
produces because it feels like it; everything produces because something
downstream asked. This is why a pipeline cannot flood itself.

### pull

The direction the engine works in. The terminal asks the stage before it, which
asks the stage before that, all the way back to the source. The source produces
one element in answer to one request. This is called *strict pull*, and it is
the default: without a [boundary](#boundary), exactly one element is in flight
in a pipeline at any moment.

### backpressure

What you get for free from [pull](#pull): a slow sink slows the source, because
the source only ever produces what was asked for. You never configure
backpressure — you configure the places where it is deliberately relaxed
(see [boundary](#boundary)).

### boundary

A place where more than one element may be in flight at once. There are exactly
three kinds, and each is something you asked for in the graph:

- a [buffer](#buffer), which holds up to its declared capacity;
- an asynchronous stage, which keeps up to its declared
  [parallelism](#parallelism) in flight;
- a [junction](#junction), which holds an element until every leg has taken it.

The rule this gives you is worth memorising: **a pipeline's memory is bounded by
what its boundaries declare, not by how long the stream is.** Ten elements or
ten million, the live heap is the same.

---

## What you build: sources, flows, sinks

### Source

The beginning of a pipeline: something that produces elements. `Source<T>` is
also a *value* — building one runs nothing, allocates no connection, and starts
no work. You can hold it, pass it around, and use it twice.

### Flow

A step in the middle: `Flow<TIn, TOut>` takes elements of one type and produces
another. Also a value, also reusable. A flow with nothing attached to either end
is a legitimate thing to keep in a variable and use in three pipelines.

### Sink

The end: something that consumes elements. Some sinks produce a value when the
stream ends — a count, a fold, a collected list — and that value arrives through
a [result slot](#result-slot).

### terminal

The engine's word for a sink. You will meet it in error messages and in the
provider SDK. A terminal is a fold: a seed, a step applied to each element, and
an optional finish.

### stage

One step in a pipeline, of any kind — a source, a flow, a sink, or a junction.

### stage kind

*Which* step: `select`, `where`, `buffer`, `group-by`, `broadcast`, and about
seventy more. A kind is a name in the [graph document](#graph-document), not a
CLR type, which is what lets a pipeline written in C# be understood by a runtime
that has never seen your code.

### occurrence

One *use* of a stage in one graph, with a name. Two `Select` calls in one
pipeline are two occurrences of one kind. Occurrence names are what make graphs
comparable and errors legible: a failure names the occurrence, not "the third
lambda".

### node

An occurrence as it appears in the [graph document](#graph-document): an
identifier, the stage it refers to, and its parameters.

---

## What a pipeline *is*: documents and identity

### graph document

The description of a pipeline as data — a JSON document naming stages, their
connections, and their numeric parameters. It never contains a delegate, a
closure, a CLR type name, a connection string, or a grain reference. This is the
single most important design fact in the library: **the description of a
pipeline and the code that runs it are separate things**, which is what lets a
pipeline be fingerprinted, stored, sent to another process, and continued after
the process that authored it is gone.

### canonical JSON

The one spelling of a document. Members are ordered, numbers have one form,
whitespace is fixed. Two graphs that mean the same thing produce the same bytes
— which is what makes the [fingerprint](#fingerprint) meaningful.

### fingerprint

The SHA-256 of a document's canonical bytes. Two pipelines with the same
fingerprint are the same pipeline, whatever language they were authored in. A
[durable run](#durable-run) uses it to refuse to continue under a document it
was not written for.

### RunnableGraph

A closed graph: a source, its stages, and a sink, complete and ready to run. It
is an immutable value. If any of its stages carries a lambda, the graph is
*nondeployable* — it can run in your process, and it cannot be sent to a
[silo](#silo), because a lambda cannot travel.

### pipeline

A `PipelineDefinition`: a `RunnableGraph` that has been given an identity and a
[revision](#revision) with `AsPipeline(id, revision)`, and which has passed the
check that everything in it can be resolved by name rather than by delegate. A
pipeline is what a cluster can run.

### revision

A number you increase when a pipeline's shape changes. It sits beside the
identity, and it is what a durable run compares against when deciding whether
the thing it stored still describes the thing you are asking for.

### local stage

A stage carrying your delegate. It runs in your process and marks its graph
nondeployable. Every `Select(x => ...)` is one.

### registered stage

A stage a host knows *by name*, registered at startup with the code that
implements it. Pipelines built from registered stages carry no delegates and can
therefore run on a silo. Registering is how you take something you wrote and
make it deployable.

### catalog

The set of stages one host knows, and the fingerprint over that set. A host
refuses a pipeline naming a stage it cannot resolve — before running anything,
by name, rather than failing halfway through.

### fragment

A reusable piece of a graph, composed into larger graphs. Identities inside a
fragment are rebased when it is imported, so importing the same fragment twice
gives you two independent copies rather than a name collision.

---

## What happens when you run it

### materialize

Turn a graph into a running thing. `MaterializeAsync` is the only call that
starts anything: everything before it was value construction.

### run

One execution of one graph. Materializing the same graph twice gives you two
runs that share nothing.

### run identity

The name of a run. For an ordinary run the library generates it. For a
[durable run](#durable-run) *you* choose it, and it is the unit of continuation
— two materializations under one name are one run.

### run handle

The control surface of a run: how it ended, what it produced, and how to stop
it. `RunHandle` locally, `OrleansRunHandle` in a cluster. It is also
`IAsyncDisposable`, and disposing it stops the run and waits for it.

### result slot

A named, typed place where a sink's value appears. You declare it while
authoring (`out ResultSlot<long> processed`) and read it from the run handle
(`await run.GetValueAsync(processed)`). Slots are how a pipeline returns
something without the author holding a reference to the sink's internals.

### completion

The run's outcome as an awaitable task. Awaiting it makes the run's failure your
own: a failed run's completion faults with the exception a stage threw.

### ending

The run's outcome as a *value* rather than as a task outcome: `Completed`, or
`Failed` with a type name and message. Reached through `WatchTermination`, which
is the surface for a monitor that wants to react to endings rather than to
inherit them. Note that there are only two endings — a cancelled run has no
ending, because cancelling abandons a run rather than finishing it.

### shutdown

"Stop producing, and let what is already in flight finish." A shutdown drains:
an aggregate resolves its slot with what it accumulated, and the run's
completion reports success.

### cancellation

"Stop now." Nothing is drained, slots do not resolve, and the completion reports
cancellation. Cancellation is the opposite half of the pair from
[shutdown](#shutdown), and the difference between them is the difference between
"we are done" and "abandon this".

### drain

What a [shutdown](#shutdown) does: the source stops being asked for elements,
and everything already admitted travels to the terminal.

---

## Branching: junctions and multiport shapes

### junction

A stage with more than one input or more than one output. Junctions are where a
pipeline stops being a line and becomes a graph. Each junction holds an element
until every leg it owes has taken it, which is why a junction is a
[boundary](#boundary).

### broadcast

One input, several outputs, **every** element to **every** leg. Use it when two
things must both see the whole stream.

### balance

One input, several outputs, **each** element to **exactly one** leg — whichever
is ready. Use it to spread work.

### partition

One input, several outputs, and *you* choose the leg with a routing function.
Use it when the leg matters: by region, by tenant, by priority.

### merge

Several inputs, one output, elements in whatever order they arrive.

### concat

Several inputs, one output, the first leg's elements entirely before the
second's. Use it when order across legs is the point.

### interleave

Several inputs, one output, taking a declared number from each leg in turn.

### zip

Several inputs, one output, one element from *each* leg combined into a row. A
zip advances at the speed of its slowest leg by construction.

### combine-latest

Several inputs, one output, emitting whenever any leg produces, combined with
the most recent element from every other leg.

### unzip

One input carrying a composite, several outputs carrying its parts.

### branch

An authoring shape: a path from a junction's output that ends in a sink. You
build branches when you need a junction, and they close the shape.

### fork

An authoring shape: a junction output that carries on as a flow rather than
ending immediately.

---

## Bounds: buffers, windows, keys

### buffer

A declared holding place between two stages. `BufferOptions` names its capacity
and what to do when it fills. A buffer is the ordinary way to let a fast
producer and a slow consumer coexist without unbounded memory.

### overflow policy

What a full [buffer](#buffer) does. Five answers, and you choose:

| Policy | What happens |
|---|---|
| `Backpressure` | The producer waits. Nothing is lost. |
| `DropOldest` | The oldest held element is discarded to make room. |
| `DropNewest` | The arriving element is discarded. |
| `DropBuffer` | The whole buffer is discarded. |
| `Fail` | The run fails with `BufferOverflowException`. |

Four of the five lose elements, which is the point of stating them: dropping is
a decision you make out loud rather than a thing that happens to you.

### parallelism

How many asynchronous callbacks may be in flight at one time
(`ParallelismOptions.MaxConcurrency`). It is a bound, not a target: the engine
never runs more, and runs fewer when there is less to do.

### ordered / unordered

Two forms of asynchronous mapping. Ordered emits results in the order their
elements arrived, which means a finished result waits for the ones before it.
Unordered emits each result as soon as it exists. Ordered is the default choice;
choose unordered when the callbacks vary a lot in duration, or when they wait on
each other.

### window

A bounded group of consecutive elements — by count (`Grouped`), by count and
time (`GroupedWithin`), by a weight you compute (`GroupedWeightedWithin`), or
overlapping (`Sliding`). A window is a bound: the memory a grouping stage holds
is one window.

### group-by

Splitting a stream into substreams by a key, each substream running the same
flow. Because a substream costs memory, the number of *simultaneously live* keys
is declared (`MaxActiveKeys`) and exceeding it is a decision you make in advance:
fail the run, or evict the least recently used key.

---

## Failure: supervision and poison

### supervision scope

A region of a pipeline you have declared a failure policy for: how many
attempts, how long to wait between them, and what to do when they run out. A
stage inside a scope that throws is retried according to the scope rather than
failing the run.

### attempt

One try of a supervised element. `MaxAttempts: 3` means the original plus two
retries.

### poison element

An element that used up every attempt its scope allowed. What happens next is
what the scope declared: fail the run, drop the element, or replace it with a
fallback value. Poison elements are counted, and the count is on the run's
snapshot.

---

## Durability: checkpoints and continuation

### durable run

A run named by you that survives the death of the process running it. Its
progress is written to a [checkpoint store](#checkpoint-store), and a later
materialization under the same name continues it rather than starting a second
one.

### checkpoint

A stored position: where each source had got to, what each stateful stage held,
and what each sink had finished with. A checkpoint is taken on a cadence you
declare — every so many elements, every so long, or both — and taking one holds
the run for its duration.

### checkpoint store

Where checkpoints go: an `ICheckpointStore` you implement over any document
store that can refuse a stale writer. Three duties: a write is atomic per
document, a write is a compare-and-swap on the [ETag](#etag), and a clear is
destructive. The second is the one that carries the weight — it is how a
superseded process is stopped from overwriting a live one.

### ETag

The opaque version string a store gives a checkpoint. A writer presents the one
it last saw; a store whose stored version has moved on refuses the write. Never
compare ETags for order — only for equality.

### cursor

What a source says about where it had got to, so that a resume can reopen there.

### mark

What a sink says about what it had finished with, so that a resume knows what
not to redo.

### replay window

The elements between the last stored [checkpoint](#checkpoint) and the moment
the process died — the ones a resumed run delivers a second time. You size it by
choosing the checkpoint cadence: more frequent checkpoints, smaller window,
higher cost.

### at-least-once

The delivery guarantee between checkpoints. Not exactly-once, and the library
says so rather than implying otherwise: after a crash, everything in the
[replay window](#replay-window) is delivered again. Sinks that must not repeat
work need to be idempotent, or to use an adapter whose row in the adapter
reference states a stronger guarantee.

---

## In a cluster

### silo

One Orleans server process. Registering the library on a silo is what lets
pipelines run there.

### grain

Orleans' unit of addressable, single-threaded state. The library uses grains for
the pieces of a run that must exist exactly once.

### activation

One in-memory instance of a grain. Orleans creates and recycles activations as
it sees fit; a run whose activation is recycled is either continued (durable) or
reported lost (ordinary).

### coordinator

The grain that owns a pipeline's identity: it validates documents, issues
[epochs](#epoch), remembers durable-run declarations, and records how runs
ended.

### epoch

A number ordering *claims to ownership* of a durable run. Every attempt carries
one; the coordinator issues them; a higher one supersedes a lower one.

### fencing

Refusing an operation because it carries a stale [epoch](#epoch) or a stale
[ETag](#etag). Fencing is how a process that has been superseded — because its
silo died and the run moved — is stopped from writing over the process that
replaced it.

### claim

Reading what a durable-run name holds, without taking it. Reading fences nobody.

---

## Reading it back

### snapshot

One reading of a run's observable state: its status plus five counters —
elements dropped, supervised failures, poison elements, checkpoints written, and
total time spent holding the run for checkpoints. Not a consistent cut across
the whole run; each number is exact on its own.

### meter and span

The OpenTelemetry names the library publishes under, both called
`Orleans.Dataflow`. Metrics count runs, drops, supervised failures, poison
elements and checkpoints; spans cover materialization and each run.
