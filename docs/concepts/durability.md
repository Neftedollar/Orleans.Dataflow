# Durability

*What does it mean for a run to survive a crash, and what does it cost?*

An ordinary [run](../reference/glossary.md#run) is a thing in memory. Its
position, its stage state, and its results live exactly as long as the process
holding it, and when that process dies they die with it. A
[durable run](../reference/glossary.md#durable-run) is a run that writes down
where it got to, so a later process can pick it up. This page is about what gets
written down, what writing it costs, what a resume can and cannot promise, and
the numbers you have to choose.

## Why an ordinary run cannot survive

Nothing is hidden here: an ordinary run stores nothing anywhere. There is no
implicit journal, no best-effort recovery, no "we kept the last few elements just
in case". When the process ends, the run ended with it, and on a cluster that is
reported as a specific thing — a lost run, not a failed one, because it never
reached a terminal state and never will.

That is a design choice rather than a limitation to work around. A run that
silently persisted something would make every pipeline pay for a durability
nobody asked for, and would make "is this durable?" a question you could not
answer by reading the code.

## Naming a run

The first thing durability needs is a **name**, and it has to be *yours*.

An ordinary run is named by the host, freshly, per materialization — two
`MaterializeAsync` calls are two runs, and that is exactly right. A durable run
is named by whoever will resume it, because **a resume is the same run
continuing**, not a second run reading the first one's notes. Two durable
materializations under one name are one run: the second hands you a handle to the
run that already exists, or continues it from its stored position if the process
hosting it has died.

A name allocated per attempt would contradict resume outright — nothing would be
able to find the previous attempt's position.

```csharp
private static readonly RunId Run = RunId.Create("orders-of-the-day");

DurableRunOptions Durability() => new()
{
    Store = store,
    RunId = Run,
    EveryElements = everyElements,
};

await using RunHandle attempt =
    await new LocalDataflowHost().MaterializeDurableAsync(crashing, Durability(), cancellationToken);
```

> From the `durable` scenario,
> [`samples/Orleans.Dataflow.Samples/CSharp/Durable.cs`](../../samples/Orleans.Dataflow.Samples/CSharp/Durable.cs).

A locally authored graph has no identity of its own — its document says
`anonymous` — so the run name is what actually separates two checkpoints in a
store. A checkpoint of a *different* local graph stored under the same run name
is caught by the [fingerprint](../reference/glossary.md#fingerprint) rather than
by the key. A deployment that wants the key itself to separate graphs names its
graphs, which is what a [pipeline](../reference/glossary.md#pipeline) is for.

## What a checkpoint contains

A [checkpoint](../reference/glossary.md#checkpoint) is one canonical JSON
document with five members, and all five are always present:

```json
{"cursors":{…},"fingerprint":"sha256:…","marks":{…},"revision":3,"states":{…}}
```

| Member | What it holds |
|---|---|
| `cursors` | Where each source had got to. |
| `marks` | What each sink had finished with. |
| `states` | What each durable scope was holding. |
| `fingerprint` | The document this position belongs to. |
| `revision` | The revision of that document. |

Three of those are **tables keyed by node identifier**, because a node identifier
is the one name a document and a checkpoint of it both agree on. This is where
[occurrence names](graphs-and-identity.md#occurrence-names) stop being a nicety
and become load-bearing.

A run with nothing of a kind writes an empty object rather than leaving the
member out — an absent `marks` would be ambiguous between "this run has no sink
marks" and "this was written by something that had no concept of them".

**Every value inside a checkpoint is canonical JSON.** A cursor's position, a
scope's state, and a sink's mark are each produced by a stage, and each hands over
a canonical value — no object, no CLR type name, no serializer's opinion. That is
precisely what lets one process write a checkpoint another process reads. A stage
that cannot express its state in that form declares nothing and contributes
nothing.

### Cursors

A [cursor](../reference/glossary.md#cursor) is a source saying where it was. Not
every source can say. A source over a list can — its cursor is `{"index":n}` —
and reopening it re-enumerates the very sequence you handed over and skips that
many elements. That puts a requirement on **you**: a sequence that enumerates
differently the second time resumes into different elements, and one shorter than
the stored position fails the resume by name. A source over a list has every
business declaring an index cursor; one over an iterator reading a socket has
none.

The position is advanced when an element has actually **travelled through** the
segment it entered, not when the sequence was asked for the next one — a sequence
learns its element was wanted only when the next is requested, and the moment
between those two is exactly where a hold lands. A cursor counting pulls would be
one behind at every capture.

An Orleans stream source stores the sequence token of the element the run
*delivered*, promoted when the run reports the delivery rather than when the
subscription received it, because a bounded ingress holds elements the run has
not taken yet. Rewinding to a token is **inclusive** of the element that token
names, so a stream cursor's replay window is one element wider than an index
cursor's, and there is no "token plus one" to narrow it with. Sources that
declare no cursor resume from now. [Adapters](../reference/adapters.md) has the
per-source table.

### Marks

A [mark](../reference/glossary.md#mark) is a sink saying what it had finished
with. The order is the whole contract: a committing sink runs its side effect and
**then** advances its mark. A callback that throws leaves the mark where it was,
so the number always describes work that finished. A mark that moved first would
promise a commit that had not happened — and a resume's duplicate window would
become a **loss** window.

A mark counts committed deliveries and is not a source position. The two agree
for a graph that neither drops nor multiplies elements between a source and its
sink, and they part company across a resume, because a replayed element is a
second delivery of one element. Marks are restored on resume, so a run that has
committed eleven elements over two attempts says eleven rather than starting over.

### Durable state

An ordinary stage's state is *not* in the checkpoint. If you want a stage's state
to survive, you put it in a **durable scope**, and you tell the scope how to
write that state down:

```csharp
RunnableGraph Build(int failAt, List<long> seen) =>
    Source.From(Enumerable.Range(1, 10))
        .Select(n => n == failAt ? throw new InvalidOperationException($"the host died at {n}") : n)
        .Durable(Flow.For<int>().Scan(
            0L,
            (total, n) => total + n,
            total => CanonicalJsonValue.Parse(total.ToString(CultureInfo.InvariantCulture)),
            value => value.ToElement().GetInt64()))
        .To(s => s.ForEach(total => seen.Add(total)));
```

Ten elements, a capture every three, the process dying at the eighth. Here is the
document it left behind:

```json
{"cursors":{"stage-0001":{"index":6}},"fingerprint":"sha256:440bc3301abc…","marks":{},"revision":1,"states":{"stage-0003":{"stages":[21]}}}
```

All five members, `marks` empty because this graph has no committing sink, and
the scan's running total — 21, the sum of 1 through 6 — written down as a number
by the codec you supplied.

A durable scope admits a deliberately short list of stages, and the reason is the
one thing it promises. A stage inside one must hand its state over as a canonical
value: a `select` and a `where` hold nothing, a `take` and a `skip` hold a count,
and a `scan` holds a value of a type no document names — so a `scan` exports only
when you supply a codec, which is a pair of delegates and therefore cannot be
stated in a document. Everything else — a distinct, a batch, a sliding window,
the prefix operators — is refused **by name**, because admitting one would produce
a resume that silently reset state the scope had promised to keep.

The two projections must round-trip: applying `restore` to `export`'s answer has
to give back a state the fold would have produced. Nothing checks that, because
nothing could; a codec that does not round-trip restores a state the run was
never in.

## What taking a checkpoint costs

**A capture holds the run for its duration.** Hold, snapshot, write, release —
and nothing overlaps, the store write included. While a checkpoint is being taken
and written, no element moves anywhere in the graph.

That is deliberately the simple answer. Something cleverer is only worth building
once the simple one has been measured, and it *is* measured: the run's
[snapshot](../reference/glossary.md#snapshot) carries `TotalCheckpointHold`, the
sum of every hold on the run's own clock, beside the count of checkpoints
written. If your store is slow, that number is where you find out.

The hold uses the same machinery as `PauseAsync`, which is why the two behave
identically: every segment parks between elements, and an element already
produced and waiting is held rather than in flight.

One subtlety with a practical consequence: a capture due at element *n* does not
complete until element *n+1* has been produced, because a source segment takes
its next step before it parks. On a source that goes quiet, a timed capture stays
open until the next delivery completes the step. It is a delay and not a loss —
the capture that was due is taken as soon as the next element arrives — but a
cadence that must fire on an idle source is not something this library provides.

## Choosing the cadence

Two bounds, and you may declare either, both, or neither.

| Field | Means |
|---|---|
| `EveryElements` | Capture after this many elements have been **admitted** — every element a source of this run hands to the graph, summed across sources. |
| `Interval` | At most this long between two *timed* captures, measured on the run's clock. |

**A run that declares neither never touches the store at all.** That is the
honest reading of "durable options with no timing in them", and it is asserted
rather than assumed. Durability with no cadence is a declaration and not a
promise.

Elements are counted as admitted rather than as committed at a sink, because
committed is a different number for every graph that filters or batches; and
summed across sources rather than per source, because otherwise a two-source
graph's cadence would depend on which source happened to be faster.

The element bound is **exact**. It is reached on the source segment's own thread
and the hold is requested there, before that segment takes another step — so a
source of six elements at a bound of three stores cursor six as a number rather
than as a range. A loop beside the run, waking up when it noticed, would let a
fast source deliver an unbounded number of further elements first.

The trade is simple to state and yours to make with numbers:

| Shorter cadence | Longer cadence |
|---|---|
| Smaller replay window after a crash | Larger replay window |
| A resumed run starts delivering fresh work sooner | More redundant work on resume |
| More holds, so lower throughput | Fewer holds |
| More store writes, so more store cost | Fewer writes |

## The replay window

Here is the whole guarantee in one measured example. Twelve orders, a capture
every three, the process dying while handling the ninth:

```text
orders-in-the-feed                        12
checkpoint-every-orders                    3
first-attempt/delivered                   order-000 … order-007
first-attempt/status                      Failed
first-attempt/checkpoints-written          2
second-attempt/delivered                  order-006 order-007 order-008 order-009 order-010 order-011
second-attempt/status                     Completed
delivered-twice-the-at-least-once-window  order-006 order-007
every-order-delivered-at-least-once       yes
```

Follow the arithmetic. Two checkpoints were written, at elements three and six.
The first attempt had actually delivered eight orders when it died. The stored
position said six. So the second attempt began at six and **orders six and seven
were delivered twice** — exactly the elements between the last stored position
and the crash. Every order was delivered at least once, and no order was lost.

That set — the elements between the last stored checkpoint and the moment the
process died — is the [replay window](../reference/glossary.md#replay-window),
and its size is what your cadence buys or spends. It is **never zero**.

The published benchmark measures the same thing at scale: 20,000 elements with a
capture every 3,000, over five silo kills, replays a median of **2,000 elements**
and delivers the first fresh element a median of **16.1 ms** after the kill. Read
the second number carefully — it is bimodal. The client's poll is what notices
the run is gone, and a poll that was already in flight when its target died waits
out the whole response timeout before retrying. Four runs of the same arrangement
measured 34, 40, 34, and **5889** milliseconds. Quote the mode you got, and read
[BENCHMARKS.md](../BENCHMARKS.md) for what the measurement leaves out — no
network, no real store, and no failure detection, which makes it a floor rather
than an estimate.

## Why at-least-once and not exactly-once

Because the replay window exists, and it cannot be closed.

Between the last stored position and the crash, work happened that the store does
not know about. On resume, that work is done again. There is no arrangement of
cadence, store, or protocol inside this library that makes the window zero:
narrowing it is what the cadence is for, and narrowing has a cost, and the limit
of narrowing is a checkpoint per element — which is still not zero, because a
process can die between the element and the write.

**Nothing anywhere in this library claims exactly-once.** What follows for you:

- **A sink that must not repeat work has to be idempotent**, or has to use an
  adapter whose row in the [adapter reference](../reference/adapters.md) states a
  stronger guarantee.
- **Durable scope state does not duplicate.** A checkpoint is one moment — the
  scope's state and the cursor are captured at the same quiet point — so a
  replayed element is added to a durable scope's state exactly once. At-least-once
  is the *sink's* window, not the scope's.

The second point is easy to doubt, so here it is measured. The running-total
graph above, crashing at element eight and resuming:

```text
first attempt:  Failed: dropped 0, supervised 0, poison 0, checkpoints 2, held 00:00:00.0042059
first attempt delivered  [1, 3, 6, 10, 15, 21, 28]
second attempt: Completed: dropped 0, supervised 0, poison 0, checkpoints 1, held 00:00:00.0000367
second attempt delivered [28, 36, 45, 55]
true total of 1..10 is 55; the resumed run ended at 55
```

The sink saw the total `28` **twice** — that is the replay window, one element
wide. The scan's own state did **not** double-count it: the resumed run ends at
55, the true sum of 1 through 10. Note also `held 00:00:00.0042059` — two
captures cost this run a little over four milliseconds of stillness, and that is
the number the cadence trade is made against.

### And there is a loss window

Stated plainly, because it is the sharp edge. A graph that **holds** elements
between its cursor and its mark at capture time loses those elements on resume.

Concretely: a batch of five sitting *outside* a durable scope, captured at cursor
eight with one batch committed, resumes at eight — and the three elements the
half-full batch was holding are gone. `8 − 5 = 3`, a number the checkpoint itself
hands you because it carries both the cursor and the mark rather than one of them.

That is a boundary rather than a defect: the batch was not declared durable, so
it reset, and the cursor had counted what it was holding. A graph that must not
lose them puts the batch inside a durable scope, or puts the committing sink
where the elements actually land.

## What happens on resume

Locally, `MaterializeDurableAsync` starts a durable run and
`MaterializeFromCheckpointAsync` continues one. On a cluster there is no second
protocol at all: a run's host reads its checkpoint key when it activates, and a
checkpoint being present *means* the run is resumed.

A resume reads the stored document with its ETag and presents that same ETag at
its own first capture — which is how a stale attempt still writing loses to the
fresh one.

**Four refusals, all by name and all before the first element:**

1. The store holds nothing for that run.
2. The stored document is not one this runtime can read.
3. It was taken of a **different fingerprint or revision**.
4. It names a node this graph has no such stage for.

Each of them says what happened and what to do:

```text
The checkpoint stored for the run 'h14' was taken of the graph sha256:a3627262c82e… and
this is a run of sha256:06210756c11b…. A resume continues the very graph the checkpoint
describes […]

The checkpoint store holds nothing for the run 'never-ran' of the graph 'anonymous', so
there is no run to continue. A run reaches its first checkpoint only once its declared
timing has made one due; a run that crashed before that resumes by being started fresh.
```

The opposite case is *not* a refusal: a stage the plan has that the checkpoint
does not mention is a source that had delivered nothing or a scope that had not
been reached, and each starts from its beginning as it would in a fresh run.

The third refusal is the one to internalize. **There is no cross-revision
checkpoint migration.** "Same fingerprint and same revision, or refuse" is the
whole rule, and the refusal names both fingerprints. Migrating a position across
a changed document would need a declared correspondence between the two — which
node is the same node across an edit, what a partial fold's state means when its
chain changed, what a cursor of a replaced source means — and guessing any of
those wrong corrupts a run silently. So the library refuses, and gives you two
answers instead: run the new revision under a **new name**, beside the old one,
or **replace** the name, which clears the stored position and starts the document
over. Both are described in the [runbooks](../operations/runbooks.md).

**A run that completes writes no checkpoint.** Its outcome is recorded elsewhere,
which is what makes "the last stored capture is what a resume replays from" true
with no special case for the final one.

## What the store must promise

`ICheckpointStore` is three members over one document per `(graph, run)` pair,
and the duties **are** the contract. A store that shirks any of them turns
at-least-once into silent loss.

**`WriteAsync` is atomic per document.** A reader never observes a torn
checkpoint: it sees the previous document or the new one, whole. This is the one
duty no test can hold for you, so the contract states it and your implementation
carries it.

**`WriteAsync` is a compare-and-swap on the [ETag](../reference/glossary.md#etag).**
A write presenting a stale ETag throws `CheckpointConflictException`, and that
refusal is load-bearing: it is how a superseded attempt — a writer on a host the
cluster has moved past — is [fenced](../reference/glossary.md#fencing) out. A
store that helpfully does last-writer-wins re-opens the very race this closes.

**`ClearAsync` is the destructive half of replacement**, under the same ETag
discipline. It exists, and the runtime never calls it: how long a finished run's
position is worth keeping is your deployment's question, not the runtime's.

Any document store with conditional writes implements this — blob leases, ETags,
a SQL rowversion. Anything that can refuse a stale writer will do. A sample
implementation in about fifty lines is in
[`samples/Orleans.Dataflow.Samples/SampleCheckpointStore.cs`](../../samples/Orleans.Dataflow.Samples/SampleCheckpointStore.cs),
and [Checkpoint stores](../operations/checkpoint-stores.md) is the implementation
guide.

## When the store cannot answer

Two situations, and only the store can tell them apart.

**A refusal is not retried.** `CheckpointConflictException` says somebody else
owns this run now. The stale writer stops immediately and the run fails with that
exception, unwrapped. Retrying with the fresh ETag would overwrite the position a
live attempt is building with a snapshot of a run that owns nothing — which is
exactly the corruption the ETag exists to prevent.

**Anything else is retried.** A timeout or an outage says nothing about
ownership, so the same document is presented again five times over roughly four
seconds — waits of 0.1 s, 0.3 s, 0.9 s, 2.7 s — **inside the capture's hold**.
The run is stalled for the whole of it, and the cost lands in the checkpoint-hold
measurement, which is where a deployment discovers that its store is slow.

**What happens when four seconds are not enough is the part worth knowing.** The
attempt ends: the run's completion faults with the store's own exception as its
cause. But nothing is written down as the run's outcome, so **the name is not
retired** — re-declaring the run and starting it again resumes it from the last
checkpoint the store accepted. Do *not* reach for a replacement here: replacing
clears the checkpoint, which is the one thing an outage did not damage. Until
something re-declares it, a stranded run keeps answering with its failure rather
than healing on its own, which is deliberate: you have to be able to see what the
store did.

## Where to go next

- [Durable runs](../guides/durable-runs.md) — a complete program that survives a
  restart.
- [Surviving a crash](../start/surviving-a-crash.md) — the tutorial version.
- [Checkpoint stores](../operations/checkpoint-stores.md) — implementing the
  contract.
- [The cluster model](cluster-model.md) — what happens when a silo dies with a
  run on it.
- [Runbooks](../operations/runbooks.md) — replacing a run, retiring a name,
  rolling an upgrade, recovering from a store outage.
