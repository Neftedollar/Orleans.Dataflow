# Options

Every options type, every field, its default, and what happens at the boundary.

**One record per concern, never one options bag.** Buffering and parallelism are
different decisions with different defaults, so a type carrying both would let an
author set one while meaning the other. The consequence is that there are ten
small types on this page rather than one large one.

**Required members have no default and no unbounded spelling.** Every bound in
this library that could otherwise be a memory leak is `required`: a buffer's
capacity, a collect's element bound, a distinct's key bound, a group-by's active
key bound, an asynchronous stage's concurrency. An unbounded default would be a
leak that compiles.

**Values are checked where the stage is placed, not in the setter.** So `with`
expressions and object initializers compose freely, and the diagnostic names the
operator's own parameter rather than a property somewhere else. The one exception
is `OrleansDataflowClientOptions.PollInterval`, which is a settable property with
its check in the setter because that is what the .NET options-registration
convention needs.

**Which types shape a document.** The seven operator options types are records:
they become a node's payload and part of the graph's
[fingerprint](glossary.md#fingerprint), so value equality is exactly what they
mean and `with` is how you vary one. `DurableRunOptions`,
`DurablePipelineOptions`, and `OrleansDataflowClientOptions` are classes: they
shape a *run* or a *client*, reach no document, and hold live services, so the
equality and the `ToString` a record would generate are promises they cannot
keep.

**Examples on this page** were compiled and executed against the library in a
scratch project written for this page, so every value shown is one the operator
that takes it accepts.

---

## `BufferOptions`

Namespace `Orleans.Dataflow`. Taken by `Buffer`, by `Delay` as its holdback, by
`Source.Queue`, and by the ingress of every Orleans push adapter.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `Capacity` | `int` | ✅ | — | at least 1 | A capacity of 1 is the smallest buffer and still a real [boundary](glossary.md#boundary) — it decouples the segments on either side into two loops. |
| `OverflowPolicy` | `OverflowPolicy` | | `Backpressure` | a declared member of the enumeration | See the table below. |

### `OverflowPolicy`

| Value | What a full buffer does | Loses elements |
|---|---|---|
| `Backpressure` | The producer waits until there is room. | no |
| `DropOldest` | The oldest held element is discarded to make room. | yes |
| `DropNewest` | The arriving element is discarded. | yes |
| `DropBuffer` | The whole buffer is discarded. | yes |
| `Fail` | The run fails with [`BufferOverflowException`](errors.md#bufferoverflowexception). | — |

The lossless value is the default deliberately: every other value discards
elements, and discarding elements is a thing an author says out loud. Every drop
is counted and appears on the run's
[snapshot](run-handles.md#runsnapshot) as `DroppedElements`.

Two adapters refuse `Backpressure` outright — the reminder trigger and the
broadcast-channel source — because a clock and a fan-out relay cannot be slowed
by one listener. See [adapters](adapters.md).

```csharp
new BufferOptions { Capacity = 32, OverflowPolicy = OverflowPolicy.DropOldest }
```

```fsharp
BufferOptions(Capacity = 32, OverflowPolicy = OverflowPolicy.DropOldest)
```

---

## `ParallelismOptions`

Namespace `Orleans.Dataflow`. Taken by every asynchronous mapping operator, by
`MergeMap`, and by `ForEachAsync`.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `MaxConcurrency` | `int` | ✅ | — | at least 1 | A maximum of 1 is the sequential asynchronous map — one callback runs, its result is emitted, the next element starts. It is a real setting, and it is what you write for a callback that talks to something tolerating no concurrency at all. |

It is a bound, not a target: the engine never runs more, and runs fewer when
there is less to do. Parallelism and buffering are separate decisions — an
asynchronous stage bounds how much work is *outstanding*, and a `BufferOptions`
in front of it bounds how much work is *waiting to start*.

### `ValveMode`

Not an options type but the one other value an operator takes by enumeration:
the mode a `Valve` starts in.

| Value | The stream at the start of the run |
|---|---|
| `Open` | flows; this is the parameter's default |
| `Closed` | is held at the valve until something calls `IValve.Open()` |

A valve is steered while the run is running through a
[control slot](run-handles.md#result-slots-and-control-slots), so this value only
decides where it begins.

---

## `CollectOptions`

Namespace `Orleans.Dataflow`. Taken by the collecting sink.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `MaxElements` | `int` | ✅ | — | at least 1 | A run delivering exactly this many elements succeeds with all of them; the element *after* them fails the run with [`CollectOverflowException`](errors.md#collectoverflowexception). The bound is a size the result may reach, not one it may not. |

Failing rather than truncating is the whole point of the bound. A truncated list
is a wrong answer in the shape of a right one, and nothing downstream could tell
that elements were missing. If you want the first *n*, write `Take(n)`.

A collected result that crosses a grain boundary meets a *second* bound, declared
by the silo rather than by the document — see
[`LimitResultSize`](hosting.md#silo-settings) and
[`ResultTooLargeException`](errors.md#resulttoolargeexception). The two answer
different questions: how much a run may accumulate, and how much a host is
willing to put on one message.

---

## `DistinctOptions`

Namespace `Orleans.Dataflow`. Taken by `Distinct`.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `MaxTrackedKeys` | `int` | ✅ | — | at least 1 | Counts *distinct keys*, not elements: a repeated element is recognized and dropped without occupying anything new, so a stream of one key forever runs inside a bound of 1. |
| `OverflowPolicy` | `KeyOverflowPolicy` | | `Fail` | a declared member | See below. |

### `KeyOverflowPolicy`

| Value | What the key past the bound does |
|---|---|
| `Fail` | The run fails with [`TrackedKeyOverflowException`](errors.md#trackedkeyoverflowexception). Exact deduplication over the whole run. |
| `EvictOldest` | The oldest tracked key is forgotten. The stream is then distinct over a *window* rather than over its history, and an element whose key was evicted is emitted a second time. |

Defaulted to failing rather than required, unlike the bound beside it, and the
two are different questions. How much a stage may remember has no answer this
library could guess; what to do when it has remembered that much has one honest
answer, which is to report that the bound was wrong instead of silently becoming
a weaker operator.

---

## `GroupByOptions`

Namespace `Orleans.Dataflow`. Taken by `GroupBy`.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `MaxActiveKeys` | `int` | ✅ | — | at least 1 | Counts keys with a substream open, not elements. A key whose substream ended of its own accord — a `Take` inside the group flow reaching its bound — **still occupies its place**, because remembering that a key has ended is what keeps it ended. |
| `OverflowPolicy` | `ActiveKeyOverflowPolicy` | | `Fail` | a declared member | See below. |

### `ActiveKeyOverflowPolicy`

| Value | What the key past the bound does |
|---|---|
| `Fail` | The run fails with [`TrackedKeyOverflowException`](errors.md#trackedkeyoverflowexception), and the message names the key. One substream per key over the whole run. |
| `EvictIdle` | The least recently used key's substream ends where it stood, and the same key can start a second one later. The stream downstream is grouped over a window of activity rather than over the whole run. |

`GroupBy` is the sharpest way this vocabulary lets memory grow with data — one
*running substream* per distinct key rather than one entry per key — which is why
this bound gets a type of its own rather than a share of somebody else's.

---

## `ThrottleOptions`

Namespace `Orleans.Dataflow`. Taken by `Throttle`.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `Elements` | `int` | ✅ | — | at least 1 | Counted in *cost units*, which equal elements only for the overload with no cost function. |
| `Per` | `TimeSpan` | ✅ | — | positive and finite | A rate has no meaning without the period it is measured over, so a default period would be a rate the author did not write. |
| `MaximumBurst` | `int?` | | `Elements` | at least 1 when written | The bucket's size, and therefore the longest quiet period a stream can bank. An element whose cost exceeds it can **never** be admitted by waiting, and *both* modes fail the run for one. |
| `Mode` | `ThrottleMode` | | `Shaping` | a declared member | See below. |

### `ThrottleMode`

| Value | What an element with no budget does |
|---|---|
| `Shaping` | Waits until there is budget. |
| `Enforcing` | Fails the run with [`RateLimitExceededException`](errors.md#ratelimitexceededexception). |

The waiting value is the default deliberately: the other one ends the run, and
ending a run is something an author says out loud.

**The model is a token bucket and is stated rather than implied.** The bucket
holds `MaximumBurst` cost units, starts full, and refills at `Elements` units per
`Per` — *continuously* rather than in steps, so a throttle of ten per second
admits one element every hundred milliseconds instead of ten at the top of each
second. An element costs one unit unless the operator was given a cost function,
in which case it costs what that function answers.

The default burst is `Elements`, which is the bucket that starts full and holds
one period's worth: the smallest burst that lets a stream arriving exactly at the
declared rate pass without being paced at all.

```csharp
new ThrottleOptions { Elements = 100, Per = TimeSpan.FromSeconds(1), MaximumBurst = 200 }
```

```fsharp
ThrottleOptions(Elements = 100, Per = TimeSpan.FromSeconds 1.0, MaximumBurst = 200)
```

---

## `SupervisionOptions`

Namespace `Orleans.Dataflow`. Taken by `Supervised`.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `Form` | `SupervisionForm` | ✅ | — | a declared member | There is no supervision an author could have meant without saying it. The three members below are read **only** for `Retry` and are refused on every other form. |
| `MaxAttempts` | `int` | | `1` | at least 1 | Counts *attempts*, not retries: 3 means one offer and two re-offers. 1 is legal and means "no re-offer" — the exhaustion answer is applied to the first failure. |
| `Backoff` | `IReadOnlyList<TimeSpan>` | | empty | each rung zero or more | **The last rung repeats**, so a ladder shorter than the attempt count means "and then this long every time". An empty ladder means every re-offer happens at once. A rung of *zero* is admitted, unlike every other duration in this vocabulary. |
| `OnExhaustion` | `RetryExhaustion` | | `Fail` | a declared member | See below. |

### `SupervisionForm`

| Value | What a failure raised inside the scope does |
|---|---|
| `Resume` | The failing element is dropped and the scope's stage state is kept: a scan keeps counting, a distinct keeps its keys, a batch keeps its open group. |
| `RestartStage` | The failing element is dropped and every stage inside the scope resets to its seed, rebuilt from the very factories a fresh run builds them from. |
| `Retry` | The element is offered to the scope's **first** stage again, up to `MaxAttempts`, waiting `Backoff` between attempts, with `OnExhaustion` as the answer when they run out. |
| `Recover` | The scope emits a declared fallback element and ends its stream *successfully*: everything above the scope stops, everything below drains, the run reports success. This is the form the three-argument `Supervised` takes, and the only form it takes. |

There is no fifth value for "fail the run", because that is not a form a scope
takes — it is what happens outside every scope.

**No form names an exception type.** A policy that filtered by type would need
CLR type names in a document, which the definition plane forbids, or a declared
failure taxonomy, which is design work of its own. A scope supervises every
failure raised inside it alike.

### `RetryExhaustion`

| Value | What a [poison element](glossary.md#poison-element) does |
|---|---|
| `Fail` | The run fails with the exception of the last attempt — the author's own instance, not a wrapper naming the attempts. |
| `Resume` | The element is dropped and the scope's stage state is kept. |
| `RestartStage` | The element is dropped and every stage inside the scope resets to its seed. The answer for a scope whose stages saw the element once *per attempt* and are therefore holding state that counted it several times. |

There is deliberately no `Retry` among them — retrying an element that has run
out of retries is not an answer — and no `Recover`, because ending the stream
after a fallback is a decision about the stream rather than about this element.

An element that exhausts its attempts moves the run's `PoisonElements` count, so
"we retried and gave up" is a number a monitor can read rather than a silence.

The waits are taken on the run's own clock and are not jittered. Jitter answers a
question a per-element retry inside one run does not ask — it spreads a fleet's
restarts, and there is no fleet here.

```csharp
new SupervisionOptions
{
    Form = SupervisionForm.Retry,
    MaxAttempts = 3,
    Backoff = [TimeSpan.Zero, TimeSpan.FromMilliseconds(50)],
    OnExhaustion = RetryExhaustion.Resume,
}
```

```fsharp
SupervisionOptions(
    Form = SupervisionForm.Retry,
    MaxAttempts = 3,
    Backoff = [| TimeSpan.Zero; TimeSpan.FromMilliseconds 50.0 |],
    OnExhaustion = RetryExhaustion.Resume)
```

---

## `DurableRunOptions`

Namespace `Orleans.Dataflow`. Taken by `LocalDataflowHost.MaterializeDurableAsync`
and `MaterializeFromCheckpointAsync`. A class rather than a record.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `Store` | `ICheckpointStore` | ✅ | — | — | A live service the caller owns and may share between runs. See [checkpoint stores](../operations/checkpoint-stores.md). |
| `RunId` | `RunId` | ✅ | — | a valid run identity | The name a checkpoint is keyed by, together with the graph's identity. A resume presents the same one, because a resume is *the same run continuing*. |
| `Interval` | `TimeSpan?` | | `null` | positive when written | "At most this long between two *timed* captures." A capture the element bound made due does not postpone the next timed one. The first is due one interval after the run starts. |
| `EveryElements` | `int?` | | `null` | at least 1 when written | Counted as elements **admitted** — every element a source of this run hands to the graph, summed across the sources. Not elements committed at a sink. |

**A run that declares neither an interval nor an element bound never touches the
store**, and that is the honest reading of "durable options with no timing in
them" rather than a mistake. There is no default interval, because a default
would make every durable run pay for a cadence nobody chose.

**A capture holds the run for its duration.** The engine reaches a quiescent
point through the same machinery `PauseAsync` uses, so while a checkpoint is
being taken and written, no element moves anywhere in the graph. A shorter
interval and a smaller element bound both buy a smaller
[replay window](glossary.md#replay-window) and both cost throughput. The total
cost is measurable: `TotalCheckpointHold` on the run's
[snapshot](run-handles.md#runsnapshot).

**The run identity is the author's and not the host's.** An ordinary run is named
by the host with a fresh identifier per materialization, because two runs of one
graph are two runs. A durable run is named by whoever will resume it.

```csharp
DurableRunOptions durable = new()
{
    Store = store,
    RunId = RunId.Create("nightly-2026-08-19"),
    EveryElements = 1_000,
    Interval = TimeSpan.FromSeconds(30),
};
```

---

## `DurablePipelineOptions`

Namespace `Orleans.Dataflow.Hosting`, from the Orleans package. Taken by
`OrleansDataflowHost.MaterializeDurableAsync` and `ReplaceDurableRunAsync`. The
cluster-facing counterpart of `DurableRunOptions` — the same three things minus
the store, which the silo supplies for itself.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `RunId` | `string` | ✅ | — | the runtime's identifier grammar | Text rather than a typed identity because the client surface takes strings and validates. A value outside the grammar is refused by the silo rather than accepted and then unaddressable. |
| `Interval` | `TimeSpan?` | | `null` | positive when written | As above. |
| `EveryElements` | `int?` | | `null` | at least 1 when written | As above. |

**Materializing one durable pipeline twice under one `RunId` addresses one run.**
The second call hands back a handle to the run that already exists, or continues
it from its checkpoint if the silo hosting it has died. Two independent durable
runs are two names.

Where the checkpoints go is not here: it is the silo's
[`UseCheckpointStore`](hosting.md#silo-settings), because where a checkpoint
lives is a property of the deployment and not of a call.

---

## `OrleansDataflowClientOptions`

Namespace `Orleans.Dataflow.Hosting`, from the Orleans package. Passed to
`AddOrleansDataflowClient`. A class with a settable property, because the
container resolves exactly one instance of it.

| Field | Type | Required | Default | Bounds | At the boundary |
|---|---|---|---|---|---|
| `PollInterval` | `TimeSpan` | | `DefaultPollInterval`, **20 ms** | positive; the setter throws `ArgumentOutOfRangeException` otherwise | A run's completion is observed up to one interval after it happens, and a client watching many runs makes one call per run per interval. |
| `DefaultPollInterval` | `static readonly TimeSpan` | — | 20 ms | — | Short, because the runs a poll watches are usually short too. A deployment whose runs last hours widens it. |

Completion is observed by polling and this is the one knob that choice needs. An
observer is best-effort by design in Orleans, so a completion delivered by one
would have to be backed by a poll anyway.

```csharp
services.AddOrleansDataflowClient(options => options.PollInterval = TimeSpan.FromSeconds(1));
```

---

## Settings that are not options types

Three things a deployment configures are method arguments on a builder rather
than a type of their own, because they belong to a silo rather than to a graph or
a run. All three are on
[`IOrleansDataflowBuilder`](hosting.md#the-silo-builder-extension):

| Setting | Default | What it bounds |
|---|---|---|
| `LimitResultSize(maximumBytes)` | `OrleansDataflowResults.DefaultMaximumResultBytes` — **1 MiB** (1 048 576) | The largest result this silo will send across a grain boundary, measured on the value's Orleans-serialized form. At least 1. Exceeding it fails *that read* with [`ResultTooLargeException`](errors.md#resulttoolargeexception) and nothing else. |
| `UsePlacement(runGrains, keyedExecutors)` | `DataflowPlacement.ClusterDefault` for both | Where the run grain and a keyed stage's per-key executors are placed. Values: `ClusterDefault`, `Random`, `PreferLocal`, `HashBased`. |
| `UseCheckpointStore(resolver)` | none | Where this silo's durable runs keep their checkpoints. **A silo without one runs no durable pipeline**, and refuses a request for one at the declaration rather than at the first capture. |

Each replaces whatever a previous call said rather than adding to it, because a
silo has one bound, one placement, and one store.

---

## The default that is a decision

Read the defaults column downwards and one pattern shows: **every default is the
value that loses nothing.** `Backpressure` over dropping. `Fail` over evicting.
`Shaping` over enforcing. `Fail` over swallowing an exhausted retry. The library
never quietly becomes a weaker thing than it said it was; every weakening is a
value an author wrote down.

The bounds themselves have no defaults at all, and that is the same principle
seen from the other side: a bound this library guessed would be a promise it
could not keep about a stream it has never seen.

---

## Related

- [Operators](operators.md) — which operator takes which options type.
- [Errors](errors.md) — the exception each boundary raises.
- [Bounding memory](../guides/bounding-memory.md) — choosing these numbers for a
  real pipeline.
- [Durable runs](../guides/durable-runs.md) — choosing a checkpoint cadence.
