# Adapters

Every source and sink adapter that ships: what it connects to, what it guarantees
on delivery, what it replays after a resume, and what it does not promise.

An **adapter** is a [registered stage](glossary.md#registered-stage) that talks to
something outside the run. It is addressed by name in a document, so a pipeline
built from adapters carries no delegate and can run on a silo. That is the
difference from the [operators](operators.md), which are engine primitives: there
is no external system behind an `IEnumerable<T>`, a bounded queue, or a `Take`.

**Twelve adapters ship, in two vocabularies.**

| Vocabulary | Package | Registered by | Stages |
|---|---|---|---|
| `dotnet` | `Orleans.Dataflow` | `AddDotnetStages()` | 2 |
| `orleans` | `Orleans.Dataflow.Cluster` | any Orleans binding | 10 |

The `dotnet` vocabulary lives in the core package deliberately: nothing about a
periodic tick or an `IObservable<T>` is an Orleans concept, so one registration
serves a silo and a `LocalDataflowHost` alike.

The `orleans` vocabulary is published exactly when a silo registers at least one
Orleans binding, and then all ten arrive at once — they ship as one vocabulary,
and a half-published one would fail at the first element instead of at the start.

**Examples on this page** were compiled and executed in a scratch project written
for this page: every binding is created and every payload written, so the values
they carry are ones the adapters' own validation accepts. The one line that cannot
be checked is the observable a deployment supplies, and it is marked as such.

---

## At a glance

| Stage | Direction | Delivery | Replays after a resume | Bounded by |
|---|---|---|---|---|
| [`dotnet/timer@v1`](#dotnettimerv1) | source | at-most-once | nothing — tick numbers start at zero | the pull itself; no queue |
| [`dotnet/observable@v1`](#dotnetobservablev1) | source | best effort | nothing | a declared ingress |
| [`orleans/stream-source@v1`](#orleansstream-sourcev1) | source | the provider's | **from a stored sequence token**, inclusive of the element it names | a declared ingress |
| [`orleans/stream-sink@v1`](#orleansstream-sinkv1) | sink | the provider's | nothing | one publication in flight |
| [`orleans/grain-enumerable@v1`](#orleansgrain-enumerablev1) | source | at-most-once within one call | nothing | the enumeration's own pull |
| [`orleans/grain-call@v1`](#orleansgrain-callv1) | flow | at-most-once | nothing | `maxInFlight` |
| [`orleans/grain-call-keyed@v1`](#orleansgrain-call-keyedv1) | flow | at-most-once | nothing | one call per key, plus `maxInFlight` across keys |
| [`orleans/grain-call-sink@v1`](#orleansgrain-call-sinkv1) | sink | at-most-once | **acknowledged calls are not redone** | `maxInFlight` |
| [`orleans/reminder-trigger@v1`](#orleansreminder-triggerv1) | source | at-most-once, best effort | nothing — missed ticks are gone | a declared ingress; not backpressuring |
| [`orleans/observer@v1`](#orleansobserverv1) | source | best effort | nothing | a declared ingress |
| [`orleans/broadcast-source@v1`](#orleansbroadcast-sourcev1) | source | best effort | nothing | a declared ingress; not backpressuring |
| [`orleans/broadcast-sink@v1`](#orleansbroadcast-sinkv1) | sink | best effort | nothing | one publication in flight |

**Only two adapters contribute to a checkpoint.** The stream source declares a
[cursor](glossary.md#cursor); the grain-call sink declares a
[mark](glossary.md#mark). Everything else resumes from *now* — and that is stated
per adapter rather than generalized, because a position an adapter could not
honor is worse than no position at all.

**Nothing here is exactly-once.** Between checkpoints the guarantee is
[at-least-once](glossary.md#at-least-once), and the library says so rather than
implying otherwise: after a crash, everything in the
[replay window](glossary.md#replay-window) is delivered again. A sink that must
not repeat work needs to be idempotent.

---

## The questions every adapter answers

Every row below states the same eleven things, because these are what a caller
needs and what no test can check for them:

**Backpressure** — can the upstream system honor demand, or must this adapter use
a bounded buffer with a drop or fail policy? **Acknowledgement** — what exact
event does a successful read or write await? **Delivery** — at-most-once,
at-least-once, best effort, or a provider-specific boundary? **Ordering** —
global, per partition, source order, or unspecified? **Replay** — is history
available, and from which cursor? **Checkpoint** — who owns the cursor and when
may it advance? **Retry** — which failures are retryable and what duplicate
window does retry introduce? **Idempotency** — does the adapter enforce it,
accept a key, or require the sink to provide it? **Serialization** — which schema
identity survives storage or transport? **Cancellation** — where is it observed?
**Resource bounds** — how many subscriptions, connections, buffers, in-flight
requests.

These are stage capabilities and provider facts, not one global "delivery
guarantee" switch. **This library does not make a graph exactly-once by
configuration.**

---

## What each adapter publishes

Every adapter ships four things a document author or a provider may need to name.
The typed handle is what you attach; the `StageRef` and the `ContractReference`
are what a document carries, and they are exposed so a deployment can inspect,
compare, or hand-author one.

| Adapter | Typed handle | Payload writer | `StageRef` | Payload contract |
|---|---|---|---|---|
| timer | `DotnetStages.Timer()` | `DotnetStages.TimerParameters` | `DotnetStages.TimerStage` | `DotnetStages.TimerParameterContract` |
| observable | `DotnetStages.Observable` | `DotnetStages.ObservableParameters` | `DotnetStages.ObservableStage` | `DotnetStages.ObservableParameterContract` |
| stream source | `OrleansStages.StreamSource` | `OrleansStages.StreamSourceParameters` | `OrleansStages.StreamSourceStage` | `OrleansStages.StreamSourceParameterContract` |
| stream sink | `OrleansStages.StreamSink` | `OrleansStages.StreamSinkParameters` | `OrleansStages.StreamSinkStage` | `OrleansStages.StreamSinkParameterContract` |
| grain enumerable | `OrleansStages.GrainEnumerable` | `OrleansStages.GrainEnumerableParameters` | `OrleansStages.GrainEnumerableStage` | `OrleansStages.GrainEnumerableParameterContract` |
| grain call | `OrleansStages.GrainCall` | `OrleansStages.GrainCallParameters` | `OrleansStages.GrainCallStage` | `OrleansStages.GrainCallParameterContract` |
| keyed grain call | `OrleansStages.KeyedGrainCall` | `OrleansStages.KeyedGrainCallParameters` | `OrleansStages.KeyedGrainCallStage` | `OrleansStages.KeyedGrainCallParameterContract` |
| grain call sink | `OrleansStages.GrainCallSink` | `OrleansStages.GrainCallSinkParameters` | `OrleansStages.GrainCallSinkStage` | `OrleansStages.GrainCallSinkParameterContract` |
| reminder trigger | `OrleansStages.ReminderTrigger()` | `OrleansStages.ReminderTriggerParameters` | `OrleansStages.ReminderTriggerStage` | `OrleansStages.ReminderTriggerParameterContract` |
| observer bridge | `OrleansStages.ObserverBridge` | `OrleansStages.ObserverBridgeParameters` | `OrleansStages.ObserverBridgeStage` | `OrleansStages.ObserverBridgeParameterContract` |
| broadcast source | `OrleansStages.BroadcastSource` | `OrleansStages.BroadcastSourceParameters` | `OrleansStages.BroadcastSourceStage` | `OrleansStages.BroadcastSourceParameterContract` |
| broadcast sink | `OrleansStages.BroadcastSink` | `OrleansStages.BroadcastSinkParameters` | `OrleansStages.BroadcastSinkStage` | `OrleansStages.BroadcastSinkParameterContract` |

Each vocabulary also publishes what a host registers and what an author binds
types to:

| Member | On | What it is |
|---|---|---|
| `Provider` | both | the `ProviderId` — `dotnet` and `orleans` |
| `Catalog` | both | the `StageCatalog` a host registers with `AddCatalog` |
| `Element<T>()` | both | the element contract this vocabulary carries values under |
| `ElementContract` | both | that contract's own `ContractReference` |
| `Tick` | both | the `ElementContract<long>` a timer or a reminder emits |
| `BroadcastSourceNamespace` | `OrleansStages` | the one namespace a broadcast source may consume, `orleans-dataflow-broadcast` |
| `BroadcastSourceChannel(provider, key)` | `OrleansStages` | builds a `OrleansStreamAddress` inside that namespace |
| `ObserverBridgeKey(…)` | `OrleansStages` | the address a pusher sends to; two overloads, one taking a `PipelineRunTicket` |

## Bindings: naming the code a document may not carry

A document names a *binding*; the deployment registers what that name resolves
to. Every binding is created by a static `Create` and exposes what it declares.

| Binding | Created with | Members | Registered with |
|---|---|---|---|
| `ObservableBinding<T>` | `ObservableBinding.Create(name, output, open)` | `Name`, `Output` | `AddObservable` |
| `StreamElementBinding<T>` | `StreamElementBinding.Create(element)` | `Element` | `AddStreamElement` |
| `BroadcastElementBinding<T>` | `BroadcastElementBinding.Create(element)` | `Element` | `AddBroadcastElement` |
| `GrainCallBinding<TIn, TOut>` | `GrainCallBinding.Create(name, input, output, call)` | `Name`, `Input`, `Output` | `AddGrainCall` |
| `KeyedGrainCallBinding<TIn, TOut>` | `KeyedGrainCallBinding.Create(name, input, output, key, call)` | `Name`, `Input`, `Output` | `AddKeyedGrainCall` |
| `GrainCallSinkBinding<TIn>` | `GrainCallSinkBinding.Create(name, input, call)` | `Name`, `Input` | `AddGrainCallSink` |
| `GrainEnumerableBinding<T>` | `GrainEnumerableBinding.Create(name, output, open)` | `Name`, `Output` | `AddGrainEnumerable` |
| `ObserverBridgeBinding<T>` | `ObserverBridgeBinding.Create(name, output)` | `Name`, `Output` | `AddObserverBridge` |

**The two element bindings carry no name and the six others do**, and the
difference is real: a stream or a broadcast channel is addressed by the document
(a provider, a namespace, a key), so the binding only has to say which CLR type
carries the contract. A call, an enumeration, a bridge, and an observable *are*
the code, so each needs a name a document can carry.

The delegate on a call binding receives the silo's `IGrainFactory`, the element,
and a cancellation token; a keyed binding additionally carries the function that
answers an element's partition key.

## `OrleansStreamAddress`

Where a stream adapter reads or writes. A readonly struct with value equality.

| Member | What it is |
|---|---|
| `OrleansStreamAddress.Create(provider, streamNamespace, key)` | Two overloads: a `string` key or a `Guid` key. |
| `Provider` | the stream provider's registered name |
| `Namespace` | the stream namespace |
| `Key` | the stream key, rendered as text |
| `IsDefault` | whether this is the default value, which addresses nothing |

---

## The .NET vocabulary

Registered with `AddDotnetStages()` on either builder. Reached through
`Orleans.Dataflow.Adapters.DotnetStages`.

### `dotnet/timer@v1`

`DotnetStages.Timer()`, parameters `DotnetStages.TimerParameters(period,
tickLimit)`.

**What it connects to.** Nothing outside the process — it is the registered,
deployable form of a periodic tick.

**Elements.** `long` indices from zero, one per tick, in order. The index counts
the ticks *this run* emitted and is never a wall-clock reading, so it is stable to
compare and useless to schedule against.

**Backpressure.** The pull itself, and no queue anywhere. The timer is awaited on
the run's own source thread, so a run slower than the period simply ticks later.
**Ticks do not accumulate and none is dropped**, because there is no buffer for
them to accumulate in. That is the one honest difference between a run-scoped
timer and a push source, and it is why this stage declares no ingress bound.

**Acknowledgement.** None; a tick is generated rather than delivered.

**Delivery, ordering, retry, idempotency.** At-most-once by construction; the
tick index; nothing retries; not enforced.

**Completion.** The declared `tickLimit`, or never. A timer with a limit ends its
sequence after that many ticks and the run completes; a timer without one ends
only when the run does. `period` is at least one millisecond; `tickLimit` is zero
or more, and zero means "until the run ends".

**Replay and checkpoint.** None. Tick zero is due a declared delay after *the run*
started, so a resumed run's clock starts again and its tick numbers start at zero.
A schedule that must survive a restart is a reminder, not a tick.

**Shutdown and cancellation.** A graceful shutdown ends the sequence at once
rather than after the current period, so the ticks already inside the graph drain
and no further tick is produced. A cancellation abandons the wait and the run.

**Resource bounds.** One timer per run per occurrence.

### `dotnet/observable@v1`

`DotnetStages.Observable(binding)`, parameters
`DotnetStages.ObservableParameters(binding, ingress)`. The observable itself is a
deployment registration — `AddObservable` — because a document may not carry a
delegate.

**What it connects to.** Any `IObservable<T>` the deployment names.

**Backpressure.** The declared ingress bound, **paid by the notification's own
thread**. `OnNext` returns `void` and has nothing to await, so under
`OverflowPolicy.Backpressure` a full ingress blocks whichever thread the
observable pushes on until the run makes room. A producer that cannot pay that
declares a dropping policy and the drops are counted; the failing policy faults
the run.

**Acknowledgement.** The offer into the ingress, and never end-to-end processing.

**Delivery.** Best effort. An element the ingress accepted may still be lost by a
run that fails, and a graceful shutdown abandons whatever the ingress still holds.
That is not a contradiction of the acknowledgement boundary but the honest reading
of it.

**Ordering.** Whatever the observable gives. `IObserver<T>` requires notifications
to be serialized, and this adapter preserves that order into the ingress.

**Replay, checkpoint, retry, idempotency.** None; none; none; not enforced.

**Subscription lifetime.** One run: made at the run's first pull and disposed in
the `finally` the engine reaches on every terminal path. A *cold* observable
therefore gets one producer per run; a *hot* one shares its elements between
concurrent runs. That is the observable's own character, not this adapter's
choice.

**Serialization.** The element contract reference travels in the payload.

**A .NET event is deliberately not a second stage.** It is one adapter away from
an `IObservable<T>` — add a handler on subscribe, remove it on dispose — and a
stage for it would be a second registration surface and a second set of lifetime
rules for the same delivery semantics.

```csharp
IObservable<int> ticker = /* whatever the deployment owns */;

ObservableBinding<int> ticks = ObservableBinding.Create("ticks", DotnetStages.Element<int>(), () => ticker);

LocalDataflowHost host = new(builder => builder.AddDotnetStages().AddObservable(ticks));

_ = DotnetStages.ObservableParameters(ticks, new BufferOptions { Capacity = 32, OverflowPolicy = OverflowPolicy.DropOldest });
```

---

## The Orleans vocabulary

Reached through `Orleans.Dataflow.Adapters.OrleansStages`. Every one of these
needs its element type registered on the silo, because a document never names a
CLR type.

### `orleans/stream-source@v1`

`OrleansStages.StreamSource(binding)`, parameters
`StreamSourceParameters(binding, stream, ingress)`. Registered with
`AddStreamElement<T>`.

**What it connects to.** An Orleans stream, addressed by
`OrleansStreamAddress.Create(provider, streamNamespace, key)` — a provider name, a
namespace, and a `string` or `Guid` key.

**Acknowledgement.** Delivery into the run's bounded ingress, and never end-to-end
processing: an element this adapter accepted may still be lost by a run that fails
afterwards.

**Backpressure.** The declared ingress bound. Under the backpressuring policy a
full ingress delays the provider's own pulling agent, **which serves a whole
queue** — so a run that stops draining delays delivery to every consumer of that
queue and not only to itself. Under a dropping policy the delivery is answered at
once and the drop is counted; under the failing policy the run faults.

**Delivery and ordering.** The named provider's, reported rather than
generalized. Orleans orders one stream from one producer and nothing across
producers, and the memory provider is non-durable by design.

**Replay — from a stored cursor, and only there.** An ordinary run subscribes
without a token and reads what arrives after it subscribed; a run declared durable
subscribes at the token its previous attempt recorded.

**Checkpoint — the sequence token of the last element the run delivered.** It is
stored as `{"index":n,"sequence":n,"token":"…"}`: the provider's own two numbers,
readable by anyone, beside the token itself as the silo serializer's bytes in
base64. **That last value is the one thing in a checkpoint document that is not
portable outside the deployment that wrote it.** The position is promoted when the
*run* has delivered the element rather than when the subscription received it,
because a bounded ingress holds elements the run has not taken and a cursor that
counted arrivals would skip them on resume.

**The replay window includes the element the cursor names.** A subscription opened
at a token receives that element again — Orleans exposes no "token plus one" — so
a stream source's window is one element wider than an index cursor's.

**Two ways it degrades, both stated rather than promised around.** A provider
whose `IsRewindable` is false refuses the token, and the resumed run fails on its
subscription rather than silently reading from now. A rewindable provider that has
*purged* the token has nothing to replay — the memory provider empties its queue
cache when its last consumer leaves — so how far back a resume can reach is the
provider's cache configuration and not this adapter's promise.

A run that had delivered nothing stores no position and resumes as a fresh run
does.

**Retry and idempotency.** None; not enforced.

**Serialization.** The element type is a deployment registration and the payload
carries its contract reference, so a document written against another signature is
refused rather than cast.

**Resource bounds.** One subscription per run per occurrence, one ingress of the
declared capacity, nothing persisted.

### `orleans/stream-sink@v1`

`OrleansStages.StreamSink(binding)`, parameters
`StreamSinkParameters(binding, stream)`.

**Acknowledgement.** One awaited `OnNextAsync` per element — *publication*, and
never end-to-end delivery. What a consumer then does with the element is between
the consumer and the provider.

**Backpressure.** The awaited publication itself, so a slow provider slows the run
rather than filling a queue.

**Delivery and ordering.** The provider's. Elements are published one at a time in
the order the run produced them, which is the strongest order this adapter can
offer, because Orleans orders one stream from one producer.

**Replay, checkpoint, retry, idempotency.** None; none — this adapter owns no
cursor; none; not enforced.

**Completion.** A run that ends signals nothing on the stream, because a stream
has no end this publisher owns. A run that fails likewise leaves it alone.

**Cancellation.** Observed between elements, because a terminal is a synchronous
fold handed no token.

**Resource bounds.** One element in flight.

### `orleans/grain-enumerable@v1`

`OrleansStages.GrainEnumerable(binding)`, parameters
`GrainEnumerableParameters(binding)`. Registered with `AddGrainEnumerable<T>`.

**What it connects to.** A grain method returning `IAsyncEnumerable<T>`.

**Acknowledgement.** The call-scoped pull — an element is taken when the run asks
for it, and Orleans batches the transport underneath at its own default.

**Backpressure.** The enumeration's own, which is why this is the one Orleans
source that needs no ingress buffer: a run that stops pulling stops the grain from
producing.

**Delivery.** At-most-once within one call; there is no redelivery of an element
the run took.

**Ordering.** The grain's own, preserved end to end.

**Replay and checkpoint.** None. Resuming where a previous run stopped needs an
application cursor the grain owns, and nothing in this adapter keeps one.

**Cancellation.** Cooperative and carried by the run's own token. Orleans defaults
`MessagingOptions.CancelRequestOnTimeout` to false, so a response timeout does not
cancel the grain-side enumeration — a grain that ignores the token delays the
run's stop until it next yields. Disposal is awaited on every terminal path.

**Resource bounds.** One enumeration per run per occurrence.

### `orleans/grain-call@v1`

`OrleansStages.GrainCall(binding)`, parameters
`GrainCallParameters(binding, maxInFlight, timeout)`. Registered with
`AddGrainCall<TIn, TOut>`.

**What it connects to.** Any grain method the deployment names, as a transforming
flow.

**Acknowledgement.** The awaited reply, which acknowledges *that method
invocation* and nothing the grain may have started behind it.

**Backpressure and credit.** The declared `maxInFlight`. A call in flight is
credit spent and its reply is the grant; elements reach the stage through a
bounded channel, and nothing on the wire carries credit.

**Delivery.** At-most-once from this adapter — it never retries, and a call that
fails faults the run.

**Ordering.** Emission is in input order; the calls themselves overlap up to the
bound, so the grains see them concurrently and only what *leaves* the stage is
ordered.

**Timeout.** Its own, declared per occurrence, raising
[`GrainCallTimeoutException`](errors.md#graincalltimeoutexception). It is enforced
whether or not the registered call honors the token it was given.

**Replay, checkpoint, retry, idempotency.** None; none — its effects flow onward,
and the stream's own progress is a cursor's to state; none; not enforced, and not
enforceable by this adapter. A deployment that wants a retry writes it inside the
registered call, where the duplicate window it opens is the deployment's own to
state.

**Serialization.** The payload carries the input and output contract references,
so a document compiled against a different signature is refused.

**Resource bounds.** `maxInFlight` calls at once.

### `orleans/grain-call-keyed@v1`

`OrleansStages.KeyedGrainCall(binding)`, parameters
`KeyedGrainCallParameters(binding, maxInFlight, distributed, timeout)`. Registered
with `AddKeyedGrainCall<TIn, TOut>`, which carries the routing function as well as
the call.

**Acknowledgement.** The awaited reply, whether the call is made from inside the
run or from the key's executor grain.

**Backpressure and credit.** One call in flight per key, plus the declared bound
across keys, both held by the run. The reply is the grant and no credit message
exists.

**Ordering.** **Per key, in the run's order**, because in-flight per key is one.
Emission across keys is in input order. Orleans documents no pairwise ordering
between activations, so this ordering is the adapter's own doing rather than the
platform's.

**Delivery.** At-most-once from this adapter — the first failure faults the run
and nothing retries.

**Distribution.** Opt-in per occurrence with `distributed: true`. Executors are
keyed `{graph}/{run}/{node}/{key}`, per-run, stateless, and left to activation
collection. Where they are placed is a
[hosting setting](hosting.md#silo-settings). A failure inside a distributed
executor reaches the run as `KeyedExecutionFailedException` naming the executor's
own address — see [errors](errors.md#pipelinerunfailedexception).

**Replay, checkpoint, retry, idempotency.** None; none; none; not enforced.

**Resource bounds.** One credit entry per key *with work in flight*, so the
accounting is bounded by the declared bound and never by the key space.

### `orleans/grain-call-sink@v1`

`OrleansStages.GrainCallSink(binding)`, parameters
`GrainCallSinkParameters(binding, maxInFlight, timeout)`. Registered with
`AddGrainCallSink<TIn>`.

The terminating form of the grain call, and **the only sink in the library that
declares a commit mark.**

**Acknowledgement.** The awaited reply.

**Checkpoint — the commit mark `{"acknowledged":n}`**: how many of its calls have
been answered, advanced *after* the reply is awaited and never on a throw, so a
stored mark describes acknowledged work only. **The mark can lag the truth by up
to `maxInFlight`** — a reply is counted when the window's queue reaches it, not
when it lands — which widens a resume's replay and never narrows it. At a bound of
one the mark is exact.

**What a mark means is exactly the acknowledgement above and no more**: an
answered invocation, not anything the grain did behind it.

**Ordering.** The terminating form orders its effects only at a bound of one.

**Delivery, replay, retry, idempotency.** At-most-once from this adapter; no
replay; no retry; not enforced.

**Cancellation.** Observed between elements, because a terminal is a synchronous
fold handed no token — a call already in flight runs to its own end or to the
declared timeout.

**Resource bounds.** `maxInFlight` calls at once.

### `orleans/reminder-trigger@v1`

`OrleansStages.ReminderTrigger()`, parameters
`ReminderTriggerParameters(period, ingress)`.

**What it connects to.** An Orleans reminder — the durable schedule.

**Acknowledgement.** The offer into the run's bounded ingress and nothing further
downstream.

**Backpressure.** The declared bound, and **the overflow policy may not be
`Backpressure`**: a clock cannot be slowed, and a tick parked in a full queue
would hold the grain turn that owns the cluster's reminder for this run. The
builder refuses it at the call.

**Delivery and ordering.** At-most-once, best effort; the tick index, a `long`
counting the ticks this run received.

**Replay — none, and this is the sharpest fact on this page.** The reminder
*definition* survives a restart and the run does not, so a reminder that should
have fired while nothing was running fires *once* when a silo picks it up again,
and the ticks in between are gone. **The durable half of this stage is a schedule
and never a stream.**

**Checkpoint, retry, idempotency.** None; none; not enforced.

**Period.** Whole milliseconds, and at least the cluster's
`ReminderOptions.MinimumReminderPeriod`, which Orleans enforces by throwing rather
than clamping — so a document below it is refused at materialization, naming the
configured minimum.

**Cleanup.** The reminder is unregistered on every terminal path the run can
reach, and a tick that finds no live run unregisters it from the tick side.

**Resource bounds.** One reminder and one trigger activation per run per
occurrence.

### `orleans/observer@v1`

`OrleansStages.ObserverBridge(binding)`, parameters
`ObserverBridgeParameters(binding, ingress)`. Registered with
`AddObserverBridge<T>`.

**The direction is the other way round from a subscription.** The run publishes a
receiver at `{graph}/{run}/{binding}` — spell the address with
`OrleansStages.ObserverBridgeKey(graphId, runId, bridge)` — and grain code
anywhere in the cluster pushes at that address for as long as the run is
listening.

**Acknowledgement.** The offer into the run's bounded ingress, **reported to the
pusher** as `Accepted`, `Dropped`, `Closed`, or `Failed`. Best effort made
observable, so a caller learns that a run stopped listening rather than guessing.

**Backpressure.** The declared bound, paid by the pusher. Under the backpressuring
policy the push waits for room, and because the bridge grain is not reentrant,
every other pusher waits behind it.

**Delivery.** Best effort — no delivery to a run that has not attached yet or has
already ended.

**Ordering.** One pusher's elements in the order it sent them, because the bridge
serializes pushes. Nothing is ordered across pushers.

**Replay, checkpoint, idempotency.** None — there is no history; none; not
enforced.

**Retry.** None. A receiver whose process is gone hangs the push until Orleans'
response timeout, is then reported `Closed`, and is forgotten — so the cost is
paid once per lost run rather than once per push.

**Resource bounds.** One bridge activation per run per binding, nothing
persisted.

### `orleans/broadcast-source@v1`

`OrleansStages.BroadcastSource(binding)`, parameters
`BroadcastSourceParameters(binding, provider, channel, ingress)`. Registered with
`AddBroadcastElement<T>`.

**The namespace is not yours to choose, and that is the platform's doing.** A
Broadcast Channel is subscribed **implicitly and only implicitly**: a grain *type*
names the namespaces it receives in a compile-time attribute, so nothing can
subscribe to a namespace decided at run time. A dataflow run therefore consumes
channel *keys* inside one package-owned namespace,
`OrleansStages.BroadcastSourceNamespace` — the constant
`"orleans-dataflow-broadcast"` — reached through the relay grain that carries the
attribute, and the document names the key. A deployment that wants its own
namespace consumed writes its own subscriber grain type.

**Backpressure.** A bounded ingress whose overflow policy **may not be
`Backpressure`**: the relay grain forwards to every run listening to the channel
on one non-reentrant turn, so a run waiting for room would stop the channel for
all of them — and under a fire-and-forget provider it would stop it while no
publisher was waiting.

**Acknowledgement.** Delivery into that ingress, and never end-to-end processing.
A publisher learns nothing of it, because `Publish` reports no per-subscriber
outcome.

**Delivery.** Best effort, with two named ways to lose an element: a publication
that arrives with nothing attached is dropped silently, and a publication that
finds a full ingress is dropped or fails by the declared policy.

**Ordering.** One publisher's elements reach one run in publication order, because
the relay is non-reentrant and forwards a publication completely before starting
the next. Nothing is ordered across publishers, and the fan-out across listening
runs is concurrent.

**Replay — none, ever.** A channel keeps no history, so a run that attaches a
moment late is not caught up.

**Checkpoint, retry, idempotency.** None — this adapter owns no cursor; none, and
a receiver that refuses or fails once is forgotten, because an unreachable one
costs the whole response timeout per push; not enforced.

**Serialization.** An element of another type on the same channel key fails the
run that declared the contract, naming both types, while leaving the publisher and
every other listener alone.

**Resource bounds.** One relay activation per channel key, one attach row per
listening run, nothing persisted.

**A channel's identity is a namespace plus a key with no provider in it**, so one
key published through two providers reaches one subscriber activation. Telling
those publications apart is the adapter's work rather than the platform's.

### `orleans/broadcast-sink@v1`

`OrleansStages.BroadcastSink(binding)`, parameters
`BroadcastSinkParameters(binding, channel, fireAndForgetDelivery)`. The sink is
unaffected by the namespace rule above and addresses **any** namespace, because
publishing needs no subscription.

**Acknowledgement.** One awaited `Publish` per element, and what that awaits
depends on the provider. With `FireAndForgetDelivery` off it completes when every
implicit subscriber has handled the element; with it on, when the deliveries have
been dispatched and a subscriber that throws is never reported. Either way it is
publication rather than end-to-end processing. **The declared mode is checked
against the silo's provider at materialization**, because a channel's mode belongs
to the provider and cannot be chosen per publication.

**Backpressure.** The awaited publication.

**Delivery.** Best effort — a channel has no explicit subscription and no
subscriber list a publisher can see, so a publication to a channel nobody listens
to is a success.

**Ordering.** One element at a time in the run's order; nothing is promised across
publishers.

**Replay, checkpoint, retry, idempotency.** None; none; none; not enforced.

**Completion.** A run that ends signals nothing on the channel.

**Resource bounds.** One publication in flight.

```csharp
ElementContract<int> element = OrleansStages.Element<int>();
StreamElementBinding<int> stream = StreamElementBinding.Create(element);
OrleansStreamAddress address = OrleansStreamAddress.Create("memory", "orders", "shard-1");

_ = OrleansStages.StreamSource(stream);
_ = OrleansStages.StreamSourceParameters(stream, address, new BufferOptions { Capacity = 64 });

GrainCallBinding<int, int> call = GrainCallBinding.Create(
    "price",
    element,
    element,
    (grains, n, ct) => Task.FromResult(n));

_ = OrleansStages.GrainCall(call);
_ = OrleansStages.GrainCallParameters(call, maxInFlight: 8, timeout: TimeSpan.FromSeconds(5));
```

---

## Test-support stages

`Orleans.Dataflow.Testing` ships four stages that exist so a *seam* can be proven
where no adapter is available to prove it. They are test surface and belong in
test projects.

| Stage | Spelling | Control it publishes | What it is |
|---|---|---|---|
| marking sink | `TestSink.Marking<T>(controlName, commit)` | `IMarkingSink` | The one local sink that declares a commit mark: **the number of elements whose callback has returned**, advanced *after* the side effect and never before it. A callback that throws leaves the mark where it was. Read the number from `IMarkingSink.Mark`. It is restored across a resume, so the number is the run's rather than the attempt's. |
| sink probe | `TestSink.Probe<T>(controlName)` | `ISinkProbe<T>` | A rendezvous with the elements a graph delivers. |
| source probe | `TestSource.Probe<T>(controlName)` | `ISourceProbe<T>` | A source a test drives element by element. |
| fault point | `TestFlow.FaultPoint<T>(…)` | `IFaultPoint`, on the overloads taking a control name | A flow that throws on a declared arrival, raising [`FaultInjectedException`](errors.md#the-testing-package) unless the test names its own exception. Four overloads, crossing "named control" with "own exception". |

Each control is reached the way any other is, through
[`RunnableGraph.Control<T>(name)`](run-handles.md#result-slots-and-control-slots).

| Control | Members |
|---|---|
| `ISinkProbe<T>` | `ReceiveAsync(ct)` — the next element; `ExpectCompletedAsync(ct)`; `ExpectFailedAsync(ct)`, which answers the exception. Each wait becomes a [`ProbeTerminatedException`](errors.md#the-testing-package) if the run can no longer answer it. |
| `ISourceProbe<T>` | `EmitAsync(element, ct)`, `Complete()`, `Fail(exception)`, and `PullsObserved` — how many times the run asked, which is how a test asserts on [backpressure](glossary.md#backpressure) itself. |
| `IFaultPoint` | `Arm(mode, firstFailure)`, `Disarm()`, and the counters `ElementsSeen` and `FaultsThrown`. |
| `IMarkingSink` | `Mark` — the committed count. |

`FaultPointMode` is `Never`, `Once`, or `Always`, and `firstFailure` is the
one-based arrival the arming starts at.

**The marking sink counts committed *deliveries*, not source positions.** The two
agree only for a graph that neither drops nor multiplies elements between a source
and this sink, and they part company across a resume, because a replayed element
is a second delivery of one element.

Two more test facilities ship beside the stages:

| Type | Members | What it is |
|---|---|---|
| `InMemoryCheckpointStore` | `ReadAsync`, `WriteAsync`, `ClearAsync`, plus `Count`, `Holds(graph, run)`, and `Supersede(graph, run)` | An [`ICheckpointStore`](../operations/checkpoint-stores.md) with a real ETag discipline. `Supersede` moves the stored version without writing, which is how a test provokes a [`CheckpointConflictException`](errors.md#checkpointconflictexception). |
| `TestClock` | `Advance(delta)`, `PendingTimers`, `WaitForTimersAsync(count, ct)`, and the `TimeProvider` overrides `GetUtcNow`, `GetTimestamp`, `TimestampFrequency`, `CreateTimer` | A `TimeProvider` whose `Advance` drives every timing operator in a run. `WaitForTimersAsync` is how a test waits for the run to *have* registered its timers before advancing, which is what makes the sequence deterministic rather than a sleep. |

---

## Facts about Orleans these adapters preserve

Stated here because a caller reasoning about the rows above needs them, and
because they are properties of the platform rather than decisions this library
took.

- **Orleans stream guarantees vary by provider.** Simple message streams and
  broadcast channels are transient and best effort; persistent providers may be
  at-least-once and rewindable under provider-specific rules.
- **An awaited grain call is request/reply, not a durable queue.**
- **A one-way grain call is best effort.**
- **Grain observers are unreliable by design** and require resubscription after
  failure.
- **Timers stop with activation lifetime. Reminder registrations survive restart,
  but missed reminder ticks are not replayed.**
- **Orleans serializer contracts used for runtime transport are not automatically
  the right durable or external storage format.** Checkpoints and external
  envelopes need version-tolerant schema design.

Reference: [Orleans streaming](https://learn.microsoft.com/dotnet/orleans/streaming/),
[stream providers](https://learn.microsoft.com/dotnet/orleans/streaming/stream-providers),
[broadcast channels](https://learn.microsoft.com/dotnet/orleans/streaming/broadcast-channel),
[observers](https://learn.microsoft.com/dotnet/orleans/grains/observers),
[timers and reminders](https://learn.microsoft.com/dotnet/orleans/grains/timers-and-reminders).

---

## Related

- [Hosting](hosting.md) — the registration each adapter needs.
- [Provider SDK](provider-sdk.md) — writing an adapter of your own.
- [Options](options.md#bufferoptions) — the ingress bound every push adapter
  takes.
- [Orleans streams and grains](../guides/orleans-integration.md) — these adapters
  in a working program.
- [Durability](../concepts/durability.md) — what a cursor and a mark buy you.
