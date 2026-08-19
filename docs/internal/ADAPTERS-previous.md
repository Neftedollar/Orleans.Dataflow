# Source and sink adapter model

Orleans.Dataflow should support many sources and sinks without turning the core package into a collection of unrelated client libraries. This document records the provider boundary and the semantic facts every adapter must expose.

## Adapter contract

Every source and sink adapter must document and expose enough metadata for validation and diagnostics to answer these questions:

| Concern | Required answer |
|---|---|
| Backpressure | Can the upstream system honor demand, or must this adapter use a bounded buffer/drop/fail policy? |
| Acknowledgement | What exact event does a successful read/write await? |
| Delivery | At-most-once, at-least-once, best-effort, or a provider-specific transactional boundary? |
| Ordering | Global, per partition/key, source-provider order, or unspecified? |
| Replay | Is history available, and from which cursor/token/offset? |
| Checkpoint | Who owns the cursor and when may it advance? |
| Batching | What triggers flush and what happens to a partial batch on cancellation/failure? |
| Retry | Which failures are retryable and what duplicate window does retry introduce? |
| Idempotency | Does the adapter enforce it, accept an application key, or require the sink to provide it? |
| Serialization | Which schema identity/version survives storage or external transport? |
| Resource bounds | Maximum partitions, subscriptions, connections, buffers, in-flight requests, and bytes. |

These are stage capabilities and provider facts, not a single global `DeliveryGuarantee` switch. In particular, Orleans.Dataflow does not make a graph “exactly once” by configuration.

Every shipped adapter answers these questions in its row below, and the answers are prose because they are semantics: no test can check that an acknowledgement boundary is where its documentation says it is. What *is* checked mechanically, from M4.5b, is the structural half every adapter shares — that its ports declare real contracts in canonical order, that its payload reader refuses a member the stage does not declare, that its factory answers for every stage its catalog declares and refuses a stranger by name, and that the runtime it builds has the shape its specification declares. `ProviderConformance` in `Orleans.Dataflow.Testing` is that check, and both vocabularies this repository ships run it in their own suites (see [REGISTERED-STAGES.md](REGISTERED-STAGES.md)).

## Package boundary

### Core runtime and abstractions

The base package should contain:

- adapter provider/catalog contracts;
- typed source and sink stage descriptors;
- bounded ingress/egress primitives;
- completion, cancellation, and offer/write outcomes;
- capability and diagnostic metadata;
- no vendor client dependency.

### Orleans-native package

If package size or dependency layering warrants it, the Orleans-specific runtime can be a separate package from the graph abstractions. Its primary adapters are:

- Orleans Streams;
- awaited and keyed grain calls;
- grain `IAsyncEnumerable<T>` methods;
- timer and reminder triggers;
- observer and Broadcast Channel bridges with explicit best-effort semantics.

### Optional provider packages

External systems belong in focused packages such as:

- `Orleans.Dataflow.IO`;
- `Orleans.Dataflow.Http`;
- `Orleans.Dataflow.SignalR`;
- `Orleans.Dataflow.Kafka`;
- provider-specific database/outbox/change-feed packages;
- provider-specific Azure messaging packages when Orleans Stream providers do not already supply the required semantics.

Names are provisional until repository and NuGet namespace checks are performed.

## Priority catalog

### Core local sources

These are engine primitives rather than provider adapters: there is no external system behind an
`IEnumerable<T>`, a bounded queue, or a tick, so the eleven-question table above has nothing to answer for
them beyond what one line says. Their contracts are in the capability matrix's source and sink families and
their bounds are the runtime's own. The questionnaire applies to the rows below that name a system outside
the process, and every one of those answers it.

**One question the local sources do answer, since M5.2, is the checkpoint one**, and it is answered per
source rather than generalized: a source either declares a cursor — a position a checkpoint stores and a
resume reopens at — or it declares none, contributes nothing to a checkpoint, and **resumes from now**. The
"Cursor" note in each row below is that answer. Only one local source declares one, which is the honest
state of the model and not a gap in a table: an index over a re-enumerable sequence is what a local runtime
can prove. **The cursor the model was designed for arrived with M5.3** — a rewindable Orleans stream's
sequence token, in the stream-source row below — and it is the one that shows what the seam is worth,
because rewinding a log is a platform operation rather than a re-enumeration this runtime performs itself.

| Source | Priority | Notes |
|---|---:|---|
| Empty/single/failed/never/repeat/cycle | P0 | Pure lifecycle primitives and deterministic tests. Cursor: none. A source with nothing to remember has no position to store, and `cycle` and `never` have one they could not honor — a cycle's lap number is meaningless against a sequence an author may re-enumerate differently, and a source that never emits has nowhere to be. All of them resume from now, which for these is indistinguishable from starting again. |
| `IEnumerable<T>` | P0 | Enumerator is created and disposed per materialization. **Cursor: an index**, and this is the local proof vehicle of ADR 0007's cursor model. The position is `{"index":n}` — how many elements this source handed the run and that travelled through the segment they entered — and it advances when the *run* has delivered the element rather than when the sequence is asked for the next one, so a capture's position is exact rather than one behind. Reopening re-enumerates the very sequence the author handed over and skips that many elements, which makes the requirement the author's to meet and is stated as such: the sequence has to be re-enumerable and stable, a sequence shorter than the stored position fails the resume by name, and a sequence that enumerates differently the second time resumes into different elements. A source over a list has every business declaring this cursor; one over an iterator that reads a socket has none. |
| `IAsyncEnumerable<T>` | P0 | Cancellation and async disposal flow from the run. Cursor: none. The same index would be spellable and would be a worse promise than the synchronous one: an asynchronous sequence is usually a live feed rather than a re-readable collection, and giving it a position the engine could not honor is exactly the foot-gun the per-source rule exists to avoid. Resumes from now. |
| Task/deferred factory | P0 | Deferred factory executes once per materialization. Cursor: none; the factory runs again in the resumed run, so a resume re-produces the element rather than skipping it. |
| Unfold/async unfold | P1 | Explicit state and completion result. Cursor: none, and the reason is worth naming because this source *has* a state: the generator's state is a value of a type no document names, so storing it needs the codec a durable scope's scan needs, and no spelling asks an author for one here yet. It restarts from its seed on resume. |
| Bounded queue | P0 | Offers return accepted/dropped/closed/failed; acceptance is not downstream completion. Cursor: none, and none is possible — the elements are the producers' and the queue is per run, so a resumed run has a new empty queue and resumes from now. What was in flight when the run died is the producers' to re-offer. |
| Bounded `Channel<T>` | P1 | Local bridge only; channel completion is not persistence. Cursor: none. The reader is external state the author handed over — two runs of one graph compete for its elements — so there is no position this runtime owns. Resumes from now, reading whatever the channel holds then. |
| Tick/manual clock | P1 | Missed-tick behavior and slow-consumer policy are explicit. This is `Source.Tick`, the local operator: a tick that comes due while the run is busy is skipped rather than queued, and the tick's number is the contract, so a consumer that fell behind can see that it did. The registered `dotnet/timer@v1` adapter is a different stage with a different bound and is listed under Orleans-native sources, where its row says so. Cursor: none. Tick zero is due a declared delay after *the run* started, so a resumed run's clock starts again and its tick numbers start at zero; a schedule that must survive a restart is a reminder and not a tick. |
| Resource unfold | P1 | Resource is closed on completion, cancellation, and failure. Cursor: none. |

### Orleans-native sources

| Source | Priority | Notes |
|---|---:|---|
| Orleans Stream subscription | P0 | **Implemented (M3 phase 3).** Acknowledgement: delivery into the run's bounded ingress, and never end-to-end processing — an element this adapter accepted may still be lost by a run that fails afterwards. Backpressure: the declared ingress bound. Under the backpressuring policy a full ingress delays the provider's own pulling agent, which serves a whole queue, so a run that stops draining delays delivery to every consumer of that queue and not only to itself (observed, not deduced); under a dropping policy the delivery is answered at once and the drop is counted; under the failing policy the run faults. Delivery and ordering: the named provider's, reported rather than generalized — Orleans orders one stream from one producer and nothing across producers, and the memory provider is non-durable by design. Replay: **from a stored cursor, since M5.3, and only there.** An ordinary run subscribes without a token and reads what arrives after it subscribed; a run declared durable subscribes at the token its previous attempt recorded. Checkpoint: **the sequence token of the last element the run delivered**, stored as `{"index":n,"sequence":n,"token":"…"}` — the provider's own two numbers, readable by anyone, beside the token itself as the silo serializer's bytes in base64, which is what a reopening subscription needs and is the one value in a checkpoint document that is not portable outside the deployment that wrote it. The position is promoted when the *run* has delivered the element rather than when the subscription received it, because a bounded ingress holds elements the run has not taken and a cursor that counted arrivals would skip them on resume. **The replay window includes the element the cursor names**: a subscription opened at a token receives that element again (probed, not assumed — Orleans exposes no "token plus one"), so a stream source's window is one element wider than an index cursor's. **Two ways it degrades, both stated rather than promised around**: a provider whose `IsRewindable` is false refuses the token and the resumed run fails on its subscription rather than silently reading from now; and a rewindable provider that has *purged* the token — the memory provider empties its queue cache when its last consumer leaves (probed) — has nothing to replay, so how far back a resume can reach is the provider's cache configuration and not this adapter's promise. A run that had delivered nothing stores no position and resumes as a fresh run does. Retry: none. Idempotency: not enforced. Serialization: the element type is a deployment registration (`AddStreamElement`) and the payload carries its contract reference, so a document written against another signature is refused rather than cast. Resource bounds: one subscription per run per occurrence, one ingress of the declared capacity, nothing persisted. |
| Grain `IAsyncEnumerable<T>` | P1 | **Implemented (M3 phase 3).** Acknowledgement: the call-scoped pull — an element is taken when the run asks for it, and Orleans batches the transport underneath at its own default. Backpressure: the enumeration's own, which is why this is the one Orleans source that needs no ingress buffer: a run that stops pulling stops the grain from producing. Delivery: at-most-once within one call; there is no redelivery of an element the run took. Ordering: the grain's own, preserved end to end. Replay: none. Checkpoint: none here — resuming where a previous run stopped needs an application cursor the grain owns, and nothing in this adapter keeps one. Retry: none. Idempotency: not enforced. Cancellation: cooperative and carried by the run's own token; Orleans 10 defaults `MessagingOptions.CancelRequestOnTimeout` to false, so a response timeout does not cancel the grain-side enumeration and a grain that ignores the token delays the run's stop until it next yields. Disposal is awaited on every terminal path. Resource bounds: one enumeration per run per occurrence. |
| Timer | P1 | **Implemented (M3 phase 5), and it ships in the .NET vocabulary rather than the Orleans one** — `dotnet/timer@v1`, because nothing about a periodic tick is an Orleans concept, so one registration serves a silo and an in-process host alike. Acknowledgement: none; a tick is generated rather than delivered. Backpressure: the pull itself and no queue anywhere — the timer is awaited on the run's own source thread, so a run slower than the period simply ticks later. Ticks do not accumulate and none is dropped, because there is no buffer for them to accumulate in. Delivery: at-most-once by construction. Ordering: the tick index, a `long` counting from zero. Replay: none. Checkpoint: none. Retry: none. Idempotency: not enforced. Scope and durability: one run, in memory, non-durable — the run is the activation of this model — created at the run's first pull and disposed on every terminal path. A trigger that must survive a restart is a reminder. Resource bounds: one timer per run per occurrence. |
| Reminder | P1 | **Implemented (M3 phase 5).** Acknowledgement: the offer into the run's bounded ingress and nothing further downstream. Backpressure: the declared bound, and the overflow policy may not be `backpressure` — a clock cannot be slowed, and a tick parked in a full queue would hold the grain turn that owns the cluster's reminder for this run. Delivery: at-most-once, best effort. Ordering: the tick index, a `long` counting the ticks this run received. Replay: none, and this is the row's sharpest fact — the reminder *definition* survives a restart and the run does not, so a reminder that should have fired while nothing was running fires once when a silo picks it up again and the ticks in between are gone. The durable half of this stage is a schedule and never a stream. Checkpoint: none; durable resume is M5's. Retry: none. Idempotency: not enforced. Period: whole milliseconds and at least the cluster's `ReminderOptions.MinimumReminderPeriod`, which Orleans enforces by throwing rather than clamping (probed), so a document below it is refused at materialization naming the configured minimum. Cleanup: the reminder is unregistered on every terminal path the run can reach, and a tick that finds no live run unregisters it from the tick side. Resource bounds: one reminder and one trigger activation per run per occurrence. |
| Grain observer | P2 | **Implemented (M3 phase 4c) as the observer bridge, and the direction is the other way round from a subscription:** the run publishes a receiver at `{graph}/{run}/{binding}`, and grain code anywhere in the cluster pushes at that address for as long as the run is listening. Acknowledgement: the offer into the run's bounded ingress, reported to the pusher as Accepted, Dropped, Closed, or Failed — best effort made observable, so a caller learns that a run stopped listening rather than guessing. Backpressure: the declared bound, paid by the pusher; under the backpressuring policy the push waits for room, and because the bridge grain is not reentrant every other pusher waits behind it. Delivery: best effort — no delivery to a run that has not attached yet or has already ended. Ordering: one pusher's elements in the order it sent them, because the bridge serializes pushes; nothing is ordered across pushers. Replay: none; there is no history. Checkpoint: none. Retry: none — a receiver whose process is gone hangs the push until Orleans' response timeout (thirty seconds by default, measured), is then reported Closed, and is forgotten, so the cost is paid once per lost run rather than once per push. Idempotency: not enforced. Serialization: the element type is a deployment registration (`AddObserverBridge`). Resource bounds: one bridge activation per run per binding, nothing persisted. |
| Broadcast Channel | P2 | **Implemented (M3 phase 4b).** Backpressure: a bounded ingress whose overflow policy may not be `backpressure` — the relay grain forwards to every run listening to the channel on one non-reentrant turn, so a run waiting for room would stop the channel for all of them, and under a fire-and-forget provider it would stop it while no publisher was waiting. Acknowledgement: delivery into that ingress, and never end-to-end processing; a publisher learns nothing of it, because `Publish` reports no per-subscriber outcome. Delivery: best effort, with two named ways to lose an element — a publication that arrives with nothing attached is dropped silently, and a publication that finds a full ingress is dropped or fails by the declared policy. Ordering: one publisher's elements reach one run in publication order, because the relay is non-reentrant and forwards a publication completely before starting the next; nothing is ordered across publishers, and the fan-out across listening runs is concurrent. Replay: none, ever — a channel keeps no history, so a run that attaches a moment late is not caught up. Checkpoint: none; this adapter owns no cursor. Retry: none; nothing is re-sent and a receiver that refuses or fails once is forgotten, because an unreachable one costs the whole response timeout per push. Idempotency: not enforced. Serialization: the element type is a deployment registration (`AddBroadcastElement`), and an element of another type on the same channel key fails the run that declared the contract, naming both types, while leaving the publisher and every other listener alone. Resource bounds: one relay activation per channel key, one attach row per listening run, nothing persisted. Namespace: consumption is confined to one package-owned namespace and the document names a channel key inside it — a property of the platform rather than a choice, spelled out under Orleans facts below. |
| Controlled grain group | P1 | A coordinator enumerates bounded keys/partitions; this is not an implicit cluster-wide grain scan. |

### Core local sinks

**The checkpoint question these answer is the commit mark**, and the answer for every shipping local sink is
the same one: none of them declares one. A fold, a collect, and a callback have no acknowledgement outside
the process to point at, so a mark on one would say only "the run reached this element", which is what a
cursor already says. The one sink that does declare a mark is the testing one below, and it exists so that
the *seam* can be proven where no adapter is available to prove it.

| Sink | Priority | Notes |
|---|---:|---|
| Ignore/completion | P0 | Materializes terminal completion. Commit mark: none. |
| First/last | P0 | Early cancellation and empty behavior are explicit. Commit mark: none. |
| Fold/reduce | P1 | Final result and overflow behavior are specified. Commit mark: none; the result exists only when the run ends, so there is no partial commit to mark. |
| Bounded collect | P1 | Requires element or byte cap; never silently accumulates an unbounded list. Commit mark: none. |
| Sequential callback | P0 | Awaited callback is the processing boundary. Commit mark: none — the callback's own effect is where a commit would be, and the engine cannot know when the author's effect became durable. `TestSink.Marking<T>` is the same sink with the author saying so. |
| Bounded parallel callback | P0 | Ordering and in-flight limit are explicit. Commit mark: none, and one would be harder here than for the sequential form: callbacks complete out of order, so "elements through position P are committed" needs a low-water mark rather than a count. |
| Bounded `Channel<T>`/queue | P1 | Write acceptance differs from consumer processing. Commit mark: none; a write is acceptance and not processing, which is the row's own first sentence. |
| Marking sink (Testing package) | P1 | **Implemented (M5.2)**, and it is test-support surface: `local/marking-sink@v1` in the core vocabulary, `TestSink.Marking<T>` as the only spelling, in `Orleans.Dataflow.Testing`. Commit mark: **the number of elements whose callback has returned**, advanced *after* the side effect and never before it — a callback that throws leaves the mark where it was. The mark counts committed deliveries rather than source positions: the two agree only for a graph that neither drops nor multiplies elements between a source and this sink, and they part company across a resume, because a replayed element is a second delivery of one element. It is restored across a resume, so the number is the run's rather than the attempt's. |

### Orleans-native sinks and flows

| Adapter | Priority | Notes |
|---|---:|---|
| Awaited grain call | P0 | **Implemented (M3 phase 3)**, in a transforming form and a terminating one. Acknowledgement: the awaited reply, which acknowledges that method invocation and nothing the grain may have started behind it. Backpressure and credit: the declared `maxInFlight`; a call in flight is credit spent and its reply is the grant, elements reach the stage through a bounded channel, and nothing on the wire carries credit. Delivery: at-most-once from this adapter — it never retries, and a call that fails faults the run. Ordering: emission is in input order; the calls themselves overlap up to the bound, so the grains see them concurrently and only what leaves the stage is ordered. The terminating form orders its effects only at a bound of one. Replay: none. Checkpoint: **the terminating form declares a commit mark since M5.5** — `{"acknowledged":n}`, how many of its calls have been answered, advanced *after* the reply is awaited and never on a throw, so a stored mark describes acknowledged work only. The mark can lag the truth by up to `maxInFlight` — a reply is counted when the window's queue reaches it, not when it lands — which widens a resume's replay and never narrows it; at a bound of one the mark is exact, and that is the arrangement the crash suite measures. What a mark means is exactly the acknowledgement row above and no more: an answered invocation, not anything the grain did behind it. The transforming form still declares none — its effects flow onward and the stream's own progress is the cursor's to state. Retry: none — a deployment that wants one writes it inside the registered call, where the duplicate window it opens is the deployment's own to state. Idempotency: not enforced, and not enforceable by this adapter. Serialization: the call is a deployment registration (`AddGrainCall`, `AddGrainCallSink`) and the payload carries its input and output contract references, so a document compiled against a different signature is refused. Cancellation: observed between elements for the terminating form, because a terminal is a synchronous fold handed no token — a call already in flight runs to its own end or to Orleans' call timeout. Resource bounds: `maxInFlight` calls at once. |
| Keyed grain call | P1 | **Implemented (M3 phase 4a).** Acknowledgement: the awaited reply, whether the call is made from inside the run or from the key's executor grain. Backpressure and credit: one call in flight per key plus the declared bound across keys, both held by the run — the reply is the grant and no credit message exists. Ordering: per key, in the run's order, because in-flight per key is one; probed, because Orleans documents no pairwise ordering between activations and was measured reordering pipelined calls within a single silo. Emission across keys is in input order. Delivery: at-most-once from this adapter — the first failure faults the run and nothing retries. Distribution: opt-in per occurrence; executors are keyed `{graph}/{run}/{node}/{key}`, per-run, stateless, and left to activation collection. Placement of those executors is a hosting option. Resource bounds: one credit entry per key with work in flight, so the accounting is bounded by the declared bound and never by the key space. |
| Orleans Stream publication | P0 | **Implemented (M3 phase 3).** Acknowledgement: one awaited `OnNextAsync` per element — publication, and never end-to-end delivery; what a consumer then does with the element is between the consumer and the provider. Backpressure: the awaited publication itself, so a slow provider slows the run rather than filling a queue. Delivery and ordering: the provider's; elements are published one at a time in the order the run produced them, which is the strongest order this adapter can offer because Orleans orders one stream from one producer. Replay: none. Checkpoint: none; this adapter owns no cursor. Retry: none. Idempotency: not enforced. Serialization: the element type is a deployment registration (`AddStreamElement`) and the payload carries its contract reference. Completion: a run that ends signals nothing on the stream, because a stream has no end this publisher owns; a run that fails likewise leaves it alone. Cancellation: observed between elements, for the same terminal-seam reason the grain-call sink states. Resource bounds: one element in flight. |
| Broadcast Channel publication | P2 | **Implemented (M3 phase 4b).** Acknowledgement: one awaited `Publish` per element, and what that awaits depends on the provider — with `FireAndForgetDelivery` off it completes when every implicit subscriber has handled the element, with it on when the deliveries have been dispatched and a subscriber that throws is never reported. Either way it is publication rather than end-to-end processing. The declared mode is checked against the silo's provider at materialization, because a channel's mode belongs to the provider and cannot be chosen per publication. Backpressure: the awaited publication. Delivery: best effort — a channel has no explicit subscription and no subscriber list a publisher can see, so a publication to a channel nobody listens to is a success. Ordering: one element at a time in the run's order; nothing is promised across publishers. Replay: none; a channel keeps no history. Checkpoint: none. Retry: none. Idempotency: not enforced. Serialization: the element type is a deployment registration (`AddBroadcastElement`). Completion: a run that ends signals nothing on the channel. Resource bounds: one publication in flight. |
| One-way grain call | P2 | Explicit best-effort adapter; never the default durable sink. |
| Grain observer callback | P2 | Best effort; disconnect and resubscription behavior are surfaced. |

### External adapters

| Adapter | Priority | Required design focus |
|---|---:|---|
| Kafka | P1 | Partition assignment, offsets, consumer groups, commit timing, replay, rebalance, duplicate handling, transactions. |
| Relational database/outbox | P1 | Transaction boundary, idempotency key, batching, conflict handling, checkpoint/outbox ownership. |
| Database change feed | P1 | Provider cursor, schema evolution, retention, partition ordering, resume. |
| HTTP polling/pagination | P2 | Cursor/ETag, rate limits, timeouts, retries, cancellation, deduplication. |
| HTTP/serverless/webhook sink | P2 | Reused clients, timeout, idempotency key, retry policy, response classification. |
| SSE source | P2 | Reconnect cursor, heartbeat timeout, duplicate window, bounded parsing. |
| SignalR source/sink | P2 | Connection-scoped lifecycle, cancellation, reconnect/application sequence IDs, bounded channel. |
| File/stream/`PipeReader` source | P2 | Silo-local versus shared ownership, offset, partial record, rotation, truncation, async disposal. |
| File/stream/`PipeWriter` sink | P2 | Flush/fsync boundary, atomic replacement/append, partial writes, rotation. |
| Azure Queue/Event Hubs | P2 | Prefer Orleans stream-provider integration where it preserves semantics; provider-specific package otherwise. |
| `IObservable<T>`/.NET event source | P2 | **Implemented (M3 phase 5) in the .NET vocabulary as `dotnet/observable@v1`**, so one registration serves a silo and an in-process host alike. Acknowledgement: the offer into the run's bounded ingress, and not end-to-end processing. Backpressure: the declared bound, paid by the notification's own thread — `OnNext` returns `void` and has nothing to await, so under the backpressuring policy a full ingress blocks whichever thread the observable pushes on until the run makes room; a producer that cannot pay that declares a dropping policy and the drop is counted, and the failing policy faults the run. Delivery: best effort; an element the ingress accepted may still be lost by a run that fails, and a graceful shutdown abandons whatever the ingress still holds. Ordering: whatever the observable gives — `IObserver<T>` requires notifications to be serialized and this adapter preserves that order into the ingress. Replay: none. Checkpoint: none. Retry: none. Idempotency: not enforced. Subscription lifetime: one run, made at the run's first pull and disposed in the `finally` the engine reaches on every terminal path; a cold observable therefore gets one producer per run and a hot one shares its elements between concurrent runs, which is the observable's own character. Serialization: the observable is a deployment registration (`AddObservable`) and the payload carries its element contract reference. **A .NET event is deliberately not a second stage**: it is one adapter away from an `IObservable<T>` — add a handler on subscribe, remove it on dispose — and a stage for it would be a second registration surface and a second set of lifetime rules for the same delivery semantics. |
| Reactive Streams bridge | P2 | Protocol compliance and cancellation/error translation. |

## Orleans facts the API must preserve

- Orleans Stream guarantees vary by provider. Simple Message Streams and Broadcast Channels are transient/best effort; persistent providers may be at-least-once and rewindable under provider-specific rules.
- A Broadcast Channel is subscribed **implicitly and only implicitly**: a grain *type* names the namespaces it receives in a compile-time attribute, so nothing subscribes to a namespace decided at run time. Consuming an arbitrary namespace of a deployment's choosing is therefore not something any design can offer — it is a property of the platform and not a decision taken by this package. A dataflow run consumes channel keys inside one package-owned namespace (`orleans-dataflow-broadcast`), reached through the relay grain that carries the attribute, and the document names the **key**. The Broadcast Channel *sink* is unaffected and addresses any namespace, because publishing needs no subscription. A deployment that wants its own namespace consumed writes its own subscriber grain type.
- A channel's identity is a namespace plus a key with no provider in it — so one key published through two providers reaches one subscriber activation, and telling those publications apart is the adapter's work rather than the platform's. Under a provider configured for checked delivery, a subscriber that throws fails the publisher; under fire-and-forget it is invisible. Both were probed rather than read.
- An awaited grain call is request/reply, not a durable queue.
- A one-way grain call is best effort.
- Grain observers are unreliable by design and require resubscription after failure.
- Timers stop with activation lifetime. Reminder registrations survive restart, but missed reminder ticks are not replayed.
- Orleans serializer contracts used for runtime transport are not automatically the right durable or external storage format. Checkpoints and external envelopes require version-tolerant schema design.

## Primary references

- [Orleans streaming](https://learn.microsoft.com/en-us/dotnet/orleans/streaming/)
- [Orleans stream providers](https://learn.microsoft.com/en-us/dotnet/orleans/streaming/stream-providers)
- [Orleans Broadcast Channels](https://learn.microsoft.com/en-us/dotnet/orleans/streaming/broadcast-channel)
- [Orleans grains](https://learn.microsoft.com/en-us/dotnet/orleans/grains/)
- [Orleans observers](https://learn.microsoft.com/en-us/dotnet/orleans/grains/observers)
- [Orleans timers and reminders](https://learn.microsoft.com/en-us/dotnet/orleans/grains/timers-and-reminders)
- [.NET channels](https://learn.microsoft.com/en-us/dotnet/core/extensions/channels)
- [.NET HTTP client guidelines](https://learn.microsoft.com/en-us/dotnet/fundamentals/networking/http/httpclient-guidelines)
- [ASP.NET Core SignalR streaming](https://learn.microsoft.com/en-us/aspnet/core/signalr/streaming?view=aspnetcore-10.0)
- [Orleans serialization](https://learn.microsoft.com/en-us/dotnet/orleans/host/configuration-guide/serialization)

The adapter list can grow. The semantic questionnaire cannot be skipped.
