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

| Source | Priority | Notes |
|---|---:|---|
| Empty/single/failed/never/repeat/cycle | P0 | Pure lifecycle primitives and deterministic tests. |
| `IEnumerable<T>` | P0 | Enumerator is created and disposed per materialization. |
| `IAsyncEnumerable<T>` | P0 | Cancellation and async disposal flow from the run. |
| Task/deferred factory | P0 | Deferred factory executes once per materialization. |
| Unfold/async unfold | P1 | Explicit state and completion result. |
| Bounded queue | P0 | Offers return accepted/dropped/closed/failed; acceptance is not downstream completion. |
| Bounded `Channel<T>` | P1 | Local bridge only; channel completion is not persistence. |
| Tick/manual clock | P1 | Missed-tick behavior and slow-consumer policy are explicit. |
| Resource unfold | P1 | Resource is closed on completion, cancellation, and failure. |

### Orleans-native sources

| Source | Priority | Notes |
|---|---:|---|
| Orleans Stream subscription | P0 | Delivery, ordering, replay token, and subscription behavior come from the selected provider. |
| Grain `IAsyncEnumerable<T>` | P1 | Call-lifetime backpressure; resume requires an application cursor/checkpoint. |
| Timer | P1 | Activation scoped, non-durable, no replay. |
| Reminder | P1 | Reminder definition survives restarts; ticks missed during downtime are not replayed. |
| Grain observer | P2 | Best effort, no replay, resubscription required. |
| Broadcast Channel | P2 | Fire-and-forget/best effort, no history. |
| Controlled grain group | P1 | A coordinator enumerates bounded keys/partitions; this is not an implicit cluster-wide grain scan. |

### Core local sinks

| Sink | Priority | Notes |
|---|---:|---|
| Ignore/completion | P0 | Materializes terminal completion. |
| First/last | P0 | Early cancellation and empty behavior are explicit. |
| Fold/reduce | P1 | Final result and overflow behavior are specified. |
| Bounded collect | P1 | Requires element or byte cap; never silently accumulates an unbounded list. |
| Sequential callback | P0 | Awaited callback is the processing boundary. |
| Bounded parallel callback | P0 | Ordering and in-flight limit are explicit. |
| Bounded `Channel<T>`/queue | P1 | Write acceptance differs from consumer processing. |

### Orleans-native sinks and flows

| Adapter | Priority | Notes |
|---|---:|---|
| Awaited grain call | P0 | A reply acknowledges that method invocation, not an arbitrary downstream side effect. |
| Keyed grain call | P1 | Bounded parallelism and per-key ordering are first-class options. |
| Orleans Stream publication | P0 | Publication acknowledgement and end-to-end delivery are provider-specific. |
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
| `IObservable<T>`/.NET event source | P2 | Cannot force upstream demand; bounded buffer and overflow are mandatory. |
| Reactive Streams bridge | P2 | Protocol compliance and cancellation/error translation. |

## Orleans facts the API must preserve

- Orleans Stream guarantees vary by provider. Simple Message Streams and Broadcast Channels are transient/best effort; persistent providers may be at-least-once and rewindable under provider-specific rules.
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
