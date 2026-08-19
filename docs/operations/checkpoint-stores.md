# Checkpoint stores

A [checkpoint store](../reference/glossary.md#checkpoint-store) is where a
[durable run](../reference/glossary.md#durable-run) keeps its position. It is
three methods, and the duties are the contract: **a store that shirks any of them
turns at-least-once into silent loss.**

If you are here because a store is misbehaving, jump to
[What the library does when your store misbehaves](#what-the-library-does-when-your-store-misbehaves)
— that section is the one an incident needs.

## The interface

```csharp
public interface ICheckpointStore
{
    ValueTask<StoredCheckpoint?> ReadAsync(
        GraphId graph,
        RunId run,
        CancellationToken cancellationToken = default);

    ValueTask<string> WriteAsync(
        GraphId graph,
        RunId run,
        CanonicalJsonValue checkpoint,
        string? expectedETag,
        CancellationToken cancellationToken = default);

    ValueTask ClearAsync(
        GraphId graph,
        RunId run,
        string? expectedETag,
        CancellationToken cancellationToken = default);
}
```

One document per `(graph, run)` pair. The value is canonical UTF-8 JSON and never
an object — no CLR type name reaches a store through this interface, which is what
lets one process's store hold another process's checkpoint.

**It must be safe to call from any thread.** One run's capture loop is the only
writer of that run's document, but a resumed attempt reads while a stale one may
still be writing — which is the whole point of the
[ETag](../reference/glossary.md#etag).

## The three duties

### 1. `WriteAsync` is atomic per document

A reader never observes a torn checkpoint. It sees the previous document or the
new one, whole.

**If you do not honour it:** a resume restores half of one position and half of
another, which is a position no attempt was ever at. The cursors and the marks
would disagree, and the run would either skip elements or replay a window nobody
can compute.

This is the one duty no test can hold for you. The `InMemoryCheckpointStore`
shipped for tests lives in the test process and cannot be torn by a silo dying,
so the contract states the duty and **your implementation carries it**. A store
built out of several rows has to work for it; a store with a single-document
write gets it free.

### 2. `WriteAsync` is a compare-and-swap on the ETag

A write presenting a stale ETag throws `CheckpointConflictException`. That
refusal is load bearing: it is how a superseded attempt — a zombie writer on a
silo the cluster has moved past — is fenced out.

**If you do not honour it** — if your store "helpfully" last-writer-wins — two
attempts of one run interleave their snapshots into a document describing
neither, and a resume restores a position no attempt was ever at. You re-open the
very race the epoch protocol closes.

The ETag is opaque. **Never compare ETags for order, only for equality.** A
counter is a fine implementation, and a better one than a hash of the content:
two identical checkpoints written in sequence are still two writes, and a reader
that could not tell them apart would not be fencing anything.

`expectedETag` is `null` when the writer believes the store holds nothing for
that pair. A fresh run always presents `null` at its first capture, which is why
starting fresh over a name that already holds a position is refused rather than
silently destructive.

### 3. `ClearAsync` is destructive, and honours the same ETag discipline

Clearing is the destructive half of
[replacement](runbooks.md#replacing-a-durable-run) and of
[retirement](runbooks.md#retiring-a-run-identity). What the name held is gone.

**If you do not honour it** — if your store soft-deletes, keeps versions, or has
a backup that a read would fall through to — a replacement leaves the old
position reachable, and the "fresh" run continues the run it was meant to
destroy. A deployment whose store keeps history has more to say about what a
clear means, and has to say it in the implementation.

Clearing a pair the store already holds nothing for is **not** an error when the
caller presented `null`, for the reason reading one is not: "there is nothing
here" is the state the call was asking for.

## Implementing one

The reference implementation is fifty lines and lives in the samples:
[`samples/Orleans.Dataflow.Samples/SampleCheckpointStore.cs`](../../samples/Orleans.Dataflow.Samples/SampleCheckpointStore.cs).
Its own documentation says which of the three duties it honours and which it
fakes for a single-process demonstration — worth reading before you write yours.

Over a real document store, the mapping is mechanical. Anything that can refuse a
stale writer works:

| Store | ETag is | The conditional write |
|---|---|---|
| Azure Blob Storage | The blob's ETag | `If-Match` on upload; a `412` is the conflict. |
| Azure Cosmos DB | The item's `_etag` | `ItemRequestOptions.IfMatchEtag`; a `412` is the conflict. |
| Amazon S3 | The object's ETag | A conditional `PutObject`; the precondition failure is the conflict. |
| SQL Server / PostgreSQL | A `rowversion` / `xmin`, or a version column you bump | `UPDATE … WHERE id = @id AND version = @expected`; zero rows affected is the conflict. |
| DynamoDB | A version attribute | A conditional expression on that attribute; the condition-check failure is the conflict. |

The shape, whichever you pick:

```csharp
public async ValueTask<string> WriteAsync(
    GraphId graph,
    RunId run,
    CanonicalJsonValue checkpoint,
    string? expectedETag,
    CancellationToken cancellationToken = default)
{
    try
    {
        // One conditional write of one document. Not a read-then-write: the check has to be atomic
        // with the thing it guards, or it guards nothing.
        return await _store.PutIfMatchAsync(Key(graph, run), checkpoint.ToString(), expectedETag, cancellationToken);
    }
    catch (PreconditionFailed)
    {
        // The one exception type the runtime treats as "somebody else owns this run".
        throw CheckpointConflictException.Superseded(graph, run, expectedETag, held: null);
    }

    // Anything else — a timeout, a throttle, a transport error — travels out as itself. Do not
    // convert it into a conflict: the runtime tells the two apart and treats them very differently.
}
```

Five things to get right, in order of how much they cost when wrong:

1. **Throw `CheckpointConflictException` for a precondition failure and nothing
   else.** This is the decision the whole page turns on.
2. **Let every other failure out as itself.** A timeout dressed up as a conflict
   retires a run that only needed a retry.
3. **Do the check and the write as one operation.** A read, a compare, and a
   write is three operations and fences nothing.
4. **Return the new ETag**, which the writer presents next time.
5. **Size for the document, not for the element.** A checkpoint holds every
   source's cursor, every durable scope's state, and every sink's mark. It is
   small for most graphs and is not bounded by the library.

Test yours against the two failure modes rather than only the happy path. The
shipped
[`DurableStoreOutageTests`](../../tests/Orleans.Dataflow.OrleansTests/Cluster/DurableStoreOutageTests.cs)
does exactly that with a store that can be told to refuse a fixed number of
writes, and it is a good template.

## What the library does when your store misbehaves

**This is the single most important operational fact on the page.**

A store can give three answers, they are three different facts, and **only the
store can tell them apart**:

| The store says | The runtime concludes | What it does |
|---|---|---|
| **Accepted** | Progress. | Carries on. |
| **`CheckpointConflictException`** | *Somebody else owns this run now.* | Fails the attempt **immediately**. No retry. |
| **Anything else** (timeout, throttle, transport error) | *Nothing at all about ownership.* | Retries the same document, then fails the attempt if it never lands. |

### A refusal is not retried

A `CheckpointConflictException` says somebody else owns this run, so the stale
writer dies at its first refusal. Retrying it would overwrite the position a
fresh attempt is building with a snapshot of a run that no longer owns anything.

Measured as a number rather than as a length of time: the capture that meets a
conflict asks the store **once**
(`DurableStoreOutageTests.ARefusedWriteStillKillsTheAttemptOnItsFirstRefusalAndIsNotRetried`).

### A non-answer is retried

The same document is presented again five times over roughly four seconds —
0.1 s, 0.3 s, 0.9 s, 2.7 s — **inside the capture's hold**. The run is stalled
for the whole of it, and the cost shows up in the
`orleans.dataflow.checkpoint.hold.duration` histogram, which is where a
deployment discovers that its store is slow.

A store that misses one write is absorbed entirely: every element is delivered
once, in order, the store holds a position, and the declaration records that the
run completed
(`DurableStoreOutageTests.AStoreThatMissesOneWriteIsRetriedAndTheRunNeverNoticesTheOutage`).

### What happens when four seconds are not enough

The part worth knowing, and the reason the distinction above matters:

- **The attempt ends.** Its completion faults with
  `CheckpointWriteFailedException`, carrying your store's own exception as the
  cause — so the caller learns which store failed and how.
- **Nothing is written down as the run's outcome**, so the name is **not**
  retired. The declaration stays open, with an attempt that stranded, which is a
  different fact from a run that finished.
- **The position the store did accept is still there**, untouched by the failed
  writes.

**So the recovery is to re-declare the run and start it again**, which resumes it
from the last checkpoint the store accepted. It takes a fresh
[epoch](../reference/glossary.md#epoch), exactly as a resume after a silo death
does.

**Do not reach for `ReplaceDurableRunAsync` here.** Replacing clears the
checkpoint — the one thing an outage did not damage — and a long pipeline would
pay for a store hiccup with all of its progress, through an operator action that
reads like recovery. The whole procedure is in
[Recovering from a checkpoint store outage](runbooks.md#recovering-from-a-checkpoint-store-outage).

Until something re-declares it, a stranded run keeps answering with its failure
rather than healing on its own. That is deliberate: the caller has to be able to
see what the store did.

All three behaviours are proven by
[`DurableStoreOutageTests`](../../tests/Orleans.Dataflow.OrleansTests/Cluster/DurableStoreOutageTests.cs).

## What a store cannot give you

- **A run surviving the loss of its store.** The store *is* the durability. Its
  availability, its retention, and its backups are the deployment's.
- **Exactly-once.** Between commit marks the promise is
  [at-least-once](../reference/glossary.md#at-least-once), and every stronger
  claim is a specific adapter's, stated on its row with its window.
- **Cross-document migration.** A checkpoint is written for one document. A run
  identity holding a different document is refused, by name and with both
  fingerprints.

## Next

- [Durable runs](../guides/durable-runs.md) — the store from the author's side, with a whole program.
- [Runbooks](runbooks.md) — recovering from an outage, step by step.
- [Monitoring](monitoring.md) — the hold histogram, and what a rising one means.
