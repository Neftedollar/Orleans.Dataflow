using System.Collections.Concurrent;
using System.Globalization;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// A checkpoint store that keeps its documents in memory and enforces the ETag the contract is built on.
/// </summary>
/// <remarks>
/// <para>
/// <b>It is a store and not a mock</b>, which is the same distinction the multi-silo tests' coordinator
/// store draws and it is drawn for the same reason: the property the checkpoint model rests on is optimistic
/// concurrency, so an implementation that accepted every write would let a test prove nothing about the one
/// thing that matters. A write presenting an ETag the store no longer holds is refused with
/// <see cref="CheckpointConflictException"/>, exactly as an ETag-enforcing grain store refuses a superseded
/// activation.
/// </para>
/// <para>
/// <b>Nothing aliases.</b> A <see cref="CanonicalJsonValue"/> is immutable and holds its own bytes, so a
/// document handed to <see cref="WriteAsync"/> and read back later is the same value whatever the writer
/// did with its own copy afterwards. That is the round trip the coordinator store buys with a serializer,
/// bought here by the payload type instead — which is the canonical plane's whole point.
/// </para>
/// <para>
/// <b>The ETag is a counter rendered as text, and callers must treat it as opaque.</b> Comparing two of them
/// for order is a mistake this implementation happens to reward and a real store would punish; only equality
/// means anything.
/// </para>
/// <para>
/// It ships in the testing package because a durable store is a deployment's — exactly as the coordinator's
/// is. What is here is the in-memory implementation that belongs beside a test store: it keeps every
/// checkpoint in this process and nothing it holds outlives the process that wrote it.
/// </para>
/// <para>
/// Every member is safe to call from any thread. One instance serves any number of runs and any number of
/// graphs; two runs under one identity are two writers of one document, which is the case the ETag exists
/// for and which it is safe to rely on.
/// </para>
/// </remarks>
public sealed class InMemoryCheckpointStore : ICheckpointStore
{
    private readonly ConcurrentDictionary<Key, Stored> _documents = new();

    /// <summary>Gets the number of checkpoint documents this store currently holds.</summary>
    /// <value>One per <c>(graph, run)</c> pair that has been written and not cleared.</value>
    public int Count => _documents.Count;

    /// <summary>Reports whether this store holds anything for one run.</summary>
    /// <param name="graph">The graph identity.</param>
    /// <param name="run">The run identity.</param>
    /// <returns><see langword="true"/> when a document is held for that pair.</returns>
    /// <remarks>
    /// For the assertion "a run that declared no checkpoint timing never touched the store", which is a
    /// statement about the store rather than about the run and can therefore only be made here.
    /// </remarks>
    public bool Holds(GraphId graph, RunId run) => _documents.ContainsKey(new Key(graph, run));

    /// <inheritdoc/>
    public ValueTask<StoredCheckpoint?> ReadAsync(
        GraphId graph,
        RunId run,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return new ValueTask<StoredCheckpoint?>(
            _documents.TryGetValue(new Key(graph, run), out Stored? stored)
                ? new StoredCheckpoint(stored.Document, stored.ETag)
                : null);
    }

    /// <inheritdoc/>
    public ValueTask<string> WriteAsync(
        GraphId graph,
        RunId run,
        CanonicalJsonValue checkpoint,
        string? expectedETag,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Key key = new(graph, run);

        // The whole update is taken under one lock rather than through a compare-and-swap, because the check
        // and the write have to be one step: two writers that both read "version 4" and both wrote would
        // otherwise produce a store at version 6 with one of the two documents silently gone, which is
        // exactly the corruption the ETag is here to refuse.
        lock (_documents)
        {
            Stored? stored = _documents.TryGetValue(key, out Stored? found) ? found : null;

            if (!string.Equals(expectedETag, stored?.ETag, StringComparison.Ordinal))
            {
                throw CheckpointConflictException.Superseded(graph, run, expectedETag, stored?.ETag);
            }

            Stored next = new(checkpoint, (stored?.Version ?? 0L) + 1L);

            _documents[key] = next;

            return new ValueTask<string>(next.ETag);
        }
    }

    /// <inheritdoc/>
    public ValueTask ClearAsync(
        GraphId graph,
        RunId run,
        string? expectedETag,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        Key key = new(graph, run);

        lock (_documents)
        {
            Stored? stored = _documents.TryGetValue(key, out Stored? found) ? found : null;

            if (!string.Equals(expectedETag, stored?.ETag, StringComparison.Ordinal))
            {
                throw CheckpointConflictException.Superseded(graph, run, expectedETag, stored?.ETag);
            }

            _ = _documents.TryRemove(key, out _);

            return default;
        }
    }

    /// <summary>Writes one run's document behind its back, as a competing attempt's write would.</summary>
    /// <param name="graph">The graph identity.</param>
    /// <param name="run">The run identity.</param>
    /// <exception cref="InvalidOperationException">The store holds nothing for that pair.</exception>
    /// <remarks>
    /// The only honest way to produce a real ETag conflict against a live run, and it is the coordinator
    /// store's <c>Supersede</c> read over a checkpoint: a test cannot stage two attempts of one run inside
    /// one process, but it can put the store into exactly the state a second attempt would leave it in — the
    /// same bytes under a newer ETag — and then watch the live run discover that at its next capture.
    /// </remarks>
    public void Supersede(GraphId graph, RunId run)
    {
        Key key = new(graph, run);

        lock (_documents)
        {
            if (!_documents.TryGetValue(key, out Stored? stored))
            {
                throw new InvalidOperationException(
                    $"The store holds no checkpoint for the run '{run}' of the graph '{graph}', so there is nothing for a competing writer to supersede. A run writes its first checkpoint once its declared timing has made one due; supersede it after that and not before.");
            }

            _documents[key] = new Stored(stored.Document, stored.Version + 1L);
        }
    }

    /// <summary>What the store holds for one run.</summary>
    /// <param name="Document">The stored checkpoint.</param>
    /// <param name="Version">The version the ETag is the text of.</param>
    private sealed record Stored(CanonicalJsonValue Document, long Version)
    {
        /// <summary>Gets the ETag a reader is handed and a writer must present.</summary>
        internal string ETag { get; } = Version.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>What one checkpoint is addressed by.</summary>
    /// <param name="Graph">The graph the run belongs to.</param>
    /// <param name="Run">The run.</param>
    private readonly record struct Key(GraphId Graph, RunId Run);
}
