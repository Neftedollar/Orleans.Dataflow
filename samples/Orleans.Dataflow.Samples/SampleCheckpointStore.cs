using System.Collections.Concurrent;
using System.Globalization;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Samples;

/// <summary>
/// Where the durable scenario's checkpoints are kept: a dictionary in this process, with a real ETag.
/// </summary>
/// <remarks>
/// <para>
/// <b>What a store owes the runtime, and which of it this one honors.</b> The interface has three duties and
/// this implementation is honest about exactly two of them.
/// </para>
/// <list type="bullet">
/// <item>
/// <b>Atomic per document — honored.</b> A checkpoint is one value replaced under one lock, so no reader
/// ever sees half of one. That is the duty a real store discharges with a single-document write, and it is
/// the one a store built out of several rows would have to work for.
/// </item>
/// <item>
/// <b>Compare-and-swap on the ETag — honored, and this is the duty worth implementing carefully.</b> A
/// writer presents the ETag it last saw; if the stored one has moved on, the write is refused with
/// <see cref="CheckpointConflictException"/> and the writer has lost the run to somebody else. Getting this
/// wrong is not a performance problem: two attempts of one run would interleave their snapshots into a
/// document describing neither, and a resume would restore a position no attempt was ever at.
/// </item>
/// <item>
/// <b>Destructive clear — faked, in the only sense that matters here.</b> The removal itself is real and
/// fenced by the same ETag, but "destroyed" for a dictionary means the entry is gone from this process. A
/// deployment whose store keeps versions, soft deletes, or backups has more to say about what a clear
/// means, and should say it here.
/// </item>
/// </list>
/// <para>
/// What this store is <em>not</em> is durable, which is the whole of what a single-process demonstration
/// gives up: the process ending takes every checkpoint with it. That is why the durable scenario stands up a
/// second host rather than a second process — the resume is real, and the store surviving is pretended.
/// </para>
/// <para>
/// It is also, at fifty lines, the whole of what a deployment must implement. Point a real one at a document
/// database or a blob store with an ETag and the runtime above it does not change.
/// </para>
/// </remarks>
internal sealed class SampleCheckpointStore : ICheckpointStore
{
    /// <summary>One entry per run, keyed by the pair the interface addresses.</summary>
    private readonly ConcurrentDictionary<(GraphId Graph, RunId Run), StoredCheckpoint> _checkpoints = new();

    /// <summary>What the next ETag is made of.</summary>
    /// <remarks>
    /// A counter rather than a hash of the content, because two identical checkpoints written in sequence
    /// are still two writes and a reader that could not tell them apart would not be fencing anything.
    /// </remarks>
    private long _revisions;

    /// <inheritdoc/>
    public ValueTask<StoredCheckpoint?> ReadAsync(
        GraphId graph,
        RunId run,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Holding nothing is an ordinary answer: a run that has not reached its first capture has written
        // nothing, and resuming one starts from the beginning.
        return ValueTask.FromResult<StoredCheckpoint?>(
            _checkpoints.TryGetValue((graph, run), out StoredCheckpoint stored) ? stored : null);
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

        string next = Interlocked.Increment(ref _revisions).ToString(CultureInfo.InvariantCulture);
        StoredCheckpoint written = new() { Document = checkpoint, ETag = next };

        lock (_checkpoints)
        {
            Fence(graph, run, expectedETag);

            _checkpoints[(graph, run)] = written;
        }

        return ValueTask.FromResult(next);
    }

    /// <inheritdoc/>
    public ValueTask ClearAsync(
        GraphId graph,
        RunId run,
        string? expectedETag,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_checkpoints)
        {
            Fence(graph, run, expectedETag);

            _ = _checkpoints.TryRemove((graph, run), out _);
        }

        return ValueTask.CompletedTask;
    }

    /// <summary>Refuses a caller whose ETag is not the one the store holds.</summary>
    /// <param name="graph">The graph identity.</param>
    /// <param name="run">The run identity.</param>
    /// <param name="expectedETag">What the caller last saw, or null when it believes nothing is stored.</param>
    /// <exception cref="CheckpointConflictException">The caller has been superseded.</exception>
    /// <remarks>
    /// Called under the lock that the write or the removal then happens under, because a check that is not
    /// atomic with the thing it guards guards nothing.
    /// </remarks>
    private void Fence(GraphId graph, RunId run, string? expectedETag)
    {
        string? held = _checkpoints.TryGetValue((graph, run), out StoredCheckpoint stored) ? stored.ETag : null;

        if (!string.Equals(held, expectedETag, StringComparison.Ordinal))
        {
            throw CheckpointConflictException.Superseded(graph, run, expectedETag, held);
        }
    }
}
