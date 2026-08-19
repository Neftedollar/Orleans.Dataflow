using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// Where a run's checkpoints are kept, and the fencing that decides which writer still owns them.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is the coordinator store's shape generalized</b>, and the generalization is the contract rather
/// than a convenience: one document per <c>(graph, run)</c> pair, read with the ETag it currently carries,
/// written only by a writer presenting that ETag, and refused loudly when the ETag has moved on. Against a
/// real ETag-enforcing store that buys exactly one thing — a superseded activation's write fails and the
/// fresh one's truth survives — and a checkpoint needs exactly the same property for exactly the same
/// reason: two attempts of one run must not be able to interleave their snapshots into a document that
/// describes neither.
/// </para>
/// <para>
/// <b>The key is a pair and the word "checkpoint" is not part of it.</b> A grain store addresses several
/// states of one grain by name, so a coordinator's key reads <c>(GraphId, RunId, "checkpoint")</c>;
/// this interface holds checkpoints and nothing else, so the third component is the interface
/// rather than an argument. An implementation over a store that needs the name supplies its own.
/// </para>
/// <para>
/// <b>The value is a canonical value and never an object.</b> What is stored is the document
/// <see cref="LocalDataflowHost"/> writes: canonical UTF-8 JSON whose members are the five parts of a
/// checkpoint — the graph fingerprint, the revision, the per-source cursors, the per-scope durable state, and
/// the per-sink commit marks. No CLR type name reaches a store through this interface, which is what lets one
/// deployment's store hold another process' checkpoint (the wire discipline, unchanged).
/// </para>
/// <para>
/// <b>Threading.</b> An implementation must be safe to call from any thread. One run's capture loop is the
/// only writer of that run's document, but a resumed attempt reads while a stale one may still be writing,
/// which is the whole point of the ETag.
/// </para>
/// </remarks>
public interface ICheckpointStore
{
    /// <summary>Reads the checkpoint one run has written, if it has written one.</summary>
    /// <param name="graph">The graph identity the run belongs to.</param>
    /// <param name="run">The run identity, which a resume continues rather than replaces.</param>
    /// <param name="cancellationToken">A token that stops this read.</param>
    /// <returns>
    /// The stored document and the ETag it carries, or <see langword="null"/> when the store holds nothing
    /// for that pair.
    /// </returns>
    /// <remarks>
    /// Holding nothing is an ordinary answer and never an error: a run that has not reached its first
    /// capture has written nothing, and a resume of one is a run that starts from the beginning.
    /// </remarks>
    ValueTask<StoredCheckpoint?> ReadAsync(GraphId graph, RunId run, CancellationToken cancellationToken = default);

    /// <summary>Writes one checkpoint, refusing the write when somebody else has written since.</summary>
    /// <param name="graph">The graph identity the run belongs to.</param>
    /// <param name="run">The run identity.</param>
    /// <param name="checkpoint">The document to store, in canonical form.</param>
    /// <param name="expectedETag">
    /// The ETag the writer last saw, or <see langword="null"/> when it believes the store holds nothing for
    /// this pair.
    /// </param>
    /// <param name="cancellationToken">A token that stops this write.</param>
    /// <returns>The ETag the stored document now carries, which the writer presents next time.</returns>
    /// <exception cref="CheckpointConflictException">
    /// <paramref name="expectedETag"/> is not the ETag the store holds, so this writer has been superseded.
    /// </exception>
    /// <remarks>
    /// The refusal is the contract and not an implementation detail. A writer whose write is refused has
    /// lost the run to somebody else and must stop rather than retry with the fresh ETag: retrying would
    /// overwrite the truth the fresh attempt is building with a snapshot of a run that no longer owns
    /// anything.
    /// </remarks>
    ValueTask<string> WriteAsync(
        GraphId graph,
        RunId run,
        CanonicalJsonValue checkpoint,
        string? expectedETag,
        CancellationToken cancellationToken = default);

    /// <summary>Forgets one run's checkpoint, refusing when somebody else has written since.</summary>
    /// <param name="graph">The graph identity the run belongs to.</param>
    /// <param name="run">The run identity.</param>
    /// <param name="expectedETag">
    /// The ETag the caller last saw, or <see langword="null"/> when it believes the store holds nothing.
    /// </param>
    /// <param name="cancellationToken">A token that stops this call.</param>
    /// <returns>A task that completes when the store holds nothing for that pair.</returns>
    /// <exception cref="CheckpointConflictException">
    /// <paramref name="expectedETag"/> is not the ETag the store holds.
    /// </exception>
    /// <remarks>
    /// Clearing a pair the store already holds nothing for is not an error when the caller presented
    /// <see langword="null"/>, for the reason reading one is not: "there is nothing here" is the state the
    /// call was asking for.
    /// </remarks>
    ValueTask ClearAsync(
        GraphId graph,
        RunId run,
        string? expectedETag,
        CancellationToken cancellationToken = default);
}
