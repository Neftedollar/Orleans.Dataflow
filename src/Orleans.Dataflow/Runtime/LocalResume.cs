using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// What a checkpoint does to a plan before the resumed run's first element.
/// </summary>
/// <remarks>
/// <para>
/// Three tables handed back to three seams, and one rule about each direction of a mismatch. A node the
/// checkpoint names that this plan has no seam for is a <b>refusal</b>: the checkpoint was taken of a graph
/// whose nodes are not these nodes, which the fingerprint should already have caught, so reaching here means
/// something is wrong that a resume must not paper over. A seam this plan has that the checkpoint does not
/// name is <b>not</b> a refusal: it is a source that had delivered nothing, a scope that had not been
/// reached, or a sink that had committed nothing when the snapshot was taken, and each of those starts from
/// its beginning exactly as a fresh run would.
/// </para>
/// <para>
/// Everything this class does not touch resets, and that is the contract rather than an omission. A resumed
/// run builds every stage of the graph from the very factories a fresh run builds them from; the only state
/// that survives is what a durable scope exported, the only position that survives is what a cursor
/// reported, and the only mark that survives is what a marking sink counted.
/// </para>
/// </remarks>
internal static class LocalResume
{
    /// <summary>Restores one plan's seams from one checkpoint.</summary>
    /// <param name="plan">The freshly compiled plan of the resumed run.</param>
    /// <param name="checkpoint">The checkpoint the store handed back.</param>
    /// <exception cref="InvalidOperationException">
    /// The checkpoint names a node this plan has no such seam for, or carries a value a seam cannot read.
    /// </exception>
    /// <remarks>
    /// Called on the thread that materializes the run, before any segment has started. Restoring a seam
    /// mid-run would change a stage's state under an element that was already inside it.
    /// </remarks>
    internal static void Restore(LocalRunPlan plan, LocalCheckpoint checkpoint)
    {
        Apply(
            checkpoint.Cursors,
            plan.Cursors,
            "cursor",
            "a source that declares one",
            static (cursor, position) => cursor.RestoreTo(position));

        Apply(
            checkpoint.States,
            plan.DurableStates,
            "durable state",
            "a durable scope",
            static (scope, state) => scope.Restore(state));

        Apply(
            checkpoint.Marks,
            plan.Marks,
            "commit mark",
            "a sink that declares one",
            static (sink, mark) => sink.Restore(mark));
    }

    /// <summary>Hands one table of stored values back to the seams of the plan.</summary>
    /// <typeparam name="TSeam">The seam being restored.</typeparam>
    /// <param name="stored">What the checkpoint carried, keyed by node.</param>
    /// <param name="seams">What the plan has, keyed by node.</param>
    /// <param name="what">What kind of value this is, for the diagnostic.</param>
    /// <param name="owner">What kind of node owns one, for the diagnostic.</param>
    /// <param name="restore">What to do with each pair.</param>
    private static void Apply<TSeam>(
        IReadOnlyDictionary<NodeId, CanonicalJsonValue> stored,
        IReadOnlyDictionary<NodeId, TSeam> seams,
        string what,
        string owner,
        Action<TSeam, CanonicalJsonValue> restore)
    {
        foreach (KeyValuePair<NodeId, CanonicalJsonValue> entry in stored)
        {
            if (!seams.TryGetValue(entry.Key, out TSeam? seam))
            {
                throw new InvalidOperationException(
                    $"The checkpoint carries a {what} for the node '{entry.Key}', and this graph has no such node, or the node it has is not {owner}. A resume restores into the graph the checkpoint was taken of; a checkpoint of another graph is refused rather than partly applied.");
            }

            restore(seam, entry.Value);
        }
    }
}
