using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// A sink that knows how far it has committed, as the capture loop and a resume see it.
/// </summary>
/// <remarks>
/// <para>
/// The sink half of ADR 0007's checkpoint, as a runtime seam rather than a promise about sinks in general.
/// A sink <em>declares</em> a mark by being planned as one of these; every other sink contributes nothing to
/// a checkpoint, which is stated per sink in the adapter table rather than generalized here.
/// </para>
/// <para>
/// <b>Two members, where <see cref="LocalSourceCursor"/> has three.</b> A cursor is advanced by the run,
/// because delivery through a segment is a fact only the run knows; a mark is advanced by whoever owns the
/// effect, because when an effect became real is a fact only that owner knows. So the engine reads a mark and
/// restores one, and never advances one — which is why the local marking sink advances its own count inside
/// its callback and a registered sink advances its own inside its adapter.
/// </para>
/// <para>
/// The interface exists because there are now two implementations of one idea: the vocabulary's own marking
/// sink, and whatever a provider hands across the public seam. The capture loop and
/// <see cref="LocalResume"/> deal only in this shape, so neither of them knows which of the two it is
/// holding.
/// </para>
/// </remarks>
internal interface ILocalCommitMark
{
    /// <summary>Gets how far this sink has committed, as a checkpoint carries it.</summary>
    /// <value>The mark, as a canonical value only this sink has to understand.</value>
    CanonicalJsonValue Mark { get; }

    /// <summary>Takes back a mark this sink reported earlier.</summary>
    /// <param name="mark">The mark, as a checkpoint carried it.</param>
    /// <exception cref="InvalidOperationException">The value is not a mark this sink understands.</exception>
    void Restore(CanonicalJsonValue mark);
}

/// <summary>
/// The commit mark of a sink a provider built, seen by the engine.
/// </summary>
/// <param name="declared">The mark the provider's stage runtime carries.</param>
/// <remarks>
/// A pass-through and deliberately nothing more, exactly as <see cref="LocalProvidedCursor"/> is: what a
/// mark counts, when it advances, and what a restored one means are the adapter's, and this type exists only
/// because the engine's seam is internal and the provider's is public. The two members line up one for one.
/// </remarks>
internal sealed class LocalProvidedMark(Hosting.DataflowSinkMark declared) : ILocalCommitMark
{
    /// <inheritdoc/>
    public CanonicalJsonValue Mark => declared.Mark;

    /// <inheritdoc/>
    public void Restore(CanonicalJsonValue mark) => declared.RestoreTo(mark);
}
