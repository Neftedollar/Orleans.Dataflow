using System.Globalization;
using System.Text.Json;
using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// A sink that can say how far it has committed: the side effect, and the number that only moves after it.
/// </summary>
/// <remarks>
/// <para>
/// ADR 0007's sink half — "elements through position P are committed" — in the smallest honest shape: the
/// callback is the commit, and the mark is what has been committed. <see cref="Commit"/> calls the author's
/// callback first and increments afterwards, so a callback that throws leaves the mark where it was. That
/// order is the whole of what makes it a mark rather than a counter: a mark that moved first would promise a
/// commit that had not happened, and the duplicate window of a resume would then be a lie in the dangerous
/// direction.
/// </para>
/// <para>
/// The duplicate window of a resume is exactly the elements between the cursor a checkpoint stored and the
/// mark at the moment of the crash: those were committed and are replayed, because the cursor the resume
/// opens at does not know about them. That is what "at-least-once between commit marks" means, said in the
/// two numbers a test can subtract.
/// </para>
/// <para>
/// <b>The mark counts committed deliveries and is not a source position.</b> The two agree exactly for a
/// graph that neither drops nor multiplies elements between a source and this sink, which is the shape the
/// resume proof uses; they part company for any graph that filters, batches, or fans out, and they part
/// company again across a resume, because a replayed element is a second delivery of one element. Saying
/// "committed deliveries" is what keeps a reader from doing arithmetic the number cannot support.
/// </para>
/// <para>
/// <b>The mark is restored across a resume</b>, so a run that has committed eleven elements over two
/// attempts says eleven rather than starting over at the three it committed since the last crash. A mark
/// that reset would make a second crash's checkpoint describe less work than the first's.
/// </para>
/// </remarks>
internal sealed class LocalMarkingSink(Action<object?> callback) : ILocalCommitMark
{
    /// <summary>The member of this sink's mark holding how many elements it has committed.</summary>
    internal const string CommittedMember = "committed";

    private long _committed;

    /// <summary>Gets how many elements this sink's side effect has completed for.</summary>
    /// <value>The running count across the run and every resume of it.</value>
    /// <remarks>
    /// Read by a test through the sink's control and by the capture loop through <see cref="Mark"/>. It is
    /// the same number in both places, which is the point: what a test asserts is what a checkpoint stores.
    /// </remarks>
    internal long Committed => Interlocked.Read(ref _committed);

    /// <summary>Gets how far this sink has committed, as a checkpoint carries it.</summary>
    /// <value>The mark, as a canonical value only this sink has to understand.</value>
    /// <remarks>
    /// Reached through <see cref="ILocalCommitMark"/> by the capture loop, which since M5.5 holds a seam
    /// rather than this class: a registered sink declares a mark of its own and lands in the very same table.
    /// </remarks>
    public CanonicalJsonValue Mark =>
        CanonicalJsonValue.Parse(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"{CommittedMember}\":{Interlocked.Read(ref _committed)}}}"));

    /// <summary>Runs the author's side effect for one element and then advances the mark.</summary>
    /// <param name="element">The element that reached this sink.</param>
    /// <remarks>
    /// In this order and never the other one. A mark that moved first would say an element was committed
    /// while the commit was still running, and a crash in the middle of it would then be a lost element
    /// reported as a delivered one — which is exactly the direction a duplicate window must never lean.
    /// </remarks>
    internal void Commit(object? element)
    {
        callback(element);

        _ = Interlocked.Increment(ref _committed);
    }

    /// <summary>Takes back a mark this sink reported earlier.</summary>
    /// <param name="mark">The mark, as a checkpoint carried it.</param>
    /// <exception cref="InvalidOperationException">The value is not a mark this sink understands.</exception>
    public void Restore(CanonicalJsonValue mark)
    {
        JsonElement declared = mark.IsDefault ? throw Unreadable(mark) : mark.ToElement();

        if (declared.ValueKind is not JsonValueKind.Object ||
            !declared.TryGetProperty(CommittedMember, out JsonElement committed) ||
            committed.ValueKind is not JsonValueKind.Number ||
            !committed.TryGetInt64(out long value) ||
            value < 0)
        {
            throw Unreadable(mark);
        }

        _committed = value;
    }

    /// <summary>Builds the failure a mark this sink cannot read produces.</summary>
    /// <param name="mark">The mark as the checkpoint carried it.</param>
    /// <returns>The exception.</returns>
    private static InvalidOperationException Unreadable(CanonicalJsonValue mark) =>
        new($"The checkpoint carries the mark {mark} for a marking sink, and such a sink's mark is an object with a '{CommittedMember}' member holding a count of zero or more committed elements. The checkpoint was written by a different graph or by hand.");
}
