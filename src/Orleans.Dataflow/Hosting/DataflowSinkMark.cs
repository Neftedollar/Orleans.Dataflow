using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// How a registered sink says how far its side effect has actually got, so that a checkpoint can store the
/// mark and a resume can take it back.
/// </summary>
/// <remarks>
/// <para>
/// ADR 0007's commit mark, on the provider side of the runtime-factory seam and rhymed with
/// <see cref="DataflowSourceCursor"/> on purpose: a sink <em>declares</em> a mark by being built through
/// <see cref="DataflowStageRuntime.Terminal(Func{object?}, Func{object?, object?, object?}, Func{object?, object?}?, bool, DataflowSinkMark)"/>;
/// every other registered sink contributes nothing to a checkpoint and says nothing about what it committed,
/// which is stated per adapter in the adapter table rather than generalized here. The two halves of a
/// checkpoint's arithmetic are exactly these two types: a cursor says what a source handed over and a mark
/// says what a sink finished with, and the duplicate window of a resume is the difference.
/// </para>
/// <para>
/// <b>It has two members where a cursor has three, and the missing one is the finding rather than an
/// omission.</b> A cursor is advanced by the <em>run</em>, because only the run knows that an element has
/// travelled through the segment it entered. A mark is advanced by the <em>adapter</em>, because only the
/// adapter knows when its effect became real — an acknowledgement that lands after the fold returned, a
/// transaction that commits on flush, a queue that answers later — and a seam that offered the engine an
/// "advance now" member would be inviting a provider to move the mark at the moment the engine can see
/// rather than at the moment the commit happened. So the engine reads and restores, and the advancing stays
/// where the knowledge is.
/// </para>
/// <para>
/// <b>The mark advances after the effect and never before it.</b> That direction is the whole contract: a
/// mark that moved first would promise a commit that had not happened, and the duplicate window of a resume
/// would become a loss window. An adapter that cannot tell the two moments apart declares no mark at all.
/// </para>
/// <para>
/// <b>Lagging is safe and leading is not.</b> A capture holds the run at a safe point, and an adapter may
/// legitimately have effects in flight that the engine's quiescence does not cover — an awaited call started
/// by a fold that has already returned is exactly that. A mark that under-reports at such a moment produces a
/// wider replay and never a lost element, which is the direction this seam is built to lean; nothing here
/// tries to make the number tight.
/// </para>
/// <para>
/// <b>The mark is a canonical value and that is the seam's requirement rather than a preference.</b> A
/// checkpoint is read by a process that is not the one that wrote it, so a mark enters it as canonical JSON
/// with no CLR type name in the document's own grammar. An adapter whose mark cannot be said in that plane
/// declares none.
/// </para>
/// <para>
/// <b>Threading.</b> <see cref="Mark"/> is read from the capture loop's thread while the run is held
/// quiescent, and is written by whatever thread the adapter's own effect completes on;
/// <see cref="RestoreTo"/> is called on the thread that materializes the run, before any segment has
/// started. An implementation whose mark is more than one word writes it so that the reading is a fact
/// rather than a race the quiescence happens to have closed.
/// </para>
/// </remarks>
public abstract class DataflowSinkMark
{
    /// <summary>Initializes a new instance of the <see cref="DataflowSinkMark"/> class.</summary>
    protected DataflowSinkMark()
    {
    }

    /// <summary>Gets how far this sink has committed.</summary>
    /// <value>
    /// The mark, as a canonical value only this adapter has to understand. It means "the effect for these
    /// elements finished", never "the run reached them" — where the run reached is the cursor's question,
    /// which is exactly why a checkpoint carries both.
    /// </value>
    public abstract CanonicalJsonValue Mark { get; }

    /// <summary>Takes back a mark this sink reported earlier.</summary>
    /// <param name="mark">The mark, as a checkpoint carried it.</param>
    /// <exception cref="InvalidOperationException">
    /// The value is not a mark this sink understands, which means the checkpoint was written by a different
    /// graph or by hand.
    /// </exception>
    /// <remarks>
    /// Called before the run starts, on the thread that materializes it, and only for a resume. It is what
    /// makes the number the <em>run's</em> rather than the attempt's: a sink that had committed eight
    /// elements over two attempts says eight, and a mark that reset would make a second crash's checkpoint
    /// describe less work than the first's.
    /// </remarks>
    public abstract void RestoreTo(CanonicalJsonValue mark);
}
