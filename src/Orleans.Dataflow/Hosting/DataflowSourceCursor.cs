using Orleans.Dataflow.Serialization;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// How a registered source says where it is, so that a checkpoint can store the position and a resume can
/// reopen at it.
/// </summary>
/// <remarks>
/// <para>
/// The checkpoint model's cursor, on the provider side of the runtime-factory seam. A source
/// <em>declares</em> a cursor by being built through
/// <see cref="DataflowStageRuntime.Source(Func{DataflowRunTokens, IAsyncEnumerable{object?}}, DataflowSourceCursor)"/>;
/// every other registered source contributes nothing to a checkpoint and <b>resumes from now</b>, which is
/// stated per adapter in the adapter table rather than generalized here. The local vocabulary's own index
/// cursor is the same three moving parts seen from the other side of the seam, which is why this type has
/// exactly those three and no more.
/// </para>
/// <para>
/// <b>The cursor is the provider's own object and the opener closes over it.</b> Nothing here opens
/// anything: an adapter that has been restored to a position reads that position from its own cursor
/// instance when the run asks for its sequence, because only the adapter knows whether a position is an
/// index to skip, a token to subscribe at, or an offset to seek to. One cursor is built per node per
/// materialization, exactly as the rest of a stage runtime is, so two runs of one pipeline never share one.
/// </para>
/// <para>
/// <b>The position is a canonical value and that is the seam's requirement rather than a preference.</b> A
/// checkpoint is read by a process that is not the one that wrote it, so a position enters it as canonical
/// JSON with no CLR type name in the document's own grammar. An adapter whose position cannot be said in
/// that plane declares no cursor at all.
/// </para>
/// <para>
/// <b><see cref="Delivered"/> is called by the run and never by the sequence</b>, and the difference is the
/// whole reason a stored position is exact. A sequence learns that its element was wanted only when the next
/// one is asked for, and the moment between those two — element delivered, next not yet asked for — is
/// exactly where a capture's hold lands; a cursor that counted what it had yielded would be one ahead at
/// every capture. The run therefore reports the delivery once the element has travelled all the way through
/// the segment it entered, and exactly one element is ever outstanding between a yield and that report.
/// </para>
/// <para>
/// <b>Threading.</b> <see cref="Delivered"/> is called from the source segment's own thread and
/// <see cref="Position"/> is read from the capture loop's while the run is held quiescent;
/// <see cref="RestoreTo"/> is called on the thread that materializes the run, before any segment has
/// started. An implementation whose position is more than one word writes it so that the reading is a fact
/// rather than a race the quiescence happens to have closed.
/// </para>
/// </remarks>
public abstract class DataflowSourceCursor
{
    /// <summary>Initializes a new instance of the <see cref="DataflowSourceCursor"/> class.</summary>
    protected DataflowSourceCursor()
    {
    }

    /// <summary>Gets where this source has reached.</summary>
    /// <value>
    /// The position, as a canonical value only this adapter's own opener has to understand. It means
    /// "handed over and delivered through its segment", never "committed at a sink" — whether an element
    /// reached a sink is the commit mark's question, which is exactly why a checkpoint carries both.
    /// </value>
    public abstract CanonicalJsonValue Position { get; }

    /// <summary>Records that the element the sequence last yielded has been delivered through its segment.</summary>
    /// <remarks>
    /// Called once per element the run took, after that element travelled through the segment it entered.
    /// A source that yielded nothing is never told anything, and a source whose run ended mid-element is
    /// told nothing about that element — which is what makes the stored position a place the resumed run
    /// may safely start after.
    /// </remarks>
    public abstract void Delivered();

    /// <summary>Takes back a position this cursor reported earlier.</summary>
    /// <param name="position">The position, as a checkpoint carried it.</param>
    /// <exception cref="InvalidOperationException">
    /// The value is not a position this cursor understands, which means the checkpoint was written by a
    /// different graph or by hand.
    /// </exception>
    /// <remarks>
    /// Called before the run starts, on the thread that materializes it, and only for a resume. A cursor
    /// restored mid-run would be a source told to be somewhere else while it was reading.
    /// </remarks>
    public abstract void RestoreTo(CanonicalJsonValue position);
}
