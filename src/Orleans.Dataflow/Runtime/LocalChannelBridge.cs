using System.Threading.Channels;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One channel read by a run, seen the way a segment's pull loop needs it: two synchronous calls over boxed
/// elements.
/// </summary>
/// <remarks>
/// <para>
/// The wait is split from the read so that the loop can wait on the run's stop token and take everything
/// the channel already holds without waiting again. A refusal to wait any longer is either the channel
/// ending or the run stopping, and the loop tells those apart rather than this type.
/// </para>
/// <para>
/// A reader is external state the author owns, and this type deliberately does nothing about that: it is
/// neither reset per run nor completed by the run, because a run does not own what it was handed.
/// </para>
/// </remarks>
internal abstract class LocalChannelSource
{
    /// <summary>Waits until the channel has an element or has ended.</summary>
    /// <param name="cancellationToken">The run's stop token, which releases the wait.</param>
    /// <returns><see langword="true"/> when an element may be available; <see langword="false"/> at the end.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    internal abstract bool WaitToRead(CancellationToken cancellationToken);

    /// <summary>Takes one element if the channel has one ready.</summary>
    /// <param name="element">The element taken, when this method returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when an element was taken.</returns>
    internal abstract bool TryRead(out object? element);
}

/// <summary>
/// The bridge over one typed channel reader.
/// </summary>
/// <typeparam name="T">The element type the channel carries.</typeparam>
/// <param name="reader">The reader the author handed the graph.</param>
/// <remarks>
/// The wait is converted to a task before it is blocked on, for the reason
/// <see cref="LocalAsyncCursor{T}"/> converts its own: a <see cref="ValueTask{TResult}"/> from a pooled
/// source may be awaited once and is not safe to block on directly.
/// </remarks>
internal sealed class LocalChannelSource<T>(ChannelReader<T> reader) : LocalChannelSource
{
    /// <inheritdoc/>
    internal override bool WaitToRead(CancellationToken cancellationToken) =>
        reader.WaitToReadAsync(cancellationToken).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc/>
    internal override bool TryRead(out object? element)
    {
        bool read = reader.TryRead(out T? value);

        element = value;

        return read;
    }
}

/// <summary>
/// One channel written by a run, seen the way a terminal needs it: write an element, and complete the
/// writer when the run is over.
/// </summary>
/// <remarks>
/// <para>
/// The write is the sink's backpressure. A bounded writer with no room holds the segment's thread until
/// there is room, which is exactly what a slow synchronous callback does, so a channel sink needs no policy
/// of its own: the channel the author created already carries theirs.
/// </para>
/// <para>
/// A write that is accepted is not a write that was consumed. The element is in the channel, and whether
/// anything ever reads it is the author's business on the other side; this is the acceptance-versus-
/// consumption distinction every bounded egress adapter has to state.
/// </para>
/// </remarks>
internal abstract class LocalChannelSink
{
    /// <summary>Writes one element, waiting for room.</summary>
    /// <param name="element">The element to write.</param>
    /// <param name="context">The tokens and the pause gate of the run.</param>
    /// <exception cref="OperationCanceledException">The run was cancelled while this element waited.</exception>
    /// <exception cref="ChannelClosedException">
    /// The writer was completed by something other than this run, so the element cannot be delivered.
    /// </exception>
    /// <remarks>
    /// The whole context and not only a token, because the wait for room is one of this runtime's own and
    /// has to say so: a segment blocked here takes no step until a consumer on the author's side of the
    /// channel makes room, which is exactly the state <see cref="LocalPause.Idle"/> exists to count. It is
    /// the mirror of the wait a channel <em>source</em> takes on an empty reader, and it reports itself for
    /// the same reason — the two halves of one adapter cannot answer a pause differently.
    /// </remarks>
    internal abstract void Write(object? element, LocalRunContext context);

    /// <summary>Completes the writer with the run's outcome.</summary>
    /// <param name="failure">
    /// The failure the run ended with, or <see langword="null"/> when it ended successfully.
    /// </param>
    /// <remarks>
    /// Completion is attempted rather than asserted: a writer the author already completed is not this
    /// run's problem to report, and a run that threw over it during teardown would replace the outcome
    /// worth reading.
    /// </remarks>
    internal abstract void Close(Exception? failure);
}

/// <summary>
/// The bridge over one typed channel writer.
/// </summary>
/// <typeparam name="T">The element type the channel carries.</typeparam>
/// <param name="writer">The writer the author handed the graph.</param>
internal sealed class LocalChannelSink<T>(ChannelWriter<T> writer) : LocalChannelSink
{
    /// <inheritdoc/>
    /// <remarks>
    /// The synchronous case is answered without touching the pause gate at all, because a write that
    /// needed no room to appear is not a wait and taking the gate's lock per element would be a cost paid
    /// by every run that has a channel sink in it. Only the write that really blocks reports itself, which
    /// is the shape every other wait in this runtime that can complete at once already uses.
    /// </remarks>
    internal override void Write(object? element, LocalRunContext context)
    {
        ValueTask written = writer.WriteAsync((T)element!, context.RunToken);

        if (written.IsCompleted)
        {
            written.GetAwaiter().GetResult();

            return;
        }

        context.Pause.Idle();

        try
        {
            written.AsTask().GetAwaiter().GetResult();
        }
        finally
        {
            context.Pause.Busy();
        }
    }

    /// <inheritdoc/>
    internal override void Close(Exception? failure) => _ = writer.TryComplete(failure);
}
