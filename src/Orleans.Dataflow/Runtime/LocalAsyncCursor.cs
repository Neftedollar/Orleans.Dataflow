namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One enumeration of an asynchronous sequence, seen the way a segment's pull loop needs it: three
/// synchronous calls over boxed elements.
/// </summary>
/// <remarks>
/// <para>
/// A segment is one loop on one dedicated thread, and that is what makes an asynchronous source ordinary
/// rather than special: the thread waits for the next element exactly as it waits for a slow synchronous
/// one, and the run observes cancellation between elements rather than inside one. Blocking a thread-pool
/// thread this way would be a defect; blocking the segment's own thread is what the thread is for.
/// </para>
/// <para>
/// The wait is a real wait on the returned task rather than an abandonment. A
/// <c>MoveNextAsync</c> outstanding when the run is cancelled is awaited to its outcome, so an element the
/// author's sequence had already produced is not lost silently and a failure it was about to report is
/// still the run's to see. Cancellation is cooperative: a sequence that ignores the token it was opened
/// with delays the run's stop until it next yields, which is the same slow-source rule a blocking
/// synchronous sequence follows.
/// </para>
/// </remarks>
internal abstract class LocalAsyncCursor
{
    /// <summary>Gets the element the last successful <see cref="MoveNext"/> produced.</summary>
    internal abstract object? Current { get; }

    /// <summary>Waits for the next element.</summary>
    /// <returns><see langword="true"/> when <see cref="Current"/> holds one; <see langword="false"/> at the end.</returns>
    internal abstract bool MoveNext();

    /// <summary>Releases the enumeration, waiting for its asynchronous disposal to finish.</summary>
    /// <remarks>
    /// Called on every terminal path of the run, including the ones where reading the sequence is what went
    /// wrong. Awaiting the disposal rather than starting it is the whole point: an asynchronous sequence
    /// that closes a file, a connection, or a subscription has not closed it until its
    /// <c>DisposeAsync</c> has finished, and a run that ended before then would have leaked it.
    /// </remarks>
    internal abstract void Dispose();
}

/// <summary>
/// The enumeration of one typed asynchronous sequence.
/// </summary>
/// <typeparam name="T">The element type the sequence produces.</typeparam>
/// <param name="elements">The enumerator, already opened with the run's token.</param>
/// <remarks>
/// The tasks are awaited through <see cref="ValueTask{TResult}.AsTask"/> rather than blocked on directly.
/// A <see cref="ValueTask{TResult}"/> backed by a pooled source may only be awaited once and is not safe to
/// block on before it completes; converting it to a task first is the supported way to wait, and it is what
/// keeps a compiler-generated <c>async</c> iterator working here at all.
/// </remarks>
internal sealed class LocalAsyncCursor<T>(IAsyncEnumerator<T> elements) : LocalAsyncCursor
{
    /// <inheritdoc/>
    internal override object? Current => elements.Current;

    /// <inheritdoc/>
    internal override bool MoveNext() => elements.MoveNextAsync().AsTask().GetAwaiter().GetResult();

    /// <inheritdoc/>
    internal override void Dispose() => elements.DisposeAsync().AsTask().GetAwaiter().GetResult();
}

/// <summary>
/// Opens one enumeration of an asynchronous sequence under a run's token.
/// </summary>
/// <param name="cancellationToken">The run's own token, which the sequence is opened with.</param>
/// <returns>The enumeration.</returns>
/// <remarks>
/// The element type is pinned when the source is authored rather than recovered by reflection from the
/// bound value, because <see cref="IAsyncEnumerable{T}"/> is an interface and one class may implement it
/// for two element types; the type argument the author wrote is the only statement of which of them the
/// graph means. This is the same reason a deduplicating stage carries
/// <see cref="EqualityComparer{T}.Default"/> rather than an element type the runtime would have to guess.
/// </remarks>
internal delegate LocalAsyncCursor LocalAsyncCursorFactory(CancellationToken cancellationToken);
