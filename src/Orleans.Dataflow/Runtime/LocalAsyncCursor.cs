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

    /// <summary>Starts the step to the next element without waiting for it.</summary>
    /// <returns>
    /// The task that answers <see langword="true"/> when <see cref="Current"/> holds an element and
    /// <see langword="false"/> at the end of the sequence.
    /// </returns>
    /// <remarks>
    /// The half of a pull a pump can put in a wait-set. A segment reading one sequence has no use for it and
    /// blocks in <see cref="MoveNext"/>; a merge-map's pump holds one outstanding step per live inner
    /// sequence and sleeps on all of them at once, which is a wait no other pump of this runtime takes. The
    /// task is the caller's to observe exactly once: the step is started here and not again until this one
    /// has answered, which is what keeps the single-consumption rule of a
    /// <see cref="ValueTask{TResult}"/> intact.
    /// </remarks>
    internal abstract Task<bool> Advance();

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
    internal override Task<bool> Advance() => elements.MoveNextAsync().AsTask();

    /// <inheritdoc/>
    internal override bool MoveNext() => Advance().GetAwaiter().GetResult();

    /// <inheritdoc/>
    internal override void Dispose() => elements.DisposeAsync().AsTask().GetAwaiter().GetResult();
}

/// <summary>
/// The enumeration of one ordinary synchronous sequence, seen through the asynchronous cursor.
/// </summary>
/// <typeparam name="T">The element type the sequence produces.</typeparam>
/// <param name="elements">The enumerator, already obtained.</param>
/// <remarks>
/// <para>
/// What makes a merge-map over <c>Func&lt;T, IEnumerable&lt;TNext&gt;&gt;</c> the same operator rather than a
/// second one: the pump admits enumerations, waits on their steps, and releases them, and none of that cares
/// whether a step ever actually waited. A synchronous step answers a completed task, so the pump's wait
/// returns at once and the element is emitted on the very pass that asked for it.
/// </para>
/// <para>
/// The price is stated rather than hidden: the sequence is advanced on the pump's own thread, so a
/// synchronous inner sequence that blocks holds up every other inner sequence beside it. That is the same
/// slow-source rule this runtime states everywhere else — what an author's code does on the thread it was
/// given is the author's — and it is the reason the asynchronous spelling exists.
/// </para>
/// </remarks>
internal sealed class LocalSequenceCursor<T>(IEnumerator<T> elements) : LocalAsyncCursor
{
    /// <summary>The answer of a step that found an element, allocated once rather than per element.</summary>
    private static readonly Task<bool> Produced = Task.FromResult(true);

    /// <summary>The answer of a step that found the end of the sequence.</summary>
    private static readonly Task<bool> Ended = Task.FromResult(false);

    /// <inheritdoc/>
    internal override object? Current => elements.Current;

    /// <inheritdoc/>
    internal override Task<bool> Advance() => elements.MoveNext() ? Produced : Ended;

    /// <inheritdoc/>
    internal override bool MoveNext() => elements.MoveNext();

    /// <inheritdoc/>
    internal override void Dispose() => elements.Dispose();
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

/// <summary>
/// Opens one enumeration of the sequence a merge-map's function answers for one element.
/// </summary>
/// <param name="element">The element the function is asked about.</param>
/// <param name="cancellationToken">The run's own token, which an asynchronous inner sequence is opened with.</param>
/// <returns>The enumeration.</returns>
/// <remarks>
/// One delegate for both spellings of the operator, because both answer the same thing to the pump: an
/// enumeration it owns, advances one step at a time, and releases. Which of the two an occurrence carries is
/// decided once, when the binding is read, and never again per element.
/// </remarks>
internal delegate LocalAsyncCursor LocalInnerCursorFactory(object? element, CancellationToken cancellationToken);
