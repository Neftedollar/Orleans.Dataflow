namespace Orleans.Dataflow.Testing;

/// <summary>
/// The producing side of a demand-aware source probe: how a test hands elements to a running graph, one at
/// a time, and what the graph's demand for them was.
/// </summary>
/// <typeparam name="T">The element type the probe emits, which is the graph's source element type.</typeparam>
/// <remarks>
/// <para>
/// A probe is a <em>control</em> and it is per run: <see cref="TestSource.Probe{T}"/> declares it on the
/// graph under a name, every materialization builds its own, and
/// <see cref="RunHandle.GetValueAsync{TResult}"/> resolves that name against one run. Two runs of one graph
/// therefore never share a probe, and an element emitted into one is never seen by the other.
/// </para>
/// <para>
/// <b>What makes it demand-aware.</b> An emit completes when the run has <em>taken</em> the element, not
/// when something accepted it into a buffer. The test therefore cannot outrun the run — an emit that has
/// not returned is an element the graph has not asked for yet — and the run cannot receive what was never
/// emitted. That is the two-sided property a probe exists to make testable, and it is what
/// <see cref="PullsObserved"/> measures from the other direction.
/// </para>
/// <para>
/// Every member is safe to call from any thread at any point in the run's life, with one exception stated
/// where it applies: <see cref="EmitAsync"/> hands over one element at a time and refuses to be called
/// again while an earlier emit is still outstanding.
/// </para>
/// </remarks>
public interface ISourceProbe<in T>
{
    /// <summary>Gets the number of elements the run has asked this probe for.</summary>
    /// <value>The running count of pulls, which is at least the number of elements taken.</value>
    /// <remarks>
    /// <para>
    /// The demand meter. A pull is one turn of the run's own loop asking for the next element, so this
    /// counts requests rather than deliveries: a run waiting for an element has already spent the pull it
    /// is waiting on, which is why a strict-pull chain settles at exactly one more pull than it has been
    /// given elements.
    /// </para>
    /// <para>
    /// That bound is the assertion worth writing. <c>PullsObserved &lt;= emitted + 1</c> holds for every
    /// graph and every buffer size, because a runtime that prefetched — that pulled a second element before
    /// the first had been dealt with — would exceed it. What changes with the buffers an author declared is
    /// how many elements the run will accept before it stops asking, and that is the other half of the same
    /// measurement.
    /// </para>
    /// <para>
    /// Read without synchronization and therefore a reading of a moment: it is a number to assert on once
    /// the run has come to rest, not a value to spin on.
    /// </para>
    /// </remarks>
    long PullsObserved { get; }

    /// <summary>Hands the run exactly one element and waits until it has taken it.</summary>
    /// <param name="element">The element to emit, which may be <see langword="null"/>.</param>
    /// <param name="cancellationToken">A token that stops this wait; it does not affect the run.</param>
    /// <returns>A task that completes when the run has taken the element.</returns>
    /// <exception cref="ProbeTerminatedException">
    /// The run ended before it took the element, whether it completed, failed, or was cancelled.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Another emit on this probe is still outstanding. A probe hands over one element at a time, and two
    /// overlapping emits would make "the run has taken it" a statement about neither of them.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <remarks>
    /// Cancelling the wait does not withdraw the element: it may already be in the run's hands, and a probe
    /// that pretended otherwise would make the elements a test emitted a matter of timing.
    /// </remarks>
    ValueTask EmitAsync(T element, CancellationToken cancellationToken = default);

    /// <summary>Ends the stream normally, as a source running out of elements does.</summary>
    /// <remarks>
    /// The elements already taken travel on and the run completes after the last of them. Calling this
    /// twice, or after <see cref="Fail"/>, or after the run ended, changes nothing.
    /// </remarks>
    void Complete();

    /// <summary>Ends the stream with a failure, faulting the run with it.</summary>
    /// <param name="exception">The failure the run reports, unwrapped.</param>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// The counterpart of <see cref="Complete"/>: failure wins over everything queued behind it, so an
    /// element emitted and not yet delivered is abandoned rather than delivered.
    /// </remarks>
    void Fail(Exception exception);
}
