namespace Orleans.Dataflow.Testing;

/// <summary>
/// The consuming side of a demand-aware sink probe: how a test releases elements one at a time and how it
/// awaits the way the run ended.
/// </summary>
/// <typeparam name="T">The element type the probe receives, which is the graph's terminal element type.</typeparam>
/// <remarks>
/// <para>
/// A probe is a <em>control</em> and it is per run: <see cref="TestSink.Probe{T}"/> declares it on the
/// graph under a name, every materialization builds its own, and
/// <see cref="RunHandle.GetValueAsync{TResult}"/> resolves that name against one run.
/// </para>
/// <para>
/// <b>The sink is the demand.</b> The run delivers nothing to a probe sink that has not been asked:
/// an element that reaches it waits for a <see cref="ReceiveAsync"/>, which holds the run's own thread,
/// which is backpressure. A test that receives nothing therefore watches the graph fill exactly the
/// capacity its author declared and then stop — which is how a bounded-memory claim becomes an assertion
/// rather than a hope.
/// </para>
/// <para>
/// <b>Nothing here hangs on a run that has ended.</b> Every wait is answered when the run ends, with a
/// <see cref="ProbeTerminatedException"/> naming the outcome, because a test that hangs reports nothing at
/// all. The two expectations are assertion helpers rather than rethrowers:
/// <see cref="ExpectFailedAsync"/> returns the run's exception instead of throwing it, so the test decides
/// what to assert about it.
/// </para>
/// </remarks>
public interface ISinkProbe<T>
{
    /// <summary>Releases exactly one element to be delivered and returns it.</summary>
    /// <param name="cancellationToken">A token that stops this wait; it does not affect the run.</param>
    /// <returns>A task that resolves with the element the run delivers next.</returns>
    /// <exception cref="ProbeTerminatedException">
    /// The run ended before it delivered another element, whether it completed, failed, or was cancelled.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <remarks>
    /// <para>
    /// One call releases one element. Several calls may be outstanding at once and are answered in the
    /// order they were made, which is how a test asks for a run of elements without receiving them one
    /// round trip at a time.
    /// </para>
    /// <para>
    /// Cancelling the wait withdraws the request, so no element is consumed by a receive nobody is waiting
    /// for — unless the run handed one over in the same instant, in which case the element wins and is
    /// returned, because an element already delivered cannot be un-delivered.
    /// </para>
    /// </remarks>
    ValueTask<T> ReceiveAsync(CancellationToken cancellationToken = default);

    /// <summary>Waits for the run to complete successfully.</summary>
    /// <param name="cancellationToken">A token that stops this wait; it does not affect the run.</param>
    /// <returns>A task that completes when the run has completed.</returns>
    /// <exception cref="ProbeTerminatedException">The run failed or was cancelled instead.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <remarks>
    /// Completion here means the run's own end, which arrives after the last element has been delivered —
    /// so a test that expects a stream of three elements receives three and then awaits this, and the two
    /// assertions together say that there was no fourth.
    /// </remarks>
    ValueTask ExpectCompletedAsync(CancellationToken cancellationToken = default);

    /// <summary>Waits for the run to fail and returns the exception it failed with.</summary>
    /// <param name="cancellationToken">A token that stops this wait; it does not affect the run.</param>
    /// <returns>A task that resolves with the run's failure, unwrapped and instance-identical.</returns>
    /// <exception cref="ProbeTerminatedException">The run completed or was cancelled instead.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <remarks>
    /// The failure is returned rather than thrown on purpose. A test that expected a failure wants to
    /// assert about it — its type, its message, its identity with the exception the stage threw — and a
    /// helper that rethrew would force every such test through a <c>catch</c> to get at the value it asked
    /// for.
    /// </remarks>
    ValueTask<Exception> ExpectFailedAsync(CancellationToken cancellationToken = default);
}
