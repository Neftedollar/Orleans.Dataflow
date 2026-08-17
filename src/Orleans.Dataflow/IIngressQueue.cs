using System.Diagnostics.CodeAnalysis;

namespace Orleans.Dataflow;

/// <summary>
/// The producer side of one run's bounded ingress queue: how elements are pushed into a graph that is
/// already running.
/// </summary>
/// <typeparam name="T">The element type the queue accepts, which is the graph's source element type.</typeparam>
/// <remarks>
/// <para>
/// A queue is a <em>control</em> rather than a result, and it is per run: <see cref="Source.Queue{T}"/>
/// declares it on the graph under a name, and every materialization builds its own queue and resolves that
/// name to its own handle. Two runs of one graph therefore never share a queue, which is the same rule that
/// makes a source enumerator and an aggregate seed per-run.
/// </para>
/// <para>
/// The control resolves at the start of a run rather than at its end, because the run has to be running for
/// anything to be offered to it. <see cref="RunHandle.GetValueAsync{TResult}"/> is what hands it over, and
/// the task it returns for a control is already complete by the time the handle exists.
/// </para>
/// <para>
/// Every member is safe to call from any thread at any point in the run's life, including before the first
/// element, after the queue has been completed, and after the run has ended. Nothing here throws for the
/// state of the queue; a queue that cannot take an element says so in the
/// <see cref="QueueOfferOutcome"/> it returns.
/// </para>
/// </remarks>
[SuppressMessage(
    "Naming",
    "CA1711:Identifiers should not have incorrect suffix",
    Justification = "The suffix is reserved so that only a queue is called one, and this is a queue: elements are offered at one end, held under a declared bound, and taken in order at the other. Renaming it to satisfy the letter of a rule it satisfies in substance would cost the reader the one word that says what it is.")]
public interface IIngressQueue<T>
{
    /// <summary>Offers one element to the queue.</summary>
    /// <param name="element">The element to enqueue, which may be <see langword="null"/>.</param>
    /// <param name="cancellationToken">A token that stops this offer; it does not affect the run.</param>
    /// <returns>What became of the element.</returns>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> was already cancelled, or was cancelled while this offer was
    /// waiting for room. Nothing about the queue itself is ever reported this way.
    /// </exception>
    /// <remarks>
    /// <para>
    /// What a full queue does is the author's declared <see cref="OverflowPolicy"/>, applied at the moment
    /// of the offer and only then. <see cref="OverflowPolicy.Backpressure"/> waits for room, which is what
    /// makes this method asynchronous at all; the drop policies answer at once; and
    /// <see cref="OverflowPolicy.Fail"/> fails the queue and the run.
    /// </para>
    /// <para>
    /// The wait of a backpressuring offer ends when there is room, when the queue is completed, or when the
    /// run ends — never in silence, and never with an exception for any of the three.
    /// </para>
    /// </remarks>
    ValueTask<QueueOfferOutcome> OfferAsync(T element, CancellationToken cancellationToken = default);

    /// <summary>Ends the queue normally, as a source running out of elements does.</summary>
    /// <remarks>
    /// The elements already accepted are delivered first: completing a queue is a drain, not a stop, so a
    /// run whose only source is this queue completes successfully once the last accepted element has
    /// reached the sink. Every later offer answers <see cref="QueueOfferOutcome.Closed"/>. Calling this
    /// twice, or after <see cref="Fail"/>, or after the run ended, changes nothing.
    /// </remarks>
    void Complete();

    /// <summary>Ends the queue with a failure, faulting the run with it.</summary>
    /// <param name="exception">The failure the run reports, unwrapped.</param>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// The counterpart of <see cref="Complete"/> and the opposite of it in exactly one way: the elements
    /// still queued are abandoned rather than delivered, because failure wins over everything queued behind
    /// it. Every later offer answers <see cref="QueueOfferOutcome.Failed"/>.
    /// </para>
    /// <para>
    /// This member exists because a producer's own work can fail, and the only other thing it could do
    /// about that is <see cref="Complete"/> — which would report a successful run over a stream that was
    /// never finished. A silent lie is a worse contract than a fourth outcome.
    /// </para>
    /// </remarks>
    void Fail(Exception exception);
}
