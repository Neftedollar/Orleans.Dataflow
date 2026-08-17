using System.Collections;
using System.Threading.Channels;

namespace Orleans.Dataflow.Runtime;

/// <summary>
/// One run's bounded ingress queue: the boundary between producers that push and a graph that pulls.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a queue at all.</b> Every other source in this vocabulary is pulled: the run asks for the next
/// element and the source produces it. A producer that pushes cannot be asked, so the only honest way to
/// join it to a pulling runtime is a bounded queue with a declared policy for what happens when it is full.
/// That is the whole of this type, and it is why the policy is the very
/// <see cref="OverflowPolicy"/> a buffer declares: a full ingress queue and a full buffer are the same
/// situation seen from the two sides of a graph.
/// </para>
/// <para>
/// <b>Outcomes, never exceptions.</b> An offer answers with a <see cref="QueueOfferOutcome"/> for every
/// state the queue can be in — room, full, completed, failed, and a run that has ended. None of the five is
/// exceptional, and a producer that had to tell them apart from <c>catch</c> blocks would be writing its
/// control flow in the wrong construct.
/// </para>
/// <para>
/// <b>Completing drains and failing abandons.</b> That is the same distinction shutdown and cancellation
/// make one level up: <see cref="Complete"/> lets the elements already accepted through and the run
/// succeeds after them; <see cref="Fail"/> discards them, because failure wins over everything queued
/// behind it.
/// </para>
/// <para>
/// <b>Threading.</b> Any number of producers may offer at once, from any thread, at any point in the run's
/// life. The drop policies read and write the channel as one step under a lock, because deciding that the
/// queue is full and making room for the element are one decision; the backpressuring policy needs no lock,
/// because waiting for room is what the channel already does.
/// </para>
/// </remarks>
internal sealed class LocalIngressQueue
{
    /// <summary>The queue is still accepting elements.</summary>
    private const int Open = 0;

    /// <summary>The queue has been completed and its contents are being delivered.</summary>
    private const int Completed = 1;

    /// <summary>The queue has been failed and its contents were abandoned.</summary>
    private const int Faulted = 2;

    private readonly Channel<object?> _channel;
    private readonly OverflowPolicy _policy;
    private readonly int _capacity;
    private readonly Lock _gate = new();

    private long _dropped;
    private int _state;
    private volatile bool _ended;

    /// <summary>The failure a producer ended this queue with, read by the segment that drains it.</summary>
    /// <remarks>
    /// Volatile because it is written by a producer's thread and read by the run's, and the two meet only
    /// through the channel: the reader learns that the queue ended when the channel completes, which
    /// happens after this is assigned, and the ordering has to be stated rather than inferred from the
    /// channel's own synchronization.
    /// </remarks>
    private volatile Exception? _failure;

    /// <summary>Initializes a new instance of the <see cref="LocalIngressQueue"/> class.</summary>
    /// <param name="capacity">The greatest number of elements the queue holds; at least one.</param>
    /// <param name="policy">What the queue does when an element is offered to it and it is full.</param>
    /// <remarks>
    /// The channel always waits when it is full, whatever the declared policy is. The two policies a
    /// bounded channel could apply itself accept the write and drop something silently, and an offer that
    /// answered <see cref="QueueOfferOutcome.Accepted"/> for an element the channel had just discarded
    /// would be the one lie this type cannot afford.
    /// </remarks>
    internal LocalIngressQueue(int capacity, OverflowPolicy policy)
    {
        _capacity = capacity;
        _policy = policy;
        _channel = Channel.CreateBounded<object?>(
            new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = false,
            });
    }

    /// <summary>Gets the number of elements this queue's overflow policy has discarded.</summary>
    /// <value>The running count, which stays zero for a queue that never overflows or never drops.</value>
    internal long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>Offers one element to the queue.</summary>
    /// <param name="element">The element to enqueue.</param>
    /// <param name="cancellationToken">The caller's own token, which stops this offer and nothing else.</param>
    /// <returns>What became of the element.</returns>
    /// <remarks>
    /// <para>
    /// The caller's own token is examined before anything else, exactly as
    /// <see cref="System.Threading.Channels.ChannelWriter{T}.WriteAsync"/> examines it: an offer made with
    /// a cancelled token is cancelled whether or not the queue happened to have room, so a producer's loop
    /// stops at a predictable point instead of one that depends on how full the queue was.
    /// </para>
    /// <para>
    /// After that the synchronous decision answers every case but one: only a backpressuring offer that
    /// found no room has anything to wait for, and only that offer allocates.
    /// </para>
    /// </remarks>
    internal ValueTask<QueueOfferOutcome> OfferAsync(object? element, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<QueueOfferOutcome>(cancellationToken);
        }

        return TryOffer(element, out QueueOfferOutcome outcome)
            ? new ValueTask<QueueOfferOutcome>(outcome)
            : new ValueTask<QueueOfferOutcome>(WaitForRoomAsync(element, cancellationToken));
    }

    /// <summary>Ends the queue normally, delivering what it already holds.</summary>
    /// <remarks>
    /// The first of <see cref="Complete"/> and <see cref="Fail"/> decides how the queue ended, for the
    /// reason the first failure decides how a run ended: a terminal state that could be overwritten would
    /// make the outcome depend on a race.
    /// </remarks>
    internal void Complete()
    {
        if (Interlocked.CompareExchange(ref _state, Completed, Open) != Open)
        {
            return;
        }

        // The channel keeps its contents; completing it is what tells the reading segment that no more are
        // coming, and a producer parked for room is released with a refusal it reports as an outcome.
        _ = _channel.Writer.TryComplete();
    }

    /// <summary>Ends the queue with a failure, abandoning what it holds.</summary>
    /// <param name="exception">The failure the run reports.</param>
    internal void Fail(Exception exception)
    {
        if (Interlocked.CompareExchange(ref _state, Faulted, Open) != Open)
        {
            return;
        }

        _failure = exception;
        _ = _channel.Writer.TryComplete();

        // Discarded rather than delivered, and deliberately not counted as drops: nothing discarded them by
        // policy, the stream they were travelling to has failed. The channel is completed first, so a
        // producer parked for room cannot slip an element in behind this.
        while (_channel.Reader.TryRead(out object? _))
        {
        }
    }

    /// <summary>Records that the run will never read from this queue again.</summary>
    /// <remarks>
    /// Called when the segment reading the queue stops and again when the run settles, because the two are
    /// not the same moment: a run whose stream ended downstream stops reading long before its last
    /// asynchronous callback finishes, and a run cancelled before its first pull never reads at all. Both
    /// calls are idempotent, and either of them is enough to make every later offer answer
    /// <see cref="QueueOfferOutcome.Closed"/>.
    /// </remarks>
    internal void EndRun()
    {
        _ended = true;
        _ = _channel.Writer.TryComplete();
    }

    /// <summary>The sequence the run pulls the offered elements from.</summary>
    /// <param name="context">The tokens of the run.</param>
    /// <returns>The sequence, which ends when the queue is completed and drained.</returns>
    /// <remarks>
    /// The failure is checked before every element and again at the end, so a queue failed while it still
    /// held elements faults the run instead of delivering them, and a queue failed while the reader was
    /// parked faults it instead of ending quietly. The run's token reaches the wait through the stop token,
    /// so a shutdown ends this sequence exactly as running out of elements would.
    /// </remarks>
    internal IEnumerable Elements(LocalRunContext context)
    {
        try
        {
            while (true)
            {
                if (_failure is { } pending)
                {
                    throw pending;
                }

                if (_channel.Reader.TryRead(out object? element))
                {
                    yield return element;

                    continue;
                }

                if (!WaitToRead(context))
                {
                    break;
                }
            }

            if (_failure is { } failure)
            {
                throw failure;
            }
        }
        finally
        {
            EndRun();
        }
    }

    /// <summary>Decides an offer without waiting.</summary>
    /// <param name="element">The element to enqueue.</param>
    /// <param name="outcome">The outcome, when this method returns <see langword="true"/>.</param>
    /// <returns>
    /// <see langword="false"/> only for a backpressuring offer that found no room and has to wait.
    /// </returns>
    /// <remarks>
    /// The drop policies hold the lock across the whole decision, because "the queue is full" and "here is
    /// the room I just made" have to be one step: two producers that each read the queue as full and then
    /// each made room would evict twice for one element.
    /// </remarks>
    private bool TryOffer(object? element, out QueueOfferOutcome outcome)
    {
        if (Closing(out outcome))
        {
            return true;
        }

        if (_policy is OverflowPolicy.Backpressure)
        {
            if (_channel.Writer.TryWrite(element))
            {
                outcome = QueueOfferOutcome.Accepted;

                return true;
            }

            // Either the queue is full, in which case the caller waits, or it closed between the check
            // above and this write, in which case the wait ends at once with the refusal.
            return Closing(out outcome);
        }

        lock (_gate)
        {
            if (Closing(out outcome))
            {
                return true;
            }

            if (_channel.Writer.TryWrite(element))
            {
                outcome = QueueOfferOutcome.Accepted;

                return true;
            }

            outcome = Overflow(element);

            return true;
        }
    }

    /// <summary>Applies the declared policy to an element offered to a full queue.</summary>
    /// <param name="element">The element that found no room.</param>
    /// <returns>What became of it.</returns>
    /// <remarks>
    /// The outcome is about this element and about nothing else. Evicting what was already queued makes
    /// room for the offered element, so those policies accept it and count what they discarded; only the
    /// policy that discards the arriving element reports it dropped.
    /// </remarks>
    private QueueOfferOutcome Overflow(object? element)
    {
        switch (_policy)
        {
            case OverflowPolicy.DropNewest:
                Interlocked.Increment(ref _dropped);

                return QueueOfferOutcome.Dropped;
            case OverflowPolicy.DropOldest:
                if (_channel.Reader.TryRead(out object? _))
                {
                    Interlocked.Increment(ref _dropped);
                }

                return Replace(element);
            case OverflowPolicy.DropBuffer:
                while (_channel.Reader.TryRead(out object? _))
                {
                    Interlocked.Increment(ref _dropped);
                }

                return Replace(element);
            case OverflowPolicy.Fail:
            default:
                // The two labels share a section because only 'fail' can reach either: the backpressuring
                // policy never gets here, the three dropping policies are named above, and a policy no
                // member declares was refused when the source was authored and again when its payload was
                // read. Failing is also the safest answer for a value that somehow arrived anyway.
                Fail(BufferOverflowException.Full(_capacity));

                return QueueOfferOutcome.Failed;
        }
    }

    /// <summary>Writes an element into the room a drop policy just made.</summary>
    /// <param name="element">The element to enqueue.</param>
    /// <returns><see cref="QueueOfferOutcome.Accepted"/>, or the refusal of a queue that closed meanwhile.</returns>
    /// <remarks>
    /// The write can still fail, and only one way: the queue was completed, failed, or ended between the
    /// eviction and this write. The reader taking an element at the same moment is delivery rather than
    /// loss and cannot cost this element its room, because the room was already made.
    /// </remarks>
    private QueueOfferOutcome Replace(object? element) =>
        _channel.Writer.TryWrite(element)
            ? QueueOfferOutcome.Accepted
            : Refusal();

    /// <summary>Reports the refusal of a queue that is no longer accepting, if it is not accepting.</summary>
    /// <param name="outcome">The refusal, when this method returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the queue refuses every element from now on.</returns>
    private bool Closing(out QueueOfferOutcome outcome)
    {
        if (Volatile.Read(ref _state) == Open && !_ended)
        {
            outcome = QueueOfferOutcome.Accepted;

            return false;
        }

        outcome = Refusal();

        return true;
    }

    /// <summary>Reports why the queue refuses elements.</summary>
    /// <returns>
    /// <see cref="QueueOfferOutcome.Failed"/> for a queue that was failed, and
    /// <see cref="QueueOfferOutcome.Closed"/> for one that was completed or whose run has ended.
    /// </returns>
    private QueueOfferOutcome Refusal() =>
        Volatile.Read(ref _state) == Faulted ? QueueOfferOutcome.Failed : QueueOfferOutcome.Closed;

    /// <summary>Waits for room and reports what became of the element.</summary>
    /// <param name="element">The element to enqueue.</param>
    /// <param name="cancellationToken">The caller's own token.</param>
    /// <returns>What became of the element.</returns>
    /// <remarks>
    /// <para>
    /// The wait ends three ways and only one of them is an exception. Room appears and the element is
    /// accepted; the queue stops accepting, which the channel reports as a refusal and this method reports
    /// as an outcome; or the caller's own token is cancelled, which is the caller's business and is raised.
    /// </para>
    /// <para>
    /// Nothing links the run's own stopping to this wait, and nothing needs to: every way the queue stops
    /// accepting — completed, failed, or its run ended — completes the channel, and completing a channel is
    /// what releases the writers parked in it. One mechanism, and no token source to own.
    /// </para>
    /// </remarks>
    private async Task<QueueOfferOutcome> WaitForRoomAsync(object? element, CancellationToken cancellationToken)
    {
        try
        {
            await _channel.Writer.WriteAsync(element, cancellationToken).ConfigureAwait(false);

            return QueueOfferOutcome.Accepted;
        }
        catch (ChannelClosedException)
        {
            return Refusal();
        }
    }

    /// <summary>Waits until the queue has an element or has ended.</summary>
    /// <param name="context">The tokens of the run.</param>
    /// <returns><see langword="true"/> when an element may be available.</returns>
    /// <remarks>
    /// A shutdown ends the wait as the queue running out would, because that is what shutdown means: stop
    /// producing and keep what you have. A cancellation is raised and abandons the run.
    /// </remarks>
    private bool WaitToRead(LocalRunContext context)
    {
        try
        {
            return _channel.Reader.WaitToReadAsync(context.StopToken).AsTask().GetAwaiter().GetResult();
        }
        catch (OperationCanceledException) when (context.ShuttingDown)
        {
            return false;
        }
    }
}

/// <summary>
/// The typed producer facade over one run's ingress queue.
/// </summary>
/// <typeparam name="T">The element type the queue accepts.</typeparam>
/// <param name="queue">The run's queue, which works in boxed elements as the rest of the plan does.</param>
/// <remarks>
/// The facade exists because the queue is created by the runtime, which has no type argument, and handed to
/// an author, who has one. It is built by a factory the authoring surface closed over
/// <typeparamref name="T"/>, exactly as an asynchronous source's opener is, so nothing here has to recover
/// an element type the document never recorded.
/// </remarks>
internal sealed class IngressQueue<T>(LocalIngressQueue queue) : IIngressQueue<T>
{
    /// <inheritdoc/>
    public ValueTask<QueueOfferOutcome> OfferAsync(T element, CancellationToken cancellationToken = default) =>
        queue.OfferAsync(element, cancellationToken);

    /// <inheritdoc/>
    public void Complete() => queue.Complete();

    /// <inheritdoc/>
    public void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        queue.Fail(exception);
    }

    /// <summary>Returns a one-line diagnostic summary of this queue.</summary>
    /// <returns>The literal <c>ingress queue</c>.</returns>
    /// <remarks>A queue's interesting state is its run's, and the method never throws.</remarks>
    public override string ToString() => "ingress queue";
}
