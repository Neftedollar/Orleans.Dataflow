using Orleans.Dataflow.Runtime;

namespace Orleans.Dataflow.Testing;

/// <summary>
/// The typed producer facade over one run's source probe.
/// </summary>
/// <typeparam name="T">The element type the probe emits.</typeparam>
/// <remarks>
/// <para>
/// A probe source is an ingress queue of one element under backpressure, and this is the rendezvous laid
/// over it. The queue supplies everything a probe needs but one: room for an element and the fact that it
/// was accepted, the four outcomes an offer can have, the completion and failure that end a stream, the
/// shutdown-aware wait the reading segment parks in, and the pull accounting that makes demand measurable.
/// What it does not supply is the moment the run <em>takes</em> an element, because no ordinary producer
/// needs it; that is the one thing this type adds, through the queue's observer seam, and it is the whole
/// difference between "accepted into a buffer" and "handed over".
/// </para>
/// <para>
/// Composing rather than writing a second queue is not only economy. The stop discipline, the pause
/// accounting, and the drain-versus-abandon split are runtime semantics with tests of their own; a probe
/// carrying its own copy of them would be a second implementation that could quietly disagree with the one
/// under test, which is exactly the wrong property for a measuring instrument.
/// </para>
/// <para>
/// The rendezvous is counted rather than flagged. An emit that is cancelled leaves its element in the
/// queue — cancelling a wait does not un-hand an element — so "mine has been taken" cannot be "an element
/// has been taken"; it is "the number taken has reached the number offered including mine", which stays
/// true however many earlier emits stopped waiting.
/// </para>
/// </remarks>
internal sealed class SourceProbe<T> : ISourceProbe<T>, ILocalQueueObserver
{
    private readonly LocalIngressQueue _queue;
    private readonly Lock _gate = new();
    private TaskCompletionSource? _waiter;
    private long _offered;
    private long _taken;
    private long _awaited;
    private bool _emitting;
    private bool _ended;

    /// <summary>Initializes a new instance of the <see cref="SourceProbe{T}"/> class.</summary>
    /// <param name="queue">The run's own queue, which this probe watches and offers into.</param>
    /// <remarks>
    /// The observer is attached here, which is when the plan is compiled and before any segment starts, so
    /// no element can be taken before there is anything watching for it.
    /// </remarks>
    internal SourceProbe(LocalIngressQueue queue)
    {
        _queue = queue;

        queue.Observe(this);
    }

    /// <inheritdoc/>
    public long PullsObserved => _queue.Pulls;

    /// <inheritdoc/>
    public async ValueTask EmitAsync(T element, CancellationToken cancellationToken = default)
    {
        Begin();

        try
        {
            QueueOfferOutcome outcome = await _queue.OfferAsync(element, cancellationToken).ConfigureAwait(false);

            if (outcome is not QueueOfferOutcome.Accepted)
            {
                throw ProbeTerminatedException.Refused(outcome);
            }

            // The offer put the element into a queue of one; this is the half that makes an emit a
            // rendezvous. The run completes the wait by taking the element and the run's end fails it, so
            // the two ways this stops waiting are the two things that can happen to an emitted element.
            if (Handed() is not { } taken)
            {
                return;
            }

            await (cancellationToken.CanBeCanceled
                ? taken.WaitAsync(cancellationToken)
                : taken).ConfigureAwait(false);
        }
        finally
        {
            End();
        }
    }

    /// <inheritdoc/>
    public void Complete() => _queue.Complete();

    /// <inheritdoc/>
    public void Fail(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        _queue.Fail(exception);
    }

    /// <summary>Returns a one-line diagnostic summary of this probe.</summary>
    /// <returns>Text of the form <c>source probe (3 pulls)</c>.</returns>
    /// <remarks>The count is the demand meter at the moment of the call, and the method never throws.</remarks>
    public override string ToString() => $"source probe ({PullsObserved} pulls)";

    /// <inheritdoc/>
    void ILocalQueueObserver.Taken()
    {
        TaskCompletionSource? waiter = null;

        lock (_gate)
        {
            _taken++;

            if (_waiter is not null && _taken >= _awaited)
            {
                waiter = _waiter;
                _waiter = null;
            }
        }

        _ = waiter?.TrySetResult();
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Both halves matter. The flag makes every later emit fail at once instead of offering into a queue
    /// nothing will read, and an emit still waiting is failed rather than left waiting for an element
    /// nobody will take. The runtime reports the end more than once, so both are written idempotently.
    /// </remarks>
    void ILocalQueueObserver.Ended()
    {
        TaskCompletionSource? waiter;

        lock (_gate)
        {
            _ended = true;
            waiter = _waiter;
            _waiter = null;
        }

        _ = waiter?.TrySetException(ProbeTerminatedException.Closed());
    }

    /// <summary>Claims this probe for one emit.</summary>
    /// <exception cref="ProbeTerminatedException">The run has already ended.</exception>
    /// <exception cref="InvalidOperationException">An earlier emit is still outstanding.</exception>
    private void Begin()
    {
        lock (_gate)
        {
            if (_ended)
            {
                throw ProbeTerminatedException.Closed();
            }

            if (_emitting)
            {
                throw new InvalidOperationException(
                    "This probe is already emitting an element the run has not taken yet. A probe hands the run one element at a time, because 'the run has taken it' is a statement about one element; emit the next one after this call returns.");
            }

            _emitting = true;
        }
    }

    /// <summary>Releases this probe at the end of one emit.</summary>
    private void End()
    {
        lock (_gate)
        {
            _emitting = false;
            _waiter = null;
        }
    }

    /// <summary>Registers the wait for the element this emit has just offered.</summary>
    /// <returns>
    /// The task that completes when the run takes it, or <see langword="null"/> when it has already been
    /// taken.
    /// </returns>
    /// <exception cref="ProbeTerminatedException">The run ended between the offer and this call.</exception>
    private Task? Handed()
    {
        lock (_gate)
        {
            long ticket = ++_offered;

            if (_taken >= ticket)
            {
                return null;
            }

            if (_ended)
            {
                throw ProbeTerminatedException.Closed();
            }

            _awaited = ticket;
            _waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            return _waiter.Task;
        }
    }
}
