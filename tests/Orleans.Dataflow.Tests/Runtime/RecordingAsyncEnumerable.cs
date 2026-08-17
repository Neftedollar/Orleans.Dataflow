namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// An asynchronous sequence that records how a runtime treated it, and can be told to misbehave on demand.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
/// <remarks>
/// <para>
/// The asynchronous counterpart of <see cref="RecordingEnumerable{T}"/>, written by hand for the same
/// reason: every claim the runtime tests make about an asynchronous source is about something an
/// <see langword="async"/> iterator hides. How many times the sequence was opened, which token it was
/// opened with, whether the enumeration's <c>DisposeAsync</c> was awaited to completion rather than merely
/// started, and whether a pull outstanding at cancellation was awaited to its outcome — none of those is
/// visible from a compiler-generated state machine.
/// </para>
/// <para>
/// <see cref="DisposalCompleted"/> is the one that proves the contract that matters most. Disposal is
/// counted when it is entered and this task completes only when it returns, so a runtime that started the
/// disposal and moved on would leave the count at one and this task incomplete.
/// </para>
/// </remarks>
internal sealed class RecordingAsyncEnumerable<T> : IAsyncEnumerable<T>
{
    private readonly IReadOnlyList<T> _elements;
    private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _enumerations;
    private int _pulls;
    private int _disposals;
    private int _completedDisposals;

    /// <summary>Initializes a new instance of the <see cref="RecordingAsyncEnumerable{T}"/> class.</summary>
    /// <param name="elements">The elements to hand out, in order.</param>
    internal RecordingAsyncEnumerable(params T[] elements) => _elements = elements;

    /// <summary>Gets or sets the failure to raise from a pull, chosen per zero-based element position.</summary>
    /// <value>
    /// A function returning the exception instance to throw before producing the element at that position,
    /// or <see langword="null"/> to produce it; <see langword="null"/> to never fail.
    /// </value>
    internal Func<int, Exception?>? PullFailure { get; set; }

    /// <summary>Gets or sets the hold to apply before a pull, chosen per zero-based element position.</summary>
    /// <value>
    /// A function returning the task the pull awaits before producing the element at that position, or
    /// <see langword="null"/> to produce it at once; <see langword="null"/> to never hold.
    /// </value>
    /// <remarks>
    /// Awaited rather than blocked on, which is the whole point of an asynchronous source: the hold is
    /// where a test decides whether the pull observes the token it was opened with.
    /// </remarks>
    internal Func<int, Task?>? PullBarrier { get; set; }

    /// <summary>Gets or sets the hold to apply inside the enumeration's asynchronous disposal.</summary>
    /// <value>The task the disposal awaits, or <see langword="null"/> to release at once.</value>
    /// <remarks>
    /// This is how a test tells "the disposal was started" apart from "the disposal finished": with a hold
    /// here, a run that did not await the disposal would complete while
    /// <see cref="DisposalCompleted"/> was still pending.
    /// </remarks>
    internal Task? DisposalBarrier { get; set; }

    /// <summary>Gets or sets whether a pull ignores the token it was opened with.</summary>
    /// <value>
    /// <see langword="true"/> to await the barrier without the token, which is the badly behaved source the
    /// slow-source rule is about.
    /// </value>
    internal bool IgnoresToken { get; set; }

    /// <summary>Gets the task that completes when an enumeration's disposal has finished.</summary>
    internal Task DisposalCompleted => _disposed.Task;

    /// <summary>Gets the number of times this sequence was enumerated.</summary>
    internal int Enumerations => Volatile.Read(ref _enumerations);

    /// <summary>Gets the number of elements this sequence handed out.</summary>
    internal int Pulls => Volatile.Read(ref _pulls);

    /// <summary>Gets the number of times an enumeration's disposal was entered.</summary>
    internal int Disposals => Volatile.Read(ref _disposals);

    /// <summary>Gets the number of times an enumeration's disposal returned.</summary>
    internal int CompletedDisposals => Volatile.Read(ref _completedDisposals);

    /// <summary>Gets the token the most recent enumeration was opened with.</summary>
    /// <remarks>
    /// A source is opened with the run's own token, which is what
    /// <c>WithCancellation</c> would have supplied; a test reads it here to prove the run passed one at all.
    /// </remarks>
    internal CancellationToken OpenedWith { get; private set; }

    /// <inheritdoc/>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _enumerations);
        OpenedWith = cancellationToken;

        return new Cursor(this, cancellationToken);
    }

    /// <summary>One enumeration of a <see cref="RecordingAsyncEnumerable{T}"/>.</summary>
    /// <param name="owner">The sequence being enumerated.</param>
    /// <param name="cancellationToken">The token the enumeration was opened with.</param>
    private sealed class Cursor(RecordingAsyncEnumerable<T> owner, CancellationToken cancellationToken)
        : IAsyncEnumerator<T>
    {
        private int _position = -1;

        /// <inheritdoc/>
        public T Current => owner._elements[_position];

        /// <inheritdoc/>
        public async ValueTask<bool> MoveNextAsync()
        {
            int next = _position + 1;

            if (owner.PullFailure?.Invoke(next) is { } failure)
            {
                throw failure;
            }

            if (owner.PullBarrier?.Invoke(next) is { } barrier)
            {
                await (owner.IgnoresToken ? barrier : barrier.WaitAsync(cancellationToken)).ConfigureAwait(false);
            }

            if (next >= owner._elements.Count)
            {
                return false;
            }

            _position = next;
            Interlocked.Increment(ref owner._pulls);

            return true;
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref owner._disposals);

            if (owner.DisposalBarrier is { } barrier)
            {
                await barrier.ConfigureAwait(false);
            }

            Interlocked.Increment(ref owner._completedDisposals);
            owner._disposed.TrySetResult();
        }
    }
}
