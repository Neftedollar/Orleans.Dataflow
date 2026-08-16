using System.Collections;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// A sequence that records how a runtime treated it, and can be told to misbehave on demand.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
/// <remarks>
/// <para>
/// Written by hand rather than as an iterator method, because every claim the runtime tests make is about
/// something an iterator hides: how many times the sequence was enumerated, how many elements were
/// actually pulled, whether the enumerator was released, and how many elements the runtime was holding at
/// once.
/// </para>
/// <para>
/// <see cref="PeakInFlight"/> is the one that proves the bound. The sequence counts an element as in
/// flight from the moment it hands it out; the graph's terminal calls <see cref="Consumed"/> when it is
/// done with it. A runtime that read ahead would hand out a second element before the first was consumed
/// and the peak would be two, whatever the final results looked like.
/// </para>
/// </remarks>
internal sealed class RecordingEnumerable<T> : IEnumerable<T>
{
    private readonly IReadOnlyList<T> _elements;
    private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _enumerations;
    private int _pulls;
    private int _releases;
    private int _inFlight;
    private int _peakInFlight;

    /// <summary>Initializes a new instance of the <see cref="RecordingEnumerable{T}"/> class.</summary>
    /// <param name="elements">The elements to hand out, in order.</param>
    internal RecordingEnumerable(params T[] elements) => _elements = elements;

    /// <summary>Gets or sets the failure to raise instead of producing an enumerator.</summary>
    /// <value>The exception instance to throw, or <see langword="null"/> to enumerate normally.</value>
    internal Exception? EnumerationFailure { get; set; }

    /// <summary>Gets or sets the failure to raise when the enumerator is released.</summary>
    /// <value>The exception instance to throw from disposal, or <see langword="null"/> to release cleanly.</value>
    /// <remarks>The release is still counted before the failure is raised, so a test can assert both.</remarks>
    internal Exception? ReleaseFailure { get; set; }

    /// <summary>Gets or sets the failure to raise from a pull, chosen per zero-based element position.</summary>
    /// <value>
    /// A function returning the exception instance to throw before producing the element at that position,
    /// or <see langword="null"/> to produce it; <see langword="null"/> to never fail.
    /// </value>
    internal Func<int, Exception?>? PullFailure { get; set; }

    /// <summary>Gets or sets the hold to apply before a pull, chosen per zero-based element position.</summary>
    /// <value>
    /// A function returning the task the pull blocks on before producing the element at that position, or
    /// <see langword="null"/> to produce it at once; <see langword="null"/> to never hold.
    /// </value>
    /// <remarks>
    /// This is how a test sequences a run that has more than one segment in it. A gate inside a stage says
    /// "stop the run here"; this says "stop the source until something else has happened", which is what
    /// makes a buffer's contents at a given moment a fact rather than a race. It is consulted for the pull
    /// past the last element too, so a test can also observe the moment a sequence runs out — every
    /// element before it has by then been handed to the run and offered onwards.
    /// </remarks>
    internal Func<int, Task?>? PullBarrier { get; set; }

    /// <summary>Gets or sets the observer of every element handed out.</summary>
    /// <value>
    /// An action receiving the number of elements handed out so far, counting this one; or
    /// <see langword="null"/> to observe nothing.
    /// </value>
    /// <remarks>
    /// Called after the element is counted and before the pull returns, so a test can learn that a run has
    /// reached a bound at the moment it reaches it rather than by asking afterwards.
    /// </remarks>
    internal Action<int>? Pulled { get; set; }

    /// <summary>Gets the task that completes when an enumerator of this sequence is first released.</summary>
    /// <remarks>
    /// A run releases its enumerator on every terminal path, so this completing means the segment that
    /// pulled from this sequence has stopped — which is how a test observes a source segment that failed
    /// without being able to signal anything itself.
    /// </remarks>
    internal Task Released => _released.Task;

    /// <summary>Gets the number of times this sequence was enumerated.</summary>
    internal int Enumerations => Volatile.Read(ref _enumerations);

    /// <summary>Gets the number of elements this sequence handed out.</summary>
    internal int Pulls => Volatile.Read(ref _pulls);

    /// <summary>Gets the number of times an enumerator of this sequence was released.</summary>
    internal int Releases => Volatile.Read(ref _releases);

    /// <summary>Gets the greatest number of elements that were in flight at one moment.</summary>
    internal int PeakInFlight => Volatile.Read(ref _peakInFlight);

    /// <summary>Records that the graph's terminal has finished with the element it was handed.</summary>
    internal void Consumed() => Interlocked.Decrement(ref _inFlight);

    /// <inheritdoc/>
    public IEnumerator<T> GetEnumerator()
    {
        Interlocked.Increment(ref _enumerations);

        return EnumerationFailure is { } failure ? throw failure : new Cursor(this);
    }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>Records that one element was handed out and tells the observer about it.</summary>
    private void Yielded()
    {
        int pulls = Interlocked.Increment(ref _pulls);

        int inFlight = Interlocked.Increment(ref _inFlight);

        if (inFlight > Volatile.Read(ref _peakInFlight))
        {
            Volatile.Write(ref _peakInFlight, inFlight);
        }

        Pulled?.Invoke(pulls);
    }

    /// <summary>Records that an enumerator was released, and fails if it was told to.</summary>
    private void Release()
    {
        Interlocked.Increment(ref _releases);
        _released.TrySetResult();

        if (ReleaseFailure is { } failure)
        {
            throw failure;
        }
    }

    /// <summary>One enumeration of a <see cref="RecordingEnumerable{T}"/>.</summary>
    /// <param name="owner">The sequence being enumerated.</param>
    private sealed class Cursor(RecordingEnumerable<T> owner) : IEnumerator<T>
    {
        private int _position = -1;

        /// <inheritdoc/>
        public T Current => owner._elements[_position];

        /// <inheritdoc/>
        object? IEnumerator.Current => Current;

        /// <inheritdoc/>
        public bool MoveNext()
        {
            int next = _position + 1;

            if (owner.PullFailure?.Invoke(next) is { } failure)
            {
                throw failure;
            }

            // Blocking, because a source that takes a long time is a source that blocks its segment's own
            // thread, and holding it any other way would be testing something the runtime does not do.
            owner.PullBarrier?.Invoke(next)?.GetAwaiter().GetResult();

            if (next >= owner._elements.Count)
            {
                return false;
            }

            _position = next;
            owner.Yielded();

            return true;
        }

        /// <inheritdoc/>
        public void Reset() => throw new NotSupportedException("A run enumerates a sequence once, forwards.");

        /// <inheritdoc/>
        public void Dispose() => owner.Release();
    }
}
