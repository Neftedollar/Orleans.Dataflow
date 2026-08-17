using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Hosting;
using Xunit;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The hosts, contracts, and controllable observable the .NET push-adapter tests are written against.
/// </summary>
/// <remarks>
/// <para>
/// Every host here is built by the constructor a deployment uses, with the very binding the graph is
/// authored against, because that pairing is the thing under test: one declaration handed to a host and to
/// the authoring helpers, and a document that names it.
/// </para>
/// <para>
/// The observable is hand-rolled rather than taken from a library, and it is deliberately the crudest
/// possible one: subscribing adds an observer, pushing calls <c>OnNext</c> on the calling thread, and
/// disposing removes it. That is what makes a claim about who pays for backpressure checkable — the thread
/// the test pushed on is the thread that blocks.
/// </para>
/// </remarks>
internal static class DotnetFixtures
{
    /// <summary>Gets the running test's own cancellation token.</summary>
    internal static CancellationToken TestToken => TestContext.Current.CancellationToken;

    /// <summary>Gets the contract the tests carry notes under.</summary>
    internal static ElementContract<string> NoteContract { get; } =
        ElementContract.For<string>("dotnet-note", 1);

    /// <summary>Gets a host that publishes the vocabulary and registers nothing else.</summary>
    /// <returns>The host.</returns>
    internal static LocalDataflowHost TimerHost() => new(static dataflow => dataflow.AddDotnetStages());

    /// <summary>Builds a host that publishes one observable binding.</summary>
    /// <param name="binding">The binding.</param>
    /// <returns>The host.</returns>
    internal static LocalDataflowHost HostFor(ObservableBinding<string> binding) =>
        new(dataflow => dataflow.AddObservable(binding));

    /// <summary>Declares a binding over one controllable observable.</summary>
    /// <param name="name">The name the document addresses it by.</param>
    /// <param name="observable">The observable, which the binding opens once per run.</param>
    /// <returns>The binding.</returns>
    internal static ObservableBinding<string> Binding(string name, TestObservable<string> observable) =>
        ObservableBinding.Create(name, NoteContract, () => observable);
}

/// <summary>
/// An <see cref="IObservable{T}"/> a test drives by hand, counting its subscriptions and its disposals.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
/// <remarks>
/// Hot by construction: one instance is one producer, and two runs subscribing to it both receive whatever
/// is pushed afterwards. That is the honest shape for testing a bridge, because the adapter's contract is
/// about the subscription and never about where the elements come from.
/// </remarks>
internal sealed class TestObservable<T> : IObservable<T>
{
    private readonly Lock _gate = new();
    private readonly List<IObserver<T>> _observers = [];
    private readonly Milestones _subscribed = new();
    private readonly Milestones _disposed = new();
    private int _subscriptions;
    private int _disposals;
    private Exception? _subscribeFailure;

    /// <summary>Gets how many times this observable has been subscribed to.</summary>
    internal int Subscriptions => Volatile.Read(ref _subscriptions);

    /// <summary>Gets how many of those subscriptions have been disposed.</summary>
    internal int Disposals => Volatile.Read(ref _disposals);

    /// <summary>Waits until this observable has been subscribed to a given number of times.</summary>
    /// <param name="count">How many subscriptions to wait for.</param>
    /// <returns>A task that completes at that subscription, and at once if it already happened.</returns>
    /// <remarks>
    /// The signal that replaces a delay. A run subscribes at its first pull, which is a moment no caller
    /// controls, so a test that pushed before waiting for this would be racing the run rather than testing
    /// it.
    /// </remarks>
    internal Task SubscriptionsReach(int count) => _subscribed.Reached(count);

    /// <summary>Waits until a given number of subscriptions have been disposed.</summary>
    /// <param name="count">How many disposals to wait for.</param>
    /// <returns>A task that completes at that disposal, and at once if it already happened.</returns>
    internal Task DisposalsReach(int count) => _disposed.Reached(count);

    /// <summary>Gets how many observers are subscribed right now.</summary>
    internal int Observers
    {
        get
        {
            lock (_gate)
            {
                return _observers.Count;
            }
        }
    }

    /// <summary>Makes every later subscription throw.</summary>
    /// <param name="failure">The exception to throw.</param>
    internal void FailOnSubscribe(Exception failure) => _subscribeFailure = failure;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentNullException.ThrowIfNull(observer);

        int count = Interlocked.Increment(ref _subscriptions);

        if (_subscribeFailure is { } failure)
        {
            _subscribed.Reach(count);

            throw failure;
        }

        lock (_gate)
        {
            _observers.Add(observer);
        }

        _subscribed.Reach(count);

        return new Subscription(this, observer);
    }

    /// <summary>Pushes one element at every subscriber, on the calling thread.</summary>
    /// <param name="value">The element.</param>
    internal void Push(T value)
    {
        foreach (IObserver<T> observer in Snapshot())
        {
            observer.OnNext(value);
        }
    }

    /// <summary>Ends the sequence for every subscriber.</summary>
    internal void Complete()
    {
        foreach (IObserver<T> observer in Snapshot())
        {
            observer.OnCompleted();
        }
    }

    /// <summary>Fails the sequence for every subscriber.</summary>
    /// <param name="failure">The failure, handed on unchanged.</param>
    internal void Fail(Exception failure)
    {
        foreach (IObserver<T> observer in Snapshot())
        {
            observer.OnError(failure);
        }
    }

    /// <summary>Takes a stable list of the current subscribers.</summary>
    /// <returns>The subscribers.</returns>
    private IObserver<T>[] Snapshot()
    {
        lock (_gate)
        {
            return [.. _observers];
        }
    }

    /// <summary>Removes one observer when the run disposes it.</summary>
    /// <param name="owner">The observable.</param>
    /// <param name="observer">The observer.</param>
    private sealed class Subscription(TestObservable<T> owner, IObserver<T> observer) : IDisposable
    {
        private int _disposed;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            lock (owner._gate)
            {
                _ = owner._observers.Remove(observer);
            }

            owner._disposed.Reach(Interlocked.Increment(ref owner._disposals));
        }
    }
}

/// <summary>
/// A counter a test can await: "tell me when this has happened <c>n</c> times".
/// </summary>
/// <remarks>
/// The generalisation of a one-shot signal, and the reason no test here waits for a length of time. A
/// milestone asked for before it happens completes when it happens; one asked for afterwards completes at
/// once, so a test never has to arrange its awaits before the run it is watching.
/// </remarks>
internal sealed class Milestones
{
    private readonly Lock _gate = new();
    private readonly Dictionary<int, TaskCompletionSource> _pending = [];
    private int _reached;

    /// <summary>Records that the counted thing has happened once more.</summary>
    /// <param name="count">The new count.</param>
    internal void Reach(int count)
    {
        List<TaskCompletionSource> release = [];

        lock (_gate)
        {
            _reached = Math.Max(_reached, count);

            int[] due = [.. _pending.Keys.Where(at => at <= _reached)];

            foreach (int at in due)
            {
                release.Add(_pending[at]);

                _ = _pending.Remove(at);
            }
        }

        foreach (TaskCompletionSource source in release)
        {
            _ = source.TrySetResult();
        }
    }

    /// <summary>Waits until the counted thing has happened a given number of times.</summary>
    /// <param name="count">The count to wait for.</param>
    /// <returns>The task that completes then.</returns>
    internal Task Reached(int count)
    {
        lock (_gate)
        {
            if (_reached >= count)
            {
                return Task.CompletedTask;
            }

            if (!_pending.TryGetValue(count, out TaskCompletionSource? source))
            {
                source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _pending.Add(count, source);
            }

            return source.Task;
        }
    }
}
