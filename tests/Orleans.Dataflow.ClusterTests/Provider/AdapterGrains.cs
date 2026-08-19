using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Dataflow.ClusterTests.Provider;

/// <summary>
/// What the adapter tests watch, in one place.
/// </summary>
/// <remarks>
/// Static because the cluster is in-process, exactly as <see cref="TestSignals"/> is: a silo and the test
/// share a process, so a static table is the simplest thing that is also true. Every adapter test resets it
/// first, which is safe because the cluster tests share one collection and therefore run one at a time.
/// </remarks>
internal static class AdapterObservations
{
    private static int _inFlight;
    private static int _peakInFlight;
    private static int _opened;
    private static int _disposed;

    /// <summary>Gets the prices the recording sink call has been handed, in the order it was handed them.</summary>
    internal static ConcurrentQueue<AdapterPrice> Recorded { get; } = new();

    /// <summary>Gets the prices a consumer grain has read off a stream.</summary>
    internal static ConcurrentQueue<AdapterPrice> Published { get; } = new();

    /// <summary>Gets the elements the counting sink has seen, in the order it saw them.</summary>
    internal static ConcurrentQueue<object?> Counted { get; } = new();

    /// <summary>Gets the number of gated calls in flight right now.</summary>
    internal static int InFlight => Volatile.Read(ref _inFlight);

    /// <summary>Gets the greatest number of gated calls that were ever in flight at once.</summary>
    internal static int PeakInFlight => Volatile.Read(ref _peakInFlight);

    /// <summary>Gets the number of grain enumerations that were opened.</summary>
    internal static int Opened => Volatile.Read(ref _opened);

    /// <summary>Gets the number of grain enumerations that were disposed.</summary>
    internal static int Disposed => Volatile.Read(ref _disposed);

    /// <summary>Forgets everything, so one test's observations are its own.</summary>
    internal static void Reset()
    {
        Recorded.Clear();
        Published.Clear();
        Counted.Clear();
        Volatile.Write(ref _inFlight, 0);
        Volatile.Write(ref _peakInFlight, 0);
        Volatile.Write(ref _opened, 0);
        Volatile.Write(ref _disposed, 0);
    }

    /// <summary>Records that one gated call has started.</summary>
    internal static void Entered()
    {
        int now = Interlocked.Increment(ref _inFlight);
        int peak = Volatile.Read(ref _peakInFlight);

        while (now > peak)
        {
            int seen = Interlocked.CompareExchange(ref _peakInFlight, now, peak);

            if (seen == peak)
            {
                break;
            }

            peak = seen;
        }
    }

    /// <summary>Records that one gated call has finished.</summary>
    internal static void Left() => Interlocked.Decrement(ref _inFlight);

    /// <summary>Records that one grain enumeration was opened.</summary>
    internal static void Open() => Interlocked.Increment(ref _opened);

    /// <summary>Records that one grain enumeration was disposed.</summary>
    internal static void Dispose() => Interlocked.Increment(ref _disposed);
}

/// <summary>The grain the priced-order calls reach.</summary>
public interface IAdapterPricingGrain : IGrainWithStringKey
{
    /// <summary>Prices one order.</summary>
    /// <param name="order">The order.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The price.</returns>
    Task<AdapterPrice> PriceAsync(AdapterOrder order, CancellationToken cancellationToken);

    /// <summary>Prices one order after the test has released the gate, counting how many are held.</summary>
    /// <param name="order">The order.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The price.</returns>
    Task<AdapterPrice> PriceGatedAsync(AdapterOrder order, CancellationToken cancellationToken);

    /// <summary>Prices one order once that order's own signal is raised.</summary>
    /// <param name="order">The order.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The price.</returns>
    Task<AdapterPrice> PriceOnSignalAsync(AdapterOrder order, CancellationToken cancellationToken);

    /// <summary>Refuses to price one order.</summary>
    /// <param name="order">The order.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>Nothing; the call throws.</returns>
    Task<AdapterPrice> PriceRefusedAsync(AdapterOrder order, CancellationToken cancellationToken);

    /// <summary>Holds one order until the test releases every held call.</summary>
    /// <param name="order">The order.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The price.</returns>
    Task<AdapterPrice> PriceHeldAsync(AdapterOrder order, CancellationToken cancellationToken);
}

/// <summary>The pricing grain.</summary>
/// <remarks>
/// Marked <see cref="ReentrantAttribute"/> because the concurrency test needs several calls in flight in
/// one grain at once: a non-reentrant grain would serialize them and the bound under test would be
/// unobservable. That is a property of the test's instrument and not of the adapter, which bounds calls in
/// flight whatever the grain does with them.
/// </remarks>
[Reentrant]
public sealed class AdapterPricingGrain : Grain, IAdapterPricingGrain
{
    /// <summary>The signal the gated call waits for.</summary>
    internal const string GateSignal = "adapter-gate-release";

    /// <summary>The signal the held call waits for.</summary>
    internal const string HeldSignal = "adapter-held-release";

    /// <summary>The prefix of the per-order signals the signalled call waits for.</summary>
    internal const string SignalPrefix = "adapter-price-";

    /// <inheritdoc/>
    public Task<AdapterPrice> PriceAsync(AdapterOrder order, CancellationToken cancellationToken) =>
        Task.FromResult(Price(order));

    /// <inheritdoc/>
    public async Task<AdapterPrice> PriceGatedAsync(AdapterOrder order, CancellationToken cancellationToken)
    {
        AdapterObservations.Entered();

        try
        {
            await TestSignals.Reached(GateSignal).WaitAsync(cancellationToken);

            return Price(order);
        }
        finally
        {
            AdapterObservations.Left();
        }
    }

    /// <inheritdoc/>
    public async Task<AdapterPrice> PriceOnSignalAsync(AdapterOrder order, CancellationToken cancellationToken)
    {
        AdapterObservations.Entered();

        try
        {
            await TestSignals
                .Reached(SignalPrefix + order.Amount.ToString(CultureInfo.InvariantCulture))
                .WaitAsync(cancellationToken);

            return Price(order);
        }
        finally
        {
            AdapterObservations.Left();
        }
    }

    /// <inheritdoc/>
    public Task<AdapterPrice> PriceRefusedAsync(AdapterOrder order, CancellationToken cancellationToken) =>
        throw new InvalidOperationException(
            $"the pricing grain refuses the order '{order.Id}' and says so in words a hop cannot lose");

    /// <inheritdoc/>
    public async Task<AdapterPrice> PriceHeldAsync(AdapterOrder order, CancellationToken cancellationToken)
    {
        AdapterObservations.Entered();

        try
        {
            await TestSignals.Reached(HeldSignal);

            return Price(order);
        }
        finally
        {
            AdapterObservations.Left();
        }
    }

    /// <summary>Prices one order.</summary>
    /// <param name="order">The order.</param>
    /// <returns>The price.</returns>
    private static AdapterPrice Price(AdapterOrder order) => new(order.Id, order.Amount * 10L);
}

/// <summary>The grain the recording sink call reaches.</summary>
public interface IAdapterLedgerGrain : IGrainWithStringKey
{
    /// <summary>Records one price.</summary>
    /// <param name="price">The price.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>A task that completes when the price has been recorded.</returns>
    Task RecordAsync(AdapterPrice price, CancellationToken cancellationToken);

    /// <summary>Records one price once the test has released the ledger, counting how many are held.</summary>
    /// <param name="price">The price.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>A task that completes when the price has been recorded.</returns>
    Task RecordGatedAsync(AdapterPrice price, CancellationToken cancellationToken);

    /// <summary>Parks on a release nothing raises, so that only the caller's token can end the call.</summary>
    /// <param name="price">The price, which is never recorded because this call never gets that far.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>A task that never completes successfully.</returns>
    /// <remarks>
    /// The instrument for the one claim about a terminating grain call that no other callee here can make:
    /// whether the run's cancellation reaches a call this sink already has in flight. Every other held call
    /// in this suite is released by a signal the test raises, so a test using one could not tell a call the
    /// run cancelled from a call the test let go. This one has nothing to release it, so a call that ends at
    /// all ended because its caller's token crossed the hop.
    /// </remarks>
    Task RecordUntilCancelledAsync(AdapterPrice price, CancellationToken cancellationToken);

    /// <summary>Records one price once the test releases it, giving up if its caller's token is cancelled.</summary>
    /// <param name="price">The price.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>A task that completes when the price has been recorded.</returns>
    /// <remarks>
    /// The other half of <see cref="RecordUntilCancelledAsync"/>'s instrument, and it measures the opposite
    /// claim: a callee that <em>would</em> abandon its work if its caller cancelled, held while a graceful
    /// shutdown is asked for. A shutdown drains into a sink rather than abandoning it, so this call must not
    /// be cancelled by one — and a sink built on the stop token instead of the run token is exactly what
    /// would cancel it.
    /// </remarks>
    Task RecordWhenReleasedAsync(AdapterPrice price, CancellationToken cancellationToken);

    /// <summary>Writes one price into the log the test process keeps.</summary>
    /// <param name="price">The price.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>A task that completes when the price has been written down.</returns>
    /// <remarks>
    /// The log rather than the shared observations, so that a test in its own collection can read what this
    /// grain was handed without racing another collection's resets. It is also what makes the record outlive
    /// the silo the grain happened to be hosted on, which is the whole point of asking a callee what it saw
    /// after a crash.
    /// </remarks>
    Task LogAsync(AdapterPrice price, CancellationToken cancellationToken);
}

/// <summary>The ledger grain.</summary>
/// <remarks>
/// Reentrant for the reason the pricing grain is: the sink's own bound is what the test is about, and a
/// non-reentrant grain would serialize the calls and hide it.
/// </remarks>
[Reentrant]
public sealed class AdapterLedgerGrain : Grain, IAdapterLedgerGrain
{
    /// <summary>The signal the gated ledger waits for.</summary>
    internal const string GateSignal = "adapter-ledger-release";

    /// <summary>The signal the cancellable ledger raises once a call has reached it.</summary>
    internal const string CancellableEntered = "adapter-ledger-cancellable-entered";

    /// <summary>The signal the cancellable ledger raises when its caller's token is cancelled.</summary>
    internal const string CancellableCancelled = "adapter-ledger-cancellable-cancelled";

    /// <summary>The signal the cancellable ledger waits for, which nothing in this suite ever raises.</summary>
    /// <remarks>
    /// Named rather than left as a literal because its whole meaning is that it is unraised: a reader who
    /// searches this suite for it finds exactly one mention, which is the proof that the wait below has no
    /// way out but the token.
    /// </remarks>
    internal const string CancellableRelease = "adapter-ledger-cancellable-release";

    /// <summary>The signal the draining ledger raises once a call has reached it.</summary>
    internal const string DrainEntered = "adapter-ledger-drain-entered";

    /// <summary>The signal the draining ledger waits for, which the test raises after asking for a shutdown.</summary>
    internal const string DrainRelease = "adapter-ledger-drain-release";

    /// <summary>The log the logging ledger writes every price it is handed into.</summary>
    internal const string Log = "adapter-ledger-log";

    /// <inheritdoc/>
    public Task LogAsync(AdapterPrice price, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(price);

        TestDeliveries.Record(Log, price.Total);

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task RecordAsync(AdapterPrice price, CancellationToken cancellationToken)
    {
        AdapterObservations.Recorded.Enqueue(price);

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public async Task RecordGatedAsync(AdapterPrice price, CancellationToken cancellationToken)
    {
        AdapterObservations.Entered();

        try
        {
            await TestSignals.Reached(GateSignal);

            AdapterObservations.Recorded.Enqueue(price);
        }
        finally
        {
            AdapterObservations.Left();
        }
    }

    /// <inheritdoc/>
    public async Task RecordUntilCancelledAsync(AdapterPrice price, CancellationToken cancellationToken)
    {
        AdapterObservations.Entered();

        // Raised before the wait, so a test that has observed it knows the call is inside this grain rather
        // than still on its way to it.
        TestSignals.Raise(CancellableEntered);

        try
        {
            await TestSignals.Reached(CancellableRelease).WaitAsync(cancellationToken);

            AdapterObservations.Recorded.Enqueue(price);
        }
        catch (OperationCanceledException)
        {
            // The whole measurement. Nothing raises the release, so reaching here means the token this call
            // was handed was cancelled and the cancellation crossed the hop from the run that made the call.
            TestSignals.Raise(CancellableCancelled);

            throw;
        }
        finally
        {
            AdapterObservations.Left();
        }
    }

    /// <inheritdoc/>
    public async Task RecordWhenReleasedAsync(AdapterPrice price, CancellationToken cancellationToken)
    {
        AdapterObservations.Entered();

        TestSignals.Raise(DrainEntered);

        try
        {
            // Cooperative on purpose. This callee would abandon its work if its caller cancelled, so a run
            // whose sink carried the stop token would lose the element the shutdown was letting through.
            await TestSignals.Reached(DrainRelease).WaitAsync(cancellationToken);

            AdapterObservations.Recorded.Enqueue(price);
        }
        finally
        {
            AdapterObservations.Left();
        }
    }
}

/// <summary>The grain that produces into and consumes from a stream from its own context.</summary>
public interface IAdapterStreamGrain : IGrainWithStringKey
{
    /// <summary>Publishes one order.</summary>
    /// <param name="providerName">The stream provider's registration name.</param>
    /// <param name="streamNamespace">The stream namespace.</param>
    /// <param name="key">The stream key.</param>
    /// <param name="order">The order.</param>
    /// <returns>A task that completes when the provider has accepted the order.</returns>
    Task PublishAsync(string providerName, string streamNamespace, string key, AdapterOrder order);

    /// <summary>Subscribes to a stream of prices and collects what arrives.</summary>
    /// <param name="providerName">The stream provider's registration name.</param>
    /// <param name="streamNamespace">The stream namespace.</param>
    /// <param name="key">The stream key.</param>
    /// <returns>A task that completes when the subscription exists.</returns>
    Task CollectAsync(string providerName, string streamNamespace, string key);

    /// <summary>Counts the subscriptions this grain itself holds on a stream of prices.</summary>
    /// <param name="providerName">The stream provider's registration name.</param>
    /// <param name="streamNamespace">The stream namespace.</param>
    /// <param name="key">The stream key.</param>
    /// <returns>The count.</returns>
    Task<int> CountOwnSubscriptionsAsync(string providerName, string streamNamespace, string key);
}

/// <summary>The stream grain.</summary>
public sealed class AdapterStreamGrain : Grain, IAdapterStreamGrain
{
    /// <inheritdoc/>
    public Task PublishAsync(string providerName, string streamNamespace, string key, AdapterOrder order) =>
        this.GetStreamProvider(providerName)
            .GetStream<AdapterOrder>(StreamId.Create(streamNamespace, key))
            .OnNextAsync(order);

    /// <inheritdoc/>
    public async Task CollectAsync(string providerName, string streamNamespace, string key) =>
        _ = await this.GetStreamProvider(providerName)
            .GetStream<AdapterPrice>(StreamId.Create(streamNamespace, key))
            .SubscribeAsync((price, _) =>
            {
                AdapterObservations.Published.Enqueue(price);

                return Task.CompletedTask;
            });

    /// <inheritdoc/>
    public async Task<int> CountOwnSubscriptionsAsync(string providerName, string streamNamespace, string key)
    {
        IList<StreamSubscriptionHandle<AdapterPrice>> handles = await this
            .GetStreamProvider(providerName)
            .GetStream<AdapterPrice>(StreamId.Create(streamNamespace, key))
            .GetAllSubscriptionHandles();

        return handles.Count;
    }
}

/// <summary>The grain whose asynchronous enumeration heads a run.</summary>
public interface IAdapterFeedGrain : IGrainWithStringKey
{
    /// <summary>Enumerates orders.</summary>
    /// <param name="count">How many to yield, or zero to yield without end.</param>
    /// <param name="cancellationToken">The run's token, carried by Orleans to this grain.</param>
    /// <returns>The sequence.</returns>
    IAsyncEnumerable<AdapterOrder> EnumerateAsync(int count, CancellationToken cancellationToken);
}

/// <summary>The feed grain, instrumented so that opening and disposing it are facts a test can read.</summary>
public sealed class AdapterFeedGrain : Grain, IAdapterFeedGrain
{
    /// <summary>The signal the endless feed raises once it has yielded its first order.</summary>
    internal const string EndlessSignal = "adapter-endless-started";

    /// <inheritdoc/>
    public async IAsyncEnumerable<AdapterOrder> EnumerateAsync(
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        AdapterObservations.Open();

        try
        {
            for (long index = 1; count == 0 || index <= count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                yield return new AdapterOrder(
                    string.Create(CultureInfo.InvariantCulture, $"order-{index}"),
                    index);

                if (count == 0 && index == 1)
                {
                    TestSignals.Raise(EndlessSignal);
                }

                if (count == 0)
                {
                    // An endless feed that spun would starve the silo. Yielding the thread between elements
                    // keeps it cooperative and keeps the token the only thing that stops it.
                    await Task.Yield();
                }
            }
        }
        finally
        {
            AdapterObservations.Dispose();
        }
    }
}
