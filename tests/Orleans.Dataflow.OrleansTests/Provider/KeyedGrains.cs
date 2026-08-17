using System.Collections.Concurrent;
using System.Globalization;
using Orleans.Concurrency;
using Orleans.Metadata;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Orleans.Dataflow.OrleansTests.Provider;

/// <summary>
/// What the keyed tests watch: which key each call reached, in what order, and how many were ever in flight
/// at once — in total and within one key.
/// </summary>
/// <remarks>
/// The per-key peak is the whole point and is the number the stage's contract is about. A global peak says
/// the declared bound is respected, which the plain grain call already proves; a per-key peak of one says
/// that a key's elements never overlap, which is where the stage's ordering promise comes from and is the
/// only thing that distinguishes this stage from the unkeyed one.
/// </remarks>
internal static class KeyedObservations
{
    private static int _inFlight;
    private static int _peakInFlight;
    private static int _generation;

    /// <summary>Gets the arrivals, in the order the grains recorded them.</summary>
    internal static ConcurrentQueue<KeyedArrival> Arrivals { get; } = new();

    /// <summary>Gets the greatest number of calls ever in flight at once within one key.</summary>
    internal static ConcurrentDictionary<string, int> PeakPerKey { get; } = new(StringComparer.Ordinal);

    /// <summary>Gets the number of calls in flight right now, across every key.</summary>
    internal static int InFlight => Volatile.Read(ref _inFlight);

    /// <summary>Gets the greatest number of calls ever in flight at once, across every key.</summary>
    internal static int PeakInFlight => Volatile.Read(ref _peakInFlight);

    /// <summary>Gets the keys that have a call in flight right now.</summary>
    internal static ConcurrentDictionary<string, int> Live { get; } = new(StringComparer.Ordinal);

    /// <summary>Gets the name of the signal the gated keyed call is waiting for right now.</summary>
    /// <remarks>
    /// One signal per test rather than one for the suite, and the generation is what makes it so. Signals
    /// are raised once and stay raised, so a fixed name would mean the second test to use the gate found it
    /// already open and measured a stage that was never held — which is a test that passes without asserting
    /// anything. Resetting bumps the generation, so a test that forgets to reset gets a name nobody raises
    /// and fails loudly instead.
    /// </remarks>
    internal static string Gate =>
        string.Create(CultureInfo.InvariantCulture, $"adapter-keyed-gate-{Volatile.Read(ref _generation)}");

    /// <summary>Forgets everything, so one test's observations are its own.</summary>
    internal static void Reset()
    {
        Arrivals.Clear();
        PeakPerKey.Clear();
        Live.Clear();
        Volatile.Write(ref _inFlight, 0);
        Volatile.Write(ref _peakInFlight, 0);
        _ = Interlocked.Increment(ref _generation);
    }

    /// <summary>Records that one call for a key has started.</summary>
    /// <param name="key">The key.</param>
    /// <param name="amount">The amount of the order the call carries.</param>
    internal static void Entered(string key, long amount)
    {
        Arrivals.Enqueue(new KeyedArrival(key, amount));

        Raise(ref _inFlight, ref _peakInFlight);

        int now = Live.AddOrUpdate(key, 1, static (_, held) => held + 1);

        _ = PeakPerKey.AddOrUpdate(key, now, (_, peak) => Math.Max(peak, now));
    }

    /// <summary>Records that one call for a key has finished.</summary>
    /// <param name="key">The key.</param>
    internal static void Left(string key)
    {
        _ = Interlocked.Decrement(ref _inFlight);
        _ = Live.AddOrUpdate(key, 0, static (_, held) => held - 1);
    }

    /// <summary>Counts one arrival and remembers the peak it produced.</summary>
    /// <param name="count">The counter.</param>
    /// <param name="peak">The peak.</param>
    private static void Raise(ref int count, ref int peak)
    {
        int now = Interlocked.Increment(ref count);
        int seen = Volatile.Read(ref peak);

        while (now > seen)
        {
            int found = Interlocked.CompareExchange(ref peak, now, seen);

            if (found == seen)
            {
                return;
            }

            seen = found;
        }
    }
}

/// <summary>One call's arrival at a keyed grain.</summary>
/// <param name="Key">The key the element was routed to.</param>
/// <param name="Amount">The amount of the order, which is also its position in the run.</param>
internal readonly record struct KeyedArrival(string Key, long Amount);

/// <summary>
/// The grain a keyed call reaches, addressed by the key the routing function produced.
/// </summary>
/// <remarks>
/// Reentrant on purpose, and the whole keyed suite would be vacuous without it. A non-reentrant grain
/// serializes the calls that reach it, so a per-key peak of one would be that grain's doing and would say
/// nothing about the adapter. Reentrant, the only thing holding a key to one call at a time is the stage's
/// own credit — which is exactly the claim under test.
/// </remarks>
public interface IAdapterKeyedGrain : IGrainWithStringKey
{
    /// <summary>Prices one order, recording that it arrived.</summary>
    /// <param name="order">The order.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The price.</returns>
    Task<AdapterPrice> PriceAsync(AdapterOrder order, CancellationToken cancellationToken);

    /// <summary>Prices one order once the test has released the gate.</summary>
    /// <param name="order">The order.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The price.</returns>
    Task<AdapterPrice> PriceGatedAsync(AdapterOrder order, CancellationToken cancellationToken);

    /// <summary>Refuses to price one order.</summary>
    /// <param name="order">The order.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>Nothing; the call throws.</returns>
    Task<AdapterPrice> PriceRefusedAsync(AdapterOrder order, CancellationToken cancellationToken);
}

/// <summary>The keyed grain.</summary>
[Reentrant]
public sealed class AdapterKeyedGrain : Grain, IAdapterKeyedGrain
{
    /// <inheritdoc/>
    public async Task<AdapterPrice> PriceAsync(AdapterOrder order, CancellationToken cancellationToken)
    {
        string key = this.GetPrimaryKeyString();

        KeyedObservations.Entered(key, order.Amount);

        try
        {
            // The yield is what makes the arrival order worth recording. Answering inside the arriving turn
            // would mean a key never had two calls to overlap even if the stage had sent two.
            await Task.Yield();

            return Price(order);
        }
        finally
        {
            KeyedObservations.Left(key);
        }
    }

    /// <inheritdoc/>
    public async Task<AdapterPrice> PriceGatedAsync(AdapterOrder order, CancellationToken cancellationToken)
    {
        string key = this.GetPrimaryKeyString();

        KeyedObservations.Entered(key, order.Amount);

        try
        {
            await TestSignals.Reached(KeyedObservations.Gate).WaitAsync(cancellationToken);

            return Price(order);
        }
        finally
        {
            KeyedObservations.Left(key);
        }
    }

    /// <inheritdoc/>
    public Task<AdapterPrice> PriceRefusedAsync(AdapterOrder order, CancellationToken cancellationToken) =>
        throw new InvalidOperationException(
            $"the keyed grain refuses the order '{order.Id}' and says so in words two hops cannot lose");

    /// <summary>Prices one order.</summary>
    /// <param name="order">The order.</param>
    /// <returns>The price.</returns>
    private static AdapterPrice Price(AdapterOrder order) => new(order.Id, order.Amount * 10L);
}

/// <summary>
/// The grain that answers what placement strategy its own silo would use for a grain type.
/// </summary>
/// <remarks>
/// Placement is resolved inside a silo and nowhere else, so a test that wants to know what a silo would do
/// has to ask one — the same reason the reminder probe is a grain. Asking Orleans' own
/// <see cref="PlacementStrategyResolver"/> rather than this package's resolver is deliberate: what matters
/// is not that the resolver returns the right answer when called, but that the runtime calls it at all and
/// prefers it to the default it would otherwise have used.
/// </remarks>
public interface IPlacementProbeGrain : IGrainWithStringKey
{
    /// <summary>Reports the placement strategy this silo would use for one of the dataflow grain types.</summary>
    /// <param name="grain">
    /// <c>run</c> for the run grain, <c>executor</c> for the keyed executor, or <c>probe</c> for this grain
    /// itself, which nothing configures and which therefore reports the cluster's own default.
    /// </param>
    /// <returns>The strategy's CLR type name.</returns>
    Task<string> StrategyAsync(string grain);
}

/// <summary>The placement probe grain.</summary>
internal sealed class PlacementProbeGrain(PlacementStrategyResolver placement, GrainTypeResolver types)
    : Grain, IPlacementProbeGrain
{
    /// <inheritdoc/>
    public Task<string> StrategyAsync(string grain)
    {
        ArgumentNullException.ThrowIfNull(grain);

        Type target = grain switch
        {
            "run" => typeof(Dataflow.Grains.PipelineRunGrain),
            "executor" => typeof(Dataflow.Grains.KeyedExecutorGrain),
            _ => typeof(PlacementProbeGrain),
        };

        return Task.FromResult(placement.GetPlacementStrategy(types.GetGrainType(target)).GetType().Name);
    }
}
