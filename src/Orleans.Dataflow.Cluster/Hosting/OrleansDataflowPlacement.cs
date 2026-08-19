using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Grains;
using Orleans.Metadata;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Orleans.Dataflow.Hosting;

/// <summary>
/// Where a deployment wants Orleans.Dataflow's own grains placed.
/// </summary>
/// <remarks>
/// <para>
/// A deliberately short list, and every entry is one of Orleans' own strategies rather than anything
/// invented here: placing grains is the cluster's job and this only chooses which of its answers to ask for.
/// A deployment that wants a strategy not named here writes its own
/// <c>IPlacementStrategyResolver</c>, which is the extension point Orleans already publishes and which this
/// is one implementation of.
/// </para>
/// <para>
/// The reason this exists at all is that the cluster default changed. Orleans 9.2 made
/// <c>ResourceOptimizedPlacement</c> the default, so a run grain and a keyed executor now land wherever the
/// silos' load says — which is usually right and is exactly wrong for a test that means to assert that keyed
/// work spread across silos, because a lightly loaded cluster may honestly put everything on one host.
/// Pinning <see cref="Random"/> is how such a test asks for spread instead of hoping for it.
/// </para>
/// </remarks>
public enum DataflowPlacement
{
    /// <summary>Leave the decision to the cluster, whatever it has been configured to do.</summary>
    /// <remarks>
    /// The default, and it defers rather than naming a strategy: a deployment that has configured its own
    /// default placement keeps it, and a resolver that answered "resource optimized" here would silently
    /// override that with today's Orleans default the day Orleans changed it again.
    /// </remarks>
    ClusterDefault = 0,

    /// <summary>Place each activation on a silo chosen at random.</summary>
    /// <remarks>
    /// Spread without regard to load, and the strategy a test pins when it means to assert that work
    /// reached more than one silo.
    /// </remarks>
    Random = 1,

    /// <summary>Place each activation on the silo that first called it, when that silo can host it.</summary>
    /// <remarks>
    /// The opposite trade: no network hop for the common call, and no spread at all. Reasonable for keyed
    /// executors whose work is cheap and whose caller is one run.
    /// </remarks>
    PreferLocal = 2,

    /// <summary>Place each activation on the silo its identity hashes to.</summary>
    /// <remarks>
    /// The one that makes a key's placement a property of the key. Two runs asking for the same partition
    /// still get different executors — an executor's address carries the run — but one run's keys are
    /// spread deterministically rather than by load, which is what a deployment wants when it has arranged
    /// its own data by the same key.
    /// </remarks>
    HashBased = 3,
}

/// <summary>
/// What a silo was told about placing the run grain and the keyed executor grains.
/// </summary>
/// <remarks>
/// Internal because it is not something anything reads twice: a deployment states it once through
/// <see cref="IOrleansDataflowBuilder.UsePlacement"/> and the resolver below is the only consumer. Making it
/// a public options class would publish a shape that says nothing a deployment cannot already say.
/// </remarks>
internal sealed class OrleansDataflowPlacementOptions
{
    /// <summary>Gets or sets where run grains are placed.</summary>
    internal DataflowPlacement RunGrains { get; set; } = DataflowPlacement.ClusterDefault;

    /// <summary>Gets or sets where the keyed stage's per-key executor grains are placed.</summary>
    internal DataflowPlacement KeyedExecutors { get; set; } = DataflowPlacement.ClusterDefault;
}

/// <summary>
/// The resolver that answers Orleans' placement question for this package's two placeable grain types.
/// </summary>
/// <param name="services">The silo's container, which the grain-type resolver is read from.</param>
/// <param name="options">What the deployment asked for.</param>
/// <remarks>
/// <para>
/// <see cref="IPlacementStrategyResolver"/> is Orleans' own hook and is consulted before the attribute a
/// grain class carries, which is what makes placement a hosting decision here rather than a compile-time
/// one. A grain class annotated with <c>[RandomPlacement]</c> would have fixed the answer in this assembly,
/// and a deployment that wanted the other answer would have had no way to say so.
/// </para>
/// <para>
/// It answers for exactly two grain types and defers for every other, including this package's own bridges
/// and triggers. Those are not placement decisions a deployment has any reason to make: a bridge and a
/// trigger are addressed by one run and called rarely, whereas a run is a unit of load and an executor is a
/// partition of one. Deferring is <see langword="false"/> rather than a strategy, so the cluster's own
/// default still applies.
/// </para>
/// <para>
/// The grain types are resolved through <see cref="GrainTypeResolver"/> rather than spelled as text, so the
/// mapping cannot drift from the classes it names: renaming a grain or giving one an explicit type alias
/// changes both sides at once. They are read lazily, on the first placement question rather than in the
/// constructor, because this resolver is constructed as part of the placement machinery itself and asking
/// the container for more of it while it is being built is a cycle waiting to be discovered by somebody
/// else.
/// </para>
/// </remarks>
internal sealed class DataflowPlacementStrategyResolver(
    IServiceProvider services,
    OrleansDataflowPlacementOptions options) : IPlacementStrategyResolver
{
    private readonly Lock _gate = new();
    private GrainType _run;
    private GrainType _executor;
    private bool _resolved;

    /// <inheritdoc/>
    public bool TryResolvePlacementStrategy(
        GrainType grainType,
        GrainProperties properties,
        out PlacementStrategy result)
    {
        Resolve();

        if (grainType.Equals(_run))
        {
            return Strategy(options.RunGrains, out result);
        }

        if (grainType.Equals(_executor))
        {
            return Strategy(options.KeyedExecutors, out result);
        }

        result = null!;

        return false;
    }

    /// <summary>Maps one declared choice onto Orleans' own strategy.</summary>
    /// <param name="placement">The choice.</param>
    /// <param name="result">The strategy, when there is one.</param>
    /// <returns><see langword="false"/> when the choice is to leave the decision to the cluster.</returns>
    private static bool Strategy(DataflowPlacement placement, out PlacementStrategy result)
    {
        result = placement switch
        {
            DataflowPlacement.Random => Spread,
            DataflowPlacement.PreferLocal => Local,
            DataflowPlacement.HashBased => Hashed,
            _ => null!,
        };

        return result is not null;
    }

    /// <summary>Gets the random-placement strategy this silo hands out.</summary>
    private static RandomPlacement Spread { get; } = new();

    /// <summary>Gets the prefer-local strategy this silo hands out.</summary>
    private static PreferLocalPlacement Local { get; } = new();

    /// <summary>Gets the hash-based strategy this silo hands out.</summary>
    /// <remarks>
    /// One instance of each rather than a fresh one per question. A placement strategy is a value with no
    /// state of its own, and this is a path the runtime takes for every activation it places.
    /// </remarks>
    private static HashBasedPlacement Hashed { get; } = new();

    /// <summary>Reads the two grain types this resolver answers for, once.</summary>
    private void Resolve()
    {
        if (Volatile.Read(ref _resolved))
        {
            return;
        }

        lock (_gate)
        {
            if (_resolved)
            {
                return;
            }

            GrainTypeResolver types = services.GetRequiredService<GrainTypeResolver>();

            _run = types.GetGrainType(typeof(PipelineRunGrain));
            _executor = types.GetGrainType(typeof(KeyedExecutorGrain));

            Volatile.Write(ref _resolved, true);
        }
    }
}
