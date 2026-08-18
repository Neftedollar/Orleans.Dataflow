using Orleans.Dataflow.Adapters;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Tests.Api;
using Xunit;
using static Orleans.Dataflow.Tests.Api.RegisteredJunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the in-process host's registration surface accepts, and what it refuses while the host is being
/// built.
/// </summary>
/// <remarks>
/// The mirror of the silo builder's own registration rules, and deliberately the same rules: one catalog
/// entry per stage reference, one factory per provider, and every check made when the host is constructed
/// rather than at the first graph. A deployment that writes one registration and hands it to both hosts
/// should meet one contract, not two.
/// </remarks>
public sealed class LocalHostRegistrationTests
{
    [Fact]
    public void AHostThatRegistersNothingStillRunsTheLocalVocabulary()
    {
        // The configuring overload with an empty configuration is exactly the lambda-only host: the local
        // vocabulary is always present, and merging it with nothing is itself.
        LocalDataflowHost configured = new(_ => { });

        Assert.NotNull(configured);
    }

    [Fact]
    public async Task ACatalogRegisteredHereIsResolvedBesideTheLocalVocabulary()
    {
        // A registered catalog adds to the local vocabulary rather than replacing it, which is what a mixed
        // chain needs: a lambda stage and a registered stage compose in one chain, so a host that resolved
        // only one of the two could not materialize what the authoring surface can close.
        LocalDataflowHost host = new(builder => builder
            .AddCatalog(Catalog)
            .AddFactory(RegisteredJunctionProvider.Provider, new RegisteredJunctionProvider()));

        RunnableGraph lambdas = Source.From([1, 2, 3]).To(sink => sink.Count(), "counted", out ResultSlot<long> counted);

        await using RunHandle run = await host.MaterializeAsync(lambdas, TestToken);

        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public void OneProviderRegisteredTwiceIsRefusedWhenTheHostIsBuilt()
    {
        // A provider ships one vocabulary and one factory builds it, so two registrations are two answers to
        // one question rather than a merge.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => new LocalDataflowHost(builder => builder
                .AddCatalog(Catalog)
                .AddFactory(RegisteredJunctionProvider.Provider, new RegisteredJunctionProvider())
                .AddFactory(RegisteredJunctionProvider.Provider, new RegisteredJunctionProvider())));

        Assert.Contains("more than one runtime factory", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OneStageReferenceRegisteredTwiceIsRefusedWhenTheHostIsBuilt()
    {
        // Two specifications for one reference are two answers to one question, and the catalog says so.
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => new LocalDataflowHost(builder => builder.AddCatalog(Catalog).AddCatalog(Catalog)));

        Assert.Contains("orleans-test", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AFactoryRegisteredAgainstTheDefaultProviderIsRefused()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => new LocalDataflowHost(builder => builder.AddFactory(default, new RegisteredJunctionProvider())));

        Assert.Equal("provider", refused.ParamName);
    }

    [Fact]
    public void ANullCatalogOrFactoryIsRefused()
    {
        Assert.Throws<ArgumentNullException>(() => new LocalDataflowHost(builder => builder.AddCatalog(null!)));
        Assert.Throws<ArgumentNullException>(
            () => new LocalDataflowHost(builder => builder.AddFactory(RegisteredJunctionProvider.Provider, null!)));
    }

    [Fact]
    public void TheDotnetVocabularyAndAProviderCatalogCoexistOnOneHost()
    {
        // "Declare once, use twice" reaches further than the .NET adapters: one host learns the runtime
        // neutral vocabulary and a provider's own, exactly as one silo does, and neither registration knows
        // about the other.
        LocalDataflowHost host = new(builder => builder
            .AddDotnetStages()
            .AddCatalog(Catalog)
            .AddFactory(RegisteredJunctionProvider.Provider, new RegisteredJunctionProvider()));

        Assert.NotNull(host);
    }

    [Fact]
    public async Task AProviderStageRunsOnAHostThatAlsoPublishesTheDotnetVocabulary()
    {
        // The claim above, made to do something: the registered branching pipeline runs on the host that
        // also published the .NET adapters, so the merged catalog and the merged factory registry are both
        // real rather than merely constructed.
        LocalDataflowHost host = new(builder => builder
            .AddDotnetStages()
            .AddObservable(DotnetFixtures.Binding("notes", new TestObservable<string>()))
            .AddCatalog(Catalog)
            .AddFactory(RegisteredJunctionProvider.Provider, new RegisteredJunctionProvider()));

        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> left, out ResultSlot<long> _);

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        Assert.Equal(3L, await run.GetValueAsync(left, TestToken));
    }
}
