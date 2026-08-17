using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.OrleansTests.Provider;
using Orleans.Hosting;
using Orleans.TestingHost;
using Xunit;
using ReminderOptions = Orleans.Hosting.ReminderOptions;

namespace Orleans.Dataflow.OrleansTests.Cluster;

/// <summary>
/// One in-process cluster running Orleans.Dataflow with the test vocabulary registered, shared by every
/// test in the collection.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="InProcessTestCluster"/> rather than the out-of-process one: phase 1 is about a run executing
/// in a cluster at all, and every question it answers — start, complete, fail, drain, cancel, fence,
/// refuse — is answered by one silo. Multi-silo behavior, killing a silo and waiting for liveness to
/// stabilize, is a later phase's, and a test cluster that spawned processes to prove none of it would only
/// spend time.
/// </para>
/// <para>
/// One silo, and stated as a choice rather than left as a default: a second silo would place coordinators
/// and runs on either of two hosts and make every test's timing a distribution, without testing anything
/// this phase claims. What it would test — that ownership survives a silo dying — is exactly what phase 4
/// builds.
/// </para>
/// <para>
/// Built once per collection because deploying a cluster costs seconds and every test here is independent
/// of every other: pipelines are addressed by graph identity, and each test uses its own.
/// </para>
/// </remarks>
public sealed class DataflowCluster : IAsyncLifetime
{
    /// <summary>The storage provider name Orleans streaming requires for its pub-sub records.</summary>
    /// <remarks>
    /// Fixed by Orleans rather than chosen here: the streaming runtime resolves its subscription store under
    /// exactly this name, and a memory store is what makes the whole thing non-durable by design.
    /// </remarks>
    internal const string PubSubStore = "PubSubStore";

    /// <summary>The reminder period this cluster enforces as its floor.</summary>
    /// <remarks>
    /// One second, which is far below Orleans' own default of one minute and is what makes a reminder test
    /// finish. The option is what the trigger adapter checks a document against, so lowering it here is not
    /// a test convenience bolted on the side but the same knob a deployment turns.
    /// </remarks>
    internal static readonly TimeSpan MinimumReminderPeriod = TimeSpan.FromSeconds(1);

    /// <summary>Gets the deployed cluster.</summary>
    /// <value>The cluster, available from the moment the fixture has initialized.</value>
    internal InProcessTestCluster Cluster { get; private set; } = null!;

    /// <summary>Gets the client host every test materializes pipelines through.</summary>
    internal OrleansDataflowHost Host { get; private set; } = null!;

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);

            // The memory stream provider and the pub-sub store it needs, registered exactly as the research
            // notes say a deployment registers them: the adapters name a provider and never configure one.
            _ = silo.AddMemoryGrainStorage(PubSubStore);
            _ = silo.AddMemoryStreams(AdapterVocabulary.StreamProvider);

            // Reminders, with a floor a test can live with. The in-memory table is non-durable, which is
            // exactly what a single-silo test wants: the reminder definition survives an activation and
            // nothing more, which is the only durability this phase claims.
            _ = silo.UseInMemoryReminderService();
            _ = silo.Configure<ReminderOptions>(options =>
                options.MinimumReminderPeriod = MinimumReminderPeriod);

            // Two broadcast channel providers with opposite delivery modes, so that the sink's declared
            // mode is checked against something rather than against itself.
            _ = silo.AddBroadcastChannel(
                AdapterVocabulary.BroadcastProvider,
                options => options.FireAndForgetDelivery = false);
            _ = silo.AddBroadcastChannel(
                AdapterVocabulary.FireAndForgetBroadcastProvider,
                options => options.FireAndForgetDelivery = true);

            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(TestVocabulary.Catalog())
                .AddFactory(TestVocabulary.Provider, new TestStageFactory())
                .AddCatalog(AdapterVocabulary.Catalog())
                .AddFactory(AdapterVocabulary.Provider, new AdapterStageFactory())
                .AddStreamElement(AdapterVocabulary.OrderElement)
                .AddStreamElement(AdapterVocabulary.PriceElement)
                .AddGrainCall(AdapterVocabulary.Pricing)
                .AddGrainCall(AdapterVocabulary.GatedPricing)
                .AddGrainCall(AdapterVocabulary.SignalledPricing)
                .AddGrainCall(AdapterVocabulary.FailingPricing)
                .AddGrainCall(AdapterVocabulary.HangingPricing)
                .AddKeyedGrainCall(AdapterVocabulary.KeyedPricing)
                .AddKeyedGrainCall(AdapterVocabulary.GatedKeyedPricing)
                .AddKeyedGrainCall(AdapterVocabulary.FailingKeyedPricing)
                .AddGrainCallSink(AdapterVocabulary.Recording)
                .AddGrainCallSink(AdapterVocabulary.GatedRecording)
                .AddGrainEnumerable(AdapterVocabulary.Feed)
                .AddGrainEnumerable(AdapterVocabulary.KeyedFeed)
                .AddGrainEnumerable(AdapterVocabulary.EndlessFeed)
                .AddObserverBridge(AdapterVocabulary.OrderBridge)
                .AddObserverBridge(AdapterVocabulary.NarrowBridge)
                .AddBroadcastElement(AdapterVocabulary.BroadcastOrder)
                .AddObservable(AdapterVocabulary.SharedOrders));
        });

        // The client-side registration, exercised the way a deployment writes it rather than by newing the
        // host up: what a client configures is one call on its services, and a test that skipped it would
        // leave the only supported spelling unproven.
        builder.ConfigureClientHost(client =>
            client.Services.AddOrleansDataflowClient(options =>
                options.PollInterval = TimeSpan.FromMilliseconds(10)));

        Cluster = builder.Build();

        await Cluster.DeployAsync();

        Host = Cluster.Client.ServiceProvider.GetRequiredService<OrleansDataflowHost>();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.DisposeAsync();
        }
    }
}

/// <summary>
/// The collection every cluster test belongs to.
/// </summary>
/// <remarks>
/// One collection and therefore one cluster: the tests run one after another against one deployment rather
/// than each paying for its own. They are independent of each other by construction — every test addresses
/// its own pipeline identity — so sharing costs them nothing.
/// </remarks>
[CollectionDefinition(Name)]
public sealed class DataflowClusterCollectionDefinition : ICollectionFixture<DataflowCluster>
{
    /// <summary>The collection's name.</summary>
    public const string Name = "orleans-dataflow-cluster";
}
