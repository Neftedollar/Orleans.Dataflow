using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Orleans.Hosting;
using Orleans.TestingHost;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// A vocabulary of two stages whose source restores its position the way an author might rather than the
/// way this repository's own providers do.
/// </summary>
/// <remarks>
/// <para>
/// Every cursor and mark this library ships validates what it is handed and refuses it with
/// <see cref="InvalidOperationException"/>, which is exactly the type the resume path used to catch. That
/// makes the shipped providers useless as evidence: the question is what happens when a provider a
/// <em>deployment</em> wrote does the obvious thing instead — reads the member it wrote and lets the reader
/// throw — and no stage in this suite did that until this one.
/// </para>
/// <para>
/// Nothing here is a straw man. <c>GetProperty</c> raising <see cref="KeyNotFoundException"/> over a renamed
/// member is what an author gets for writing the shortest correct-looking code, and a state written by one
/// revision of a restore function and read by the next is the ordinary way to reach it.
/// </para>
/// </remarks>
internal static class RestoreProbeVocabulary
{
    /// <summary>The provider these two stages belong to.</summary>
    internal static ProviderId Provider { get; } = ProviderId.Create("restore-probe");

    /// <summary>The source that counts, and restores its count by reading a member it expects to be there.</summary>
    internal static StageRef Counted { get; } = StageRef.Create(Provider, StageId.Create("counted"), 1);

    /// <summary>The sink that writes down what it is handed.</summary>
    internal static StageRef Recorded { get; } = StageRef.Create(Provider, StageId.Create("recorded"), 1);

    /// <summary>Gets the catalog a silo registers to run this vocabulary.</summary>
    /// <returns>The catalog.</returns>
    /// <remarks>
    /// The element and parameter contracts are the test vocabulary's own, because they are the part of a
    /// stage this file has nothing to say about: what is new here is one cursor's restore behaviour, and
    /// re-declaring contracts to carry it would only add ways for the two vocabularies to disagree.
    /// </remarks>
    internal static StageCatalog Catalog() =>
        StageCatalog.Create(
        [
            StageSpecification.Source(
                Counted,
                TestVocabulary.NoParameters,
                Port.Out("out", TestVocabulary.Number)),
            StageSpecification.Sink(
                Recorded,
                TestVocabulary.NoParameters,
                Port.In("in", TestVocabulary.Number)),
        ]);
}

/// <summary>
/// Builds the two stages of <see cref="RestoreProbeVocabulary"/>.
/// </summary>
internal sealed class RestoreProbeStageFactory : IDataflowStageFactory
{
    /// <summary>The log every occurrence of this vocabulary's sink writes to.</summary>
    internal const string Log = "restore-probe";

    /// <inheritdoc/>
    public DataflowStageRuntime Create(DataflowStageRequest request)
    {
        if (request.Node.Stage == RestoreProbeVocabulary.Counted)
        {
            BrittleCursor cursor = new();

            return DataflowStageRuntime.Source(tokens => Numbers(cursor, tokens), cursor);
        }

        if (request.Node.Stage == RestoreProbeVocabulary.Recorded)
        {
            return DataflowStageRuntime.Terminal(
                static () => null,
                static (state, element) =>
                {
                    TestDeliveries.Record(Log, (long)element!);

                    return state;
                },
                finish: null,
                producesResult: false);
        }

        throw new InvalidOperationException(
            $"The stage '{request.Node.Stage}' is not one this provider builds.");
    }

    /// <summary>Emits four numbers, opening wherever the cursor says.</summary>
    /// <param name="cursor">The occurrence's cursor.</param>
    /// <param name="tokens">The run's tokens, which this source does not need.</param>
    /// <param name="cancellationToken">The enumeration's token.</param>
    /// <returns>The elements.</returns>
    private static async IAsyncEnumerable<object?> Numbers(
        BrittleCursor cursor,
        DataflowRunTokens tokens,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _ = tokens;
        _ = cancellationToken;

        for (long element = cursor.Reached + 1; element <= 4L; element++)
        {
            yield return element;

            await Task.Yield();
        }
    }

    /// <summary>A cursor that reads its stored position the way an author writes one the first time.</summary>
    private sealed class BrittleCursor : DataflowSourceCursor
    {
        private long _delivered;

        /// <summary>Gets how many elements this source has delivered.</summary>
        internal long Reached => Interlocked.Read(ref _delivered);

        /// <inheritdoc/>
        public override CanonicalJsonValue Position =>
            CanonicalJsonValue.Parse($"{{\"index\":{Interlocked.Read(ref _delivered)}}}");

        /// <inheritdoc/>
        public override void Delivered() => _ = Interlocked.Increment(ref _delivered);

        /// <inheritdoc/>
        /// <remarks>
        /// No validation and no refusal of its own: it reads the member it wrote, and the reader raises
        /// <see cref="KeyNotFoundException"/> when the member is not there. That is the point of this type.
        /// </remarks>
        public override void RestoreTo(CanonicalJsonValue position) =>
            _delivered = position.ToElement().GetProperty("index").GetInt64();
    }
}

/// <summary>
/// One in-process cluster running the restore-probe vocabulary.
/// </summary>
public sealed class DurableRestoreCluster : IAsyncLifetime
{
    /// <summary>Gets the store this cluster's durable runs keep their checkpoints in.</summary>
    /// <remarks>Static for the reason the outage fixture's is: the silo delegate must close over nothing.</remarks>
    internal static InMemoryCheckpointStore Checkpoints { get; } = new();

    /// <summary>Gets the deployed cluster.</summary>
    internal InProcessTestCluster Cluster { get; private set; } = null!;

    /// <summary>Gets the client host every test here materializes pipelines through.</summary>
    internal OrleansDataflowHost Host { get; private set; } = null!;

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        InProcessTestClusterBuilder builder = new(initialSilosCount: 1);

        builder.ConfigureSilo((siloOptions, silo) =>
        {
            _ = silo.AddMemoryGrainStorage(OrleansDataflowStorage.CoordinatorProviderName);
            _ = silo.AddOrleansDataflow(dataflow => dataflow
                .AddCatalog(RestoreProbeVocabulary.Catalog())
                .AddFactory(RestoreProbeVocabulary.Provider, new RestoreProbeStageFactory())
                .UseCheckpointStore(_ => Checkpoints));
        });

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
/// The collection the restore-refusal tests share one cluster through.
/// </summary>
[CollectionDefinition(Name)]
public sealed class DurableRestoreClusterCollectionDefinition : ICollectionFixture<DurableRestoreCluster>
{
    /// <summary>The collection's name.</summary>
    public const string Name = "orleans-dataflow-durable-restore";
}

/// <summary>
/// What a resume does when the author code that restores a stored state throws something nobody planned for.
/// </summary>
/// <remarks>
/// <para>
/// <b>Restoring is the one place a resume runs an author's code</b>, and it runs it on the activation's own
/// turn, before an element exists. The resume path caught <see cref="InvalidOperationException"/> — the type
/// this repository's own providers refuse with — so anything else left the grain method as itself. This
/// repository already records why that is worse than it sounds: an exception chain is only as serializable as
/// its least prepared link, so what the client received could be a codec failure where the diagnosis should
/// have been.
/// </para>
/// <para>
/// The refusal it becomes is <see cref="PipelineResumeRefusedException"/> rather than a rejected start,
/// because that is what it is: the run exists, its position is on disk, and the thing that cannot continue it
/// is what the store handed back. A caller reads that as "reconcile the checkpoint or start a new run", which
/// is a different action from "fix the deployment".
/// </para>
/// </remarks>
[Collection(DurableRestoreClusterCollectionDefinition.Name)]
public sealed class DurableRestoreRefusalTests(DurableRestoreCluster cluster)
{
    /// <summary>Gets the token that fails a hung test rather than letting it block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ARestoreFunctionThatThrowsAnythingAtAllRefusesTheResumeByName()
    {
        const string Run = "brittle";

        PipelineDefinition pipeline = Pipeline("restore-brittle");

        TestDeliveries.Clear(RestoreProbeStageFactory.Log);

        // A checkpoint of this very document at this very revision, so every refusal the resume path checks
        // for itself passes and the only thing left to fail is the author's own restore function. The stored
        // position names a member the cursor does not read, which is what a state written by one revision of
        // a restore function and read by the next looks like.
        _ = await DurableRestoreCluster.Checkpoints.WriteAsync(
            GraphId.Create(pipeline.Id.Value),
            RunId.Create(Run),
            LocalCheckpointDocument.Write(
                pipeline.Fingerprint,
                GraphRevision.Create(1),
                new Dictionary<NodeId, CanonicalJsonValue>
                {
                    [NodeId.Create("numbers")] = CanonicalJsonValue.Parse("{\"at\":2}"),
                },
                new Dictionary<NodeId, CanonicalJsonValue>(),
                new Dictionary<NodeId, CanonicalJsonValue>()),
            expectedETag: null,
            Token);

        PipelineResumeRefusedException refused = await Assert.ThrowsAsync<PipelineResumeRefusedException>(
            () => cluster.Host.MaterializeDurableAsync(
                pipeline,
                new DurablePipelineOptions { RunId = Run, EveryElements = 2 },
                Token));

        // Named, and named with the thing that actually happened: the type and the message of what the
        // author's code threw, carried as text because that is all that survives the hop.
        Assert.Contains(typeof(KeyNotFoundException).FullName!, refused.Message, StringComparison.Ordinal);
        Assert.Contains(Run, refused.Message, StringComparison.Ordinal);
        Assert.Equal(pipeline.Fingerprint.ToString(), refused.DeclaredFingerprint);
        Assert.Equal(pipeline.Fingerprint.ToString(), refused.StoredFingerprint);

        // Nothing ran. A resume that refuses leaves the run exactly where it was, which is what makes the
        // refusal a thing an operator can act on rather than a thing that has already happened.
        Assert.Empty(TestDeliveries.Of(RestoreProbeStageFactory.Log));
        Assert.True(DurableRestoreCluster.Checkpoints.Holds(
            GraphId.Create(pipeline.Id.Value),
            RunId.Create(Run)));
    }

    [Fact]
    public async Task ARestoreFunctionThatReadsWhatItWroteContinuesTheRun()
    {
        const string Run = "sound";

        PipelineDefinition pipeline = Pipeline("restore-sound");

        TestDeliveries.Clear(RestoreProbeStageFactory.Log);

        // The counter-check, and it is what makes the test above a statement about the throwing restore
        // rather than about this vocabulary being unresumable. The same cursor, handed the position it
        // writes, continues the run from it.
        _ = await DurableRestoreCluster.Checkpoints.WriteAsync(
            GraphId.Create(pipeline.Id.Value),
            RunId.Create(Run),
            LocalCheckpointDocument.Write(
                pipeline.Fingerprint,
                GraphRevision.Create(1),
                new Dictionary<NodeId, CanonicalJsonValue>
                {
                    [NodeId.Create("numbers")] = CanonicalJsonValue.Parse("{\"index\":2}"),
                },
                new Dictionary<NodeId, CanonicalJsonValue>(),
                new Dictionary<NodeId, CanonicalJsonValue>()),
            expectedETag: null,
            Token);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeDurableAsync(
            pipeline,
            new DurablePipelineOptions { RunId = Run, EveryElements = 2 },
            Token);

        await Deadline.Within(handle.Completion, $"the resumed run {handle.RunId} completed");

        Assert.Equal([3L, 4L], TestDeliveries.Of(RestoreProbeStageFactory.Log));
    }

    /// <summary>Builds the two-stage pipeline this file's tests run.</summary>
    /// <param name="id">The pipeline's identity, which is also its coordinator's key.</param>
    /// <returns>The pipeline.</returns>
    private static PipelineDefinition Pipeline(string id)
    {
        StageCatalog catalog = RestoreProbeVocabulary.Catalog();

        RunnableGraph graph = Source
            .FromRegistered(
                RegisteredStage.Source(catalog, RestoreProbeVocabulary.Counted, TestVocabulary.Number),
                "numbers",
                TestVocabulary.Empty)
            .To(
                RegisteredStage.Sink(catalog, RestoreProbeVocabulary.Recorded, TestVocabulary.Number),
                "recorded",
                TestVocabulary.Empty);

        return graph.AsPipeline(GraphId.Create(id), GraphRevision.Create(1));
    }
}
