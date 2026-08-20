using System.Diagnostics;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.ClusterTests.Provider;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Grains;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Xunit;

namespace Orleans.Dataflow.ClusterTests.Cluster;

/// <summary>
/// ADR 0009 over a silo: a pipeline of registered stages with local plumbing between them deploys, runs,
/// and answers the same number the local runtime answers for the very same document.
/// </summary>
/// <remarks>
/// <para>
/// Every document here is built through the definition plane directly rather than through the fluent API,
/// and it has to be: the authoring surface has no spelling for naming a local occurrence, so a graph it
/// closes declares <c>ephemeral-identity</c> and <c>AsPipeline</c> refuses it. What these documents contain
/// is what that surface will write once it does — the same stage references and the same payloads, written
/// by the same payload writers — under names an author chose.
/// </para>
/// <para>
/// <b>The registered stages of these documents declare the opaque element contract.</b> That is not a
/// stylistic choice: every local port declares <c>local-opaque@v1</c>, the graph compiler's element rule
/// compares an edge's two contracts for equality, and a buffer between two stages carrying a real contract
/// therefore produces one diagnostic per edge across the seam.
/// <c>DeployablePlumbingTests.PlumbingBetweenTwoStagesThatTypeTheirElementsIsStillRefusedByTheElementRule</c>
/// in the core suite measures that refusal directly. It is a gap in ADR 0009, and until it is closed a
/// deployable document with plumbing in it can only be written by a provider whose elements are typed in
/// the CLR rather than in the document.
/// </para>
/// <para>
/// The deterministic proof that a declared capacity bounds a real channel lives in the core suite too,
/// where <c>PipelineMaterializer</c> — the deployable path with the cluster taken out of it — can be driven
/// against a stage that holds an element until a test lets it go. What a cluster adds to that is the wire,
/// and that is what these tests are for.
/// </para>
/// </remarks>
[Collection(DataflowClusterCollectionDefinition.Name)]
public sealed class DeployablePlumbingClusterTests(DataflowCluster cluster)
{
    /// <summary>The name every document here exposes its total under.</summary>
    private const string TotalSlot = "total";

    /// <summary>The duration the delaying document holds each of its elements for.</summary>
    /// <remarks>
    /// Long enough that a run which ignored it finishes measurably sooner and short enough that a suite can
    /// afford it twice. The assertion below it is a lower bound, so a loaded machine makes the test slower
    /// rather than flakier.
    /// </remarks>
    private static readonly TimeSpan Held = TimeSpan.FromMilliseconds(400);

    /// <summary>Gets the token that cancels a hung test rather than letting a run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task APipelineWithABufferInItDeploysAndRunsOnASilo()
    {
        // The claim, on a silo. Nine numbers leave a registered source, four of them pass a named local
        // take, they cross a named local buffer of capacity four, and a registered sink sums them: one
        // through four is ten, and a take whose payload had not been read would have made it forty-five.
        //
        // Before this checkpoint the document was refused twice over — 'nondeployable' stopped it being a
        // pipeline at all, and the coordinator would then have refused 'local/take@v1' and 'local/buffer@v1'
        // for having no registered runtime factory.
        PipelineDefinition pipeline = Plumbed("plumbed-run", count: 9, take: 4, capacity: 4);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        Assert.Equal(10L, await handle.GetValueAsync(Slot(pipeline), Token));
    }

    [Fact]
    public async Task TheSameDocumentAnswersTheSameNumberOnALocalHost()
    {
        // The comparison that matters most. The two paths differ in where the binding table comes from and
        // in nothing else — a silo rehydrates it from the document, a local host is handed the same table —
        // so a disagreement here would mean the deployable path is a second implementation after all.
        PipelineDefinition pipeline = Plumbed("plumbed-parity", count: 9, take: 4, capacity: 4);

        await using OrleansRunHandle deployed = await cluster.Host.MaterializeAsync(pipeline, Token);

        await deployed.Completion;

        long onASilo = await deployed.GetValueAsync(Slot(pipeline), Token);

        RunnableGraph graph = new(
            pipeline.Document,
            pipeline.Fingerprint,
            LocalPlumbing.Bindings(pipeline.Document));
        LocalDataflowHost host = new(builder => builder
            .AddCatalog(TestVocabulary.Catalog())
            .AddFactory(TestVocabulary.Provider, new TestStageFactory()));

        await using RunHandle locally = await host.MaterializeAsync(graph, Token);

        await locally.Completion;

        Assert.Equal(
            onASilo,
            await locally.GetValueAsync(
                ResultSlot<long>.Create(
                    ResultSlotId.Create(TotalSlot),
                    pipeline.Fingerprint,
                    graph.AuthoringNonce),
                Token));
        Assert.Equal(10L, onASilo);
    }

    [Fact]
    public async Task ATimingPayloadIsReadFromTheDocumentAndPacedByTheRunsOwnClock()
    {
        // A second plumbing payload, of a different shape, read on the deployable path: a delay whose
        // duration and holdback are two numbers in the node. Its holdback is one, so the two elements are
        // held one after the other and the run cannot finish sooner than twice the declared delay — which a
        // run that ignored the payload would.
        PipelineDefinition pipeline = Delaying("plumbed-delay", count: 2);
        long started = Stopwatch.GetTimestamp();

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        TimeSpan elapsed = Stopwatch.GetElapsedTime(started);

        Assert.Equal(3L, await handle.GetValueAsync(Slot(pipeline), Token));
        Assert.True(
            elapsed >= Held + Held - TimeSpan.FromMilliseconds(50),
            $"two elements held for {Held.TotalMilliseconds}ms each finished in {elapsed.TotalMilliseconds}ms, so the delay's payload was not what paced the run");
    }

    [Fact]
    public void ABuffersCapacityIsPartOfTheDocumentAndThereforeOfItsIdentity()
    {
        // A capacity that reached the silo as decoration rather than as configuration would leave two
        // pipelines that differ only in it indistinguishable. They are not: the number is in the node, the
        // node is in the canonical bytes, and the fingerprint the client computes is the one the silo
        // reports back on the ticket.
        PipelineDefinition four = Plumbed("plumbed-identity", count: 4, take: null, capacity: 4);
        PipelineDefinition eight = Plumbed("plumbed-identity", count: 4, take: null, capacity: 8);

        Assert.NotEqual(four.Fingerprint, eight.Fingerprint);
        Assert.Equal(
            four.Document.Nodes.Count,
            eight.Document.Nodes.Count);
    }

    [Fact]
    public async Task TheTicketReportsTheSameFingerprintTheClientComputedForAPlumbedDocument()
    {
        PipelineDefinition pipeline = Plumbed("plumbed-ticket", count: 4, take: null, capacity: 8);

        await using OrleansRunHandle handle = await cluster.Host.MaterializeAsync(pipeline, Token);

        await handle.Completion;

        Assert.Equal(pipeline.Fingerprint.ToString(), handle.Ticket.GraphFingerprint);
        Assert.Equal(10L, await handle.GetValueAsync(Slot(pipeline), Token));
    }

    [Fact]
    public async Task ALocalStageWhoseBehaviorIsADelegateIsRefusedByTheSiloByName()
    {
        // A silo must never accept a document it will then fail to build, so the refusal is at the start
        // rather than at materialization, and it names the node, the stage, and the reason. The document is
        // otherwise perfectly formed: named occurrences, real payloads, no capability tokens.
        PipelineDefinition pipeline = Middle("plumbed-refused", "select", "local-parameters");

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("'middle'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("local/select@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("behavior is a delegate", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AValveIsRefusedForItsControlRatherThanForABehaviorItDoesNotHave()
    {
        // The refusal that is not about delegates, kept apart because it is a different mistake: a valve
        // binds nothing at all, and what it produces is an object an author flips by name inside the process
        // that built the graph. A run on a silo has nobody to hand that to.
        PipelineDefinition pipeline = Middle(
            "plumbed-valve",
            "valve",
            "local-valve-parameters",
            CanonicalJsonValue.Parse("""{"mode":"open"}"""));

        PipelineRejectedException refused = await Assert.ThrowsAsync<PipelineRejectedException>(
            () => cluster.Host.MaterializeAsync(pipeline, Token));

        Assert.Contains("local/valve@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("produces a runtime control", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ADeployableDocumentWithPlumbingInItDeclaresNoCapabilityAtAll()
    {
        // What used to make this document not a pipeline. Neither token is present: no stage of it binds
        // behavior, and every occurrence of it is named by its author.
        PipelineDefinition pipeline = Plumbed("plumbed-tokens", count: 4, take: 2, capacity: 8);

        Assert.Empty(pipeline.Document.Capabilities);
        Assert.Equal(
            ["numbers", "out", "queueing", "taken"],
            pipeline.Document.Nodes.Select(node => node.Id.Value));
    }

    /// <summary>Builds the pipeline these tests run: a source, an optional take, a buffer, and a sink.</summary>
    /// <param name="id">The pipeline identity, which is also its coordinator's key.</param>
    /// <param name="count">How many numbers the source emits, counting up from one.</param>
    /// <param name="take">How many a named local take passes, or <see langword="null"/> for no take.</param>
    /// <param name="capacity">The named local buffer's declared capacity.</param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// The payloads are written by the very writers the authoring surface uses, so what travels here is
    /// byte-identical to what a fluent <c>Buffer(...)</c> writes. A test that spelled the JSON itself would
    /// keep passing if the two spellings drifted, which is the one thing it must not do.
    /// </remarks>
    private static PipelineDefinition Plumbed(string id, int count, int? take, int capacity)
    {
        List<StageNode> nodes =
        [
            Source(count),
            Node(
                "queueing",
                Local("buffer"),
                "local-buffer-parameters",
                LocalBufferParameters.Write(new BufferOptions { Capacity = capacity })),
            Sink(),
        ];
        List<GraphEdge> edges = [];

        if (take is { } passed)
        {
            nodes.Add(Node("taken", Local("take"), "local-count-parameters", LocalCountParameters.Write(passed)));
            edges.Add(Edge("numbers", "out", "taken", "in"));
            edges.Add(Edge("taken", "out", "queueing", "in"));
        }
        else
        {
            edges.Add(Edge("numbers", "out", "queueing", "in"));
        }

        edges.Add(Edge("queueing", "out", "out", "in"));

        return Definition(id, nodes, edges);
    }

    /// <summary>Builds a pipeline whose plumbing is one delay rather than a buffer.</summary>
    /// <param name="id">The pipeline identity.</param>
    /// <param name="count">How many numbers the source emits.</param>
    /// <returns>The pipeline.</returns>
    private static PipelineDefinition Delaying(string id, int count) =>
        Definition(
            id,
            [
                Source(count),
                Node(
                    "waiting",
                    Local("delay"),
                    "local-delay-parameters",
                    LocalDelayParameters.Write(Held, new BufferOptions { Capacity = 1 })),
                Sink(),
            ],
            [
                Edge("numbers", "out", "waiting", "in"),
                Edge("waiting", "out", "out", "in"),
            ]);

    /// <summary>Builds a pipeline whose middle node is one local stage, whatever it is.</summary>
    /// <param name="id">The pipeline identity.</param>
    /// <param name="stage">The local stage identifier text.</param>
    /// <param name="parameterContract">The parameter contract identifier text that stage declares.</param>
    /// <param name="parameters">The payload, defaulting to the empty object.</param>
    /// <returns>The pipeline.</returns>
    private static PipelineDefinition Middle(
        string id,
        string stage,
        string parameterContract,
        CanonicalJsonValue? parameters = null) =>
        Definition(
            id,
            [
                Source(4),
                Node("middle", Local(stage), parameterContract, parameters ?? TestVocabulary.Empty),
                Sink(),
            ],
            [
                Edge("numbers", "out", "middle", "in"),
                Edge("middle", "out", "out", "in"),
            ]);

    /// <summary>Builds the registered source node every document here begins at.</summary>
    /// <param name="count">How many numbers it emits, counting up from one.</param>
    /// <returns>The node.</returns>
    private static StageNode Source(int count) =>
        StageNode.Create(
            NodeId.Create("numbers"),
            TestVocabulary.OpaqueRange,
            TestVocabulary.RangeParameters,
            TestRangeParameters.Write(count));

    /// <summary>Builds the registered sink node every document here ends at.</summary>
    /// <returns>The node.</returns>
    private static StageNode Sink() =>
        StageNode.Create(
            NodeId.Create("out"),
            TestVocabulary.OpaqueSum,
            TestVocabulary.NoParameters,
            TestVocabulary.Empty);

    /// <summary>Builds the deployable definition of one hand-written document.</summary>
    /// <param name="id">The pipeline identity.</param>
    /// <param name="nodes">The nodes.</param>
    /// <param name="edges">The edges.</param>
    /// <returns>The pipeline.</returns>
    /// <remarks>
    /// Constructed directly rather than through <see cref="RunnableGraph.AsPipeline"/>, because there is no
    /// runnable graph here to re-identify: the document was written against the definition plane. What that
    /// call would have checked — that neither deployability token is declared — is asserted over the very
    /// document this builds, in a test of its own.
    /// </remarks>
    private static PipelineDefinition Definition(
        string id,
        IEnumerable<StageNode> nodes,
        IEnumerable<GraphEdge> edges)
    {
        GraphDocument document = GraphDocument.Create(
            GraphId.Create(id),
            GraphRevision.Create(1),
            [],
            nodes,
            edges,
            [
                ResultSlotDefinition.Create(
                    ResultSlotId.Create(TotalSlot),
                    TestVocabulary.Total.Reference,
                    PortAddress.Create(NodeId.Create("out"), PortId.Create("total"))),
            ]);

        return new PipelineDefinition(document, GraphDocumentSerializer.Fingerprint(document));
    }

    /// <summary>Builds the node of one local occurrence under the name its author gave it.</summary>
    /// <param name="id">The node identifier text.</param>
    /// <param name="stage">The local stage reference.</param>
    /// <param name="parameterContract">The parameter contract identifier text.</param>
    /// <param name="parameters">The payload.</param>
    /// <returns>The node.</returns>
    private static StageNode Node(
        string id,
        StageRef stage,
        string parameterContract,
        CanonicalJsonValue parameters) =>
        StageNode.Create(
            NodeId.Create(id),
            stage,
            ContractReference.Create(ContractId.Create(parameterContract), 1),
            parameters);

    /// <summary>Builds a local stage reference at major version 1.</summary>
    /// <param name="stage">The stage identifier text, such as <c>buffer</c>.</param>
    /// <returns>The reference.</returns>
    private static StageRef Local(string stage) =>
        StageRef.Create(ProviderId.Create("local"), StageId.Create(stage), 1);

    /// <summary>Builds one edge from two port addresses written as text.</summary>
    /// <param name="fromNode">The producing node.</param>
    /// <param name="fromPort">The producing port.</param>
    /// <param name="toNode">The consuming node.</param>
    /// <param name="toPort">The consuming port.</param>
    /// <returns>The edge.</returns>
    private static GraphEdge Edge(string fromNode, string fromPort, string toNode, string toPort) =>
        GraphEdge.Create(
            PortAddress.Create(NodeId.Create(fromNode), PortId.Create(fromPort)),
            PortAddress.Create(NodeId.Create(toNode), PortId.Create(toPort)));

    /// <summary>Recovers the typed slot one of these pipelines resolves its total under.</summary>
    /// <param name="pipeline">The pipeline.</param>
    /// <returns>The slot.</returns>
    private static ResultSlot<long> Slot(PipelineDefinition pipeline) =>
        pipeline.ResultSlot(TotalSlot, TestVocabulary.Total);
}
