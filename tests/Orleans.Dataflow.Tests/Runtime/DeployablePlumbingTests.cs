using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Hosting;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Tests.Api;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;
using static Orleans.Dataflow.Tests.Runtime.PlumbingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// ADR 0009 on the deployable path: which local stages a document states completely, what a host publishes
/// for them, what it refuses by name, and what a run of one actually does.
/// </summary>
/// <remarks>
/// <para>
/// The claim is that a document holding local plumbing runs on the deployable path and produces the same
/// answer the local path produces, because the two are one planner reading one payload. These tests exercise
/// <see cref="PipelineMaterializer"/>, which is that path with the cluster taken out of it; the cluster tests
/// carry the same document over a silo.
/// </para>
/// <para>
/// Nothing here rehydrates a payload twice or reads one in a way an authored graph would not. That is the
/// whole architectural argument for rehydration over a second implementation, so what these tests assert is
/// the observable half of it: the same numbers, the same fusion, the same boundary.
/// </para>
/// </remarks>
public sealed class DeployablePlumbingTests
{
    /// <summary>Gets the token that cancels a hung test rather than letting a run block the suite.</summary>
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public void TheCatalogAHostPublishesIsExactlyTheStagesADocumentStatesCompletely()
    {
        // The published half of the vocabulary, against the list written by hand in ApiFixtures. A host
        // publishing the whole local catalog would be promising to run a 'local/select@v1' it cannot build;
        // publishing less than this would refuse documents it could execute perfectly well.
        Assert.Equal(
            DeployableLocalStages,
            LocalPlumbing.Catalog.Specifications.Select(specification => specification.Stage.Stage.Value));

        Assert.All(
            LocalPlumbing.Catalog.Specifications,
            specification =>
            {
                Assert.Equal("local", specification.Stage.Provider.Value);
                Assert.Empty(specification.RequiredCapabilities);
            });

        // And each published specification is the very one the full catalog declares, so a document
        // validated against a silo's catalog and the same document validated against the authoring one
        // cannot disagree about ports, contracts, or checks.
        foreach (StageSpecification published in LocalPlumbing.Catalog.Specifications)
        {
            Assert.True(
                LocalStageCatalog.Instance.TryGetSpecification(
                    published.Stage,
                    out StageSpecification? authored));
            Assert.Equal(
                StageCatalogSerializer.Serialize(StageCatalog.Create([published])),
                StageCatalogSerializer.Serialize(StageCatalog.Create([authored!])));
        }
    }

    [Theory]
    [InlineData("buffer", true)]
    [InlineData("take", true)]
    [InlineData("merge", true)]
    [InlineData("count", true)]
    [InlineData("select", false)]
    [InlineData("valve", false)]
    [InlineData("first-or-default", false)]
    [InlineData("group-by", false)]
    public void OnlyThePlumbingRehydratesFromAStageReference(string stage, bool rehydrates) =>
        Assert.Equal(rehydrates, LocalPlumbing.Rehydrates(LocalStage(stage)));

    [Fact]
    public void ARegisteredStageNeverRehydratesHoweverItIsSpelled()
    {
        // The provider is what the question is about. A registered stage named 'buffer' is somebody else's
        // buffer and is built by their factory, so answering yes for it would leave a node with no behavior
        // at all.
        Assert.False(LocalPlumbing.Rehydrates(Stage("buffer")));
        Assert.False(LocalPlumbing.Rehydrates(Numbers));

        // And a local reference this build declares no stage for is not plumbing either, however local it
        // looks: the vocabulary is a closed set and membership is decided by reading it.
        Assert.False(LocalPlumbing.Rehydrates(Local("no-such-stage")));
        Assert.False(LocalPlumbing.Rehydrates(
            StageRef.Create(ProviderId.Create("local"), StageId.Create("buffer"), 2)));
    }

    [Fact]
    public void APlumbedDocumentIsAcceptedAndNamesNoCapability()
    {
        GraphDocument document = Summing(count: 4, capacity: 8);

        Assert.Null(LocalPlumbing.Refusal(document));
        Assert.Empty(document.Capabilities);
        Assert.True(GraphCompiler.Validate(document, Catalog).IsValid);
    }

    [Theory]
    [InlineData("select", "behavior is a delegate")]
    [InlineData("group-by", "behavior is a delegate")]
    [InlineData("first-or-default", "default of an element type")]
    [InlineData("last-or-default", "default of an element type")]
    [InlineData("valve", "produces a runtime control")]
    public void ALocalStageThatCannotDeployIsRefusedByNameAndBySentence(string stage, string reason)
    {
        // Refused by name, with the reason that fits that stage, before anything is started. A silo that
        // accepted such a document would fail at materialization instead — a run that dies where a
        // reconcilable mistake should have been reported.
        GraphDocument document = Document(
            "refused",
            [
                Registered("numbers", Numbers),
                Plumbing("middle", stage, "local-parameters", Empty),
                Registered("out", Sum),
            ],
            [Edge("numbers", "out", "middle", "in"), Edge("middle", "out", "out", "in")]);

        string refusal = Assert.IsType<string>(LocalPlumbing.Refusal(document));

        Assert.Contains("'middle'", refusal, StringComparison.Ordinal);
        Assert.Contains($"local/{stage}@v1", refusal, StringComparison.Ordinal);
        Assert.Contains(reason, refusal, StringComparison.Ordinal);

        // And the materializer refuses it too, rather than leaving the sentence to a caller who might not
        // have asked. The catalog is beside the point: this one publishes the stage in question.
        InvalidOperationException refused = Assert.Throws<InvalidOperationException>(
            () => PipelineMaterializer.Start(
                document,
                GraphDocumentSerializer.Fingerprint(document),
                new CompositeStageCatalog(RegisteredCatalog, LocalStageCatalog.Instance),
                Factories(),
                "refused",
                Token));

        Assert.Contains($"local/{stage}@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ALocalReferenceThisBuildDoesNotDeclareIsRefusedAsSuch()
    {
        GraphDocument document = Document(
            "unknown",
            [
                Registered("numbers", Numbers),
                Plumbing("middle", "sluice", "local-parameters", Empty),
                Registered("out", Sum),
            ],
            [Edge("numbers", "out", "middle", "in"), Edge("middle", "out", "out", "in")]);

        string refusal = Assert.IsType<string>(LocalPlumbing.Refusal(document));

        Assert.Contains("'middle'", refusal, StringComparison.Ordinal);
        Assert.Contains("declares no local stage for", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void RehydrationCarriesTheNodesOwnPayloadAndNothingElse()
    {
        GraphDocument document = Summing(count: 9, capacity: 3, take: 4);
        IReadOnlyDictionary<NodeId, LocalStageDescriptor> bindings = LocalPlumbing.Bindings(document);

        // One entry per local node and not one more: a registered node is the binder's business, and an
        // entry for one would make the planner stop asking the factory that owns it.
        Assert.Equal(
            ["queueing", "taken"],
            bindings.Keys.Select(node => node.Value).Order(StringComparer.Ordinal));

        foreach (StageNode node in document.Nodes.Where(node => node.Stage.Provider.Value == "local"))
        {
            LocalStageDescriptor descriptor = bindings[node.Id];

            Assert.Equal(node.Stage, descriptor.Stage);
            Assert.Equal(node.Parameters, descriptor.Parameters);
            Assert.Equal(node.ParameterContract, descriptor.ParameterContract);
            Assert.Null(descriptor.Behavior);
            Assert.Null(descriptor.Seed);
        }
    }

    [Fact]
    public void ACountingSinkRehydratesWithTheZeroTheVocabularyFixedRatherThanWithNothing()
    {
        // The one shape whose seed is not null, and the reason SeedOf exists. A rehydrated counting sink
        // that started from null would add one to a null reference on its first element.
        LocalStageDescriptor counting = LocalStageDescriptor.Rehydrated(LocalStageKind.Count, Empty);

        Assert.Equal(0L, counting.Seed);
        Assert.Null(counting.Behavior);
    }

    [Fact]
    public void AShapeADocumentDoesNotStateCompletelyRefusesToBeRehydrated()
    {
        // Written as a loop rather than a theory because the shape is an internal enumeration and a public
        // theory parameter cannot name one. The four are the whole of what a document cannot restate: a
        // delegate, a control, and the two defaults of an element type nobody named.
        foreach (LocalStageKind kind in (LocalStageKind[])
        [
            LocalStageKind.Select,
            LocalStageKind.Valve,
            LocalStageKind.FirstOrDefault,
            LocalStageKind.LastOrDefault,
        ])
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                () => LocalStageDescriptor.Rehydrated(kind, Empty));
        }
    }

    [Fact]
    public async Task APlumbedDocumentRunsOnTheDeployablePathAndProducesTheRightAnswer()
    {
        // The claim, end to end and without a cluster: a registered source, a named local take, a named
        // local buffer with a real capacity, and a registered sink. Nine numbers are emitted, four pass the
        // take, and the sum of one through four is ten — which is the assertion that the take's payload was
        // read from the document, because a take that quietly passed everything would sum to forty-five.
        GraphDocument document = Summing(count: 9, capacity: 4, take: 4);

        Assert.Equal(10L, await RunAsync(document));
    }

    [Fact]
    public async Task TheLocalPathAndTheDeployablePathAgreeOnTheSameDocument()
    {
        // The most important assertion in this file. The two paths differ in where the binding table comes
        // from — an authoring surface on one side, this document on the other — and in nothing else, so a
        // disagreement here would mean the deployable path is a second implementation after all.
        GraphDocument document = Summing(count: 9, capacity: 4, take: 4);
        GraphFingerprint fingerprint = GraphDocumentSerializer.Fingerprint(document);

        long deployable = await RunAsync(document);

        // The local host takes a built graph rather than a document, so the document is handed to it with
        // exactly the bindings the deployable path builds: same planner, same payload, same descriptors.
        RunnableGraph graph = new(document, fingerprint, LocalPlumbing.Bindings(document));
        LocalDataflowHost host = new(builder => builder
            .AddCatalog(RegisteredCatalog)
            .AddFactory(ProviderId.Create(Provider), new PlumbingStageFactory(9, null)));

        await using RunHandle handle = await host.MaterializeAsync(graph, Token);

        await handle.Completion;

        Assert.Equal(
            deployable,
            await handle.GetValueAsync(
                ResultSlot<long>.Create(ResultSlotId.Create("total"), fingerprint, graph.AuthoringNonce),
                Token));
    }

    [Fact]
    public void ADeclaredCapacityBecomesTheChannelTheRunActuallyHolds()
    {
        // A buffer that silently became unbounded would pass every structural check this repository has, so
        // the number is followed all the way into the plan: the boundary a plumbed document compiles to is
        // the capacity and the policy the node declared and not a handoff.
        foreach ((int capacity, OverflowPolicy policy) in ((int, OverflowPolicy)[])
        [
            (1, OverflowPolicy.Fail),
            (4, OverflowPolicy.Backpressure),
            (64, OverflowPolicy.DropOldest),
        ])
        {
            GraphDocument document = Summing(
                count: 4,
                capacity,
                take: null,
                policy);
            LocalRunPlan plan = LocalRunPlanner.Compile(
                document,
                LocalPlumbing.Bindings(document),
                new StageRuntimeBinder(Catalog, Factories()),
                "capacity",
                TimeProvider.System);

            LocalBoundary boundary = Assert.Single(plan.Boundaries);

            Assert.Equal(capacity, boundary.Capacity);
            Assert.Equal(policy, boundary.Policy);
        }
    }

    [Fact]
    public async Task ADeclaredCapacityIsEnforcedByARunAndNotOnlyRecordedByAPlan()
    {
        // The behavioural half of the same claim, and it is deterministic rather than timed. The holding
        // flow takes one element and never gives it back, so the channel in front of it fills to exactly its
        // declared capacity of one and the third offer meets a full channel — which under the declared
        // policy fails the run by name. A buffer that ignored its capacity would never fill and this run
        // would hang instead, which the test's own cancellation token turns into a failure rather than a
        // pass.
        TaskCompletionSource release = new();

        try
        {
            GraphDocument document = Held(capacity: 1, OverflowPolicy.Fail);

            BufferOverflowException overflowed = await Assert.ThrowsAsync<BufferOverflowException>(
                () => RunAsync(document, release));

            Assert.Contains("capacity", overflowed.Message, StringComparison.Ordinal);
        }
        finally
        {
            release.SetResult();
        }
    }

    [Fact]
    public async Task TheSameGraphUnderARoomierCapacityCompletes()
    {
        // The control for the assertion above, so that "it failed" is a statement about the capacity rather
        // than about the shape: the same document, the same holding flow, the same nine elements, and a
        // capacity of nine — which the whole stream fits inside whatever the consumer is doing, so this run
        // cannot overflow however the two segments happen to interleave. One through nine sums to
        // forty-five, so nothing was dropped on the way either.
        //
        // Nine and not eight, measured rather than guessed: at capacity eight this same run does overflow,
        // because a released holding flow still takes one element at a time and the source is entitled to
        // fill the channel ahead of it. That is the capacity being honoured too, and it is why the pair
        // below the bound and at it is the honest way to state the claim.
        TaskCompletionSource release = new();

        release.SetResult();

        Assert.Equal(45L, await RunAsync(Held(capacity: 9, OverflowPolicy.Fail), release));
    }

    [Fact]
    public void ACycleOnTheDeployablePathIsRelievedByTheVeryPolicyItsNodeDeclares()
    {
        // The rule ADR 0005 states and the planner reads out of a buffer's payload: a loop is legal exactly
        // when it passes a boundary that can answer without room below it. Reading it needs the buffer's
        // binding, which on this path only exists because the node was rehydrated — before this checkpoint
        // the deployable path had an empty binding table, so no cycle could ever be relieved there and every
        // one of them was refused as a deadlock.
        //
        // The pair is what makes it a measurement: the same graph under a dropping policy compiles and under
        // backpressure is refused by name. Compiled rather than run, deliberately — a relieved loop of live
        // elements has no ending to assert, and what is under test is the decision rather than the traffic.
        LocalRunPlan relieved = LocalRunPlanner.Compile(
            Looping(OverflowPolicy.DropOldest),
            LocalPlumbing.Bindings(Looping(OverflowPolicy.DropOldest)),
            new StageRuntimeBinder(Catalog, Factories()),
            "cycle",
            TimeProvider.System);

        Assert.Contains(relieved.Boundaries, boundary => boundary.Policy is OverflowPolicy.DropOldest);

        GraphDocument deadlocked = Looping(OverflowPolicy.Backpressure);

        Assert.Contains(
            "passes no boundary that can answer without room below it",
            Assert.Throws<InvalidOperationException>(() => LocalRunPlanner.Compile(
                deadlocked,
                LocalPlumbing.Bindings(deadlocked),
                new StageRuntimeBinder(Catalog, Factories()),
                "cycle",
                TimeProvider.System)).Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void PlumbingBetweenTwoStagesThatTypeTheirElementsIsStillRefusedByTheElementRule()
    {
        // ADR 0009's headline example, measured. Every local port declares 'local-opaque@v1' because a local
        // graph's element types live in the C# type system; a registered port declares whatever its provider
        // registered; and the graph compiler's element rule compares the two for equality. So a buffer
        // between two stages carrying a real contract produces one diagnostic per edge across the seam, and
        // the document validates nowhere — against this host's catalog, against the local one, or against
        // any composite of them.
        //
        // This is a gap in ADR 0009 rather than in this implementation: nothing the ADR changes touches the
        // element rule, and the rule is what refuses the shape the ADR exists to enable. It is pinned here
        // so that the day the plane learns to carry an element contract through a transparent stage, this
        // test fails and says so.
        GraphDocument document = Summing(count: 4, capacity: 8);
        GraphValidationReport report = GraphCompiler.Validate(document, TypedCatalog);

        Assert.False(report.IsValid);
        Assert.All(
            report.Diagnostics,
            diagnostic => Assert.Equal("element-contract-mismatch", diagnostic.Rule));
        Assert.Equal(2, report.Diagnostics.Count);
        Assert.All(
            report.Diagnostics,
            diagnostic => Assert.Contains("local-opaque@v1", diagnostic.Message, StringComparison.Ordinal));

        // And the plumbing itself is not what is wrong with it: nothing about the document is refused by the
        // rules ADR 0009 does change.
        Assert.Null(LocalPlumbing.Refusal(document));
        Assert.Empty(document.Capabilities);
    }

    /// <summary>Builds the summing document: a source, optional plumbing, a buffer, and a sink.</summary>
    /// <param name="count">How many numbers the source emits, counting up from one.</param>
    /// <param name="capacity">The buffer's declared capacity.</param>
    /// <param name="take">How many elements a named take passes, or <see langword="null"/> for no take.</param>
    /// <param name="policy">The buffer's declared overflow policy.</param>
    /// <returns>The document.</returns>
    private static GraphDocument Summing(
        int count,
        int capacity,
        int? take = null,
        OverflowPolicy policy = OverflowPolicy.Backpressure)
    {
        BufferOptions options = new() { Capacity = capacity, OverflowPolicy = policy };

        return take is { } passed
            ? Document(
                "summing",
                [
                    Registered("numbers", Numbers),
                    Take("taken", passed),
                    Buffer("queueing", options),
                    Registered("out", Sum),
                ],
                [
                    Edge("numbers", "out", "taken", "in"),
                    Edge("taken", "out", "queueing", "in"),
                    Edge("queueing", "out", "out", "in"),
                ],
                [Total("out")])
            : Document(
                "summing",
                [
                    Registered("numbers", Numbers),
                    Buffer("queueing", options),
                    Registered("out", Sum),
                ],
                [
                    Edge("numbers", "out", "queueing", "in"),
                    Edge("queueing", "out", "out", "in"),
                ],
                [Total("out")]);
    }

    /// <summary>Builds a document whose stream loops back through a merge, entirely out of plumbing.</summary>
    /// <param name="policy">The overflow policy of the buffer that closes the loop.</param>
    /// <returns>The document.</returns>
    /// <remarks>
    /// Every node of the cycle is local plumbing — a merge, a broadcast, and a buffer — which is what makes
    /// this a statement about rehydration rather than about a provider: none of the three could be built on
    /// this path at all before ADR 0009, and the decision about the loop is read out of the buffer's own
    /// payload.
    /// </remarks>
    private static GraphDocument Looping(OverflowPolicy policy) =>
        Document(
            "looping",
            [
                Registered("numbers", Numbers),
                Plumbing("joined", "merge", "local-parameters", Empty),
                Plumbing("split", "broadcast", "local-parameters", Empty),
                Buffer("round", new BufferOptions { Capacity = 4, OverflowPolicy = policy }),
                Registered("out", Sum),
            ],
            [
                Edge("numbers", "out", "joined", "in-0"),
                Edge("joined", "out", "split", "in"),
                Edge("split", "out-0", "out", "in"),
                Edge("split", "out-1", "round", "in"),
                Edge("round", "out", "joined", "in-1"),
            ],
            [Total("out")]);

    /// <summary>Builds a document whose buffer feeds a flow that holds its first element.</summary>
    /// <param name="capacity">The buffer's declared capacity.</param>
    /// <param name="policy">The buffer's declared overflow policy.</param>
    /// <returns>The document.</returns>
    private static GraphDocument Held(int capacity, OverflowPolicy policy) =>
        Document(
            "held",
            [
                Registered("numbers", Numbers),
                Buffer("queueing", new BufferOptions { Capacity = capacity, OverflowPolicy = policy }),
                Registered("holding", Hold),
                Registered("out", Sum),
            ],
            [
                Edge("numbers", "out", "queueing", "in"),
                Edge("queueing", "out", "holding", "in"),
                Edge("holding", "out", "out", "in"),
            ],
            [Total("out")]);

    /// <summary>Materializes a document through the deployable path and reads its total.</summary>
    /// <param name="document">The document.</param>
    /// <param name="release">What the holding flow waits on, when the document declares one.</param>
    /// <returns>The total the run resolved.</returns>
    private static async Task<long> RunAsync(GraphDocument document, TaskCompletionSource? release = null)
    {
        GraphFingerprint fingerprint = GraphDocumentSerializer.Fingerprint(document);
        LocalRun run = PipelineMaterializer.Start(
            document,
            fingerprint,
            Catalog,
            Factories(release),
            "plumbing-test",
            Token);

        await using RunHandle handle = new(run);

        await handle.Completion;

        return await handle.GetValueAsync(
            ResultSlot<long>.Create(
                ResultSlotId.Create("total"),
                fingerprint,
                PipelineMaterializer.PipelineNonce),
            Token);
    }

    /// <summary>Builds the runtime factories a host of this fixture registers.</summary>
    /// <param name="release">What the holding flow waits on, when a document declares one.</param>
    /// <returns>The registry.</returns>
    /// <remarks>
    /// One provider and no <c>local</c> entry, which is the point: the plumbing needs no factory, so a host
    /// that registers none can still run a document holding it.
    /// </remarks>
    private static StageRuntimeRegistry Factories(TaskCompletionSource? release = null) =>
        new(
        [
            new KeyValuePair<ProviderId, IStageRuntimeFactory>(
                ProviderId.Create(Provider),
                new DataflowStageFactoryAdapter(new PlumbingStageFactory(9, release))),
        ]);
}
