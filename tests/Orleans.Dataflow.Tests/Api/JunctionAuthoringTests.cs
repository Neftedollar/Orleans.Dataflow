using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What the junction surface emits: the nodes, the edges, the ports, and the slots.
/// </summary>
/// <remarks>
/// <para>
/// The nine programs of <see cref="JunctionPrograms"/> are the subject. That they compile is the ADR's
/// inference claim and is asserted by the build; what is asserted here is the document each one closes,
/// spelled out node by node and edge by edge, because a junction's arity, its leg order, and which leg a
/// branch took are all edges and nothing else.
/// </para>
/// <para>
/// The port names are written out rather than read from <c>LocalVocabulary</c>. A test that echoed the
/// production constants back at itself would pass whatever they said; <c>out-0</c>, <c>in-1</c>,
/// <c>left</c>, and <c>right</c> here are the statement that the fluent surface wires the ports the engine's
/// own fixtures wire.
/// </para>
/// </remarks>
public sealed class JunctionAuthoringTests
{
    [Fact]
    public void ABroadcastWiresOneLegPerBranchInArgumentOrder()
    {
        (RunnableGraph graph, _, _) = JunctionPrograms.BroadcastTwoSinks();

        Assert.Equal(["stage-0001", "stage-0002", "stage-0003", "stage-0004"], NodeIds(graph.Document));
        Assert.Equal(["from-enumerable", "broadcast", "count", "fold"], StageIds(graph.Document));
        Assert.Equal(
            [
                "stage-0001#out -> stage-0002#in",
                "stage-0002#out-0 -> stage-0003#in",
                "stage-0002#out-1 -> stage-0004#in",
            ],
            Edges(graph.Document));
    }

    [Fact]
    public void EveryResultBearingBranchDeclaresASlotOnItsOwnTerminal()
    {
        // "Named multiple results", concretely: two names, two producers, one document. The producer of each
        // is the branch's own terminal, which is what makes the names mean something an author chose rather
        // than a position the builder happened to allocate.
        (RunnableGraph graph, ResultSlot<long> counted, ResultSlot<decimal> totaled) =
            JunctionPrograms.BroadcastTwoSinks();

        Assert.Equal(["counted", "totaled"], Slots(graph.Document));
        Assert.Equal(
            ["counted -> stage-0003#result", "totaled -> stage-0004#result"],
            Producers(graph.Document));
        Assert.Equal(graph.Fingerprint, counted.Graph);
        Assert.Equal(graph.Fingerprint, totaled.Graph);
    }

    [Fact]
    public void ATapKeepsTheMainLineOnTheFirstLegAndTheBranchOnTheSecond()
    {
        (RunnableGraph graph, _) = JunctionPrograms.TapForAudit();

        Assert.Equal(
            ["from-enumerable", "broadcast", "select", "ignore", "where", "count"],
            StageIds(graph.Document));
        Assert.Equal(
            [
                "stage-0001#out -> stage-0002#in",
                "stage-0002#out-0 -> stage-0005#in",
                "stage-0002#out-1 -> stage-0003#in",
                "stage-0003#out -> stage-0004#in",
                "stage-0005#out -> stage-0006#in",
            ],
            Edges(graph.Document));
    }

    [Fact]
    public void AFanInWiresTheReceiverToTheFirstInputAndTheArgumentsAfterIt()
    {
        (RunnableGraph graph, _) = JunctionPrograms.MergeAndConcat();

        Assert.Equal(
            ["from-enumerable", "from-enumerable", "merge", "from-enumerable", "concat", "count"],
            StageIds(graph.Document));
        Assert.Equal(
            [
                "stage-0001#out -> stage-0003#in-0",
                "stage-0002#out -> stage-0003#in-1",
                "stage-0003#out -> stage-0005#in-0",
                "stage-0004#out -> stage-0005#in-1",
                "stage-0005#out -> stage-0006#in",
            ],
            Edges(graph.Document));
    }

    [Fact]
    public void ChainedMergesAreTwoJunctionsAndSaySo()
    {
        // ADR 0006 refuses to flatten a chained merge into one junction, and this is that refusal as a
        // document: three sources joined two at a time are two nodes, and joining them as peers is one.
        // Both are legal and they are different graphs, which is the honest encoding of an associative
        // operation whose documents are not interchangeable.
        Source<int> first = Source.From<int>([1]);
        Source<int> second = Source.From<int>([2]);
        Source<int> third = Source.From<int>([3]);

        RunnableGraph chained = first.Merge(second).Merge(third).To(Sink.Ignore<int>());
        RunnableGraph peers = first.Merge(second, third).To(Sink.Ignore<int>());

        Assert.Equal(2, chained.Document.Nodes.Count(node => node.Stage.Stage.Value == "merge"));
        Assert.Equal(1, peers.Document.Nodes.Count(node => node.Stage.Stage.Value == "merge"));
        Assert.NotEqual(chained.Fingerprint, peers.Fingerprint);
    }

    [Fact]
    public void ADiamondReConvergesOnOneJoiningJunction()
    {
        // The shape a tree cannot express: two edges leave one broadcast and two edges arrive at one zip,
        // and the second of those arrivals is the edge that closes the diamond.
        (RunnableGraph graph, _) = JunctionPrograms.DiamondForkZip();

        Assert.Equal(
            ["from-enumerable", "broadcast", "select", "select", "zip", "count"],
            StageIds(graph.Document));
        Assert.Equal(
            [
                "stage-0001#out -> stage-0002#in",
                "stage-0002#out-0 -> stage-0003#in",
                "stage-0002#out-1 -> stage-0004#in",
                "stage-0003#out -> stage-0005#in-0",
                "stage-0004#out -> stage-0005#in-1",
                "stage-0005#out -> stage-0006#in",
            ],
            Edges(graph.Document));
    }

    [Fact]
    public void AForkThroughIdentityFlowsCostsNoStageAtAll()
    {
        // Flow.For<T>() contributes no occurrence, so a leg that does nothing is the junction's own leg port
        // wired straight to the join. That is the honest encoding of doing nothing to every element, and it
        // is what makes the identity flow usable as a branch anchor rather than as a stage.
        RunnableGraph graph = Source.From<int>([1, 2])
            .Fork(Flow.For<int>(), Flow.For<int>().Select(value => value * 2))
            .Zip((left, right) => left + right)
            .To(Sink.Ignore<int>());

        Assert.Equal(["from-enumerable", "broadcast", "select", "zip", "ignore"], StageIds(graph.Document));
        Assert.Equal(
            [
                "stage-0001#out -> stage-0002#in",
                "stage-0002#out-0 -> stage-0004#in-0",
                "stage-0002#out-1 -> stage-0003#in",
                "stage-0003#out -> stage-0004#in-1",
                "stage-0004#out -> stage-0005#in",
            ],
            Edges(graph.Document));
    }

    [Fact]
    public void AnUnzipWiresTheHalvesToTheirNamedPorts()
    {
        (RunnableGraph graph, _, _) = JunctionPrograms.UnzipPairs();

        Assert.Equal(["from-enumerable", "unzip", "count", "count"], StageIds(graph.Document));
        Assert.Equal(
            [
                "stage-0001#out -> stage-0002#in",
                "stage-0002#left -> stage-0003#in",
                "stage-0002#right -> stage-0004#in",
            ],
            Edges(graph.Document));
    }

    [Fact]
    public void AnInterleaveWritesItsSegmentSizeIntoTheDocument()
    {
        // The one junction with a payload, and the reason it has one: how many inputs the rotation runs over
        // is stated by the edges like every junction's arity, but how many elements it takes from each is a
        // number that changes the sequence and therefore belongs in the fingerprint.
        RunnableGraph graph = Source.From<int>([1, 2])
            .Interleave(Source.From<int>([3, 4]), 2)
            .To(Sink.Ignore<int>());

        StageNode interleave = graph.Document.Nodes.Single(node => node.Stage.Stage.Value == "interleave");

        Assert.Equal(Contract("local-interleave-parameters"), interleave.ParameterContract);
        Assert.Equal("""{"segmentSize":2}""", interleave.Parameters.ToString());
    }

    [Fact]
    public void ABranchCanEndInARegisteredSinkWithItsOwnOccurrenceName()
    {
        // The registered branch forms are overloads and not a second shape, exactly as ADR 0006 said they
        // would be: the same To family, the same branch value, the same junction call. The occurrence names
        // are the author's and the automatic number falls to the one occurrence nobody could name — the
        // junction, which is a local stage.
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> counted);

        Assert.Equal(
            ["count-out", "index-out", "normalize", "orders-in", "stage-0003"],
            NodeIds(graph.Document));
        Assert.Equal(["count-sink", "index-sink", "normalize", "order-source", "broadcast"], StageIds(graph.Document));
        Assert.Equal(["counted"], Slots(graph.Document));
        Assert.Equal(graph.Fingerprint, counted.Graph);
    }

    [Fact]
    public void AFanOutOfRegisteredStagesStillFailsValidationAtEveryLocalSeam()
    {
        // The honest consequence, stated as a test rather than left to be discovered. A junction is a local
        // stage, and every local port declares the opaque contract 'local-opaque@v1' because a local graph
        // is typed by C# generics rather than by registered contracts. Wiring a registered stage to one is
        // therefore a seam, and a seam is an element-contract-mismatch — the same rule a mixed chain has
        // broken since ADR 0004, at the same place, for the same reason.
        //
        // The consequence in one sentence: a graph whose junction is local is a local graph whatever its
        // branches are made of. Since M4.5 that is a statement about local junctions rather than about
        // branching graphs — a provider can register a junction of its own, and RegisteredJunctionTests
        // asserts that the same shape built out of one validates with no seam at all. This test is what
        // keeps the mixing rule from being quietly weakened on the way there.
        RunnableGraph graph = RegisteredFanOut(out ResultSlot<long> _);

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, RegisteredFixtures.MixedCatalog);

        Assert.False(report.IsValid);
        Assert.All(report.Diagnostics, diagnostic => Assert.Equal("element-contract-mismatch", diagnostic.Rule));
        Assert.All(
            report.Diagnostics,
            diagnostic => Assert.Contains("local-opaque@v1", diagnostic.Message, StringComparison.Ordinal));

        // One per edge that crosses between the two vocabularies: into the junction, and out of each leg.
        Assert.Equal(3, report.Diagnostics.Count);
    }

    [Fact]
    public void AFanOutOfRegisteredStagesDeclaresBothTokensTheJunctionBringsWithIt()
    {
        // The same stages in a chain declare nothing: a registered occurrence carries its identity and its
        // parameters in the document, and a named one is edit-stable, so a fully registered chain is a
        // pipeline candidate. Adding a junction between them adds one local, unnamed occurrence, and the two
        // tokens that occurrence brings are the whole difference.
        //
        // 'nondeployable' because every local stage requires it, and 'ephemeral-identity' because this
        // surface has no spelling for naming a local junction occurrence — there is nothing durable for the
        // name to identify. Its sibling is
        // RegisteredJunctionTests.AFullyRegisteredFanOutDeclaresNeitherToken, which asserts the absence of
        // both on the same shape once the junction itself is a registered stage.
        RunnableGraph chain = Source.FromRegistered(
                RegisteredFixtures.OrderSource,
                "orders-in",
                RegisteredFixtures.SourceParameters)
            .Via(RegisteredFixtures.Normalize, "normalize", RegisteredFixtures.NormalizeParameters)
            .To(RegisteredFixtures.IndexSink, "index-out", RegisteredFixtures.IndexParameters);

        RunnableGraph fanOut = RegisteredFanOut(out ResultSlot<long> _);

        Assert.Empty(Capabilities(chain.Document));
        Assert.Equal(
            ["ephemeral-identity", "nondeployable"],
            Capabilities(fanOut.Document).Order(StringComparer.Ordinal));
        Assert.Equal(
            ["stage-0003"],
            NodeIds(fanOut.Document).Where(id => id.StartsWith("stage-", StringComparison.Ordinal)));
    }

    [Fact]
    public void EveryJunctionProgramValidatesAgainstTheLocalCatalog()
    {
        // The same claim the linear surface makes in CatalogValidationTests, for the shapes that surface
        // cannot express. The graph compiler resolves every stage, checks every contract, requires every
        // required port to be connected, and requires every capability a stage needs to be declared; a
        // junction graph that came out of this API has to survive all of it.
        foreach ((string name, RunnableGraph graph) in JunctionGraphs())
        {
            GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

            Assert.True(report.IsValid, $"{name}: {report}");
        }
    }

    [Fact]
    public void TheSameJunctionGraphsAreRejectedByACatalogThatKnowsNothing()
    {
        // Without this, "every junction graph is valid" would be a claim about a lenient compiler rather
        // than about the documents being right.
        StageCatalog empty = StageCatalog.Create([]);

        foreach ((string name, RunnableGraph graph) in JunctionGraphs())
        {
            GraphValidationReport report = GraphCompiler.Validate(graph.Document, empty);

            Assert.False(report.IsValid, name);
            Assert.All(report.Diagnostics, diagnostic => Assert.Equal("unknown-stage", diagnostic.Rule));
        }
    }

    [Fact]
    public void EverySlotAJunctionGraphDeclaresIsProducedByAResultPort()
    {
        // A junction graph lifts the one-result bound a linear graph has, and this is what replaces it: any
        // number of results, each on a result port of a distinct producer, and never two under one name —
        // which the document itself enforces and TwoBranchesUnderOneSlotNameAreRejected exercises.
        foreach ((string name, RunnableGraph graph) in JunctionGraphs())
        {
            Assert.All(
                graph.Document.ResultSlots,
                slot => Assert.True(slot.Producer.Port.Value is "result" or "control", $"{name}: {slot.Producer.Port}"));
            Assert.Equal(
                graph.Document.ResultSlots.Count,
                graph.Document.ResultSlots.Select(slot => slot.Producer).Distinct().Count());
            Assert.Equal(graph.Document.ResultSlots.Count, graph.ResultSlots.Count);
        }
    }

    [Fact]
    public void AJunctionGraphIsRefusedAsAPipeline()
    {
        // The consequence of the two tokens, at the surface an author would meet it: a graph with a junction
        // in it is not a deployable pipeline, and it says which tokens stand in the way rather than failing
        // somewhere later.
        RunnableGraph graph = JunctionPrograms.BroadcastTwoSinks().Graph;

        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => graph.AsPipeline(GraphId.Create("orders"), GraphRevision.Create(1)));

        Assert.Contains("ephemeral-identity", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("nondeployable", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TwoBranchesUnderOneSlotNameAreRejected()
    {
        // Reachable for the first time in this milestone: a linear graph is closed by one To carrying at
        // most one name, and a junction graph carries one per branch. The document's own uniqueness rule is
        // what reports it, which is the same rule that would report it for a hand-built document.
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => Source.From<int>([1, 2]).BroadcastTo(
                Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> _),
                Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> _)));

        Assert.Contains("counted", rejected.Message, StringComparison.Ordinal);
    }

    /// <summary>Builds the fan-out whose source, flow, and both sinks are registered stages.</summary>
    /// <param name="counted">When this method returns, the slot the counting branch declared.</param>
    /// <returns>The closed graph.</returns>
    /// <remarks>
    /// Built once and asserted on twice, because the two claims about it are independent: what it contains,
    /// and what a catalog says about it.
    /// </remarks>
    private static RunnableGraph RegisteredFanOut(out ResultSlot<long> counted) =>
        Source.FromRegistered(
                RegisteredFixtures.OrderSource,
                "orders-in",
                RegisteredFixtures.SourceParameters)
            .Via(RegisteredFixtures.Normalize, "normalize", RegisteredFixtures.NormalizeParameters)
            .BroadcastTo(
                Flow.For<OrderDocument>()
                    .To(RegisteredFixtures.IndexSink, "index-out", RegisteredFixtures.IndexParameters),
                Flow.For<OrderDocument>().To(
                    RegisteredFixtures.CountSink,
                    "count-out",
                    RegisteredFixtures.CountParameters,
                    "counted",
                    out counted));

    /// <summary>Reads the result slot names of a document in its canonical order.</summary>
    /// <param name="document">The document to read.</param>
    /// <returns>The slot name texts.</returns>
    private static IReadOnlyList<string> Slots(GraphDocument document) =>
        [.. document.ResultSlots.Select(slot => slot.Id.Value)];

    /// <summary>Reads the result slots of a document as name-to-producer text, in its canonical order.</summary>
    /// <param name="document">The document to read.</param>
    /// <returns>Texts of the form <c>counted -&gt; stage-0003#result</c>.</returns>
    private static IReadOnlyList<string> Producers(GraphDocument document) =>
        [.. document.ResultSlots.Select(slot => $"{slot.Id} -> {slot.Producer}")];

    /// <summary>Enumerates the junction graphs every general claim in this file is made over.</summary>
    /// <returns>One named graph per program, plus the shapes the programs do not reach.</returns>
    /// <remarks>
    /// The nine programs are the ADR's, and the three after them are the combinators no program happened to
    /// use. A junction added to the surface later has exactly one place to be added here.
    /// </remarks>
    private static IEnumerable<(string Name, RunnableGraph Graph)> JunctionGraphs()
    {
        yield return ("broadcast to two sinks", JunctionPrograms.BroadcastTwoSinks().Graph);
        yield return ("tap for audit", JunctionPrograms.TapForAudit().Graph);
        yield return ("balance workers", JunctionPrograms.BalanceWorkers());
        yield return ("partition by size", JunctionPrograms.PartitionBySize().Graph);
        yield return ("merge and concat", JunctionPrograms.MergeAndConcat().Graph);
        yield return ("zip prices and quantities", JunctionPrograms.ZipPricesAndQuantities().Graph);
        yield return ("diamond fork zip", JunctionPrograms.DiamondForkZip().Graph);
        yield return ("unzip pairs", JunctionPrograms.UnzipPairs().Graph);
        yield return ("fast path slow path", JunctionPrograms.FastPathSlowPath().Graph);

        yield return (
            "interleave",
            Source.From<int>([1, 2]).Interleave(Source.From<int>([3, 4]), 2).To(Sink.Ignore<int>()));

        yield return (
            "combine latest",
            Source.From<int>([1, 2])
                .CombineLatest(Source.From<string>(["a"]), (value, text) => $"{text}{value}")
                .To(Sink.Ignore<string>()));

        yield return (
            "zip into pairs",
            Source.From<int>([1, 2])
                .Zip(Source.From<string>(["a", "b"]))
                .To(s => s.Collect(new CollectOptions { MaxElements = 4 }), "pairs", out ResultSlot<IReadOnlyList<(int, string)>> _));

        yield return (
            "three-way merge",
            Source.From<int>([1]).Merge(Source.From<int>([2]), Source.From<int>([3])).To(Sink.Ignore<int>()));

        yield return (
            "a tap that declares a result",
            Source.From<int>([1, 2])
                .AlsoTo(Flow.For<int>().To(s => s.Count(), "tapped", out ResultSlot<long> _))
                .To(s => s.Count(), "kept", out ResultSlot<long> _));

        yield return (
            "eight legs",
            Source.From<int>([1, 2]).BroadcastTo(
                [.. Enumerable.Range(0, 8).Select(_ => Flow.For<int>().To(Sink.Ignore<int>()))]));
    }
}
