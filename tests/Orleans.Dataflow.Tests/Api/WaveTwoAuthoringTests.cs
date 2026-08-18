using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What the M4.3 wave-2 operators write into a document: which stage each one is, which numbers travel with
/// it, and what two builds of one program produce.
/// </summary>
/// <remarks>
/// <para>
/// The split every stage of this vocabulary makes, asked of the new ones. A group size, a window, a step, a
/// weight bound, and a key overflow policy are configuration: they change what the graph observably does, a
/// document can state them, and they are in the fingerprint. A cost function, a flattening function, and an
/// element type's equality are behavior: they are delegates, and a delegate is never durable topology.
/// </para>
/// <para>
/// Determinism is asserted as bytes rather than as equality, because a fingerprint is taken over bytes: two
/// builds of one program have to produce the same document, and two programs differing in a number have to
/// produce different ones.
/// </para>
/// </remarks>
public sealed class WaveTwoAuthoringTests
{
    [Fact]
    public void GroupedWritesItsSizeUnderTheCountContract()
    {
        StageNode grouped = Second(Source.From(OrderEvents)
            .Grouped(50)
            .To(Sink.Ignore<IReadOnlyList<OrderCreated>>())
            .Document);

        // The contract take, skip, and repeat already share: a count is a count, and which stage a node is
        // is the stage reference's job to say.
        Assert.Equal(LocalStage("grouped"), grouped.Stage);
        Assert.Equal(Contract("local-count-parameters"), grouped.ParameterContract);
        Assert.Equal("""{"count":50}""", grouped.Parameters.ToString());
    }

    [Fact]
    public void SlidingWritesItsSizeAndStepUnderAContractOfItsOwn()
    {
        StageNode sliding = Second(Source.From(OrderEvents)
            .Sliding(4, 2)
            .To(Sink.Ignore<IReadOnlyList<OrderCreated>>())
            .Document);

        Assert.Equal(LocalStage("sliding"), sliding.Stage);
        Assert.Equal(Contract("local-window-parameters"), sliding.ParameterContract);
        Assert.Equal("""{"size":4,"step":2}""", sliding.Parameters.ToString());
    }

    [Fact]
    public void GroupedWithinWritesItsCountAndItsWindowAsTicks()
    {
        StageNode batch = Second(Source.From(OrderEvents)
            .GroupedWithin(10, TimeSpan.FromSeconds(2))
            .To(Sink.Ignore<IReadOnlyList<OrderCreated>>())
            .Document);

        // The window is ticks and never formatted text, and the clock it will be measured by is nowhere in
        // the document: a clock is runtime and a duration is definition.
        Assert.Equal(LocalStage("grouped-within"), batch.Stage);
        Assert.Equal(Contract("local-grouped-within-parameters"), batch.ParameterContract);
        Assert.Equal(
            $$"""{"maxElements":10,"windowTicks":{{TimeSpan.FromSeconds(2).Ticks}}}""",
            batch.Parameters.ToString());
    }

    [Fact]
    public void AWeightedBatchWritesAllThreeBoundsAndNoCostFunction()
    {
        StageNode batch = Second(Source.From(OrderEvents)
            .GroupedWithin(10, 4096, TimeSpan.FromSeconds(2), _ => 1)
            .To(Sink.Ignore<IReadOnlyList<OrderCreated>>())
            .Document);

        Assert.Equal(LocalStage("grouped-weighted-within"), batch.Stage);
        Assert.Equal(Contract("local-grouped-weighted-parameters"), batch.ParameterContract);
        Assert.Equal(
            $$"""{"maxElements":10,"maxWeight":4096,"windowTicks":{{TimeSpan.FromSeconds(2).Ticks}}}""",
            batch.Parameters.ToString());
    }

    [Fact]
    public void DistinctWritesItsOverflowPolicyBesideItsBound()
    {
        StageNode failing = Second(Source.From(OrderEvents)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 4 })
            .To(Sink.Ignore<OrderCreated>())
            .Document);
        StageNode evicting = Second(Source.From(OrderEvents)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 4, OverflowPolicy = KeyOverflowPolicy.EvictOldest })
            .To(Sink.Ignore<OrderCreated>())
            .Document);

        // The policy changes what the same stream through the same bound does, so it is in the payload and
        // the two graphs are two graphs.
        Assert.Equal("""{"maxTrackedKeys":4,"overflowPolicy":"fail"}""", failing.Parameters.ToString());
        Assert.Equal("""{"maxTrackedKeys":4,"overflowPolicy":"evict-oldest"}""", evicting.Parameters.ToString());
        Assert.NotEqual(failing.Parameters, evicting.Parameters);
    }

    [Fact]
    public void TheOperatorsWhoseWholeBehaviorIsADelegateCarryTheEmptyPayload()
    {
        GraphDocument document = Source.From(OrderEvents)
            .DeduplicateConsecutive()
            .SelectMany(order => new[] { order })
            .To(Sink.Ignore<OrderCreated>())
            .Document;

        Assert.Equal(
            ["from-enumerable", "deduplicate-consecutive", "select-many", "ignore"],
            StageIds(document));
        Assert.All(
            document.Nodes.Skip(1).Take(2),
            node =>
            {
                Assert.Equal(Contract("local-parameters"), node.ParameterContract);
                Assert.Equal("{}", node.Parameters.ToString());
            });
    }

    [Fact]
    public void TwoBuildsOfOneProgramProduceIdenticalBytes()
    {
        Assert.Equal(
            GraphDocumentSerializer.Serialize(Batched(3, TimeSpan.FromSeconds(1)).Document),
            GraphDocumentSerializer.Serialize(Batched(3, TimeSpan.FromSeconds(1)).Document));
    }

    [Fact]
    public void TwoBatchesDifferingOnlyInANumberAreTwoGraphs()
    {
        GraphFingerprint three = GraphDocumentSerializer.Fingerprint(Batched(3, TimeSpan.FromSeconds(1)).Document);
        GraphFingerprint four = GraphDocumentSerializer.Fingerprint(Batched(4, TimeSpan.FromSeconds(1)).Document);
        GraphFingerprint slower = GraphDocumentSerializer.Fingerprint(Batched(3, TimeSpan.FromSeconds(2)).Document);

        Assert.NotEqual(three, four);
        Assert.NotEqual(three, slower);
    }

    [Fact]
    public void TwoSlidingWindowsDifferingOnlyInTheirStepAreTwoGraphs()
    {
        GraphFingerprint stepping = GraphDocumentSerializer.Fingerprint(
            Source.Range(1, 3).Sliding(3, 1).To(Sink.Ignore<IReadOnlyList<int>>()).Document);
        GraphFingerprint striding = GraphDocumentSerializer.Fingerprint(
            Source.Range(1, 3).Sliding(3, 2).To(Sink.Ignore<IReadOnlyList<int>>()).Document);

        Assert.NotEqual(stepping, striding);
    }

    [Fact]
    public void EveryWaveTwoOperatorValidatesAgainstTheLocalCatalog()
    {
        foreach (RunnableGraph graph in Representative())
        {
            GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

            Assert.True(report.IsValid, string.Join("; ", report.Diagnostics.Select(one => one.Message)));
        }
    }

    [Fact]
    public void AGraphCarryingAWaveTwoOperatorIsNondeployable()
    {
        foreach (RunnableGraph graph in Representative())
        {
            Assert.Contains(CapabilityToken.Nondeployable, graph.Document.Capabilities);
        }
    }

    [Fact]
    public void TheBatchingOperatorsRefuseASizeThatHoldsNothing()
    {
        Source<int> numbers = Source.Range(1, 3);
        Flow<int, int> flow = Flow.For<int>();

        foreach (int size in (int[])[0, -1, int.MinValue])
        {
            Assert.Throws<ArgumentOutOfRangeException>("size", () => { _ = numbers.Grouped(size); });
            Assert.Throws<ArgumentOutOfRangeException>("size", () => { _ = flow.Grouped(size); });
            Assert.Throws<ArgumentOutOfRangeException>("size", () => { _ = numbers.Sliding(size, 1); });
            Assert.Throws<ArgumentOutOfRangeException>("step", () => { _ = numbers.Sliding(1, size); });
            Assert.Throws<ArgumentOutOfRangeException>(
                "maxElements",
                () => { _ = numbers.GroupedWithin(size, TimeSpan.FromSeconds(1)); });
            Assert.Throws<ArgumentOutOfRangeException>(
                "maxWeight",
                () => { _ = numbers.GroupedWithin(1, size, TimeSpan.FromSeconds(1), _ => 1); });
        }
    }

    [Fact]
    public void ATimedBatchRefusesAWindowOfNoLength()
    {
        Source<int> numbers = Source.Range(1, 3);

        foreach (TimeSpan window in (TimeSpan[])[TimeSpan.Zero, TimeSpan.FromTicks(-1), Timeout.InfiniteTimeSpan])
        {
            Assert.Throws<ArgumentOutOfRangeException>("window", () => { _ = numbers.GroupedWithin(2, window); });
            Assert.Throws<ArgumentOutOfRangeException>(
                "window",
                () => { _ = numbers.GroupedWithin(2, 2, window, _ => 1); });
        }
    }

    [Fact]
    public void TheNewOperatorsRefuseANullDelegate()
    {
        Source<int> numbers = Source.Range(1, 3);
        Flow<int, int> flow = Flow.For<int>();

        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = numbers.SelectMany<int>(null!); });
        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = flow.SelectMany<int>(null!); });
        Assert.Throws<ArgumentNullException>(
            "cost",
            () => { _ = numbers.GroupedWithin(2, 2, TimeSpan.FromSeconds(1), null!); });
        Assert.Throws<ArgumentNullException>(
            "cost",
            () => { _ = flow.GroupedWithin(2, 2, TimeSpan.FromSeconds(1), null!); });
    }

    [Fact]
    public void AFlowCarryingTheWaveTwoOperatorsComposesIntoEveryGraphSeparately()
    {
        Flow<int, IReadOnlyList<int>> batched = Flow.For<int>()
            .DeduplicateConsecutive()
            .SelectMany(value => new[] { value, value })
            .Grouped(3);

        RunnableGraph one = Source.Range(1, 4).Via(batched).To(Sink.Ignore<IReadOnlyList<int>>());
        RunnableGraph two = Source.Range(1, 4)
            .Via(batched)
            .Via(Flow.For<IReadOnlyList<int>>().Grouped(2))
            .To(Sink.Ignore<IReadOnlyList<IReadOnlyList<int>>>());

        Assert.Equal(
            ["range", "deduplicate-consecutive", "select-many", "grouped", "ignore"],
            StageIds(one.Document));
        Assert.Equal(
            ["range", "deduplicate-consecutive", "select-many", "grouped", "grouped", "ignore"],
            StageIds(two.Document));
    }

    /// <summary>Reads the second node of a document, which is the operator under test.</summary>
    /// <param name="document">The closed document.</param>
    /// <returns>The node.</returns>
    private static StageNode Second(GraphDocument document) => document.Nodes[1];

    /// <summary>Builds the batching graph the determinism assertions are written over.</summary>
    /// <param name="size">The group size.</param>
    /// <param name="window">The window.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Batched(int size, TimeSpan window) =>
        Source.Range(1, 10).GroupedWithin(size, window).To(Sink.Ignore<IReadOnlyList<int>>());

    /// <summary>Builds one closed graph per operator this wave adds.</summary>
    /// <returns>The graphs, in the order the wave's sections are written in.</returns>
    private static IEnumerable<RunnableGraph> Representative()
    {
        yield return Source.Range(1, 6).Grouped(2).To(Sink.Ignore<IReadOnlyList<int>>());
        yield return Source.Range(1, 6).Sliding(3, 1).To(Sink.Ignore<IReadOnlyList<int>>());
        yield return Source.Range(1, 6)
            .GroupedWithin(3, TimeSpan.FromSeconds(1))
            .To(Sink.Ignore<IReadOnlyList<int>>());
        yield return Source.Range(1, 6)
            .GroupedWithin(3, 9, TimeSpan.FromSeconds(1), value => value)
            .To(Sink.Ignore<IReadOnlyList<int>>());
        yield return Source.Range(1, 6).SelectMany(value => new[] { value }).To(Sink.Ignore<int>());
        yield return Source.Range(1, 6).DeduplicateConsecutive().To(Sink.Ignore<int>());
        yield return Source.Range(1, 6)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 2, OverflowPolicy = KeyOverflowPolicy.EvictOldest })
            .To(Sink.Ignore<int>());
        yield return Source.Range(1, 6).Prepend(0).Append(7).To(Sink.Ignore<int>());
        yield return Source.Range(1, 6)
            .DivertTo(value => value > 3, Flow.For<int>().To(s => s.Ignore()))
            .To(Sink.Ignore<int>());
    }
}
