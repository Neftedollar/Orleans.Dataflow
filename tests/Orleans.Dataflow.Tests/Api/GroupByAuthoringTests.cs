using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a keyed stage writes into a document, and what the operator refuses before it writes anything.
/// </summary>
/// <remarks>
/// <para>
/// A keyed stage is the first shape of this vocabulary whose payload carries other stages, and that is what
/// most of this file is about. The bound and the policy are configuration for the reason every number is;
/// the group flow is configuration because leaving it out would make two graphs that observably differ look
/// identical, and a fingerprint that could not tell a group of two from a group of three would be a
/// fingerprint that missed the operator's whole point.
/// </para>
/// <para>
/// Determinism is asserted as bytes rather than as equality, because a fingerprint is taken over bytes: two
/// builds of one program have to produce the same document, and two programs differing anywhere inside the
/// group flow have to produce different ones.
/// </para>
/// </remarks>
public sealed class GroupByAuthoringTests
{
    [Fact]
    public void GroupByWritesItsBoundItsPolicyAndTheStagesOfItsGroupFlow()
    {
        StageNode keyed = Second(Source.Range(1, 6)
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 4 },
                value => value % 2,
                Flow.For<int>().Scan(0, (running, value) => running + value))
            .To(Sink.Ignore<int>())
            .Document);

        // Each stage of the group flow writes its own stage reference and its own payload, so a reader of
        // the document can see what one key's substream is made of without the binding table. What the
        // stages *do* is not here, exactly as it is not anywhere else in a local document.
        Assert.Equal(LocalStage("group-by"), keyed.Stage);
        Assert.Equal(Contract("local-group-by-parameters"), keyed.ParameterContract);
        Assert.Equal(
            """{"group":[{"parameters":{},"stage":"local/scan@v1"}],"maxActiveKeys":4,"overflowPolicy":"fail"}""",
            keyed.Parameters.ToString());
    }

    [Fact]
    public void AGroupFlowsOwnNumbersTravelWithIt()
    {
        StageNode keyed = Second(Source.Range(1, 6)
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2, OverflowPolicy = ActiveKeyOverflowPolicy.EvictIdle },
                value => value % 2,
                Flow.For<int>().Take(3).Grouped(2))
            .To(Sink.Ignore<IReadOnlyList<int>>())
            .Document);

        // The payload of a stage inside a group flow is the payload that stage writes when it stands on its
        // own, under the contract it declares for itself; there is no second grammar for a nested one.
        Assert.Equal(
            """{"group":[{"parameters":{"count":3},"stage":"local/take@v1"},{"parameters":{"count":2},"stage":"local/grouped@v1"}],"maxActiveKeys":2,"overflowPolicy":"evict-idle"}""",
            keyed.Parameters.ToString());
    }

    [Fact]
    public void AnIdentityGroupFlowWritesAnEmptyChain()
    {
        StageNode keyed = Second(Source.Range(1, 6)
            .GroupBy(new GroupByOptions { MaxActiveKeys = 2 }, value => value % 2, Flow.For<int>())
            .To(Sink.Ignore<int>())
            .Document);

        // An identity flow contributes no occurrence anywhere else either, so an empty chain is the honest
        // statement rather than a shape to refuse: what it describes is a keyed stage that costs a key table
        // and passes every key's elements through.
        Assert.Equal(
            """{"group":[],"maxActiveKeys":2,"overflowPolicy":"fail"}""",
            keyed.Parameters.ToString());
    }

    [Fact]
    public void TwoBuildsOfOneKeyedProgramProduceTheSameDocument()
    {
        Assert.Equal(
            Bytes(Keyed(4, Flow.For<int>().Take(3))),
            Bytes(Keyed(4, Flow.For<int>().Take(3))));
    }

    [Fact]
    public void TwoKeyedGraphsDifferingOnlyInTheirBoundAreTwoGraphs()
    {
        Assert.NotEqual(
            Keyed(4, Flow.For<int>().Take(3)).Fingerprint,
            Keyed(5, Flow.For<int>().Take(3)).Fingerprint);
    }

    [Fact]
    public void TwoKeyedGraphsDifferingOnlyInsideTheGroupFlowAreTwoGraphs()
    {
        // The number is three stages down from anything the outer document says, and the fingerprint sees
        // it: a group of three and a group of four are two graphs, which is what putting the flow in the
        // payload bought.
        Assert.NotEqual(
            Keyed(4, Flow.For<int>().Take(3)).Fingerprint,
            Keyed(4, Flow.For<int>().Take(4)).Fingerprint);
    }

    [Fact]
    public void TwoKeyedGraphsWhoseGroupFlowsAreDifferentShapesAreTwoGraphs()
    {
        Assert.NotEqual(
            Keyed(4, Flow.For<int>().Take(3)).Fingerprint,
            Keyed(4, Flow.For<int>().Skip(3)).Fingerprint);
    }

    [Fact]
    public void AGroupFlowThatCannotRunPerKeyIsRefusedNamingEveryOffendingStage()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(() => Source.Range(1, 6)
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>()
                    .Select(value => value)
                    .Buffer(new BufferOptions { Capacity = 2 })
                    .SelectAsync(
                        new ParallelismOptions { MaxConcurrency = 1 },
                        (value, _) => Task.FromResult(value))));

        // Every one of them and its position, because a group flow is written as one expression: an author
        // told about the buffer alone would fix it and be told about the asynchronous stage on the next run.
        Assert.Equal("group", refused.ParamName);
        Assert.Contains("'local/buffer@v1' at position 2", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'local/select-async@v1' at position 3", refused.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("position 1", refused.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("local/select-many@v1")]
    [InlineData("local/group-by@v1")]
    public void TheTwoShapesRefusedForThisOperatorsOwnReasonsAreRefusedByName(string stage)
    {
        Flow<int, int> group = stage is "local/select-many@v1"
            ? Flow.For<int>().SelectMany(value => new[] { value })
            : Flow.For<int>().GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value,
                Flow.For<int>());

        ArgumentException refused = Assert.Throws<ArgumentException>(() => Source.Range(1, 6)
            .GroupBy(new GroupByOptions { MaxActiveKeys = 2 }, value => value % 2, group)
            .To(Sink.Ignore<int>()));

        Assert.Contains($"'{stage}' at position 1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AGroupFlowMayCarryEveryShapeThatFusesPerKey()
    {
        // The whole admitted list in one flow, so that a shape quietly dropped from it fails here rather
        // than in whichever test happened to use it.
        RunnableGraph graph = Source.Range(1, 6)
            .GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                value => value % 2,
                Flow.For<int>()
                    .Select(value => value)
                    .Where(value => value > 0)
                    .Scan(0, (running, value) => running + value)
                    .Skip(0)
                    .Take(100)
                    .TakeWhile(value => value < 1000)
                    .TakeThrough(value => value < 1000)
                    .SkipWhile(value => value < 0)
                    .Distinct(new DistinctOptions { MaxTrackedKeys = 8 })
                    .DeduplicateConsecutive()
                    .Sliding(2, 1)
                    .Grouped(2))
            .To(Sink.Ignore<IReadOnlyList<IReadOnlyList<int>>>());

        Assert.Equal(3, graph.Document.Nodes.Count);
    }

    [Fact]
    public void AKeyedStageRefusesABoundBelowOne()
    {
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.Range(1, 6).GroupBy(
                new GroupByOptions { MaxActiveKeys = 0 },
                value => value % 2,
                Flow.For<int>()));

        Assert.Equal("options", refused.ParamName);
        Assert.Contains("MaxActiveKeys", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AKeyedStageRefusesAPolicyNoMemberDeclares()
    {
        ArgumentOutOfRangeException refused = Assert.Throws<ArgumentOutOfRangeException>(
            () => Source.Range(1, 6).GroupBy(
                new GroupByOptions { MaxActiveKeys = 2, OverflowPolicy = (ActiveKeyOverflowPolicy)7 },
                value => value % 2,
                Flow.For<int>()));

        Assert.Equal("options", refused.ParamName);
        Assert.Contains("EvictIdle", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AKeyedStageRefusesEveryMissingArgument()
    {
        _ = Assert.Throws<ArgumentNullException>(
            () => Source.Range(1, 6).GroupBy(null!, (int value) => value, Flow.For<int>()));
        _ = Assert.Throws<ArgumentNullException>(
            () => Source.Range(1, 6).GroupBy<int, int>(
                new GroupByOptions { MaxActiveKeys = 2 },
                null!,
                Flow.For<int>()));
        _ = Assert.Throws<ArgumentNullException>(
            () => Source.Range(1, 6).GroupBy(
                new GroupByOptions { MaxActiveKeys = 2 },
                (int value) => value,
                (Flow<int, int>)null!));
        _ = Assert.Throws<ArgumentNullException>(
            () => Flow.For<int>().GroupBy(null!, (int value) => value, Flow.For<int>()));
    }

    [Fact]
    public void AKeyedStageOnAFlowWritesTheNodeASourceWrites()
    {
        // One operator with two spellings, and the document cannot tell which of them was written — which
        // is the property every operator of this vocabulary has and the one a nested payload could have
        // broken by accident.
        Assert.Equal(
            Second(Source.Range(1, 6)
                .GroupBy(new GroupByOptions { MaxActiveKeys = 3 }, value => value % 2, Flow.For<int>().Take(2))
                .To(Sink.Ignore<int>())
                .Document).Parameters.ToString(),
            Second(Source.Range(1, 6)
                .Via(Flow.For<int>().GroupBy(
                    new GroupByOptions { MaxActiveKeys = 3 },
                    value => value % 2,
                    Flow.For<int>().Take(2)))
                .To(Sink.Ignore<int>())
                .Document).Parameters.ToString());
    }

    [Fact]
    public void EveryStageOfTheVocabularyIsRecoverableFromTheTextADocumentSpells()
    {
        // A group flow's payload names its stages by the text a stage reference renders as, so the reverse
        // lookup has to be total. It is built from the forward one, and this is what says so: a reference
        // declared after the table that reads it would be the default value in it, which would collide with
        // every other default and be silently wrong rather than fail to compile.
        foreach (LocalStageKind kind in Enum.GetValues<LocalStageKind>())
        {
            Assert.True(
                LocalVocabulary.TryReadStage(LocalVocabulary.StageOf(kind).ToString(), out LocalStageKind read),
                $"The stage '{kind}' is not recoverable from its own text.");
            Assert.Equal(kind, read);
        }
    }

    [Fact]
    public void AKeyedGraphDeclaresTheCapabilitiesEveryLocalGraphDeclares()
    {
        RunnableGraph graph = Keyed(4, Flow.For<int>().Take(3));

        Assert.Equal(["ephemeral-identity", "nondeployable"], Capabilities(graph.Document));
    }

    /// <summary>Reads the node a keyed chain of three puts its keyed stage at.</summary>
    /// <param name="document">The closed document.</param>
    /// <returns>The node.</returns>
    private static StageNode Second(GraphDocument document) => document.Nodes[1];

    /// <summary>Builds the keyed graph the determinism assertions are written over.</summary>
    /// <param name="maxActiveKeys">The bound on active keys.</param>
    /// <param name="group">The group flow.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Keyed(int maxActiveKeys, Flow<int, int> group) =>
        Source.Range(1, 10)
            .GroupBy(new GroupByOptions { MaxActiveKeys = maxActiveKeys }, value => value % 2, group)
            .To(Sink.Ignore<int>());

    /// <summary>Serializes a graph's document to the bytes its fingerprint is taken over.</summary>
    /// <param name="graph">The closed graph.</param>
    /// <returns>The canonical bytes.</returns>
    private static byte[] Bytes(RunnableGraph graph) =>
        GraphDocumentSerializer.Serialize(graph.Document);
}
