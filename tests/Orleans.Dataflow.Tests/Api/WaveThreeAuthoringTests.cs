using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What the M4.3 wave-3 operators write into a document: which stage each one is, which numbers travel with
/// it, and what two builds of one program produce.
/// </summary>
/// <remarks>
/// <para>
/// The split every stage of this vocabulary makes, asked of the three new ones. A merge-map's bound on open
/// inner sequences is configuration — it changes what the graph observably does, a document can state it,
/// and it is in the fingerprint — and it is the only number this wave adds. The functions are behavior, and
/// so, more interestingly, is the <i>shape</i> of a merge-map's function: an asynchronous inner sequence and
/// an ordinary one build the same node, because how an author's sequence produces its elements is behavior
/// in exactly the way the body of a mapping function is.
/// </para>
/// <para>
/// The two asynchronous folds declare nothing at all, and the absence is itself the contract: one fold of
/// either runs at a time because the next one folds this one's answer, so there is no bound for an author to
/// write down and none for a document to carry.
/// </para>
/// </remarks>
public sealed class WaveThreeAuthoringTests
{
    [Fact]
    public void MergeMapWritesItsBoundUnderTheParallelismContract()
    {
        StageNode merged = Second(Source.From(OrderEvents)
            .MergeMap(new ParallelismOptions { MaxConcurrency = 4 }, Normalize)
            .To(Sink.Ignore<OrderDocument>())
            .Document);

        // The contract the asynchronous stages already share: a bound on concurrent work is a number, and
        // which stage a node is is the stage reference's job to say.
        Assert.Equal(LocalStage("merge-map"), merged.Stage);
        Assert.Equal(Contract("local-parallelism-parameters"), merged.ParameterContract);
        Assert.Equal("""{"maxConcurrency":4}""", merged.Parameters.ToString());
    }

    [Fact]
    public void BothSpellingsOfAMergeMapBuildTheSameDocument()
    {
        GraphDocument asynchronous = Source.Range(1, 6)
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, Counting)
            .To(Sink.Ignore<int>())
            .Document;
        GraphDocument ordinary = Source.Range(1, 6)
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, value => Enumerable.Repeat(value, value))
            .To(Sink.Ignore<int>())
            .Document;

        // Byte-identical, which is the claim: what a document states is that this node merges what its
        // function answers, and a delegate is never durable topology.
        Assert.Equal(
            GraphDocumentSerializer.Serialize(asynchronous),
            GraphDocumentSerializer.Serialize(ordinary));
    }

    [Fact]
    public void TheAsynchronousFoldsCarryTheEmptyPayload()
    {
        GraphDocument document = Source.From(OrderEvents)
            .ScanAsync(0m, (total, order, _) => Task.FromResult(total + order.Total))
            .To(
                s => s.AggregateAsync(0m, (highest, total, _) => Task.FromResult(Math.Max(highest, total))),
                "highest",
                out ResultSlot<decimal> _)
            .Document;

        Assert.Equal(["from-enumerable", "scan-async", "fold-async"], StageIds(document));
        Assert.All(
            document.Nodes.Skip(1),
            node =>
            {
                Assert.Equal(Contract("local-parameters"), node.ParameterContract);
                Assert.Equal("{}", node.Parameters.ToString());
            });
    }

    [Fact]
    public void AnAsynchronousFoldDeclaresTheFoldsOwnResultContract()
    {
        RunnableGraph graph = Source.From(OrderEvents)
            .To(
                s => s.AggregateAsync(0m, (total, order, _) => Task.FromResult(total + order.Total)),
                "total",
                out ResultSlot<decimal> _);

        // The identity says which shape produced the value, and awaiting is not a different shape; nothing
        // is renamed and no fourth result identity is invented.
        Assert.Equal(Contract("local-fold-result"), Assert.Single(graph.Document.ResultSlots).ResultContract);
        Assert.Equal("result", Assert.Single(graph.Document.ResultSlots).Producer.Port.Value);
    }

    [Fact]
    public void TwoMergeMapsDifferingOnlyInTheirBoundAreTwoGraphs()
    {
        GraphFingerprint two = GraphDocumentSerializer.Fingerprint(Merged(2).Document);
        GraphFingerprint three = GraphDocumentSerializer.Fingerprint(Merged(3).Document);

        Assert.NotEqual(two, three);
    }

    [Fact]
    public void TwoBuildsOfOneWaveThreeProgramProduceIdenticalBytes()
    {
        Assert.Equal(
            GraphDocumentSerializer.Serialize(Merged(4).Document),
            GraphDocumentSerializer.Serialize(Merged(4).Document));
    }

    [Fact]
    public void EveryWaveThreeOperatorValidatesAgainstTheLocalCatalog()
    {
        foreach (RunnableGraph graph in Representative())
        {
            GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

            Assert.True(report.IsValid, string.Join("; ", report.Diagnostics.Select(one => one.Message)));
        }
    }

    [Fact]
    public void AGraphCarryingAWaveThreeOperatorIsNondeployable()
    {
        foreach (RunnableGraph graph in Representative())
        {
            Assert.Contains(CapabilityToken.Nondeployable, graph.Document.Capabilities);
        }
    }

    [Fact]
    public void AMergeMapRefusesABoundThatOpensNothing()
    {
        Source<int> numbers = Source.Range(1, 3);
        Flow<int, int> flow = Flow.For<int>();

        foreach (int concurrency in (int[])[0, -1, int.MinValue])
        {
            ParallelismOptions options = new() { MaxConcurrency = concurrency };

            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = numbers.MergeMap(options, Counting); });
            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = numbers.MergeMap(options, value => Enumerable.Repeat(value, value)); });
            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = flow.MergeMap(options, Counting); });
            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = flow.MergeMap(options, value => Enumerable.Repeat(value, value)); });
        }
    }

    [Fact]
    public void TheWaveThreeOperatorsRefuseANullDelegate()
    {
        Source<int> numbers = Source.Range(1, 3);
        Flow<int, int> flow = Flow.For<int>();
        ParallelismOptions options = new() { MaxConcurrency = 2 };

        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = numbers.MergeMap(options, (Func<int, IAsyncEnumerable<int>>)null!); });
        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = numbers.MergeMap(options, (Func<int, IEnumerable<int>>)null!); });
        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = flow.MergeMap(options, (Func<int, IAsyncEnumerable<int>>)null!); });
        Assert.Throws<ArgumentNullException>("folder", () => { _ = numbers.ScanAsync(0, null!); });
        Assert.Throws<ArgumentNullException>("folder", () => { _ = flow.ScanAsync(0, null!); });
        Assert.Throws<ArgumentNullException>("folder", () => { _ = Sink.AggregateAsync<int, long>(0L, null!); });
        Assert.Throws<ArgumentNullException>("folder", () => { _ = Sink.For<int>().AggregateAsync(0L, null!); });
        Assert.Throws<ArgumentNullException>(
            "options",
            () => { _ = numbers.MergeMap(null!, Counting); });
    }

    [Fact]
    public void TheNamedSinkSpellingAndTheFactorySpellingBuildOneSink()
    {
        GraphDocument named = Source.Range(1, 3)
            .To(Sink.AggregateAsync<int, long>(0L, (sum, value, _) => Task.FromResult(sum + value)), "total", out _)
            .Document;
        GraphDocument inferred = Source.Range(1, 3)
            .To(s => s.AggregateAsync(0L, (sum, value, _) => Task.FromResult(sum + value)), "total", out _)
            .Document;

        Assert.Equal(GraphDocumentSerializer.Serialize(named), GraphDocumentSerializer.Serialize(inferred));
    }

    [Fact]
    public void AFlowCarryingTheWaveThreeOperatorsComposesIntoEveryGraphSeparately()
    {
        Flow<int, long> merged = Flow.For<int>()
            .MergeMap(new ParallelismOptions { MaxConcurrency = 2 }, Counting)
            .ScanAsync(0L, (sum, value, _) => Task.FromResult(sum + value));

        RunnableGraph one = Source.Range(1, 4).Via(merged).To(Sink.Ignore<long>());
        RunnableGraph two = Source.Range(1, 4)
            .Via(merged)
            .Via(Flow.For<long>().Grouped(2))
            .To(Sink.Ignore<IReadOnlyList<long>>());

        Assert.Equal(["range", "merge-map", "scan-async", "ignore"], StageIds(one.Document));
        Assert.Equal(["range", "merge-map", "scan-async", "grouped", "ignore"], StageIds(two.Document));
    }

    /// <summary>Reads the second node of a document, which is the operator under test.</summary>
    /// <param name="document">The closed document.</param>
    /// <returns>The node.</returns>
    private static StageNode Second(GraphDocument document) => document.Nodes[1];

    /// <summary>Builds the merging graph the determinism assertions are written over.</summary>
    /// <param name="concurrency">How many inner sequences may be open at once.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Merged(int concurrency) =>
        Source.Range(1, 10)
            .MergeMap(new ParallelismOptions { MaxConcurrency = concurrency }, Counting)
            .To(Sink.Ignore<int>());

    /// <summary>Builds one closed graph per operator this wave adds.</summary>
    /// <returns>The graphs, in the order the wave's sections are written in.</returns>
    private static IEnumerable<RunnableGraph> Representative()
    {
        yield return Source.Range(1, 6)
            .MergeMap(new ParallelismOptions { MaxConcurrency = 3 }, Counting)
            .To(Sink.Ignore<int>());
        yield return Source.Range(1, 6)
            .MergeMap(new ParallelismOptions { MaxConcurrency = 3 }, value => Enumerable.Repeat(value, value))
            .To(Sink.Ignore<int>());
        yield return Source.Range(1, 6)
            .ScanAsync(0L, (sum, value, _) => Task.FromResult(sum + value))
            .To(Sink.Ignore<long>());
        yield return Source.Range(1, 6)
            .To(s => s.AggregateAsync(0L, (sum, value, _) => Task.FromResult(sum + value)), "total", out _);
    }

    /// <summary>An asynchronous sequence of one number, repeated that many times.</summary>
    /// <param name="value">The number.</param>
    /// <returns>The sequence.</returns>
    /// <remarks>
    /// Written as a method so that the authoring surface is exercised with a method group as well as with a
    /// lambda: an asynchronous iterator cannot be written inline in C#, so a method group is what an author
    /// actually passes.
    /// </remarks>
    private static async IAsyncEnumerable<int> Counting(int value)
    {
        for (int index = 0; index < value; index++)
        {
            await Task.Yield();

            yield return value;
        }
    }

    /// <summary>An asynchronous sequence of the one document a valid order event normalizes into.</summary>
    /// <param name="order">The order event.</param>
    /// <returns>The sequence, which is empty for an invalid event.</returns>
    private static async IAsyncEnumerable<OrderDocument> Normalize(OrderCreated order)
    {
        await Task.Yield();

        if (order.IsValid)
        {
            yield return new OrderDocument(order.OrderId, order.Total);
        }
    }
}
