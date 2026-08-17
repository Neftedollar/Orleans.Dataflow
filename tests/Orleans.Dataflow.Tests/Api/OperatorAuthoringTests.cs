using System.Text;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What the operators and sources counted in numbers write into a document, and what they refuse to be
/// built from.
/// </summary>
/// <remarks>
/// <para>
/// The same split the boundaries made: a count, a range, and a key bound are configuration — values a
/// document can carry honestly, which change what a graph observably does — so they belong in the payload
/// and in the identity it is fingerprinted into. A predicate, a folder, and a generator are behavior and
/// stay where every delegate stays.
/// </para>
/// <para>
/// The payload bytes are pinned rather than round-tripped, because the payload is part of a fingerprint
/// that other runtimes will have to agree with. Round-tripping would prove this process consistent with
/// itself; the bytes are what another one has to reproduce.
/// </para>
/// </remarks>
public sealed class OperatorAuthoringTests
{
    [Fact]
    public void TakeWritesItsCountAsCanonicalJson()
    {
        StageNode take = Second(Source.From(OrderEvents).Take(3).To(Sink.Ignore<OrderCreated>()).Document);

        Assert.Equal(LocalStage("take"), take.Stage);
        Assert.Equal(Contract("local-count-parameters"), take.ParameterContract);
        Assert.Equal("""{"count":3}""", take.Parameters.ToString());
        Assert.Equal(
            Encoding.UTF8.GetBytes("""{"count":3}"""),
            take.Parameters.CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void SkipAndRepeatWriteTheirCountsUnderTheSameContractAsTake()
    {
        // One payload shape for the three stages counted in elements, told apart by their stage reference
        // alone. A count is a count whichever of them carries it.
        GraphDocument skipped = Source.From(OrderEvents).Skip(2).To(Sink.Ignore<OrderCreated>()).Document;
        GraphDocument repeated = Source.Repeat("x", 4).To(Sink.Ignore<string>()).Document;

        Assert.Equal(Contract("local-count-parameters"), Second(skipped).ParameterContract);
        Assert.Equal("""{"count":2}""", Second(skipped).Parameters.ToString());
        Assert.Equal(Contract("local-count-parameters"), repeated.Nodes[0].ParameterContract);
        Assert.Equal("""{"count":4}""", repeated.Nodes[0].Parameters.ToString());
    }

    [Fact]
    public void RangeWritesBothOfItsBoundsAsCanonicalJson()
    {
        StageNode range = Source.Range(-2, 5).To(Sink.Ignore<int>()).Document.Nodes[0];

        Assert.Equal(LocalStage("range"), range.Stage);
        Assert.Equal(Contract("local-range-parameters"), range.ParameterContract);

        // Canonical form sorts the members, and 'count' precedes 'start' ordinally.
        Assert.Equal("""{"count":5,"start":-2}""", range.Parameters.ToString());
        Assert.Equal(
            Encoding.UTF8.GetBytes("""{"count":5,"start":-2}"""),
            range.Parameters.CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void DistinctWritesItsKeyBoundAsCanonicalJson()
    {
        StageNode distinct = Second(Source.From(OrderEvents)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 1000 })
            .To(Sink.Ignore<OrderCreated>())
            .Document);

        Assert.Equal(LocalStage("distinct"), distinct.Stage);
        Assert.Equal(Contract("local-distinct-parameters"), distinct.ParameterContract);
        Assert.Equal("""{"maxTrackedKeys":1000}""", distinct.Parameters.ToString());
        Assert.Equal(
            Encoding.UTF8.GetBytes("""{"maxTrackedKeys":1000}"""),
            distinct.Parameters.CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void ForEachAsyncWritesItsConcurrencyUnderTheParallelismContract()
    {
        // A callback sink is an asynchronous stage that emits nothing, so it declares what every
        // asynchronous stage declares: how many callbacks may run at once.
        GraphDocument document = Source.From(OrderEvents)
            .To(s => s.ForEachAsync(new ParallelismOptions { MaxConcurrency = 3 }, (_, _) => Task.CompletedTask))
            .Document;

        Assert.Equal(["from-enumerable", "for-each-async"], StageIds(document));
        Assert.Equal(Contract("local-parallelism-parameters"), document.Nodes[1].ParameterContract);
        Assert.Equal("""{"maxConcurrency":3}""", document.Nodes[1].Parameters.ToString());
    }

    [Fact]
    public void TheOperatorsWhoseWholeBehaviorIsADelegateCarryTheEmptyPayload()
    {
        GraphDocument document = Source.From(OrderEvents)
            .Scan(0m, (total, order) => total + order.Total)
            .TakeWhile(total => total < 100m)
            .TakeThrough(total => total < 90m)
            .SkipWhile(total => total < 5m)
            .To(s => s.ForEach(_ => { }))
            .Document;

        Assert.Equal(
            ["from-enumerable", "scan", "take-while", "take-through", "skip-while", "for-each"],
            StageIds(document));

        foreach (StageNode node in document.Nodes)
        {
            Assert.Equal(Contract("local-parameters"), node.ParameterContract);
            Assert.Equal(CanonicalJsonValue.Parse("{}"), node.Parameters);
        }
    }

    [Fact]
    public void TheSourcesWhoseElementsAreValuesCarryTheEmptyPayloadToo()
    {
        // A repeated value, an awaited task, and an exception are values of types a local document knows
        // nothing about, so they stay in the binding table; only the numbers beside them are written down.
        foreach ((string stage, RunnableGraph graph) in (( string, RunnableGraph)[])
        [
            ("empty", Source.Empty<int>().To(Sink.Ignore<int>())),
            ("single", Source.Single(1).To(Sink.Ignore<int>())),
            ("from-task", Source.FromTask(Task.FromResult(1)).To(Sink.Ignore<int>())),
            ("failed", Source.Failed<int>(new InvalidOperationException("no")).To(Sink.Ignore<int>())),
            ("unfold", Source.Unfold(
                    0,
                    (int state, out int value, out int next) =>
                    {
                        value = state;
                        next = state + 1;

                        return state < 3;
                    })
                .To(Sink.Ignore<int>())),
        ])
        {
            StageNode source = graph.Document.Nodes[0];

            Assert.Equal(LocalStage(stage), source.Stage);
            Assert.Equal(Contract("local-parameters"), source.ParameterContract);
            Assert.Equal(CanonicalJsonValue.Parse("{}"), source.Parameters);
        }
    }

    [Fact]
    public void TheResultBearingSinksDeclareTheirOwnResultContract()
    {
        RunnableGraph first = Source.From(OrderEvents).To(s => s.First(), "head", out ResultSlot<OrderCreated> _);
        RunnableGraph counted = Source.From(OrderEvents).To(s => s.Count(), "counted", out ResultSlot<long> _);

        Assert.Equal(Contract("local-result"), Assert.Single(first.Document.ResultSlots).ResultContract);
        Assert.Equal("stage-0002", Assert.Single(first.Document.ResultSlots).Producer.Node.Value);
        Assert.Equal(Contract("local-result"), Assert.Single(counted.Document.ResultSlots).ResultContract);

        // The fold keeps the identity it has always declared; the sinks that arrived later declare the
        // general one rather than renaming it.
        RunnableGraph folded = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        Assert.Equal(Contract("local-fold-result"), Assert.Single(folded.Document.ResultSlots).ResultContract);
    }

    [Fact]
    public void TwoGraphsDifferingOnlyInACountHaveDifferentFingerprints()
    {
        // The whole reason a count is in the document. The two graphs share every delegate, every stage
        // reference, and every edge, and they behave differently, so their identities differ too.
        Assert.NotEqual(Taking(2).Fingerprint, Taking(3).Fingerprint);
        Assert.Equal(Taking(2).Fingerprint, Taking(2).Fingerprint);
    }

    [Fact]
    public void TwoGraphsDifferingOnlyInARangeOrAKeyBoundHaveDifferentFingerprints()
    {
        Assert.NotEqual(Ranged(0, 5).Fingerprint, Ranged(1, 5).Fingerprint);
        Assert.NotEqual(Ranged(0, 5).Fingerprint, Ranged(0, 6).Fingerprint);
        Assert.Equal(Ranged(3, 4).Fingerprint, Ranged(3, 4).Fingerprint);

        Assert.NotEqual(Deduplicating(10).Fingerprint, Deduplicating(11).Fingerprint);
        Assert.Equal(Deduplicating(10).Fingerprint, Deduplicating(10).Fingerprint);
    }

    [Fact]
    public void TwoGraphsDifferingOnlyInAPredicateStillShareAFingerprint()
    {
        // The other half of the same claim, so that it is not overstated: a document records a stage and
        // its parameters, never a delegate.
        RunnableGraph one = Source.From(OrderEvents)
            .TakeWhile(order => order.IsValid)
            .To(Sink.Ignore<OrderCreated>());
        RunnableGraph other = Source.From(OrderEvents)
            .TakeWhile(order => order.Total > 1000m)
            .To(Sink.Ignore<OrderCreated>());

        Assert.Equal(one.Fingerprint, other.Fingerprint);
    }

    [Fact]
    public void ADocumentCarryingTheNewPayloadsSurvivesSerializationByteForByte()
    {
        GraphDocument document = Source.Range(7, 3)
            .Skip(1)
            .Distinct(new DistinctOptions { MaxTrackedKeys = 64 })
            .Take(2)
            .To(Sink.Ignore<int>())
            .Document;

        byte[] bytes = GraphDocumentSerializer.Serialize(document);
        GraphDocument decoded = GraphDocumentSerializer.Deserialize(bytes);

        Assert.Equal(document, decoded);
        Assert.Equal(bytes, GraphDocumentSerializer.Serialize(decoded));
        Assert.Equal(
            GraphDocumentSerializer.Fingerprint(document),
            GraphDocumentSerializer.Fingerprint(decoded));
        Assert.Equal("""{"count":3,"start":7}""", decoded.Nodes[0].Parameters.ToString());
        Assert.Equal("""{"maxTrackedKeys":64}""", decoded.Nodes[2].Parameters.ToString());
    }

    [Fact]
    public void TheCountedOperatorsRejectACountThatCountsNothing()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);
        Flow<OrderCreated, OrderCreated> flow = Flow.For<OrderCreated>();

        foreach (int count in (int[])[-1, int.MinValue])
        {
            Assert.Throws<ArgumentOutOfRangeException>("count", () => { _ = orders.Take(count); });
            Assert.Throws<ArgumentOutOfRangeException>("count", () => { _ = orders.Skip(count); });
            Assert.Throws<ArgumentOutOfRangeException>("count", () => { _ = flow.Take(count); });
            Assert.Throws<ArgumentOutOfRangeException>("count", () => { _ = flow.Skip(count); });
            Assert.Throws<ArgumentOutOfRangeException>("count", () => { _ = Source.Repeat("x", count); });
            Assert.Throws<ArgumentOutOfRangeException>("count", () => { _ = Source.Range(0, count); });
        }

        // Zero is admitted everywhere it is written, because all three shapes mean something with it.
        Assert.Equal(3, orders.Take(0).To(Sink.Ignore<OrderCreated>()).Document.Nodes.Count);
        Assert.Equal(2, Source.Repeat("x", 0).To(Sink.Ignore<string>()).Document.Nodes.Count);
        Assert.Equal(2, Source.Range(0, 0).To(Sink.Ignore<int>()).Document.Nodes.Count);
    }

    [Fact]
    public void ARangeRejectsBoundsWhoseLastElementWouldNotFit()
    {
        // The check LINQ applies, reported against the count, because the start is a number the author
        // chose freely and the count is the one that has to fit beside it.
        ArgumentOutOfRangeException rejected =
            Assert.Throws<ArgumentOutOfRangeException>("count", () => { _ = Source.Range(int.MaxValue, 2); });

        Assert.Contains("2147483647", rejected.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentOutOfRangeException>("count", () => { _ = Source.Range(2, int.MaxValue); });

        // The two largest ranges that do fit are built rather than refused: one that ends at the largest
        // integer from the far end, and one that ends there having started at one.
        Assert.Equal(2, Source.Range(int.MaxValue - 1, 2).To(Sink.Ignore<int>()).Document.Nodes.Count);
        Assert.Equal(2, Source.Range(1, int.MaxValue).To(Sink.Ignore<int>()).Document.Nodes.Count);
    }

    [Fact]
    public void TheDeduplicatingOperatorRejectsOptionsThatRememberNothing()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);
        Flow<OrderCreated, OrderCreated> flow = Flow.For<OrderCreated>();

        Assert.Throws<ArgumentNullException>("options", () => { _ = orders.Distinct(null!); });
        Assert.Throws<ArgumentNullException>("options", () => { _ = flow.Distinct(null!); });

        foreach (int bound in (int[])[0, -1, int.MinValue])
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = orders.Distinct(new DistinctOptions { MaxTrackedKeys = bound }); });
            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = flow.Distinct(new DistinctOptions { MaxTrackedKeys = bound }); });
        }
    }

    [Fact]
    public void TheDelegateTakingOperatorsAndSourcesRejectANullDelegate()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);
        Flow<OrderCreated, OrderCreated> flow = Flow.For<OrderCreated>();

        Assert.Throws<ArgumentNullException>("folder", () => { _ = orders.Scan(0L, (Func<long, OrderCreated, long>)null!); });
        Assert.Throws<ArgumentNullException>("folder", () => { _ = flow.Scan(0L, (Func<long, OrderCreated, long>)null!); });
        Assert.Throws<ArgumentNullException>("predicate", () => { _ = orders.TakeWhile(null!); });
        Assert.Throws<ArgumentNullException>("predicate", () => { _ = orders.TakeThrough(null!); });
        Assert.Throws<ArgumentNullException>("predicate", () => { _ = orders.SkipWhile(null!); });
        Assert.Throws<ArgumentNullException>("predicate", () => { _ = flow.TakeWhile(null!); });
        Assert.Throws<ArgumentNullException>("predicate", () => { _ = flow.TakeThrough(null!); });
        Assert.Throws<ArgumentNullException>("predicate", () => { _ = flow.SkipWhile(null!); });
        Assert.Throws<ArgumentNullException>("task", () => { _ = Source.FromTask<int>(null!); });
        Assert.Throws<ArgumentNullException>("exception", () => { _ = Source.Failed<int>(null!); });
        Assert.Throws<ArgumentNullException>("generator", () => { _ = Source.Unfold<int, int>(0, null!); });
        Assert.Throws<ArgumentNullException>("callback", () => { _ = Sink.ForEach<int>(null!); });
        Assert.Throws<ArgumentNullException>(
            "callback",
            () => { _ = Sink.ForEachAsync<int>(new ParallelismOptions { MaxConcurrency = 1 }, null!); });
        Assert.Throws<ArgumentNullException>(
            "options",
            () => { _ = Sink.ForEachAsync<int>(null!, (_, _) => Task.CompletedTask); });
        Assert.Throws<ArgumentOutOfRangeException>(
            "options",
            () => { _ = Sink.ForEachAsync<int>(new ParallelismOptions { MaxConcurrency = 0 }, (_, _) => Task.CompletedTask); });
    }

    [Fact]
    public void ARejectedOperatorLeavesTheValueItWasCalledOnUnchanged()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);

        Assert.Throws<ArgumentOutOfRangeException>("count", () => { _ = orders.Take(-1); });

        Assert.Equal("source (1 stage)", orders.ToString());
        Assert.Equal(2, orders.To(Sink.Ignore<OrderCreated>()).Document.Nodes.Count);
    }

    [Fact]
    public void TheOptionRecordRendersItselfForALogLine()
    {
        Assert.Equal(
            "distinct (up to 1000 tracked keys)",
            new DistinctOptions { MaxTrackedKeys = 1000 }.ToString());

        // Never throws, including for a value placing a stage would refuse.
        Assert.Equal("distinct (up to 0 tracked keys)", new DistinctOptions { MaxTrackedKeys = 0 }.ToString());
    }

    [Fact]
    public void AFlowCarryingTheNewOperatorsComposesIntoEveryGraphSeparately()
    {
        Flow<int, long> windowed = Flow.For<int>()
            .Skip(1)
            .Scan(0L, (sum, value) => sum + value)
            .Take(4);

        RunnableGraph graph = Source.Range(1, 10)
            .Via(windowed)
            .To(s => s.Count(), "counted", out ResultSlot<long> _);

        Assert.Equal(
            ["range", "skip", "scan", "take", "count"],
            StageIds(graph.Document));
        Assert.True(GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance).IsValid);
    }

    /// <summary>Reads the second node of a document, which is the operator under test in most of these.</summary>
    /// <param name="document">The document.</param>
    /// <returns>The node.</returns>
    private static StageNode Second(GraphDocument document) => document.Nodes[1];

    /// <summary>Builds the same graph with one take count.</summary>
    /// <param name="count">The number of elements to take.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Taking(int count) =>
        Source.From(OrderEvents).Take(count).To(Sink.Ignore<OrderCreated>());

    /// <summary>Builds the same graph with one range.</summary>
    /// <param name="start">The first element.</param>
    /// <param name="count">The number of elements.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Ranged(int start, int count) =>
        Source.Range(start, count).To(Sink.Ignore<int>());

    /// <summary>Builds the same graph with one key bound.</summary>
    /// <param name="maxTrackedKeys">The bound.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Deduplicating(int maxTrackedKeys) =>
        Source.From(OrderEvents)
            .Distinct(new DistinctOptions { MaxTrackedKeys = maxTrackedKeys })
            .To(Sink.Ignore<OrderCreated>());
}
