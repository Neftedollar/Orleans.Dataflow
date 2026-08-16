using System.Text;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a buffer and an asynchronous stage write into a document, and what they refuse to be built from.
/// </summary>
/// <remarks>
/// <para>
/// These three stages are the first of the local vocabulary whose behavior a document can state. A
/// capacity, an overflow policy, and a concurrency bound are configuration rather than behavior: they are
/// values a graph can carry honestly, they change what a graph observably does, and so they belong in the
/// payload and in the identity it is fingerprinted into. The delegate stays where every delegate stays.
/// </para>
/// <para>
/// The payload bytes are pinned rather than round-tripped, because the payload is now part of a
/// fingerprint that other runtimes will have to agree with. Round-tripping would prove this process
/// consistent with itself; the bytes are what another one has to reproduce.
/// </para>
/// </remarks>
public sealed class BoundaryAuthoringTests
{
    [Fact]
    public void ABufferWritesItsCapacityAndPolicyAsCanonicalJson()
    {
        GraphDocument document = Source.From(OrderEvents)
            .Buffer(new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.DropOldest })
            .To(Sink.Ignore<OrderCreated>())
            .Document;

        StageNode buffer = document.Nodes[1];

        Assert.Equal(LocalStage("buffer"), buffer.Stage);
        Assert.Equal(Contract("local-buffer-parameters"), buffer.ParameterContract);
        Assert.Equal("""{"capacity":8,"overflowPolicy":"drop-oldest"}""", buffer.Parameters.ToString());
        Assert.Equal(
            Encoding.UTF8.GetBytes("""{"capacity":8,"overflowPolicy":"drop-oldest"}"""),
            buffer.Parameters.CanonicalUtf8Bytes.ToArray());
    }

    [Theory]
    [InlineData(OverflowPolicy.Backpressure, "backpressure")]
    [InlineData(OverflowPolicy.DropOldest, "drop-oldest")]
    [InlineData(OverflowPolicy.DropNewest, "drop-newest")]
    [InlineData(OverflowPolicy.DropBuffer, "drop-buffer")]
    [InlineData(OverflowPolicy.Fail, "fail")]
    public void EveryOverflowPolicyIsSpelledInKebabCase(OverflowPolicy policy, string spelling)
    {
        // Spelled rather than numbered: a number would make the document's meaning depend on the
        // declaration order of a CLR enumeration that no other frontend can see.
        GraphDocument document = Source.From(OrderEvents)
            .Buffer(new BufferOptions { Capacity = 1, OverflowPolicy = policy })
            .To(Sink.Ignore<OrderCreated>())
            .Document;

        Assert.Equal($$"""{"capacity":1,"overflowPolicy":"{{spelling}}"}""", document.Nodes[1].Parameters.ToString());
    }

    [Fact]
    public void AnAsynchronousStageWritesItsConcurrencyAsCanonicalJsonAndKeepsItsCallbackOutOfTheDocument()
    {
        GraphDocument document = Source.From(OrderEvents)
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 4 },
                (order, _) => Task.FromResult(order.OrderId))
            .To(Sink.Ignore<string>())
            .Document;

        StageNode stage = document.Nodes[1];

        Assert.Equal(LocalStage("select-async"), stage.Stage);
        Assert.Equal(Contract("local-parallelism-parameters"), stage.ParameterContract);
        Assert.Equal("""{"maxConcurrency":4}""", stage.Parameters.ToString());
        Assert.Equal(
            Encoding.UTF8.GetBytes("""{"maxConcurrency":4}"""),
            stage.Parameters.CanonicalUtf8Bytes.ToArray());
    }

    [Fact]
    public void TheTwoAsynchronousSpellingsAreTwoStagesUnderOneParameterContract()
    {
        // Ordering is which operator was written, so it is the stage; the bound is a number, so it is the
        // payload. The two therefore share a parameter contract and differ in their stage reference.
        GraphDocument document = Source.From(OrderEvents)
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 2 }, (order, _) => Task.FromResult(order.Total))
            .SelectAsyncUnordered(new ParallelismOptions { MaxConcurrency = 2 }, (total, _) => Task.FromResult(total))
            .To(Sink.Ignore<decimal>())
            .Document;

        Assert.Equal(
            ["from-enumerable", "select-async", "select-async-unordered", "ignore"],
            StageIds(document));
        Assert.Equal(document.Nodes[1].ParameterContract, document.Nodes[2].ParameterContract);
        Assert.Equal(document.Nodes[1].Parameters, document.Nodes[2].Parameters);
    }

    [Fact]
    public void ABoundaryIsAnOrdinaryLinkInTheChainAndWiresLikeOne()
    {
        GraphDocument document = Source.From(OrderEvents)
            .Buffer(new BufferOptions { Capacity = 2 })
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 1 }, (order, _) => Task.FromResult(order.Total))
            .To(Sink.Ignore<decimal>())
            .Document;

        Assert.Equal(["from-enumerable", "buffer", "select-async", "ignore"], StageIds(document));
        Assert.Equal(
            [
                "stage-0001#out -> stage-0002#in",
                "stage-0002#out -> stage-0003#in",
                "stage-0003#out -> stage-0004#in",
            ],
            Edges(document));
        Assert.True(GraphCompiler.Validate(document, LocalStageCatalog.Instance).IsValid);
    }

    [Fact]
    public void ADocumentCarryingBoundaryPayloadsSurvivesSerializationByteForByte()
    {
        // The first payloads this authoring surface writes that are not the empty object, and a
        // fingerprint over them is only worth something if the bytes survive a round trip.
        GraphDocument document = Source.From(OrderEvents)
            .Buffer(new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.DropNewest })
            .SelectAsyncUnordered(
                new ParallelismOptions { MaxConcurrency = 3 },
                (order, _) => Task.FromResult(order.Total))
            .To(Sink.Ignore<decimal>())
            .Document;

        byte[] bytes = GraphDocumentSerializer.Serialize(document);
        GraphDocument decoded = GraphDocumentSerializer.Deserialize(bytes);

        Assert.Equal(document, decoded);
        Assert.Equal(bytes, GraphDocumentSerializer.Serialize(decoded));
        Assert.Equal(
            GraphDocumentSerializer.Fingerprint(document),
            GraphDocumentSerializer.Fingerprint(decoded));
        Assert.Equal("""{"capacity":8,"overflowPolicy":"drop-newest"}""", decoded.Nodes[1].Parameters.ToString());
    }

    [Fact]
    public void TwoGraphsDifferingOnlyInCapacityHaveDifferentFingerprints()
    {
        // The whole reason the options are in the document. The two graphs share every delegate, every
        // stage reference, and every edge, and they behave differently, so their identities differ too.
        Func<OrderCreated, bool> shared = order => order.IsValid;

        Assert.NotEqual(Buffered(2, shared).Fingerprint, Buffered(3, shared).Fingerprint);
        Assert.Equal(Buffered(2, shared).Fingerprint, Buffered(2, shared).Fingerprint);
    }

    [Fact]
    public void TwoGraphsDifferingOnlyInOverflowPolicyHaveDifferentFingerprints()
    {
        Assert.NotEqual(
            Policed(OverflowPolicy.Backpressure).Fingerprint,
            Policed(OverflowPolicy.DropOldest).Fingerprint);
        Assert.Equal(Policed(OverflowPolicy.Fail).Fingerprint, Policed(OverflowPolicy.Fail).Fingerprint);
    }

    [Fact]
    public void TwoGraphsDifferingOnlyInMaxConcurrencyHaveDifferentFingerprints()
    {
        Assert.NotEqual(Parallel(1).Fingerprint, Parallel(2).Fingerprint);
        Assert.Equal(Parallel(4).Fingerprint, Parallel(4).Fingerprint);
    }

    [Fact]
    public void TwoGraphsDifferingOnlyInTheDelegatesStillShareAFingerprint()
    {
        // The other half of the same claim, so that it is not overstated: a document records a stage and
        // its parameters, never a delegate, so two buffers of one capacity are one shape whatever the
        // lambdas around them compute. That is exactly why a result slot binds to the built instance as
        // well as to the fingerprint.
        Assert.Equal(
            Buffered(4, order => order.IsValid).Fingerprint,
            Buffered(4, order => order.Total > 1000m).Fingerprint);
    }

    [Fact]
    public void TheBufferOperatorsRejectOptionsThatDescribeNoBuffer()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);
        Flow<OrderCreated, OrderCreated> flow = Flow.For<OrderCreated>();

        Assert.Throws<ArgumentNullException>("options", () => { _ = orders.Buffer(null!); });
        Assert.Throws<ArgumentNullException>("options", () => { _ = flow.Buffer(null!); });

        foreach (int capacity in (int[])[0, -1, int.MinValue])
        {
            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = orders.Buffer(new BufferOptions { Capacity = capacity }); });
            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = flow.Buffer(new BufferOptions { Capacity = capacity }); });
        }

        // An enumeration is not a closed set at run time, and a policy no member declares has no spelling
        // in a document and no behavior in a run.
        Assert.Throws<ArgumentOutOfRangeException>(
            "options",
            () => { _ = orders.Buffer(new BufferOptions { Capacity = 1, OverflowPolicy = (OverflowPolicy)99 }); });
    }

    [Fact]
    public void TheAsynchronousOperatorsRejectOptionsAndCallbacksThatDescribeNoStage()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);
        Flow<OrderCreated, OrderCreated> flow = Flow.For<OrderCreated>();
        ParallelismOptions one = new() { MaxConcurrency = 1 };

        Assert.Throws<ArgumentNullException>(
            "options",
            () => { _ = orders.SelectAsync(null!, (order, _) => Task.FromResult(order.Total)); });
        Assert.Throws<ArgumentNullException>(
            "options",
            () => { _ = orders.SelectAsyncUnordered(null!, (order, _) => Task.FromResult(order.Total)); });
        Assert.Throws<ArgumentNullException>(
            "options",
            () => { _ = flow.SelectAsync(null!, (order, _) => Task.FromResult(order.Total)); });
        Assert.Throws<ArgumentNullException>(
            "options",
            () => { _ = flow.SelectAsyncUnordered(null!, (order, _) => Task.FromResult(order.Total)); });

        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = orders.SelectAsync<decimal>(one, null!); });
        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = orders.SelectAsyncUnordered<decimal>(one, null!); });
        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = flow.SelectAsync<decimal>(one, null!); });
        Assert.Throws<ArgumentNullException>(
            "selector",
            () => { _ = flow.SelectAsyncUnordered<decimal>(one, null!); });

        foreach (int concurrency in (int[])[0, -1, int.MinValue])
        {
            ParallelismOptions options = new() { MaxConcurrency = concurrency };

            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = orders.SelectAsync(options, (order, _) => Task.FromResult(order.Total)); });
            Assert.Throws<ArgumentOutOfRangeException>(
                "options",
                () => { _ = flow.SelectAsyncUnordered(options, (order, _) => Task.FromResult(order.Total)); });
        }
    }

    [Fact]
    public void ARejectedOperatorLeavesTheValueItWasCalledOnUnchanged()
    {
        // The check happens before anything is built, so a rejected call costs the author nothing at all.
        Source<OrderCreated> orders = Source.From(OrderEvents);

        Assert.Throws<ArgumentOutOfRangeException>(
            "options",
            () => { _ = orders.Buffer(new BufferOptions { Capacity = 0 }); });

        Assert.Equal("source (1 stage)", orders.ToString());
        Assert.Equal(2, orders.To(Sink.Ignore<OrderCreated>()).Document.Nodes.Count);
    }

    [Fact]
    public void TheOptionRecordsRenderThemselvesForALogLine()
    {
        Assert.Equal(
            "buffer (capacity 8, drop-oldest)",
            new BufferOptions { Capacity = 8, OverflowPolicy = OverflowPolicy.DropOldest }.ToString());
        Assert.Equal("buffer (capacity 1, backpressure)", new BufferOptions { Capacity = 1 }.ToString());
        Assert.Equal("parallelism (max concurrency 4)", new ParallelismOptions { MaxConcurrency = 4 }.ToString());

        // Never throws, including for values placing a stage would refuse.
        Assert.Equal(
            "buffer (capacity 0, 99)",
            new BufferOptions { Capacity = 0, OverflowPolicy = (OverflowPolicy)99 }.ToString());
    }

    /// <summary>Builds the same graph with one buffer capacity and one predicate.</summary>
    /// <param name="capacity">The buffer capacity.</param>
    /// <param name="predicate">The filter, so that two graphs can differ in their delegates alone.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Buffered(int capacity, Func<OrderCreated, bool> predicate) =>
        Source.From(OrderEvents)
            .Where(predicate)
            .Buffer(new BufferOptions { Capacity = capacity })
            .To(Sink.Ignore<OrderCreated>());

    /// <summary>Builds the same graph under one overflow policy.</summary>
    /// <param name="policy">The policy.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Policed(OverflowPolicy policy) =>
        Source.From(OrderEvents)
            .Buffer(new BufferOptions { Capacity = 4, OverflowPolicy = policy })
            .To(Sink.Ignore<OrderCreated>());

    /// <summary>Builds the same graph under one concurrency bound.</summary>
    /// <param name="concurrency">The bound.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Parallel(int concurrency) =>
        Source.From(OrderEvents)
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = concurrency },
                (order, _) => Task.FromResult(order.Total))
            .To(Sink.Ignore<decimal>());
}
