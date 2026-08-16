using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// Authoring values are immutable and reusable, and composing one never disturbs it.
/// </summary>
/// <remarks>
/// <para>
/// This is the property AGENTS.md states as a product boundary and the one that decides whether a flow can
/// be shared across a codebase. The tests are written as before-and-after comparisons of what a value
/// builds, rather than as inspections of what a value holds: a value that still builds byte-identical
/// documents after being composed into three graphs was not modified by any of them.
/// </para>
/// <para>
/// Two graphs built from the same reusable flow number their occurrences from <c>stage-1</c> independently,
/// so their node identifiers overlap. They are different documents and nothing relates them, so that is not
/// a collision; the fragment algebra's import scoping exists for the case where two copies of one fragment
/// meet inside a single graph, which flat numbering at closure never produces.
/// </para>
/// </remarks>
public sealed class ReuseAndImmutabilityTests
{
    [Fact]
    public void OneFlowInTwoGraphsProducesTwoIndependentDocuments()
    {
        Flow<OrderCreated, OrderDocument> normalize =
            Flow.For<OrderCreated>().Where(order => order.IsValid).Select(OrderDocument.FromEvent);

        Source<OrderCreated> orders = Source.From(OrderEvents);

        RunnableGraph discarded = orders.Via(normalize).To(Sink.Ignore<OrderDocument>());
        RunnableGraph counted = orders
            .Via(normalize)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        Assert.NotSame(discarded.Document, counted.Document);
        Assert.NotEqual(discarded.Fingerprint, counted.Fingerprint);

        // The node identifiers overlap across the two documents, which is what flat numbering means. The
        // two documents are unrelated, so nothing is ambiguous about it.
        Assert.Equal(["stage-1", "stage-2", "stage-3", "stage-4"], NodeIds(discarded.Document));
        Assert.Equal(["stage-1", "stage-2", "stage-3", "stage-4"], NodeIds(counted.Document));
    }

    [Fact]
    public void ComposingASourceLeavesTheSourceUnchanged()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);

        byte[] before = GraphDocumentSerializer.Serialize(orders.To(Sink.Ignore<OrderCreated>()).Document);

        _ = orders.Where(order => order.IsValid);
        _ = orders.Select(OrderDocument.FromEvent);
        _ = orders.Via(Flow.For<OrderCreated>().Where(order => order.IsValid));
        _ = orders.To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        byte[] after = GraphDocumentSerializer.Serialize(orders.To(Sink.Ignore<OrderCreated>()).Document);

        Assert.Equal(before, after);
    }

    [Fact]
    public void ComposingAFlowLeavesTheFlowUnchanged()
    {
        Flow<OrderCreated, OrderCreated> valid = Flow.For<OrderCreated>().Where(order => order.IsValid);

        byte[] before = Bytes(valid);

        _ = valid.Select(OrderDocument.FromEvent);
        _ = valid.Where(order => order.Total > 5m);
        _ = valid.Via(Flow.For<OrderCreated>().Where(order => order.Total > 5m));

        Assert.Equal(before, Bytes(valid));

        static byte[] Bytes(Flow<OrderCreated, OrderCreated> flow) =>
            GraphDocumentSerializer.Serialize(
                Source.From(OrderEvents).Via(flow).To(Sink.Ignore<OrderCreated>()).Document);
    }

    [Fact]
    public void ComposingASinkLeavesTheSinkUnchanged()
    {
        SinkWithResult<OrderCreated, long> counting =
            Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1);

        GraphFingerprint before = Source.From(OrderEvents).To(counting, "processed").Graph.Fingerprint;

        _ = counting.ToSink();
        _ = Source.From(OrderEvents).To(counting, "other");

        Assert.Equal(before, Source.From(OrderEvents).To(counting, "processed").Graph.Fingerprint);
    }

    [Fact]
    public void OneFlowUsedTwiceInOneGraphContributesTwoDistinctOccurrences()
    {
        // Composing the same reusable value twice into one graph is the case that would collide if node
        // identifiers were allocated when a value is created instead of when a graph is closed.
        Flow<OrderCreated, OrderCreated> valid = Flow.For<OrderCreated>().Where(order => order.IsValid);

        RunnableGraph graph = Source.From(OrderEvents)
            .Via(valid)
            .Via(valid)
            .To(Sink.Ignore<OrderCreated>());

        Assert.Equal(["stage-1", "stage-2", "stage-3", "stage-4"], NodeIds(graph.Document));
        Assert.Equal(["from-enumerable", "where", "where", "ignore"], StageIds(graph.Document));
        Assert.Equal(
            [
                "stage-1#out -> stage-2#in",
                "stage-2#out -> stage-3#in",
                "stage-3#out -> stage-4#in",
            ],
            Edges(graph.Document));
    }

    [Fact]
    public void OneSourceHeadsAnyNumberOfGraphs()
    {
        Source<OrderDocument> head = Source.From(OrderEvents).Select(OrderDocument.FromEvent);

        RunnableGraph[] graphs =
        [
            head.To(Sink.Ignore<OrderDocument>()),
            head.Where(order => order.Total > 5m).To(Sink.Ignore<OrderDocument>()),
            head.To(s => s.Aggregate(0m, (total, order) => total + order.Total), "total", out ResultSlot<decimal> _),
        ];

        Assert.Equal(3, graphs.Distinct().Count());
        Assert.Equal(3, graphs.Select(graph => graph.Fingerprint).Distinct().Count());
    }

    [Fact]
    public void TheSequenceASourceReadsIsNeverEnumeratedWhileAGraphIsBuilt()
    {
        // Building a graph starts no work, and that has to include not touching the author's sequence.
        CountingSequence elements = new();

        _ = Source.From(elements)
            .Where(value => value > 0)
            .Select(value => value * 2)
            .To(s => s.Aggregate(0, (sum, value) => sum + value), "total", out ResultSlot<int> _);

        Assert.Equal(0, elements.EnumerationCount);
    }

    /// <summary>A sequence that records how often it was enumerated.</summary>
    private sealed class CountingSequence : IEnumerable<int>
    {
        /// <summary>Gets the number of enumerators handed out so far.</summary>
        internal int EnumerationCount { get; private set; }

        /// <inheritdoc/>
        public IEnumerator<int> GetEnumerator()
        {
            EnumerationCount++;

            return Enumerable.Empty<int>().GetEnumerator();
        }

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
