using Orleans.Dataflow.Compilation;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The flagship example of C-SHARP-API.md, compiled as written.
/// </summary>
/// <remarks>
/// <para>
/// The graph-construction half of the example is pasted verbatim; the two host lines that follow it in the
/// document are omitted because no runtime exists yet. If a rename or a signature change ever makes the
/// documented example stop compiling, this file stops compiling with it, which is the point: the example is
/// a contract, not an illustration.
/// </para>
/// <para>
/// The counter-example the document keeps deliberately — <c>To(Sink.Aggregate(0L, (count, _) =&gt; count + 1),
/// ...)</c>, which fails with <c>CS0411</c> — cannot appear here, because a test that does not compile
/// cannot live in a passing suite. Its evidence is the ADR 0004 compile prototypes.
/// </para>
/// </remarks>
public sealed class FlagshipExampleTests
{
    [Fact]
    public void TheFlagshipExampleCompilesAsWritten()
    {
        IEnumerable<OrderCreated> orderEvents = OrderEvents;

        // Begin verbatim quotation of docs/design/C-SHARP-API.md.
        Source<OrderCreated> orders = Source.From(orderEvents);

        Flow<OrderCreated, OrderDocument> normalize =
            Flow.For<OrderCreated>()
                .Where(order => order.IsValid)
                .Select(OrderDocument.FromEvent);

        RunnableGraph graph = orders
            .Via(normalize)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);
        // End verbatim quotation.

        Assert.Equal(["stage-0001", "stage-0002", "stage-0003", "stage-0004"], NodeIds(graph.Document));
        Assert.Equal(["from-enumerable", "where", "select", "fold"], StageIds(graph.Document));
        Assert.Equal(["processed"], graph.ResultSlots.Select(slot => slot.Value));
        Assert.Equal("processed", processed.Id.Value);
        Assert.Equal(graph.Fingerprint, processed.Graph);
    }

    [Fact]
    public void TheFlagshipExampleGraphValidatesAgainstTheLocalCatalog()
    {
        Source<OrderCreated> orders = Source.From(OrderEvents);

        Flow<OrderCreated, OrderDocument> normalize =
            Flow.For<OrderCreated>()
                .Where(order => order.IsValid)
                .Select(OrderDocument.FromEvent);

        RunnableGraph graph = orders
            .Via(normalize)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        GraphValidationReport report = GraphCompiler.Validate(graph.Document, LocalStageCatalog.Instance);

        Assert.True(report.IsValid, report.ToString());
    }

    [Fact]
    public void TheReuseExampleClosesTwoIndependentGraphs()
    {
        // The second example of C-SHARP-API.md: one flow, two graphs. The sinks differ from the document's
        // because the registered adapters it names do not exist yet; the shape of the claim does not.
        Source<OrderCreated> orders = Source.From(OrderEvents);

        Flow<OrderCreated, OrderDocument> normalize =
            Flow.For<OrderCreated>()
                .Where(order => order.IsValid)
                .Select(OrderDocument.FromEvent);

        RunnableGraph toSearchIndex = orders.Via(normalize).To(Sink.Ignore<OrderDocument>());
        RunnableGraph toArchive = orders
            .Via(normalize)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "archived", out ResultSlot<long> _);

        Assert.NotSame(toSearchIndex.Document, toArchive.Document);
        Assert.Equal(["from-enumerable", "where", "select", "ignore"], StageIds(toSearchIndex.Document));
        Assert.Equal(["from-enumerable", "where", "select", "fold"], StageIds(toArchive.Document));
        Assert.Empty(toSearchIndex.ResultSlots);
        Assert.Equal(["archived"], toArchive.ResultSlots.Select(slot => slot.Value));

        // The flow itself is untouched by either closure: composing it a third time still describes the
        // same two occurrences it always did.
        Assert.Equal(
            ["from-enumerable", "where", "select", "ignore"],
            StageIds(orders.Via(normalize).To(Sink.Ignore<OrderDocument>()).Document));
    }

    [Fact]
    public void AnAsyncMethodCanCarryTheTupleFormOutOfItsBody()
    {
        // ADR 0004 section 3: the out form is banned in async signatures (CS1988), which is why the tuple
        // form exists beside it. This is the reachable half of that argument.
        (RunnableGraph Graph, ResultSlot<long> Slot) closed = Build();

        Assert.Equal(["counted"], closed.Graph.ResultSlots.Select(slot => slot.Value));
        Assert.Equal(closed.Graph.Fingerprint, closed.Slot.Graph);

        static (RunnableGraph Graph, ResultSlot<long> Slot) Build() =>
            Source.From(OrderEvents).To(s => s.Aggregate(0L, (count, _) => count + 1), "counted");
    }
}
