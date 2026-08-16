using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The split between what a local graph writes down and what it merely carries.
/// </summary>
/// <remarks>
/// <para>
/// AGENTS.md forbids serializing delegates, closures, or captured state as durable topology, and ADR 0004
/// section 6 says where they live instead: in the authoring value, bound at local materialization, never in
/// the document. That makes the binding table the one piece of a closed graph no public API can observe,
/// which is why the test assembly is granted access to it — an unobservable table is otherwise an untested
/// one.
/// </para>
/// <para>
/// The tests state both halves of the split: every delegate the author supplied is there, keyed by the
/// identifier the document declares, and nothing the author supplied changes the document.
/// </para>
/// </remarks>
public sealed class BindingTableTests
{
    [Fact]
    public void EveryOccurrenceBindsExactlyTheDelegateTheAuthorSupplied()
    {
        IEnumerable<OrderCreated> elements = OrderEvents;
        Func<OrderCreated, bool> predicate = order => order.IsValid;
        Func<OrderCreated, OrderDocument> selector = OrderDocument.FromEvent;
        Func<long, OrderDocument, long> folder = (count, _) => count + 1;

        RunnableGraph graph = Source.From(elements)
            .Where(predicate)
            .Select(selector)
            .To(Sink.Aggregate(7L, folder), "processed", out ResultSlot<long> _);

        Assert.Same(elements, Binding(graph, "stage-1").Behavior);
        Assert.Same(predicate, Binding(graph, "stage-2").Behavior);
        Assert.Same(selector, Binding(graph, "stage-3").Behavior);
        Assert.Same(folder, Binding(graph, "stage-4").Behavior);

        Assert.Equal(LocalStageKind.FromEnumerable, Binding(graph, "stage-1").Kind);
        Assert.Equal(LocalStageKind.Where, Binding(graph, "stage-2").Kind);
        Assert.Equal(LocalStageKind.Select, Binding(graph, "stage-3").Kind);
        Assert.Equal(LocalStageKind.Fold, Binding(graph, "stage-4").Kind);
    }

    [Fact]
    public void TheFoldSeedIsBoundBesideTheDocumentAndNeverInsideIt()
    {
        RunnableGraph seeded = Source.From(OrderEvents)
            .To(s => s.Aggregate(41L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        RunnableGraph unseeded = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> _);

        Assert.Equal(41L, Assert.IsType<long>(Binding(seeded, "stage-2").Seed));
        Assert.Equal(0L, Assert.IsType<long>(Binding(unseeded, "stage-2").Seed));

        // A runtime value is not topology, so the two graphs are the same document.
        Assert.Equal(seeded.Fingerprint, unseeded.Fingerprint);
    }

    [Fact]
    public void TheBindingTableIsKeyedByExactlyTheDocumentsNodeIds()
    {
        RunnableGraph graph = Source.From(OrderEvents)
            .Where(order => order.IsValid)
            .Select(OrderDocument.FromEvent)
            .To(Sink.Ignore<OrderDocument>());

        Assert.Equal(
            graph.Document.Nodes.Select(node => node.Id.Value).Order(StringComparer.Ordinal),
            graph.LocalBindings.Keys.Select(id => id.Value).Order(StringComparer.Ordinal));
    }

    [Fact]
    public void ADiscardingSinkBindsNoBehaviorAtAll()
    {
        RunnableGraph graph = Source.From(OrderEvents).To(Sink.Ignore<OrderCreated>());

        LocalStageDescriptor ignore = Binding(graph, "stage-2");

        Assert.Equal(LocalStageKind.Ignore, ignore.Kind);
        Assert.Null(ignore.Behavior);
        Assert.Null(ignore.Seed);
        Assert.Equal("local/ignore@v1", ignore.ToString());
    }

    [Fact]
    public void OneFlowUsedTwiceBindsTwoOccurrencesToTheSameDescription()
    {
        // The two occurrences are distinct because their node identifiers are, not because their
        // descriptions are: a reusable value contributes the same immutable description at both positions,
        // which is exactly what makes reuse cheap and safe.
        Func<OrderCreated, bool> predicate = order => order.IsValid;
        Flow<OrderCreated, OrderCreated> valid = Flow.For<OrderCreated>().Where(predicate);

        RunnableGraph graph = Source.From(OrderEvents).Via(valid).Via(valid).To(Sink.Ignore<OrderCreated>());

        Assert.Same(predicate, Binding(graph, "stage-2").Behavior);
        Assert.Same(predicate, Binding(graph, "stage-3").Behavior);
        Assert.Same(Binding(graph, "stage-2"), Binding(graph, "stage-3"));
        Assert.Equal(4, graph.LocalBindings.Count);
    }

    /// <summary>Reads the binding of one node of a closed graph.</summary>
    /// <param name="graph">The graph.</param>
    /// <param name="nodeId">The node identifier text.</param>
    /// <returns>The bound occurrence.</returns>
    private static LocalStageDescriptor Binding(RunnableGraph graph, string nodeId) =>
        graph.LocalBindings[NodeId.Create(nodeId)];
}
