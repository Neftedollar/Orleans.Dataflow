using System.Globalization;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// How a graph is identified and how a result slot binds to it.
/// </summary>
/// <remarks>
/// ADR 0004 section 4 replaced the identity-and-revision check of ADR 0002 with the document fingerprint,
/// because two anonymous graphs built by the same code share their identity and the old check was therefore
/// vacuous for exactly the common case. These tests fix both halves of the amendment: what the fingerprint
/// covers, and what it deliberately does not.
/// </remarks>
public sealed class FingerprintAndSlotTests
{
    [Fact]
    public void TheGraphsFingerprintIsTheFingerprintOfItsDocument()
    {
        RunnableGraph graph = Counted("processed");

        Assert.Equal(GraphDocumentSerializer.Fingerprint(graph.Document), graph.Fingerprint);
        Assert.Equal(
            GraphFingerprint.OfSerialized(GraphDocumentSerializer.Serialize(graph.Document)),
            graph.Fingerprint);
    }

    [Fact]
    public void TheRepresentativeLambdaGraphHasThePinnedFingerprint()
    {
        // Every other fingerprint claim in this suite is relative: two graphs agree, or they differ. That
        // catches a change of shape and cannot catch a change of encoding — a different capability set, a
        // different numbering, a different port name would move every authored fingerprint at once and
        // every relative assertion would still pass. This is the absolute pin, and it is meant to be
        // updated deliberately rather than silently: a slot binds to this value, so moving it invalidates
        // every slot any caller is holding across the change.
        Assert.Equal(
            "sha256:3e947c06b0c47f3380e0f79ab896a46ab2d0e1b4e3679edeecd2016f98cd71b6",
            Counted("processed").Fingerprint.ToString());
    }

    [Fact]
    public void ADocumentRoundTripsThroughItsCanonicalBytes()
    {
        RunnableGraph graph = Counted("processed");

        byte[] bytes = GraphDocumentSerializer.Serialize(graph.Document);
        GraphDocument restored = GraphDocumentSerializer.Deserialize(bytes);

        Assert.Equal(graph.Document, restored);
        Assert.Equal(bytes, GraphDocumentSerializer.Serialize(restored));
        Assert.Equal(graph.Fingerprint, GraphDocumentSerializer.Fingerprint(restored));
    }

    [Fact]
    public void ASlotIsBoundToTheFingerprintOfTheGraphThatDeclaredIt()
    {
        RunnableGraph graph = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);

        Assert.Equal(graph.Fingerprint, processed.Graph);
        Assert.Equal("processed", processed.Id.Value);
        Assert.False(processed.IsDefault);
    }

    [Fact]
    public void TwoContentIdenticalGraphsShareAFingerprintButNotSlots()
    {
        // The fingerprint identifies shape, and the fingerprints of these two graphs are equal by design.
        // A slot of a nondeployable graph additionally binds to the built instance's authoring nonce,
        // because a lambda graph's document never records what its delegates compute: two graphs that
        // merely look the same must not resolve each other's results.
        (RunnableGraph Graph, ResultSlot<long> Slot) first = Build();
        (RunnableGraph Graph, ResultSlot<long> Slot) second = Build();

        Assert.NotSame(first.Graph, second.Graph);
        Assert.Equal(first.Graph.Fingerprint, second.Graph.Fingerprint);
        Assert.NotEqual(first.Slot, second.Slot);
        Assert.Equal(first.Slot.Id, second.Slot.Id);
        Assert.Equal(first.Slot.Graph, second.Slot.Graph);
        Assert.NotEqual(first.Slot.AuthoringNonce, second.Slot.AuthoringNonce);
        Assert.Equal(first.Graph.AuthoringNonce, first.Slot.AuthoringNonce);

        static (RunnableGraph Graph, ResultSlot<long> Slot) Build() =>
            Source.From(OrderEvents).To(s => s.Aggregate(0L, (count, _) => count + 1), "processed");
    }

    [Fact]
    public void AddingOneStageChangesTheFingerprint()
    {
        RunnableGraph without = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> withoutSlot);

        RunnableGraph with = Source.From(OrderEvents)
            .Where(order => order.IsValid)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> withSlot);

        Assert.NotEqual(without.Fingerprint, with.Fingerprint);
        Assert.NotEqual(withoutSlot, withSlot);
        Assert.Equal(withoutSlot.Id, withSlot.Id);
    }

    [Fact]
    public void RenamingASlotChangesTheFingerprint()
    {
        // A slot name is document content, not a label beside the document, so two graphs that differ only
        // in it are two different graphs. That is what makes a slot resolvable against exactly the runs of
        // the graph that declared it.
        Assert.NotEqual(Counted("processed").Fingerprint, Counted("handled").Fingerprint);
    }

    [Fact]
    public void TwoGraphsOfTheSameShapeShareAFingerprintButTheNonceKeepsTheirSlotsApart()
    {
        // The limit of the fingerprint, pinned rather than left to be discovered: a local document records
        // the shape of a graph and never the behavior of its delegates, so these two graphs are
        // byte-identical even though one counts and the other sums. The authoring nonce is what closes the
        // gap for slots — resolving the counting slot against the summing graph's run must fail loudly,
        // and the inequality below is the compile-side half of that guarantee.
        (RunnableGraph Graph, ResultSlot<long> Slot) counting =
            Source.From<long>([1L, 2L, 3L]).To(s => s.Aggregate(0L, (count, _) => count + 1), "value");

        (RunnableGraph Graph, ResultSlot<long> Slot) summing =
            Source.From<long>([10L, 20L]).To(s => s.Aggregate(0L, (sum, element) => sum + element), "value");

        Assert.Equal(counting.Graph.Fingerprint, summing.Graph.Fingerprint);
        Assert.NotEqual(counting.Slot, summing.Slot);
    }

    [Fact]
    public void TheSameSinkAttachedTwiceUnderTwoNamesDeclaresTwoDistinctSlots()
    {
        SinkWithResult<OrderCreated, long> counting =
            Sink.Aggregate<OrderCreated, long>(0L, (count, _) => count + 1);

        Source<OrderCreated> orders = Source.From(OrderEvents);

        (RunnableGraph Graph, ResultSlot<long> Slot) first = orders.To(counting, "first");
        (RunnableGraph Graph, ResultSlot<long> Slot) second = orders.To(counting, "second");

        Assert.NotEqual(first.Slot.Id, second.Slot.Id);
        Assert.NotEqual(first.Slot, second.Slot);

        // The two graphs differ, because the slot name they declare is part of the document. A slot belongs
        // to the document that declared it, never to the sink value it came from.
        Assert.NotEqual(first.Graph.Fingerprint, second.Graph.Fingerprint);
        Assert.Equal(first.Graph.Fingerprint, first.Slot.Graph);
        Assert.Equal(second.Graph.Fingerprint, second.Slot.Graph);

        // And the sink is unchanged by either attachment.
        Assert.Equal(first.Graph.Fingerprint, orders.To(counting, "first").Graph.Fingerprint);
    }

    [Fact]
    public void SlotsWithTheSameNameInDifferentlyShapedGraphsAreNotEqual()
    {
        RunnableGraph shortGraph = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> shortSlot);

        RunnableGraph longGraph = Source.From(OrderEvents)
            .Where(order => order.IsValid)
            .Select(OrderDocument.FromEvent)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> longSlot);

        Assert.NotEqual(shortGraph.Fingerprint, longGraph.Fingerprint);
        Assert.False(shortSlot == longSlot);
        Assert.True(shortSlot != longSlot);
    }

    [Fact]
    public void ADefaultSlotNamesNothingAndSaysSo()
    {
        ResultSlot<long> slot = default;

        Assert.True(slot.IsDefault);
        Assert.Equal("(default ResultSlot)", slot.ToString());
        Assert.Throws<InvalidOperationException>(() => { _ = slot.Id; });
        Assert.Throws<InvalidOperationException>(() => { _ = slot.Graph; });
    }

    [Fact]
    public void ASlotRendersAllThreeComponentsItIsEqualBy()
    {
        RunnableGraph graph = Source.From(OrderEvents)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), "processed", out ResultSlot<long> processed);

        string nonceDigits = graph.AuthoringNonce.ToString("N", CultureInfo.InvariantCulture)[..8];

        Assert.Equal($"processed@{graph.Fingerprint}#{nonceDigits}", processed.ToString());
        Assert.StartsWith("graph sha256:", graph.ToString(), StringComparison.Ordinal);
        Assert.EndsWith("(2 nodes, 1 result slot)", graph.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TwoSlotsOfLookAlikeGraphsRenderDifferentlyBecauseTheNonceIsPartOfTheText()
    {
        // A text form that showed only the name and the fingerprint would print one line for two slots
        // that are not equal, which is the confusion the nonce exists to prevent, reappearing in the logs.
        (RunnableGraph Graph, ResultSlot<long> Slot) first = Build();
        (RunnableGraph Graph, ResultSlot<long> Slot) second = Build();

        Assert.Equal(first.Graph.Fingerprint, second.Graph.Fingerprint);
        Assert.NotEqual(first.Slot.ToString(), second.Slot.ToString());
        Assert.StartsWith($"processed@{first.Graph.Fingerprint}#", first.Slot.ToString(), StringComparison.Ordinal);

        static (RunnableGraph Graph, ResultSlot<long> Slot) Build() =>
            Source.From(OrderEvents).To(s => s.Aggregate(0L, (count, _) => count + 1), "processed");
    }

    /// <summary>Builds the representative counting graph under one slot name.</summary>
    /// <param name="slotName">The name to expose the fold's result under.</param>
    /// <returns>The closed graph.</returns>
    private static RunnableGraph Counted(string slotName) =>
        Source.From(OrderEvents)
            .Select(OrderDocument.FromEvent)
            .Where(order => order.Total > 5m)
            .To(s => s.Aggregate(0L, (count, _) => count + 1), slotName, out ResultSlot<long> _);
}
