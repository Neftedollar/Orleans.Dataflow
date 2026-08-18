using Xunit;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The one slot that exists before its graph does, and the rules that keep it honest.
/// </summary>
/// <remarks>
/// <para>
/// A branch names its result where its sink is written, which is one expression before the junction call
/// that closes the graph. Every other slot is handed back by the call that closed a graph and has an
/// identity from the moment it exists; a branch slot has one from the junction call onwards, and this file
/// is what that sentence means in practice.
/// </para>
/// <para>
/// The window is deliberately narrow — a branch is written as an argument of the call that consumes it — so
/// the tests below reach it the only way an author could: by storing the branch in a variable first.
/// </para>
/// </remarks>
public sealed class BranchSlotTests
{
    [Fact]
    public void ABranchSlotNamesItsGraphFromTheJunctionCallOnwards()
    {
        Branch<int> counting = Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> counted);

        RunnableGraph graph = Source.From<int>([1, 2, 3]).BroadcastTo(counting, Flow.For<int>().To(Sink.Ignore<int>()));

        Assert.Equal("counted", counted.Id.Value);
        Assert.Equal(graph.Fingerprint, counted.Graph);
        Assert.Equal(graph.AuthoringNonce, counted.AuthoringNonce);
        Assert.False(counted.IsDefault);
    }

    [Fact]
    public void ABranchSlotOfAnUnclosedBranchNamesNoGraph()
    {
        // Not an empty answer and not a fingerprint of nothing: reading a slot whose graph does not exist
        // yet is a mistake, and it says which mistake it is.
        _ = Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> counted);

        InvalidOperationException rejected = Assert.Throws<InvalidOperationException>(() => counted.Graph);

        Assert.Contains("has not been closed yet", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("BroadcastTo", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnclosedBranchSlotStillRendersAndCompares()
    {
        // Neither operation may fail in any state a value can be in, which for this type includes the state
        // where its graph does not exist. The text says which state it is rather than pretending to a
        // fingerprint.
        _ = Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> first);
        _ = Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> second);

        Assert.Equal("counted@(unclosed branch)", first.ToString());
        Assert.Equal(first, second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
    }

    [Fact]
    public void ABranchThatDeclaresAResultClosesExactlyOneGraph()
    {
        // The rule the nonce exists to protect, applied one step earlier. A slot binds to the graph that
        // declared it; a branch handed to a second junction call would leave the first graph's slot pointing
        // at the second graph, so the second call is refused instead.
        Branch<int> counting = Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> counted);

        RunnableGraph first = Source.From<int>([1]).BroadcastTo(counting, Flow.For<int>().To(Sink.Ignore<int>()));

        InvalidOperationException rejected = Assert.Throws<InvalidOperationException>(
            () => Source.From<int>([2]).BroadcastTo(counting, Flow.For<int>().To(Sink.Ignore<int>())));

        Assert.Contains("closes exactly one graph", rejected.Message, StringComparison.Ordinal);
        Assert.Equal(first.Fingerprint, counted.Graph);
    }

    [Fact]
    public void ABranchThatDeclaresNoResultIsReusableWithoutLimit()
    {
        // The other half of the same rule, and the common case: nothing binds, so nothing can be taken over.
        Branch<int> discard = Flow.For<int>().To(Sink.Ignore<int>());

        RunnableGraph first = Source.From<int>([1]).BroadcastTo(discard, discard);
        RunnableGraph second = Source.From<int>([2, 3]).BalanceTo(discard, discard);

        Assert.NotEqual(first.Fingerprint, second.Fingerprint);
        Assert.Equal(4, first.Document.Nodes.Count);
    }

    [Fact]
    public void AGraphThatFailsToCloseLeavesItsBranchSlotsUnbound()
    {
        // Binding happens after the document exists, so a rejected build costs the author nothing: the
        // branches are exactly as they were and can be handed to a call that does close.
        Branch<int> counting = Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> counted);
        Branch<int> colliding = Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> _);

        _ = Assert.Throws<ArgumentException>(() => Source.From<int>([1]).BroadcastTo(counting, colliding));

        RunnableGraph recovered = Source.From<int>([1]).BroadcastTo(counting, Flow.For<int>().To(Sink.Ignore<int>()));

        Assert.Equal(recovered.Fingerprint, counted.Graph);
    }

    [Fact]
    public void TwoBranchSlotsOfTwoGraphsAreNotEqualEvenUnderOneName()
    {
        // What the authoring nonce buys, stated for branch slots: two graphs of one shape share a
        // fingerprint because a document records no delegate, and their slots still do not resolve each
        // other's results.
        RunnableGraph first = Source.From<int>([1]).BroadcastTo(
            Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> fromFirst),
            Flow.For<int>().To(Sink.Ignore<int>()));

        RunnableGraph second = Source.From<int>([1]).BroadcastTo(
            Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> fromSecond),
            Flow.For<int>().To(Sink.Ignore<int>()));

        Assert.Equal(first.Fingerprint, second.Fingerprint);
        Assert.Equal(fromFirst.Id, fromSecond.Id);
        Assert.Equal(fromFirst.Graph, fromSecond.Graph);
        Assert.NotEqual(fromFirst, fromSecond);
    }

    [Fact]
    public void ATapCarriesItsBranchSlotUntilTheGraphIsClosed()
    {
        // A tap consumes a branch and returns a source, so a result-bearing tap names its slot long before
        // anything closes the graph. The slot is declared beside whatever the main line declares.
        RunnableGraph graph = Source.From<int>([1, 2, 3])
            .AlsoTo(Flow.For<int>().To(s => s.Count(), "tapped", out ResultSlot<long> tapped))
            .Where(value => value > 1)
            .To(s => s.Count(), "kept", out ResultSlot<long> kept);

        Assert.Equal(["kept", "tapped"], graph.ResultSlots.Select(slot => slot.Value).Order(StringComparer.Ordinal));
        Assert.Equal(graph.Fingerprint, tapped.Graph);
        Assert.Equal(graph.Fingerprint, kept.Graph);
        Assert.Equal(tapped.AuthoringNonce, kept.AuthoringNonce);
    }

    [Fact]
    public void ABranchSlotSurvivesTheSlotNameRulesOfEveryOtherSlot()
    {
        // A branch name is a ResultSlotId like every other, validated by the same grammar and before the
        // sink factory lambda runs, so a rejected name never costs the author a side effect.
        bool invoked = false;

        _ = Assert.Throws<ArgumentException>(
            () => Flow.For<int>().To(
                s =>
                {
                    invoked = true;

                    return s.Count();
                },
                "Not A Slot Name",
                out ResultSlot<long> _));

        Assert.False(invoked);
        _ = Assert.Throws<ArgumentNullException>(
            () => Flow.For<int>().To(s => s.Count(), null!, out ResultSlot<long> _));
    }
}
