using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Xunit;
using static Orleans.Dataflow.Tests.Authoring.FragmentFixtures;

namespace Orleans.Dataflow.Tests.Authoring;

/// <summary>
/// Tests for <see cref="GraphFragmentComposer.Wire"/>, the operator whose edge has both ends in one
/// fragment.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="GraphFragmentComposer.Connect"/> merges two fragments and therefore cannot express an edge
/// inside one. That is not a gap in what documents are legal — a diamond and a cycle are both documents the
/// engine runs — so it was a gap in the algebra, and this is the operator that closes it. Everything else
/// about it is <c>Connect</c>'s rules read for one fragment instead of two: both ports open, both consumed,
/// the rest of the boundary in its order.
/// </para>
/// <para>
/// The fixtures are the algebra's own: nodes with no meaning beyond their identity, because no rule here
/// depends on what a stage is.
/// </para>
/// </remarks>
public sealed class GraphFragmentWireTests
{
    [Fact]
    public void WireClosesADiamondThatConnectCannotReach()
    {
        // One node splits into two and the two meet again. Every edge but the last joins two fragments; the
        // last one has both ends inside the fragment the others built, which is the whole reason this
        // operator exists.
        GraphFragment split = GraphFragment.Create(
            [Node("split")],
            [],
            [Port("split", "in")],
            [Port("split", "out-0"), Port("split", "out-1")]);

        GraphFragment join = GraphFragment.Create(
            [Node("join")],
            [],
            [Port("join", "in-0"), Port("join", "in-1")],
            [Port("join", "out")]);

        GraphFragment first = GraphFragmentComposer.Connect(split, Port("split", "out-0"), join, Port("join", "in-0"));
        GraphFragment diamond = GraphFragmentComposer.Wire(first, Port("split", "out-1"), Port("join", "in-1"));

        Assert.Equal(["join", "split"], NodeIds(diamond));
        Assert.Equal(
            ["split#out-0 -> join#in-0", "split#out-1 -> join#in-1"],
            EdgeTexts(diamond));
        Assert.Equal([Port("split", "in")], diamond.OpenInputs);
        Assert.Equal([Port("join", "out")], diamond.OpenOutputs);
    }

    [Fact]
    public void WireBuildsTheCycleTheComposerCouldNotBuildBefore()
    {
        // ADR 0006 sends a loop to the fragment algebra, "where edges are explicit". This is that sentence
        // being true: an output wired back to an input the stream already passed through is a cycle, and
        // nothing here objects, because whether an edge runs forwards is the document's to say and the
        // planner's to read.
        GraphFragment chain = GraphFragmentComposer.Append(Flow("a"), Flow("b"));
        GraphFragment cycle = GraphFragmentComposer.Wire(chain, Port("b", "out"), Port("a", "in"));

        Assert.Equal(["a#out -> b#in", "b#out -> a#in"], EdgeTexts(cycle));
        Assert.Empty(cycle.OpenInputs);
        Assert.Empty(cycle.OpenOutputs);
    }

    [Fact]
    public void WireKeepsTheRestOfTheBoundaryInItsOrder()
    {
        GraphFragment junction = GraphFragment.Create(
            [Node("a"), Node("b")],
            [],
            [Port("a", "in1"), Port("b", "in2")],
            [Port("a", "out1"), Port("b", "out2")]);

        GraphFragment wired = GraphFragmentComposer.Wire(junction, Port("a", "out1"), Port("b", "in2"));

        Assert.Equal([Port("a", "in1")], wired.OpenInputs);
        Assert.Equal([Port("b", "out2")], wired.OpenOutputs);
        Assert.Equal(["a#out1 -> b#in2"], EdgeTexts(wired));
    }

    [Fact]
    public void WireDoesNotModifyTheFragmentItReads()
    {
        GraphFragment chain = GraphFragmentComposer.Append(Flow("a"), Flow("b"));

        _ = GraphFragmentComposer.Wire(chain, Port("b", "out"), Port("a", "in"));

        Assert.Equal(["a#out -> b#in"], EdgeTexts(chain));
        Assert.Equal([Port("a", "in")], chain.OpenInputs);
        Assert.Equal([Port("b", "out")], chain.OpenOutputs);
    }

    [Fact]
    public void WireRejectsAPortThatIsNotOpenOnTheSideItWasGivenFor()
    {
        GraphFragment chain = GraphFragmentComposer.Append(Flow("a"), Flow("b"));

        ArgumentException output = Assert.Throws<ArgumentException>(
            () => GraphFragmentComposer.Wire(chain, Port("a", "out"), Port("a", "in")));
        ArgumentException input = Assert.Throws<ArgumentException>(
            () => GraphFragmentComposer.Wire(chain, Port("b", "out"), Port("b", "in")));

        Assert.Equal("output", output.ParamName);
        Assert.Equal("input", input.ParamName);
        Assert.Contains("only an open port can be connected", output.Message, StringComparison.Ordinal);
        Assert.Contains("'b#in' is not an open input", input.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WireBuildsASelfLoopForTheCycleRuleToJudge()
    {
        // ADR 0005 subsumed the old self-loop refusal into the cycle rule, so the algebra builds the edge
        // and the planner judges the loop where it judges every loop. The fragment's part of the contract
        // is bookkeeping: the edge exists and both ports stopped being open.
        GraphFragment flow = Flow("a");

        GraphFragment looped = GraphFragmentComposer.Wire(flow, Port("a", "out"), Port("a", "in"));

        Assert.Contains(looped.Edges, edge => edge.From.Node == edge.To.Node);
        Assert.DoesNotContain(Port("a", "out"), looped.OpenOutputs);
        Assert.DoesNotContain(Port("a", "in"), looped.OpenInputs);
    }

    [Fact]
    public void WireRejectsANullFragment()
    {
        ArgumentNullException rejected = Assert.Throws<ArgumentNullException>(
            () => GraphFragmentComposer.Wire(null!, Port("a", "out"), Port("b", "in")));

        Assert.Equal("fragment", rejected.ParamName);
    }
}
