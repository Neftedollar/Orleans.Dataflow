using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Xunit;
using static Orleans.Dataflow.Tests.Authoring.FragmentFixtures;

namespace Orleans.Dataflow.Tests.Authoring;

/// <summary>
/// Tests for <see cref="GraphFragmentComposer.Connect"/> and <see cref="GraphFragmentComposer.Append"/>.
/// </summary>
public sealed class GraphFragmentConnectTests
{
    [Fact]
    public void ConnectJoinsTwoLinearFragments()
    {
        GraphFragment connected = GraphFragmentComposer.Connect(
            Source("reader"),
            Port("reader", "out"),
            Sink("writer"),
            Port("writer", "in"));

        Assert.Equal(["reader", "writer"], NodeIds(connected));
        Assert.Equal(["reader#out -> writer#in"], EdgeTexts(connected));
        Assert.Empty(connected.OpenInputs);
        Assert.Empty(connected.OpenOutputs);
    }

    [Fact]
    public void ConnectKeepsTheEdgesOfBothFragmentsAndAddsExactlyOne()
    {
        GraphFragment upstream = GraphFragmentComposer.Append(Source("reader"), Flow("mapper"));
        GraphFragment downstream = GraphFragmentComposer.Append(Flow("shape"), Sink("writer"));

        GraphFragment connected = GraphFragmentComposer.Connect(
            upstream,
            Port("mapper", "out"),
            downstream,
            Port("shape", "in"));

        Assert.Equal(["mapper", "reader", "shape", "writer"], NodeIds(connected));
        Assert.Equal(
            ["mapper#out -> shape#in", "reader#out -> mapper#in", "shape#out -> writer#in"],
            EdgeTexts(connected));
    }

    [Fact]
    public void ConnectRemovesOnlyTheTwoConsumedPortsAndOrdersTheRestUpstreamFirst()
    {
        GraphFragment connected = GraphFragmentComposer.Connect(
            Junction("u"),
            Port("u-b", "out2"),
            Junction("d"),
            Port("d-a", "in1"));

        Assert.Equal(["d-a", "d-b", "u-a", "u-b"], NodeIds(connected));
        Assert.Equal(["u-b#out2 -> d-a#in1"], EdgeTexts(connected));

        // Open inputs: every upstream input in its order, then the downstream ones minus the consumed.
        Assert.Equal(
            [Port("u-a", "in1"), Port("u-b", "in2"), Port("d-b", "in2")],
            connected.OpenInputs);

        // Open outputs: the upstream ones minus the consumed, then every downstream output in its order.
        Assert.Equal(
            [Port("u-a", "out1"), Port("d-a", "out1"), Port("d-b", "out2")],
            connected.OpenOutputs);
    }

    [Fact]
    public void ConnectDoesNotModifyEitherInputFragment()
    {
        GraphFragment upstream = Source("reader");
        GraphFragment downstream = Sink("writer");

        _ = GraphFragmentComposer.Connect(upstream, Port("reader", "out"), downstream, Port("writer", "in"));

        Assert.Equal([Port("reader", "out")], upstream.OpenOutputs);
        Assert.Equal([Port("writer", "in")], downstream.OpenInputs);
        Assert.Empty(upstream.Edges);
        Assert.Empty(downstream.Edges);
    }

    [Fact]
    public void ConnectRejectsANullUpstreamFragment()
    {
        Assert.Throws<ArgumentNullException>(
            "upstream",
            () =>
            {
                _ = GraphFragmentComposer.Connect(null!, Port("reader", "out"), Sink("writer"), Port("writer", "in"));
            });
    }

    [Fact]
    public void ConnectRejectsANullDownstreamFragment()
    {
        Assert.Throws<ArgumentNullException>(
            "downstream",
            () =>
            {
                _ = GraphFragmentComposer.Connect(Source("reader"), Port("reader", "out"), null!, Port("writer", "in"));
            });
    }

    [Fact]
    public void ConnectRejectsAnAddressThatIsNotAnOpenOutputOfTheUpstreamFragment()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "upstreamOutput",
            () =>
            {
                _ = GraphFragmentComposer.Connect(
                    Source("reader"),
                    Port("reader", "ghost"),
                    Sink("writer"),
                    Port("writer", "in"));
            });

        Assert.Contains(
            "'reader#ghost' is not an open output of the upstream fragment",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Contains(
            "The upstream fragment's open outputs are: 'reader#out'.",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectRejectsAnAddressThatIsNotAnOpenInputOfTheDownstreamFragment()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "downstreamInput",
            () =>
            {
                _ = GraphFragmentComposer.Connect(
                    Source("reader"),
                    Port("reader", "out"),
                    Sink("writer"),
                    Port("writer", "ghost"));
            });

        Assert.Contains(
            "'writer#ghost' is not an open input of the downstream fragment",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Contains(
            "The downstream fragment's open inputs are: 'writer#in'.",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectRejectsAnOpenPortOfTheWrongSide()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "upstreamOutput",
            () =>
            {
                _ = GraphFragmentComposer.Connect(
                    Flow("mapper"),
                    Port("mapper", "in"),
                    Sink("writer"),
                    Port("writer", "in"));
            });

        Assert.Contains(
            "'mapper#in' is not an open output of the upstream fragment",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectRejectsADefaultAddressAsAnUnopenPortRatherThanThrowingWhileReportingIt()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "upstreamOutput",
            () => { _ = GraphFragmentComposer.Connect(Source("reader"), default, Sink("writer"), Port("writer", "in")); });

        Assert.Contains(
            "'(default PortAddress)' is not an open output of the upstream fragment",
            exception.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectNamesTheEmptyOpenListWhenTheFragmentHasNoPortsOnThatSide()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            "upstreamOutput",
            () =>
            {
                _ = GraphFragmentComposer.Connect(
                    Sink("writer"),
                    Port("writer", "out"),
                    Sink("other"),
                    Port("other", "in"));
            });

        Assert.Contains("open outputs are: none.", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectReportsEveryCollidingNodeIdAndPointsAtImport()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            () =>
            {
                _ = GraphFragmentComposer.Connect(
                    Chain(),
                    Port("b", "out"),
                    Chain(),
                    Port("a", "in"));
            });

        Assert.Null(exception.ParamName);
        Assert.Contains(
            "Composing two fragments requires disjoint node ids, and these two share 2 node ids:",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Contains("1. 'a'.", exception.Message, StringComparison.Ordinal);
        Assert.Contains("2. 'b'.", exception.Message, StringComparison.Ordinal);
        Assert.Contains("Use Import to rebase one or both fragments", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectReportsASingleCollisionInTheSingularForm()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            () =>
            {
                _ = GraphFragmentComposer.Connect(
                    Source("relay"),
                    Port("relay", "out"),
                    Sink("relay"),
                    Port("relay", "in"));
            });

        Assert.Contains("these two share 1 node id:", exception.Message, StringComparison.Ordinal);
        Assert.Contains("1. 'relay'.", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("self-loop", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectRejectsAFragmentConnectedToItselfAsACollision()
    {
        GraphFragment fragment = Chain();

        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => { _ = GraphFragmentComposer.Connect(fragment, Port("b", "out"), fragment, Port("a", "in")); });

        Assert.Contains("share 2 node ids:", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ConnectAcceptsTwoCopiesOfOneFragmentOnceOneIsImported()
    {
        GraphFragment connected = GraphFragmentComposer.Connect(
            Chain(),
            Port("b", "out"),
            GraphFragmentComposer.Import(Chain(), "second"),
            Port("second/a", "in"));

        Assert.Equal(["a", "b", "second/a", "second/b"], NodeIds(connected));
        Assert.Equal(
            ["a#out -> b#in", "b#out -> second/a#in", "second/a#out -> second/b#in"],
            EdgeTexts(connected));
        Assert.Equal([Port("a", "in")], connected.OpenInputs);
        Assert.Equal([Port("second/b", "out")], connected.OpenOutputs);
    }

    [Fact]
    public void AppendJoinsTheOnlyOpenOutputToTheOnlyOpenInput()
    {
        GraphFragment appended = GraphFragmentComposer.Append(Source("reader"), Flow("mapper"));

        Assert.Equal(["mapper", "reader"], NodeIds(appended));
        Assert.Equal(["reader#out -> mapper#in"], EdgeTexts(appended));
        Assert.Empty(appended.OpenInputs);
        Assert.Equal([Port("mapper", "out")], appended.OpenOutputs);
    }

    [Fact]
    public void AppendEqualsConnectNamingTheSameTwoAddresses()
    {
        GraphFragment appended = GraphFragmentComposer.Append(Source("reader"), Sink("writer"));
        GraphFragment connected = GraphFragmentComposer.Connect(
            Source("reader"),
            Port("reader", "out"),
            Sink("writer"),
            Port("writer", "in"));

        Assert.Equal(connected, appended);
        Assert.Equal(connected.GetHashCode(), appended.GetHashCode());
    }

    [Fact]
    public void AppendIsAssociativeOnALinearChain()
    {
        GraphFragment left = GraphFragmentComposer.Append(
            GraphFragmentComposer.Append(Source("reader"), Flow("mapper")),
            Sink("writer"));

        GraphFragment right = GraphFragmentComposer.Append(
            Source("reader"),
            GraphFragmentComposer.Append(Flow("mapper"), Sink("writer")));

        Assert.Equal(left, right);
    }

    [Fact]
    public void AppendSurfacesANodeIdCollisionThroughConnect()
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => { _ = GraphFragmentComposer.Append(Flow("mapper"), Flow("mapper")); });

        Assert.Contains("these two share 1 node id:", exception.Message, StringComparison.Ordinal);
        Assert.Contains("1. 'mapper'.", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AppendRejectsANullFragment()
    {
        Assert.Throws<ArgumentNullException>(
            "upstream",
            () => { _ = GraphFragmentComposer.Append(null!, Sink("writer")); });
        Assert.Throws<ArgumentNullException>(
            "downstream",
            () => { _ = GraphFragmentComposer.Append(Source("reader"), null!); });
    }

    [Fact]
    public void AppendRejectsAnUpstreamFragmentWithNoOpenOutput()
    {
        Assert.Contains(
            "the upstream fragment has 0 open outputs and the downstream fragment has 1 open inputs",
            AppendRejection(Sink("writer"), Sink("other")),
            StringComparison.Ordinal);
    }

    [Fact]
    public void AppendRejectsAnUpstreamFragmentWithTwoOpenOutputs()
    {
        Assert.Contains(
            "the upstream fragment has 2 open outputs and the downstream fragment has 1 open inputs",
            AppendRejection(Junction("u"), Sink("writer")),
            StringComparison.Ordinal);
    }

    [Fact]
    public void AppendRejectsADownstreamFragmentWithNoOpenInput()
    {
        Assert.Contains(
            "the upstream fragment has 1 open outputs and the downstream fragment has 0 open inputs",
            AppendRejection(Source("reader"), Source("other")),
            StringComparison.Ordinal);
    }

    [Fact]
    public void AppendRejectsADownstreamFragmentWithTwoOpenInputs()
    {
        Assert.Contains(
            "the upstream fragment has 1 open outputs and the downstream fragment has 2 open inputs",
            AppendRejection(Source("reader"), Junction("d")),
            StringComparison.Ordinal);
    }

    [Fact]
    public void AppendPointsAtConnectWhenTheShapeIsNotLinear()
    {
        string message = AppendRejection(Junction("u"), Junction("d"));

        Assert.Contains("Append joins one open output to one open input", message, StringComparison.Ordinal);
        Assert.Contains("Use Connect to name the two addresses explicitly.", message, StringComparison.Ordinal);
    }

    private static string AppendRejection(GraphFragment upstream, GraphFragment downstream)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => { _ = GraphFragmentComposer.Append(upstream, downstream); });

        Assert.Null(exception.ParamName);

        return exception.Message;
    }

    /// <summary>Builds a two-node fragment with one open input and one open output.</summary>
    /// <returns>The fragment, whose nodes are <c>a</c> and <c>b</c>.</returns>
    private static GraphFragment Chain() => GraphFragmentComposer.Append(Flow("a"), Flow("b"));

    /// <summary>Builds a two-node fragment with two open ports on each side.</summary>
    /// <param name="prefix">The prefix of both node identifiers.</param>
    /// <returns>The fragment.</returns>
    private static GraphFragment Junction(string prefix) =>
        GraphFragment.Create(
            [Node($"{prefix}-a"), Node($"{prefix}-b")],
            [],
            [Port($"{prefix}-a", "in1"), Port($"{prefix}-b", "in2")],
            [Port($"{prefix}-a", "out1"), Port($"{prefix}-b", "out2")]);
}
