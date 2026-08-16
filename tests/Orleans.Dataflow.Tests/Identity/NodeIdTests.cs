using Orleans.Dataflow.Identity;
using Xunit;

namespace Orleans.Dataflow.Tests.Identity;

/// <summary>
/// Tests for <see cref="NodeId"/>, including the import-scope rebasing primitive.
/// </summary>
public sealed class NodeIdTests
{
    [Theory]
    [InlineData("a")]
    [InlineData("normalize")]
    [InlineData("import-a")]
    [InlineData("orders/normalize")]
    [InlineData("orders/import-a/normalize")]
    [InlineData("a/b/c/d/e/f")]
    public void ParseRoundTripsCanonicalPath(string candidate)
    {
        NodeId nodeId = NodeId.Parse(candidate);

        Assert.Equal(candidate, nodeId.Value);
        Assert.Equal(candidate, nodeId.ToString());
        Assert.False(nodeId.IsDefault);
    }

    [Fact]
    public void ParseAndCreateAgreeOnSingleSegment()
    {
        Assert.Equal(NodeId.Create("a"), NodeId.Parse("a"));
        Assert.Equal(NodeId.Create("normalize"), NodeId.Parse("normalize"));
        Assert.True(NodeId.Create("normalize") == NodeId.Parse("normalize"));
    }

    [Fact]
    public void CreateRejectsMultiSegmentPath()
    {
        ArgumentException exception =
            Assert.Throws<ArgumentException>("segment", () => { _ = NodeId.Create("orders/normalize"); });

        Assert.Contains("orders/normalize", exception.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(NodeId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateRejectsNull()
    {
        Assert.Throws<ArgumentNullException>("segment", () => { _ = NodeId.Create(null!); });
    }

    [Fact]
    public void ParseRejectsNull()
    {
        Assert.Throws<ArgumentNullException>("path", () => { _ = NodeId.Parse(null!); });
    }

    [Theory]
    [InlineData("")]
    [InlineData("/")]
    [InlineData("/a")]
    [InlineData("a/")]
    [InlineData("a//b")]
    [InlineData("a/-b")]
    [InlineData("a/b-")]
    [InlineData("a/B")]
    [InlineData("A/b")]
    [InlineData("a/b c")]
    [InlineData("a/b.c")]
    [InlineData("a/b_c")]
    [InlineData("a/é")]
    [InlineData("a/İ")]
    [InlineData("-a/b")]
    [InlineData("a--b/c")]
    public void ParseRejectsInvalidPath(string candidate)
    {
        ArgumentException exception = Assert.Throws<ArgumentException>("path", () => { _ = NodeId.Parse(candidate); });

        Assert.Contains(candidate, exception.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(NodeId), exception.Message, StringComparison.Ordinal);
        Assert.False(NodeId.TryParse(candidate, out NodeId nodeId));
        Assert.True(nodeId.IsDefault);
    }

    [Fact]
    public void TryParseRejectsNullWithoutThrowing()
    {
        Assert.False(NodeId.TryParse(null, out NodeId nodeId));
        Assert.True(nodeId.IsDefault);
    }

    [Fact]
    public void ParseRejectsOverLengthSegment()
    {
        string candidate = "orders/" + new string('a', 65);

        ArgumentException exception = Assert.Throws<ArgumentException>("path", () => { _ = NodeId.Parse(candidate); });

        Assert.Contains("65", exception.Message, StringComparison.Ordinal);
        Assert.Contains("64", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseAcceptsMaximumDepth()
    {
        string candidate = PathOfDepth(NodeId.MaxDepth);

        NodeId nodeId = NodeId.Parse(candidate);

        Assert.Equal(NodeId.MaxDepth, nodeId.Depth);
    }

    [Fact]
    public void ParseRejectsExcessiveDepth()
    {
        string candidate = PathOfDepth(NodeId.MaxDepth + 1);

        ArgumentException exception = Assert.Throws<ArgumentException>("path", () => { _ = NodeId.Parse(candidate); });

        Assert.Contains("16", exception.Message, StringComparison.Ordinal);
        Assert.False(NodeId.TryParse(candidate, out _));
    }

    [Fact]
    public void ParseAcceptsMaximumPathLength()
    {
        string candidate = PathOfLength(NodeId.MaxPathLength);

        Assert.Equal(NodeId.MaxPathLength, candidate.Length);
        Assert.Equal(candidate, NodeId.Parse(candidate).Value);
    }

    [Fact]
    public void ParseRejectsExcessivePathLength()
    {
        string candidate = PathOfLength(NodeId.MaxPathLength + 1);

        Assert.Equal(NodeId.MaxPathLength + 1, candidate.Length);

        ArgumentException exception = Assert.Throws<ArgumentException>("path", () => { _ = NodeId.Parse(candidate); });

        Assert.Contains("257", exception.Message, StringComparison.Ordinal);
        Assert.Contains("256", exception.Message, StringComparison.Ordinal);
        Assert.False(NodeId.TryParse(candidate, out _));
    }

    [Theory]
    [InlineData("a", 1)]
    [InlineData("orders/normalize", 2)]
    [InlineData("orders/import-a/normalize", 3)]
    public void DepthCountsSegments(string candidate, int expectedDepth)
    {
        Assert.Equal(expectedDepth, NodeId.Parse(candidate).Depth);
    }

    [Fact]
    public void GetSegmentsReturnsPathSegmentsInOrder()
    {
        NodeId nodeId = NodeId.Parse("orders/import-a/normalize");

        Assert.Collection(
            nodeId.GetSegments(),
            segment => Assert.Equal("orders", segment),
            segment => Assert.Equal("import-a", segment),
            segment => Assert.Equal("normalize", segment));
    }

    [Fact]
    public void GetSegmentsReturnsAFreshListPerCall()
    {
        NodeId nodeId = NodeId.Parse("orders/normalize");

        Assert.NotSame(nodeId.GetSegments(), nodeId.GetSegments());
    }

    [Fact]
    public void InScopePrefixesSingleSegmentIdentifier()
    {
        Assert.Equal(NodeId.Parse("s/a"), NodeId.Create("a").InScope("s"));
        Assert.Equal("s/a", NodeId.Create("a").InScope("s").Value);
    }

    [Fact]
    public void InScopePrefixesMultiSegmentIdentifier()
    {
        Assert.Equal("s/a/b", NodeId.Parse("a/b").InScope("s").Value);
        Assert.Equal(3, NodeId.Parse("a/b").InScope("s").Depth);
    }

    [Fact]
    public void NestedScopesComposeByNestingPrefixes()
    {
        NodeId nested = NodeId.Parse("a/b").InScope("s1").InScope("s2");

        Assert.Equal("s2/s1/a/b", nested.Value);
        Assert.Equal(NodeId.Parse("s2/s1/a/b"), nested);
    }

    [Fact]
    public void RebasingEqualInputsIsDeterministic()
    {
        NodeId first = NodeId.Parse("import/enrich").InScope("orders");
        NodeId second = NodeId.Parse("import/enrich").InScope("orders");

        Assert.Equal(first, second);
        Assert.True(first == second);
        Assert.Equal(first.GetHashCode(), second.GetHashCode());
        Assert.Equal("orders/import/enrich", first.Value);
    }

    [Fact]
    public void DistinctScopesProduceDisjointIdentifierSets()
    {
        NodeId[] fragment = [NodeId.Create("normalize"), NodeId.Parse("import/enrich")];

        HashSet<NodeId> imported = [.. fragment.Select(nodeId => nodeId.InScope("orders"))];
        HashSet<NodeId> reimported = [.. fragment.Select(nodeId => nodeId.InScope("returns"))];

        Assert.Equal(2, imported.Count);
        Assert.Equal(2, reimported.Count);
        Assert.False(imported.Overlaps(reimported));
        Assert.Contains(NodeId.Parse("orders/normalize"), imported);
        Assert.Contains(NodeId.Parse("returns/import/enrich"), reimported);
    }

    [Fact]
    public void InScopeDoesNotModifyTheOriginal()
    {
        NodeId original = NodeId.Parse("a/b");

        _ = original.InScope("s");

        Assert.Equal("a/b", original.Value);
    }

    [Fact]
    public void InScopeRejectsNull()
    {
        Assert.Throws<ArgumentNullException>("scopeSegment", () => { _ = NodeId.Create("a").InScope(null!); });
    }

    [Theory]
    [InlineData("")]
    [InlineData("-s")]
    [InlineData("s-")]
    [InlineData("s--t")]
    [InlineData("S")]
    [InlineData("s t")]
    [InlineData("s/t")]
    [InlineData("s.t")]
    [InlineData("s_t")]
    [InlineData("é")]
    public void InScopeRejectsInvalidScopeSegment(string candidate)
    {
        ArgumentException exception =
            Assert.Throws<ArgumentException>("scopeSegment", () => { _ = NodeId.Create("a").InScope(candidate); });

        Assert.Contains(candidate, exception.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(NodeId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void InScopeRejectsResultBeyondMaximumDepth()
    {
        NodeId deepest = NodeId.Parse(PathOfDepth(NodeId.MaxDepth));

        ArgumentException exception =
            Assert.Throws<ArgumentException>("scopeSegment", () => { _ = deepest.InScope("s"); });

        Assert.Contains("17", exception.Message, StringComparison.Ordinal);
        Assert.Contains("16", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void InScopeAcceptsResultAtMaximumDepth()
    {
        NodeId rebased = NodeId.Parse(PathOfDepth(NodeId.MaxDepth - 1)).InScope("s");

        Assert.Equal(NodeId.MaxDepth, rebased.Depth);
    }

    [Fact]
    public void InScopeRejectsResultBeyondMaximumPathLength()
    {
        NodeId longest = NodeId.Parse(PathOfLength(NodeId.MaxPathLength));

        ArgumentException exception =
            Assert.Throws<ArgumentException>("scopeSegment", () => { _ = longest.InScope("s"); });

        Assert.Contains("258", exception.Message, StringComparison.Ordinal);
        Assert.Contains("256", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void InScopeAcceptsResultAtMaximumPathLength()
    {
        NodeId rebased = NodeId.Parse(PathOfLength(NodeId.MaxPathLength - 2)).InScope("s");

        Assert.Equal(NodeId.MaxPathLength, rebased.Value.Length);
    }

    [Fact]
    public void EqualPathsAreEqualAndShareHashCode()
    {
        NodeId left = NodeId.Parse("orders/normalize");
        NodeId right = NodeId.Parse("orders/normalize");

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact]
    public void DifferentPathsAreNotEqual()
    {
        NodeId left = NodeId.Parse("orders/normalize");
        NodeId right = NodeId.Parse("returns/normalize");

        Assert.NotEqual(left, right);
        Assert.True(left != right);
        Assert.NotEqual(NodeId.Create("normalize"), NodeId.Parse("orders/normalize"));
    }

    [Fact]
    public void DefaultInstanceIsDefault()
    {
        Assert.True(default(NodeId).IsDefault);
        Assert.Equal(default, default(NodeId));
        Assert.NotEqual(default, NodeId.Create("a"));
    }

    [Fact]
    public void DefaultInstanceMembersThrowInvalidOperationException()
    {
        NodeId nodeId = default;

        Assert.Throws<InvalidOperationException>(() => { _ = nodeId.Depth; });
        Assert.Throws<InvalidOperationException>(() => { _ = nodeId.GetSegments(); });
        Assert.Throws<InvalidOperationException>(() => { _ = nodeId.InScope("s"); });

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() => { _ = nodeId.Value; });

        Assert.Contains(nameof(NodeId), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultInstanceToStringIsDiagnosticLiteralAndDoesNotThrow()
    {
        Assert.Equal("(default NodeId)", default(NodeId).ToString());
    }

    [Fact]
    public void DefaultInstanceIsUsableInAHashSet()
    {
        HashSet<NodeId> identifiers = [default, NodeId.Create("a"), default, NodeId.Create("a")];

        Assert.Equal(2, identifiers.Count);
    }

    [Fact]
    public void ComparisonIsOrdinalOverTheWholePathText()
    {
        // The ordering table, and the pin ADR 0003 fixes for a document's canonical node order: comparison
        // is over the whole '/'-joined path and not segment by segment, so 'a' precedes 'a-b' because it
        // is a prefix of it, and 'a-b' precedes 'a/b' because '-' precedes '/' in code-point order. A
        // segment-wise comparison would put 'a/b' second, and a document would sort differently.
        string[] ordered = ["a", "a-b", "a/b", "a/b-c", "a/b/c", "ab", "b"];

        for (int index = 1; index < ordered.Length; index++)
        {
            NodeId left = NodeId.Parse(ordered[index - 1]);
            NodeId right = NodeId.Parse(ordered[index]);

            Assert.True(left.CompareTo(right) < 0, $"'{ordered[index - 1]}' should sort before '{ordered[index]}'");
            Assert.True(right.CompareTo(left) > 0, $"'{ordered[index]}' should sort after '{ordered[index - 1]}'");
            Assert.True(left < right);
            Assert.True(left <= right);
            Assert.True(right > left);
            Assert.True(right >= left);
        }
    }

    [Fact]
    public void SortingUsesTheSameOrderWhicheverWayTheInputArrived()
    {
        NodeId[] shuffled =
        [
            NodeId.Parse("b"),
            NodeId.Parse("a/b"),
            NodeId.Parse("a"),
            NodeId.Parse("ab"),
            NodeId.Parse("a-b"),
        ];

        Array.Sort(shuffled);

        Assert.Equal(["a", "a-b", "a/b", "ab", "b"], shuffled.Select(id => id.Value));
    }

    [Fact]
    public void TheDefaultInstanceSortsBeforeEveryCreatedOne()
    {
        NodeId created = NodeId.Create("a");

        Assert.True(default(NodeId).CompareTo(created) < 0);
        Assert.True(created.CompareTo(default) > 0);
        Assert.Equal(0, default(NodeId).CompareTo(default));
        Assert.True(default(NodeId) < created);
        Assert.True(created >= default(NodeId));
    }

    [Fact]
    public void ComparisonIsConsistentWithEquality()
    {
        NodeId left = NodeId.Parse("orders/import-a/normalize");
        NodeId right = NodeId.Parse("orders/import-a/normalize");

        Assert.Equal(0, left.CompareTo(right));
        Assert.Equal(left, right);
        Assert.True(left <= right);
        Assert.True(left >= right);
        Assert.False(left < right);
        Assert.False(left > right);
    }

    [Fact]
    public void AutomaticStageNumberingSortsInAuthoringOrderBecauseItIsPadded()
    {
        // The property the authoring API's zero padding buys, stated against the identifier type that
        // provides the order rather than against the graph that depends on it.
        NodeId[] padded = [NodeId.Create("stage-0010"), NodeId.Create("stage-0002")];
        NodeId[] unpadded = [NodeId.Create("stage-10"), NodeId.Create("stage-2")];

        Array.Sort(padded);
        Array.Sort(unpadded);

        Assert.Equal(["stage-0002", "stage-0010"], padded.Select(id => id.Value));
        Assert.Equal(["stage-10", "stage-2"], unpadded.Select(id => id.Value));
    }

    private static string PathOfDepth(int depth) =>
        string.Join(NodeId.Separator, Enumerable.Repeat("a", depth));

    private static string PathOfLength(int totalLength) =>
        string.Join(
            NodeId.Separator,
            new string('a', 64),
            new string('b', 64),
            new string('c', 64),
            new string('d', totalLength - (3 * 64) - 3));

    [Fact]
    public void TheNonGenericComparisonAgreesWithTheTypedOne()
    {
        // F#'s 'comparison' constraint is satisfied by System.IComparable and not by IComparable<'T>, so
        // this implementation is what lets the type key an F# Set or Map.
        IComparable left = NodeId.Create("a");
        NodeId right = NodeId.Parse("a/b");

        Assert.True(typeof(IComparable).IsAssignableFrom(typeof(NodeId)));
        Assert.Equal(((NodeId)left).CompareTo(right), left.CompareTo(right));
        Assert.True(left.CompareTo(null) > 0);
        Assert.Throws<ArgumentException>("obj", () => left.CompareTo("not a NodeId"));
    }
}
