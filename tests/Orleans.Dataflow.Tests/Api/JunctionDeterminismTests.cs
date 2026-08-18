using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Api.ApiFixtures;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// What a junction graph's identity is made of, and what changes it.
/// </summary>
/// <remarks>
/// <para>
/// Two claims, and they are opposite sides of one statement. Building the same program twice has to produce
/// the same bytes, or a fingerprint identifies nothing; and reordering the arguments of a junction call has
/// to produce different bytes, or branch order is not identity-bearing and ADR 0006's promise that
/// reordering a junction is like reordering a chain would be false.
/// </para>
/// <para>
/// Byte equality is asserted rather than fingerprint equality wherever both would do. A fingerprint is a
/// digest of the bytes, so equal bytes is the stronger statement and the one that fails with something a
/// reader can diff.
/// </para>
/// </remarks>
public sealed class JunctionDeterminismTests
{
    [Fact]
    public void TwoBuildsOfOneJunctionProgramProduceTheSameBytes()
    {
        foreach ((string name, Func<RunnableGraph> build) in JunctionBuilds())
        {
            Assert.Equal(
                GraphDocumentSerializer.Serialize(build().Document),
                GraphDocumentSerializer.Serialize(build().Document));
            Assert.Equal(build().Fingerprint, build().Fingerprint);

            // And the nonce is the one thing that is not shared, which is what keeps two look-alike graphs
            // from resolving each other's results.
            Assert.NotEqual(build().AuthoringNonce, build().AuthoringNonce);
            Assert.False(string.IsNullOrEmpty(name));
        }
    }

    [Fact]
    public void SwappingTwoBranchesChangesTheDocument()
    {
        // Branch order is argument order and argument order is identity: the occurrences of the first branch
        // are numbered before the second's, so the two documents differ in which node stands where even
        // though the graphs do the same thing.
        RunnableGraph counting = Source.From<int>([1, 2]).BroadcastTo(
            Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> _),
            Flow.For<int>().To(s => s.Aggregate(0L, (sum, value) => sum + value), "summed", out ResultSlot<long> _));

        RunnableGraph summing = Source.From<int>([1, 2]).BroadcastTo(
            Flow.For<int>().To(s => s.Aggregate(0L, (sum, value) => sum + value), "summed", out ResultSlot<long> _),
            Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> _));

        Assert.Equal(["from-enumerable", "broadcast", "count", "fold"], StageIds(counting.Document));
        Assert.Equal(["from-enumerable", "broadcast", "fold", "count"], StageIds(summing.Document));
        Assert.NotEqual(counting.Fingerprint, summing.Fingerprint);
    }

    [Fact]
    public void SwappingTwoMergedSourcesChangesTheDocument()
    {
        // The fan-in half of the same rule. The two merges join the same two streams and a merge is
        // symmetric in what it emits, and the documents are still different: which source reaches in-0 is
        // written down, and a fingerprint covers everything written down.
        Source<int> left = Source.From<int>([1]);
        Source<int> right = Source.From<int>([2]).Select(value => value + 1);

        RunnableGraph leftFirst = left.Merge(right).To(Sink.Ignore<int>());
        RunnableGraph rightFirst = right.Merge(left).To(Sink.Ignore<int>());

        Assert.Equal(["from-enumerable", "from-enumerable", "select", "merge", "ignore"], StageIds(leftFirst.Document));
        Assert.Equal(["from-enumerable", "select", "from-enumerable", "merge", "ignore"], StageIds(rightFirst.Document));
        Assert.NotEqual(leftFirst.Fingerprint, rightFirst.Fingerprint);
    }

    [Fact]
    public void TwoBranchesThatDifferOnlyInTheirSlotNamesAreTwoGraphs()
    {
        // A slot name is durable identity and lives in the document, so renaming one is a different graph.
        // That is the same rule a linear graph follows, reached here through a branch.
        RunnableGraph first = Source.From<int>([1]).BroadcastTo(
            Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> _),
            Flow.For<int>().To(Sink.Ignore<int>()));

        RunnableGraph second = Source.From<int>([1]).BroadcastTo(
            Flow.For<int>().To(s => s.Count(), "tallied", out ResultSlot<long> _),
            Flow.For<int>().To(Sink.Ignore<int>()));

        Assert.Equal(StageIds(first.Document), StageIds(second.Document));
        Assert.NotEqual(first.Fingerprint, second.Fingerprint);
    }

    [Fact]
    public void AJunctionGraphIsUnchangedByBuildingAnotherGraphFromItsParts()
    {
        // Immutability, for the values a junction call consumes. A branch, a flow, and a source are read by
        // the call and never amended, so composing them into a second graph cannot disturb the first — which
        // is the property that makes them worth calling values.
        Source<int> numbers = Source.From<int>([1, 2, 3]);
        Branch<int> discard = Flow.For<int>().To(Sink.Ignore<int>());

        byte[] before = GraphDocumentSerializer.Serialize(
            numbers.BroadcastTo(discard, Flow.For<int>().To(Sink.Ignore<int>())).Document);

        _ = numbers.Select(value => value * 2).BalanceTo(discard, discard);
        _ = numbers.AlsoTo(discard).To(Sink.Ignore<int>());

        byte[] after = GraphDocumentSerializer.Serialize(
            numbers.BroadcastTo(discard, Flow.For<int>().To(Sink.Ignore<int>())).Document);

        Assert.Equal(before, after);
    }

    [Fact]
    public void OneResultlessBranchUsedTwiceInOneGraphIsTwoOccurrences()
    {
        // The flat numbering ADR 0004 asks for, reached through a branch instead of a flow: a value composed
        // twice contributes its occurrences twice and they are numbered as the distinct occurrences they
        // are. Nothing is shared between them at run time either, which is what makes this legal at all.
        Branch<int> discard = Flow.For<int>().Select(value => value + 1).To(Sink.Ignore<int>());

        RunnableGraph graph = Source.From<int>([1]).BalanceTo(discard, discard);

        Assert.Equal(
            ["from-enumerable", "balance", "select", "ignore", "select", "ignore"],
            StageIds(graph.Document));
        Assert.Equal(6, graph.Document.Nodes.Count);
    }

    [Fact]
    public void TheCapabilityTokensOfAJunctionGraphAreTheOnesItsOccurrencesRequire()
    {
        foreach ((string name, Func<RunnableGraph> build) in JunctionBuilds())
        {
            GraphDocument document = build().Document;

            Assert.Equal(["ephemeral-identity", "nondeployable"], Capabilities(document).Order(StringComparer.Ordinal));
            Assert.False(string.IsNullOrEmpty(name));
        }
    }

    /// <summary>Enumerates the junction programs, as builders that can be run twice.</summary>
    /// <returns>One named builder per program.</returns>
    /// <remarks>
    /// Builders rather than graphs, because the claim is about building: two invocations of one program have
    /// to produce the same document, and a list of already-built graphs could not say that.
    /// </remarks>
    private static IEnumerable<(string Name, Func<RunnableGraph> Build)> JunctionBuilds()
    {
        yield return ("broadcast to two sinks", static () => JunctionPrograms.BroadcastTwoSinks().Graph);
        yield return ("tap for audit", static () => JunctionPrograms.TapForAudit().Graph);
        yield return ("balance workers", JunctionPrograms.BalanceWorkers);
        yield return ("partition by size", static () => JunctionPrograms.PartitionBySize().Graph);
        yield return ("merge and concat", static () => JunctionPrograms.MergeAndConcat().Graph);
        yield return ("zip prices and quantities", static () => JunctionPrograms.ZipPricesAndQuantities().Graph);
        yield return ("diamond fork zip", static () => JunctionPrograms.DiamondForkZip().Graph);
        yield return ("unzip pairs", static () => JunctionPrograms.UnzipPairs().Graph);
        yield return ("fast path slow path", static () => JunctionPrograms.FastPathSlowPath().Graph);
    }
}
