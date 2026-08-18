using System.Text.Json;
using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Serialization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.DurableFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a durable scope is: a region of a graph whose stages' state a checkpoint can carry, declared in the
/// document, and refusing by name every stage whose state it could not.
/// </summary>
/// <remarks>
/// <para>
/// The scope is <b>not</b> a supervision form, and the tests say so by shape rather than by comment: it
/// takes no policy, it answers no failure, and it composes with a supervision scope rather than replacing
/// one. The two answer different questions — what a failing element costs, and what a dead process costs —
/// and the one place they would have overlapped is a contradiction, since a restarting supervision form
/// resets every state in its scope and this one keeps every state across a resume.
/// </para>
/// <para>
/// The refusals are the interesting half and they land in two places on purpose. What the <em>document</em>
/// can state — which shapes the chain holds — is refused at authoring and by the payload reader alike, in
/// the same words. What only the <em>binding</em> knows — whether a scan carries a state codec — is refused
/// when the plan is built, which is the line M5.1 drew for every disagreement between a scope's two planes.
/// </para>
/// </remarks>
public sealed class DurableScopeTests
{
    [Fact]
    public void AGraphHoldingADurableScopeDeclaresDurableState()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .Durable(Flow.For<int>().Scan(0L, (sum, value) => sum + value, WriteTotal, ReadTotal))
            .To(s => s.Ignore());

        // The token is what tells a host that this graph expects state to survive a process, so a host that
        // does not know the word refuses the document rather than running it without durability.
        Assert.Contains(CapabilityToken.Create("durable-state"), graph.Document.Capabilities);
        Assert.Contains(CapabilityToken.Nondeployable, graph.Document.Capabilities);
    }

    [Fact]
    public void AGraphWithNoDurableScopeDeclaresNoSuchToken()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .Scan(0L, (sum, value) => sum + value)
            .To(s => s.Ignore());

        Assert.DoesNotContain(CapabilityToken.Create("durable-state"), graph.Document.Capabilities);
    }

    [Fact]
    public void TheDocumentStatesTheChainTheScopeIsMadeOf()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .Durable(Flow.For<int>().Where(value => value > 0).Take(2))
            .To(s => s.Ignore());

        StageNode scope = graph.Document.Nodes.Single(
            node => node.Stage.ToString() == "local/durable@v1");
        JsonElement stages = scope.Parameters.ToElement().GetProperty("scope");

        Assert.Equal(2, stages.GetArrayLength());
        Assert.Equal("local/where@v1", stages[0].GetProperty("stage").GetString());
        Assert.Equal("local/take@v1", stages[1].GetProperty("stage").GetString());
        Assert.Equal(2, stages[1].GetProperty("parameters").GetProperty("count").GetInt32());
    }

    [Fact]
    public void TwoScopesOverDifferentChainsAreTwoGraphs()
    {
        RunnableGraph first = Source.From([1]).Durable(Flow.For<int>().Take(1)).To(s => s.Ignore());
        RunnableGraph second = Source.From([1]).Durable(Flow.For<int>().Take(2)).To(s => s.Ignore());

        Assert.NotEqual(first.Fingerprint, second.Fingerprint);
    }

    [Fact]
    public void AScanWithACodecAndOneWithoutAreOneGraph()
    {
        RunnableGraph coded = Source.From([1])
            .Durable(Flow.For<int>().Scan(0L, (sum, value) => sum + value, WriteTotal, ReadTotal))
            .To(s => s.Ignore());
        RunnableGraph plain = Source.From([1])
            .Durable(Flow.For<int>().Scan(0L, (sum, value) => sum + value))
            .To(s => s.Ignore());

        // A codec is a delegate and no delegate enters a document, so the two fingerprint alike — which is
        // exactly why "this scan exports state" cannot be a document fact and is refused when the plan is
        // built instead.
        Assert.Equal(coded.Fingerprint, plain.Fingerprint);
    }

    [Fact]
    public async Task AScanWithNoStateCodecIsRefusedByNameWhenThePlanIsBuilt()
    {
        RunnableGraph graph = Source.From([1, 2, 3])
            .Durable(Flow.For<int>().Scan(0L, (sum, value) => sum + value))
            .To(s => s.Ignore());

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("exports no state", refused.Message, StringComparison.Ordinal);
        Assert.Contains("local/scan@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AStageWhoseStateIsNotACanonicalValueIsRefusedByName()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1, 2, 3])
                .Durable(Flow.For<int>().Distinct(new DistinctOptions { MaxTrackedKeys = 4 }))
                .To(s => s.Ignore()));

        Assert.Contains("local/distinct@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("canonical value", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryRefusedStageOfAChainIsNamedRatherThanTheFirst()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1, 2, 3])
                .Durable(Flow.For<int>().TakeWhile(value => value > 0).Grouped(2))
                .To(s => s.Ignore()));

        Assert.Contains("local/take-while@v1", refused.Message, StringComparison.Ordinal);
        Assert.Contains("local/grouped@v1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AStageDeclaringAControlIsRefusedInsideTheScope()
    {
        ArgumentException refused = Assert.Throws<ArgumentException>(
            () => Source.From([1, 2, 3])
                .Durable(Flow.For<int>().Via(TestFlow.FaultPoint<int>("point", FaultPointMode.Never, 1)))
                .To(s => s.Ignore()));

        Assert.Contains("not nodes of the document", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyScopeIsLegalAndIsTheIdentityChain()
    {
        RunnableGraph graph = Source.From([1, 2, 3]).Durable(Flow.For<int>()).To(s => s.Ignore());

        StageNode scope = graph.Document.Nodes.Single(node => node.Stage.ToString() == "local/durable@v1");

        Assert.Equal(0, scope.Parameters.ToElement().GetProperty("scope").GetArrayLength());
    }

    [Theory]
    [InlineData("""{}""", "scope")]
    [InlineData("""{"scope":{}}""", "scope")]
    [InlineData("""{"scope":[],"interval":5}""", "interval")]
    [InlineData("""{"scope":[{"stage":"local/distinct@v1","parameters":{"maxTrackedKeys":2,"overflowPolicy":"fail"}}]}""", "durable scope")]
    public void AHandWrittenPayloadIsRefusedInTheSameWordsTheAuthoringSurfaceUses(string json, string expected)
    {
        Assert.False(LocalDurableParameters.TryRead(
            CanonicalJsonValue.Parse(json),
            out IReadOnlyList<LocalInnerStage> scope,
            out IReadOnlyList<string> violations));
        Assert.Empty(scope);
        Assert.Contains(violations, violation => violation.Contains(expected, StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheScopeRunsItsChainAndEmitsWhatItProduces()
    {
        List<long> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Durable(Flow.For<int>().Where(value => value % 2 == 0).Scan(0L, (sum, value) => sum + value, WriteTotal, ReadTotal))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The chain is executed by the scope rather than beside it, so what leaves the scope is what the
        // chain produced and nothing else: the odd elements never reach the scan.
        Assert.Equal([2L, 6L], observed);
    }

    [Fact]
    public async Task AFaultPointComposesWithTheScopeAndContributesNoStateToIt()
    {
        List<long> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Durable(Flow.For<int>()
                .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 3))
                .Scan(0L, (sum, value) => sum + value, WriteTotal, ReadTotal))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        _ = await Assert.ThrowsAsync<FaultInjectedException>(async () => await run.Completion);

        // A fault point holds no state of the author's — its arrival counter belongs to the run, which is
        // M5.1's own statement about a restart read over a resume — so it is admitted inside the scope and
        // exports nothing. The scope is not a supervision scope, so the failure travels to the run: this is
        // the composition, not a policy.
        Assert.Equal([1L, 3L], observed);
    }

    [Fact]
    public async Task AScopeInsideASupervisedSectionIsAComposition()
    {
        List<long> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>().Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2)))
            .Durable(Flow.For<int>().Scan(0L, (sum, value) => sum + value, WriteTotal, ReadTotal))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The supervised section drops the second element and the durable scope below it never sees one,
        // which is what "composition rather than mode" means: neither knows the other exists.
        Assert.Equal([1L, 4L, 8L], observed);
    }
}
