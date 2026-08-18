using Orleans.Dataflow.Tests.Api;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The nine authored junction programs, run and asserted on their results.
/// </summary>
/// <remarks>
/// <para>
/// The authoring tests prove the documents are what they should be; these prove the documents are what the
/// engine already runs. That is the whole claim of this checkpoint — the fluent surface emits the junction
/// nodes and edges the M4.1 engine was built against, so nothing here asserts a document byte. It asserts
/// counts, totals, rows, and one multiset, which is what an author would check.
/// </para>
/// <para>
/// The results are exact wherever the semantics are exact. The one place they are not is the merging
/// diamond, whose two paths race by design: what is asserted there is the multiset both paths produced,
/// because the order is genuinely undefined and an assertion that fixed it would be asserting a timing.
/// </para>
/// </remarks>
public sealed class FluentJunctionTests
{
    [Fact]
    public async Task BroadcastingToTwoSinksResolvesBothSlots()
    {
        (RunnableGraph graph, ResultSlot<long> counted, ResultSlot<decimal> totaled) =
            JunctionPrograms.BroadcastTwoSinks();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both branches have");

        Assert.Equal(2L, await run.GetValueAsync(counted, TestToken));
        Assert.Equal(30m, await run.GetValueAsync(totaled, TestToken));
    }

    [Fact]
    public async Task ATapAuditsWithoutDisturbingTheMainLine()
    {
        (RunnableGraph graph, ResultSlot<long> kept) = JunctionPrograms.TapForAudit();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the main line and the tap have");

        Assert.Equal(1L, await run.GetValueAsync(kept, TestToken));
    }

    [Fact]
    public async Task BalancingWorkAcrossTwoBranchesCompletes()
    {
        // Nothing to resolve: both branches discard. What is under test is that a hundred elements pass
        // through a balance with two identical legs and the run ends, which is the whole of what a graph
        // with no result promises.
        RunnableGraph graph = JunctionPrograms.BalanceWorkers();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both workers have");

        Assert.Empty(graph.ResultSlots);
    }

    [Fact]
    public async Task BalancingGivesEveryElementToExactlyOneBranch()
    {
        // Which leg an element takes is not defined — a balance hands it to whichever is ready — so the
        // claim that can be made is the one that matters: nothing is lost and nothing is duplicated. The
        // program above cannot say that, because both of its branches discard.
        RunnableGraph graph = Source.From(Enumerable.Range(0, 100)).BalanceTo(
            Flow.For<int>().To(s => s.Count(), "left", out ResultSlot<long> left),
            Flow.For<int>().To(s => s.Count(), "right", out ResultSlot<long> right));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both branches have");

        Assert.Equal(100L, await run.GetValueAsync(left, TestToken) + await run.GetValueAsync(right, TestToken));
    }

    [Fact]
    public async Task PartitioningBySizeSendsEachOrderToItsOwnClass()
    {
        (RunnableGraph graph, ResultSlot<long> small, ResultSlot<long> large) = JunctionPrograms.PartitionBySize();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both classes have");

        Assert.Equal(1L, await run.GetValueAsync(small, TestToken));
        Assert.Equal(1L, await run.GetValueAsync(large, TestToken));
    }

    [Fact]
    public async Task MergingTwoSourcesAndConcatenatingAThirdDeliversEveryElement()
    {
        (RunnableGraph graph, ResultSlot<long> all) = JunctionPrograms.MergeAndConcat();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when all three sources have");

        Assert.Equal(21L, await run.GetValueAsync(all, TestToken));
    }

    [Fact]
    public async Task ZippingPricesWithQuantitiesTotalsTheLines()
    {
        // 10 x 3 and 20 x 4, paired positionally: the answer is a statement about which price met which
        // quantity, not only about how many rows there were.
        (RunnableGraph graph, ResultSlot<decimal> total) = JunctionPrograms.ZipPricesAndQuantities();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both inputs have");

        Assert.Equal(110m, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task TheDiamondRejoinsOneRowPerElement()
    {
        (RunnableGraph graph, ResultSlot<long> rows) = JunctionPrograms.DiamondForkZip();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the rejoined stream has");

        Assert.Equal(1L, await run.GetValueAsync(rows, TestToken));
    }

    [Fact]
    public async Task UnzippingPairsFeedsBothHalves()
    {
        (RunnableGraph graph, ResultSlot<long> names, ResultSlot<long> ages) = JunctionPrograms.UnzipPairs();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both halves have");

        Assert.Equal(2L, await run.GetValueAsync(names, TestToken));
        Assert.Equal(2L, await run.GetValueAsync(ages, TestToken));
    }

    [Fact]
    public async Task TheMergingDiamondCollectsWhatBothPathsProduced()
    {
        (RunnableGraph graph, ResultSlot<IReadOnlyList<string>> seen) = JunctionPrograms.FastPathSlowPath();

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both paths have");

        IReadOnlyList<string> collected = await run.GetValueAsync(seen, TestToken);

        // A multiset, deliberately: one path is a cache hit and the other sleeps, so which arrives first is
        // a timing and not a contract. What the merge promises is that both arrive exactly once.
        Assert.Equal(["cache:a", "fetch:a"], collected.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task ARunRefusesTheSlotOfABranchThatClosedNoGraph()
    {
        // The practical end of the branch-slot rule: a slot whose junction call never ran names no graph, so
        // a run cannot be asked for it. It fails where the slot is read rather than resolving something that
        // happens to share a name.
        (RunnableGraph graph, ResultSlot<long> counted, _) = JunctionPrograms.BroadcastTwoSinks();

        _ = Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> unclosed);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both branches have");

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(unclosed, TestToken));
        Assert.Equal(2L, await run.GetValueAsync(counted, TestToken));
    }

    [Fact]
    public async Task TwoRunsOfOneJunctionGraphResolveIndependently()
    {
        // A junction graph is a description like every other: materializing it twice starts two runs, and
        // the slots of the one graph resolve against either. The counted branch is the one that would show
        // a shared accumulator, because a count that continued would come back as four.
        (RunnableGraph graph, ResultSlot<long> counted, ResultSlot<decimal> totaled) =
            JunctionPrograms.BroadcastTwoSinks();

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(first.Completion, "the first run completes");
        await Reaches(second.Completion, "the second run completes");

        Assert.Equal(2L, await first.GetValueAsync(counted, TestToken));
        Assert.Equal(2L, await second.GetValueAsync(counted, TestToken));
        Assert.Equal(30m, await second.GetValueAsync(totaled, TestToken));
    }
}
