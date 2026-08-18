using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the authored junctions actually carry: the values, not only the counts.
/// </summary>
/// <remarks>
/// <para>
/// The nine programs prove the shapes run. They do not prove that the combiner a zip is bound to builds the
/// row the author wrote, that an unzip's halves reach the halves they are named after, or that an
/// interleave's segment size means what its payload says — a count comes out the same when a projection is
/// swapped. These are the tests that would fail if the authoring surface handed the engine the right shape
/// with the wrong behavior bound to it.
/// </para>
/// <para>
/// Every element type here is chosen so that a mix-up cannot pass: the halves of a pair are a string and an
/// integer, and the two sides of a zip are distinguishable in the row they build.
/// </para>
/// </remarks>
public sealed class FluentJunctionValueTests
{
    [Fact]
    public async Task TheTupleZipPairsTheInputsInTheOrderTheyWereWritten()
    {
        // First is the receiver's element and Second is the argument's. Nothing else in the suite would
        // notice them being swapped, because both halves are present either way.
        RunnableGraph graph = Source.From<int>([1, 2])
            .Zip(Source.From<string>(["a", "b"]))
            .Select(row => $"{row.First}{row.Second}")
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "rows", out ResultSlot<IReadOnlyList<string>> rows);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both inputs have");

        Assert.Equal(["1a", "2b"], await run.GetValueAsync(rows, TestToken));
    }

    [Fact]
    public async Task CombineLatestBuildsARowFromEachArrivalAndTheOtherSidesLatest()
    {
        // One element on the left and three on the right, so every row has to carry the one left element:
        // that is the whole difference from a zip, which would have produced a single row and stopped.
        RunnableGraph graph = Source.From<string>(["setting"])
            .CombineLatest(Source.From<int>([1, 2, 3]), (setting, value) => $"{setting}:{value}")
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "rows", out ResultSlot<IReadOnlyList<string>> rows);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both inputs have");

        IReadOnlyList<string> collected = await run.GetValueAsync(rows, TestToken);

        // How many rows a combine-latest emits depends on how the two inputs interleave in time, which is a
        // scheduling fact and not a contract. What is a contract: every row pairs the one left element with
        // a right element, the last right element is represented, and no right element is invented.
        Assert.NotEmpty(collected);
        Assert.All(collected, row => Assert.StartsWith("setting:", row, StringComparison.Ordinal));
        Assert.Contains("setting:3", collected);
        Assert.All(collected, row => Assert.Contains(row, (string[])["setting:1", "setting:2", "setting:3"]));
    }

    [Fact]
    public async Task AnInterleaveTakesItsDeclaredSegmentFromEachInputInTurn()
    {
        // The segment size is the one number a junction writes into its document, and this is the sequence
        // it buys: two from the left, two from the right, and on until both run out.
        RunnableGraph graph = Source.From<int>([1, 2, 3, 4, 5, 6])
            .Interleave(Source.From<int>([10, 20, 30]), 2)
            .To(s => s.Collect(new CollectOptions { MaxElements = 16 }), "joined", out ResultSlot<IReadOnlyList<int>> joined);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both inputs have");

        Assert.Equal([1, 2, 10, 20, 3, 4, 30, 5, 6], await run.GetValueAsync(joined, TestToken));
    }

    [Fact]
    public async Task AConcatEmitsItsFirstInputToTheEndBeforeItsSecond()
    {
        RunnableGraph graph = Source.From<int>([1, 2, 3])
            .Concat(Source.From<int>([10, 20]))
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "joined", out ResultSlot<IReadOnlyList<int>> joined);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both inputs have");

        Assert.Equal([1, 2, 3, 10, 20], await run.GetValueAsync(joined, TestToken));
    }

    [Fact]
    public async Task AnUnzipSendsTheLeftHalfLeftAndTheRightHalfRight()
    {
        // Two differently typed halves collected rather than counted, because a count comes out the same
        // when the two projections are swapped and the values do not.
        RunnableGraph graph = Source.From<(string Name, int Age)>([("ada", 36), ("alan", 41)])
            .UnzipTo(
                Flow.For<string>().To(
                    s => s.Collect(new CollectOptions { MaxElements = 4 }),
                    "names",
                    out ResultSlot<IReadOnlyList<string>> names),
                Flow.For<int>().To(
                    s => s.Collect(new CollectOptions { MaxElements = 4 }),
                    "ages",
                    out ResultSlot<IReadOnlyList<int>> ages));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both halves have");

        Assert.Equal(["ada", "alan"], await run.GetValueAsync(names, TestToken));
        Assert.Equal([36, 41], await run.GetValueAsync(ages, TestToken));
    }

    [Fact]
    public async Task APartitionSendsEachElementToTheBranchItsRouterNames()
    {
        // The router's answer is the leg's position, so a partition that ignored it — or read it as
        // something else — would show up as elements on the wrong side rather than as a different count.
        RunnableGraph graph = Source.From<int>([1, 2, 3, 4, 5])
            .PartitionTo(
                value => value % 2,
                Flow.For<int>().To(
                    s => s.Collect(new CollectOptions { MaxElements = 8 }),
                    "even",
                    out ResultSlot<IReadOnlyList<int>> even),
                Flow.For<int>().To(
                    s => s.Collect(new CollectOptions { MaxElements = 8 }),
                    "odd",
                    out ResultSlot<IReadOnlyList<int>> odd));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both classes have");

        Assert.Equal([2, 4], await run.GetValueAsync(even, TestToken));
        Assert.Equal([1, 3, 5], await run.GetValueAsync(odd, TestToken));
    }

    [Fact]
    public async Task ATapSeesEveryElementTheMainLineSees()
    {
        // A broadcast delivers to every leg, so the tap is not a sample: it is the same stream, and the
        // main line's own filtering happens downstream of the junction rather than before it.
        RunnableGraph graph = Source.From<int>([1, 2, 3])
            .AlsoTo(Flow.For<int>().To(
                s => s.Collect(new CollectOptions { MaxElements = 8 }),
                "audited",
                out ResultSlot<IReadOnlyList<int>> audited))
            .Where(value => value > 2)
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "kept", out ResultSlot<IReadOnlyList<int>> kept);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the main line and the tap have");

        Assert.Equal([1, 2, 3], await run.GetValueAsync(audited, TestToken));
        Assert.Equal([3], await run.GetValueAsync(kept, TestToken));
    }

    [Fact]
    public async Task AForkThroughTwoIdentityFlowsPairsEachElementWithItself()
    {
        // Both legs contribute no occurrence at all, so the broadcast's own leg ports are wired straight to
        // the zip's inputs. That is the smallest diamond expressible, and it is the one where nothing but
        // the junctions is in the document.
        RunnableGraph graph = Source.From<int>([1, 2])
            .Fork(Flow.For<int>(), Flow.For<int>())
            .Zip((left, right) => left + right)
            .To(s => s.Collect(new CollectOptions { MaxElements = 4 }), "doubled", out ResultSlot<IReadOnlyList<int>> doubled);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the rejoined stream has");

        Assert.Equal([2, 4], await run.GetValueAsync(doubled, TestToken));
    }

    [Fact]
    public async Task AFanInFeedsAFanOutThroughOneGraph()
    {
        // The composition claim: a junction call returns an ordinary source, so what follows it is the whole
        // vocabulary and not a restricted one. Two sources merge, the merged stream is filtered, and the
        // result is broadcast to two branches.
        RunnableGraph graph = Source.From<int>([1, 2, 3])
            .Merge(Source.From<int>([10, 20]))
            .Where(value => value != 2)
            .BroadcastTo(
                Flow.For<int>().To(s => s.Count(), "counted", out ResultSlot<long> counted),
                Flow.For<int>().To(s => s.Aggregate(0, (sum, value) => sum + value), "summed", out ResultSlot<int> summed));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both branches have");

        Assert.Equal(4L, await run.GetValueAsync(counted, TestToken));
        Assert.Equal(34, await run.GetValueAsync(summed, TestToken));
    }

    [Fact]
    public async Task TwoTapsInARowAreTwoJunctionsAndBothSeeEverything()
    {
        RunnableGraph graph = Source.From<int>([1, 2])
            .AlsoTo(Flow.For<int>().To(s => s.Count(), "first", out ResultSlot<long> first))
            .AlsoTo(Flow.For<int>().To(s => s.Count(), "second", out ResultSlot<long> second))
            .To(s => s.Count(), "kept", out ResultSlot<long> kept);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both taps and the main line have");

        Assert.Equal(2, graph.Document.Nodes.Count(node => node.Stage.Stage.Value == "broadcast"));
        Assert.Equal(2L, await run.GetValueAsync(first, TestToken));
        Assert.Equal(2L, await run.GetValueAsync(second, TestToken));
        Assert.Equal(2L, await run.GetValueAsync(kept, TestToken));
    }

    [Fact]
    public async Task ARuntimeControlDeclaredInsideAJunctionGraphStillResolves()
    {
        // A control is named on the stage that produces it rather than by the closing call, so a junction
        // graph has to carry it out of whichever input or branch it stands in. Here it stands on one input
        // of a merge, which is a position no chain could put it in.
        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "ingress")
            .Merge(Source.From<int>([10, 20]))
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        ResultSlot<IIngressQueue<int>> control = graph.Control<IIngressQueue<int>>("ingress");

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));

        queue.Complete();

        await Reaches(run.Completion, "the run completes when the queue is closed and the sequence has ended");

        Assert.Equal([1, 2, 10, 20], (await run.GetValueAsync(seen, TestToken)).Order());
    }

    [Fact]
    public async Task ATapOnAJoinedSourceKeepsItsSlotOnItsOwnBranch()
    {
        // The positions of everything the right-hand source carries move when it is placed beside the left,
        // and a result its tap already asked for moves with them. If it did not, the slot would resolve
        // whatever occurrence happens to stand at the old position — which, in a graph this size, is a
        // different sink that also produces a number.
        RunnableGraph graph = Source.From<int>([1, 2, 3, 4])
            .Merge(
                Source.From<int>([10, 20])
                    .AlsoTo(Flow.For<int>().To(s => s.Count(), "tapped", out ResultSlot<long> tapped))
                    .Select(value => value + 1))
            .To(s => s.Count(), "joined", out ResultSlot<long> joined);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both inputs and the tap have");

        Assert.Equal(2L, await run.GetValueAsync(tapped, TestToken));
        Assert.Equal(6L, await run.GetValueAsync(joined, TestToken));
    }

    [Fact]
    public async Task OneSourceMergedWithItselfIsTwoIndependentStreams()
    {
        // A value composed twice contributes its occurrences twice, so this is two enumerations of one
        // sequence rather than one stream forked in two. Stating it as a test because the alternative
        // reading — that the two inputs share a source — would be a very different graph.
        Source<int> numbers = Source.From<int>([1, 2, 3]);

        RunnableGraph graph = numbers.Merge(numbers)
            .To(s => s.Aggregate(0, (sum, value) => sum + value), "total", out ResultSlot<int> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both copies have");

        Assert.Equal(2, graph.Document.Nodes.Count(node => node.Stage.Stage.Value == "from-enumerable"));
        Assert.Equal(12, await run.GetValueAsync(total, TestToken));
    }
}
