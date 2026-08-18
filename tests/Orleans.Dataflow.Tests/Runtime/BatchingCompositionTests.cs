using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// Where the wave-2 operators meet the rest of the engine: a junction, an asynchronous boundary, another
/// batch, and the operators that end a stream.
/// </summary>
/// <remarks>
/// <para>
/// Wave 1 recorded that none of its operators was proven across a junction and that "compose by
/// construction" is an argument rather than a measurement. These operators hold elements back and emit them
/// when their stream ends, which is a second thing that could compose wrongly, so this is where the argument
/// is measured: a batch on a leg of a fan-out, a batch feeding a fan-in, a batch below an asynchronous
/// boundary, and two of them in one chain.
/// </para>
/// <para>
/// The one that could go silently wrong is the last: a residue travels through the stages below the one that
/// produced it, so an operator that ended the stream has to refuse a residue offered to it afterwards. That
/// is asserted directly rather than reasoned about.
/// </para>
/// </remarks>
public sealed class BatchingCompositionTests
{
    [Fact]
    public async Task ABatchOnEachLegOfABroadcastEmitsItsOwnPartialGroup()
    {
        List<IReadOnlyList<int>> left = [];
        List<IReadOnlyList<int>> right = [];

        RunnableGraph graph = Source.Range(1, 5)
            .BroadcastTo(
                Flow.For<int>().Grouped(2).To(s => s.ForEach(left.Add)),
                Flow.For<int>().Grouped(3).To(s => s.ForEach(right.Add)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Each leg is its own segment with its own batch and its own end of stream, so each answers for the
        // group it was holding rather than for the other's.
        Assert.Equal([[1, 2], [3, 4], [5]], left.Select(group => group.ToArray()));
        Assert.Equal([[1, 2, 3], [4, 5]], right.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task ABatchFeedingAMergeDeliversItsPartialGroupThroughTheJunction()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 4)
            .Grouped(3)
            .Merge(Source.Range(10, 4).Grouped(3))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Four groups reach the merge: two full and two partial, one of each from each input. A merge makes
        // no promise about their order, so the assertion is over the set of groups.
        Assert.Equal(4, observed.Count);
        Assert.Contains(observed, group => group.SequenceEqual([1, 2, 3]));
        Assert.Contains(observed, group => group.SequenceEqual([4]));
        Assert.Contains(observed, group => group.SequenceEqual([10, 11, 12]));
        Assert.Contains(observed, group => group.SequenceEqual([13]));
    }

    [Fact]
    public async Task ATimedBatchOnALegOfABroadcastIsWokenByItsOwnWindow()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<IReadOnlyList<int>> batched = [];
        List<int> plain = [];

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "in")
            .BroadcastTo(
                Flow.For<int>().GroupedWithin(10, Second).To(s => s.ForEach(batched.Add)),
                Flow.For<int>().To(s => s.ForEach(plain.Add)));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("in"), TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        await Reaches(() => plain.Count == 1, "the element reaching the plain leg", TestToken);

        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => batched.Count == 1, "the window closing on the batched leg", TestToken);

        Assert.Equal([1], batched[0]);

        queue.Complete();
        await run.Completion;
    }

    [Fact]
    public async Task ABatchBelowAnAsynchronousBoundaryDeliversItsPartialGroup()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 5)
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 2 }, (value, _) => Task.FromResult(value * 2))
            .Grouped(3)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The batch is fused below the asynchronous stage rather than in its own segment, so the end of the
        // stream it sees is the asynchronous pump running out of both input and callbacks.
        Assert.Equal([[2, 4, 6], [8, 10]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AFlatteningStageBelowAnAsynchronousBoundaryStillFlattens()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.Range(1, 3)
            .SelectAsync(new ParallelismOptions { MaxConcurrency = 1 }, (value, _) => Task.FromResult(value))
            .SelectMany(value => new[] { value, -value })
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, -1, 2, -2, 3, -3], observed);
    }

    [Fact]
    public async Task TwoBatchesInOneChainEachAnswerForWhatTheyHeld()
    {
        List<IReadOnlyList<IReadOnlyList<int>>> observed = [];

        RunnableGraph graph = Source.Range(1, 7)
            .Grouped(2)
            .Grouped(2)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The first batch's residue reaches the second one as an ordinary element and joins its open group,
        // which then leaves as the second one's own residue. Four groups in, two pairs and a single out.
        Assert.Equal(2, observed.Count);
        Assert.Equal([[1, 2], [3, 4]], observed[0].Select(group => group.ToArray()));
        Assert.Equal([[5, 6], [7]], observed[1].Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AStreamEndedByAStageBelowABatchIsNotReopenedByThatBatchsResidue()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.From([1, 2, 9, 10, 3])
            .Grouped(2)
            .TakeWhile(group => group[0] < 5)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The group [9, 10] ends the stream, and the element after it is never pulled — so the batch is
        // holding nothing when the segment stops and there is no residue to offer. Worth asserting rather
        // than assuming: the residue walk runs on the same path, and a batch that had kept the element
        // would hand [3] to a predicate that accepts it, past a boundary already closed.
        Assert.Equal([[1, 2]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AResidueOfferedIntoABoundaryADownstreamStopClosedIsAbandoned()
    {
        RunnableGraph graph = Source.Range(1, 100)
            .Grouped(3)
            .Buffer(new BufferOptions { Capacity = 1 })
            .Take(1)
            .To(
                s => s.Collect(new CollectOptions { MaxElements = 8 }),
                "groups",
                out ResultSlot<IReadOnlyList<IReadOnlyList<int>>> groups);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The take is below a boundary, so it stops the batch's segment from underneath rather than from
        // inside it: the batch is left holding a partial group with nowhere to put it. The group is
        // abandoned exactly as any element arriving at a channel a downstream completion closed is, which
        // is why the run ends successfully and delivers one group rather than two.
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(
            [[1, 2, 3]],
            (await run.GetValueAsync(groups, TestToken)).Select(group => group.ToArray()));
    }

    [Fact]
    public async Task ATakeThatEndedTheStreamRefusesTheResidueOfferedToItAfterwards()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 7).Grouped(2).Take(2).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([[1, 2], [3, 4]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task ATimedBatchAboveATakeIsRefusedByItAfterTheBoundIsSpent()
    {
        LocalDataflowHost host = Timed(out TestClock _);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 7)
            .GroupedWithin(2, Second)
            .Take(2)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([[1, 2], [3, 4]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task ADivertedBranchCanBatchWhatItReceives()
    {
        List<IReadOnlyList<int>> diverted = [];
        List<int> onward = [];

        RunnableGraph graph = Source.Range(1, 9)
            .DivertTo(value => value % 3 == 0, Flow.For<int>().Grouped(2).To(s => s.ForEach(diverted.Add)))
            .To(s => s.ForEach(onward.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([[3, 6], [9]], diverted.Select(group => group.ToArray()));
        Assert.Equal([1, 2, 4, 5, 7, 8], onward);
    }

    [Fact]
    public async Task AFlatteningStageFeedingAJunctionSplitsEveryInnerElement()
    {
        List<int> left = [];
        List<int> right = [];

        RunnableGraph graph = Source.From([1, 2])
            .SelectMany(value => new[] { value, value * 10 })
            .BroadcastTo(
                Flow.For<int>().To(s => s.ForEach(left.Add)),
                Flow.For<int>().To(s => s.ForEach(right.Add)));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Each inner element is an element of the junction's stream, so both legs see all four.
        Assert.Equal([1, 10, 2, 20], left);
        Assert.Equal([1, 10, 2, 20], right);
    }

    [Fact]
    public async Task ACountClosedBatchFusesAndATimedOneDoesNot()
    {
        RecordingEnumerable<int> fused = new(1, 2, 3, 4, 5, 6);
        RecordingEnumerable<int> cut = new(1, 2, 3, 4, 5, 6);
        Gate holding = new();
        Gate open = new();

        RunnableGraph grouped = Source.From(fused)
            .Grouped(2)
            .To(s => s.ForEach(_ =>
            {
                holding.Wait();
                fused.Consumed();
                fused.Consumed();
            }));

        RunnableGraph timed = Source.From(cut)
            .GroupedWithin(2, TimeSpan.FromHours(1))
            .To(s => s.ForEach(_ =>
            {
                open.Wait();
                cut.Consumed();
                cut.Consumed();
            }));

        await using RunHandle first = await Host.MaterializeAsync(grouped, TestToken);
        await holding.Reached;

        // A batch closed by a count is an ordinary fused stage: the source has been asked for exactly the
        // two elements the group holds and no more, because there is no queue in front of it.
        Assert.Equal(2, fused.Pulls);

        holding.Open();
        await first.Completion;

        await using RunHandle second = await Host.MaterializeAsync(timed, TestToken);
        await open.Reached;
        await Reaches(() => cut.Pulls > 2, "the handoff in front of the timed batch filling", TestToken);

        // A batch closed by a clock is a boundary, so there is one handoff in front of it and the source
        // runs ahead by it. That is the price of being able to emit while nothing is arriving, and it is
        // asserted rather than described.
        Assert.True(cut.Pulls > 2, $"the timed batch fused: the source was pulled {cut.Pulls} times");

        open.Open();
        await second.Completion;
    }

    [Fact]
    public async Task ABatchAboveADroppingBufferIsAnsweredByThatPolicy()
    {
        RunnableGraph graph = Source.Range(1, 9)
            .Grouped(1)
            .Buffer(new BufferOptions { Capacity = 1, OverflowPolicy = OverflowPolicy.DropNewest })
            .To(TestSink.Probe<IReadOnlyList<int>>("out"));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        ISinkProbe<IReadOnlyList<int>> sink =
            await run.GetValueAsync(graph.Control<ISinkProbe<IReadOnlyList<int>>>("out"), TestToken);

        // The groups a batch emits are elements like any others: the boundary below it applies its own
        // policy to them, and the run does not fail.
        Assert.Equal([1], await sink.ReceiveAsync(TestToken));

        await run.ShutdownAsync();
        await run.Completion;
    }
}
