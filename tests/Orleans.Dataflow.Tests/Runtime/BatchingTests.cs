using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the batching operators closed by a count promise: which groups come out, what happens to the
/// elements still held when the stream ends, and what a second run of the same graph starts from.
/// </summary>
/// <remarks>
/// <para>
/// The groups are asserted element by element rather than by count, because an operator that emitted the
/// right number of the wrong groups would pass a count. The last group is the interesting one everywhere: it
/// is the only one a count can never close, so it is the one the end of the stream has to answer for, and
/// every test here says what it expects of it.
/// </para>
/// <para>
/// Grouping is the first shape of this vocabulary that holds elements back rather than answering each one as
/// it arrives, so a second run of one graph is materialized wherever the stage carries state — which is
/// everywhere here — because state that leaked from one run to the next is exactly what a single run cannot
/// show.
/// </para>
/// </remarks>
public sealed class BatchingTests
{
    [Fact]
    public async Task GroupedEmitsFullGroupsAndThePartialLastOne()
    {
        RunnableGraph graph = Source.Range(1, 7)
            .Grouped(3)
            .To(
                s => s.Collect(new CollectOptions { MaxElements = 8 }),
                "groups",
                out ResultSlot<IReadOnlyList<IReadOnlyList<int>>> groups);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Seven elements by three: two full groups and a last one holding what was left. The partial group
        // is emitted because its elements arrived and were accepted, not because the count was reached.
        Assert.Equal(
            [[1, 2, 3], [4, 5, 6], [7]],
            (await run.GetValueAsync(groups, TestToken)).Select(group => group.ToArray()));
    }

    [Fact]
    public async Task GroupedOverALengthThatDividesEmitsNoPartialGroup()
    {
        RunnableGraph graph = Source.Range(1, 6)
            .Grouped(3)
            .To(
                s => s.Collect(new CollectOptions { MaxElements = 8 }),
                "groups",
                out ResultSlot<IReadOnlyList<IReadOnlyList<int>>> groups);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // An empty group is not a group. A stream whose length is a multiple of the size gives exactly the
        // groups it filled and nothing after them.
        Assert.Equal(
            [[1, 2, 3], [4, 5, 6]],
            (await run.GetValueAsync(groups, TestToken)).Select(group => group.ToArray()));
    }

    [Fact]
    public async Task GroupedOverAnEmptyStreamEmitsNothingAtAll()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Empty<int>().Grouped(3).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(observed);
    }

    [Fact]
    public async Task EveryRunOfAGroupedGraphStartsWithAnEmptyGroup()
    {
        List<IReadOnlyList<int>> first = [];
        List<IReadOnlyList<int>> second = [];
        List<IReadOnlyList<int>> observed = first;

        RunnableGraph graph = Source.Range(1, 4).Grouped(3).To(s => s.ForEach(group => observed.Add(group)));

        await using (RunHandle one = await Host.MaterializeAsync(graph, TestToken))
        {
            await one.Completion;
        }

        observed = second;

        await using (RunHandle two = await Host.MaterializeAsync(graph, TestToken))
        {
            await two.Completion;
        }

        // The second run does not continue the first one's open group: both see the same two groups, and
        // the partial one holds one element rather than growing across runs.
        Assert.Equal([[1, 2, 3], [4]], first.Select(group => group.ToArray()));
        Assert.Equal([[1, 2, 3], [4]], second.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AGroupIsAListOfItsOwnThatNothingLaterTouches()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 4).Grouped(2).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Two groups, and the first is still the first after the second was built: the stage copies a group
        // out rather than handing over the buffer it goes on filling.
        Assert.Equal(2, observed.Count);
        Assert.NotSame(observed[0], observed[1]);
        Assert.Equal([1, 2], observed[0]);
        Assert.Equal([3, 4], observed[1]);
    }

    [Fact]
    public async Task ATakeBelowAGroupedRefusesThePartialGroupPastItsBound()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 7).Grouped(3).Take(1).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The stream ended at the take, so the group the batch was still holding has nowhere to go: a spent
        // take refuses it exactly as it refuses any element past its bound.
        Assert.Equal([[1, 2, 3]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task ATakeAboveAGroupedStillDeliversThePartialGroupItLeftBehind()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 100).Take(5).Grouped(3).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The take ends the stream at the fifth element, which is the batch's end of stream: the two
        // elements it was holding are a group and are emitted, exactly as a source running out would leave
        // them.
        Assert.Equal([[1, 2, 3], [4, 5]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AGroupedBelowABufferDeliversItsPartialGroupAcrossTheBoundary()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 5)
            .Buffer(new BufferOptions { Capacity = 2 })
            .Grouped(3)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The batch is in a segment of its own here rather than fused into the source's, so what ends its
        // stream is its input channel completing. The answer is the same one, which is the point.
        Assert.Equal([[1, 2, 3], [4, 5]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task GroupedOfOneIsEveryElementInAListOfItsOwn()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 3).Grouped(1).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([[1], [2], [3]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task SlidingOverlapsWindowsWhenTheStepIsSmallerThanTheSize()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 4).Sliding(3, 1).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The moving average's shape. Nothing is emitted at the end here, because everything the stage was
        // still holding has already appeared in the window before it.
        Assert.Equal([[1, 2, 3], [2, 3, 4]], observed.Select(window => window.ToArray()));
    }

    [Fact]
    public async Task SlidingPartitionsTheStreamWhenTheStepEqualsTheSize()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 5).Sliding(2, 2).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Grouped written the long way, trailing partial window and all.
        Assert.Equal([[1, 2], [3, 4], [5]], observed.Select(window => window.ToArray()));
    }

    [Fact]
    public async Task SlidingSamplesTheStreamWhenTheStepIsLargerThanTheSize()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 8).Sliding(2, 3).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The elements between two windows are passed over and never carried: 3 and 6 appear in no window,
        // and 8 is left in the buffer having never been in one, so it leaves as the final partial window.
        Assert.Equal([[1, 2], [4, 5], [7, 8]], observed.Select(window => window.ToArray()));
    }

    [Fact]
    public async Task SlidingOverAStreamShorterThanTheWindowEmitsEverythingItHad()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 2).Sliding(5, 1).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // No window ever filled, so everything the stage held is unseen and leaves as one final window.
        Assert.Equal([[1, 2]], observed.Select(window => window.ToArray()));
    }

    [Fact]
    public async Task SlidingOverAnEmptyStreamEmitsNothingAtAll()
    {
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Empty<int>().Sliding(3, 1).To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Empty(observed);
    }

    [Fact]
    public async Task EveryRunOfASlidingGraphStartsWithAnEmptyWindow()
    {
        List<IReadOnlyList<int>> first = [];
        List<IReadOnlyList<int>> second = [];
        List<IReadOnlyList<int>> observed = first;

        RunnableGraph graph = Source.Range(1, 3).Sliding(2, 1).To(s => s.ForEach(window => observed.Add(window)));

        await using (RunHandle one = await Host.MaterializeAsync(graph, TestToken))
        {
            await one.Completion;
        }

        observed = second;

        await using (RunHandle two = await Host.MaterializeAsync(graph, TestToken))
        {
            await two.Completion;
        }

        Assert.Equal([[1, 2], [2, 3]], first.Select(window => window.ToArray()));
        Assert.Equal([[1, 2], [2, 3]], second.Select(window => window.ToArray()));
    }

    [Fact]
    public async Task AFailingStageBelowABatchFailsTheRunWithItsOwnException()
    {
        InvalidOperationException failure = new("the group is wrong");

        RunnableGraph graph = Source.Range(1, 5)
            .Grouped(2)
            .Select<int>(group => throw failure)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion));
    }

    [Fact]
    public async Task ACancelledRunAbandonsTheGroupTheBatchWasHolding()
    {
        using CancellationTokenSource cancellation = new();
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 100)
            .Grouped(3)
            .To(s => s.ForEach(group =>
            {
                observed.Add(group);
                cancellation.Cancel();
            }));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);

        // Exactly the group that was delivered before the cancellation. What the stage was holding when the
        // run was abandoned is abandoned with it, which is what a cancellation does to every element in
        // flight.
        Assert.Single(observed);
        Assert.Equal([1, 2, 3], observed[0]);
    }
}
