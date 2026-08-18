using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;
using static Orleans.Dataflow.Tests.Runtime.TimingFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a batch closed by a size, a weight, or a clock promises, measured on a clock a test moves by hand.
/// </summary>
/// <remarks>
/// <para>
/// The claim these tests exist for is the one no count-closed batch can make: a group is emitted while
/// nothing is arriving. That is only assertable at all because the clock is controlled — with a real one a
/// test could only wait and hope — and it is why every graph here is materialized through a host measuring
/// by a <see cref="TestClock"/>.
/// </para>
/// <para>
/// The window belongs to the group rather than to the stage, so the assertions are written from the arrival
/// of a group's first element rather than from the start of the run. The two differ exactly when the stream
/// goes quiet, which is the case worth pinning: an empty window emits nothing because there is no group open
/// to time.
/// </para>
/// </remarks>
public sealed class GroupedWithinTests
{
    [Fact]
    public async Task AGroupIsEmittedWhenItReachesItsSizeWithoutTheClockMoving()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 6)
            .GroupedWithin(3, Second)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The clock never moved. The size is the first of the two bounds to be reached here, and reaching it
        // is what closes the group.
        Assert.Equal(TimeSpan.Zero, clock.GetElapsedTime(clock.GetTimestamp(), clock.GetTimestamp()));
        Assert.Equal([[1, 2, 3], [4, 5, 6]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AGroupIsEmittedWhenItsWindowClosesWithNothingArriving()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "in")
            .GroupedWithin(10, Second)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("in"), TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));

        // One element and a bound of ten: nothing a count could close, so the window is what closes it.
        // Acceptance into the ingress says nothing about arrival at the stage — the element may still be
        // travelling through the handoff when an advance lands, and a window cannot be armed by an element
        // that is not there yet — so the fact awaited before the clock moves is the arming itself.
        await clock.WaitForTimersAsync(1, TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => observed.Count == 1, "the first window closing", TestToken);

        Assert.Equal([1], observed[0]);

        // The next window is timed from its own first element, so a second element offered after the
        // first group left starts a fresh window and leaves in a group of its own.
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));

        await clock.WaitForTimersAsync(1, TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => observed.Count == 2, "the second window closing", TestToken);

        Assert.Equal([2], observed[1]);

        queue.Complete();
        await run.Completion;

        Assert.Equal(2, observed.Count);
    }

    [Fact]
    public async Task AWindowWithNoElementsInItEmitsNothing()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "in")
            .GroupedWithin(10, Second)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("in"), TestToken);

        // No group is open, so no window is running. Ten windows' worth of time passes and nothing is
        // emitted, because an empty group is not a group.
        clock.Advance(Second * 10);
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        Assert.Empty(observed);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(7, TestToken));

        // The window that closes this group is timed from this element's arrival and not from the run's
        // start, which the ten idle windows would otherwise have passed long ago. The arming is the fact
        // awaited before the clock moves: acceptance into the ingress is not arrival at the stage.
        await clock.WaitForTimersAsync(1, TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => observed.Count == 1, "the window after the quiet", TestToken);

        Assert.Equal([7], observed[0]);

        queue.Complete();
        await run.Completion;
    }

    [Fact]
    public async Task AGroupIsNotEmittedOneTickBeforeItsWindowCloses()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "in")
            .GroupedWithin(10, Second)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("in"), TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));

        // Without this rendezvous the test can pass without testing anything: an advance landing before
        // the element reaches the stage arms nothing, and an empty observed list would then be the
        // element's absence rather than the window's patience.
        await clock.WaitForTimersAsync(1, TestToken);
        await clock.AdvanceAsync(1, Second - Instant, TestToken);
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        // One tick short of the deadline is still inside the window.
        Assert.Empty(observed);

        clock.Advance(Instant);
        await Reaches(() => observed.Count == 1, "the window closing", TestToken);

        Assert.Equal([1], observed[0]);

        queue.Complete();
        await run.Completion;
    }

    [Fact]
    public async Task TheEndOfTheStreamEmitsTheOpenGroup()
    {
        LocalDataflowHost host = Timed(out TestClock _);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Range(1, 7)
            .GroupedWithin(3, Second)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The same answer a count-closed batch gives, and the same reason: the elements arrived and were
        // accepted, so the group they are in is emitted when nothing more can join it.
        Assert.Equal([[1, 2, 3], [4, 5, 6], [7]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AWeightedGroupClosesBeforeTheElementThatWouldBreakItsBound()
    {
        LocalDataflowHost host = Timed(out TestClock _);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.From([3, 3, 3, 1])
            .GroupedWithin(100, 7, Second, weight => weight)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // Six fits under seven and nine does not, so the third element starts the next group rather than
        // pushing the first one past its bound. The bound is never exceeded, which is the whole promise.
        Assert.Equal([[3, 3], [3, 1]], observed.Select(group => group.ToArray()));
        Assert.All(observed, group => Assert.True(group.Sum() <= 7));
    }

    [Fact]
    public async Task AWeightedGroupStillClosesOnItsElementCount()
    {
        LocalDataflowHost host = Timed(out TestClock _);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.From([1, 1, 1, 1, 1])
            .GroupedWithin(2, 1000, Second, _ => 1)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        // The weight bound is never approached here; the count is the first of the three to be reached.
        Assert.Equal([[1, 1], [1, 1], [1]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task AWeightedGroupIsEmittedWhenItsWindowClosesWithNothingArriving()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "in")
            .GroupedWithin(100, 1000, Second, weight => weight)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("in"), TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(5, TestToken));

        await clock.WaitForTimersAsync(1, TestToken);
        await clock.AdvanceAsync(1, Second, TestToken);
        await Reaches(() => observed.Count == 1, "the weighted window closing", TestToken);

        Assert.Equal([5], observed[0]);

        queue.Complete();
        await run.Completion;
    }

    [Fact]
    public async Task AnElementHeavierThanTheWholeBoundFailsTheRun()
    {
        LocalDataflowHost host = Timed(out TestClock _);

        RunnableGraph graph = Source.From([1, 50])
            .GroupedWithin(100, 7, Second, weight => weight)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        // No group of this batch could ever carry it, so waiting for one that could would never end.
        Assert.Contains("50", refused.Message, StringComparison.Ordinal);
        Assert.Contains("7", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ANegativeWeightFailsTheRun()
    {
        LocalDataflowHost host = Timed(out TestClock _);

        RunnableGraph graph = Source.From([1, -4])
            .GroupedWithin(100, 7, Second, weight => weight)
            .To(s => s.ForEach(_ => { }));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Contains("-4", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PausingARunWaitingForAWindowReachesQuiescenceWithoutTheClockMoving()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "in")
            .GroupedWithin(10, Second)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("in"), TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        await clock.WaitForTimersAsync(1, TestToken);

        // The batch's own wait is this runtime's own — it sleeps on its input channel and on the run's
        // wakeup latch — so it reports itself and the run comes to rest inside it with the clock still.
        await run.PauseAsync(TestToken).WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        Assert.True(run.IsPaused);
        Assert.Empty(observed);

        // The window closes while the run is held. Time passes for a paused run, and a paused run still
        // takes no step: the group waits at the safe point rather than being delivered.
        clock.Advance(Second * 10);
        await Task.Delay(TimeSpan.FromMilliseconds(50), TestToken);

        Assert.Empty(observed);

        await run.ResumeAsync();
        await Reaches(() => observed.Count == 1, "the group held across the pause", TestToken);

        Assert.Equal([1], observed[0]);

        queue.Complete();
        await run.Completion;
    }

    [Fact]
    public async Task AShutdownDeliversTheGroupTheBatchWasHolding()
    {
        LocalDataflowHost host = Timed(out TestClock clock);
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "in")
            .GroupedWithin(10, Second)
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("in"), TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));

        // The armed timer is what says the element reached the batch and opened a group: the window is
        // armed from a group's first element, so a timer existing is the batch holding one.
        await clock.WaitForTimersAsync(1, TestToken);

        await run.ShutdownAsync();
        await run.Completion;

        // A shutdown ends the stream as running out would, so the open group leaves rather than being
        // abandoned. What is still upstream of the batch is the ingress queue's own question and not this
        // one; nothing here claims a shutdown drains a queue.
        Assert.Equal([[1]], observed.Select(group => group.ToArray()));
    }

    [Fact]
    public async Task ACancelledRunAbandonsTheGroupAndDisposalReturns()
    {
        LocalDataflowHost host = Timed(out TestClock _);
        using CancellationTokenSource cancellation = new();
        List<IReadOnlyList<int>> observed = [];

        RunnableGraph graph = Source.Queue<int>(new BufferOptions { Capacity = 4 }, "in")
            .GroupedWithin(10, Second)
            .To(s => s.ForEach(observed.Add));

        RunHandle run = await host.MaterializeAsync(graph, cancellation.Token);

        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("in"), TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));

        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);

        // The load-bearing claim is that disposal returns: a segment that could not be woken from its own
        // wait would hang here rather than fail.
        await run.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30), TestToken);

        Assert.Empty(observed);
    }

    [Fact]
    public async Task TwoRunsOfOneGraphTimeTheirWindowsIndependently()
    {
        LocalDataflowHost host = Timed(out TestClock _);
        List<IReadOnlyList<int>> first = [];
        List<IReadOnlyList<int>> second = [];
        List<IReadOnlyList<int>> observed = first;

        RunnableGraph graph = Source.Range(1, 4)
            .GroupedWithin(3, Second)
            .To(s => s.ForEach(group => observed.Add(group)));

        await using (RunHandle one = await host.MaterializeAsync(graph, TestToken))
        {
            await one.Completion;
        }

        observed = second;

        await using (RunHandle two = await host.MaterializeAsync(graph, TestToken))
        {
            await two.Completion;
        }

        Assert.Equal([[1, 2, 3], [4]], first.Select(group => group.ToArray()));
        Assert.Equal([[1, 2, 3], [4]], second.Select(group => group.ToArray()));
    }
}
