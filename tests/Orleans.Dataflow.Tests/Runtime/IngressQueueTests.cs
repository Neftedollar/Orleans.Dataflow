using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The bounded ingress queue: what an offer answers in every state the queue can be in, what completing and
/// failing it do to the run, and what a control slot is.
/// </summary>
/// <remarks>
/// <para>
/// The claim under all of these is that an offer never throws for the state of the queue. A producer meets
/// a full queue, a completed queue, and an ended run in the ordinary course of its work, so all three are
/// values of <see cref="QueueOfferOutcome"/> and none of them is an exception; the only exception an offer
/// can raise is the caller's own cancellation, which is the caller's business.
/// </para>
/// <para>
/// The policies are asserted one at a time against a queue held full by a gate in the sink, because "full"
/// is otherwise a race: the reader takes elements as fast as the producer offers them. With the reader held
/// inside the sink, the queue's contents at the moment of an offer are a fact.
/// </para>
/// </remarks>
public sealed class IngressQueueTests
{
    [Fact]
    public async Task OfferedElementsReachTheSinkInTheOrderTheyWereOffered()
    {
        RunnableGraph graph = Ingress(4, OverflowPolicy.Backpressure)
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(3, TestToken));

        queue.Complete();

        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task TheControlResolvesBeforeAnyElementHasBeenOffered()
    {
        RunnableGraph graph = Ingress(2, OverflowPolicy.Backpressure)
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Task<IIngressQueue<int>> resolved = run.GetValueAsync(control, TestToken);

        // A control is a run-start value: its task is already complete when the handle is handed over,
        // which is what makes it usable at all — nothing could be offered to a run that had to end first.
        Assert.True(resolved.IsCompletedSuccessfully);
        Assert.False(run.Completion.IsCompleted);
        Assert.False(run.GetValueAsync(count, TestToken).IsCompleted);

        (await resolved).Complete();
        await run.Completion;

        Assert.Equal(0L, await run.GetValueAsync(count, TestToken));
    }

    [Fact]
    public async Task TheControlOfARunThatHasAlreadyEndedStillResolvesAndAnswersEveryOfferClosed()
    {
        // The honest consequence of a control being a run-start value: a run that ends before the first
        // element still hands one back. It does not fault with the run, because nothing about the queue
        // failed; it reports the truth an offer needs, which is that the run has ended.
        RunnableGraph graph = Ingress(2, OverflowPolicy.Backpressure)
            .Take(0)
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        Assert.Equal(0L, await run.GetValueAsync(count, TestToken));
        Assert.Equal(QueueOfferOutcome.Closed, await queue.OfferAsync(1, TestToken));

        queue.Complete();

        Assert.Equal(QueueOfferOutcome.Closed, await queue.OfferAsync(2, TestToken));
    }

    [Fact]
    public async Task TheControlOfACancelledRunStillResolvesAndAnswersClosed()
    {
        using CancellationTokenSource cancellation = new();

        await cancellation.CancelAsync();

        RunnableGraph graph = Ingress(2, OverflowPolicy.Backpressure)
            .To(Sink.Ignore<int>());

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);

        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        Assert.Equal(QueueOfferOutcome.Closed, await queue.OfferAsync(1, TestToken));
    }

    [Fact]
    public async Task CompletingTheQueueEndsTheRunTheWayASourceRunningOutDoes()
    {
        RunnableGraph graph = Ingress(4, OverflowPolicy.Backpressure)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        _ = await queue.OfferAsync(4, TestToken);
        _ = await queue.OfferAsync(5, TestToken);

        Assert.False(run.Completion.IsCompleted);

        queue.Complete();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(9L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task CompletingTheQueueDeliversWhatItAlreadyHeld()
    {
        // Completing drains. The sink is held on its first element while three more are queued, so the
        // completion arrives while the queue is not empty; every one of them still reaches the sink.
        Gate gate = new();

        RunnableGraph graph = Ingress(4, OverflowPolicy.Backpressure)
            .Select(value =>
            {
                gate.Wait();

                return value;
            })
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        _ = await queue.OfferAsync(1, TestToken);
        await gate.Reached;

        _ = await queue.OfferAsync(2, TestToken);
        _ = await queue.OfferAsync(3, TestToken);

        queue.Complete();
        gate.Open();

        await run.Completion;

        Assert.Equal([1, 2, 3], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task FailingTheQueueFaultsTheRunWithThatExceptionAndAbandonsWhatItHeld()
    {
        InvalidOperationException failure = new("the producer's own work failed");
        Gate gate = new();

        RunnableGraph graph = Ingress(4, OverflowPolicy.Backpressure)
            .Select(value =>
            {
                gate.Wait();

                return value;
            })
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        _ = await queue.OfferAsync(1, TestToken);
        await gate.Reached;

        _ = await queue.OfferAsync(2, TestToken);
        _ = await queue.OfferAsync(3, TestToken);

        queue.Fail(failure);
        gate.Open();

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(count, TestToken)));
        Assert.Equal(QueueOfferOutcome.Failed, await queue.OfferAsync(4, TestToken));
    }

    [Fact]
    public async Task TheFirstOfCompleteAndFailDecidesHowTheQueueEnded()
    {
        RunnableGraph graph = Ingress(2, OverflowPolicy.Backpressure)
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        queue.Complete();
        queue.Fail(new InvalidOperationException("too late to fail a queue that already ended"));

        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(0L, await run.GetValueAsync(count, TestToken));
        Assert.Equal(QueueOfferOutcome.Closed, await queue.OfferAsync(1, TestToken));
    }

    [Fact]
    public async Task ABackpressuringOfferWaitsForRoomRatherThanRefusingOrDropping()
    {
        Gate gate = new();

        RunnableGraph graph = Ingress(2, OverflowPolicy.Backpressure)
            .To(s => s.ForEach(_ => gate.Wait()));

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        _ = await queue.OfferAsync(1, TestToken);
        await gate.Reached;

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));
        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(3, TestToken));

        ValueTask<QueueOfferOutcome> parked = queue.OfferAsync(4, TestToken);

        Assert.False(parked.IsCompleted);

        gate.Open();

        Assert.Equal(QueueOfferOutcome.Accepted, await parked);

        queue.Complete();
        await run.Completion;

        Assert.Equal(0L, run.DroppedElements);
    }

    [Fact]
    public async Task ABackpressuringOfferParkedWhenTheRunEndsIsReleasedWithAnOutcome()
    {
        // The deadlock a bounded ingress could otherwise create. The producer is parked for room in a queue
        // nothing will drain again, and what releases it is the run ending — reported as an outcome,
        // because a producer that had to catch an exception here would be writing control flow in a catch.
        Gate gate = new();

        RunnableGraph graph = Ingress(1, OverflowPolicy.Backpressure)
            .To(s => s.ForEach(_ => gate.Wait()));

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        _ = await queue.OfferAsync(1, TestToken);
        await gate.Reached;

        _ = await queue.OfferAsync(2, TestToken);

        ValueTask<QueueOfferOutcome> parked = queue.OfferAsync(3, TestToken);

        Assert.False(parked.IsCompleted);

        gate.Open();
        await run.DisposeAsync();

        QueueOfferOutcome outcome = await parked;

        Assert.Contains(outcome, (QueueOfferOutcome[])[QueueOfferOutcome.Accepted, QueueOfferOutcome.Closed]);
    }

    [Theory]
    [InlineData(OverflowPolicy.DropNewest, QueueOfferOutcome.Dropped, new[] { 1, 2 })]
    [InlineData(OverflowPolicy.DropOldest, QueueOfferOutcome.Accepted, new[] { 1, 3 })]
    [InlineData(OverflowPolicy.DropBuffer, QueueOfferOutcome.Accepted, new[] { 1, 3 })]
    public async Task ADroppingPolicyAnswersForTheOfferedElementAndCountsWhatItDiscarded(
        OverflowPolicy policy,
        QueueOfferOutcome expected,
        int[] delivered)
    {
        // The outcome is about the element that was offered and about nothing else. A policy that evicts
        // what was already queued makes room for this element and therefore accepts it; only the policy
        // that discards the arriving element reports it dropped. How many elements a run lost is the run's
        // own count, which is what keeps a drop from ever being silent.
        Gate gate = new();

        RunnableGraph graph = Ingress(1, policy)
            .Select(value =>
            {
                gate.Wait();

                return value;
            })
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        await gate.Reached;

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));
        Assert.Equal(expected, await queue.OfferAsync(3, TestToken));

        queue.Complete();
        gate.Open();

        await run.Completion;

        Assert.Equal(delivered, await run.GetValueAsync(seen, TestToken));
        Assert.Equal(1L, run.DroppedElements);
    }

    [Fact]
    public async Task TheFailingPolicyAnswersFailedAndFaultsTheRunWithABufferOverflow()
    {
        Gate gate = new();

        RunnableGraph graph = Ingress(1, OverflowPolicy.Fail)
            .Select(value =>
            {
                gate.Wait();

                return value;
            })
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        await gate.Reached;

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));
        Assert.Equal(QueueOfferOutcome.Failed, await queue.OfferAsync(3, TestToken));

        gate.Open();

        BufferOverflowException failure =
            await Assert.ThrowsAsync<BufferOverflowException>(() => run.Completion);

        Assert.Contains("capacity 1", failure.Message, StringComparison.Ordinal);
        Assert.Same(failure, await Assert.ThrowsAsync<BufferOverflowException>(() => run.GetValueAsync(count, TestToken)));
        Assert.Equal(QueueOfferOutcome.Failed, await queue.OfferAsync(4, TestToken));
    }

    [Fact]
    public async Task AnOfferAfterCompletionIsClosedAndAnOfferAfterTheRunEndsIsClosedToo()
    {
        RunnableGraph graph = Ingress(2, OverflowPolicy.Backpressure)
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        _ = await queue.OfferAsync(1, TestToken);
        queue.Complete();

        Assert.Equal(QueueOfferOutcome.Closed, await queue.OfferAsync(2, TestToken));

        await run.Completion;

        Assert.Equal(1L, await run.GetValueAsync(count, TestToken));
        Assert.Equal(QueueOfferOutcome.Closed, await queue.OfferAsync(3, TestToken));
    }

    [Fact]
    public async Task ShuttingDownARunWaitingOnItsQueueCompletesItWithWhatItHas()
    {
        TaskCompletionSource delivered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = Ingress(4, OverflowPolicy.Backpressure)
            .Select(value =>
            {
                delivered.TrySetResult();

                return value;
            })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        _ = await queue.OfferAsync(7, TestToken);

        // An offer that has been accepted is not an element the run has taken yet, and shutdown means
        // "stop pulling": the element has to be past the run's pull before the run is stopped, or this
        // test would be asserting a race rather than a rule.
        await delivered.Task;

        await run.ShutdownAsync();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(7L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(QueueOfferOutcome.Closed, await queue.OfferAsync(8, TestToken));
    }

    [Fact]
    public async Task TwoRunsOfOneQueueGraphOfferIntoTwoQueues()
    {
        RunnableGraph graph = Ingress(4, OverflowPolicy.Backpressure)
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle first = await Host.MaterializeAsync(graph, TestToken);
        await using RunHandle second = await Host.MaterializeAsync(graph, TestToken);

        IIngressQueue<int> left = await first.GetValueAsync(control, TestToken);
        IIngressQueue<int> right = await second.GetValueAsync(control, TestToken);

        Assert.NotSame(left, right);

        _ = await left.OfferAsync(1, TestToken);
        _ = await right.OfferAsync(2, TestToken);

        left.Complete();
        right.Complete();

        await first.Completion;
        await second.Completion;

        Assert.Equal([1], await first.GetValueAsync(seen, TestToken));
        Assert.Equal([2], await second.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task AQueueChainComposesWithOperatorsBuffersAndAsyncStagesDownstreamOfIt()
    {
        RunnableGraph graph = Ingress(4, OverflowPolicy.Backpressure)
            .Where(value => value % 2 == 1)
            .Buffer(new BufferOptions { Capacity = 2 })
            .SelectAsync(
                new ParallelismOptions { MaxConcurrency = 2 },
                async (value, token) =>
                {
                    await Task.Yield();

                    return value * 10;
                })
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        for (int value = 1; value <= 6; value++)
        {
            Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(value, TestToken));
        }

        queue.Complete();
        await run.Completion;

        Assert.Equal([10, 30, 50], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task ConcurrentOffersRacingCompletionAllAnswerWithAnOutcomeAndNeverThrow()
    {
        // The race the whole outcome model exists for: many producers offering while the queue is being
        // completed and the run is ending. Nothing here asserts which offers won — that is the race — only
        // that every one of them answered with a declared outcome, that no offer threw, and that the
        // elements the sink saw are exactly the ones that were accepted.
        RunnableGraph graph = Ingress(8, OverflowPolicy.Backpressure)
            .To(s => s.Collect(new CollectOptions { MaxElements = 128 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);
        TaskCompletionSource start = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Task<(int Value, QueueOfferOutcome Outcome)>[] offers =
        [
            .. Enumerable.Range(0, 32).Select(value => Task.Run(
                async () =>
                {
                    await start.Task;

                    return (value, await queue.OfferAsync(value, TestToken));
                },
                TestToken)),
        ];

        Task completing = Task.Run(
            async () =>
            {
                await start.Task;

                queue.Complete();
            },
            TestToken);

        start.SetResult();

        (int Value, QueueOfferOutcome Outcome)[] results = await Task.WhenAll(offers);

        await completing;
        await run.Completion;

        Assert.All(
            results,
            result => Assert.Contains(
                result.Outcome,
                (QueueOfferOutcome[])[QueueOfferOutcome.Accepted, QueueOfferOutcome.Closed]));

        int[] accepted =
            [.. results.Where(result => result.Outcome is QueueOfferOutcome.Accepted).Select(result => result.Value)];

        Assert.Equal(
            accepted.Order(),
            (await run.GetValueAsync(seen, TestToken)).Order());
    }

    [Fact]
    public async Task ConcurrentOffersRacingTheEndOfTheRunAllAnswerWithAnOutcomeAndNeverThrow()
    {
        RunnableGraph graph = Ingress(4, OverflowPolicy.DropNewest)
            .To(Sink.Ignore<int>());

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);
        TaskCompletionSource start = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Task<QueueOfferOutcome>[] offers =
        [
            .. Enumerable.Range(0, 32).Select(value => Task.Run(
                async () =>
                {
                    await start.Task;

                    return await queue.OfferAsync(value, TestToken);
                },
                TestToken)),
        ];

        Task stopping = Task.Run(
            async () =>
            {
                await start.Task;

                await run.DisposeAsync();
            },
            TestToken);

        start.SetResult();

        QueueOfferOutcome[] outcomes = await Task.WhenAll(offers);

        await stopping;

        Assert.All(
            outcomes,
            outcome => Assert.Contains(
                outcome,
                (QueueOfferOutcome[])
                [
                    QueueOfferOutcome.Accepted,
                    QueueOfferOutcome.Dropped,
                    QueueOfferOutcome.Closed,
                ]));
    }

    [Fact]
    public async Task AnOfferIsCancelledByTheCallersOwnTokenAndTheRunIsNot()
    {
        Gate gate = new();

        RunnableGraph graph = Ingress(1, OverflowPolicy.Backpressure)
            .To(s => s.ForEach(_ => gate.Wait()));

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        _ = await queue.OfferAsync(1, TestToken);
        await gate.Reached;
        _ = await queue.OfferAsync(2, TestToken);

        using CancellationTokenSource giving = new();

        ValueTask<QueueOfferOutcome> parked = queue.OfferAsync(3, giving.Token);

        Assert.False(parked.IsCompleted);

        await giving.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await parked);

        // The caller gave up; the run did not. It is still going, and another offer still works.
        Assert.False(run.Completion.IsCompleted);

        gate.Open();
        queue.Complete();

        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task AnOfferMadeWithAnAlreadyCancelledTokenIsCancelledEvenWhenThereIsRoom()
    {
        // The predictable half of the rule above: whether an offer is cancelled must not depend on how full
        // the queue happened to be, which is also what a channel's own write does with a cancelled token.
        RunnableGraph graph = Ingress(4, OverflowPolicy.Backpressure).To(Sink.Ignore<int>());

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        using CancellationTokenSource giving = new();

        await giving.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await queue.OfferAsync(1, giving.Token));

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));

        queue.Complete();
        await run.Completion;
    }

    [Fact]
    public async Task FailingAQueueWithNoExceptionIsRejected()
    {
        RunnableGraph graph = Ingress(2, OverflowPolicy.Backpressure).To(Sink.Ignore<int>());

        ResultSlot<IIngressQueue<int>> control = Control(graph);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(control, TestToken);

        Assert.Equal("exception", Assert.Throws<ArgumentNullException>(() => queue.Fail(null!)).ParamName);

        queue.Complete();
        await run.Completion;
    }

    /// <summary>Starts a chain at a bounded ingress queue named <c>ingress</c>.</summary>
    /// <param name="capacity">The queue's capacity.</param>
    /// <param name="policy">What the queue does when it is full.</param>
    /// <returns>The source, ready to be extended or closed.</returns>
    private static Source<int> Ingress(int capacity, OverflowPolicy policy) =>
        Source.Queue<int>(new BufferOptions { Capacity = capacity, OverflowPolicy = policy }, "ingress");

    /// <summary>Reads the typed slot of the queue a closed graph declares.</summary>
    /// <param name="graph">The closed graph.</param>
    /// <returns>The slot a run resolves the queue through.</returns>
    /// <remarks>
    /// The slot is read from the closed graph rather than handed out by the source, because a slot binds to
    /// a document that does not exist until the chain is closed. This is the spelling an author uses.
    /// </remarks>
    private static ResultSlot<IIngressQueue<int>> Control(RunnableGraph graph) =>
        graph.Control<IIngressQueue<int>>("ingress");
}
