using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The sources that wait: an asynchronous sequence, the two deferred factories, an endless repeat, a source
/// that never ends, and an asynchronous unfold.
/// </summary>
/// <remarks>
/// <para>
/// Every one of them is asserted against the same four questions the synchronous sources answer — fresh per
/// run, released on every terminal path, cancellable, and failing with the author's own exception — because
/// waiting is the only thing that changes and none of those four is allowed to.
/// </para>
/// <para>
/// The asynchronous sequence gets two claims of its own that no synchronous source can make: that the
/// enumeration is opened with the run's token, and that its <c>DisposeAsync</c> is awaited to completion
/// rather than merely started. Both are facts a test can only see through an instrumented sequence, which
/// is what <see cref="RecordingAsyncEnumerable{T}"/> exists for.
/// </para>
/// </remarks>
public sealed class AsyncSourceTests
{
    [Fact]
    public async Task AnAsyncEnumerableSourceEmitsItsElementsInOrder()
    {
        RecordingAsyncEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Source.FromAsyncEnumerable(elements)
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3], await run.GetValueAsync(seen, TestToken));
        Assert.Equal(1, elements.Enumerations);
        Assert.Equal(3, elements.Pulls);
    }

    [Fact]
    public async Task AnAsyncEnumerableSourceIsOpenedFreshWithTheRunsOwnTokenForEveryRun()
    {
        RecordingAsyncEnumerable<int> elements = new(1, 2);

        RunnableGraph graph = Source.FromAsyncEnumerable(elements)
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            await first.Completion;

            Assert.Equal(2L, await first.GetValueAsync(count, TestToken));
        }

        CancellationToken firstToken = elements.OpenedWith;

        await using (RunHandle second = await Host.MaterializeAsync(graph, TestToken))
        {
            await second.Completion;

            Assert.Equal(2L, await second.GetValueAsync(count, TestToken));
        }

        Assert.Equal(2, elements.Enumerations);
        Assert.Equal(4, elements.Pulls);
        Assert.Equal(2, elements.CompletedDisposals);

        // Each run opened the sequence with a token of its own, and neither handed over the default one:
        // this is the 'WithCancellation' the run supplies on the author's behalf.
        Assert.NotEqual(firstToken, elements.OpenedWith);
        Assert.True(elements.OpenedWith.CanBeCanceled);
    }

    [Fact]
    public async Task AnAsyncEnumerableSourceIsDisposedToCompletionOnEveryTerminalPath()
    {
        // Three terminal paths, one claim: the disposal is awaited, not started. The barrier is what tells
        // the two apart — a run that returned before the disposal finished would leave the completed count
        // behind the entered count.
        foreach (string path in new[] { "end", "failure", "cancellation" })
        {
            TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
            InvalidOperationException failure = new("the sink refuses this element");
            RecordingAsyncEnumerable<int> elements = new(1, 2, 3) { DisposalBarrier = release.Task };

            using CancellationTokenSource cancellation = new();

            RunnableGraph graph = Source.FromAsyncEnumerable(elements)
                .To(Sink.ForEach<int>(value =>
                {
                    if (path == "failure" && value == 2)
                    {
                        throw failure;
                    }

                    if (path == "cancellation" && value == 2)
                    {
                        cancellation.Cancel();
                    }
                }));

            await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

            release.SetResult();

            await run.Completion.ContinueWith(static _ => { }, TestContext.Current.CancellationToken);
            await elements.DisposalCompleted;

            Assert.Equal(1, elements.Disposals);
            Assert.Equal(1, elements.CompletedDisposals);
        }
    }

    [Fact]
    public async Task AnAsyncEnumerableSourceThatFailsFaultsTheRunWithItsOwnException()
    {
        InvalidOperationException failure = new("the third element is not available");
        RecordingAsyncEnumerable<int> elements = new(1, 2, 3)
        {
            PullFailure = position => position == 2 ? failure : null,
        };

        RunnableGraph graph = Source.FromAsyncEnumerable(elements).To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Equal(1, elements.CompletedDisposals);
    }

    [Fact]
    public async Task AnAsyncEnumerableSourceThatHonoursItsTokenStopsAtCancellation()
    {
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingAsyncEnumerable<int> elements = new(1, 2, 3)
        {
            PullBarrier = position =>
            {
                if (position != 1)
                {
                    return null;
                }

                reached.TrySetResult();

                return held.Task;
            },
        };

        using CancellationTokenSource cancellation = new();

        RunnableGraph graph = Source.FromAsyncEnumerable(elements).To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await reached.Task;
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);

        Assert.Equal(1, elements.Pulls);
        Assert.Equal(1, elements.CompletedDisposals);
    }

    [Fact]
    public async Task AnAsyncEnumerableSourceThatIgnoresItsTokenDelaysTheStopUntilItYields()
    {
        // The documented slow-source rule, made a fact rather than a caveat. The sequence never looks at
        // the token, so cancelling the run cannot end the pull; the run is still going, and it ends only
        // once the sequence itself lets go.
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingAsyncEnumerable<int> elements = new(1, 2, 3)
        {
            IgnoresToken = true,
            PullBarrier = position =>
            {
                if (position != 1)
                {
                    return null;
                }

                reached.TrySetResult();

                return held.Task;
            },
        };

        using CancellationTokenSource cancellation = new();

        RunnableGraph graph = Source.FromAsyncEnumerable(elements).To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await reached.Task;
        await cancellation.CancelAsync();

        Assert.False(run.Completion.IsCompleted);

        held.SetResult();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);

        Assert.Equal(1, elements.CompletedDisposals);
    }

    [Fact]
    public async Task ADeferredFactoryProducesOneFreshElementPerRunAndNoneAtAuthoring()
    {
        int invocations = 0;

        RunnableGraph graph = Source.FromFactory(() => Interlocked.Increment(ref invocations))
            .To(s => s.First(), "value", out ResultSlot<int> value);

        Assert.Equal(0, invocations);

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            Assert.Equal(1, await first.GetValueAsync(value, TestToken));
        }

        await using (RunHandle second = await Host.MaterializeAsync(graph, TestToken))
        {
            Assert.Equal(2, await second.GetValueAsync(value, TestToken));
        }

        Assert.Equal(2, invocations);
    }

    [Fact]
    public async Task ADeferredFactoryThatThrowsFaultsTheRunWithItsOwnExceptionUnwrapped()
    {
        InvalidOperationException failure = new("the element cannot be produced");

        RunnableGraph graph = Source.FromFactory<int>(() => throw failure).To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
    }

    [Fact]
    public async Task AnAsyncDeferredFactoryReceivesTheRunsTokenAndProducesOneElementPerRun()
    {
        int invocations = 0;
        CancellationToken observed = default;

        RunnableGraph graph = Source.FromAsyncFactory(token =>
            {
                observed = token;

                return Task.FromResult(Interlocked.Increment(ref invocations));
            })
            .To(s => s.First(), "value", out ResultSlot<int> value);

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            Assert.Equal(1, await first.GetValueAsync(value, TestToken));
        }

        Assert.True(observed.CanBeCanceled);

        await using (RunHandle second = await Host.MaterializeAsync(graph, TestToken))
        {
            Assert.Equal(2, await second.GetValueAsync(value, TestToken));
        }

        Assert.Equal(2, invocations);
    }

    [Fact]
    public async Task AnAsyncDeferredFactoryThatFailsFaultsTheRunWithItsOwnExceptionUnwrapped()
    {
        InvalidOperationException failure = new("the element cannot be produced");

        RunnableGraph graph = Source
            .FromAsyncFactory<int>(async token =>
            {
                await Task.Yield();

                throw failure;
            })
            .To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
    }

    [Fact]
    public async Task NeverEmitsNothingAndAShutdownResolvesTheEmptyAggregate()
    {
        RunnableGraph graph = Source.Never<int>()
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.False(run.Completion.IsCompleted);

        await run.ShutdownAsync();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(0L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task NeverIsCancelledByCancellationRatherThanCompleted()
    {
        using CancellationTokenSource cancellation = new();

        RunnableGraph graph = Source.Never<int>()
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        Assert.False(run.Completion.IsCompleted);

        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(count, TestToken));
    }

    [Fact]
    public async Task NeverIsReleasedByDisposalToo()
    {
        RunnableGraph graph = Source.Never<string>().To(Sink.Ignore<string>());

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task CycleRepeatsItsSequenceUntilSomethingDownstreamEndsTheRun()
    {
        int[] lap = [1, 2, 3];

        RunnableGraph graph = Source.Cycle(lap)
            .Take(7)
            .To(s => s.Collect(new CollectOptions { MaxElements = 16 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([1, 2, 3, 1, 2, 3, 1], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task CycleTakesAFreshEnumeratorPerLapAndReleasesEachOne()
    {
        RecordingEnumerable<int> elements = new(1, 2);

        RunnableGraph graph = Source.Cycle(elements)
            .Take(5)
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(5L, await run.GetValueAsync(count, TestToken));

        // Three laps for five elements out of a sequence of two, and every enumerator released: two laps
        // that ran out, and a third the take ended in the middle of.
        Assert.Equal(3, elements.Enumerations);
        Assert.Equal(3, elements.Releases);
        Assert.Equal(5, elements.Pulls);
    }

    [Fact]
    public async Task CycleReleasesTheLapItWasInWhenTheRunIsCancelled()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        Gate gate = new();

        using CancellationTokenSource cancellation = new();

        RunnableGraph graph = Source.Cycle(elements).To(Sink.ForEach<int>(_ => gate.Wait()));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await gate.Reached;
        await cancellation.CancelAsync();
        gate.Open();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);

        Assert.Equal(elements.Enumerations, elements.Releases);
    }

    [Fact]
    public async Task CycleOverAnEmptySequenceFaultsTheRunRatherThanLoopingForever()
    {
        RunnableGraph graph = Source.Cycle(Array.Empty<int>()).To(Sink.Ignore<int>());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        Assert.Contains("cycle over nothing", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ATakeOfNoElementsNeverTouchesEvenACycleOverAnEmptySequence()
    {
        // Take(0) is resolved when the plan is built, so the source is not enumerated at all — which is
        // what keeps it from faulting on a cycle that would otherwise be an endless loop over nothing.
        RecordingEnumerable<int> elements = new();

        RunnableGraph graph = Source.Cycle(elements)
            .Take(0)
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(0L, await run.GetValueAsync(count, TestToken));
        Assert.Equal(0, elements.Enumerations);
    }

    [Fact]
    public async Task AnAsyncUnfoldProducesItsElementsAndEndsWhenTheGeneratorSaysSo()
    {
        RunnableGraph graph = Source
            .UnfoldAsync<int, string>(1, async (state, token) =>
            {
                await Task.Yield();

                return state <= 8 ? new(state.ToString(System.Globalization.CultureInfo.InvariantCulture), state * 2) : null;
            })
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<string>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(["1", "2", "4", "8"], await run.GetValueAsync(seen, TestToken));
    }

    [Fact]
    public async Task AnAsyncUnfoldStartsFromItsSeedInEveryRunAndReceivesTheRunsToken()
    {
        CancellationToken observed = default;

        RunnableGraph graph = Source
            .UnfoldAsync<int, int>(0, (state, token) =>
            {
                observed = token;

                return Task.FromResult<UnfoldStep<int, int>?>(state < 3 ? new(state, state + 1) : null);
            })
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using (RunHandle first = await Host.MaterializeAsync(graph, TestToken))
        {
            Assert.Equal(3L, await first.GetValueAsync(total, TestToken));
        }

        await using (RunHandle second = await Host.MaterializeAsync(graph, TestToken))
        {
            Assert.Equal(3L, await second.GetValueAsync(total, TestToken));
        }

        Assert.True(observed.CanBeCanceled);
    }

    [Fact]
    public async Task AnAsyncUnfoldThatFailsFaultsTheRunWithItsOwnExceptionUnwrapped()
    {
        InvalidOperationException failure = new("the third step cannot be produced");

        RunnableGraph graph = Source
            .UnfoldAsync<int, int>(0, async (state, token) =>
            {
                await Task.Yield();

                return state < 2 ? new UnfoldStep<int, int>(state, state + 1) : throw failure;
            })
            .To(s => s.Count(), "count", out ResultSlot<long> count);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, (await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(count, TestToken))));
    }

    [Fact]
    public async Task AnEndlessAsyncUnfoldIsBoundedByATakeDownstreamOfIt()
    {
        RunnableGraph graph = Source
            .UnfoldAsync<int, int>(0, (state, token) =>
                Task.FromResult<UnfoldStep<int, int>?>(new(state, state + 1)))
            .Take(4)
            .To(s => s.Collect(new CollectOptions { MaxElements = 8 }), "seen", out ResultSlot<IReadOnlyList<int>> seen);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal([0, 1, 2, 3], await run.GetValueAsync(seen, TestToken));
    }
}
