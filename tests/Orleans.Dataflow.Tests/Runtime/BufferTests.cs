using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What each overflow policy does to a buffer that is full, and how many elements it admits to doing it
/// to.
/// </summary>
/// <remarks>
/// <para>
/// Every scenario here is the same shape, so that the policy is the only variable: nine elements, a buffer
/// of three, and a terminal parked on the first element. The source is held until the terminal is parked
/// and released again only once the source has run out, so which elements were in the buffer when each of
/// the last five arrived is a fact and not a race — which is what makes the kept and dropped sets
/// assertable at all.
/// </para>
/// <para>
/// The kept set and the drop count are asserted together on purpose. The elements are distinct, so the two
/// together name exactly which elements were lost, and a policy that dropped the right number of the wrong
/// ones fails on the first assertion while one that quietly lost an extra element fails on the second.
/// </para>
/// </remarks>
public sealed class BufferTests
{
    [Theory]
    [InlineData(OverflowPolicy.DropOldest, new[] { 1, 7, 8, 9 }, 5L)]
    [InlineData(OverflowPolicy.DropNewest, new[] { 1, 2, 3, 4 }, 5L)]
    [InlineData(OverflowPolicy.DropBuffer, new[] { 1, 8, 9 }, 6L)]
    public async Task EachDroppingPolicyKeepsExactlyTheElementsItPromises(
        OverflowPolicy policy,
        int[] kept,
        long dropped)
    {
        Gate gate = new();
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9);

        elements.PullBarrier = position =>
        {
            if (position == 9)
            {
                exhausted.TrySetResult();
            }

            return position == 1 ? gate.Reached : null;
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3, OverflowPolicy = policy })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);
                        gate.Wait();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The terminal is parked on the first element and every other element has been offered to a
        // buffer that could hold at most three of them.
        await exhausted.Task;

        gate.Open();
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(kept, observed);
        Assert.Equal(kept.Sum(), await run.GetValueAsync(total, TestToken));
        Assert.Equal(dropped, run.DroppedElements);
        Assert.Equal(9, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task BackpressureLosesNothingAndStallsTheSourceInstead()
    {
        // The same shape as the dropping policies, and the difference is visible before anything is
        // delivered: the source cannot run out at all while the terminal is parked, because the fifth
        // element has nowhere to go. Releasing the terminal is what lets the rest of the sequence through.
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9)
        {
            Pulled = pulls =>
            {
                if (pulls == 5)
                {
                    saturated.TrySetResult();
                }
            },
        };

        elements.PullBarrier = position =>
        {
            if (position == 9)
            {
                exhausted.TrySetResult();
            }

            return position == 1 ? gate.Reached : null;
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3, OverflowPolicy = OverflowPolicy.Backpressure })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);
                        gate.Wait();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await saturated.Task;

        Assert.False(exhausted.Task.IsCompleted);
        Assert.Equal(5, elements.Pulls);

        gate.Open();
        await run.Completion;

        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9], observed);
        Assert.Equal(45L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(0L, run.DroppedElements);
    }

    [Fact]
    public async Task TheFailPolicyFaultsTheRunOnTheFirstOverflowAndDeliversNothingBehindIt()
    {
        Gate gate = new();
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9)
        {
            PullBarrier = position => position == 1 ? gate.Reached : null,
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3, OverflowPolicy = OverflowPolicy.Fail })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);
                        gate.Wait();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The source segment overflowed the buffer, faulted, and released its enumerator; the terminal is
        // still parked on the first element and has been told to stop.
        await elements.Released;

        gate.Open();

        BufferOverflowException overflow =
            await Assert.ThrowsAsync<BufferOverflowException>(() => run.Completion);

        Assert.Same(overflow, await Assert.ThrowsAsync<BufferOverflowException>(() => run.GetValueAsync(total, TestToken)));
        Assert.Contains("capacity 3", overflow.Message, StringComparison.Ordinal);

        // The three elements the buffer was holding are behind the failure and are never delivered.
        Assert.Equal([1], observed);
        Assert.Equal(0L, run.DroppedElements);
        Assert.Equal(5, elements.Pulls);
    }

    [Theory]
    [InlineData(OverflowPolicy.Backpressure)]
    [InlineData(OverflowPolicy.DropOldest)]
    [InlineData(OverflowPolicy.DropNewest)]
    [InlineData(OverflowPolicy.DropBuffer)]
    public async Task EveryElementOfACompletedRunIsEitherDeliveredOrCounted(OverflowPolicy policy)
    {
        // Nothing is gated here, so the two segments genuinely contend for the buffer: the writer applies
        // its policy while the reader is taking elements out from under it. The claim is an invariant
        // rather than a fact — an element the runtime lost without counting breaks it whatever the
        // interleaving was, which is what a fixed expected set could never catch.
        const int Count = 2000;

        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, Count)]);
        int delivered = 0;

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3, OverflowPolicy = policy })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        delivered++;

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(Count, elements.Pulls);
        Assert.Equal(Count, delivered + run.DroppedElements);

        if (policy is OverflowPolicy.Backpressure)
        {
            Assert.Equal(Count, delivered);
            Assert.Equal(0L, run.DroppedElements);
        }
    }

    [Fact]
    public async Task ABufferOfOneIsStillABoundaryRatherThanFusion()
    {
        // The smallest buffer there is still cuts the chain: the terminal holds one, the channel holds
        // one, and the source holds one it cannot hand over.
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);

        elements.Pulled = pulls =>
        {
            if (pulls == 3)
            {
                saturated.TrySetResult();
            }
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 1 })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();
                        elements.Consumed();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;
        await saturated.Task;

        Assert.Equal(3, elements.Pulls);

        gate.Open();
        await run.Completion;

        Assert.Equal(3, elements.PeakInFlight);
        Assert.Equal(15L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task CancellationAbandonsWhateverABufferWasHolding()
    {
        using CancellationTokenSource cancellation = new();
        Gate gate = new();
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);

        elements.PullBarrier = position =>
        {
            if (position == 4)
            {
                exhausted.TrySetResult();
            }

            return position == 1 ? gate.Reached : null;
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3 })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);
                        gate.Wait();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        // Every element has been offered, the buffer is holding the last three, and the terminal is parked
        // on the first.
        await exhausted.Task;
        await cancellation.CancelAsync();

        gate.Open();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(total, TestToken));

        // Cancellation abandons what the boundary was holding; a shutdown would have delivered it.
        Assert.Equal([1], observed);
        Assert.Equal(0L, run.DroppedElements);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ShutdownDeliversWhateverABufferWasHolding()
    {
        // The same graph and the same held terminal as the cancellation above, stopped the other way.
        Gate gate = new();
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);

        elements.PullBarrier = position =>
        {
            if (position == 4)
            {
                exhausted.TrySetResult();
            }

            return position == 1 ? gate.Reached : null;
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3 })
            .To(
                s => s.Aggregate(
                    0L,
                    (sum, value) =>
                    {
                        observed.Add(value);
                        gate.Wait();

                        return sum + value;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await exhausted.Task;

        Task shutdown = run.ShutdownAsync().AsTask();

        gate.Open();
        await shutdown;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal([1, 2, 3, 4], observed);
        Assert.Equal(10L, await run.GetValueAsync(total, TestToken));
    }
}
