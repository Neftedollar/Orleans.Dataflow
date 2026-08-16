using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a graceful stop does, and how it differs from an abrupt one.
/// </summary>
/// <remarks>
/// The distinction these tests fix is the seed of the drain-and-abort vocabulary: shutdown completes the
/// run as if the source had ended, so an aggregate resolves with the state it has; cancellation resolves
/// nothing. The two are asserted against the same held run so that the difference is the only variable.
/// </remarks>
public sealed class ShutdownTests
{
    [Fact]
    public async Task ShutdownResolvesTheAggregateWithTheStateSoFar()
    {
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3, 4);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task shutdown = run.ShutdownAsync().AsTask();

        gate.Open();
        await shutdown;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        // Only the first element was folded, and its state is what the slot resolves with.
        Assert.Equal(1L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(1, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ShutdownBeforeTheFirstPullCompletesTheRunWithTheSeed()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        // Racing the run to its first element is not the point; whichever of the two happens first, the
        // run completes successfully and resolves the state it reached.
        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.ShutdownAsync();

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.InRange(await run.GetValueAsync(total, TestToken), 0L, 6L);
    }

    [Fact]
    public async Task ShutdownAfterTheRunEndedChangesNothing()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;
        await run.ShutdownAsync();
        await run.ShutdownAsync();

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ShutdownOfAFailedRunDoesNotThrowAndDoesNotHideTheFailure()
    {
        Gate gate = new();
        InvalidOperationException failure = new("the folder refuses");

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(
                s => s.Aggregate<long>(
                    0L,
                    (sum, value) =>
                    {
                        gate.Wait();

                        throw failure;
                    }),
                "total",
                out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task shutdown = run.ShutdownAsync().AsTask();

        gate.Open();
        await shutdown;

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));
    }

    [Fact]
    public async Task CancellationWinsOverAConcurrentShutdownRequest()
    {
        using CancellationTokenSource cancellation = new();
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await gate.Reached;

        // Both requests are in before the run can observe either, so which one wins is a rule and not a
        // race: the loop examines cancellation first.
        Task shutdown = run.ShutdownAsync().AsTask();
        await cancellation.CancelAsync();

        gate.Open();
        await shutdown;

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(total, TestToken));
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ShutdownDistinguishesItselfFromCancellationOnOneShape()
    {
        // The same graph, the same held element, one stopped gracefully and one abruptly. Shutdown
        // resolves the slot; cancellation cancels it.
        Assert.Equal(1L, await Stopped(shutdown: true));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await Stopped(shutdown: false));

        static async Task<long> Stopped(bool shutdown)
        {
            using CancellationTokenSource cancellation = new();
            Gate gate = new();
            RunnableGraph graph = Summing(
                new RecordingEnumerable<int>(1, 2, 3),
                _ => gate.Wait(),
                out ResultSlot<long> total);

            await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
            await gate.Reached;

            // Both requests are made in full before the gate opens, so that each is provably made while
            // the run is still holding its first element. Cancelling has to be awaited to reach that
            // state: a linked token source is cancelled by a registered callback, and CancelAsync runs
            // callbacks asynchronously, so merely calling it leaves the run's own token still uncancelled.
            Task stopping = Task.CompletedTask;

            if (shutdown)
            {
                stopping = run.ShutdownAsync().AsTask();
            }
            else
            {
                await cancellation.CancelAsync();
            }

            gate.Open();
            await stopping;

            return await run.GetValueAsync(total, TestToken);
        }
    }
}
