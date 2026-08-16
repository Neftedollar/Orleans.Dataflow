using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What disposing a handle does, and what it refuses to do.
/// </summary>
/// <remarks>
/// Disposal is teardown, so the claims are about what it never does: never throws, never hides how the run
/// ended, never leaves an enumerator held, and never minds being called twice or after the run is over.
/// The handles here are disposed by hand rather than with <c>await using</c>, because calling it twice is
/// part of what is being asserted.
/// </remarks>
public sealed class DisposalTests
{
    [Fact]
    public async Task DisposeAsyncCancelsAHeldRunAndWaitsForItToStop()
    {
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> total);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task disposal = run.DisposeAsync().AsTask();

        gate.Open();
        await disposal;

        // Disposal returned only once the run had stopped and let go of its enumerator.
        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
        Assert.Equal(1, elements.Releases);
        Assert.Equal(1, elements.Pulls);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task DisposeAsyncNeverThrowsForTheCancellationItCaused()
    {
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), _ => gate.Wait(), out ResultSlot<long> _);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task disposal = run.DisposeAsync().AsTask();

        gate.Open();
        await disposal;

        Assert.Equal(TaskStatus.RanToCompletion, disposal.Status);
    }

    [Fact]
    public async Task DisposeAsyncNeverThrowsForAFailureTheRunAlreadyHad()
    {
        InvalidOperationException failure = new("the selector refuses");

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .Select<int>(_ => throw failure)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // Wait for the failure first, so that disposal is disposing a run that has definitely already
        // failed rather than racing its own cancellation against the selector.
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));

        await run.DisposeAsync();

        // Nothing was hidden: the failure is still the run's answer, on completion and on the result.
        Assert.Equal(TaskStatus.Faulted, run.Completion.Status);
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));
    }

    [Fact]
    public async Task DisposeAsyncTwiceIsSafe()
    {
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> _);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task first = run.DisposeAsync().AsTask();

        gate.Open();
        await first;
        await run.DisposeAsync();
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task DisposeAsyncOfACompletedRunObservesItWithoutChangingIt()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task DisposeAsyncConcurrentWithShutdownLeavesOneTerminalState()
    {
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), _ => gate.Wait(), out ResultSlot<long> total);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await gate.Reached;

        Task shutdown = run.ShutdownAsync().AsTask();
        Task disposal = run.DisposeAsync().AsTask();

        gate.Open();
        await Task.WhenAll(shutdown, disposal);

        // Whichever request the loop saw first, both callers agree afterwards and the run has exactly one
        // answer. Disposal cancels, so the answer here is cancellation.
        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ARunDisposedWithoutBeingAwaitedStillReleasesItsEnumerator()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, out ResultSlot<long> _);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.DisposeAsync();

        Assert.True(run.Completion.IsCompleted);
        Assert.InRange(elements.Releases, 0, 1);
        Assert.Equal(elements.Enumerations, elements.Releases);
    }
}
