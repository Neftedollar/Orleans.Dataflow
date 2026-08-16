using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the materialization token does to a run.
/// </summary>
/// <remarks>
/// Cancellation is the abrupt half of the pair whose graceful half is shutdown: the run stops, its results
/// cancel with it, and nothing is resolved. Every test here holds the run at a known element first, so that
/// "cancelled while running" is a fact rather than a hope about timing.
/// </remarks>
public sealed class CancellationTests
{
    [Fact]
    public async Task TheMaterializationTokenCancelsARunHeldMidElement()
    {
        using CancellationTokenSource cancellation = new();
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await gate.Reached;
        await cancellation.CancelAsync();
        gate.Open();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(total, TestToken));

        // The element in flight was finished and no further element was pulled.
        Assert.Equal(1, elements.Pulls);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ARunCancelledBeforeItsFirstPullNeverTouchesTheSource()
    {
        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();

        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        // Materialization does not throw for an already-cancelled token: cancellation is an outcome of a
        // run, so the caller still receives a handle to await and dispose.
        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.GetValueAsync(total, TestToken));

        Assert.Equal(0, elements.Enumerations);
        Assert.Equal(0, elements.Pulls);
        Assert.Equal(0, elements.Releases);
    }

    [Fact]
    public async Task CancellingAfterTheRunCompletedChangesNothing()
    {
        using CancellationTokenSource cancellation = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await run.Completion;
        await cancellation.CancelAsync();

        // A terminal state, once reached, is the run's answer forever.
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task ARunMaterializedWithNoTokenAtAllStillCompletes()
    {
        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, out ResultSlot<long> total);

        // Both members declare a default token, and this is the one test that leaves them at their
        // defaults: every other test passes the running test's token so that a hung run dies with its
        // test. A run linked to a token that can never be cancelled has to complete and release all the
        // same, and asking for its result has to take the path that skips the wait wrapper entirely.
#pragma warning disable xUnit1051 // The default overloads are the subject of this test rather than an oversight in it.
        await using RunHandle run = await Host.MaterializeAsync(graph);
        await run.Completion;

        Assert.Equal(6L, await run.GetValueAsync(total));
#pragma warning restore xUnit1051

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(1, elements.Releases);
    }

    [Fact]
    public async Task ACancelledRunReleasesItsEnumeratorEvenWhileHeld()
    {
        using CancellationTokenSource cancellation = new();
        Gate gate = new();
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);
        RunnableGraph graph = Summing(elements, _ => gate.Wait(), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);
        await gate.Reached;

        Assert.Equal(0, elements.Releases);

        await cancellation.CancelAsync();
        gate.Open();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);

        Assert.Equal(1, elements.Releases);
    }
}
