using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the watch over a run's ending reports, and how it differs from the run's completion.
/// </summary>
/// <remarks>
/// <para>
/// The claim is that a run's ending is readable as a value while the run's outcome is still thrown by
/// <see cref="RunHandle.Completion"/>: a completed run resolves the watch with
/// <see cref="RunEnding.Completed"/>, a failed run <em>resolves</em> it with the failure's type name and
/// message, and a cancelled run cancels it, because cancelling abandons a run rather than ending one.
/// </para>
/// <para>
/// Every test here reads both surfaces of the same run, because the pair is the point. A watch that agreed
/// with completion by faulting would carry no information completion did not already carry, and a
/// completion that stopped rethrowing the author's own exception would have paid for the watch with the
/// thing the suite has asserted since the beginning.
/// </para>
/// </remarks>
public sealed class WatchTerminationTests
{
    [Fact]
    public async Task ACompletedRunResolvesItsWatchBeforeItsCompletionTransitions()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.Completion;

        // Synchronous on purpose, and it is the ordering that makes it so: the watch is settled inside the
        // one method that settles a run, immediately before completion is. A caller that has awaited
        // completion therefore reads a settled ending rather than a pending one, and asserting that with an
        // await instead of this property would have proved nothing about the order of the two.
        Assert.True(run.WatchTermination.IsCompletedSuccessfully);

        RunEnding ending = await run.WatchTermination;

        Assert.Equal(RunEndingKind.Completed, ending.Kind);
        Assert.Null(ending.FailureType);
        Assert.Null(ending.FailureMessage);
        Assert.Equal("completed", ending.ToString());

        // The one instance every completed run reports, because a completed ending carries nothing that is
        // this run's own.
        Assert.Same(RunEnding.Completed, ending);

        // And the run really did complete: the watch is a reading of the same outcome the rest of the
        // handle reports, not a second answer beside it.
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.Equal(6L, await run.GetValueAsync(total, TestToken));
    }

    [Fact]
    public async Task EveryReadingOfTheWatchIsTheSameTask()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // Read before the run ends and again after it, because "one task" has to hold across the transition
        // as well: a property that built a fresh wrapper per reading would give two callers two tasks and
        // make "the run's ending" a thing that could be awaited twice with two answers.
        Task<RunEnding> watching = run.WatchTermination;

        await run.Completion;

        Assert.Same(watching, run.WatchTermination);
        Assert.Same(await watching, await run.WatchTermination);
    }

    [Fact]
    public async Task AFailedRunFaultsItsCompletionAndResolvesItsWatchWithTheFailure()
    {
        // The ADR 0002 tension, as a test. A result slot resolves at the end of a run and *carries* the
        // run's outcome — it faults when the run failed — so a slot typed "how it ended" could never resolve
        // to "failed"; the two assertions below are the same run answering both ways at once, which is the
        // whole of "a control can carry an outcome without becoming it". A watch that faulted like the
        // completion would leave the affordance unbuilt while looking built.
        InvalidOperationException failure = new("the selector refuses the third element");

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3, 4))
            .Select(value => value == 3 ? throw failure : value)
            .To(s => s.Aggregate(0L, (sum, value) => sum + value), "total", out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // The throwing half: the very instance the author's code threw, unwrapped, exactly as it always has
        // been.
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => run.GetValueAsync(total, TestToken)));

        // The reading half: the same failure as a value, and the task carrying it succeeded.
        Assert.True(run.WatchTermination.IsCompletedSuccessfully);

        RunEnding ending = await run.WatchTermination;

        Assert.Equal(RunEndingKind.Failed, ending.Kind);
        Assert.Equal(typeof(InvalidOperationException).FullName, ending.FailureType);
        Assert.Equal("the selector refuses the third element", ending.FailureMessage);
        Assert.Equal(failure.Message, ending.FailureMessage);
        Assert.Equal(
            "failed with System.InvalidOperationException: the selector refuses the third element",
            ending.ToString());
    }

    [Fact]
    public async Task TheMaterializationTokenCancelsTheWatchRatherThanEndingIt()
    {
        using CancellationTokenSource cancellation = new();
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), _ => gate.Wait(), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        // Held at the first element, so "cancelled while running" is a fact rather than a hope about timing.
        await gate.Reached;
        await cancellation.CancelAsync();
        gate.Open();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WatchTermination);

        // Cancelled and not faulted, which is the distinction the whole type rests on: cancelling abandons
        // a run rather than finishing one, so there is no ending to resolve with and no failure to report.
        Assert.Equal(TaskStatus.Canceled, run.WatchTermination.Status);
        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task DisposingARunCancelsItsWatchTheSameWay()
    {
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), _ => gate.Wait(), out ResultSlot<long> _);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await gate.Reached;

        ValueTask disposing = run.DisposeAsync();

        gate.Open();
        await disposing;

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WatchTermination);

        Assert.Equal(TaskStatus.Canceled, run.WatchTermination.Status);
    }

    [Fact]
    public async Task AnAuthorsOwnCancellationExceptionIsAFailureAndTheWatchReportsItAsOne()
    {
        // A stage that raises a cancellation nobody asked for. The run's token is untouched, so this is not
        // the run being cancelled — it is author code throwing an exception that happens to be of that type,
        // and the runtime tells the two apart by asking the token rather than by looking at the type. The
        // watch therefore resolves with a failure whose type name is the cancellation's own, which is the
        // sharpest available statement that "cancelled" is a fact about the run rather than about an
        // exception.
        OperationCanceledException thrown = new("the fold cancelled something of its own");

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(
                s => s.Aggregate(0L, (sum, value) => value == 2 ? throw thrown : sum + value),
                "total",
                out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);

        // Faulted rather than cancelled, which is what makes the ending an ending at all.
        Assert.Equal(TaskStatus.Faulted, run.Completion.Status);

        RunEnding ending = await run.WatchTermination;

        Assert.Equal(RunEndingKind.Failed, ending.Kind);
        Assert.Equal(typeof(OperationCanceledException).FullName, ending.FailureType);
        Assert.Equal("the fold cancelled something of its own", ending.FailureMessage);
    }

    [Fact]
    public async Task AShutdownEndsTheRunAndTheWatchReportsItAsCompleted()
    {
        // The graceful stop is not a third ending: a drained run completed, and the watch says so with the
        // very value a run whose source ran out reports.
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), _ => gate.Wait(), out ResultSlot<long> total);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await gate.Reached;
        gate.Open();

        await run.ShutdownAsync();
        await run.Completion;

        Assert.Same(RunEnding.Completed, await run.WatchTermination);
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        _ = await run.GetValueAsync(total, TestToken);
    }
}
