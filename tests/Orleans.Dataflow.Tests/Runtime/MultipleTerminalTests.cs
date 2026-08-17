using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a run that ends in more than one place does: when it completes, what each of its results resolves
/// from, and what one outcome means for all of them.
/// </summary>
/// <remarks>
/// <para>
/// The countdown that terminalized a linear run generalizes to a count over terminals with no change of
/// meaning, and that sentence is two claims rather than one. The first is that every terminal has to
/// complete: a run whose fast branch is done is not a run that is done. The second is that each terminal
/// keeps its own fold: two sinks under one junction see the same elements and may still resolve entirely
/// different values, and neither can read the other's state.
/// </para>
/// <para>
/// What stays single is the outcome. Failure wins everywhere rather than in the branch that raised it,
/// cancellation abandons every branch, and a graceful shutdown drains all of them — which for a broadcast
/// means the two branches observe the same elements, because a drain that truncated one and not the other
/// would be a shutdown nobody could reason about.
/// </para>
/// </remarks>
public sealed class MultipleTerminalTests
{
    [Fact]
    public async Task EachTerminalResolvesItsOwnSlotFromItsOwnFold()
    {
        // Two counting sinks under one broadcast, and one of them behind a take. They are handed the same
        // elements and resolve different numbers, which is only possible if the states are per terminal.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Counted("stage-3", "take", 2),
                    Node("stage-4", "count"),
                    Node("stage-5", "count"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-2", 1, "stage-5"),
                ],
                [Slot("some", "stage-4"), Slot("all", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Take(2)),
                ("stage-4", LocalStageDescriptor.Count()),
                ("stage-5", LocalStageDescriptor.Count())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both counts have");

        Assert.Equal(2L, await run.GetValueAsync(Result<long>(graph, "some"), TestToken));
        Assert.Equal(5L, await run.GetValueAsync(Result<long>(graph, "all"), TestToken));
    }

    [Fact]
    public async Task ThreeBranchesOfDifferentKindsEachResolveTheirOwnResult()
    {
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "count"),
                    Collect("stage-4", 8),
                    Node("stage-5", "last"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-4"),
                    Leg("stage-2", 2, "stage-5"),
                ],
                [Slot("count", "stage-3"), Slot("seen", "stage-4"), Slot("last", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(4, 5, 6))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Count()),
                ("stage-4", Collecting(8)),
                ("stage-5", LocalStageDescriptor.Last())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when all three branches have");

        int[] seen = await run.GetValueAsync(Result<int[]>(graph, "seen"), TestToken);

        Assert.Equal(3L, await run.GetValueAsync(Result<long>(graph, "count"), TestToken));
        Assert.Equal([4, 5, 6], seen);
        Assert.Equal(6, await run.GetValueAsync(Result<int>(graph, "last"), TestToken));
    }

    [Fact]
    public async Task ARunEndsOnlyWhenEveryTerminalHasEnded()
    {
        // One branch takes a single element and is finished with the run still going. The run is not
        // finished, because the other branch is parked, and that is the whole of "counted, not singular".
        Gate gate = new();
        TaskCompletionSource ended = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Counted("stage-3", "take", 1),
                    Node("stage-4", "for-each"),
                    Node("stage-5", "for-each"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-2", 1, "stage-5"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Take(1)),
                ("stage-4", Calling(_ => ended.TrySetResult())),
                ("stage-5", Calling(_ => gate.Wait()))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(ended.Task, "the short branch reaches its one element");
        await Reaches(gate.Reached, "the other branch reaches its first element");

        Assert.False(run.Completion.IsCompleted);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the parked branch has");
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task AFailureInOneBranchFailsTheRunAndEveryResult()
    {
        InvalidOperationException failure = new("the third element is not welcome here");

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "select"),
                    Node("stage-4", "ignore"),
                    Collect("stage-5", 8),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-2", 1, "stage-5"),
                ],
                [Slot("seen", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                (
                    "stage-3",
                    LocalStageDescriptor.Select((Func<int, int>)(value => value == 3 ? throw failure : value))),
                ("stage-4", LocalStageDescriptor.Ignore()),
                ("stage-5", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException reported =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        // The same instance, unwrapped, on the run and on the slot of the branch that did nothing wrong:
        // failure wins everywhere rather than in the branch that raised it.
        Assert.Same(failure, reported);
        Assert.Same(
            failure,
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await run.GetValueAsync(Result<int[]>(graph, "seen"), TestToken)));
    }

    [Fact]
    public async Task AFailureReleasesAJunctionParkedWaitingForRoom()
    {
        // The deadlock a failure has to break, and the one shape where a junction can be in it: one leg
        // has taken all the room it will ever take and is waiting for something that is not coming, so the
        // junction is parked on that leg when the other leg fails. Nothing but the run's cancellation can
        // release either of them, and if it did not, this run would never settle at all.
        InvalidOperationException failure = new("this leg refuses the first element");
        TaskCompletionSource never = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "for-each-async", "local-parallelism-parameters", """{"maxConcurrency":1}"""),
                    Node("stage-4", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6, 7, 8, 9))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                (
                    "stage-3",
                    LocalStageDescriptor.ForEachAsync(
                        new ParallelismOptions { MaxConcurrency = 1 },
                        (Func<int, CancellationToken, Task>)((_, token) => never.Task.WaitAsync(token)))),
                ("stage-4", Calling(_ => throw failure))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        Assert.Same(
            failure,
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await Reaches(run.Completion, "the failure releases every parked segment")));
    }

    [Fact]
    public async Task AnEmptyStreamFailsAStrictSinkAndThatFailureIsTheWholeRunsOutcome()
    {
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "empty"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "first"),
                    Node("stage-4", "count"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("head", "stage-3"), Slot("count", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Empty()),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.First()),
                ("stage-4", LocalStageDescriptor.Count())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException reported =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        // The message names which result has no value, which is why it was written to name one at all: a
        // graph with several of them has to say which.
        Assert.Contains("Sequence contains no elements", reported.Message, StringComparison.Ordinal);
        Assert.Contains("the result 'head'", reported.Message, StringComparison.Ordinal);

        // The counting branch is perfectly well defined and still fails, because a run has one outcome.
        Assert.Same(
            reported,
            await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await run.GetValueAsync(Result<long>(graph, "count"), TestToken)));
    }

    [Fact]
    public async Task ShutdownDrainsEveryBranch()
    {
        // A graceful stop is "stop pulling and keep what you have", and the two branches of a broadcast
        // have the same what: every element that entered the junction was placed in both legs before the
        // next was pulled, so a drain that ended one branch earlier than the other would be a drain that
        // lost something.
        Gate gate = new();
        List<int> parked = [];
        List<int> free = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "for-each"),
                    Node("stage-4", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6, 7, 8, 9))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                (
                    "stage-3",
                    Calling(value =>
                    {
                        lock (parked)
                        {
                            parked.Add(value);
                        }

                        gate.Wait();
                    })),
                (
                    "stage-4",
                    Calling(value =>
                    {
                        lock (free)
                        {
                            free.Add(value);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "one branch reaches its first element");

        ValueTask shutdown = run.ShutdownAsync();

        gate.Open();

        await Reaches(shutdown.AsTask(), "the shutdown returns once every branch has drained");
        await run.Completion;

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        lock (parked)
        {
            lock (free)
            {
                Assert.NotEmpty(parked);
                Assert.Equal(parked, free);
                Assert.Equal(parked.Order(), parked);
            }
        }
    }

    [Fact]
    public async Task CancellationAbandonsEveryBranch()
    {
        using CancellationTokenSource abandon = new();
        Gate gate = new();

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "for-each"),
                    Collect("stage-4", 16),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("seen", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6, 7, 8, 9))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Calling(_ => gate.Wait())),
                ("stage-4", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, abandon.Token);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "one branch reaches its first element");

        await abandon.CancelAsync();
        gate.Open();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await run.GetValueAsync(Result<int[]>(graph, "seen"), TestToken));

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task ASlotOfOneBranchIsNotResolvableFromAnother()
    {
        // Two results, two names, and no way for one to answer the other: the run holds one settled task
        // per declared slot and a name that was never declared is refused rather than answered.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "count"),
                    Node("stage-4", "count"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("left", "stage-3"), Slot("right", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Count()),
                ("stage-4", LocalStageDescriptor.Count())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both counts have");

        Assert.Equal(2L, await run.GetValueAsync(Result<long>(graph, "left"), TestToken));
        Assert.Equal(2L, await run.GetValueAsync(Result<long>(graph, "right"), TestToken));

        ArgumentException refused = await Assert.ThrowsAsync<ArgumentException>(
            async () => await run.GetValueAsync(Result<long>(graph, "middle"), TestToken));

        Assert.Contains("middle", refused.Message, StringComparison.Ordinal);
    }
}
