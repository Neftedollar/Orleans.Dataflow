using System.Globalization;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.DurableFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What one reading of a run reports: where the run is, and what its counters have reached.
/// </summary>
/// <remarks>
/// <para>
/// The monitor in its honest v1 shape. Every number a snapshot carries is a number the run already keeps,
/// so each test here arranges a run whose count is a fact of the arrangement — a drop policy with a known
/// drop count, a supervision scope with one injected failure, an element bound with a known number of
/// captures — and then asserts that the reading agrees with the run's own counter rather than with a
/// second implementation of the same arithmetic.
/// </para>
/// <para>
/// The status half is asserted at both ends: a run held mid-fold reads <c>Running</c>, and the same run
/// reads its terminal status afterwards. A snapshot taken only after the fact would leave "this is a
/// reading of a live run" untested, which is the one thing a monitor is for.
/// </para>
/// </remarks>
public sealed class RunSnapshotTests
{
    [Fact]
    public async Task ARunHeldMidFoldReadsRunningAndReadsCompletedAfterwards()
    {
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), _ => gate.Wait(), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        // Parked inside the fold of the first element, so the run is demonstrably executing when it is read.
        await gate.Reached;

        RunSnapshot running = run.Snapshot();

        Assert.Equal(RunSnapshotStatus.Running, running.Status);
        Assert.Equal(0L, running.DroppedElements);
        Assert.Equal(0L, running.SupervisedFailures);
        Assert.Equal(0L, running.PoisonElements);
        Assert.Equal(0L, running.Checkpoints);
        Assert.Equal(TimeSpan.Zero, running.TotalCheckpointHold);

        gate.Open();
        await run.Completion;

        Assert.Equal(RunSnapshotStatus.Completed, run.Snapshot().Status);
    }

    [Fact]
    public async Task AFailedRunReadsFailedAndAThrowingStageCostsNoCounter()
    {
        InvalidOperationException failure = new("the folder refuses");

        RunnableGraph graph = Source.From(new RecordingEnumerable<int>(1, 2, 3))
            .To(
                s => s.Aggregate(0L, (sum, value) => value == 2 ? throw failure : sum + value),
                "total",
                out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => run.Completion);

        RunSnapshot snapshot = run.Snapshot();

        Assert.Equal(RunSnapshotStatus.Failed, snapshot.Status);

        // A failure nothing contained is not a supervised one, and reading it as such would make the two
        // numbers a monitor cares about most agree with each other for the wrong reason.
        Assert.Equal(0L, snapshot.SupervisedFailures);
        Assert.Equal(0L, snapshot.PoisonElements);
    }

    [Fact]
    public async Task ACancelledRunReadsCanceledRatherThanFailed()
    {
        Gate gate = new();
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), _ => gate.Wait(), out ResultSlot<long> _);

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await gate.Reached;

        ValueTask disposing = run.DisposeAsync();

        gate.Open();
        await disposing;

        // Four statuses rather than the ending's two, and this is why: a cancelled run has no ending and
        // still has a place it stopped, which a monitor has every right to read.
        Assert.Equal(RunSnapshotStatus.Canceled, run.Snapshot().Status);
    }

    [Fact]
    public async Task TheDropCountOfASnapshotIsTheRunsOwnDropCount()
    {
        // The buffer suite's arrangement, reduced to what this claim needs: nine elements, a buffer of
        // three, a terminal parked on the first element, and a source that has run out before the terminal
        // is released. Which elements were in the buffer when each of the last five arrived is therefore a
        // fact, and five is the number the policy admits to discarding.
        Gate gate = new();
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> observed = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9);

        elements.PullBarrier = position =>
        {
            if (position == 9)
            {
                _ = exhausted.TrySetResult();
            }

            return position == 1 ? gate.Reached : null;
        };

        RunnableGraph graph = Source.From(elements)
            .Buffer(new BufferOptions { Capacity = 3, OverflowPolicy = OverflowPolicy.DropOldest })
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
                out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await exhausted.Task;

        gate.Open();
        await run.Completion;

        RunSnapshot snapshot = run.Snapshot();

        Assert.Equal(RunSnapshotStatus.Completed, snapshot.Status);
        Assert.Equal([1, 7, 8, 9], observed);
        Assert.Equal(5L, snapshot.DroppedElements);
        Assert.Equal(run.DroppedElements, snapshot.DroppedElements);
    }

    [Fact]
    public async Task ASupervisedFailureIsCountedAndTheRunStillCompletes()
    {
        List<int> observed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4])
            .Supervised(
                new SupervisionOptions { Form = SupervisionForm.Resume },
                Flow.For<int>()
                    .Via(TestFlow.FaultPoint<int>(FaultPointMode.Once, firstFailure: 2))
                    .Scan(0, (running, value) => running + value))
            .To(s => s.ForEach(observed.Add));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        RunSnapshot snapshot = run.Snapshot();

        // Completed and yet one failure was intercepted: "resume" and "nothing went wrong" are two different
        // readings, which is the whole reason the counter exists.
        Assert.Equal(RunSnapshotStatus.Completed, snapshot.Status);
        Assert.Equal([1, 4, 8], observed);
        Assert.Equal(1L, snapshot.SupervisedFailures);
        Assert.Equal(0L, snapshot.PoisonElements);
        Assert.Equal(0L, snapshot.DroppedElements);
    }

    [Fact]
    public async Task ADurableRunCountsItsCheckpointsAndTheTimeTheyHeldIt()
    {
        InMemoryCheckpointStore store = new();
        List<int> committed = [];

        RunnableGraph graph = Source.From([1, 2, 3, 4, 5, 6])
            .To(TestSink.Marking<int>("mark", committed.Add));

        await using RunHandle run = await Host.MaterializeDurableAsync(
            graph,
            Durable(store, "snapshot-counted", everyElements: 3),
            TestToken);
        await run.Completion;

        RunSnapshot snapshot = run.Snapshot();

        // Two captures, because the bound is reached at the third element and at the sixth, and each is
        // taken at the element that reached it. The store holds one document for the pair whatever the
        // count, since a capture replaces the position rather than appending to a history, so the store is
        // asked for the position instead: the last capture is the one a resume would replay from.
        Assert.Equal(2L, snapshot.Checkpoints);
        Assert.Equal(1, store.Count);
        Assert.Equal(6L, Cursor(await StoredAsync(store, "snapshot-counted", TestToken)));
        Assert.Equal([1, 2, 3, 4, 5, 6], committed);

        // The hold is cumulative and measured on the run's clock. This host measures by the system clock, so
        // what is asserted is that the cost is a reading at all rather than a number a test may name; the
        // controlled-clock run beside it in the durability suite is where the exact zero lives.
        Assert.True(snapshot.TotalCheckpointHold >= TimeSpan.Zero, snapshot.TotalCheckpointHold.ToString());
        Assert.Equal(run.CheckpointHold, snapshot.TotalCheckpointHold);
    }

    [Fact]
    public async Task ARunThatDeclaresNoDurabilityReportsNoCheckpointsForever()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        RunSnapshot snapshot = run.Snapshot();

        Assert.Equal(0L, snapshot.Checkpoints);
        Assert.Equal(TimeSpan.Zero, snapshot.TotalCheckpointHold);
    }

    [Fact]
    public async Task ARunCancelledBeforeItEverPulledStillReads()
    {
        using CancellationTokenSource cancellation = new();

        await cancellation.CancelAsync();

        RecordingEnumerable<int> elements = new(1, 2, 3);
        RunnableGraph graph = Summing(elements, out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.Completion);

        // The most extreme version of "never throws and always answers": a run whose source was never even
        // touched still has a place it stopped and a set of counters, all of them zero.
        RunSnapshot snapshot = run.Snapshot();

        Assert.Equal(RunSnapshotStatus.Canceled, snapshot.Status);
        Assert.Equal(0L, snapshot.DroppedElements);
        Assert.Equal(0L, snapshot.Checkpoints);
        Assert.Equal(0, elements.Pulls);
    }

    [Fact]
    public async Task TwoReadingsOfAnEndedRunAreTheSameReading()
    {
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        await run.Completion;

        RunSnapshot first = run.Snapshot();
        RunSnapshot second = run.Snapshot();

        // A run that has ended reports its final counters forever, and a snapshot is a value: two readings
        // of a finished run are equal without being the same object, which is what "a snapshot does not
        // update" means in code.
        Assert.Equal(first, second);
        Assert.NotSame(first, second);
        Assert.Equal(
            "Completed: dropped 0, supervised 0, poison 0, checkpoints 0, held 00:00:00",
            first.ToString());
    }

    [Fact]
    public void TheDiagnosticLineCarriesEveryValueTheReadingHolds()
    {
        // The line above pins one reading's text and therefore only catches a change to it. This one
        // re-derives what the text has to cover from the type itself, so a counter added to RunSnapshot and
        // left out of ToString fails here rather than shipping as a number no log line ever shows. The
        // values are deliberately distinct, so a member printed in place of another is a failure too.
        RunSnapshot reading = new()
        {
            Status = RunSnapshotStatus.Failed,
            DroppedElements = 11L,
            SupervisedFailures = 22L,
            PoisonElements = 33L,
            Checkpoints = 44L,
            TotalCheckpointHold = TimeSpan.FromSeconds(55),
        };

        string line = reading.ToString();

        foreach (System.Reflection.PropertyInfo property in typeof(RunSnapshot).GetProperties())
        {
            object? value = property.GetValue(reading);

            Assert.True(
                value is not null && line.Contains(Convert.ToString(value, CultureInfo.InvariantCulture)!, StringComparison.Ordinal),
                $"'{property.Name}' reads as '{value}', and the diagnostic line '{line}' does not carry it.");
        }
    }

    [Fact]
    public async Task ASnapshotIsReadableBeforeAnythingHasHappenedAndAfterEverythingHas()
    {
        // Callable at any point in the run's life and never throwing, which is the property that lets a
        // monitor sample on its own schedule rather than on the run's.
        RunnableGraph graph = Summing(new RecordingEnumerable<int>(1, 2, 3), out ResultSlot<long> _);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        RunSnapshot early = run.Snapshot();

        // Either answer is correct here and that is the point: a three-element run may already be over by
        // the time the caller looks, and a reading of a moment that has passed is what a snapshot is.
        Assert.True(
            early.Status is RunSnapshotStatus.Running or RunSnapshotStatus.Completed,
            early.ToString());

        await run.Completion;
        await run.DisposeAsync();

        // Disposing a run that has already completed changes nothing, including what it reads as.
        Assert.Equal(RunSnapshotStatus.Completed, run.Snapshot().Status);
    }
}
