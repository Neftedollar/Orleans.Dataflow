using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The combine-latest row of ADR 0005's fan-in table, which is Rx's contract and deliberately not Akka's.
/// </summary>
/// <remarks>
/// <para>
/// Four promises, each of them observable. Nothing is emitted until every input has produced at least once,
/// which is asserted as a sink that has received nothing while an input the run has already finished with
/// waits for another to start. One row leaves the junction on every arrival after that, carrying the latest
/// element of every input — asserted as an exact sequence, which is possible because a test can decide when
/// an arrival happens without deciding anything about the scheduler. An input that completes freezes its
/// last element into every later row, and the junction completes only when every input has, which is the
/// whole of what separates this junction from a zip standing in the same place. And an input that completes
/// without ever producing means no row can ever be built: such a run emits nothing and ends cleanly, which
/// is Rx's answer and is stated here rather than discovered.
/// </para>
/// <para>
/// No test here waits on a clock. A gate holds a run at a known point, a pull barrier holds a source at a
/// known element, and the deadline in <see cref="JunctionFixtures.Reaches"/> exists so that a broken
/// completion rule is reported rather than hung on.
/// </para>
/// </remarks>
public sealed class CombineLatestTests
{
    [Fact]
    public async Task CombineLatestEmitsNothingUntilEveryInputHasProduced()
    {
        // The first promise, in the form that makes it a fact: one input produces everything it has and
        // ends, and the sink has still received nothing, because the other input has not produced once. An
        // arrival before that updates the junction's state and leaves nothing at all.
        Lock counting = new();
        List<string> seen = [];
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> early = new(1, 2, 3);
        RecordingEnumerable<int> late = new(10)
        {
            PullBarrier = position => position == 0 ? held.Task : null,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "combine-latest"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(early)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(late)),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                (
                    "stage-4",
                    CallingRows(row =>
                    {
                        lock (counting)
                        {
                            seen.Add(row);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Completing(held);

        await Reaches(early.Released, "the input that produces reaches its end");

        lock (counting)
        {
            Assert.Empty(seen);
        }

        held.SetResult();

        await Reaches(run.Completion, "the run completes once the late input has produced and ended");

        // What the rows are is decided by how far the junction had drained the first input when the second
        // one arrived, which is a scheduling question and is deliberately not asserted. What is asserted is
        // everything the contract does decide: every row carries the second input's only element, and the
        // last row carries the first input's last one, because a row is the latest of every input.
        lock (counting)
        {
            Assert.NotEmpty(seen);
            Assert.All(seen, row => Assert.EndsWith("-10", row));
            Assert.Equal("3-10", seen[^1]);
        }
    }

    [Fact]
    public async Task CombineLatestEmitsARowOnEveryArrivalAndFreezesACompletedInputsLastValue()
    {
        // The exact sequence, made exact by holding each arrival until the row before it has been delivered:
        // the second input produces one element and ends, and the first input's three arrivals each emit a
        // row carrying that frozen element. Four arrivals and three rows is the first promise counted from
        // the other side — the arrival that completed the state emitted nothing.
        Lock counting = new();
        List<string> seen = [];
        TaskCompletionSource[] delivered =
        [
            new(TaskCreationOptions.RunContinuationsAsynchronously),
            new(TaskCreationOptions.RunContinuationsAsynchronously),
            new(TaskCreationOptions.RunContinuationsAsynchronously),
        ];

        RecordingEnumerable<int> steady = new(100);
        RecordingEnumerable<int> arriving = new(1, 2, 3);

        arriving.PullBarrier = position => position switch
        {
            // The second element waits for the first row and for the other input's end, so every row after
            // this one is a row emitted while one leg is provably finished.
            1 => Task.WhenAll(delivered[0].Task, steady.Released),
            2 => delivered[1].Task,
            _ => null,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "combine-latest"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(arriving)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(steady)),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                (
                    "stage-4",
                    CallingRows(row =>
                    {
                        lock (counting)
                        {
                            seen.Add(row);

                            if (seen.Count <= delivered.Length)
                            {
                                delivered[seen.Count - 1].TrySetResult();
                            }
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Completing(delivered);

        await Reaches(run.Completion, "the run completes when both inputs have");

        lock (counting)
        {
            Assert.Equal(["1-100", "2-100", "3-100"], seen);
        }

        // The frozen element was produced exactly once and appears in three rows, which is what "freezes"
        // means: the junction kept it rather than asking for it again, and asking again would have ended the
        // run rather than produced anything.
        Assert.Equal(1, steady.Pulls);
    }

    [Fact]
    public async Task CombineLatestCompletesOnlyWhenEveryInputHasCompleted()
    {
        // The rule that is the whole difference from a zip standing here: the first input ends after one
        // element and the run does not end with it. The second input is held until then, so the sequence is
        // exact, and its three arrivals emit three rows each carrying the completed input's frozen element.
        // A zip in this position emits one row and completes.
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> brief = new(1);
        RecordingEnumerable<int> lasting = new(10, 20, 30)
        {
            PullBarrier = position => position == 0 ? held.Task : null,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "combine-latest"),
                    Collect("stage-4", 8),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("rows", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(brief)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(lasting)),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                ("stage-4", CollectingRows(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Completing(held);

        await Reaches(brief.Released, "the first input ends after its one element");

        Assert.False(run.Completion.IsCompleted);

        held.SetResult();

        await Reaches(run.Completion, "the run completes when the second input has ended too");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(["1-10", "1-20", "1-30"], rows);
    }

    [Fact]
    public async Task CombineLatestEmitsNothingAtAllWhenAnInputCompletesWithoutProducing()
    {
        // Rx's answer, stated rather than discovered: an input that ends without producing means the state a
        // row is built from can never be complete, so no arrival on any other input can ever emit one. The
        // junction does not fail and does not stop early — it goes on reading the inputs that are live,
        // emits nothing, and completes when the last of them has completed.
        RecordingEnumerable<int> speaking = new(1, 2, 3);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "empty"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "combine-latest"),
                    Collect("stage-4", 8),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("rows", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Empty()),
                ("stage-2", LocalStageDescriptor.FromEnumerable(speaking)),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                ("stage-4", CollectingRows(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes although no row could ever be built");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Empty(rows);
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        // The input that could speak was read to its end all the same, which is what makes this a completion
        // rather than an early stop: nothing was cancelled, there was simply never a row.
        Assert.Equal(3, speaking.Pulls);
    }

    [Fact]
    public async Task CombineLatestHoldsTheLatestOfEveryInputAndReadsNoFurtherAhead()
    {
        // The N bound, from both sides. It reads no further ahead than any other junction: with the sink
        // parked, four elements leave the fast input and no fifth — one row in the sink's hand, one row in
        // the junction's output channel, one element in the input's channel, one in the source's hand — so
        // what the junction holds is the latest of each input and never a queue of them. And it really does
        // hold one per input for as long as it runs: the other input produces a single element at the very
        // start, and every one of the nine rows carries it.
        Gate gate = new();
        Lock counting = new();
        List<string> seen = [];
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource readAhead = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource emitted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RecordingEnumerable<int> fast = new(1, 2, 3, 4, 5, 6, 7, 8, 9)
        {
            Pulled = pulls =>
            {
                if (pulls == 4)
                {
                    saturated.TrySetResult();
                }
            },
        };

        fast.PullBarrier = position =>
        {
            if (position == 4)
            {
                readAhead.TrySetResult();
            }

            // The second element waits for the first row, which is the only way to know that the junction
            // has read the other input's element rather than merely that it was produced. Without the hold
            // this input could arrive twice before the other arrived once, and the first row would carry a
            // latest of two — a fact about which thread started first and about nothing else.
            return position == 1 ? emitted.Task : null;
        };

        RecordingEnumerable<int> once = new(10)
        {
            PullBarrier = position => position == 1 ? held.Task : null,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "combine-latest"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(fast)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(once)),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                (
                    "stage-4",
                    CallingRows(row =>
                    {
                        emitted.TrySetResult();
                        gate.Wait();

                        lock (counting)
                        {
                            seen.Add(row);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable releasing = Completing(held, emitted);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the fast input reaches the bound a parked sink allows");

        Assert.False(readAhead.Task.IsCompleted);
        Assert.Equal(4, fast.Pulls);

        gate.Open();
        held.SetResult();

        await Reaches(run.Completion, "the run completes once the sink and the second input are released");

        lock (counting)
        {
            Assert.Equal(
                ["1-10", "2-10", "3-10", "4-10", "5-10", "6-10", "7-10", "8-10", "9-10"],
                seen);
        }

        Assert.Equal(1, once.Pulls);
    }

    [Fact]
    public async Task CombineLatestReadsASlowInputWhileAFasterOneKeepsArriving()
    {
        // Reaching every input matters more here than it does for a merge, because this junction cannot emit
        // at all until every input has produced once: an endless input beside one that speaks once is the
        // shape where a junction that never looked away from the fast one would emit nothing for the whole
        // life of the run. What this proves is that the slow input is reached and stays in every row; it is
        // deliberately not called a fairness proof, because a bounded channel empties between one arrival
        // and the next, so a pump that scanned in port order every time would also find the slow element
        // eventually — the rotation is the reason it cannot be starved, and this run is not the measurement
        // that would tell the two apart.
        Lock counting = new();
        List<string> seen = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "combine-latest"),
                    Counted("stage-4", "take", 6),
                    Node("stage-5", "for-each"),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-4", "stage-5"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(99))),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                ("stage-4", LocalStageDescriptor.Take(6)),
                (
                    "stage-5",
                    CallingRows(row =>
                    {
                        lock (counting)
                        {
                            seen.Add(row);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the slow input's one element is read and the rows start flowing");

        lock (counting)
        {
            Assert.Equal(6, seen.Count);
            Assert.All(seen, row => Assert.Equal("1-99", row));
        }
    }

    [Fact]
    public async Task TheCombinerReceivesARowOfItsOwnRatherThanTheJunctionsSlots()
    {
        // The same promise as a zip's, and here it is the sharper of the two: this junction keeps the latest
        // of every input for the whole run and writes over one slot on every arrival, so an author who kept
        // the array they were handed would find every row they had ever received turned into the last one.
        // Reading the kept rows after the run is over is the only way to tell a copy from a view.
        Lock keeping = new();
        List<object?[]> handed = [];
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> brief = new(1);
        RecordingEnumerable<int> lasting = new(10, 20, 30)
        {
            PullBarrier = position => position == 0 ? held.Task : null,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "combine-latest"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(brief)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(lasting)),
                (
                    "stage-3",
                    LocalStageDescriptor.CombineLatest((Func<object?[], object?>)(parts =>
                    {
                        lock (keeping)
                        {
                            handed.Add(parts);
                        }

                        return string.Join('-', parts);
                    }))),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Completing(held);

        await Reaches(brief.Released, "the first input ends after its one element");

        held.SetResult();

        await Reaches(run.Completion, "the run completes when both inputs have");

        lock (keeping)
        {
            Assert.Equal(
                ["1-10", "1-20", "1-30"],
                handed.Select(row => string.Join('-', row)));
        }
    }

    [Fact]
    public async Task AFailingInputFailsACombiningRunWhileThePumpWaitsOnTheOthers()
    {
        // The shared rule again, in the position where this junction holds state: a failure on an input the
        // pump is not reading cancels the run's token, and every wait this pump takes is taken on it.
        InvalidOperationException failure = new("the second input gives up");

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "never"),
                    Node("stage-2", "failed"),
                    Node("stage-3", "combine-latest"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Never()),
                ("stage-2", LocalStageDescriptor.Failed(failure)),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException raised =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Same(failure, raised);
    }

    [Fact]
    public async Task APauseTakesEffectOnACombineLatestWaitingForItsFirstRow()
    {
        // The pause claim in the state only this junction has: one input has produced and ended, its element
        // is being held for a row that cannot be built yet, and the pump is asleep on the input that has
        // never produced. The run is arranged so there is no other way to be quiet, so the quiescence a
        // pause waits for is that wait reporting itself.
        RecordingEnumerable<int> once = new(7);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "never"),
                    Node("stage-3", "combine-latest"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(once)),
                ("stage-2", LocalStageDescriptor.Never()),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(once.Released, "the input that ends is released");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a junction that cannot emit yet");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");

        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task ACombiningRunShutsDownWithWhatItHad()
    {
        // Shutdown is "stop pulling and keep what you have" for a junction that remembers: both sources
        // observe it, everything already admitted drains through the junction, the rows it emits meanwhile
        // still carry the latest of every input, and the run ends successfully rather than being cancelled.
        Gate gate = new();
        Lock counting = new();
        List<string> seen = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "cycle"),
                    Node("stage-3", "combine-latest"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(2))),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                (
                    "stage-4",
                    CallingRows(row =>
                    {
                        gate.Wait();

                        lock (counting)
                        {
                            seen.Add(row);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "the run reaches the sink");

        Task shutdown = run.ShutdownAsync().AsTask();

        gate.Open();

        await Reaches(shutdown, "the shutdown of a combining run completes");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        lock (counting)
        {
            Assert.NotEmpty(seen);
            Assert.All(seen, row => Assert.Equal("1-2", row));
        }
    }

    [Fact]
    public async Task CancellingACombiningRunEndsItCanceled()
    {
        using CancellationTokenSource cancellation = new();

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "never"),
                    Node("stage-2", "never"),
                    Node("stage-3", "combine-latest"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Never()),
                ("stage-2", LocalStageDescriptor.Never()),
                ("stage-3", LocalStageDescriptor.CombineLatest(Rows())),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await cancellation.CancelAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);
        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }
}
