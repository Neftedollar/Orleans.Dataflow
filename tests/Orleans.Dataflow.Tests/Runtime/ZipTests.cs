using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The zip row of ADR 0005's fan-in table, as behavior rather than as a claim about the plan.
/// </summary>
/// <remarks>
/// <para>
/// A zip promises four things and every one of them is observable from outside the engine. What it emits is
/// one row per element from each input, paired positionally, which is an exact sequence and is asserted
/// against one. When it completes is as soon as any input does, which is what makes the shorter input the
/// one that decides — and the columns it was holding at that moment are discarded, which is asserted
/// against the combiner's own record rather than inferred from what the sink received. What it holds is at
/// most N−1 elements, counted the way the buffer and fan-in suites count: how far a source gets before it
/// parks, read after the run is over. And how it demands is per row — an input that has already given the
/// pending row its column is not read again until that row is emitted, which is the same count read as a
/// statement about the input rather than about the junction.
/// </para>
/// <para>
/// No test here waits on a clock. A gate holds a run at a known point, a pull barrier holds a source at a
/// known element, and the deadline in <see cref="JunctionFixtures.Reaches"/> exists so that a broken
/// completion rule is reported rather than hung on.
/// </para>
/// </remarks>
public sealed class ZipTests
{
    [Fact]
    public async Task ZipPairsTheElementsOfItsInputsPositionally()
    {
        // The exact sequence, because a zip promises one: row i is the i-th element of every input and
        // nothing about the scheduler can change which elements meet.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Collect("stage-4", 16),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("rows", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10, 20, 30))),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", CollectingRows(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the inputs have been paired to their end");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(["1-10", "2-20", "3-30"], rows);
    }

    [Fact]
    public async Task ZipCompletesEagerlyAndDiscardsTheColumnsOfThePartialRow()
    {
        // The sharp form of eager completion. The long input is held at exactly the point where the junction
        // provably has one of its elements in hand — a column of a row whose other half does not exist yet —
        // and only then is the short input allowed to end. What the run does with that column is the whole
        // of what this test is about: a row missing a column can never be completed, so the column is
        // dropped rather than kept, and the proof is the combiner's own record, because a column that
        // reached no combiner reached nothing at all.
        Lock combining = new();
        List<string> combined = [];
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource readAhead = new(TaskCreationOptions.RunContinuationsAsynchronously);

        RecordingEnumerable<int> endless = new(1, 2, 3, 4, 5)
        {
            Pulled = pulls =>
            {
                if (pulls == 4)
                {
                    saturated.TrySetResult();
                }
            },
        };

        endless.PullBarrier = position =>
        {
            if (position == 4)
            {
                readAhead.TrySetResult();
            }

            return null;
        };

        // The pull past the last element is the one that ends this input, and it waits until the other input
        // has filled everything it can: one column inside the junction, one element in its channel, one in
        // its own hand. Without the hold the junction could reach the end before it had read anything, and
        // the discard under test would be a discard of nothing.
        RecordingEnumerable<int> once = new(10)
        {
            PullBarrier = position => position == 1 ? saturated.Task : null,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Collect("stage-4", 8),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("rows", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(endless)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(once)),
                (
                    "stage-3",
                    LocalStageDescriptor.Zip((Func<object?[], object?>)(parts =>
                    {
                        string row = string.Join('-', parts);

                        lock (combining)
                        {
                            combined.Add(row);
                        }

                        return row;
                    }))),
                ("stage-4", CollectingRows(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the junction completes as soon as the shorter input ends");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(["1-10"], rows);

        // The second element of the long input was read into the row and then dropped: it reached the
        // combiner in no row at all, which is the only place it could have gone.
        lock (combining)
        {
            Assert.Equal(["1-10"], combined);
        }

        // Four elements left the long input and no fifth: one in the row that was emitted, one in the column
        // that was discarded, one in its channel, one in its own hand. The last two are abandoned by the
        // completion rather than dropped by a policy, which is why the run ends successfully.
        Assert.Equal(4, endless.Pulls);
        Assert.False(readAhead.Task.IsCompleted);
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task ZipEmitsTheRowWhoseColumnCameFromAnInputThatHasSinceEnded()
    {
        // The other edge of the eager rule, and the one it would be easy to get wrong in the other
        // direction: an input that has given the pending row its column and then ended does not end that
        // row. The short input here produces its only element and finishes before the other input has
        // produced anything at all, and the row it is a column of is still emitted when the slow column
        // arrives — which is Rx's answer for zip([1,2],[1]) and falls out of a pump that reads a column
        // before it ever asks whether that input has more.
        RecordingEnumerable<int> brief = new(10);
        RecordingEnumerable<int> slow = new(1, 2)
        {
            PullBarrier = position => position == 0 ? brief.Released : null,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Collect("stage-4", 8),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("rows", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(slow)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(brief)),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", CollectingRows(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the row completed by the slow input is emitted and the junction ends");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(["1-10"], rows);
    }

    [Fact]
    public async Task ZipReleasesTheInputsThatWereStillLiveWhenItCompletedEagerly()
    {
        // The other half of eager completion, and the one a source can see: the junction closes every
        // channel it reads when it stops, so an input that would never have ended by itself has its demand
        // cancelled and its enumerator released. A junction that closed only the input that ended would
        // leave the endless one's thread holding an enumerator forever.
        RecordingEnumerable<int> endless = new(1);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Collect("stage-4", 8),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("rows", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(endless)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10, 20))),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", CollectingRows(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the endless input is released when the bounded one ends");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(["1-10", "1-20"], rows);
        Assert.True(endless.Releases >= 1, "the endless input's enumerator is released");
    }

    [Fact]
    public async Task ZipEmitsNothingAtAllWhenAnInputIsEmptyFromTheStart()
    {
        // The degenerate end of the eager rule, and the one shape where a row-building junction emits
        // nothing whatever: an input that has already ended when the run starts means no first row can be
        // completed, so the junction completes at its first look and releases the input that would have
        // gone on forever. It is a completion and not a failure — an empty input is an empty result.
        RecordingEnumerable<int> endless = new(1);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "empty"),
                    Node("stage-2", "cycle"),
                    Node("stage-3", "zip"),
                    Collect("stage-4", 8),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("rows", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Empty()),
                ("stage-2", LocalStageDescriptor.Cycle(endless)),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", CollectingRows(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes although no row could ever be built");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Empty(rows);
        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
        Assert.True(endless.Releases >= 1, "the endless input's enumerator is released");
    }

    [Fact]
    public async Task ZipReadsNothingUntilThereIsRoomForTheRowItWouldMake()
    {
        // Room first, read second, counted from the source end. Four elements leave each input and no fifth:
        // one pair is in the parked sink's hand, one pair is in the junction's output channel, one element
        // is in each input's own channel, and one is in each source's hand. Nothing at all is inside the
        // junction, because it never starts a row it has nowhere to put — a junction that read first and
        // waited for room afterwards would have taken one more element from each input.
        Gate gate = new();
        Lock counting = new();
        List<string> seen = [];
        int saturatedInputs = 0;
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource readAhead = new(TaskCreationOptions.RunContinuationsAsynchronously);

        void Count(int pulls)
        {
            if (pulls != 4)
            {
                return;
            }

            lock (counting)
            {
                if (++saturatedInputs == 2)
                {
                    saturated.TrySetResult();
                }
            }
        }

        Task? Barrier(int position)
        {
            if (position == 4)
            {
                readAhead.TrySetResult();
            }

            return null;
        }

        RecordingEnumerable<int> left = new(1, 2, 3, 4, 5, 6, 7, 8, 9)
        {
            Pulled = Count,
            PullBarrier = Barrier,
        };

        RecordingEnumerable<int> right = new(11, 12, 13, 14, 15, 16, 17, 18, 19)
        {
            Pulled = Count,
            PullBarrier = Barrier,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(left)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(right)),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
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

        await Reaches(saturated.Task, "both inputs reach the bound a parked sink allows");

        Assert.False(readAhead.Task.IsCompleted);
        Assert.Equal(4, left.Pulls);
        Assert.Equal(4, right.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the sink is released");

        Assert.Equal(9, left.Pulls);
        Assert.Equal(9, right.Pulls);

        lock (counting)
        {
            Assert.Equal(
                ["1-11", "2-12", "3-13", "4-14", "5-15", "6-16", "7-17", "8-18", "9-19"],
                seen);
        }
    }

    [Fact]
    public async Task ZipHoldsTheColumnsOfOneRowAndPullsEachInputOncePerRow()
    {
        // The N−1 bound, and the demand rule that produces it, on three inputs where the difference is
        // visible. The slowest input is held after its first element, so the junction is assembling a row it
        // cannot finish; each fast input has given that row exactly one column and is not read again, so
        // four of its elements exist — one in the row that was emitted, one in the column being held, one in
        // its channel, one in its hand — and a fifth is never pulled. Two columns held at once is N−1 for
        // three inputs; an input pulled twice for one row would show as a fifth element here.
        Lock counting = new();
        List<string> seen = [];
        int saturatedInputs = 0;
        TaskCompletionSource emitted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource readAhead = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);

        void Count(int pulls)
        {
            if (pulls != 4)
            {
                return;
            }

            lock (counting)
            {
                if (++saturatedInputs == 2)
                {
                    saturated.TrySetResult();
                }
            }
        }

        Task? Barrier(int position)
        {
            if (position == 4)
            {
                readAhead.TrySetResult();
            }

            return null;
        }

        RecordingEnumerable<int> left = new(1, 2, 3, 4, 5, 6, 7, 8, 9)
        {
            Pulled = Count,
            PullBarrier = Barrier,
        };

        RecordingEnumerable<int> right = new(11, 12, 13, 14, 15, 16, 17, 18, 19)
        {
            Pulled = Count,
            PullBarrier = Barrier,
        };

        RecordingEnumerable<int> slow = new(100)
        {
            PullBarrier = position => position == 1 ? held.Task : null,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "from-enumerable"),
                    Node("stage-4", "zip"),
                    Node("stage-5", "for-each"),
                ],
                [
                    Into("stage-1", "stage-4", 0),
                    Into("stage-2", "stage-4", 1),
                    Into("stage-3", "stage-4", 2),
                    Edge("stage-4", "stage-5"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(left)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(right)),
                ("stage-3", LocalStageDescriptor.FromEnumerable(slow)),
                ("stage-4", LocalStageDescriptor.Zip(Rows())),
                (
                    "stage-5",
                    CallingRows(row =>
                    {
                        lock (counting)
                        {
                            seen.Add(row);
                        }

                        emitted.TrySetResult();
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable releasing = Completing(held);

        await Reaches(emitted.Task, "the one row the slowest input allows reaches the sink");
        await Reaches(saturated.Task, "both fast inputs reach the bound one held column allows");

        Assert.False(readAhead.Task.IsCompleted);
        Assert.Equal(4, left.Pulls);
        Assert.Equal(4, right.Pulls);

        lock (counting)
        {
            Assert.Equal(["1-11-100"], seen);
        }

        held.SetResult();

        await Reaches(run.Completion, "the run completes when the slowest input ends");

        // Still four: the columns the junction was holding were discarded with the row, and nothing was read
        // to replace them.
        Assert.Equal(4, left.Pulls);
        Assert.Equal(4, right.Pulls);

        lock (counting)
        {
            Assert.Equal(["1-11-100"], seen);
        }
    }

    [Fact]
    public async Task AZipEndsEveryInputWhenTheStreamBelowItEnds()
    {
        // The completion that arrives from the other direction, which for a row-building junction has one
        // more thing to release than for a simple one. Both inputs are endless, so nothing but a stop from
        // below can end this run: the take is satisfied, the segment below closes the junction's output, and
        // the junction closes every channel it reads — including the one whose column it happened to be
        // holding for a row that will now never be built.
        RecordingEnumerable<int> left = new(1);
        RecordingEnumerable<int> right = new(2);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "cycle"),
                    Node("stage-3", "zip"),
                    Counted("stage-4", "take", 3),
                    Collect("stage-5", 8),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-4", "stage-5"),
                ],
                [Slot("rows", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(left)),
                ("stage-2", LocalStageDescriptor.Cycle(right)),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", LocalStageDescriptor.Take(3)),
                ("stage-5", CollectingRows(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the endless inputs are released when the take is satisfied");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(["1-2", "1-2", "1-2"], rows);
        Assert.True(left.Releases >= 1, "the first endless input's enumerator is released");
        Assert.True(right.Releases >= 1, "the second endless input's enumerator is released");
    }

    [Fact]
    public async Task ABufferOnAnInputOfAZipIsThatInputsOwnChannel()
    {
        // The rule a buffer in front of an asynchronous stage, on a leg of a fan-out, and on an input of a
        // merge already follows, on an input of a junction that can hold columns: the author asked for four
        // elements of prefetch on this input and the run holds four, not four plus a handoff. Seven —
        // one row in the parked sink, one row in the junction's output channel, four in the buffer the
        // author wrote, one in the source's hand — which is the same seven the same buffer allows on an
        // input of a merge, and the coincidence is worth the sentence: a zip parked for room holds no column
        // at all, because it never starts a row it has nowhere to put. The N−1 the table allows it is what
        // it holds while an input is slow, not while a sink is.
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource readAhead = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> buffered = new(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        {
            Pulled = pulls =>
            {
                if (pulls == 7)
                {
                    saturated.TrySetResult();
                }
            },
        };

        buffered.PullBarrier = position =>
        {
            if (position == 7)
            {
                readAhead.TrySetResult();
            }

            return null;
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Buffer("stage-2", 4),
                    Node("stage-3", "cycle"),
                    Node("stage-4", "zip"),
                    Node("stage-5", "for-each"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Into("stage-2", "stage-4", 0),
                    Into("stage-3", "stage-4", 1),
                    Edge("stage-4", "stage-5"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(buffered)),
                ("stage-2", Buffering(4)),
                ("stage-3", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(0))),
                ("stage-4", LocalStageDescriptor.Zip(Rows())),
                (
                    "stage-5",
                    CallingRows(_ => gate.Wait()))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source fills the buffer the author wrote and no more");

        Assert.False(readAhead.Task.IsCompleted);
        Assert.Equal(7, buffered.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the buffered input is released");

        Assert.Equal(12, buffered.Pulls);
    }

    [Fact]
    public async Task AFailingInputFailsAZippingRunWhileThePumpWaitsOnTheOthers()
    {
        // ADR 0005's first shared rule in the position that makes it worth stating, and the row-building
        // pump needed no code for it either: the junction is asleep waiting for the columns of its first
        // row, the input that fails is not one it could have read anything from, and the failure cancels the
        // run's token, on which every wait this pump takes is taken.
        InvalidOperationException failure = new("the second input gives up");

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "never"),
                    Node("stage-2", "failed"),
                    Node("stage-3", "zip"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Never()),
                ("stage-2", LocalStageDescriptor.Failed(failure)),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException raised =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Same(failure, raised);
    }

    [Fact]
    public async Task AFailingCombinerFailsTheRunWithItsOwnException()
    {
        // The combiner is the author's code and runs on the junction's own thread, so what it throws is what
        // the run reports — unwrapped and instance-identical, exactly as a mapping stage's failure is. It is
        // worth a test of its own because a junction is the one place this runtime calls an author's
        // delegate from a pump rather than from a fused stage.
        InvalidOperationException failure = new("this row cannot be built");

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10, 20))),
                ("stage-3", LocalStageDescriptor.Zip((Func<object?[], object?>)(_ => throw failure))),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException raised =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Same(failure, raised);
    }

    [Fact]
    public async Task TheCombinerReceivesARowOfItsOwnRatherThanTheJunctionsSlots()
    {
        // The array a combiner is handed is the author's to keep, which is a real promise rather than an
        // implementation detail: the junction goes on writing into its own slots — a zip releases them the
        // moment the row is placed — so an author who kept what they were handed would watch it change or
        // empty behind them. Keeping every row and reading them all after the run is over is the only way
        // to tell the two apart, because a combiner that reads its argument immediately cannot.
        Lock keeping = new();
        List<object?[]> handed = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "zip"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10, 20, 30))),
                (
                    "stage-3",
                    LocalStageDescriptor.Zip((Func<object?[], object?>)(parts =>
                    {
                        lock (keeping)
                        {
                            handed.Add(parts);
                        }

                        return string.Join('-', parts);
                    }))),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the inputs have been paired to their end");

        lock (keeping)
        {
            Assert.Equal(
                ["1-10", "2-20", "3-30"],
                handed.Select(row => string.Join('-', row)));
        }
    }

    [Fact]
    public async Task EightInputsPairIntoOneRow()
    {
        // The declared ceiling, and the statement that a row-building junction takes its arity from its
        // edges like every other one: the combiner is handed one element per wired input, in port order, and
        // nothing anywhere wrote the number eight down.
        List<Orleans.Dataflow.Definition.StageNode> nodes = [Node("stage-1", "zip"), Collect("stage-2", 4)];
        List<Orleans.Dataflow.Definition.GraphEdge> edges = [Edge("stage-1", "stage-2")];
        List<(string Node, LocalStageDescriptor Stage)> bindings =
        [
            ("stage-1", LocalStageDescriptor.Zip(Rows())),
            ("stage-2", CollectingRows(4)),
        ];

        for (int input = 0; input < LocalVocabulary.MaxFanIn; input++)
        {
            string source = $"source-{input}";

            nodes.Add(Node(source, "from-enumerable"));
            edges.Add(Into(source, "stage-1", input));
            bindings.Add((source, LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(input))));
        }

        RunnableGraph graph = Graph(Declaring(nodes, edges, [Slot("rows", "stage-2")]), Bindings([.. bindings]));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when all eight inputs have been paired");

        string[] rows = await run.GetValueAsync(Result<string[]>(graph, "rows"), TestToken);

        Assert.Equal(["0-1-2-3-4-5-6-7"], rows);
    }

    [Fact]
    public async Task APauseTakesEffectOnAZipHoldingAPartialRow()
    {
        // The control plane across branching topologies is a later checkpoint and this is not it. What is
        // claimed here is that the row-building pump comes to rest where every other segment does, in the
        // one state only this pump has: a column already read, a row that cannot be completed, and a wait on
        // the input that would complete it. The run is arranged so that there is no other way to be quiet —
        // the first input has ended and the second never produces anything — so the quiescence a pause waits
        // for is the junction's own wait reporting itself, and a wait that did not report itself would hang
        // the pause on the very quiet it caused.
        RecordingEnumerable<int> once = new(7);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "never"),
                    Node("stage-3", "zip"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(once)),
                ("stage-2", LocalStageDescriptor.Never()),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(once.Released, "the input that ends is released");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a junction holding a partial row");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");

        // Nothing can end this run of its own accord — a zip whose row can never be completed is not a
        // completed zip, it is a zip still waiting — so disposal is what ends it.
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task AZippingRunShutsDownWithWhatItHad()
    {
        // Shutdown is still "stop pulling and keep what you have" for a junction that holds a row: both
        // sources observe it, the rows already assembled drain through the junction, whatever column was
        // held when the inputs ran out belongs to a row that will never exist, and the run ends successfully
        // rather than being cancelled.
        Gate gate = new();
        Lock counting = new();
        List<string> seen = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "cycle"),
                    Node("stage-3", "zip"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(2))),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
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

        await Reaches(shutdown, "the shutdown of a zipping run completes");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        lock (counting)
        {
            Assert.NotEmpty(seen);
            Assert.All(seen, row => Assert.Equal("1-2", row));
        }
    }

    [Fact]
    public async Task CancellingAZippingRunEndsItCanceled()
    {
        // The other half of the same sentence: cancellation abandons what is queued and what is held, and a
        // junction asleep on inputs that never produce anything is released by the token rather than left
        // holding a thread.
        using CancellationTokenSource cancellation = new();

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "never"),
                    Node("stage-2", "never"),
                    Node("stage-3", "zip"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Never()),
                ("stage-2", LocalStageDescriptor.Never()),
                ("stage-3", LocalStageDescriptor.Zip(Rows())),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await cancellation.CancelAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);
        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }
}
