using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The first three rows of ADR 0005's fan-in table, one row at a time and as behavior rather than as a
/// claim about the plan.
/// </summary>
/// <remarks>
/// <para>
/// Each junction promises four things — what it emits, in what order, when it completes, and what it holds
/// — and every one of them is observable from outside the engine. What a junction emits is what the sink
/// collects. The order is what makes the three different: a concat and an interleave promise an exact
/// sequence and are asserted against one, while a merge promises no cross-input order at all and is
/// therefore asserted against the multiset and against each input's own subsequence, which is the only
/// order it does promise. When a junction completes is which elements survive the input that ended first.
/// And what it holds is how far a held source gets, which is the same bounded-memory reasoning the buffer
/// and fan-out suites use: "room first, read second" is exactly one element cheaper than "read first, wait
/// second".
/// </para>
/// <para>
/// No test here waits on a clock to make a claim. A gate holds a run at a known point, a pull barrier holds
/// a source at a known element, and the deadline in <see cref="JunctionFixtures.Reaches"/> exists so that a
/// broken completion rule is reported rather than hung on.
/// </para>
/// </remarks>
public sealed class FanInTests
{
    [Fact]
    public async Task MergeEmitsEveryElementOfEveryInputAndKeepsEachInputsOwnOrder()
    {
        // Rule 4 of ADR 0005 is the whole of what a merge promises about order: no element of one input
        // overtakes another element of that same input. Across inputs it promises nothing, so what is
        // asserted here is the multiset and the two subsequences, and never the interleaving of the two.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "merge"),
                    Collect("stage-4", 16),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("joined", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 3, 5, 7))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(2, 4, 6, 8))),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both inputs have");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8], joined.Order());
        Assert.Equal([1, 3, 5, 7], joined.Where(value => value % 2 == 1));
        Assert.Equal([2, 4, 6, 8], joined.Where(value => value % 2 == 0));
    }

    [Fact]
    public async Task MergeCompletesOnlyWhenEveryInputHasCompleted()
    {
        // The eager-complete variant is a declared mode nobody has asked for, and this is the difference it
        // would make. The second input is held at its very first pull, so the junction has nothing to read
        // and is asleep on both inputs when the first of them ends — which is exactly the moment a junction
        // that completed on the first completion would end the run, discarding five elements that had not
        // been produced yet. The held input is released only once the other has provably ended.
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> nothing = new();
        RecordingEnumerable<int> everything = new(1, 2, 3, 4, 5)
        {
            PullBarrier = position => position == 0 ? held.Task : null,
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "merge"),
                    Collect("stage-4", 16),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("joined", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(nothing)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(everything)),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Completing(held);

        await Reaches(nothing.Released, "the first input ends without producing anything");

        held.SetResult();

        await Reaches(run.Completion, "the run completes when the second input has ended too");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([1, 2, 3, 4, 5], joined);
    }

    [Fact]
    public async Task MergeDoesNotStarveAnInputBehindAFasterOne()
    {
        // The rotation is what the fairness clause of ADR 0005 buys, and this is the run that tells the
        // difference: the first input never runs out, so a junction that scanned its inputs in port order
        // every time would take from it forever and the second input's one element would never be emitted
        // at all. The sink is held until that element is provably in its channel, so what is under test is
        // the choice the junction makes and not which thread started first.
        Gate gate = new();
        RecordingEnumerable<int> endless = new(1);
        RecordingEnumerable<int> occasional = new(99);
        TaskCompletionSource collected = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> seen = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "merge"),
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
                ("stage-1", LocalStageDescriptor.Cycle(endless)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(occasional)),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", LocalStageDescriptor.Take(6)),
                (
                    "stage-5",
                    Calling(value =>
                    {
                        gate.Wait();

                        lock (seen)
                        {
                            seen.Add(value);

                            if (seen.Count == 6)
                            {
                                collected.TrySetResult();
                            }
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        // The slow input's sequence has been enumerated to its end, so its element is either in its channel
        // or already through the junction; either way the junction has had it to choose from.
        await Reaches(occasional.Released, "the slow input produces its one element");

        gate.Open();

        await Reaches(collected.Task, "the sink receives the six elements it asked for");
        await Reaches(run.Completion, "the run ends when the take is satisfied");

        lock (seen)
        {
            Assert.Contains(99, seen);
            Assert.Equal(6, seen.Count);
        }
    }

    [Fact]
    public async Task MergeHoldsAtMostOneElementOutsideItsChannels()
    {
        // The bound counted from the source end, with the second input empty so that every element the run
        // absorbed came from one place. Four: one in the parked sink, one in the junction's output channel,
        // one in the input channel, and one in the source's own hand at a full channel — and none in the
        // junction, because it asks for room before it reads. A junction that read first and waited
        // afterwards would hold one more and the source would have got one further.
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9)
        {
            Pulled = pulls =>
            {
                if (pulls == 4)
                {
                    saturated.TrySetResult();
                }
            },
        };

        elements.PullBarrier = position =>
        {
            if (position == 5)
            {
                exhausted.TrySetResult();
            }

            return null;
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "empty"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Empty()),
                ("stage-3", LocalStageDescriptor.Merge()),
                (
                    "stage-4",
                    Calling(_ =>
                    {
                        gate.Wait();
                        elements.Consumed();
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source reaches the bound a parked sink allows");

        Assert.False(exhausted.Task.IsCompleted);
        Assert.Equal(4, elements.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the sink is released");

        // The peak is the bound stated without a race: it is the greatest number of elements the run ever
        // held at once over the whole run, read after the run is over, rather than a count sampled at a
        // moment that might have been one step early.
        Assert.Equal(9, elements.Pulls);
        Assert.Equal(4, elements.PeakInFlight);
    }

    [Fact]
    public async Task MergeAbsorbsOnlyWhatItsChannelsAndItsSourcesHandsHold()
    {
        // The same bound with both inputs live, where the total is a fact and the split is not: two parked
        // channels, two hands, one output channel, one parked sink. Which source the two elements
        // downstream came from is exactly the thing a merge promises nothing about, so the assertion is the
        // sum — and the barriers are what say that neither source ran away while the other was counted.
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Lock counting = new();
        int pulled = 0;
        int outstanding = 0;
        int peak = 0;

        RecordingEnumerable<int> left = new(1, 2, 3, 4, 5, 6);
        RecordingEnumerable<int> right = new(11, 12, 13, 14, 15, 16);

        void Count(int _)
        {
            lock (counting)
            {
                peak = Math.Max(peak, ++outstanding);

                if (++pulled == 6)
                {
                    saturated.TrySetResult();
                }
            }
        }

        left.Pulled = Count;
        right.Pulled = Count;

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(left)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(right)),
                ("stage-3", LocalStageDescriptor.Merge()),
                (
                    "stage-4",
                    Calling(_ =>
                    {
                        gate.Wait();

                        lock (counting)
                        {
                            outstanding--;
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the two sources together reach the bound a parked sink allows");

        Assert.Equal(6, left.Pulls + right.Pulls);
        Assert.InRange(left.Pulls, 2, 4);
        Assert.InRange(right.Pulls, 2, 4);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the sink is released");

        // The peak is over both sequences at once and over the whole run, which is the only form in which
        // this graph's bound is a fact rather than a sample: the split between the two inputs is exactly
        // what a merge promises nothing about, and the total is what its channels and its sources' hands
        // can hold.
        lock (counting)
        {
            Assert.Equal(6, peak);
        }
    }

    [Fact]
    public async Task AFailingInputFailsTheRunWhileTheJunctionWaitsOnTheOthers()
    {
        // ADR 0005's first shared rule, in the position that makes it worth stating: the junction is asleep
        // on an input that will never produce anything, and the input that fails is one it is not reading.
        // Nothing in the fan-in pump implements this — the failure cancels the run's token and every wait
        // this loop takes is taken on that token, which is what "failure wins" already meant.
        InvalidOperationException failure = new("the second input gives up");

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "never"),
                    Node("stage-2", "failed"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Never()),
                ("stage-2", LocalStageDescriptor.Failed(failure)),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException raised =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Same(failure, raised);
    }

    [Fact]
    public async Task ConcatEmitsEachInputToItsEndInPortOrder()
    {
        // The exact sequence, because a concat promises one. The third input is empty, which is what makes
        // this also the statement that a concat ends when its *last* input does rather than when any of
        // them does: a junction that stopped at the first completed input would have emitted nothing at
        // all, and one that stopped at the first empty one would have lost nothing here but would have
        // lost everything behind it.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "empty"),
                    Node("stage-4", "concat"),
                    Collect("stage-5", 16),
                ],
                [
                    Into("stage-1", "stage-4", 0),
                    Into("stage-2", "stage-4", 1),
                    Into("stage-3", "stage-4", 2),
                    Edge("stage-4", "stage-5"),
                ],
                [Slot("joined", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(3, 4, 5))),
                ("stage-3", LocalStageDescriptor.Empty()),
                ("stage-4", LocalStageDescriptor.Concat()),
                ("stage-5", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the last input has");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([1, 2, 3, 4, 5], joined);
    }

    [Fact]
    public async Task ConcatLeavesAnInputBehindTheActiveOneParkedInItsOwnChannel()
    {
        // "Inputs behind the active one are not pulled at all" is a statement about the junction and not
        // about the run: this engine starts every segment, so the source of the second input is running
        // from the first moment. What the junction does is not read that input's channel, and a bounded
        // channel with nobody reading it is what stops the source — one element in the channel, one in the
        // source's hand, and no third pull for as long as the first input is still being emitted. That is
        // backpressure doing the work the contract describes, and it is the honest form of the promise.
        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource waiting = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource readAhead = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource emitted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> seen = [];

        RecordingEnumerable<int> active = new(1, 2, 3, 4, 5)
        {
            PullBarrier = position => position == 3 ? held.Task : null,
        };

        RecordingEnumerable<int> behind = new(11, 12, 13)
        {
            Pulled = pulls =>
            {
                if (pulls == 2)
                {
                    waiting.TrySetResult();
                }
            },
            PullBarrier = position =>
            {
                if (position == 2)
                {
                    readAhead.TrySetResult();
                }

                return null;
            },
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "concat"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(active)),
                ("stage-2", LocalStageDescriptor.FromEnumerable(behind)),
                ("stage-3", LocalStageDescriptor.Concat()),
                (
                    "stage-4",
                    Calling(value =>
                    {
                        lock (seen)
                        {
                            seen.Add(value);

                            if (seen.Count == 3)
                            {
                                emitted.TrySetResult();
                            }
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Completing(held);

        await Reaches(waiting.Task, "the input behind the active one fills its channel and stops there");
        await Reaches(emitted.Task, "the active input's first three elements reach the sink");

        // The bound holds while the source is parked: two elements exist outside the sequence and there is
        // no third pull, however long the first input takes.
        Assert.False(readAhead.Task.IsCompleted);
        Assert.Equal(2, behind.Pulls);

        lock (seen)
        {
            Assert.Equal([1, 2, 3], seen);
        }

        held.SetResult();

        await Reaches(run.Completion, "the run completes once the active input is released");

        lock (seen)
        {
            Assert.Equal([1, 2, 3, 4, 5, 11, 12, 13], seen);
        }
    }

    [Fact]
    public async Task ConcatReleasesTheInputsWhoseTurnNeverCameWhenTheRunEndsEarly()
    {
        // A downstream completion reaches every input of a joining junction at once, including the endless
        // one the junction had not started reading. The junction closes every channel it reads when it
        // stops, which is what releases the sources parked in them; a junction that closed only the input
        // it was busy with would leave the others' threads holding an enumerator forever.
        RecordingEnumerable<int> endless = new(9);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "cycle"),
                    Node("stage-3", "concat"),
                    Counted("stage-4", "take", 1),
                    Collect("stage-5", 4),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-4", "stage-5"),
                ],
                [Slot("joined", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.Cycle(endless)),
                ("stage-3", LocalStageDescriptor.Concat()),
                ("stage-4", LocalStageDescriptor.Take(1)),
                ("stage-5", Collecting(4))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the endless input is released when the sink has what it asked for");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([1], joined);
        Assert.True(endless.Releases >= 1, "the endless input's enumerator is released");
    }

    [Fact]
    public async Task InterleaveEmitsADeclaredSegmentFromEachInputInTurn()
    {
        // The exact sequence again, and this one is a stronger claim than the concat's: an interleave waits
        // for the input whose turn it is even when the other has an element ready, so the output is a
        // function of the two inputs and of the declared segment size alone. The second input runs out
        // first, and the rotation continues over the remainder in order rather than ending with it.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Interleaving("stage-3", 2),
                    Collect("stage-4", 16),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("joined", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10, 20, 30))),
                ("stage-3", LocalStageDescriptor.Interleave(2)),
                ("stage-4", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both inputs have");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([1, 2, 10, 20, 3, 4, 30, 5, 6], joined);
    }

    [Fact]
    public async Task InterleaveContinuesOverTheRemainderWhenAnInputCompletesMidRotation()
    {
        // Three inputs and a segment of one, so the rotation is visible element by element and the input
        // that ends is the middle one: what the table promises is that the rotation closes over the two
        // that are left rather than skipping a turn or stopping.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "from-enumerable"),
                    Interleaving("stage-4", 1),
                    Collect("stage-5", 16),
                ],
                [
                    Into("stage-1", "stage-4", 0),
                    Into("stage-2", "stage-4", 1),
                    Into("stage-3", "stage-4", 2),
                    Edge("stage-4", "stage-5"),
                ],
                [Slot("joined", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10))),
                ("stage-3", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(100, 200, 300))),
                ("stage-4", LocalStageDescriptor.Interleave(1)),
                ("stage-5", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when all three inputs have");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([1, 10, 100, 2, 200, 3, 300, 4], joined);
    }

    [Fact]
    public async Task InterleaveHoldsAtMostOneElementOutsideItsChannels()
    {
        // The same four the merge allows, counted on the input whose turn it is. The other input is empty,
        // so the rotation never leaves the one being counted and the number is a statement about the
        // junction rather than about the rotation.
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9)
        {
            Pulled = pulls =>
            {
                if (pulls == 4)
                {
                    saturated.TrySetResult();
                }
            },
        };

        elements.PullBarrier = position =>
        {
            if (position == 5)
            {
                exhausted.TrySetResult();
            }

            return null;
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "empty"),
                    Interleaving("stage-3", 3),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Empty()),
                ("stage-3", LocalStageDescriptor.Interleave(3)),
                (
                    "stage-4",
                    Calling(_ =>
                    {
                        gate.Wait();
                        elements.Consumed();
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source reaches the bound a parked sink allows");

        Assert.False(exhausted.Task.IsCompleted);
        Assert.Equal(4, elements.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the sink is released");

        Assert.Equal(9, elements.Pulls);
        Assert.Equal(4, elements.PeakInFlight);
    }

    [Fact]
    public async Task ABufferOnAnInputIsThatInputsOwnChannelRatherThanASecondOne()
    {
        // The rule a buffer in front of an asynchronous stage and on a leg of a fan-out already follows,
        // applied to an input of a fan-in: the author asked for four elements of prefetch on this input and
        // the run holds four, not four plus a handoff. One parked sink, one in the junction's output
        // channel, four in the buffer the author wrote, one in the source's hand.
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        {
            Pulled = pulls =>
            {
                if (pulls == 7)
                {
                    saturated.TrySetResult();
                }
            },
        };

        elements.PullBarrier = position =>
        {
            if (position == 8)
            {
                exhausted.TrySetResult();
            }

            return null;
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Buffer("stage-2", 4),
                    Node("stage-3", "empty"),
                    Node("stage-4", "merge"),
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
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", Buffering(4)),
                ("stage-3", LocalStageDescriptor.Empty()),
                ("stage-4", LocalStageDescriptor.Merge()),
                (
                    "stage-5",
                    Calling(_ =>
                    {
                        gate.Wait();
                        elements.Consumed();
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source fills the buffer the author wrote and no more");

        Assert.False(exhausted.Task.IsCompleted);
        Assert.Equal(7, elements.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the buffered input is released");

        Assert.Equal(12, elements.Pulls);
        Assert.Equal(7, elements.PeakInFlight);
    }

    [Fact]
    public async Task TwoSourcesConvergeIntoOneSinkAndOneOutcome()
    {
        // Two heads and one ending, which is the whole of what "several sources are legal exactly when they
        // converge" means at run time: two threads pull two sequences, one slot resolves, and the run ends
        // once.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "range", "local-range-parameters", """{"start":1,"count":3}"""),
                    Node("stage-2", "range", "local-range-parameters", """{"start":10,"count":3}"""),
                    Node("stage-3", "merge"),
                    Node("stage-4", "count"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("total", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Range(1, 3)),
                ("stage-2", LocalStageDescriptor.Range(10, 3)),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", LocalStageDescriptor.Count())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both sources have");

        Assert.Equal(6L, await run.GetValueAsync(Result<long>(graph, "total"), TestToken));
    }

    [Fact]
    public async Task EightSourcesJoinThroughOneJunction()
    {
        // The declared ceiling, wired to the last input. The inputs past the second are optional ports, so
        // a graph states how many it joins by wiring them, and this is the statement that all eight carry
        // elements rather than only the two the compiler insists on.
        List<Orleans.Dataflow.Definition.StageNode> nodes = [Node("stage-1", "merge"), Collect("stage-2", 16)];
        List<Orleans.Dataflow.Definition.GraphEdge> edges = [Edge("stage-1", "stage-2")];
        List<(string Node, LocalStageDescriptor Stage)> bindings =
        [
            ("stage-1", LocalStageDescriptor.Merge()),
            ("stage-2", Collecting(16)),
        ];

        for (int input = 0; input < LocalVocabulary.MaxFanIn; input++)
        {
            string source = $"source-{input}";

            nodes.Add(Node(source, "from-enumerable"));
            edges.Add(Into(source, "stage-1", input));
            bindings.Add((source, LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(input))));
        }

        RunnableGraph graph = Graph(
            Declaring(nodes, edges, [Slot("joined", "stage-2")]),
            Bindings([.. bindings]));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when all eight inputs have");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([0, 1, 2, 3, 4, 5, 6, 7], joined.Order());
    }

    [Fact]
    public async Task ADiamondRejoinsWhatItSplit()
    {
        // Fan-out and fan-in in one graph: one source, two legs that map differently, one merge, one sink.
        // The merge promises no order across its inputs, so what is asserted is the multiset — the pairing
        // of an element with the leg it came through is recoverable from the values and from nothing the
        // junction promised.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "select"),
                    Node("stage-4", "select"),
                    Node("stage-5", "merge"),
                    Collect("stage-6", 16),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-4"),
                    Into("stage-3", "stage-5", 0),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                ],
                [Slot("joined", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value * 10))),
                ("stage-4", LocalStageDescriptor.Select((Func<int, int>)(value => value + 100))),
                ("stage-5", LocalStageDescriptor.Merge()),
                ("stage-6", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both legs have rejoined");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([10, 20, 30, 101, 102, 103], joined.Order());
        Assert.Equal([10, 20, 30], joined.Where(value => value < 100));
        Assert.Equal([101, 102, 103], joined.Where(value => value >= 100));
    }

    [Fact]
    public async Task ASplitThatFeedsAJoinDirectlyNeedsNothingBetweenThem()
    {
        // The shortest diamond there is: a leg of the fan-out is already a channel, and it is the very
        // channel the fan-in reads, so no relay segment stands between the two junctions. An interleave is
        // used rather than a merge because it makes the sequence a fact: each element reaches both legs and
        // the rotation takes them one at a time.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Interleaving("stage-3", 1),
                    Collect("stage-4", 16),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Rejoins("stage-2", 0, "stage-3", 0),
                    Rejoins("stage-2", 1, "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                ],
                [Slot("joined", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Interleave(1)),
                ("stage-4", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the split has rejoined itself");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([1, 1, 2, 2, 3, 3], joined);
    }

    [Fact]
    public async Task AJoiningJunctionEndsEveryInputWhenTheStreamBelowItEnds()
    {
        // Both inputs are endless, so nothing but a completion from below can end this run. The take is
        // satisfied, the segment below closes the junction's output, and the junction closes every channel
        // it reads — which is what releases both sources rather than only the one it happened to be reading.
        RecordingEnumerable<int> left = new(1);
        RecordingEnumerable<int> right = new(2);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "cycle"),
                    Node("stage-3", "merge"),
                    Counted("stage-4", "take", 3),
                    Collect("stage-5", 8),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-4", "stage-5"),
                ],
                [Slot("joined", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(left)),
                ("stage-2", LocalStageDescriptor.Cycle(right)),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", LocalStageDescriptor.Take(3)),
                ("stage-5", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the endless inputs are released when the take is satisfied");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal(3, joined.Length);
        Assert.All(joined, value => Assert.Contains(value, (int[])[1, 2]));
        Assert.True(left.Releases >= 1, "the first endless input's enumerator is released");
        Assert.True(right.Releases >= 1, "the second endless input's enumerator is released");
    }

    [Fact]
    public async Task AJoinThatFeedsASplitIsJustAnotherBranch()
    {
        // The two junction shapes composed the other way round, and the assertion is the one only this
        // shape can make: the merge decides an order nobody promised, and the broadcast below it delivers
        // exactly that order to both of its legs, so the two sinks agree element for element whatever the
        // merge chose.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "broadcast"),
                    Collect("stage-5", 16),
                    Collect("stage-6", 16),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-4", 0, "stage-5"),
                    Leg("stage-4", 1, "stage-6"),
                ],
                [Slot("left", "stage-5"), Slot("right", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(11, 12, 13))),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", LocalStageDescriptor.Broadcast()),
                ("stage-5", Collecting(16)),
                ("stage-6", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both sinks below the split have");

        int[] left = await run.GetValueAsync(Result<int[]>(graph, "left"), TestToken);
        int[] right = await run.GetValueAsync(Result<int[]>(graph, "right"), TestToken);

        Assert.Equal([1, 2, 3, 11, 12, 13], left.Order());
        Assert.Equal(left, right);
    }

    [Fact]
    public async Task AJoinOnAnInputOfAJoinIsJustAnotherBranch()
    {
        // Nothing about a junction says what may feed it, so one of a concat's inputs is a merge. The
        // concat reads its first input to the end before touching the second, and the second is the merge
        // — so the exact prefix is a fact and the tail is the merge's multiset.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "from-enumerable"),
                    Node("stage-4", "merge"),
                    Node("stage-5", "concat"),
                    Collect("stage-6", 16),
                ],
                [
                    Into("stage-1", "stage-5", 0),
                    Into("stage-2", "stage-4", 0),
                    Into("stage-3", "stage-4", 1),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                ],
                [Slot("joined", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(10, 20))),
                ("stage-3", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(30, 40))),
                ("stage-4", LocalStageDescriptor.Merge()),
                ("stage-5", LocalStageDescriptor.Concat()),
                ("stage-6", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the joined inputs of the join have");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([1, 2], joined.Take(2));
        Assert.Equal([10, 20, 30, 40], joined.Skip(2).Order());
        Assert.Equal([10, 20], joined.Skip(2).Where(value => value < 30));
        Assert.Equal([30, 40], joined.Skip(2).Where(value => value >= 30));
    }

    [Fact]
    public async Task ADroppingBoundaryBelowAJoiningJunctionDropsRatherThanPacingIt()
    {
        // The mirror of a leg that drops. A junction secures room before it reads, and "room" at a boundary
        // whose policy is not backpressure is a question already answered: such a boundary takes the offer
        // whatever it holds, so the junction keeps reading and the policy the author declared does the
        // losing — visibly, on the run's own counter, rather than by quietly pacing the input.
        //
        // The source reaching its end while the sink has never moved past its first element is the whole
        // proof. Under a boundary that waits, this same graph stops the source after four elements, which
        // is what the bound tests above count; here it runs out, and the elements it produced meanwhile
        // were discarded rather than queued.
        Gate gate = new();
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> seen = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8)
        {
            Pulled = pulls =>
            {
                if (pulls == 8)
                {
                    exhausted.TrySetResult();
                }
            },
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "empty"),
                    Node("stage-3", "merge"),
                    Buffer("stage-4", 1, "drop-newest"),
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
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Empty()),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", Buffering(1)),
                (
                    "stage-5",
                    Calling(value =>
                    {
                        gate.Wait();

                        lock (seen)
                        {
                            seen.Add(value);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "the sink reaches its first element and stays there");
        await Reaches(exhausted.Task, "the source runs out although the sink never moved");

        // The junction had taken six of them by then and had somewhere to put two: one in the sink's hand
        // and one in the buffer the author declared. The rest were dropped, and dropping is never silent.
        Assert.Equal(8, elements.Pulls);
        Assert.True(run.DroppedElements >= 4, $"the dropping boundary discarded {run.DroppedElements}");

        gate.Open();

        await Reaches(run.Completion, "the run completes without the dropping boundary pacing the input");

        lock (seen)
        {
            // Every element either arrived or was counted as lost; nothing vanished unaccounted for, which
            // is the invariant a drop counter exists to make checkable.
            Assert.Equal(8L, seen.Count + run.DroppedElements);
            Assert.Equal(seen.Order(), seen);
        }
    }

    [Fact]
    public async Task ASplitFeedingAJoinThatWaitsHeadOfLineNeedsTheBufferItsShapeImplies()
    {
        // The hazard the two contracts make when they are composed, and the declaration that resolves it.
        // A broadcast pulls only when every live leg has room, and an interleave waits for the input whose
        // turn it is even when the other has an element ready; with a segment of two and a handoff of one,
        // the junction waits for a second element on a leg the split cannot fill until the other leg is
        // drained, and the run stops. It is the same shape as a cycle with no boundary in it: a wait that
        // only the waiter could release. Two elements of declared buffer on each leg is the head-of-line
        // depth the segment size asks for, and with it the run is not only alive but exactly determined.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Buffer("stage-3", 2),
                    Buffer("stage-4", 2),
                    Interleaving("stage-5", 2),
                    Collect("stage-6", 32),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-2", 1, "stage-4"),
                    Into("stage-3", "stage-5", 0),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                ],
                [Slot("joined", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Buffering(2)),
                ("stage-4", Buffering(2)),
                ("stage-5", LocalStageDescriptor.Interleave(2)),
                ("stage-6", Collecting(32))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the buffered legs let the rotation get its second element");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([1, 2, 1, 2, 3, 4, 3, 4, 5, 6, 5, 6], joined);
    }

    [Fact]
    public async Task APausedJoiningRunComesToRestAndMovesAgain()
    {
        // The control plane across branching topologies is a later checkpoint and this is not it. What is
        // claimed here is only that the fan-in pump comes to rest where every other segment does: a pause
        // of a joining graph reaches quiescence rather than waiting forever on a pump asleep on a wait it
        // never reported, and resuming it delivers the rest.
        //
        // One input never ends, which is what makes the claim a fact rather than a race: the run cannot
        // have finished before the pause was asked for, so the quiescence being waited on is a real one —
        // a junction asleep on its inputs, a source asleep on nothing at all, and a sink asleep on its
        // channel. Ending it is then the shutdown's job, and the drain is what resolves the sink.
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(1, 16)]);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "never"),
                    Node("stage-3", "merge"),
                    Collect("stage-4", 64),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                [Slot("joined", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Never()),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", Collecting(64))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a joining run");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(elements.Released, "the resumed run reads the input that ends to its end");
        await Reaches(run.ShutdownAsync().AsTask(), "the shutdown ends the input that does not");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([.. Enumerable.Range(1, 16)], joined);
    }

    [Fact]
    public async Task APauseTakesEffectOnAJunctionAsleepOnItsInputs()
    {
        // The sharp form of the pause claim, and the one the fan-in pump had to earn: a junction waiting on
        // several inputs at once is asleep in a wait of this runtime's own, not parked at a safe point, and
        // a wait that did not report itself would leave a pause waiting forever for the very quiet it
        // caused. The run is arranged so that there is no other way to be quiet — the first input has ended
        // and the second never produces anything, so by the time the sink has seen the one element there
        // is, the junction can only be in that wait.
        TaskCompletionSource delivered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> once = new(7);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "never"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(once)),
                ("stage-2", LocalStageDescriptor.Never()),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", Calling(_ => delivered.TrySetResult()))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(delivered.Task, "the one element there is reaches the sink");
        await Reaches(once.Released, "the input that ends is released");
        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a junction asleep on its inputs");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");

        // Nothing can end this run of its own accord, so disposal is what ends it; the claim here was only
        // that a pause reaches a junction that is waiting rather than working.
        await run.DisposeAsync();

        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }

    [Fact]
    public async Task AJoiningRunShutsDownWithWhatItHad()
    {
        // Shutdown is still "stop pulling and keep what you have" when there is more than one place to stop
        // pulling: both sources observe it, everything already admitted drains through the junction, and
        // the run ends successfully rather than being cancelled.
        Gate gate = new();
        List<int> seen = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "cycle"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(2))),
                ("stage-3", LocalStageDescriptor.Merge()),
                (
                    "stage-4",
                    Calling(value =>
                    {
                        gate.Wait();

                        lock (seen)
                        {
                            seen.Add(value);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "the run reaches the sink");

        Task shutdown = run.ShutdownAsync().AsTask();

        gate.Open();

        await Reaches(shutdown, "the shutdown of a joining run completes");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        lock (seen)
        {
            Assert.NotEmpty(seen);
        }
    }

    [Fact]
    public async Task AShutdownDrainsAConcatInItsOwnOrder()
    {
        // Shutdown stops the sources and everything already admitted keeps flowing, which for a concat
        // means its own order survives the drain: the elements queued behind the input whose turn had not
        // come are delivered after the active input's, never mixed into them. The two inputs emit different
        // values so that the boundary between them is visible in what the sink received.
        Gate gate = new();
        List<int> seen = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "cycle"),
                    Node("stage-3", "concat"),
                    Node("stage-4", "for-each"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Cycle(new RecordingEnumerable<int>(2))),
                ("stage-3", LocalStageDescriptor.Concat()),
                (
                    "stage-4",
                    Calling(value =>
                    {
                        gate.Wait();

                        lock (seen)
                        {
                            seen.Add(value);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "the run reaches the sink");

        Task shutdown = run.ShutdownAsync().AsTask();

        gate.Open();

        await Reaches(shutdown, "the shutdown of a joining run completes");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        lock (seen)
        {
            Assert.NotEmpty(seen);
            Assert.Equal(seen.Order(), seen);
        }
    }

    [Fact]
    public async Task CancellingAJoiningRunEndsItCanceled()
    {
        // The other half of the same sentence, on the same shape: cancellation abandons what is queued, and
        // a junction asleep on inputs that never produce anything is released by the token rather than left
        // holding a thread.
        using CancellationTokenSource cancellation = new();

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "never"),
                    Node("stage-2", "never"),
                    Node("stage-3", "concat"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.Never()),
                ("stage-2", LocalStageDescriptor.Never()),
                ("stage-3", LocalStageDescriptor.Concat()),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await cancellation.CancelAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);
        Assert.Equal(TaskStatus.Canceled, run.Completion.Status);
    }
}
