using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// ADR 0005's cycle rule as validation, and the corrected cycle-completion contract as behavior.
/// </summary>
/// <remarks>
/// <para>
/// Two separate claims live here. The first is the ADR's: a cycle is legal exactly when it passes a
/// boundary that can answer without room below it, refused with the loop's node path otherwise. The second
/// is the one the M4 design got wrong and this checkpoint corrects: <b>closing the external inputs of a
/// cycle does not end it</b>. Elements circulating in a loop are a live stream whether or not anything
/// outside is still producing, so a cycle ends only from inside — a stage on the loop that ends its own
/// stream — or from outside by a stop. Every test below that says "still running" is that correction stated
/// as a fact.
/// </para>
/// <para>
/// The graphs are hand-built documents, as every junction test in this suite is, and every claim about a
/// moment is anchored on a gate or a pull barrier rather than on a delay. A loop that would run forever is
/// always ended by the test that started it: a shutdown, a cancellation, or a take on the loop itself.
/// </para>
/// </remarks>
public sealed class CycleTests
{
    [Fact]
    public async Task ACycleOfNothingButWaitingBoundariesIsRefusedWithItsNodePath()
    {
        // ADR 0005's rule, and the diagnostic an author can act on: every edge of this loop waits for room
        // below it, so the merge would wait for the broadcast and the broadcast for the merge. The path is
        // in the message because "there is a cycle" is not something anyone can fix.
        InvalidOperationException refused = await Refused(Loop(relief: null));

        Assert.Contains("passes no boundary that can answer without room below it", refused.Message, StringComparison.Ordinal);
        Assert.Contains(
            "'stage-2' -> 'stage-8' -> 'stage-3' -> 'stage-4' -> 'stage-2'",
            refused.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACycleThroughABackpressuringBufferIsRefusedLikeAnyOther()
    {
        // A declared buffer is not what makes a cycle legal; a buffer that can answer without downstream
        // room is. A backpressuring one of any capacity only postpones the deadlock — the loop fills it and
        // then waits for room that only the waiter could make — so it is refused exactly as a handoff is,
        // and the diagnostic still names the buffer as part of the loop.
        InvalidOperationException refused = await Refused(Loop(relief: "backpressure", capacity: 64));

        Assert.Contains("passes no boundary that can answer without room below it", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'stage-5'", refused.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("drop-oldest")]
    [InlineData("drop-newest")]
    [InlineData("drop-buffer")]
    [InlineData("fail")]
    public async Task ACycleThroughABoundaryThatAnswersWithoutRoomIsAccepted(string policy)
    {
        // Every declared policy but backpressure answers an offer whether or not it has room — by
        // dropping, by discarding what it held, or by failing the run — so every one of them breaks the
        // wait ADR 0005 is about. The failing policy is in the list deliberately: failing is an answer, and
        // a run that fails is not a run that hangs.
        RunnableGraph graph = Loop(relief: policy, take: 6);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the loop runs and its take on the loop ends it");
    }

    [Fact]
    public async Task ASelfLoopIsTestedByTheCycleRuleRatherThanRefusedForItsOwnSake()
    {
        // M0 refused a self-loop in the definition plane and said in the message that it was doing so only
        // until cycles arrived with a boundary contract. This is that contract applied: a node whose output
        // feeds its own input is a cycle of one node, refused by the same rule and named the same way.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "merge"),
                ],
                [
                    Into("stage-1", "stage-2", 0),
                    GraphEdge.Create(
                        PortAddress.Create(NodeId.Create("stage-2"), PortId.Create("out")),
                        PortAddress.Create(NodeId.Create("stage-2"), LocalVocabulary.FanInPort(1))),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Merge())));

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("passes no boundary that can answer without room below it", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'stage-2' -> 'stage-2'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AGraphWhoseEveryBranchRunsBackIntoAJunctionHasNoTerminal()
    {
        // Newly reachable now that a cycle can be planned at all: every branch of this document ends at
        // the junction it came from, so nothing consumes anything and no outcome could ever be reported.
        // Without cycles this shape does not exist, because following the edges of a finite acyclic graph
        // always reaches a node that feeds nothing.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "merge"),
                    Buffer("stage-3", 4, "drop-oldest"),
                ],
                [Into("stage-1", "stage-2", 0), Edge("stage-2", "stage-3"), Into("stage-3", "stage-2", 1)],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Merge()),
                ("stage-3", Buffering(4))));

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("no branch of it ends in a terminal", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ALiveCycleCirculatesItsElementsAndExitsThroughATakeOnTheLoop()
    {
        // The classic feedback shape, running. A source merges with its own downstream through a dropping
        // buffer and the loop's exit is a take *on the loop*: when the take reaches its bound it ends its
        // own stream, the completion walks upstream round the loop, and everything below drains.
        List<int> observed = [];
        RunnableGraph graph = Loop(relief: "drop-oldest", take: 5, exit: observed.Add);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the take on the loop ends the run");

        // Five elements passed the take and every one of them reached the exit. What the values are is a
        // scheduling question — how many laps a given element made before the take counted it — so the
        // count is the claim and the values are not.
        lock (observed)
        {
            Assert.Equal(5, observed.Count);
        }
    }

    [Fact]
    public async Task ClosingTheExternalInputsOfACycleDoesNotEndIt()
    {
        // The corrected contract, and the headline of this checkpoint. The design said completion enters a
        // cycle once every edge into it has completed; it does not, and it must not. The source here ends
        // after three elements and the loop goes on circulating them — which is often the whole point of
        // writing one — so the run is still going long after the only thing outside it has finished. A stop
        // is what ends it, and a shutdown is the graceful one.
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource circulating = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> elements = new(1, 2, 3);
        int laps = 0;

        elements.PullBarrier = position =>
        {
            if (position == 3)
            {
                exhausted.TrySetResult();
            }

            return null;
        };

        RunnableGraph graph = Loop(
            relief: "drop-oldest",
            source: elements,
            exit: _ =>
            {
                if (Interlocked.Increment(ref laps) == 60)
                {
                    circulating.TrySetResult();
                }
            });

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(exhausted.Task, "the source runs out");
        await Reaches(circulating.Task, "the loop keeps delivering long after the source ended");

        // Sixty deliveries out of a source of three: the elements the exit is seeing came round the loop
        // rather than out of anything outside it. Nothing outside is producing and the run has not ended.
        Assert.False(run.Completion.IsCompleted);
        Assert.Equal(3, elements.Pulls);

        await run.ShutdownAsync();
        await Reaches(run.Completion, "the shutdown ends the loop");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task ACycleWhoseElementsAllDieInsideItGoesQuietRatherThanCompleting()
    {
        // The third case of the corrected contract, and the one that is a limit rather than a promise. A
        // filter on the loop eventually drops everything, and after that no pump in the cycle can ever be
        // woken: the only thing that could produce on the junction's live input, or close it, is the loop
        // itself. The run stays alive. This engine does not detect that and deliberately does not guess —
        // the sound test is "every segment idle, every channel in the component empty, every channel into
        // it closed and empty", which is a racy answer to a termination problem, and an early guess would
        // truncate a run silently. So the hang is the documented behavior, and this is it, pinned as an
        // assertion rather than left as a sentence: the loop has gone quiet, everything it produced has
        // been delivered, and the run has not ended.
        TaskCompletionSource quiet = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> observed = [];
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "merge"),
                    Node("stage-3", "select"),
                    Node("stage-4", "where"),
                    Node("stage-5", "broadcast"),
                    Node("stage-6", "for-each"),
                    Buffer("stage-7", 4, "drop-oldest"),
                ],
                [
                    Into("stage-1", "stage-2", 0),
                    Edge("stage-2", "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-4", "stage-5"),
                    Leg("stage-5", 0, "stage-6"),
                    Leg("stage-5", 1, "stage-7"),
                    Into("stage-7", "stage-2", 1),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Merge()),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value + 1))),
                ("stage-4", LocalStageDescriptor.Where((Func<int, bool>)(value =>
                {
                    if (value < 5)
                    {
                        return true;
                    }

                    quiet.TrySetResult();

                    return false;
                }))),
                ("stage-5", LocalStageDescriptor.Broadcast()),
                ("stage-6", Calling(value =>
                {
                    lock (observed)
                    {
                        observed.Add(value);
                    }
                })),
                ("stage-7", Buffering(4))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(quiet.Task, "the last element the loop was carrying is dropped inside it");

        // Nothing can wake the loop again, so this is a fact about the run rather than a moment that had
        // not arrived yet: the source ended long ago, the junction's other input is fed only by the loop,
        // and the loop is empty.
        Assert.False(run.Completion.IsCompleted);

        await run.ShutdownAsync();
        await Reaches(run.Completion, "the shutdown is what ends a quiet loop");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        lock (observed)
        {
            Assert.Equal([2, 3, 4], observed);
        }
    }

    [Fact]
    public async Task ShutdownCutsTheFeedbackEdgeAndDeliversWhatWasInsideTheLoop()
    {
        // The other half of the same contract. A shutdown is "stop admitting new work", and a feedback
        // edge is the second place work enters a graph, so a shutdown closes it exactly as it stops a
        // pull. What was in that channel is drained rather than dropped: the exit is held at its first
        // element when the stop is requested, and every element the loop was carrying reaches it
        // afterwards.
        Gate gate = new();
        List<int> observed = [];
        RunnableGraph graph = Loop(
            relief: "drop-oldest",
            source: new RecordingEnumerable<int>(1),
            exit: value =>
            {
                lock (observed)
                {
                    observed.Add(value);
                }

                gate.Wait();
            });

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "the exit is holding the loop's first delivery");

        int held;

        lock (observed)
        {
            held = observed.Count;
        }

        // Requested rather than awaited: the run cannot finish while the exit is held, and a shutdown that
        // waited here would be waiting for the very thread this test is about to release.
        Task stopping = run.ShutdownAsync().AsTask();

        gate.Open();

        await Reaches(stopping, "the shutdown returns");
        await Reaches(run.Completion, "the run ends gracefully");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);

        lock (observed)
        {
            Assert.True(
                observed.Count > held,
                $"the loop held {held} deliveries at the cut and finished with {observed.Count}");
        }
    }

    [Fact]
    public async Task CancellationEndsARunHeldInsideACycle()
    {
        // Nothing new is needed for this and that is worth proving rather than assuming: every wait a
        // junction takes is taken on the run's token, and a cycle is junctions and channels like any other
        // part of a graph. The loop is circulating with nothing outside it when the token is cancelled.
        using CancellationTokenSource cancellation = new();
        TaskCompletionSource circulating = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int laps = 0;
        RunnableGraph graph = Loop(
            relief: "drop-oldest",
            source: new RecordingEnumerable<int>(1),
            exit: _ =>
            {
                if (Interlocked.Increment(ref laps) == 20)
                {
                    circulating.TrySetResult();
                }
            });

        await using RunHandle run = await Host.MaterializeAsync(graph, cancellation.Token);

        await Reaches(circulating.Task, "the loop is circulating");

        await cancellation.CancelAsync();

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);
    }

    [Fact]
    public async Task DisposingALiveCycleEndsIt()
    {
        // The path an author reaches by leaving a loop running: `await using` ends the scope and disposal
        // cancels. It is worth its own test because a cycle is the one shape whose segments have nothing
        // outside them to run out of, so a wait that was not taken on the run's token would hang here and
        // nowhere else — and a disposal that hangs takes the caller with it.
        TaskCompletionSource circulating = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int laps = 0;
        RunnableGraph graph = Loop(
            relief: "drop-oldest",
            source: new RecordingEnumerable<int>(1),
            exit: _ =>
            {
                if (Interlocked.Increment(ref laps) == 20)
                {
                    circulating.TrySetResult();
                }
            });

        RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(circulating.Task, "the loop is circulating");
        await Reaches(run.DisposeAsync().AsTask(), "disposal ends the loop");

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run.Completion);
    }

    [Fact]
    public async Task AFailureInsideACycleFailsTheRun()
    {
        // Failure wins, from inside a loop as from anywhere else. The stage on the loop throws on the lap
        // that meets its bound, and the failure cancels the run's token, which is what wakes the junction
        // that was asleep on its inputs.
        InvalidOperationException raised = new("the loop's own stage refused an element");
        int seen = 0;
        RunnableGraph graph = Loop(
            relief: "drop-oldest",
            source: new RecordingEnumerable<int>(1),
            onLoop: value => Interlocked.Increment(ref seen) == 10 ? throw raised : value);

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Same(raised, failure);
    }

    [Fact]
    public async Task CompletionWalksAllTheWayRoundACycleWithoutRecurringForever()
    {
        // The mutual recursion between "this segment has stopped" and "this edge is closed" walks upstream
        // edge by edge, and in a cycle upstream eventually means the segment it started at. It is bounded
        // by two flags rather than by the graph being acyclic, and this is the graph that proves it: the
        // exit leg leaves first, so when the take on the loop ends its stream the walk goes junction,
        // source, feedback edge, broadcast, loop segment — and arrives back at the segment it started from,
        // which has already stopped. A missing guard would not fail an assertion here; it would exhaust the
        // stack, which is why the claim is simply that this run ends at all.
        List<int> observed = [];
        RunnableGraph graph = Loop(
            relief: "drop-oldest",
            source: new RecordingEnumerable<int>(1),
            take: 8,
            exitTake: 2,
            exit: value =>
            {
                lock (observed)
                {
                    observed.Add(value);
                }
            });

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the completion walk goes round the loop and stops");

        lock (observed)
        {
            Assert.Equal(2, observed.Count);
        }
    }

    [Fact]
    public async Task AJunctionCanCloseMoreThanOneCycleAtOnce()
    {
        // Two feedback edges into one merge, which is what a plan that keeps a *place* per feedback input
        // rather than a channel is for: the junction is built when the one input from outside arrives, with
        // two slots reserved, and each loop fills its own when the walk comes round to it. A plan that
        // reserved one slot, or that matched arrivals by order rather than by port, would wire the two
        // loops into the same input and nothing about the run would say so.
        int exited = 0;
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "merge"),
                    Counted("stage-3", "take", 12),
                    Node("stage-4", "broadcast"),
                    Node("stage-5", "for-each"),
                    Buffer("stage-6", 4, "drop-oldest"),
                    Buffer("stage-7", 4, "drop-oldest"),
                ],
                [
                    Into("stage-1", "stage-2", 0),
                    Edge("stage-2", "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-4", 0, "stage-5"),
                    Leg("stage-4", 1, "stage-6"),
                    Into("stage-6", "stage-2", 1),
                    Leg("stage-4", 2, "stage-7"),
                    Into("stage-7", "stage-2", 2),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Merge()),
                ("stage-3", LocalStageDescriptor.Take(12)),
                ("stage-4", LocalStageDescriptor.Broadcast()),
                ("stage-5", Calling(_ => Interlocked.Increment(ref exited))),
                ("stage-6", Buffering(4)),
                ("stage-7", Buffering(4))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the take on the loop ends both cycles");

        Assert.Equal(12, Volatile.Read(ref exited));
    }

    [Fact]
    public async Task TheRelievingBoundaryNeedNotStandBesideTheJunction()
    {
        // The rule is about the loop and not about an edge of it: a dropping buffer anywhere on the cycle
        // relieves it. Here a mapping stage stands between the leg and the buffer, so the buffer is neither
        // the leg's own channel nor the junction's, and the walk has to reach it as an ordinary boundary
        // inside the branch that closes the cycle.
        int exited = 0;
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "merge"),
                    Counted("stage-3", "take", 9),
                    Node("stage-4", "broadcast"),
                    Node("stage-5", "for-each"),
                    Node("stage-6", "select"),
                    Buffer("stage-7", 2, "drop-oldest"),
                ],
                [
                    Into("stage-1", "stage-2", 0),
                    Edge("stage-2", "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-4", 0, "stage-5"),
                    Leg("stage-4", 1, "stage-6"),
                    Edge("stage-6", "stage-7"),
                    Into("stage-7", "stage-2", 1),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Merge()),
                ("stage-3", LocalStageDescriptor.Take(9)),
                ("stage-4", LocalStageDescriptor.Broadcast()),
                ("stage-5", Calling(_ => Interlocked.Increment(ref exited))),
                ("stage-6", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-7", Buffering(2))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the loop relieved by a buffer one stage away runs and ends");

        Assert.Equal(9, Volatile.Read(ref exited));
    }

    [Fact]
    public async Task APartitionCanBeTheExitOfALoop()
    {
        // The iterative-computation shape, with a partition as the thing that decides an element is done.
        // Each lap adds one and the router sends anything that has reached five out of the loop; everything
        // below five goes round again. This is also the clearest statement of what the corrected contract
        // means: the loop does not end when the last element leaves it — it goes quiet — so the exit that
        // ends the run is still the take on the loop, and the router only decides where elements go.
        List<int> exited = [];
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "merge"),
                    Node("stage-3", "select"),
                    Counted("stage-4", "take", 10),
                    Node("stage-5", "partition"),
                    Node("stage-6", "for-each"),
                    Buffer("stage-7", 8, "drop-oldest"),
                ],
                [
                    Into("stage-1", "stage-2", 0),
                    Edge("stage-2", "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-4", "stage-5"),
                    Leg("stage-5", 0, "stage-6"),
                    Leg("stage-5", 1, "stage-7"),
                    Into("stage-7", "stage-2", 1),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 1, 1, 1, 1))),
                ("stage-2", LocalStageDescriptor.Merge()),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value + 1))),
                ("stage-4", LocalStageDescriptor.Take(10)),
                ("stage-5", Routing(value => value >= 5 ? 0 : 1)),
                ("stage-6", Calling(value =>
                {
                    lock (exited)
                    {
                        exited.Add(value);
                    }
                })),
                ("stage-7", Buffering(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the take on the loop ends the run");

        // Five elements each need four laps to reach five, which is twenty passes, so the take of ten is
        // reached whatever order the laps happen in. Everything that left the loop had finished computing.
        lock (exited)
        {
            Assert.All(exited, value => Assert.True(value >= 5, $"an element left the loop at {value}"));
        }
    }

    [Fact]
    public async Task APausedCycleComesToRestAndMovesAgain()
    {
        // The general control-plane statement is checkpoint 5, and this is the same claim every junction
        // checkpoint made in its turn: a cycle is junctions and channels, so the pause gate reaches it
        // through the waits it already owns. Coming to rest is a fact rather than a delay — a run with
        // nothing outside it still producing has only its own loop to quieten — and moving again is a fact
        // too, because the exit keeps counting laps afterwards.
        TaskCompletionSource circulating = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource resumed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int laps = 0;
        int released = int.MaxValue;
        RunnableGraph graph = Loop(
            relief: "drop-oldest",
            source: new RecordingEnumerable<int>(1),
            exit: _ =>
            {
                int lap = Interlocked.Increment(ref laps);

                if (lap == 20)
                {
                    circulating.TrySetResult();
                }

                if (lap > Volatile.Read(ref released) + 10)
                {
                    resumed.TrySetResult();
                }
            });

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(circulating.Task, "the loop is circulating");
        await Reaches(run.PauseAsync(TestToken), "the loop comes to rest");

        Assert.True(run.IsPaused);

        Volatile.Write(ref released, Volatile.Read(ref laps));

        await run.ResumeAsync();
        await Reaches(resumed.Task, "the loop moves again");

        await run.ShutdownAsync();
        await Reaches(run.Completion, "the shutdown ends the loop");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    [Fact]
    public async Task ShuttingDownACycleTwiceEndsItOnce()
    {
        // Cutting a feedback edge is the same walk a downstream completion takes, so it is guarded and
        // idempotent for the same reasons. A second request has nothing left to cut and changes nothing.
        RunnableGraph graph = Loop(relief: "drop-oldest", source: new RecordingEnumerable<int>(1));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await run.ShutdownAsync();
        await run.ShutdownAsync();

        await Reaches(run.Completion, "the run ends once");

        Assert.Equal(TaskStatus.RanToCompletion, run.Completion.Status);
    }

    /// <summary>Materializes a graph that is expected to be refused, and returns the refusal.</summary>
    /// <param name="graph">The graph.</param>
    /// <returns>The exception the host raised.</returns>
    private static async Task<InvalidOperationException> Refused(RunnableGraph graph) =>
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

    /// <summary>Builds the classic feedback graph: a source merged with its own downstream.</summary>
    /// <param name="relief">
    /// The overflow policy of the buffer on the feedback edge, in the payload's own spelling, or
    /// <see langword="null"/> for a loop with no buffer in it at all.
    /// </param>
    /// <param name="capacity">The capacity of that buffer.</param>
    /// <param name="take">How many elements the stage on the loop passes before ending its stream.</param>
    /// <param name="exitTake">How many elements the exit branch passes before ending its own.</param>
    /// <param name="source">The sequence outside the loop, which defaults to one element.</param>
    /// <param name="exit">What the exit sink does with each element it receives.</param>
    /// <param name="onLoop">What the stage on the loop does to each element.</param>
    /// <returns>The graph.</returns>
    /// <remarks>
    /// <para>
    /// The shape is the one ADR 0005 is about and the one an author writes: elements enter through a merge,
    /// go round through a splitting junction, and leave through one of its legs. The feedback leg carries
    /// the buffer whose policy decides whether the loop is legal at all, and it is a parameter here so that
    /// one graph serves the refusals and the executions alike.
    /// </para>
    /// <para>
    /// Both takes are large by default, so a graph built without them circulates until something stops it.
    /// A take on the loop is the author's own exit and the only way a cycle ends of its own accord; a take
    /// on the exit branch is not, and the difference is one of the things this file is about.
    /// </para>
    /// </remarks>
    private static RunnableGraph Loop(
        string? relief,
        int capacity = 4,
        int take = int.MaxValue,
        int exitTake = int.MaxValue,
        RecordingEnumerable<int>? source = null,
        Action<int>? exit = null,
        Func<int, int>? onLoop = null)
    {
        List<StageNode> nodes =
        [
            Node("stage-1", "from-enumerable"),
            Node("stage-2", "merge"),
            Counted("stage-3", "take", take),
            Node("stage-4", "broadcast"),
            Counted("stage-6", "take", exitTake),
            Node("stage-7", "for-each"),
            Node("stage-8", "select"),
        ];
        List<GraphEdge> edges =
        [
            Into("stage-1", "stage-2", 0),
            Edge("stage-2", "stage-8"),
            Edge("stage-8", "stage-3"),
            Edge("stage-3", "stage-4"),
            Leg("stage-4", 0, "stage-6"),
            Edge("stage-6", "stage-7"),
        ];
        List<(string Node, LocalStageDescriptor Stage)> bindings =
        [
            ("stage-1", LocalStageDescriptor.FromEnumerable(source ?? new RecordingEnumerable<int>(1))),
            ("stage-2", LocalStageDescriptor.Merge()),
            ("stage-3", LocalStageDescriptor.Take(take)),
            ("stage-4", LocalStageDescriptor.Broadcast()),
            ("stage-6", LocalStageDescriptor.Take(exitTake)),
            ("stage-7", Calling(exit ?? (_ => { }))),
            ("stage-8", LocalStageDescriptor.Select(onLoop ?? (value => value))),
        ];

        if (relief is null)
        {
            edges.Add(Rejoins("stage-4", 1, "stage-2", 1));
        }
        else
        {
            nodes.Add(Buffer("stage-5", capacity, relief));
            edges.Add(Leg("stage-4", 1, "stage-5"));
            edges.Add(Into("stage-5", "stage-2", 1));
            bindings.Add(("stage-5", Buffering(capacity)));
        }

        return Graph(Declaring(nodes, edges, []), Bindings([.. bindings]));
    }
}
