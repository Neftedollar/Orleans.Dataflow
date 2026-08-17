using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// ADR 0005's fan-out table, one row at a time and as behavior rather than as a claim about the plan.
/// </summary>
/// <remarks>
/// <para>
/// Each junction promises four things — what it routes, when it pulls, what it holds, and what a completed
/// leg does to it — and every one of them is observable from outside the engine. What a junction routes is
/// what the sinks collect. When it pulls is how far a held source gets: the number of elements a run can
/// absorb while a consumer is parked is a fact about the pull rule and about nothing else, and it is the
/// same bounded-memory reasoning the buffer suite uses. What it holds is the same number seen from the
/// other side, because "room first, pull second" is exactly one element cheaper than "pull first, wait
/// second". And what a completed leg does is whether the other legs keep receiving, and whether an
/// unbounded source is ever released.
/// </para>
/// <para>
/// No test here waits on a clock to make a claim. A gate holds a run at a known point, a pull barrier
/// holds a source at a known element, and the deadline in <see cref="JunctionFixtures.Reaches"/> exists so
/// that a broken completion rule is reported rather than hung on.
/// </para>
/// </remarks>
public sealed class FanOutTests
{
    [Fact]
    public async Task BroadcastDeliversEveryElementToEveryOutput()
    {
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Collect("stage-3", 8),
                    Collect("stage-4", 8),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("left", "stage-3"), Slot("right", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Collecting(8)),
                ("stage-4", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both sinks have");

        int[] left = await run.GetValueAsync(Result<int[]>(graph, "left"), TestToken);
        int[] right = await run.GetValueAsync(Result<int[]>(graph, "right"), TestToken);

        Assert.Equal([1, 2, 3], left);
        Assert.Equal([1, 2, 3], right);
    }

    [Fact]
    public async Task BroadcastDeliversToEveryOneOfEightLegs()
    {
        // The declared ceiling, wired to the last leg. The legs past the second are ignorable ports, so a
        // graph states how many it has by wiring them and this is the statement that they all carry
        // elements rather than only the two the compiler insists on.
        List<int>[] observed = [.. Enumerable.Range(0, LocalVocabulary.MaxFanOut).Select(_ => new List<int>())];
        List<Orleans.Dataflow.Definition.StageNode> nodes =
            [Node("stage-1", "from-enumerable"), Node("stage-2", "broadcast")];
        List<Orleans.Dataflow.Definition.GraphEdge> edges = [Edge("stage-1", "stage-2")];
        List<(string Node, LocalStageDescriptor Stage)> bindings =
        [
            ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2))),
            ("stage-2", LocalStageDescriptor.Broadcast()),
        ];

        for (int leg = 0; leg < LocalVocabulary.MaxFanOut; leg++)
        {
            string sink = $"sink-{leg}";
            List<int> into = observed[leg];

            nodes.Add(Node(sink, "for-each"));
            edges.Add(Leg("stage-2", leg, sink));
            bindings.Add((sink, Calling(value =>
            {
                lock (into)
                {
                    into.Add(value);
                }
            })));
        }

        RunnableGraph graph = Graph(Declaring(nodes, edges, []), Bindings([.. bindings]));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when all eight legs have");

        Assert.All(observed, leg => Assert.Equal([1, 2], leg));
    }

    [Fact]
    public async Task BroadcastPullsOnlyWhenEveryLiveOutputHasRoom()
    {
        // The slowest-consumer rule, proved from the source end. One leg is parked on its first element
        // and the other consumes as fast as it is given anything; if the junction paced itself by the fast
        // leg, the source would run to its end. It gets exactly four elements in: one parked in the slow
        // sink, one in that leg's channel, one in the junction's input channel, and one in the source's
        // own hand at a full channel.
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
            if (position == 9)
            {
                exhausted.TrySetResult();
            }

            return null;
        };

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
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Calling(_ => gate.Wait())),
                ("stage-4", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source reaches the bound the slow leg allows");

        Assert.False(exhausted.Task.IsCompleted);
        Assert.Equal(4, elements.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the slow leg is released");

        int[] seen = await run.GetValueAsync(Result<int[]>(graph, "seen"), TestToken);

        Assert.Equal(9, elements.Pulls);
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9], seen);
    }

    [Fact]
    public async Task BroadcastHoldsAtMostOneElementOutsideItsChannels()
    {
        // Both legs parked on their first element, so nothing moves anywhere and every element the run
        // absorbed is somewhere countable: one in each parked sink, one in each leg's channel, one in the
        // input channel, and one in the source's hand — and, crucially, none in the junction, because it
        // asks for room before it pulls. A junction that pulled first and waited afterwards would hold one
        // more, and the source would have got one further.
        Gate left = new();
        Gate right = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
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
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Calling(_ => left.Wait())),
                ("stage-4", Calling(_ => right.Wait()))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(left, right);

        await Reaches(left.Reached, "the left leg reaches its first element");
        await Reaches(right.Reached, "the right leg reaches its first element");
        await Reaches(saturated.Task, "the source reaches the bound two parked legs allow");

        Assert.Equal(4, elements.Pulls);

        left.Open();
        right.Open();

        await Reaches(run.Completion, "the run completes once both legs are released");
    }

    [Fact]
    public async Task ACompletedLegLeavesTheDeliverySetAndTheOthersKeepReceiving()
    {
        // A take of one element ends its leg after the first element. Rule 3 of ADR 0005 is that this
        // stops that leg feeding and nothing else: the other leg receives the whole sequence.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Counted("stage-3", "take", 1),
                    Collect("stage-4", 8),
                    Collect("stage-5", 8),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-2", 1, "stage-5"),
                ],
                [Slot("short", "stage-4"), Slot("whole", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Take(1)),
                ("stage-4", Collecting(8)),
                ("stage-5", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes although one leg ended early");

        int[] shortened = await run.GetValueAsync(Result<int[]>(graph, "short"), TestToken);
        int[] whole = await run.GetValueAsync(Result<int[]>(graph, "whole"), TestToken);

        Assert.Equal([1], shortened);
        Assert.Equal([1, 2, 3, 4, 5], whole);
    }

    [Fact]
    public async Task TheJunctionCompletesUpstreamWhenTheLastLegLeaves()
    {
        // The source is endless, so nothing but the junction completing upstream can end this run. Both
        // legs take one element and leave; when the second of them does, the junction has nowhere to
        // deliver and ends its own input, which releases the source and its enumerator.
        RecordingEnumerable<int> elements = new(7);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "cycle"),
                    Node("stage-2", "broadcast"),
                    Counted("stage-3", "take", 1),
                    Collect("stage-4", 4),
                    Counted("stage-5", "take", 1),
                    Collect("stage-6", 4),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-2", 1, "stage-5"),
                    Edge("stage-5", "stage-6"),
                ],
                [Slot("left", "stage-4"), Slot("right", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.Cycle(elements)),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Take(1)),
                ("stage-4", Collecting(4)),
                ("stage-5", LocalStageDescriptor.Take(1)),
                ("stage-6", Collecting(4))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the endless source is released when the last leg leaves");

        int[] left = await run.GetValueAsync(Result<int[]>(graph, "left"), TestToken);
        int[] right = await run.GetValueAsync(Result<int[]>(graph, "right"), TestToken);

        Assert.Equal([7], left);
        Assert.Equal([7], right);
        Assert.True(elements.Releases >= 1, "the endless sequence's enumerator is released");
    }

    [Fact]
    public async Task BalanceDeliversEachElementToExactlyOneOutput()
    {
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "balance"),
                    Collect("stage-3", 16),
                    Collect("stage-4", 16),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("left", "stage-3"), Slot("right", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6, 7, 8))),
                ("stage-2", LocalStageDescriptor.Balance()),
                ("stage-3", Collecting(16)),
                ("stage-4", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both sinks have");

        int[] left = await run.GetValueAsync(Result<int[]>(graph, "left"), TestToken);
        int[] right = await run.GetValueAsync(Result<int[]>(graph, "right"), TestToken);

        // No promise is ever made about which output receives an element — that is what partition is for —
        // so what is asserted is the promise that is made: every element arrives exactly once, in order
        // within its own leg, and neither leg is starved while both are willing.
        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8], left.Concat(right).Order());
        Assert.Equal(left.Order(), left);
        Assert.Equal(right.Order(), right);
        Assert.NotEmpty(left);
        Assert.NotEmpty(right);
    }

    [Fact]
    public async Task BalanceKeepsFeedingTheOutputsThatHaveRoomWhenOneHasNone()
    {
        // Head-of-line blocking is exactly what a balance exists to avoid, so a leg that stops consuming
        // must not stop the stream. The parked leg can absorb two elements and no more — one in its
        // callback and one in its channel — and everything else has to reach the other leg while the first
        // is still parked, which is what the four-element wait proves.
        Gate gate = new();
        TaskCompletionSource fed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> parked = [];
        List<int> willing = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "balance"),
                    Node("stage-3", "for-each"),
                    Node("stage-4", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6))),
                ("stage-2", LocalStageDescriptor.Balance()),
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
                        lock (willing)
                        {
                            willing.Add(value);

                            if (willing.Count == 4)
                            {
                                fed.TrySetResult();
                            }
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(fed.Task, "the willing leg keeps receiving while the other has no room");

        lock (parked)
        {
            Assert.Single(parked);
        }

        gate.Open();

        await Reaches(run.Completion, "the run completes once the parked leg is released");

        lock (parked)
        {
            lock (willing)
            {
                Assert.Equal([1, 2, 3, 4, 5, 6], parked.Concat(willing).Order());
                Assert.True(parked.Count <= 2, "the parked leg absorbed no more than its callback and its channel");
            }
        }
    }

    [Fact]
    public async Task BalanceHoldsAtMostOneElementOutsideItsChannels()
    {
        // The same count as the broadcast bound and two elements larger, because each of the six elements
        // the run absorbed went to exactly one leg rather than to both: one in each parked sink, one in
        // each leg's channel, one in the input channel, one in the source's hand — and none in the
        // junction, which is the whole of "holds at most one" seen from where it can be counted.
        Gate left = new();
        Gate right = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5, 6, 7, 8, 9)
        {
            Pulled = pulls =>
            {
                if (pulls == 6)
                {
                    saturated.TrySetResult();
                }
            },
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "balance"),
                    Node("stage-3", "for-each"),
                    Node("stage-4", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Balance()),
                ("stage-3", Calling(_ => left.Wait())),
                ("stage-4", Calling(_ => right.Wait()))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(left, right);

        await Reaches(left.Reached, "the left leg reaches its first element");
        await Reaches(right.Reached, "the right leg reaches its first element");
        await Reaches(saturated.Task, "the source reaches the bound two parked legs allow");

        Assert.Equal(6, elements.Pulls);

        left.Open();
        right.Open();

        await Reaches(run.Completion, "the run completes once both legs are released");
    }

    [Fact]
    public async Task UnzipDeliversEachHalfToItsOwnOutputAndTheHalvesRezipWithoutSkew()
    {
        // Zip does not exist yet, so the re-joining is done here: the two legs are paired by position and
        // the pairs have to be the rows the source produced. A junction that let one leg run ahead would
        // still produce these two lists — what it could not produce is the pairing, because a row whose
        // halves arrived a row apart cannot be recovered from either list alone.
        (int Left, int Right)[] rows = [(1, 10), (2, 20), (3, 30), (4, 40)];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "unzip"),
                    Collect("stage-3", 8),
                    Collect("stage-4", 8),
                ],
                [Edge("stage-1", "stage-2"), Half("stage-2", "left", "stage-3"), Half("stage-2", "right", "stage-4")],
                [Slot("left", "stage-3"), Slot("right", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<(int Left, int Right)>(rows))),
                (
                    "stage-2",
                    LocalStageDescriptor.Unzip(
                        (Func<(int Left, int Right), int>)(row => row.Left),
                        (Func<(int Left, int Right), int>)(row => row.Right))),
                ("stage-3", Collecting(8)),
                ("stage-4", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both halves have arrived");

        int[] left = await run.GetValueAsync(Result<int[]>(graph, "left"), TestToken);
        int[] right = await run.GetValueAsync(Result<int[]>(graph, "right"), TestToken);

        Assert.Equal([1, 2, 3, 4], left);
        Assert.Equal([10, 20, 30, 40], right);
        Assert.Equal(rows, left.Zip(right).Select(pair => (pair.First, pair.Second)));
    }

    [Fact]
    public async Task UnzipAdvancesBothLegsInLockstep()
    {
        // The lockstep is the same slowest-consumer rule the broadcast keeps, and it is what makes the
        // pairing above a contract rather than an accident of two fast sinks: with the left leg parked on
        // its first row, the right leg cannot receive a third.
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> right = [];
        RecordingEnumerable<(int Left, int Right)> rows =
            new((1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60))
            {
                Pulled = pulls =>
                {
                    if (pulls == 4)
                    {
                        saturated.TrySetResult();
                    }
                },
            };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "unzip"),
                    Node("stage-3", "for-each"),
                    Node("stage-4", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Half("stage-2", "left", "stage-3"), Half("stage-2", "right", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(rows)),
                (
                    "stage-2",
                    LocalStageDescriptor.Unzip(
                        (Func<(int Left, int Right), int>)(row => row.Left),
                        (Func<(int Left, int Right), int>)(row => row.Right))),
                ("stage-3", Calling(_ => gate.Wait())),
                (
                    "stage-4",
                    Calling(value =>
                    {
                        lock (right)
                        {
                            right.Add(value);
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source reaches the bound the parked half allows");

        Assert.Equal(4, rows.Pulls);

        lock (right)
        {
            Assert.True(right.Count <= 2, $"the free half received {right.Count} rows while the other was parked");
        }

        gate.Open();

        await Reaches(run.Completion, "the run completes once the parked half is released");

        lock (right)
        {
            Assert.Equal([10, 20, 30, 40, 50, 60], right);
        }
    }

    [Fact]
    public async Task ALegThatDropsIsNotAllowedToPaceTheOthers()
    {
        // Slowest-consumer backpressure is what a leg that waits buys; a leg the author declared as
        // dropping asked for the opposite, and a broadcast that waited for room at one would make the
        // policy unreachable. The offer applies each leg's own policy, so the dropping leg loses elements
        // and counts them, and the leg beside it receives all eight although the dropping leg never moved
        // past its first.
        Gate gate = new();
        TaskCompletionSource delivered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> seen = [];

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Buffer("stage-3", 1, "drop-newest"),
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
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6, 7, 8))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Buffering(1)),
                ("stage-4", Calling(_ => gate.Wait())),
                (
                    "stage-5",
                    Calling(value =>
                    {
                        lock (seen)
                        {
                            seen.Add(value);

                            if (seen.Count == 8)
                            {
                                delivered.TrySetResult();
                            }
                        }
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "the dropping leg reaches its first element and stays there");
        await Reaches(delivered.Task, "the other leg receives everything although the dropping leg never moved");

        // The junction offers to every live leg before it takes the next element, so the eighth element
        // had been offered to the dropping leg before this one saw it. That leg kept two — one in the
        // callback it is parked in and one in the buffer the author declared — and the other six had
        // nowhere to go and were dropped rather than waited for, which is the whole difference a policy
        // makes and is exactly what the run counts.
        Assert.Equal(6L, run.DroppedElements);

        gate.Open();

        await Reaches(run.Completion, "the run completes without the dropping leg pacing the other");

        lock (seen)
        {
            Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8], seen);
        }

        Assert.Equal(6L, run.DroppedElements);
    }

    [Fact]
    public async Task ALegThatBeginsAtAnAsynchronousStageReadsTheLegsOwnChannel()
    {
        // The leg is already a channel, so the asynchronous stage at the head of the branch reads it
        // rather than a second one behind a relay holding nothing. The count is the proof: it is the same
        // four a synchronous leg allows, and a relay in between would have let a fifth element in.
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
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

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "select-async", "local-parallelism-parameters", """{"maxConcurrency":1}"""),
                    Node("stage-4", "ignore"),
                    Collect("stage-5", 16),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-2", 1, "stage-5"),
                ],
                [Slot("seen", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                (
                    "stage-3",
                    LocalStageDescriptor.SelectAsync(
                        new ParallelismOptions { MaxConcurrency = 1 },
                        (Func<int, CancellationToken, Task<int>>)(async (value, token) =>
                        {
                            await release.Task.WaitAsync(token);

                            return value;
                        }))),
                ("stage-4", LocalStageDescriptor.Ignore()),
                ("stage-5", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(saturated.Task, "the source reaches the bound an asynchronous leg allows");

        Assert.Equal(4, elements.Pulls);

        release.SetResult();

        await Reaches(run.Completion, "the run completes once the asynchronous leg is released");

        int[] seen = await run.GetValueAsync(Result<int[]>(graph, "seen"), TestToken);

        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9], seen);
    }

    [Fact]
    public async Task AJunctionOnALegOfAJunctionIsJustAnotherBranch()
    {
        // Nothing about a junction says what may stand on its legs, so one of them is another junction.
        // The second reads the first's leg directly, because that leg is already a channel.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "broadcast"),
                    Collect("stage-4", 8),
                    Collect("stage-5", 8),
                    Collect("stage-6", 8),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Leg("stage-3", 0, "stage-4"),
                    Leg("stage-3", 1, "stage-5"),
                    Leg("stage-2", 1, "stage-6"),
                ],
                [Slot("one", "stage-4"), Slot("two", "stage-5"), Slot("three", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Broadcast()),
                ("stage-4", Collecting(8)),
                ("stage-5", Collecting(8)),
                ("stage-6", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when all three sinks have");

        int[] one = await run.GetValueAsync(Result<int[]>(graph, "one"), TestToken);
        int[] two = await run.GetValueAsync(Result<int[]>(graph, "two"), TestToken);
        int[] three = await run.GetValueAsync(Result<int[]>(graph, "three"), TestToken);

        Assert.Equal([1, 2, 3], one);
        Assert.Equal([1, 2, 3], two);
        Assert.Equal([1, 2, 3], three);
    }

    [Fact]
    public async Task APausedBranchingRunComesToRestAndMovesAgain()
    {
        // The control plane across branching topologies is a later checkpoint and this is not it. What is
        // claimed here is only that the junction pump parks where every other segment parks: a pause of a
        // fan-out graph reaches quiescence rather than waiting forever on a pump that never looked at the
        // gate, and resuming it delivers the rest.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Collect("stage-3", 32),
                    Collect("stage-4", 32),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("left", "stage-3"), Slot("right", "stage-4")]),
            Bindings(
                (
                    "stage-1",
                    LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>([.. Enumerable.Range(1, 16)]))),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Collecting(32)),
                ("stage-4", Collecting(32))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.PauseAsync(TestToken), "the pause takes effect on a branching run");

        Assert.True(run.IsPaused);

        await Reaches(run.ResumeAsync(), "the run moves again");
        await Reaches(run.Completion, "the resumed run reaches its end");

        int[] left = await run.GetValueAsync(Result<int[]>(graph, "left"), TestToken);
        int[] right = await run.GetValueAsync(Result<int[]>(graph, "right"), TestToken);

        Assert.Equal([.. Enumerable.Range(1, 16)], left);
        Assert.Equal([.. Enumerable.Range(1, 16)], right);
    }

    [Fact]
    public async Task ABufferOnALegIsThatLegsOwnChannelRatherThanASecondOne()
    {
        // The rule a buffer in front of an asynchronous stage already follows, applied to a leg: the
        // author asked for four elements of prefetch on the slow leg, and the run holds four and not five.
        // One parked callback, four in the buffer the author wrote, one in the junction's input channel,
        // one in the source's hand — and one more in the fast leg's channel is invisible here because the
        // fast leg consumes at once.
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
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

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Buffer("stage-3", 4),
                    Node("stage-4", "for-each"),
                    Collect("stage-5", 16),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-2", 1, "stage-5"),
                ],
                [Slot("seen", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", Buffering(4)),
                ("stage-4", Calling(_ => gate.Wait())),
                ("stage-5", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source fills the buffer the author wrote and no more");

        Assert.Equal(7, elements.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the buffered leg is released");

        int[] seen = await run.GetValueAsync(Result<int[]>(graph, "seen"), TestToken);

        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], seen);
    }
}
