using Orleans.Dataflow.Authoring;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The routed row of ADR 0005's fan-out table: what a partition sends where, how often it asks, what it
/// holds while it waits, and what it does with an element it cannot deliver.
/// </summary>
/// <remarks>
/// <para>
/// A partition is the one junction whose target is a function of the element, so it is the one that reads
/// before it waits. Everything that follows from that inversion is observable from outside: the routing
/// function is called once per element and never for an element the junction did not take; the element it
/// routed is held while its own leg is full, and every other leg starves for exactly as long — which is
/// the same "how far does a held source get" measurement the rest of this suite makes; and an element
/// with no destination, whether because the answer names no leg or because that leg's stream has ended,
/// fails the run rather than disappearing.
/// </para>
/// <para>
/// No test here waits on a clock. A gate holds a sink at a known element and a pull barrier holds the
/// source until something else has happened, so every claim about what a run had reached at a moment is a
/// fact rather than a hope.
/// </para>
/// </remarks>
public sealed class PartitionTests
{
    [Fact]
    public async Task PartitionSendsEachElementToTheOutputItsFunctionNames()
    {
        // The canonical split by key, and the only junction that promises which leg an element lands on.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "partition"),
                    Collect("stage-3", 8),
                    Collect("stage-4", 8),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("even", "stage-3"), Slot("odd", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6))),
                ("stage-2", Routing(value => value % 2)),
                ("stage-3", Collecting(8)),
                ("stage-4", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both legs have");

        int[] even = await run.GetValueAsync(Result<int[]>(graph, "even"), TestToken);
        int[] odd = await run.GetValueAsync(Result<int[]>(graph, "odd"), TestToken);

        Assert.Equal([2, 4, 6], even);
        Assert.Equal([1, 3, 5], odd);
    }

    [Fact]
    public async Task PartitionRoutesToEveryOneOfEightLegs()
    {
        // The declared ceiling, wired to the last leg. A routing function answers the zero-based position
        // of a port in the junction's own port order, so this is also the statement that the order the
        // vocabulary declares its ports in is the order the function is answering about.
        List<int>[] observed = [.. Enumerable.Range(0, LocalVocabulary.MaxFanOut).Select(_ => new List<int>())];
        List<Orleans.Dataflow.Definition.StageNode> nodes =
            [Node("stage-1", "from-enumerable"), Node("stage-2", "partition")];
        List<Orleans.Dataflow.Definition.GraphEdge> edges = [Edge("stage-1", "stage-2")];
        List<(string Node, LocalStageDescriptor Stage)> bindings =
        [
            ("stage-1", LocalStageDescriptor.FromEnumerable(
                new RecordingEnumerable<int>([.. Enumerable.Range(0, LocalVocabulary.MaxFanOut)]))),
            ("stage-2", Routing(value => value)),
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

        for (int leg = 0; leg < LocalVocabulary.MaxFanOut; leg++)
        {
            Assert.Equal([leg], observed[leg]);
        }
    }

    [Fact]
    public async Task PartitionCallsTheRoutingFunctionOncePerElement()
    {
        // The keyed adapter's read-once rule in its second place. A junction that consulted the function
        // again — to re-check a leg, to retry after a wait, to look once more after a pause — would be
        // requiring a purity of the author that nothing here can check, so the count is the contract.
        List<int> asked = [];
        RecordingEnumerable<int> elements = new(1, 2, 3, 4, 5);
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "partition"),
                    Collect("stage-3", 8),
                    Collect("stage-4", 8),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("low", "stage-3"), Slot("high", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", Routing(value =>
                {
                    lock (asked)
                    {
                        asked.Add(value);
                    }

                    return value > 3 ? 1 : 0;
                })),
                ("stage-3", Collecting(8)),
                ("stage-4", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when both legs have");

        Assert.Equal([1, 2, 3, 4, 5], asked);
        Assert.Equal(5, elements.Pulls);
        int[] low = await run.GetValueAsync(Result<int[]>(graph, "low"), TestToken);
        int[] high = await run.GetValueAsync(Result<int[]>(graph, "high"), TestToken);

        Assert.Equal([1, 2, 3], low);
        Assert.Equal([4, 5], high);
    }

    [Fact]
    public async Task PartitionHoldsTheRoutedElementAndStarvesEveryOtherLeg()
    {
        // Head-of-line blocking one element deep, which is what the table promises and what separates a
        // partition from a balance. The first leg is held at its first element, so the run absorbs exactly
        // five: one in the held sink's hand, one in that leg's channel, one the junction routed there and
        // is holding, one in the junction's own input channel, and one in the source's hand. Every element
        // behind those is for the *other* leg and could be delivered at once — and is not, because the
        // junction is holding one element for a leg that has no room.
        Gate gate = new();
        TaskCompletionSource saturated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> second = [];
        RecordingEnumerable<int> elements = new(0, 0, 0, 1, 1, 1, 1)
        {
            Pulled = pulls =>
            {
                if (pulls == 5)
                {
                    saturated.TrySetResult();
                }
            },
        };

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "partition"),
                    Node("stage-3", "for-each"),
                    Node("stage-4", "for-each"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", Routing(value => value)),
                ("stage-3", Calling(_ => gate.Wait())),
                ("stage-4", Calling(value =>
                {
                    lock (second)
                    {
                        second.Add(value);
                    }
                }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source reaches the bound the held leg allows");
        await Reaches(gate.Reached, "the first leg's sink is holding an element");

        // The elements destined for the second leg are already in the run and the second leg is idle. That
        // is the starvation, stated as a fact rather than as a delay nobody waited out: no further element
        // can be pulled until the held one is placed, so the count cannot grow while the gate is shut.
        Assert.Equal(5, elements.Pulls);

        lock (second)
        {
            Assert.Empty(second);
        }

        gate.Open();

        await Reaches(run.Completion, "the run completes once the held leg is released");

        Assert.Equal(7, elements.Pulls);

        lock (second)
        {
            Assert.Equal([1, 1, 1, 1], second);
        }
    }

    [Fact]
    public async Task PartitionFailsTheRunWhenTheRoutingFunctionNamesNoWiredOutput()
    {
        // ADR 0005's own decision, and the sentence has to carry both numbers: how many legs a junction
        // has is stated by its edges rather than by anything the function can see, so an answer of three
        // against two wired legs is indistinguishable from an off-by-one unless the arity is said out loud.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "partition"),
                    Node("stage-3", "ignore"),
                    Node("stage-4", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", Routing(_ => 3)),
                ("stage-3", LocalStageDescriptor.Ignore()),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Contains("answered 3", failure.Message, StringComparison.Ordinal);
        Assert.Contains("wired to 2 outputs", failure.Message, StringComparison.Ordinal);
        Assert.Contains("only 0 to 1", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PartitionFailsTheRunWhenTheRoutingFunctionAnswersBelowZero()
    {
        // The other end of the same range, tested separately because a negative index is the answer a
        // "not found" convention produces and is therefore the one an author reaches by accident.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "partition"),
                    Node("stage-3", "ignore"),
                    Node("stage-4", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", Routing(_ => -1)),
                ("stage-3", LocalStageDescriptor.Ignore()),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        InvalidOperationException failure =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await run.Completion);

        Assert.Contains("answered -1", failure.Message, StringComparison.Ordinal);
        Assert.Contains("wired to 2 outputs", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PartitionAbandonsAnElementRoutedToALegThatHasLeft()
    {
        // The case ADR 0005 does not decide, decided here and stated in the capability matrix — and decided
        // the other way from the obvious guess, for a reason found by running it. A leg that has left is a
        // stream that *ended*, and everywhere else in this engine an element arriving at a channel a
        // downstream completion closed is abandoned rather than dropped, counted, or failed on. Failing
        // instead was tried, and it made the outcome of an ordinary run a race: the completion walk closes
        // legs while elements are still travelling towards them, so the same graph ended successfully or in
        // failure depending on which arrived first.
        //
        // Here the first leg takes one element and leaves, and everything after it is routed to that
        // departed leg. The run ends cleanly with the element the sink did take, and nothing is counted as
        // a drop: nothing discarded those elements, the stream they were travelling to had ended.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "partition"),
                    Node("stage-3", "first"),
                    Node("stage-4", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("head", "stage-3")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(
                    new RecordingEnumerable<int>([.. Enumerable.Repeat(0, 64)]))),
                ("stage-2", Routing(value => value)),
                ("stage-3", LocalStageDescriptor.First()),
                ("stage-4", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run ends cleanly although a leg left under the routing function");

        Assert.Equal(0, await run.GetValueAsync(Result<int>(graph, "head"), TestToken));
        Assert.Equal(0, run.DroppedElements);
    }

    [Fact]
    public async Task PartitionCompletesUpstreamWhenTheLastLegLeaves()
    {
        // The other half of ADR 0005's third rule. Each leg takes one element and leaves; the elements
        // routed at a leg that has gone are abandoned; and when the last leg leaves, the junction has
        // nowhere to deliver at all and completes upstream, which releases an endless source rather than
        // leaving it pulling forever.
        RecordingEnumerable<int> elements = new([.. Enumerable.Range(0, 4096).Select(value => value % 2)]);
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "partition"),
                    Node("stage-3", "first"),
                    Node("stage-4", "first"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("even", "stage-3"), Slot("odd", "stage-4")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", Routing(value => value)),
                ("stage-3", LocalStageDescriptor.First()),
                ("stage-4", LocalStageDescriptor.First())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the last leg has left");
        await Reaches(elements.Released, "the source's enumerator is released");

        int even = await run.GetValueAsync(Result<int>(graph, "even"), TestToken);
        int odd = await run.GetValueAsync(Result<int>(graph, "odd"), TestToken);

        Assert.Equal(0, even);
        Assert.Equal(1, odd);
        Assert.True(elements.Pulls < 4096, $"the source was released after {elements.Pulls} of 4096");
    }

    [Fact]
    public async Task APausedRoutedRunComesToRestAndMovesAgain()
    {
        // The control plane across branching topologies is a later checkpoint and this is not it. What is
        // claimed here is what every junction checkpoint has claimed in its turn: the routed pump parks
        // where every other segment parks, so a pause of a graph containing one reaches quiescence rather
        // than waiting forever on a pump that never looked at the gate, and resuming it delivers the rest.
        //
        // The state a partition alone has — an element already routed, waiting for its own leg's room —
        // is deliberately *not* produced here, and the reason is worth recording. Filling a leg means its
        // consumer is stuck, and the only ways to keep a consumer stuck are an author's callback and a
        // probe's rendezvous. A callback blocks quiescence by design, so a test built that way would be
        // asserting a pause that the pause contract says cannot happen; the probe is checkpoint 5's, and
        // so is this case.
        //
        // The gate in the routing function is what makes "paused while running" a fact rather than a race:
        // sixteen elements through two collects can finish before a pause lands, and a run that has ended
        // reports IsPaused false by contract. Held inside the router, the run provably has an element in
        // the routed pump's own hands when the pause is requested, so the pause parks a moving run — and
        // the pending pause is asserted as pending for exactly that reason.
        Gate gate = new();
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "partition"),
                    Collect("stage-3", 32),
                    Collect("stage-4", 32),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                [Slot("even", "stage-3"), Slot("odd", "stage-4")]),
            Bindings(
                (
                    "stage-1",
                    LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>([.. Enumerable.Range(1, 16)]))),
                ("stage-2", Routing(value =>
                {
                    gate.Wait();

                    return value % 2;
                })),
                ("stage-3", Collecting(32)),
                ("stage-4", Collecting(32))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await gate.Reached;

        Task paused = run.PauseAsync(TestToken);

        // The routed pump is inside the author's routing function with an element in its hands, so a pause
        // reporting quiescence now would be reporting something untrue.
        Assert.False(paused.IsCompleted);

        gate.Open();
        await Reaches(paused, "the pause takes effect on a routed run");

        Assert.True(run.IsPaused);

        await run.ResumeAsync();
        await Reaches(run.Completion, "the resumed run finishes");

        int[] even = await run.GetValueAsync(Result<int[]>(graph, "even"), TestToken);
        int[] odd = await run.GetValueAsync(Result<int[]>(graph, "odd"), TestToken);

        Assert.Equal([2, 4, 6, 8, 10, 12, 14, 16], even);
        Assert.Equal([1, 3, 5, 7, 9, 11, 13, 15], odd);
    }

    [Fact]
    public async Task PartitionLegsRejoinThroughAMerge()
    {
        // Split by key and put the halves back together, which is the shape a partition exists inside of.
        // A merge is what a rejoin has to be here: the two legs run at their own speeds and a junction
        // that waited for one of them in a fixed order would be waiting behind a leg the split cannot
        // fill, which is the head-of-line hazard checkpoint 2 documented. What the merge promises is the
        // multiset, so that is what this asserts.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "partition"),
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
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3, 4, 5, 6))),
                ("stage-2", Routing(value => value % 2)),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value * 10))),
                ("stage-4", LocalStageDescriptor.Select((Func<int, int>)(value => value * 100))),
                ("stage-5", LocalStageDescriptor.Merge()),
                ("stage-6", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes when the merge's inputs both have");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([20, 40, 60, 100, 300, 500], [.. joined.Order()]);
    }

    [Fact]
    public async Task PartitionAppliesTheOverflowPolicyOfTheLegItRoutesTo()
    {
        // The consequence of routing to one leg specifically: a partition offers to the leg its function
        // named, so that leg's declared policy is reached exactly as a broadcast's is — unlike a balance,
        // which picks a leg that really has room and therefore never applies one. The leg is held at its
        // first element and declares drop-newest, so the junction is never paced at all: the source runs
        // to its end while the leg keeps two, and the discarded elements are counted rather than lost in
        // silence. The source's exhaustion is the moment the count is read, so the number is a fact.
        Gate gate = new();
        TaskCompletionSource exhausted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingEnumerable<int> elements = new(0, 0, 0, 0, 0, 0, 0, 0);

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
                    Node("stage-2", "partition"),
                    Buffer("stage-3", 2, "drop-newest"),
                    Node("stage-4", "for-each"),
                    Node("stage-5", "ignore"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-2", 1, "stage-5"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", Routing(value => value)),
                ("stage-3", Buffering(2)),
                ("stage-4", Calling(_ => gate.Wait())),
                ("stage-5", LocalStageDescriptor.Ignore())));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(gate.Reached, "the routed leg's sink is holding an element");
        await Reaches(exhausted.Task, "the source runs to its end without the junction ever being paced");

        // The claim is that the routed leg's policy was reached at all while the source was never paced —
        // a balance could never report this, because it picks a leg that really has room and therefore
        // never reaches a policy. The floor counts every place an element can rest instead of being
        // dropped: one in the held sink's hand, two in the leg's declared buffer, one in the boundary
        // between the source and the junction, and the one the junction itself may hold — five at most,
        // so at least three of the eight were answered by the policy. The first version claimed five and
        // forgot the last two resting places; CI's scheduling found the difference on the first day.
        Assert.Equal(8, elements.Pulls);
        Assert.True(run.DroppedElements >= 3, $"dropped {run.DroppedElements} of eight");

        gate.Open();

        await Reaches(run.Completion, "the run completes once the held sink is released");
    }
}
