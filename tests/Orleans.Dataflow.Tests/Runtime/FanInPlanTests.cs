using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What a document with several sources compiles to, and what a document that only looks like one is
/// answered with.
/// </summary>
/// <remarks>
/// <para>
/// The rule this checkpoint replaced was "exactly one node begins a chain". Several sources are legal now,
/// and exactly one thing makes them one graph rather than several: everything in the document is joined to
/// everything else, which for a graph whose sources converge through a junction is true and for two chains
/// written side by side is not. The refusals below are that rule and the two it never covered — a node fed
/// by more than one stream that is not a junction, and a cycle, which is refused as nodes no walk from a
/// source ever reaches.
/// </para>
/// <para>
/// The cardinality of a joining junction needed no new rule either, exactly as its splitting mirror did
/// not: an input port address carries at most one edge, and the graph compiler requires an edge at every
/// port that is not optional. What was missing was a stage whose port list has more than one input in it,
/// which is why half of these tests are about validation rather than about the runtime.
/// </para>
/// </remarks>
public sealed class FanInPlanTests
{
    [Fact]
    public void AJoiningJunctionThatConnectsOneInputDoesNotValidate()
    {
        // The cardinality rule is the graph compiler's existing one: a junction's first two inputs are
        // ports that are not optional, so leaving one unwired is an unconnected input.
        GraphDocument document = Declaring(
            [Node("stage-1", "from-enumerable"), Node("stage-2", "merge"), Node("stage-3", "ignore")],
            [Into("stage-1", "stage-2", 0), Edge("stage-2", "stage-3")],
            []);

        GraphValidationReport report = GraphCompiler.Validate(document, LocalStageCatalog.Instance);

        Assert.False(report.IsValid);
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.Rule == "unconnected-input-port" && diagnostic.Subject == "stage-2#in-1");
    }

    [Fact]
    public void AJoiningJunctionThatLeavesItsInputsPastTheSecondUnwiredValidates()
    {
        // The other half of the same rule, and the reason a junction carries no arity payload: the edges
        // are what say how many streams this occurrence joins, so the ports it did not wire are not a
        // violation.
        GraphDocument document = Declaring(
            [
                Node("stage-1", "from-enumerable"),
                Node("stage-2", "from-enumerable"),
                Node("stage-3", "concat"),
                Node("stage-4", "ignore"),
            ],
            [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
            []);

        Assert.True(GraphCompiler.Validate(document, LocalStageCatalog.Instance).IsValid);
    }

    [Fact]
    public void OneInputCarriesAtMostOneEdge()
    {
        // "Connected exactly per its cardinality" is two rules and this is the other one, which the
        // document itself enforces: a port is an address, and an address carries one edge. Joining two
        // streams is wiring two ports, never wiring one port twice.
        ArgumentException refused = Assert.Throws<ArgumentException>(() => Declaring(
            [
                Node("stage-1", "from-enumerable"),
                Node("stage-2", "from-enumerable"),
                Node("stage-3", "merge"),
                Node("stage-4", "ignore"),
            ],
            [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 0), Edge("stage-3", "stage-4")],
            []));

        Assert.Contains("terminates at the input port 'stage-3#in-0'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("fan-in is a junction stage rather than edge multiplicity", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnInterleaveWithoutAPositiveSegmentSizeDoesNotValidate()
    {
        // The one number a junction carries, and the one junction payload there is to get wrong. Zero is a
        // real count for a take and a skip and is not one here: a rotation that takes nothing from an input
        // is a junction that never emits, so the reader that the runtime uses is the reader that refuses it.
        GraphDocument document = Declaring(
            [
                Node("stage-1", "from-enumerable"),
                Node("stage-2", "from-enumerable"),
                Interleaving("stage-3", 0),
                Node("stage-4", "ignore"),
            ],
            [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
            []);

        GraphValidationReport report = GraphCompiler.Validate(document, LocalStageCatalog.Instance);

        Assert.False(report.IsValid);
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.Rule == "invalid-parameters" && diagnostic.Subject == "stage-3");
    }

    [Fact]
    public void OnlyTheInterleaveAmongTheJoiningJunctionsDeclaresAPayload()
    {
        // Recorded as a test because it is a decision rather than an omission. How many streams a junction
        // joins is stated by its edges, so no junction writes an arity down; how many elements a rotation
        // takes from one of them before moving on is not an edge at all, so the one junction that rotates
        // on a count writes that count down and the other four carry the empty payload. The two
        // row-building junctions are the sharpest case of the rule: what they emit is built by a combiner,
        // and a combiner is behavior, so there is nothing at all for their documents to state.
        foreach (string stage in (string[])["merge", "concat", "interleave", "zip", "combine-latest"])
        {
            Assert.True(
                LocalStageCatalog.Instance.TryGetSpecification(
                    StageRef.Create(ProviderId.Create("local"), StageId.Create(stage), 1),
                    out StageSpecification? specification));
            Assert.Equal(LocalVocabulary.MaxFanIn, specification!.InputPorts.Count);
            Assert.Single(specification.OutputPorts);
            Assert.Empty(specification.ResultPorts);

            bool rotating = stage is "interleave";

            Assert.Equal(
                rotating ? "local-interleave-parameters" : "local-parameters",
                specification.ParameterContract.Contract.Value);
            Assert.Equal(rotating, specification.ParameterValidator is not null);
        }
    }

    [Fact]
    public async Task TwoChainsThatNeverMeetAreStillRefused()
    {
        // Several sources are legal exactly when they converge, and this document is the case that shows
        // the "exactly when": one merge joining two of the three sources, and a fourth chain beside it that
        // no edge reaches. Every node is reachable from some source, so the walk finds nothing wrong; what
        // is wrong is that one outcome would have to speak for two streams that never meet.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "ignore"),
                    Node("stage-5", "from-enumerable"),
                    Node("stage-6", "ignore"),
                ],
                [
                    Into("stage-1", "stage-3", 0),
                    Into("stage-2", "stage-3", 1),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-5", "stage-6"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(2))),
                ("stage-3", LocalStageDescriptor.Merge()),
                ("stage-4", LocalStageDescriptor.Ignore()),
                ("stage-5", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(3))),
                ("stage-6", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("begin a chain and no junction joins what they feed", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'stage-5'", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACycleThroughAJoiningJunctionIsRefused()
    {
        // The shape a fan-in makes reachable for the first time, and the reason cycles are still a later
        // checkpoint: a junction fed by its own downstream is never built, because the last of its arrivals
        // never comes, and everything behind it is therefore a part of the document no walk reaches.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "merge"),
                    Node("stage-3", "broadcast"),
                    Node("stage-4", "ignore"),
                ],
                [
                    Into("stage-1", "stage-2", 0),
                    Edge("stage-2", "stage-3"),
                    Rejoins("stage-3", 0, "stage-2", 1),
                    Leg("stage-3", 1, "stage-4"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Merge()),
                ("stage-3", LocalStageDescriptor.Broadcast()),
                ("stage-4", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("do not form one graph", refused.Message, StringComparison.Ordinal);
        Assert.Contains("reached 1 of them", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ANodeFedByTwoStreamsThatIsNotAJunctionIsRefused()
    {
        // The document declares a junction and the binding declares a mapping. Neither plane is trusted to
        // imply the other, so what the runtime says is what it actually cannot do: this node is fed by two
        // streams, and joining two streams is not something a mapping knows how to be.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(2))),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-4", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("the node 'stage-3' is fed by more than one node", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnInterleaveBoundWhereTheDocumentDeclaresAMergeCannotReadItsSegmentSize()
    {
        // The half of the two-plane disagreement this runtime does catch. A rotation's segment size is
        // payload, and payload is read from the document; a node the document calls a merge carries no
        // segment size, so a binding that rotates has nothing to rotate by and the run is refused rather
        // than given a number nobody wrote.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "from-enumerable"),
                    Node("stage-3", "merge"),
                    Node("stage-4", "ignore"),
                ],
                [Into("stage-1", "stage-3", 0), Into("stage-2", "stage-3", 1), Edge("stage-3", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(2))),
                ("stage-3", LocalStageDescriptor.Interleave(2)),
                ("stage-4", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("the interleave 'stage-3' carries parameters", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'segmentSize' is missing", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AZipBoundToSomethingThatIsNotACombinerIsRefused()
    {
        // The other half of the two-plane split, on the junction that has behavior rather than payload. A
        // combiner is not durable topology, so nothing in the document says what shape it has; a binding
        // table built by hand can therefore carry anything, and what it carries is checked where the
        // mismatch is rather than in the middle of a run. Unreachable through the authoring API, whose
        // generic signatures build the combiner for the author.
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
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(2))),
                ("stage-3", LocalStageDescriptor.Zip((Func<int, int>)(value => value))),
                ("stage-4", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused =
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains(
            "must be bound to a combiner of its inputs' elements into one row",
            refused.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnInputThatWantsNothingLeavesTheJunctionAndTheOthersRunOn()
    {
        // A take of no elements is resolved when the plan is built, and in a joining graph that resolution
        // is per input: the source behind it is never enumerated, its input has ended before the run began,
        // and the junction runs on the inputs that are left. It is the fan-out's "one branch wanted
        // nothing" read from the other side of the graph.
        RecordingEnumerable<int> untouched = new(1, 2, 3);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Counted("stage-2", "take", 0),
                    Node("stage-3", "from-enumerable"),
                    Node("stage-4", "concat"),
                    Collect("stage-5", 8),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Into("stage-2", "stage-4", 0),
                    Into("stage-3", "stage-4", 1),
                    Edge("stage-4", "stage-5"),
                ],
                [Slot("joined", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(untouched)),
                ("stage-2", LocalStageDescriptor.Take(0)),
                ("stage-3", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(7, 8))),
                ("stage-4", LocalStageDescriptor.Concat()),
                ("stage-5", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes although one input wanted nothing");

        int[] joined = await run.GetValueAsync(Result<int[]>(graph, "joined"), TestToken);

        Assert.Equal([7, 8], joined);
        Assert.Equal(0, untouched.Pulls);
    }

    [Fact]
    public async Task FusionSurvivesInsideABranchThatEndsAtAJoiningJunction()
    {
        // A branch is cut at boundaries and nowhere else whichever junction ends it: two mappings between
        // the source and the merge add no channel and no slack, so the source gets exactly as far as it
        // does with none. A branch cut at every stage would have let two more elements in.
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
                    Node("stage-2", "select"),
                    Node("stage-3", "select"),
                    Node("stage-4", "empty"),
                    Node("stage-5", "merge"),
                    Node("stage-6", "for-each"),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Edge("stage-2", "stage-3"),
                    Into("stage-3", "stage-5", 0),
                    Into("stage-4", "stage-5", 1),
                    Edge("stage-5", "stage-6"),
                ],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Select((Func<int, int>)(value => value + 1))),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value * 2))),
                ("stage-4", LocalStageDescriptor.Empty()),
                ("stage-5", LocalStageDescriptor.Merge()),
                (
                    "stage-6",
                    Calling(_ =>
                    {
                        gate.Wait();
                        elements.Consumed();
                    }))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source reaches the bound a fused branch allows");

        Assert.False(exhausted.Task.IsCompleted);
        Assert.Equal(4, elements.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the fused branch is released");

        Assert.Equal(4, elements.PeakInFlight);
    }

    [Fact]
    public async Task ABufferBelowAJoiningJunctionIsThatJunctionsOwnOutputChannel()
    {
        // The mirror of the buffer standing on a leg of a fan-out: the author asked for four elements of
        // prefetch below the junction and the run holds four, not four plus a handoff. One parked sink,
        // four in the buffer, one in the junction's input channel, one in the source's hand.
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
                    Node("stage-2", "empty"),
                    Node("stage-3", "merge"),
                    Buffer("stage-4", 4),
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
                ("stage-4", Buffering(4)),
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

        await Reaches(run.Completion, "the run completes once the buffered sink is released");

        Assert.Equal(12, elements.Pulls);
        Assert.Equal(7, elements.PeakInFlight);
    }
}
