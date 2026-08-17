using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Compilation;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.JunctionFixtures;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the plan a branching document compiles to is, and what a document that is not one is answered
/// with.
/// </summary>
/// <remarks>
/// <para>
/// The plan model is not directly observable and deliberately so, but two of its properties are. Fusion
/// survives inside a branch — a junction-free chain of synchronous stages is still one loop holding one
/// element, whether it is a whole graph or a leg of one — and a buffer written next to a junction is that
/// junction's channel rather than a second one behind an implicit handoff. Both are counted the way the
/// buffer suite counts: how far a source gets while a consumer is parked.
/// </para>
/// <para>
/// The refusals are the other half. A junction's legs are ports, so how many an occurrence has is stated
/// by the edges that reach them, and the graph compiler's existing rules are what check the statement: at
/// most one edge per port address, and an edge at every port that is not ignorable. Nothing about that
/// needed a new rule — it needed a stage whose port list has more than one output in it, which is why
/// these tests are about validation rather than about the runtime.
/// </para>
/// </remarks>
public sealed class DagPlanTests
{
    [Fact]
    public async Task FusionSurvivesInsideAJunctionFreeBranch()
    {
        // Two mappings between the junction and the parked sink, and the source gets exactly as far as it
        // did with none: a fused branch is one loop holding one element, so the mappings add no channel and
        // no slack. A branch cut at every stage would have let two more elements in.
        Gate gate = new();
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
                    Node("stage-3", "select"),
                    Node("stage-4", "select"),
                    Node("stage-5", "for-each"),
                    Collect("stage-6", 16),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Edge("stage-4", "stage-5"),
                    Leg("stage-2", 1, "stage-6"),
                ],
                [Slot("seen", "stage-6")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Select((Func<int, int>)(value => value + 1))),
                ("stage-4", LocalStageDescriptor.Select((Func<int, int>)(value => value * 2))),
                ("stage-5", Calling(_ => gate.Wait())),
                ("stage-6", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source reaches the bound a fused branch allows");

        Assert.Equal(4, elements.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the fused branch is released");

        int[] seen = await run.GetValueAsync(Result<int[]>(graph, "seen"), TestToken);

        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9], seen);
    }

    [Fact]
    public async Task ABufferInFrontOfAJunctionIsThatJunctionsOwnInputChannel()
    {
        // The rule a buffer in front of an asynchronous stage follows, and for the same reason: the author
        // asked for four elements of prefetch and the run holds four, not four plus a handoff. One parked
        // sink, one in that leg's channel, four in the buffer, one in the source's hand.
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
                    Buffer("stage-2", 4),
                    Node("stage-3", "broadcast"),
                    Node("stage-4", "for-each"),
                    Collect("stage-5", 16),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Edge("stage-2", "stage-3"),
                    Leg("stage-3", 0, "stage-4"),
                    Leg("stage-3", 1, "stage-5"),
                ],
                [Slot("seen", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", Buffering(4)),
                ("stage-3", LocalStageDescriptor.Broadcast()),
                ("stage-4", Calling(_ => gate.Wait())),
                ("stage-5", Collecting(16))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        using IDisposable release = Releasing(gate);

        await Reaches(saturated.Task, "the source fills the buffer the author wrote and no more");

        Assert.Equal(7, elements.Pulls);

        gate.Open();

        await Reaches(run.Completion, "the run completes once the parked leg is released");

        int[] seen = await run.GetValueAsync(Result<int[]>(graph, "seen"), TestToken);

        Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], seen);
    }

    [Fact]
    public async Task AStageThatWantsNothingEndsItsOwnBranchAndNoOther()
    {
        // A take of no elements is resolved when the plan is built, and in a graph that resolution is per
        // branch: the leg behind it never receives anything, the other leg receives everything, and the
        // source is enumerated because somebody still wants it. In a chain the same stage means the source
        // is never touched at all, and both statements are the same rule read on the graph each belongs to.
        RecordingEnumerable<int> elements = new(1, 2, 3);

        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Counted("stage-3", "take", 0),
                    Collect("stage-4", 8),
                    Collect("stage-5", 8),
                ],
                [
                    Edge("stage-1", "stage-2"),
                    Leg("stage-2", 0, "stage-3"),
                    Edge("stage-3", "stage-4"),
                    Leg("stage-2", 1, "stage-5"),
                ],
                [Slot("none", "stage-4"), Slot("all", "stage-5")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(elements)),
                ("stage-2", LocalStageDescriptor.Broadcast()),
                ("stage-3", LocalStageDescriptor.Take(0)),
                ("stage-4", Collecting(8)),
                ("stage-5", Collecting(8))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        await Reaches(run.Completion, "the run completes although one branch wanted nothing");

        int[] none = await run.GetValueAsync(Result<int[]>(graph, "none"), TestToken);
        int[] all = await run.GetValueAsync(Result<int[]>(graph, "all"), TestToken);

        Assert.Empty(none);
        Assert.Equal([1, 2, 3], all);
        Assert.Equal(1, elements.Enumerations);
    }

    [Fact]
    public void AJunctionThatConnectsOneLegDoesNotValidate()
    {
        // The cardinality rule is the graph compiler's existing one and it needed nothing new: a junction's
        // first two legs are ports that are not ignorable, so leaving one unwired is an unconnected output.
        GraphDocument document = Declaring(
            [Node("stage-1", "from-enumerable"), Node("stage-2", "broadcast"), Node("stage-3", "ignore")],
            [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3")],
            []);

        GraphValidationReport report = GraphCompiler.Validate(document, LocalStageCatalog.Instance);

        Assert.False(report.IsValid);
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.Rule == "unconnected-output-port" && diagnostic.Subject == "stage-2#out-1");
    }

    [Fact]
    public void AJunctionThatLeavesItsLegsPastTheSecondUnwiredValidates()
    {
        // The other half of the same rule, and the reason a junction carries no arity payload: the edges
        // are what say how many legs this occurrence has, so the ports it did not wire are not a violation.
        GraphDocument document = Declaring(
            [
                Node("stage-1", "from-enumerable"),
                Node("stage-2", "broadcast"),
                Node("stage-3", "ignore"),
                Node("stage-4", "ignore"),
            ],
            [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
            []);

        Assert.True(GraphCompiler.Validate(document, LocalStageCatalog.Instance).IsValid);
    }

    [Fact]
    public void AnUnzipThatConnectsOneHalfDoesNotValidate()
    {
        GraphDocument document = Declaring(
            [Node("stage-1", "from-enumerable"), Node("stage-2", "unzip"), Node("stage-3", "ignore")],
            [Edge("stage-1", "stage-2"), Half("stage-2", "left", "stage-3")],
            []);

        GraphValidationReport report = GraphCompiler.Validate(document, LocalStageCatalog.Instance);

        Assert.False(report.IsValid);
        Assert.Contains(
            report.Diagnostics,
            diagnostic => diagnostic.Rule == "unconnected-output-port" && diagnostic.Subject == "stage-2#right");
    }

    [Fact]
    public void OneLegCarriesAtMostOneEdge()
    {
        // "Connected exactly per its cardinality" is two rules and this is the other one, which the
        // document itself enforces: a port is an address, and an address carries one edge.
        ArgumentException refused = Assert.Throws<ArgumentException>(() => Declaring(
            [
                Node("stage-1", "from-enumerable"),
                Node("stage-2", "broadcast"),
                Node("stage-3", "ignore"),
                Node("stage-4", "ignore"),
            ],
            [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 0, "stage-4")],
            []));

        Assert.Contains("originates at the output port 'stage-2#out-0'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("fan-out is a junction stage rather than edge multiplicity", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TwoLegsCannotMeetAtOneInputBeforeTheFanInJunctions()
    {
        // The structural rule that keeps this checkpoint honest about having no fan-in: an input port
        // carries one edge, so two legs cannot be joined by wiring them into one sink. Joining them is what
        // the fan-in pumps are for, and they are a later checkpoint.
        ArgumentException refused = Assert.Throws<ArgumentException>(() => Declaring(
            [Node("stage-1", "from-enumerable"), Node("stage-2", "broadcast"), Node("stage-3", "ignore")],
            [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-3")],
            []));

        Assert.Contains("terminates at the input port 'stage-3#in'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("fan-in is a junction stage rather than edge multiplicity", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABindingWhoseJunctionIsNotTheDocumentsIsRefused()
    {
        // The document says this node broadcasts and the binding says it unzips. Neither plane is trusted
        // to imply the other, so the disagreement is a sentence: an unzip's legs are named halves, and this
        // node's edges leave ports an unzip does not declare.
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "ignore"),
                    Node("stage-4", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                (
                    "stage-2",
                    LocalStageDescriptor.Unzip(
                        (Func<int, int>)(value => value),
                        (Func<int, int>)(value => value))),
                ("stage-3", LocalStageDescriptor.Ignore()),
                ("stage-4", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("the junction 'stage-2'", refused.Message, StringComparison.Ordinal);
        Assert.Contains("does not declare as an output", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnUnzipBoundToSomethingThatIsNotAProjectionIsRefused()
    {
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "unzip"),
                    Node("stage-3", "ignore"),
                    Node("stage-4", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Half("stage-2", "left", "stage-3"), Half("stage-2", "right", "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Unzip("not a function", (Func<int, int>)(value => value))),
                ("stage-3", LocalStageDescriptor.Ignore()),
                ("stage-4", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("must be bound to a array of Func<TRow, TPart> projections", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ABindingWhoseShapeIsNotAJunctionCannotStandWhereOneDoes()
    {
        RunnableGraph graph = Graph(
            Declaring(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "broadcast"),
                    Node("stage-3", "ignore"),
                    Node("stage-4", "ignore"),
                ],
                [Edge("stage-1", "stage-2"), Leg("stage-2", 0, "stage-3"), Leg("stage-2", 1, "stage-4")],
                []),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Select((Func<int, int>)(value => value))),
                ("stage-3", LocalStageDescriptor.Ignore()),
                ("stage-4", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("the node 'stage-2' feeds more than one node", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AJunctionDeclaresNoParameterPayloadOfItsOwn()
    {
        // Recorded as a test because it is a decision rather than an omission: an arity written beside the
        // edges would be a second statement of the same fact, and two statements can disagree.
        foreach (string stage in (string[])["broadcast", "balance", "unzip"])
        {
            Assert.True(
                LocalStageCatalog.Instance.TryGetSpecification(
                    StageRef.Create(ProviderId.Create("local"), StageId.Create(stage), 1),
                    out StageSpecification? specification));
            Assert.Equal("local-parameters", specification!.ParameterContract.Contract.Value);
            Assert.Null(specification.ParameterValidator);
            Assert.Empty(specification.ResultPorts);
            Assert.Single(specification.InputPorts);
        }
    }
}
