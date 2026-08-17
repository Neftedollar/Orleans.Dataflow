using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Runtime;
using Orleans.Dataflow.Serialization;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// What the run planner does with a document that declares a control, or fails to, in a way the authoring
/// API would never produce.
/// </summary>
/// <remarks>
/// <para>
/// A control is the first slot whose absence is a hang rather than a missing value: nothing but the control
/// can offer a queue an element, so a queue nobody can reach is a run that waits for a producer that cannot
/// exist. That is refused with a sentence, which is the whole reason these documents are built by hand.
/// </para>
/// <para>
/// The other half is the split every parameterized stage keeps: the document decides the configuration and
/// the binding decides the behavior. A queue whose payload says one capacity and whose binding says another
/// runs the payload's, because otherwise a hand-built document's numbers would be decoration.
/// </para>
/// </remarks>
public sealed class ControlDocumentTests
{
    [Fact]
    public async Task AQueueWithNoControlSlotIsRefusedRatherThanRunForever()
    {
        RunnableGraph graph = Graph(
            Document(
                [Queue("stage-1", 2, "backpressure"), Node("stage-2", "ignore")],
                [Edge("stage-1", "stage-2")]),
            Bindings(("stage-1", Ingress(2)), ("stage-2", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("the queue 'stage-1' declares no control slot", refused.Message, StringComparison.Ordinal);
        Assert.Contains("nothing could ever offer it an element", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoQueuesInOneGraphAreNotAChainAndAreRefusedAsSuch()
    {
        // Why "two queues in one graph" has no authored spelling: a queue is a source, and a linear chain
        // has one. The refusal is not about controls at all and arrives before the run planner even sees
        // the document — a second source leaves an output port nothing consumes, which the graph compiler's
        // connectivity rule rejects. A second ingress arrives with the fan-in junctions of a later
        // milestone, not with a second control.
        RunnableGraph graph = Graph(
            WithSlots(
                [Queue("stage-1", 2, "backpressure"), Queue("stage-2", 2, "backpressure"), Node("stage-3", "ignore")],
                [Edge("stage-2", "stage-3")],
                [Slot("left", "stage-1", "control", "local-control"), Slot("right", "stage-2", "control", "local-control")]),
            Bindings(
                ("stage-1", Ingress(2)),
                ("stage-2", Ingress(2)),
                ("stage-3", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("does not validate against the local stage catalog", refused.Message, StringComparison.Ordinal);
        Assert.Contains("stage-1", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ATerminalWithTwoResultSlotsIsRefused()
    {
        RunnableGraph graph = Graph(
            WithSlots(
                [Node("stage-1", "from-enumerable"), Node("stage-2", "count")],
                [Edge("stage-1", "stage-2")],
                [Slot("total", "stage-2", "result", "local-result"), Slot("again", "stage-2", "result", "local-result")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Count())));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("declares more than one result slot", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AQueueReadsItsCapacityAndPolicyFromTheDocumentRatherThanFromItsBinding()
    {
        // The payload says one element and drop-newest; the binding says a hundred and backpressure. The
        // run behaves as the document says, which is what makes a capacity in a document a fact rather
        // than decoration.
        Gate gate = new();

        RunnableGraph graph = Graph(
            WithSlots(
                [Queue("stage-1", 1, "drop-newest"), Node("stage-2", "for-each")],
                [Edge("stage-1", "stage-2")],
                [Slot("ingress", "stage-1", "control", "local-control")]),
            Bindings(
                ("stage-1", Ingress(100, OverflowPolicy.Backpressure)),
                ("stage-2", LocalStageDescriptor.ForEach((Action<int>)(_ => gate.Wait())))),
            Controls());

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);
        IIngressQueue<int> queue = await run.GetValueAsync(graph.Control<IIngressQueue<int>>("ingress"), TestToken);

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(1, TestToken));
        await gate.Reached;

        Assert.Equal(QueueOfferOutcome.Accepted, await queue.OfferAsync(2, TestToken));
        Assert.Equal(QueueOfferOutcome.Dropped, await queue.OfferAsync(3, TestToken));

        queue.Complete();
        gate.Open();

        await run.Completion;

        Assert.Equal(1L, run.DroppedElements);
    }

    [Fact]
    public async Task AQueueWhoseParametersThisRuntimeCannotReadIsRefusedWhereTheyAreRead()
    {
        RunnableGraph graph = Graph(
            WithSlots(
                [
                    Node("stage-1", "queue", "local-buffer-parameters", """{"capacity":2,"overflowPolicy":"drop-later"}"""),
                    Node("stage-2", "ignore"),
                ],
                [Edge("stage-1", "stage-2")],
                [Slot("ingress", "stage-1", "control", "local-control")]),
            Bindings(("stage-1", Ingress(2)), ("stage-2", LocalStageDescriptor.Ignore())));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[invalid-parameters]", refused.Message, StringComparison.Ordinal);
        Assert.Contains("'drop-later'", refused.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""{"maxElements":0}""", "is 0, and it is a positive integer")]
    [InlineData("""{}""", "the member 'maxElements' is missing")]
    [InlineData("""{"maxElements":"3"}""", "is a string, and it is a positive integer")]
    [InlineData("""{"maxElements":3,"maxBytes":8}""", "'maxBytes' is not one this stage declares")]
    [InlineData("""[]""", "the payload is an array")]
    public async Task ACollectPayloadThisVocabularyCouldNotHaveWrittenIsRefusedWhereItIsRead(
        string payload,
        string reason)
    {
        RunnableGraph graph = Graph(
            WithSlots(
                [Node("stage-1", "from-enumerable"), Node("stage-2", "collect", "local-collect-parameters", payload)],
                [Edge("stage-1", "stage-2")],
                [Slot("seen", "stage-2", "result", "local-result")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", LocalStageDescriptor.Collect(new CollectOptions { MaxElements = 4 }, Freeze()))));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("[invalid-parameters]", refused.Message, StringComparison.Ordinal);
        Assert.Contains("stage-2", refused.Message, StringComparison.Ordinal);
        Assert.Contains(reason, refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACollectingSinkReadsItsBoundFromTheDocumentRatherThanFromItsBinding()
    {
        RunnableGraph graph = Graph(
            WithSlots(
                [
                    Node("stage-1", "from-enumerable"),
                    Node("stage-2", "collect", "local-collect-parameters", """{"maxElements":2}"""),
                ],
                [Edge("stage-1", "stage-2")],
                [Slot("seen", "stage-2", "result", "local-result")]),
            Bindings(
                ("stage-1", LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1, 2, 3))),
                ("stage-2", LocalStageDescriptor.Collect(new CollectOptions { MaxElements = 99 }, Freeze()))));

        await using RunHandle run = await Host.MaterializeAsync(graph, TestToken);

        CollectOverflowException failure =
            await Assert.ThrowsAsync<CollectOverflowException>(() => run.Completion);

        Assert.Contains("bounded at 2 elements", failure.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("queue")]
    [InlineData("collect")]
    [InlineData("to-channel")]
    [InlineData("from-async-enumerable")]
    [InlineData("from-channel")]
    public async Task ABindingOfTheWrongShapeIsReportedWhereTheMismatchIs(string stage)
    {
        (StageNode node, ResultSlotDefinition[] slots) = stage switch
        {
            "queue" => (Node("stage-1", "queue", "local-buffer-parameters", """{"capacity":2,"overflowPolicy":"backpressure"}"""),
                (ResultSlotDefinition[])[Slot("ingress", "stage-1", "control", "local-control")]),
            "collect" => (Node("stage-2", "collect", "local-collect-parameters", """{"maxElements":2}"""),
                [Slot("seen", "stage-2", "result", "local-result")]),
            _ when stage == "to-channel" => (Node("stage-2", stage), []),
            _ => (Node("stage-1", stage), []),
        };

        bool source = node.Id == NodeId.Create("stage-1");

        RunnableGraph graph = Graph(
            WithSlots(
                source ? [node, Node("stage-2", "ignore")] : [Node("stage-1", "from-enumerable"), node],
                [Edge("stage-1", "stage-2")],
                slots),
            Bindings(
                (
                    "stage-1",
                    source
                        ? Wrong(stage)
                        : LocalStageDescriptor.FromEnumerable(new RecordingEnumerable<int>(1))),
                ("stage-2", source ? LocalStageDescriptor.Ignore() : Wrong(stage))));

        InvalidOperationException refused = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await Host.MaterializeAsync(graph, TestToken));

        Assert.Contains("must be bound to", refused.Message, StringComparison.Ordinal);
    }

    /// <summary>Builds a document over local nodes that declares result slots of its own.</summary>
    /// <param name="nodes">The nodes.</param>
    /// <param name="edges">The edges.</param>
    /// <param name="slots">The result slots.</param>
    /// <returns>The document.</returns>
    private static GraphDocument WithSlots(
        IEnumerable<StageNode> nodes,
        IEnumerable<GraphEdge> edges,
        IEnumerable<ResultSlotDefinition> slots) =>
        GraphDocument.Create(
            GraphId.Create("anonymous"),
            GraphRevision.Create(GraphRevision.FirstRevisionNumber),
            [CapabilityToken.Nondeployable, CapabilityToken.EphemeralIdentity],
            nodes,
            edges,
            slots);

    /// <summary>Builds one result slot declaration.</summary>
    /// <param name="name">The slot name.</param>
    /// <param name="node">The producing node's identifier text.</param>
    /// <param name="port">The producing port's identifier text.</param>
    /// <param name="contract">The result contract identifier text.</param>
    /// <returns>The declaration.</returns>
    private static ResultSlotDefinition Slot(string name, string node, string port, string contract) =>
        ResultSlotDefinition.Create(
            ResultSlotId.Create(name),
            ContractReference.Create(ContractId.Create(contract), 1),
            PortAddress.Create(NodeId.Create(node), PortId.Create(port)));

    /// <summary>Builds a queue node with a payload of its own.</summary>
    /// <param name="id">The node identifier text.</param>
    /// <param name="capacity">The declared capacity.</param>
    /// <param name="policy">The declared overflow policy, in the payload's own spelling.</param>
    /// <returns>The node.</returns>
    private static StageNode Queue(string id, int capacity, string policy) =>
        Node(
            id,
            "queue",
            "local-buffer-parameters",
            $$"""{"capacity":{{capacity}},"overflowPolicy":"{{policy}}"}""");

    /// <summary>Builds the binding of a queue occurrence.</summary>
    /// <param name="capacity">The capacity the binding declares, which the document overrides.</param>
    /// <param name="policy">The policy the binding declares, which the document overrides.</param>
    /// <returns>The descriptor.</returns>
    private static LocalStageDescriptor Ingress(
        int capacity,
        OverflowPolicy policy = OverflowPolicy.Backpressure) =>
        LocalStageDescriptor.Queue(
            new BufferOptions { Capacity = capacity, OverflowPolicy = policy },
            ResultSlotId.Create("ingress"),
            typeof(IIngressQueue<int>),
            (Func<LocalIngressQueue, object>)(queue => new IngressQueue<int>(queue)));

    /// <summary>Builds the control registry a hand-built queue graph carries beside its document.</summary>
    /// <returns>The one control, under the name the document declares it by.</returns>
    /// <remarks>
    /// Supplied the way the binding table is, and for the same reason: the type a control is handed back as
    /// lives in the C# type system and never in a document, so a document alone cannot answer for it.
    /// </remarks>
    private static Dictionary<ResultSlotId, Type> Controls() =>
        new() { [ResultSlotId.Create("ingress")] = typeof(IIngressQueue<int>) };

    /// <summary>Builds the projection a collecting sink's binding carries.</summary>
    /// <returns>The projection over boxed elements.</returns>
    private static Func<object?, object?> Freeze() =>
        static state => ((List<object?>)state!).Select(element => (int)element!).ToArray();

    /// <summary>Builds a binding of a shape the named stage cannot use.</summary>
    /// <param name="stage">The stage identifier text.</param>
    /// <returns>The mismatched descriptor.</returns>
    /// <remarks>
    /// Every one of them is bound to a sequence, which is the right binding for exactly one stage and the
    /// wrong one for all of these.
    /// </remarks>
    private static LocalStageDescriptor Wrong(string stage) => stage switch
    {
        "queue" => LocalStageDescriptor.Queue(
            new BufferOptions { Capacity = 2 },
            ResultSlotId.Create("ingress"),
            typeof(IIngressQueue<int>),
            new RecordingEnumerable<int>(1)),
        "collect" => LocalStageDescriptor.Collect(new CollectOptions { MaxElements = 2 }, new RecordingEnumerable<int>(1)),
        "to-channel" => LocalStageDescriptor.ToChannel(new RecordingEnumerable<int>(1)),
        "from-async-enumerable" => LocalStageDescriptor.FromAsyncEnumerable(new RecordingEnumerable<int>(1)),
        _ => LocalStageDescriptor.FromChannel(new RecordingEnumerable<int>(1)),
    };
}
