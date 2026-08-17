using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Orleans.Dataflow.Testing;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The hand-built documents the junction tests are written against, and the small vocabulary of helpers
/// that keeps one of them readable.
/// </summary>
/// <remarks>
/// <para>
/// A junction has no authored spelling in this checkpoint, splitting or joining. The C# graph builder is a
/// later one, and adding a surface before the engine that has to keep its promises would be a spelling
/// nobody had proven. Every graph here is therefore built the way M0's tests build one: a document of nodes
/// and edges, and a binding table beside it, which is exactly the pair the definition model says a local
/// graph is.
/// </para>
/// <para>
/// That is not a workaround. A document with a junction in it is the durable half of a branching graph, so
/// tests written against it are tests of the thing that will still be there when the builder arrives; a
/// test written against a builder would also be testing the builder.
/// </para>
/// </remarks>
internal static class JunctionFixtures
{
    /// <summary>Builds a document over local nodes that declares result slots of its own.</summary>
    /// <param name="nodes">The nodes.</param>
    /// <param name="edges">The edges.</param>
    /// <param name="slots">The result slots.</param>
    /// <returns>The document.</returns>
    internal static GraphDocument Declaring(
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

    /// <summary>Builds one result slot declaration on a sink's result port.</summary>
    /// <param name="name">The slot name.</param>
    /// <param name="node">The producing node's identifier text.</param>
    /// <param name="contract">The result contract identifier text, which is the general one by default.</param>
    /// <returns>The declaration.</returns>
    internal static ResultSlotDefinition Slot(string name, string node, string contract = "local-result") =>
        ResultSlotDefinition.Create(
            ResultSlotId.Create(name),
            ContractReference.Create(ContractId.Create(contract), 1),
            PortAddress.Create(NodeId.Create(node), PortId.Create("result")));

    /// <summary>Builds the node a probing source stands at, which is an ingress queue of one element.</summary>
    /// <param name="id">The node identifier text.</param>
    /// <returns>The node.</returns>
    /// <remarks>
    /// The capacity is the handover a probe declares and not a choice made here: room for the element being
    /// handed over and no room for a second, which is what makes an emit a rendezvous rather than a write
    /// into a buffer. Spelling it out is what a hand-built document has to do, and it agrees with the
    /// binding by construction because both come from the same authoring value.
    /// </remarks>
    internal static StageNode Emitter(string id) =>
        Node(id, "queue", "local-buffer-parameters", """{"capacity":1,"overflowPolicy":"backpressure"}""");

    /// <summary>Builds the node a probing sink stands at.</summary>
    /// <param name="id">The node identifier text.</param>
    /// <returns>The node.</returns>
    internal static StageNode Receiver(string id) => Node(id, "sink-probe");

    /// <summary>Builds one control slot declaration on a stage's control port.</summary>
    /// <param name="name">The slot name, which is also the name the probe is asked for by.</param>
    /// <param name="node">The producing node's identifier text.</param>
    /// <returns>The declaration.</returns>
    /// <remarks>
    /// The counterpart of <see cref="Slot"/> for the other kind of value a document declares. A control is
    /// produced by the <c>control</c> port and resolves at the start of a run rather than at its end, which
    /// is what lets a test hold one end of a branching graph while the run is still moving.
    /// </remarks>
    internal static ResultSlotDefinition Control(string name, string node) =>
        ResultSlotDefinition.Create(
            ResultSlotId.Create(name),
            ContractReference.Create(ContractId.Create("local-control"), 1),
            PortAddress.Create(NodeId.Create(node), PortId.Create("control")));

    /// <summary>Builds the control registry a hand-built document's probes are resolved through.</summary>
    /// <param name="controls">The pairs of slot name and control type, in any order.</param>
    /// <returns>The registry.</returns>
    internal static IReadOnlyDictionary<ResultSlotId, Type> Controls(
        params (string Name, Type Control)[] controls) =>
        controls.ToDictionary(control => ResultSlotId.Create(control.Name), control => control.Control);

    /// <summary>Takes the binding of a probing source out of the authoring value that spells it.</summary>
    /// <typeparam name="T">The element type the probe emits.</typeparam>
    /// <param name="name">The name the probe is declared under, which the document declares too.</param>
    /// <returns>The descriptor, ready to be bound to a node of a hand-built document.</returns>
    /// <remarks>
    /// <para>
    /// The probes belong to the testing package and are ordinary stages of the local vocabulary, so a
    /// junction document uses the very ones an authored chain does rather than a hand-rolled imitation. What
    /// a chain-composing surface cannot do is put one at the head of a <em>branch</em>, so the occurrence is
    /// lifted out of the authoring value and bound to a node directly — which is the same back door every
    /// junction fixture here goes through, applied to a stage that already exists.
    /// </para>
    /// <para>
    /// Reusing the real occurrence is what makes the claim worth making: the demand meter, the rendezvous,
    /// and the terminal expectations under test are the ones a test author actually gets.
    /// </para>
    /// </remarks>
    internal static LocalStageDescriptor Emitting<T>(string name) =>
        (LocalStageDescriptor)TestSource.Probe<T>(name).Stages[0];

    /// <summary>Takes the binding of a probing sink out of the authoring value that spells it.</summary>
    /// <typeparam name="T">The element type the probe receives.</typeparam>
    /// <param name="name">The name the probe is declared under, which the document declares too.</param>
    /// <returns>The descriptor, ready to be bound to a node of a hand-built document.</returns>
    /// <remarks>The mirror of <see cref="Emitting{T}"/>, for the end of a branch rather than its head.</remarks>
    internal static LocalStageDescriptor Receiving<T>(string name) =>
        (LocalStageDescriptor)TestSink.Probe<T>(name).Stages[0];

    /// <summary>Builds the typed slot of one result a hand-built document declares.</summary>
    /// <typeparam name="TResult">The type the sink resolves.</typeparam>
    /// <param name="graph">The graph the slot belongs to.</param>
    /// <param name="name">The slot name the document declares.</param>
    /// <returns>The slot.</returns>
    /// <remarks>
    /// The back door the authoring API deliberately does not have, and the same one the binding table goes
    /// through: a slot is normally handed back by closing a graph, and a document built by hand was never
    /// closed. The identities are the run's own, so everything the handle checks about a slot still holds.
    /// </remarks>
    internal static ResultSlot<TResult> Result<TResult>(RunnableGraph graph, string name) =>
        ResultSlot<TResult>.Create(ResultSlotId.Create(name), graph.Fingerprint, graph.AuthoringNonce);

    /// <summary>Builds the edge from one numbered leg of a junction to a node's input.</summary>
    /// <param name="junction">The junction's identifier text.</param>
    /// <param name="leg">The zero-based position of the leg.</param>
    /// <param name="to">The consuming node's identifier text.</param>
    /// <returns>The edge.</returns>
    internal static GraphEdge Leg(string junction, int leg, string to) =>
        GraphEdge.Create(
            PortAddress.Create(NodeId.Create(junction), LocalVocabulary.FanOutPort(leg)),
            PortAddress.Create(NodeId.Create(to), PortId.Create("in")));

    /// <summary>Builds the edge from a node's output to one numbered input of a joining junction.</summary>
    /// <param name="from">The producing node's identifier text.</param>
    /// <param name="junction">The junction's identifier text.</param>
    /// <param name="input">The zero-based position of the input.</param>
    /// <returns>The edge.</returns>
    internal static GraphEdge Into(string from, string junction, int input) =>
        GraphEdge.Create(
            PortAddress.Create(NodeId.Create(from), PortId.Create("out")),
            PortAddress.Create(NodeId.Create(junction), LocalVocabulary.FanInPort(input)));

    /// <summary>Builds the edge from one leg of a splitting junction to one input of a joining one.</summary>
    /// <param name="splitting">The splitting junction's identifier text.</param>
    /// <param name="leg">The zero-based position of the leg.</param>
    /// <param name="joining">The joining junction's identifier text.</param>
    /// <param name="input">The zero-based position of the input.</param>
    /// <returns>The edge.</returns>
    /// <remarks>
    /// The shortest diamond there is: nothing at all stands between the split and the join, so both ends of
    /// the edge are numbered ports and neither is the ordinary <c>out</c> or <c>in</c>.
    /// </remarks>
    internal static GraphEdge Rejoins(string splitting, int leg, string joining, int input) =>
        GraphEdge.Create(
            PortAddress.Create(NodeId.Create(splitting), LocalVocabulary.FanOutPort(leg)),
            PortAddress.Create(NodeId.Create(joining), LocalVocabulary.FanInPort(input)));

    /// <summary>Builds an interleave node with the segment size it declares.</summary>
    /// <param name="id">The node identifier text.</param>
    /// <param name="segmentSize">The declared number of elements taken from one input per turn.</param>
    /// <returns>The node.</returns>
    internal static StageNode Interleaving(string id, int segmentSize) =>
        Node(id, "interleave", "local-interleave-parameters", $$"""{"segmentSize":{{segmentSize}}}""");

    /// <summary>Builds the edge from one half of an unzip to a node's input.</summary>
    /// <param name="junction">The unzip's identifier text.</param>
    /// <param name="half">The port name, which is <c>left</c> or <c>right</c>.</param>
    /// <param name="to">The consuming node's identifier text.</param>
    /// <returns>The edge.</returns>
    internal static GraphEdge Half(string junction, string half, string to) =>
        GraphEdge.Create(
            PortAddress.Create(NodeId.Create(junction), PortId.Create(half)),
            PortAddress.Create(NodeId.Create(to), PortId.Create("in")));

    /// <summary>Builds a collecting sink node with the element bound it declares.</summary>
    /// <param name="id">The node identifier text.</param>
    /// <param name="maxElements">The declared bound.</param>
    /// <returns>The node.</returns>
    internal static StageNode Collect(string id, int maxElements) =>
        Node(id, "collect", "local-collect-parameters", $$"""{"maxElements":{{maxElements}}}""");

    /// <summary>Builds the binding of a collecting sink over integers.</summary>
    /// <param name="maxElements">The bound the binding declares, which the document overrides.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Collecting(int maxElements) =>
        LocalStageDescriptor.Collect(
            new CollectOptions { MaxElements = maxElements },
            (Func<object?, object?>)(state =>
                ((List<object?>)state!).Select(element => (int)element!).ToArray()));

    /// <summary>Builds a counted operator node with the count it declares.</summary>
    /// <param name="id">The node identifier text.</param>
    /// <param name="stage">The stage identifier text, which is <c>take</c> or <c>skip</c>.</param>
    /// <param name="count">The declared count.</param>
    /// <returns>The node.</returns>
    internal static StageNode Counted(string id, string stage, int count) =>
        Node(id, stage, "local-count-parameters", $$"""{"count":{{count}}}""");

    /// <summary>Builds a buffer node with the capacity and policy it declares.</summary>
    /// <param name="id">The node identifier text.</param>
    /// <param name="capacity">The declared capacity.</param>
    /// <param name="policy">The declared overflow policy, in the payload's own spelling.</param>
    /// <returns>The node.</returns>
    internal static StageNode Buffer(string id, int capacity, string policy = "backpressure") =>
        Node(
            id,
            "buffer",
            "local-buffer-parameters",
            $$"""{"capacity":{{capacity}},"overflowPolicy":"{{policy}}"}""");

    /// <summary>Builds the binding of a buffer occurrence.</summary>
    /// <param name="capacity">The capacity the binding declares, which the document overrides.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Buffering(int capacity) =>
        LocalStageDescriptor.Buffer(new BufferOptions { Capacity = capacity });

    /// <summary>Builds the binding of a routed junction over integers.</summary>
    /// <param name="router">The routing function, answering the zero-based position of a leg.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Routing(Func<int, int> router) =>
        LocalStageDescriptor.Partition(router);

    /// <summary>Builds the binding of a synchronous per-element sink over integers.</summary>
    /// <param name="callback">What to do with each element.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Calling(Action<int> callback) =>
        LocalStageDescriptor.ForEach(callback);

    /// <summary>Builds the combiner a row-building junction is bound to, which renders a row as text.</summary>
    /// <returns>The combiner, in the boxed vocabulary the runtime speaks.</returns>
    /// <remarks>
    /// Text rather than a tuple, and for a reason worth stating: a combiner receives one element per wired
    /// input, so a tuple would need one shape per arity and the eight-input case would have none at all.
    /// Rendering the parts joined by a dash keeps one fixture honest for every arity and makes the assertion
    /// read as the row it is — <c>"1-10"</c> is the first element of each of two inputs, and nothing about
    /// the rendering is what is under test.
    /// </remarks>
    internal static Func<object?[], object?> Rows() => parts => string.Join('-', parts);

    /// <summary>Builds the binding of a collecting sink over the rows a junction emits.</summary>
    /// <param name="maxElements">The bound the binding declares, which the document overrides.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor CollectingRows(int maxElements) =>
        LocalStageDescriptor.Collect(
            new CollectOptions { MaxElements = maxElements },
            (Func<object?, object?>)(state =>
                ((List<object?>)state!).Select(element => (string)element!).ToArray()));

    /// <summary>Builds the binding of a synchronous per-element sink over the rows a junction emits.</summary>
    /// <param name="callback">What to do with each row.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor CallingRows(Action<string> callback) =>
        LocalStageDescriptor.ForEach(callback);

    /// <summary>Opens gates when the scope ends, however it ends.</summary>
    /// <param name="gates">The gates to open.</param>
    /// <returns>The scope, to be declared after the run handle so that it is disposed before it.</returns>
    /// <remarks>
    /// A gate holds a real thread, and a run cannot be disposed while one of its segments is held by one:
    /// an assertion that fails between closing a gate and opening it would otherwise turn a reported
    /// failure into a test host that never exits. Declaring this after the handle is what puts it in front
    /// of the handle's own disposal, because <see langword="using"/> declarations are released in reverse.
    /// Opening a gate twice is a no-op, so a test still opens its gates where it means to.
    /// </remarks>
    internal static IDisposable Releasing(params Gate[] gates) => new Release(gates);

    /// <summary>Completes holds when the scope ends, however it ends.</summary>
    /// <param name="holds">The sources whose tasks a source is blocked on.</param>
    /// <returns>The scope, to be declared after the run handle so that it is disposed before it.</returns>
    /// <remarks>
    /// The same rule a gate follows, for the hold a pull barrier is. A source blocked inside a barrier is
    /// blocking a real thread, and a run cannot be disposed while one of its segments is held by one: an
    /// assertion that fails between arming a barrier and releasing it would otherwise turn a reported
    /// failure into a test host that never exits. Completing an already-completed source is a no-op, so a
    /// test still releases its holds where it means to.
    /// </remarks>
    internal static IDisposable Completing(params TaskCompletionSource[] holds) => new Complete(holds);

    /// <summary>Awaits a task that a correct run always completes, and fails the test rather than hanging.</summary>
    /// <param name="reached">The task to await.</param>
    /// <param name="what">What the task means, for the failure message.</param>
    /// <returns>The awaited task.</returns>
    /// <remarks>
    /// Every claim in the fan-out suite is about something a run does rather than about how long it takes,
    /// so the deadline is never a timing assertion: it is the difference between a suite that reports a
    /// broken completion rule and one that hangs until the test host gives up. The margin is generous for
    /// the same reason.
    /// </remarks>
    internal static async Task Reaches(Task reached, string what)
    {
        Task first = await Task.WhenAny(reached, Task.Delay(TimeSpan.FromSeconds(30), TestToken));

        Assert.True(ReferenceEquals(first, reached), what);

        await reached;
    }

    /// <summary>The scope <see cref="Releasing"/> returns.</summary>
    /// <param name="gates">The gates to open when it ends.</param>
    private sealed class Release(Gate[] gates) : IDisposable
    {
        /// <inheritdoc/>
        public void Dispose()
        {
            for (int index = 0; index < gates.Length; index++)
            {
                gates[index].Open();
            }
        }
    }

    /// <summary>The scope <see cref="Completing"/> returns.</summary>
    /// <param name="holds">The sources to complete when it ends.</param>
    private sealed class Complete(TaskCompletionSource[] holds) : IDisposable
    {
        /// <inheritdoc/>
        public void Dispose()
        {
            for (int index = 0; index < holds.Length; index++)
            {
                _ = holds[index].TrySetResult();
            }
        }
    }
}
