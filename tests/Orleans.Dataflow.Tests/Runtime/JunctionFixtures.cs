using Orleans.Dataflow.Authoring;
using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;
using Xunit;
using static Orleans.Dataflow.Tests.Runtime.RuntimeFixtures;

namespace Orleans.Dataflow.Tests.Runtime;

/// <summary>
/// The hand-built documents the fan-out tests are written against, and the small vocabulary of helpers
/// that keeps one of them readable.
/// </summary>
/// <remarks>
/// <para>
/// A junction has no authored spelling in this checkpoint. The C# graph builder is a later one, and adding
/// a surface before the engine that has to keep its promises would be a spelling nobody had proven. Every
/// graph here is therefore built the way M0's tests build one: a document of nodes and edges, and a
/// binding table beside it, which is exactly the pair the definition model says a local graph is.
/// </para>
/// <para>
/// That is not a workaround. A document with a junction in it is the durable half of a fan-out graph, so
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

    /// <summary>Builds the binding of a synchronous per-element sink over integers.</summary>
    /// <param name="callback">What to do with each element.</param>
    /// <returns>The descriptor.</returns>
    internal static LocalStageDescriptor Calling(Action<int> callback) =>
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
}
