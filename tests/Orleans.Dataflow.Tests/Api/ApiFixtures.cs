using Orleans.Dataflow.Definition;
using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Tests.Api;

/// <summary>
/// The domain types, sequences, and readers the C# authoring API tests are written against.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="OrderCreated"/> and <see cref="OrderDocument"/> are the types the flagship example in
/// C-SHARP-API.md names, spelled here so that the example can be pasted into a test verbatim.
/// </para>
/// <para>
/// The readers deliberately re-derive every identity from its text rather than from the production
/// constants. A test that echoed <c>LocalVocabulary</c> back at itself would pass no matter what those
/// constants said; spelling <c>local</c>, <c>select</c>, and <c>local-opaque</c> here is what makes the
/// assertions statements about the documented vocabulary.
/// </para>
/// </remarks>
internal static class ApiFixtures
{
    /// <summary>The order events the flagship example reads from.</summary>
    internal static IReadOnlyList<OrderCreated> OrderEvents { get; } =
    [
        new("order-1", 10m),
        new("order-2", 0m),
        new("order-3", 25m),
    ];

    /// <summary>Builds the stage reference of a local stage from its stage identifier text.</summary>
    /// <param name="stage">The stage identifier segment, such as <c>select</c>.</param>
    /// <returns>The stage reference under the <c>local</c> provider at major version 1.</returns>
    internal static StageRef LocalStage(string stage) =>
        StageRef.Create(ProviderId.Create("local"), StageId.Create(stage), 1);

    /// <summary>The local stages a document states completely, written out rather than derived.</summary>
    /// <value>
    /// The stage identifier texts of every shape a deployable run rebuilds from a node (ADR 0009), sorted
    /// ordinally.
    /// </value>
    /// <remarks>
    /// The independent statement of the vocabulary's own predicate. Asking
    /// <c>LocalVocabulary.RunsFromTheDocumentAlone</c> here would agree with the catalog whatever either of
    /// them said, because the catalog is built from it; a list written by hand is what makes a shape that
    /// silently changed sides fail a test rather than move both answers together. Three shapes bind no
    /// behavior and are deliberately absent: <c>first-or-default</c> and <c>last-or-default</c> carry a CLR
    /// default no document names, and <c>valve</c> produces a control an author reaches by name in the
    /// process that built the graph.
    /// </remarks>
    internal static IReadOnlyList<string> DeployableLocalStages { get; } =
    [
        "balance",
        "broadcast",
        "buffer",
        "concat",
        "count",
        "delay",
        "empty",
        "first",
        "ignore",
        "initial-delay",
        "interleave",
        "last",
        "merge",
        "never",
        "range",
        "skip",
        "skip-within",
        "take",
        "take-within",
        "tick",
        "timeout",
    ];

    /// <summary>Reports whether a document holds a stage whose behavior is bound in this process.</summary>
    /// <param name="document">The document to read.</param>
    /// <returns><see langword="true"/> when some node is a local stage this list does not admit.</returns>
    /// <remarks>
    /// The cause <c>nondeployable</c> tracks, read off the document against
    /// <see cref="DeployableLocalStages"/>. It used to be "some node's provider is <c>local</c>", which was
    /// the same answer until plumbing stopped requiring the token.
    /// </remarks>
    internal static bool HoldsBoundBehavior(GraphDocument document) =>
        document.Nodes.Any(node =>
            node.Stage.Provider.Value == "local" &&
            !DeployableLocalStages.Contains(node.Stage.Stage.Value));

    /// <summary>Builds a contract reference at major version 1 from its contract identifier text.</summary>
    /// <param name="contract">The contract identifier segment, such as <c>local-opaque</c>.</param>
    /// <returns>The contract reference.</returns>
    internal static ContractReference Contract(string contract) =>
        ContractReference.Create(ContractId.Create(contract), 1);

    /// <summary>Reads the node identifiers of a document in its canonical order.</summary>
    /// <param name="document">The document to read.</param>
    /// <returns>The identifier texts.</returns>
    internal static IReadOnlyList<string> NodeIds(GraphDocument document) =>
        [.. document.Nodes.Select(node => node.Id.Value)];

    /// <summary>Reads the stage identifiers of a document's nodes in its canonical order.</summary>
    /// <param name="document">The document to read.</param>
    /// <returns>The stage identifier texts, such as <c>from-enumerable</c>.</returns>
    internal static IReadOnlyList<string> StageIds(GraphDocument document) =>
        [.. document.Nodes.Select(node => node.Stage.Stage.Value)];

    /// <summary>Reads the edges of a document as text, in its canonical order.</summary>
    /// <param name="document">The document to read.</param>
    /// <returns>Texts of the form <c>stage-0001#out -> stage-0002#in</c>.</returns>
    internal static IReadOnlyList<string> Edges(GraphDocument document) =>
        [.. document.Edges.Select(edge => $"{edge.From} -> {edge.To}")];

    /// <summary>Reads the declared capability tokens of a document, in its canonical order.</summary>
    /// <param name="document">The document to read.</param>
    /// <returns>The token texts.</returns>
    internal static IReadOnlyList<string> Capabilities(GraphDocument document) =>
        [.. document.Capabilities.Select(token => token.Value)];

    /// <summary>An order as it arrives, in the flagship example.</summary>
    /// <param name="OrderId">The order identity.</param>
    /// <param name="Total">The order total; a nonpositive total marks the event invalid.</param>
    internal sealed record class OrderCreated(string OrderId, decimal Total)
    {
        /// <summary>Gets a value indicating whether this event is worth normalizing.</summary>
        internal bool IsValid => Total > 0m;
    }

    /// <summary>An order after normalization, in the flagship example.</summary>
    /// <param name="OrderId">The order identity.</param>
    /// <param name="Total">The order total.</param>
    internal sealed record class OrderDocument(string OrderId, decimal Total)
    {
        /// <summary>Normalizes an event into a document.</summary>
        /// <param name="order">The event.</param>
        /// <returns>The document.</returns>
        /// <remarks>
        /// A static method rather than a lambda, so that the flagship example's method-group argument to
        /// <c>Select</c> is exercised as written.
        /// </remarks>
        internal static OrderDocument FromEvent(OrderCreated order) => new(order.OrderId, order.Total);
    }
}
