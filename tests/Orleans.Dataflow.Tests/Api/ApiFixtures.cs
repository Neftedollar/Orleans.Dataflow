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
