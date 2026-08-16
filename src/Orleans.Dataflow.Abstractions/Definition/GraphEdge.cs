using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// A directed connection from one node's output port to another node's input port.
/// </summary>
/// <remarks>
/// <para>
/// An edge carries no element contract of its own. The element type flowing over it is declared by the
/// port specifications of the two stages it connects, and the graph compiler checks that those
/// declarations agree; storing the contract twice would let a document contradict its own catalog.
/// </para>
/// <para>
/// Edge multiplicity is deliberately restricted by the document model: at most one edge leaves any output
/// port and at most one edge reaches any input port. Fan-out and fan-in are stages with their own
/// semantics, not a property of the wiring, so the wiring stays a plain one-to-one relation.
/// </para>
/// <para>
/// The default value connects nothing: <see cref="IsDefault"/> reports it, the endpoint properties throw
/// for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct GraphEdge
{
    private readonly PortAddress _from;
    private readonly PortAddress _to;

    private GraphEdge(PortAddress from, PortAddress to)
    {
        _from = from;
        _to = to;
    }

    /// <summary>
    /// Gets the output port the edge originates at.
    /// </summary>
    /// <value>A created <see cref="PortAddress"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which connects nothing.
    /// </exception>
    public PortAddress From => IsDefault ? throw DefaultAccess() : _from;

    /// <summary>
    /// Gets the input port the edge terminates at.
    /// </summary>
    /// <value>A created <see cref="PortAddress"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which connects nothing.
    /// </exception>
    public PortAddress To => IsDefault ? throw DefaultAccess() : _to;

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    /// <remarks>
    /// <see cref="Create"/> rejects a default endpoint and is the only way to build an edge, so an edge
    /// either carries both endpoints or neither. Testing the origin alone therefore identifies the default
    /// instance exactly.
    /// </remarks>
    public bool IsDefault => _from.IsDefault;

    /// <summary>
    /// Creates a <see cref="GraphEdge"/> from its endpoints.
    /// </summary>
    /// <param name="from">The output port the edge originates at; must not be the default value.</param>
    /// <param name="to">The input port the edge terminates at; must not be the default value.</param>
    /// <returns>The validated edge.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="from"/> or <paramref name="to"/> is the default value, or both endpoints name the
    /// same node.
    /// </exception>
    /// <remarks>
    /// Both endpoints naming one node is a self-loop, which this milestone rejects outright rather than
    /// accepting into a document no validator can yet reason about. Cycles arrive in a later milestone
    /// together with the explicit boundary contract that gives them defined completion and failure
    /// semantics.
    /// </remarks>
    public static GraphEdge Create(PortAddress from, PortAddress to)
    {
        if (from.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(GraphEdge)} requires a created origin {nameof(PortAddress)}; the default {nameof(PortAddress)} names no port.",
                nameof(from));
        }

        if (to.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(GraphEdge)} requires a created target {nameof(PortAddress)}; the default {nameof(PortAddress)} names no port.",
                nameof(to));
        }

        if (from.Node == to.Node)
        {
            throw new ArgumentException(DescribeSelfLoop(from, to), nameof(to));
        }

        return new GraphEdge(from, to);
    }

    /// <summary>
    /// Returns the canonical text form of this edge, or a diagnostic literal when this instance is the
    /// default value.
    /// </summary>
    /// <returns>
    /// Text of the form <c>node#port -&gt; node#port</c>, or <c>"(default GraphEdge)"</c> when
    /// <see cref="IsDefault"/> is <see langword="true"/>.
    /// </returns>
    /// <remarks>This method never throws.</remarks>
    public override string ToString() => IsDefault ? "(default GraphEdge)" : $"{_from} -> {_to}";

    /// <summary>
    /// Builds the message for a rejected self-loop edge.
    /// </summary>
    /// <param name="from">The origin endpoint.</param>
    /// <param name="to">The target endpoint.</param>
    /// <returns>A message naming the offending node and the rule it breaks.</returns>
    private static string DescribeSelfLoop(PortAddress from, PortAddress to) =>
        $"An edge from '{from}' to '{to}' is a self-loop on node '{from.Node}', which is not allowed in this milestone; cycles arrive with an explicit boundary contract in a later milestone.";

    private static InvalidOperationException DefaultAccess() =>
        new(IdentifierGrammar.DescribeDefaultAccess(nameof(GraphEdge)));
}
