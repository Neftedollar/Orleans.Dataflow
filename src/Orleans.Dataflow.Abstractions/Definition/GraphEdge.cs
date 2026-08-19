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
/// Edges are ordered by origin node, origin port, target node, and target port, which is the canonical
/// order a document writes its wiring in. The default value sorts before every created one, so the order
/// is total over every instance instead of leaving a hole at the default.
/// </para>
/// <para>
/// The default value connects nothing: <see cref="IsDefault"/> reports it, the endpoint properties throw
/// for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct GraphEdge : IComparable<GraphEdge>, IComparable
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
    /// <paramref name="from"/> or <paramref name="to"/> is the default value.
    /// </exception>
    /// <remarks>
    /// Both endpoints naming one node is a self-loop, and a self-loop is a cycle of one node rather than a
    /// shape of its own. The edge is therefore built here, and the runtime that has to execute the loop is
    /// what decides whether it can — a cycle is legal exactly when it passes a
    /// boundary that can answer without room below it, and one node's output feeding its own input is
    /// tested by that rule like any other loop rather than by a special case here.
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

        return new GraphEdge(from, to);
    }

    /// <summary>
    /// Determines whether one edge sorts before another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts before <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <(GraphEdge left, GraphEdge right) => left.CompareTo(right) < 0;

    /// <summary>
    /// Determines whether one edge sorts before another in canonical order, or is equal to it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort after <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator <=(GraphEdge left, GraphEdge right) => left.CompareTo(right) <= 0;

    /// <summary>
    /// Determines whether one edge sorts after another in canonical order.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> sorts after <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >(GraphEdge left, GraphEdge right) => left.CompareTo(right) > 0;

    /// <summary>
    /// Determines whether one edge sorts after another in canonical order, or is equal to it.
    /// </summary>
    /// <param name="left">The left operand.</param>
    /// <param name="right">The right operand.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="left"/> does not sort before <paramref name="right"/>;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public static bool operator >=(GraphEdge left, GraphEdge right) => left.CompareTo(right) >= 0;

    /// <summary>
    /// Compares this edge with another in canonical order.
    /// </summary>
    /// <param name="other">The edge to compare with.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="other"/> sorts first.
    /// </returns>
    /// <remarks>
    /// The four keys are the origin node, the origin port, the target node, and the target port, in that
    /// order, which is the order <see cref="GraphDocument"/> and <see cref="Serialization.GraphEnvelopeWriter"/>
    /// write a document's wiring in. It is read off <see cref="PortAddress"/>'s own two-key order rather
    /// than restated here, so the two cannot drift apart. The default value carries no endpoint and sorts
    /// before every created edge; ordering is consistent with equality, because two edges compare equal
    /// exactly when both endpoints do.
    /// </remarks>
    public int CompareTo(GraphEdge other)
    {
        int comparison = _from.CompareTo(other._from);

        return comparison != 0 ? comparison : _to.CompareTo(other._to);
    }

    /// <summary>
    /// Compares this instance with another object in canonical order.
    /// </summary>
    /// <param name="obj">The object to compare with, which may be <see langword="null"/>.</param>
    /// <returns>
    /// A negative number when this instance sorts first, zero when the two are equal, and a positive
    /// number when <paramref name="obj"/> sorts first. A <see langword="null"/> always sorts first, which
    /// is the convention every <see cref="IComparable"/> implementation in .NET follows.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="obj"/> is not a <see cref="GraphEdge"/>.</exception>
    /// <remarks>
    /// The non-generic interface is implemented explicitly and exists for one reason: F#'s
    /// <c>comparison</c> constraint is satisfied by <see cref="IComparable"/> and not by
    /// <see cref="IComparable{T}"/>, so without it this type cannot key an F# <c>Set</c> or <c>Map</c>.
    /// C# callers bind to <see cref="CompareTo(GraphEdge)"/> instead and box nothing.
    /// </remarks>
    int IComparable.CompareTo(object? obj) => obj switch
    {
        null => 1,
        GraphEdge other => CompareTo(other),
        _ => throw new ArgumentException($"The argument must be a {nameof(GraphEdge)}.", nameof(obj)),
    };

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

    private static InvalidOperationException DefaultAccess() =>
        new(IdentifierGrammar.DescribeDefaultAccess(nameof(GraphEdge)));
}
