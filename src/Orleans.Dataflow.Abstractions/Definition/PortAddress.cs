using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// One port on one node: the endpoint an edge connects and a result slot reads from.
/// </summary>
/// <remarks>
/// <para>
/// A port address names a place in a graph, never a place in a stage catalog. Whether the addressed port
/// actually exists on the resolved stage specification, and whether it is an input, an output, or a
/// result port, is a catalog rule checked by the graph compiler; the structural model only needs the
/// address to be well formed and to point at a declared node.
/// </para>
/// <para>
/// Addresses are compared by value, which is what lets the document model detect that two edges share an
/// endpoint without comparing text by hand.
/// </para>
/// <para>
/// The default value carries no address: <see cref="IsDefault"/> reports it, the component properties
/// throw for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct PortAddress
{
    private readonly NodeId _node;
    private readonly PortId _port;

    private PortAddress(NodeId node, PortId port)
    {
        _node = node;
        _port = port;
    }

    /// <summary>
    /// Gets the node that owns the addressed port.
    /// </summary>
    /// <value>A created <see cref="NodeId"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no address.
    /// </exception>
    public NodeId Node => IsDefault ? throw DefaultAccess() : _node;

    /// <summary>
    /// Gets the addressed port on <see cref="Node"/>.
    /// </summary>
    /// <value>A created <see cref="PortId"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which carries no address.
    /// </exception>
    public PortId Port => IsDefault ? throw DefaultAccess() : _port;

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    /// <remarks>
    /// <see cref="Create"/> rejects a default component and is the only way to build an address, so an
    /// address either carries both components or neither. Testing the node alone therefore identifies the
    /// default instance exactly.
    /// </remarks>
    public bool IsDefault => _node.IsDefault;

    /// <summary>
    /// Creates a <see cref="PortAddress"/> from its components.
    /// </summary>
    /// <param name="node">The node that owns the port; must not be the default value.</param>
    /// <param name="port">The port on that node; must not be the default value.</param>
    /// <returns>The validated port address.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="node"/> or <paramref name="port"/> is the default value.
    /// </exception>
    public static PortAddress Create(NodeId node, PortId port)
    {
        if (node.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(PortAddress)} requires a created {nameof(NodeId)}; the default {nameof(NodeId)} names no node.",
                nameof(node));
        }

        if (port.IsDefault)
        {
            throw new ArgumentException(
                $"A {nameof(PortAddress)} requires a created {nameof(PortId)}; the default {nameof(PortId)} names no port.",
                nameof(port));
        }

        return new PortAddress(node, port);
    }

    /// <summary>
    /// Returns the canonical text form of this address, or a diagnostic literal when this instance is the
    /// default value.
    /// </summary>
    /// <returns>
    /// Text of the form <c>node#port</c>, or <c>"(default PortAddress)"</c> when <see cref="IsDefault"/>
    /// is <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// The separator is <c>#</c> because it is not a character of the identifier grammar, so the text
    /// splits back into its two components unambiguously even though a node identifier may itself be a
    /// <c>/</c>-joined path. The method never throws.
    /// </remarks>
    public override string ToString() => IsDefault ? "(default PortAddress)" : $"{_node}#{_port}";

    private static InvalidOperationException DefaultAccess() =>
        new(IdentifierGrammar.DescribeDefaultAccess(nameof(PortAddress)));
}
