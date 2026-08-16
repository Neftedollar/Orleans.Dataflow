using Orleans.Dataflow.Identity;

namespace Orleans.Dataflow.Definition;

/// <summary>
/// One declared output port of a stage specification: a name, the element contract it produces, and
/// whether an edge has to originate at it.
/// </summary>
/// <remarks>
/// <para>
/// A port specification is catalog data, not graph data. It says what a stage family declares; whether a
/// particular node in a particular document actually consumes the port is a rule of the graph compiler
/// (ADR 0001).
/// </para>
/// <para>
/// <see cref="IsIgnorable"/> is the output-side counterpart of an optional input, and it is deliberately
/// a different word. An ignorable output is one whose elements may be dropped without changing what the
/// stage means, such as a diagnostic trace; an output that is not ignorable is one whose elements a graph
/// has to consume, so leaving it dangling is a wiring mistake the compiler reports rather than a silently
/// discarded result.
/// </para>
/// <para>
/// The default value declares no port: <see cref="IsDefault"/> reports it, the component properties throw
/// for it, and <see cref="ToString"/> renders a diagnostic literal for it rather than throwing.
/// </para>
/// </remarks>
public readonly record struct OutputPortSpecification
{
    /// <summary>The diagnostic text <see cref="ToString"/> renders for the default value.</summary>
    private const string DefaultText = "(default OutputPortSpecification)";

    private readonly PortId _id;
    private readonly ContractReference _elementContract;
    private readonly bool _isIgnorable;

    /// <summary>
    /// Initializes a new instance of the <see cref="OutputPortSpecification"/> struct.
    /// </summary>
    /// <param name="id">The validated port name.</param>
    /// <param name="elementContract">The validated element contract.</param>
    /// <param name="isIgnorable">Whether the port may be left unconnected.</param>
    private OutputPortSpecification(PortId id, ContractReference elementContract, bool isIgnorable)
    {
        _id = id;
        _elementContract = elementContract;
        _isIgnorable = isIgnorable;
    }

    /// <summary>
    /// Gets the name of this port within its stage specification.
    /// </summary>
    /// <value>A created <see cref="PortId"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which declares no port.
    /// </exception>
    /// <remarks>
    /// Port names are unique across the whole stage, inputs, outputs, and result ports together, so this
    /// name identifies the port without also naming its direction.
    /// </remarks>
    public PortId Id => IsDefault ? throw DefaultAccess() : _id;

    /// <summary>
    /// Gets the contract of the elements this port produces.
    /// </summary>
    /// <value>A created <see cref="ContractReference"/>.</value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which declares no port.
    /// </exception>
    public ContractReference ElementContract => IsDefault ? throw DefaultAccess() : _elementContract;

    /// <summary>
    /// Gets a value indicating whether a graph may leave this port unconnected.
    /// </summary>
    /// <value>
    /// <see langword="true"/> when the elements of this port may be dropped; otherwise
    /// <see langword="false"/>.
    /// </value>
    /// <exception cref="InvalidOperationException">
    /// This instance is the default value, which declares no port.
    /// </exception>
    public bool IsIgnorable => IsDefault ? throw DefaultAccess() : _isIgnorable;

    /// <summary>
    /// Gets a value indicating whether this instance is the uninitialized default value.
    /// </summary>
    /// <value><see langword="true"/> for the default value; otherwise <see langword="false"/>.</value>
    /// <remarks>
    /// <see cref="Create(PortId, ContractReference)"/> rejects a default component and is the only way to
    /// build a specification, so a specification either carries every component or none. Testing the name
    /// alone therefore identifies the default instance exactly.
    /// </remarks>
    public bool IsDefault => _id.IsDefault;

    /// <summary>
    /// Creates an output port specification whose elements a graph must consume.
    /// </summary>
    /// <param name="id">The port name; must not be the default value.</param>
    /// <param name="elementContract">The element contract; must not be the default value.</param>
    /// <returns>The validated specification, with <see cref="IsIgnorable"/> set to <see langword="false"/>.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="id"/> or <paramref name="elementContract"/> is the default value.
    /// </exception>
    /// <remarks>
    /// Not ignorable is the default because it is the safer answer: a port nobody remembered to classify
    /// is then reported as unconnected rather than quietly dropping the elements it produces. The flag is
    /// an overload rather than an optional parameter so that every call site reads the same in source and
    /// in a decompiled signature.
    /// </remarks>
    public static OutputPortSpecification Create(PortId id, ContractReference elementContract) =>
        Create(id, elementContract, isIgnorable: false);

    /// <summary>
    /// Creates an output port specification with an explicit ignorability.
    /// </summary>
    /// <param name="id">The port name; must not be the default value.</param>
    /// <param name="elementContract">The element contract; must not be the default value.</param>
    /// <param name="isIgnorable">
    /// <see langword="true"/> when a graph may leave the port unconnected; otherwise
    /// <see langword="false"/>.
    /// </param>
    /// <returns>The validated specification.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="id"/> or <paramref name="elementContract"/> is the default value.
    /// </exception>
    public static OutputPortSpecification Create(PortId id, ContractReference elementContract, bool isIgnorable)
    {
        if (id.IsDefault)
        {
            throw new ArgumentException(DescribeDefaultMember(nameof(PortId), "port"), nameof(id));
        }

        if (elementContract.IsDefault)
        {
            throw new ArgumentException(
                DescribeDefaultMember(nameof(ContractReference), "element contract"),
                nameof(elementContract));
        }

        return new OutputPortSpecification(id, elementContract, isIgnorable);
    }

    /// <summary>
    /// Returns a diagnostic summary of this specification, or a literal when this instance is the default
    /// value.
    /// </summary>
    /// <returns>
    /// Text of the form <c>out: order@v1</c>, with <c> (ignorable)</c> appended when the port is
    /// ignorable, or <c>"(default OutputPortSpecification)"</c> when <see cref="IsDefault"/> is
    /// <see langword="true"/>.
    /// </returns>
    /// <remarks>
    /// The text is for logs and debugger display, not for serialization: the byte form of a port
    /// specification is the catalog envelope, which spells every member out. This method never throws.
    /// </remarks>
    public override string ToString() =>
        IsDefault
            ? DefaultText
            : $"{_id}: {_elementContract}{(_isIgnorable ? " (ignorable)" : string.Empty)}";

    /// <summary>Builds the message for a member supplied as its default value.</summary>
    /// <param name="typeName">The member's type name.</param>
    /// <param name="role">The member's role in the specification, in prose.</param>
    /// <returns>A message naming the type and the role.</returns>
    private static string DescribeDefaultMember(string typeName, string role) =>
        $"An {nameof(OutputPortSpecification)} requires a created {typeName}; the default {typeName} names no {role}.";

    /// <summary>Builds the error for a component read from the default value.</summary>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException DefaultAccess() =>
        new(IdentifierGrammar.DescribeDefaultAccess(nameof(OutputPortSpecification)));
}
